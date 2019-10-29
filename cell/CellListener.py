from socket import *
from VFSFileInfo import *
from pythreader import PyThread
import cellmgr_global
from logs import Logged

from py3 import to_str, to_bytes

class   CellListener(PyThread, Logged):
        def __init__(self, myid, cfg, data_server, cell_storage, vfs_server_if):
                PyThread.__init__(self)
                self.MyID = myid
                self.Port = cfg['listen_port']
                self.FarmName = cfg['farm_name']
                self.Sock = socket(AF_INET, SOCK_DGRAM)
                self.Sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                self.Sock.bind(('',self.Port))
                self.MyHost = gethostbyname(gethostname())
                self.Enabled = False
                self.DataServer = data_server
                self.CellStorage = cell_storage
                self.VFSSrvIF = vfs_server_if
                
        def enable(self):
                self.Enabled = True
                self.wakeup()

        def disable(self):
                self.Enabled = False
                self.wakeup()

        def clientIsLocal(self, addr):
                return addr[0] == '127.0.0.1' or \
                        addr[0] == self.MyHost
                        
        def run(self):
            while True:
                if self.Enabled:
                    try:    msg, addr = self.Sock.recvfrom(10000)
                    except: continue
                    if not msg: continue
                    msg = to_str(msg)
                    self.debug("run: msg: [%s]" % (msg,))
                    #if addr[0] == self.MyHost:
                    #       return  # do not talk to myself - bad sign
                    #print 'rcvd: <%s> from <%s>' % (msg, addr)
                    words = msg.split()
                    if len(words) < 2:      continue
                    if words[1] != self.FarmName:   continue
                    cmd = words[0]
                    args = words[2:]
                    ans = None
                    #print("CellListener.run: cmd: %s args: %s" % (cmd, args))
                    if cmd == 'ACCEPT':
                            ans = self.doAccept(args, msg, addr, False)
                    elif cmd == 'ACCEPTR':
                            ans = self.doAccept(args, msg, addr, True)
                    elif cmd == 'PING':
                            ans = self.doPing(args, msg, addr)
                            #print 'ans=%s' % ans
                    elif cmd == 'SEND':
                            ans = self.doSend(args, msg, addr, False)
                    elif cmd == 'SENDR':
                            ans = self.doSend(args, msg, addr, True)
                    elif cmd == 'STATPSA':
                            ans = self.doStatPsa(args, msg, addr)
                    elif cmd == 'DPATH':
                            ans = self.doDPath(args, msg, addr)
                    elif cmd == 'OPEN':
                            ans = self.doOpen(args, msg, addr)
                    if ans != None:
                        try:    self.Sock.sendto(to_bytes(ans), addr)
                        except: pass
                else:
                    self.sleep()
                    
        def doDPath(self, args, msg, addr):
                if addr[0] != '127.0.0.1':
                        return None
                if len(args) < 2:
                        return None
                lp = args[0]
                ct = int(args[1])
                psa, info = self.CellStorage.findFile(lp)
                if not psa or not info or info.CTime != ct:
                        return None
                dp = psa.fullDataPath(lp)
                return dp

        def doStatPsa(self, args, msg, addr):
                # STATPSA <farm name> [<host> <port>]
                psalst = self.CellStorage.listPSAs()
                str = ''
                for psn in psalst:
                        size, used, rsrvd, free = \
                                cellmgr_global.CellStorage.getPSA(psn).status()
                        str = str + '%s %d %d %d %d\n' % (
                                psn, size, used, rsrvd, free)
                str = str + '.\n'
                str = str + self.DataServer.statTxns()
                retaddr = addr
                if len(args) >= 2:
                        try:
                                retaddr = (args[0], int(args[1]))
                        except:
                                return None
                try:    self.Sock.sendto(to_bytes(str), retaddr)
                except: pass
                return None

        def doPing(self, args, msg, addr):
                # PING <farm name> [<host> <port>]
                np = self.DataServer.putTxns()
                ng = self.DataServer.getTxns()
                nr = self.DataServer.repTxns()
                retaddr = addr
                if len(args) >= 2:
                        try:
                                retaddr = (args[0], int(args[1]))
                        except:
                                return None
                ans = 'PONG %s %d %d %d %s' % (self.MyID, np, ng, nr, 
                                self.CellStorage.status())
                #print("sending pong:", ans)
                try:    self.Sock.sendto(to_bytes(ans), retaddr)
                except: raise
                return None
                        
        def doAccept(self, args, msg, addr, nolocal=0):
                # ACCEPT <farm name> <nfrep> <lpath> <addr> <port> <info>
                #print ("doAccept(%s, %s, %s, %s)" % (args, msg, addr, nolocal))
                if not self.VFSSrvIF.Reconciled:
                        return None
                #if nolocal and self.clientIsLocal(addr):
                #        return None
                args = msg.split(None, 6)[2:]
                #print("doAccept: args:", args)
                if len(args) < 5:
                        return None
                #print 'doAccept: %s' % args
                nfrep = int(args[0])
                lp = args[1]
                sock_addr = (args[2], int(args[3]))
                info = VFSFileInfo(lp, args[4])
                if not self.DataServer.canReceive(lp):
                        self.log("can not receive %s" % (lp,))
                        return None
                txn, attrc = self.CellStorage.receiveFile(lp, info)
                if txn == None:
                        #print 'Storage can not receive'
                        self.log("storage did not return a transaction")
                        return None
                txn.NFRep = nfrep
                delay = txn.PSA.spaceUsage() * 1.0 + float(50 - attrc)/100.0
                if not self.clientIsLocal(addr) and not nolocal:
                        delay = delay + 0.5
                delay = max(0.0, delay)
                self.DataServer.recvSocket(txn, sock_addr, delay)
                return None
                
        def doSend(self, args, msg, addr, nolocal=0):
                # SEND <farm> <lpath> <ctime> <addr> <port>
                #print 'SEND %s' % args
                if len(args) < 4:
                        return None
                lp = args[0]
                ct = int(args[1])
                sock_addr = (args[2], int(args[3]))
                if not self.DataServer.canSend(lp):
                        #print 'Mover can not send'
                        return None
                psa, info = self.CellStorage.findFile(lp)
                print("doSend: findFile -> %s %s" % (psa, info))
                if info == None or (ct != 0 and info.CTime != ct):
                        #print 'Not found'
                        return None
                txn = self.CellStorage.sendFile(lp)
                if False and not nolocal and self.clientIsLocal(addr):
                        self.DataServer.sendLocal(txn, sock_addr, 0.0)
                else:
                        self.DataServer.sendSocket(txn, sock_addr, 0.0)
                return None

        def doOpen(self, args, msg, addr):
                # OPEN <lpath> <ctime> <addr> <port> <mode>
                if not self.VFSSrvIF.Reconciled:
                        return None
                if len(args) < 4:
                        return None
                lp = args[0]
                ct = int(args[1])
                sock_addr = (args[2], int(args[3]))
                if not self.DataServer.canOpenFile(lp, mode):
                        #print 'Mover can not send'
                        return None
                mode = args[4]
                psa,info = self.CellStorage.findFile(lp, mode)
                if info == None or (ct != 0 and info.CTime != ct):
                        #print 'Not found'
                        return None
                txn = self.CellStorage.openFile(lp, mode)
                if not self.clientIsLocal(addr):
                        time.sleep(0.5)
                self.DataServer.openFile(txn, sock_addr, info, mode)
                return None
