#
# @(#) $Id: CellListener.py,v 1.19 2002/10/31 17:52:33 ivm Exp $
#
# $Log: CellListener.py,v $
# Revision 1.19  2002/10/31 17:52:33  ivm
# v2_3
#
# Revision 1.18  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.17  2002/07/30 20:27:18  ivm
# Added FTPD
#
# Revision 1.16  2002/07/16 18:44:40  ivm
# Implemented data attractions
# v2_1
#
# Revision 1.15  2002/05/07 23:02:34  ivm
# Implemented attributes and info -0
#
# Revision 1.14  2002/04/30 20:07:15  ivm
# Implemented and tested:
#       node replication
#       node hold/release
#
# Revision 1.13  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.12  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.11  2001/06/29 18:52:49  ivm
# Tested v1_4 with farm name parameter
#
# Revision 1.10  2001/06/27 14:27:36  ivm
# Introduced farm_name parameter
#
# Revision 1.9  2001/06/15 22:12:25  ivm
# Fixed bug with replication stall
#
# Revision 1.7  2001/05/30 21:36:57  ivm
# Do answer to local requests
#
# Revision 1.6  2001/05/26 15:31:09  ivm
# Improved cell stat
#
# Revision 1.5  2001/05/23 19:52:50  ivm
# Use 127.0.0.1 for local uploads
#
# Revision 1.4  2001/05/22 13:27:19  ivm
# Fixed some bugs
# Implemented non-blocking send in Replicator
# Implemented ACCEPT Remote
#
# Revision 1.3  2001/04/12 16:02:31  ivm
# Fixed Makefiles
# Fixed for fcslib 2.0
#
# Revision 1.2  2001/04/11 20:59:50  ivm
# Fixed some bugs
#
# Revision 1.1  2001/04/04 14:25:47  ivm
# Initial CVS deposit
#
#

from socket import *
from VFSFileInfo import *
from pythreader import PyThread

from py3 import to_str, to_bytes

class   CellListener(PyThread):
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
                    print ("CellListener.doRead: msg <%s> from %s" % (repr(msg), addr))
                    #if addr[0] == self.MyHost:
                    #       return  # do not talk to myself - bad sign
                    #print 'rcvd: <%s> from <%s>' % (msg, addr)
                    words = msg.split()
                    if len(words) < 2:      continue
                    if words[1] != self.FarmName:   continue
                    cmd = words[0]
                    args = words[2:]
                    ans = None
                    if cmd == 'ACCEPT':
                            ans = self.doAccept(args, msg, addr)
                    elif cmd == 'ACCEPTR':
                            ans = self.doAccept(args, msg, addr, nolocal=1)
                    elif cmd == 'PING':
                            ans = self.doPing(args, msg, addr)
                            #print 'ans=%s' % ans
                    elif cmd == 'SEND':
                            ans = self.doSend(args, msg, addr)
                    elif cmd == 'SENDR':
                            ans = self.doSend(args, msg, addr, nolocal=1)
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
                retaddr = addr
                if len(args) >= 2:
                        try:
                                retaddr = (args[0], int(args[1]))
                        except:
                                return None
                ans = 'PONG %s %d %d %s' % (self.MyID, np, ng, 
                                self.CellStorage.status())
                print("sending pong:", ans)
                try:    self.Sock.sendto(to_bytes(ans), retaddr)
                except: raise
                return None
                        
        def doAccept(self, args, msg, addr, nolocal=0):
                # ACCEPT <farm name> <nfrep> <lpath> <addr> <port> <info>
                if not self.VFSSrvIF.Reconciled:
                        return None
                if nolocal and self.clientIsLocal(addr):
                        return None
                args = msg.split(None, 6)[2:]
                if len(args) < 5:
                        return None
                #print 'doAccept: %s' % args
                nfrep = int(args[0])
                lp = args[1]
                sock_addr = (args[2], int(args[3]))
                info = VFSFileInfo(lp, args[4])
                if not self.DataServer.canReceive(lp):
                        #print 'DataMover can not receive'
                        return None
                txn, attrc = self.CellStorage.receiveFile(lp, info)
                if txn == None:
                        #print 'Storage can not receive'
                        return None
                txn.NFRep = nfrep
                delay = txn.PSA.spaceUsage() * 1.0 + float(50 - attrc)/100.0
                if not self.clientIsLocal(addr) and not nolocal:
                        delay = delay + 0.5
                delay = max(0.0, delay)
                self.DataServer.recvSocket(txn, sock_addr, delay)
                return None
                
        def doSend(self, args, msg, addr, nolocal=0):
                # SEND <lpath> <ctime> <addr> <port>
                #print 'SEND %s' % args
                if len(args) < 4:
                        return None
                lp = args[0]
                ct = int(args[1])
                sock_addr = (args[2], int(args[3]))
                if not self.DataServer.canSend(lp):
                        #print 'Mover can not send'
                        return None
                psa,info = self.CellStorage.findFile(lp)
                if info == None or (ct != 0 and info.CTime != ct):
                        #print 'Not found'
                        return None
                txn = self.CellStorage.sendFile(lp)
                if False and not nolocal and self.clientIsLocal(addr):
                        self.DataServer.sendLocal(txn, sock_addr, 0.0)
                else:
                        self.DataServer.sendSocket(txn, sock_addr, 0.5)
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
