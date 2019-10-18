#
# @(#) $Id: dfarm_api.py,v 1.36 2003/06/06 17:31:37 ivm Exp $
#
# $Log: dfarm_api.py,v $
# Revision 1.36  2003/06/06 17:31:37  ivm
# Asynchronous processing of STOR and RETR in ftpd
#
# Revision 1.35  2003/03/25 17:36:46  ivm
# Implemented non-blocking directory listing transmission
# Implemented single inventory walk-through
# Implemented re-tries on failed connections to VFS Server
#
# Revision 1.34  2003/02/24 16:41:44  ivm
# Implemented recursive delete
#
# Revision 1.33  2003/01/30 17:33:11  ivm
# .
#
# Revision 1.32  2002/10/31 17:52:33  ivm
# v2_3
#
# Revision 1.31  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.30  2002/08/23 18:11:36  ivm
# Implemented Kerberos authorization
#
# Revision 1.29  2002/08/16 19:18:28  ivm
# Implemented size estimates for ftpd
#
# Revision 1.28  2002/07/30 20:27:18  ivm
# Added FTPD
#
# Revision 1.27  2002/07/26 19:09:09  ivm
# Bi-directional EOF confirmation. Tested.
#
# Revision 1.26  2002/07/16 18:44:40  ivm
# Implemented data attractions
# v2_1
#
# Revision 1.25  2002/06/28 14:01:44  ivm
# Pass config file to the client as parameter
#
# Revision 1.24  2002/05/08 16:49:18  ivm
# Added new features to the Usage
#
# Revision 1.21  2002/05/02 17:53:49  ivm
# v2_0, tested, fixed minor bugs in API
#
# Revision 1.20  2002/04/30 20:07:15  ivm
# Implemented and tested:
#       node replication
#       node hold/release
#
# Revision 1.19  2001/10/12 21:12:01  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.18  2001/10/07 15:45:19  ivm
# Implemented multi-file put
#
# Revision 1.17  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.16  2001/06/29 18:52:49  ivm
# Tested v1_4 with farm name parameter
#
# Revision 1.15  2001/06/27 14:27:35  ivm
# Introduced farm_name parameter
#
# Revision 1.14  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.13  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.12  2001/05/29 15:40:20  ivm
# Accept local directory in "get logpath localdir"
# Print error returned by "ls"
#
# Revision 1.11  2001/05/26 15:31:09  ivm
# Improved cell stat
#
# Revision 1.10  2001/05/23 19:53:42  ivm
# .
#
# Revision 1.4  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.3  2001/04/11 20:59:50  ivm
# Fixed some bugs
#
# Revision 1.2  2001/04/04 18:05:57  ivm
# *** empty log message ***
#
# Revision 1.1  2001/04/04 14:25:47  ivm
# Initial CVS deposit
#
#

from SockStream import SockStream
from VFSFileInfo import *
import select
import os
import time
import stat
import pwd
from socket import *
from config import ConfigFile

from py3 import to_bytes, to_str

DiskFarmError = 'DiskFarmError'

class   CellInfo:
        def __init__(self, nname):
                self.Node = nname
                self.PSAs = []
                self.Txns = []

class   FileHandle:
        def __init__(self, dfc, info):
                self.Sock = socket(AF_INET, SOCK_DGRAM)
                self.Sock.bind(('',0))
                self.MyAddr = (gethostbyname(gethostname()), self.Sock.getsockname()[1])
                self.Info = info
                self.Mode = None
                self.DFC = dfc
                self.DAddr = None
                self.Offset = 0
                
        def open(self, mode, tmo = None):
                msg = 'OPEN %s %s %s %s %s %s' % (
                                self.Info.Path, self.Info.CTime, self.MyAddr[0],
                                self.MyAddr[1], mode
                        )
                self.Mode = mode
                t0 = time.time()
                done = 0
                while tmo == None or time.time() < t0 + tmo:
                        self.Sock.sendto(msg, self.DFC.CAddr)
                        r,w,e = select.select([self.Sock],[],[],2.0)
                        if r:
                                reply, addr = self.Sock.recvfrom(10000)
                                words = msg.split()
                                if len(words) >= 1 and words[0] == 'OK':
                                        self.DAddr = addr
                                        done = 1
                                        break
                return done

        def reopen(self, tmo = None):
                return self.open(self.Mode, tmo)

        def read(self, size, tmo = None):
                msg = 'READ %s %d' % (self.Offset, size)
                t0 = time.time()
                data = None
                while tmo == None or time.time() < t0 + tmo:
                        try:    self.Sock.sendto(msg, self.DAddr)
                        except: break
                        r,w,e = select.select([self.Sock],[],[],2.0)
                        if r:
                                reply, addr = self.Sock.recvfrom(10000)
                                words = msg.split(':', 1)
                                if len(words) >= 1:
                                        off = int(words[0])
                                        if off == self.Offset:
                                                data = words[1]
                                                self.Offset = self.Offset + len(data)
                                                break
                        else:
                                self.reopen(tmo)
                return data

class   DiskFarmClient:
        def __init__(self, cfg = None):
                if cfg == None:
                        cfg = os.environ['DFARM_CONFIG']
                if type(cfg) == type(''):
                        cfg = ConfigFile(cfg)
                self.Cfg = cfg
                self.CSock = socket(AF_INET, SOCK_DGRAM)
                self.CSock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
                self.CAddr = (cfg.getValue('cell','*','broadcast'),
                        cfg.getValue('cell','*','listen_port'))         
                self.DAddr = (cfg.getValue('vfssrv','*','host'),
                        cfg.getValue('vfssrv','*','api_port'))
                self.FarmName = cfg.getValue('cell','*','farm_name','*')
                self.NodeList = cfg.names('cell_class','*')
                if not self.NodeList:   self.NodeList = []
                self.NodeAddrMap = {}
                domain = cfg.getValue('cell','*','domain','')
                if domain and domain[0] != '.':
                        domain = '.' + domain
                for n in self.NodeList:
                        self.NodeAddrMap[n] = n + domain
                self.DSock = None
                self.DStr = None
                self.Username = None
                for i in range(10):
                        try:    self.Username = pwd.getpwuid(os.getuid())[0]
                        except:
                                time.sleep(1)
                        else:
                                break
                if not self.Username:
                        raise ValueError('Can not determine clients username. Possible NIS problem')

        def connect(self):
                if self.DStr != None:   return
                connected = 0
                tmo = 10
                retry = 10
                while retry > 0 and not connected:
                        self.DSock = socket(AF_INET, SOCK_STREAM)
                        try:    self.DSock.connect(self.DAddr)
                        except:
                                self.DSock.close()
                                retry = retry - 1
                                # server is out, let's wait and retry
                                if retry:       time.sleep(10)
                        else:
                                connected = 1
                if not connected:
                        raise DiskFarmError("Can not connect to VFS Server")
                self.DStr = SockStream(self.DSock)
                ans = self.DStr.sendAndRecv('HELLO %s' % self.Username)
                if ans != 'OK':
                        self.disconnect()
                        raise DiskFarmError("Error connecting to VFS Server: <%s>" % (ans,))

        def disconnect(self):
                if self.DSock != None:
                        self.DSock.close()
                        self.DSock = None
                if self.DStr != None:
                        self.DStr = None

        def open(self, lpath, mode):
                info = self.getInfo(lpath)
                if not info or info.Type != 'f':
                        raise DiskFarmError('File not found')
                h = FileHandle(self, info)
                sts = h.open(mode)
                if not sts:
                        raise DiskFarmError('Open failed')
                return h

        def holdNodes(self, nlst):
                if type(nlst) != type([]):
                        nlst = [nlst]
                self.connect()
                ans = self.DStr.sendAndRecv('HOLD %s' % ' '.join(nlst))
                if not ans:
                        return None, 'Connection closed'
                words = ans.split(None, 1)
                if words[0] == 'OK':
                        return 1, 'OK'
                else:
                        return 0, words[1]
                
        def releaseNodes(self, nlst):
                if type(nlst) != type([]):
                        nlst = [nlst]
                self.connect()
                ans = self.DStr.sendAndRecv('RELEASE %s' % ' '.join(nlst))
                if not ans:
                        return None, 'Connection closed'
                words = ans.split(None, 1)
                if words[0] == 'OK':
                        return 1, 'OK'
                else:
                        return 0, words[1]

        def replicateNodes(self, mult, nlst):
                if type(nlst) != type([]):
                        nlst = [nlst]
                self.connect()
                ans = self.DStr.sendAndRecv('REPNODE %d %s' % (mult, ' '.join(nlst)))
                if not ans:
                        return None, 'Connection closed'
                words = ans.split(None, 1)
                if words[0] == 'OK':
                        return 1, 'OK'
                else:
                        return 0, words[1]

        def replicateFile(self, lpath, mult):
                self.connect()
                ans = self.DStr.sendAndRecv('RR %s %s' % (lpath, mult))
                self.disconnect()
                words = ans.split(None, 1)
                if words[0] != 'OK':
                        return 0, ans
                return 1, 'OK'

        def getUsage(self, user):
                self.connect()
                ans = self.DStr.sendAndRecv('USAGE %s' % user)
                self.disconnect()
                if not ans:
                        return None, 'Connection closed'
                words = abs.split()
                if words[0] != 'OK':
                        return None, words[1]
                if len(words) < 4:
                        return None, 'Protocol error: <%s>' % ans
                return eval(words[1]), eval(words[2]), eval(words[3])
                
        def listFiles(self, dir = '/'):
                self.connect()
                ans = self.DStr.sendAndRecv('LIST %s' % dir)
                words = ans.split()
                if not words or words[0] != 'OK':
                        self.disconnect()
                        return ans, []
                lst = []
                #print 'listFiles: ans: <%s>' % ans
                while ans and ans != '.':
                        #print 'listFiles: ans: <%s>' % ans
                        # ans: <fn> <typ> [<info>]
                        words = ans.split(None,2)
                        if len(words) >= 2:
                                fn, typ = tuple(words[:2])
                                info = None
                                if words[2:]:
                                        if typ == 'f':
                                                info = VFSFileInfo(fn, words[2])
                                        else:
                                                info = VFSDirInfo(fn, words[2])
                                lst.append((fn, typ, info))
                        ans = self.DStr.recv()
                self.disconnect()
                return 'OK', lst

        def getInfo(self, lpath):
                self.connect()
                ans = self.DStr.sendAndRecv('GET %s' % lpath)
                self.disconnect()
                if not ans:
                        return None, 'Connection closed'
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return None, words[1]
                words = words[1].split(None, 1)
                typ = words[0]
                info = None
                if typ == 'f':
                        info = VFSFileInfo(lpath, words[1])
                elif typ == 'd':
                        info = VFSDirInfo(lpath, words[1])
                return info, ''

        def getType(self, lpath):
                self.connect()
                ans = self.DStr.sendAndRecv('GETT %s' % lpath)
                self.disconnect()
                if not ans:
                        raise DiskFarmError('Connection closed')
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return ''
                return words[1]

        def exists(self, lpath):
                return self.getType(lpath) != ''

        def isDir(self, lpath):
                return self.getType(lpath) == 'd'
        
        def isFile(self, lpath):
                return self.getType(lpath) == 'f'

        def chmod(self, lpath, prot):
                self.connect()
                ans = self.DStr.sendAndRecv('CHMOD %s %s' % (lpath, prot))
                self.disconnect()
                if not ans:
                        raise DiskFarmError('Connection closed')
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return 0, ans
                return 1, 'OK'

        
        def setAttr(self, lpath, attr, value):
                self.connect()
                ans = self.DStr.sendAndRecv('SATTR %s %s %s' % (lpath, attr, value))
                self.disconnect()
                if not ans:
                        raise DiskFarmError('Connection closed')
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return 0, ans
                return 1, 'OK'

        def fileInfo(self, lpath, path, size = None):
                info = VFSFileInfo(lpath)
                if path:
                        st = os.stat(path)
                        info.setActualSize(st[stat.ST_SIZE])
                elif size != None:
                        info.setActualSize(size)
                if info.Size != None:
                        if info.Size >= 2*1024*1024*1024:
                                raise ValueError('Source file is too large, >= 2GB')
                return info

        def dirInfo(self, lpath):
                info = VFSDirInfo(lpath)
                return info
                                
        def createFile(self, info, ncopies):
                lpath = info.Path
                self.connect()
                info.CTime = 0
                ans = self.DStr.sendAndRecv('MF %s %s %s' % (lpath, ncopies, info.serialize()))
                self.disconnect()
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return None, words[1]
                else:
                        i1 = VFSFileInfo(lpath, words[1])
                return i1, ''

        def delFile(self, lpath):
                self.connect()
                ans = self.DStr.sendAndRecv('DF %s' % lpath)
                self.disconnect()
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return 0, ans
                return 1, 'OK'

        def delDir(self, lpath):
                self.connect()
                ans = self.DStr.sendAndRecv('DD %s' % lpath)
                self.disconnect()
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return 0, ans
                return 1, 'OK'

        def makeDir(self, lpath, info):
                self.connect()
                ans = self.DStr.sendAndRecv('MD %s %s' % (lpath, info.serialize()))
                self.disconnect()
                words = ans.split( None, 1)
                if words[0] != 'OK':
                        return 0, ans
                return 1, 'OK'

        def localDataPath(self, lpath, info):
                sock = socket(AF_INET, SOCK_DGRAM)
                r = []
                retry = 5
                while retry > 0 and not r:
                        try:    sock.sendto('DPATH %s %s %s' % (self.FarmName, lpath, 
                                                info.CTime), 
                                                ('127.0.0.1', self.CAddr[1]))
                        except:
                                break
                        r,w,e = select.select([sock],[],[],3)
                        retry = retry - 1
                ans = None
                if r:
                        ans, addr = sock.recvfrom(10000)
                        if not ans:
                                ans = None
                sock.close()
                return ans
                
        def cellInfo(self, node):
                sock = socket(AF_INET, SOCK_DGRAM)
                sock.sendto('STATPSA %s' % self.FarmName, (node, self.CAddr[1]))
                r,w,e = select.select([sock],[],[],30)
                if not r:
                        sock.close()
                        return None
                ans, addr = sock.recvfrom(100000)
                lines = ans.split('\n')
                st = CellInfo(node)
                # parse PSAs
                psalst = []
                while lines:
                        l = lines[0]
                        lines = lines[1:]
                        if l == '.':    break
                        words = l.split()
                        if len(words) < 5:      continue
                        psn, size, used, rsrvd, free = tuple(words[:5])
                        size = int(size)
                        used = int(used)
                        rsrvd = int(rsrvd)
                        free = int(free)
                        psalst.append((psn, size, used, rsrvd, free))
                st.PSAs = psalst
                # parse transactions
                txlst = []
                while lines:
                        l = lines[0]
                        lines = lines[1:]
                        if l == '.':    break
                        words = l.split()
                        if len(words) < 3:      continue
                        txlst.append(tuple(words[:3]))
                st.Txns = txlst                 
                return st

        def ping(self, pongcbk=None, donecbk=None):
                sock = socket(AF_INET, SOCK_DGRAM)
                sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
                t0 = time.time()
                lst = []
                sock.sendto('PING %s' % self.FarmName, self.CAddr)
                r,w,e = select.select([sock],[],[],3)
                t = time.time()
                #print r
                while r:
                        msg, addr = sock.recvfrom(10000)
                        #print msg, addr
                        if msg:
                                words = msg.split()
                                if len(words) >= 3 and words[0] == 'PONG':
                                        words = words[1:]
                                        cid = None
                                        if len(words) > 2:
                                                cid = words[0]
                                                words = words[1:]
                                        try:
                                                nput = int(words[0])
                                                nget = int(words[1])
                                        except:
                                                pass
                                        else:
                                                sts = ''
                                                if len(words) > 2:
                                                        sts = words[2]
                                                capdct = {}
                                                for w in words[3:]:
                                                        items = w.split(':')
                                                        try:
                                                                psan = items[0]
                                                                items = items[1:]
                                                                for i in range(3):
                                                                        if items[i][-1] == 'L':
                                                                                items[i] = items[i][:-1]
                                                                capdct[psan] = (int(items[0]),
                                                                                        int(items[1]), int(items[2]))
                                                        except:
                                                                pass
                                                lst.append((addr[0], cid, t-t0, nput, nget, sts, capdct))
                                                if pongcbk != None:
                                                        pongcbk(addr[0], cid, t-t0, nput, nget, sts, capdct)
                        if donecbk != None and donecbk():
                                break
                        r,w,e = select.select([sock],[],[],0)
                        if not r:
                                r,w,e = select.select([sock],[],[],3)
                                t = time.time()
                sock.close()
                return lst

        def pingParallel(self, pongcbk=None, donecbk=None, maxping=30):
                sock = socket(AF_INET, SOCK_DGRAM)
                sock.setblocking(0)
                t0 = time.time()
                sent_times = {}
                pingmsg = to_bytes('PING %s' % self.FarmName)
                itosend = 0
                nwait = 0
                lst = []
                while itosend < len(self.NodeList) or nwait > 0:
                        msgs = []
                        while nwait > 0:
                                # collect all responses arrived so far
                                try:    msg, addr = sock.recvfrom(100000)
                                except:
                                        break
                                nwait = nwait - 1
                                msgs.append((to_str(msg), addr, time.time()))
                        while nwait >= maxping or \
                                        (nwait > 0 and itosend >= len(self.NodeList)):
                                # wait for answers if necessary
                                r,w,e = select.select([sock],[],[],3)
                                if r:
                                        msg, addr = sock.recvfrom(100000)
                                        msgs.append((to_str(msg), addr, time.time()))
                                        nwait = nwait - 1
                                else:
                                        # we have waited for 3 seconds, and are not getting 
                                        # any answers - do not wait any longer
                                        nwait = 0
                        while nwait < maxping and itosend < len(self.NodeList):
                                # send as many pings as we are allowed
                                n = self.NodeList[itosend]
                                itosend = itosend + 1
                                nwait = nwait + 1
                                addr = (self.NodeAddrMap[n], self.CAddr[1])
                                try:    
                                    sock.sendto(pingmsg, addr)
                                    print ("ping sent to", addr)
                                except: 
                                    raise
                                sent_times[n] = time.time()
                        for msg, addr, t in msgs:
                                words = msg.split()
                                if len(words) >= 3 and words[0] == 'PONG':
                                        words = words[1:]
                                        cid = None
                                        if len(words) > 2:
                                                cid = words[0]
                                                words = words[1:]
                                        if not cid or cid not in sent_times:
                                                continue
                                        t0 = sent_times[cid]
                                        try:
                                                nput = int(words[0])
                                                nget = int(words[1])
                                        except:
                                                pass
                                        else:
                                                sts = ''
                                                if len(words) > 2:
                                                        sts = words[2]
                                                capdct = {}
                                                for w in words[3:]:
                                                        items = w.split(':')
                                                        try:
                                                                psan = items[0]
                                                                items = items[1:]
                                                                for i in range(3):
                                                                        if items[i][-1] == 'L':
                                                                                items[i] = items[i][:-1]
                                                                capdct[psan] = (int(items[0]),
                                                                                        int(items[1]), int(items[2]))
                                                        except:
                                                                pass
                                                lst.append((addr[0], cid, t-t0, nput, nget, sts, capdct))
                                                if pongcbk != None:
                                                        pongcbk(addr[0], cid, t-t0, nput, nget, sts, capdct)
                        if donecbk != None and donecbk():
                                break
                sock.close()
                return lst

        def sendBCast(self, msg):
                try:    self.CSock.sendto(msg, self.CAddr)
                except: pass            
                
        def sendSendBcast(self, lpath, info, ctl_sock, ctladdr, nolocal=0, tmo=None):
                if nolocal:
                        bcast = 'SENDR'
                else:
                        bcast = 'SEND'
                bcast = bcast + ' %s %s %s %s %s' % (self.FarmName, lpath, info.CTime,
                        ctladdr[0], ctladdr[1])         
                self.CSock.sendto(bcast, self.CAddr)

        def get(self, lpath, fn, info, nolocal = 0, tmo = None):
                ctl_sock = socket(AF_INET, SOCK_STREAM)
                ctl_sock.bind(('',0))
                ctl_sock.listen(1)
                ctl_port = ctl_sock.getsockname()[1]
                ctl_host = gethostbyname(gethostname())
                self.sendSendBcast(lpath, info, ctl_sock, (ctl_host, ctl_port))
                done = 0
                t0 = time.time()
                while not done and (tmo == None or time.time() < t0 + tmo):
                        done = self.waitForConnect(ctl_sock, 1)
                        if not done:
                                self.sendSendBcast(lpath, info, ctl_sock, (ctl_host, ctl_port))
                if not done:
                        return 0, 'Transfer initiation timeout'
                mover_ctl, addr = ctl_sock.accept()
                ctl_sock.close()
                str = SockStream(mover_ctl)
                msg = str.recv()
                if msg[:5] == 'LOCAL':
                        path = msg[5].strip()
                        sts, err = self.local_get(str, fn, path)
                elif msg == 'RECV':
                        sts, err = self.remote_get(str, fn, tmo)
                else:
                        sts, err = 0, 'Transfer initiation failed (wrong answer: <%s>)' % msg
                mover_ctl.close()
                return sts, err


        def remote_get(self, str, fn, tmo):
                data_sock = socket(AF_INET, SOCK_STREAM)
                data_sock.bind(('',0))
                data_port = data_sock.getsockname()[1]
                data_host = gethostbyname(gethostname())
                data_sock.listen(1)
                str.send('SENDTO %s %s' % (data_host, data_port))
                r,w,e = select.select([data_sock],[],[],20)
                if not r:
                        data_sock.close()
                        return 0, 'Transfer initiation timeout (no answer to SENDTO)'
                tx_sock, addr = data_sock.accept()
                data_sock.close()
                fd = open(fn, 'w')
                eof = 0
                t0 = time.time()
                size = 0
                while not eof:
                        if tmo != None:
                                r,w,e = select.select([tx_sock],[],[],tmo)
                                if not r:
                                        return 0, 'Data transfer time-out'
                        data = tx_sock.recv(60000)
                        if not data:
                                eof = 1
                        else:
                                fd.write(data)
                                size = size + len(data)
                t1 = time.time()
                fd.close()
                msg = str.recv()
                if msg != 'EOF':
                        # premature end
                        #os.remove(fn)
                        return 0, 'Transfer aborted'
                try:    sndr = gethostbyaddr(addr[0])[0]
                except: sndr = addr[0]
                rate = ''
                size = size/1024.0/1024.0
                if t1 > t0 and size >= 0:
                        rate = ' at %f MB/sec' % (size/(t1-t0))
                return 1,'Retrieved %f MB from %s%s' % (size, sndr, rate)

        def local_get(self, str, fn, path):
                fr = open(path, 'r')
                fw = open(fn, 'w')
                eof = 0
                t0 = time.time()
                size = 0
                while not eof:
                        data = fr.read(100000)
                        if not data:
                                eof = 1
                        else:
                                fw.write(data)
                                size = size + len(data)
                t1 = time.time()
                rate = ''
                size = size/1024.0/1024.0
                if t1 > t0 and size >= 0:
                        rate = ' at %f MB/sec' % (size/(t1-t0))
                fr.close()
                fw.close()
                str.send('OK')
                return 1, 'Copied locally %f MB %s' % (size, rate)


        def sendAcceptBcast(self, lpath, info, ctl_sock, ctladdr, ncopies,
                                                nolocal=0):
                cmd = 'ACCEPT'
                if nolocal: cmd = 'ACCEPTR'
                
                bcast = '%s %s %d %s %s %s %s' % (cmd, self.FarmName, ncopies-1, lpath,
                        ctladdr[0], ctladdr[1], info.serialize())
                #print 'Sending <%s> to <%s>' % (bcast, self.CAddr)
                self.CSock.sendto(bcast, self.CAddr)
                
        def waitForConnect(self, ctl_sock, tmo=None):
                r,w,d = select.select([ctl_sock],[],[],tmo)
                return ctl_sock in r

        def put(self, fn, lpath, info, ncopies = 1, nolocal = 0, tmo = None):
                # put a local file into disk farm as lpath
                ctl_sock = socket(AF_INET, SOCK_STREAM)
                ctl_sock.bind(('',0))
                ctl_sock.listen(1)
                ctl_port = ctl_sock.getsockname()[1]
                ctl_host = gethostbyname(gethostname())
                
                self.sendAcceptBcast(lpath, info, ctl_sock,
                        (ctl_host, ctl_port), ncopies, nolocal=nolocal)
                done = 0
                t0 = time.time()
                while not done and (tmo == None or time.time() < t0 + tmo):
                        done = self.waitForConnect(ctl_sock, 1)
                        if not done:
                                self.sendAcceptBcast(lpath, info, ctl_sock,
                                        (ctl_host, ctl_port), ncopies, nolocal=nolocal)
                if not done:
                        ctl_sock.close()
                        return 0, 'Request time out'
                mover_ctl, addr = ctl_sock.accept()
                ctl_sock.close()
                str = SockStream(mover_ctl)
                msg = str.recv()
                if msg == 'SEND':
                        try:    sts, err = self.remote_put(addr[0] == ctl_host, str, fn)
                        except:
                                sts, err = 0, 'Error: %s %s' % (sys.exc_info()[0], sys.exc_info()[1])
                else:
                        sts, err = 0, 'Transfer initiation failed'
                mover_ctl.close()
                return sts, err
                
        def remote_put(self, local, str, fn):
                data_sock = socket(AF_INET, SOCK_STREAM)
                if local:
                        data_host = '127.0.0.1'
                else:
                        data_host = gethostbyname(gethostname())
                data_sock.bind((data_host,0))
                data_port = data_sock.getsockname()[1]
                data_sock.listen(1)
                #print 'remote put from %s %s' % (data_host, data_port)
                str.send('RCVFROM %s %s' % (data_host, data_port))
                r,w,d = select.select([data_sock],[],[],20)
                if not r:
                        return 0, 'Transfer initiation failed'
                tx_sock, addr = data_sock.accept()
                data_sock.close()
                eof = 0
                fd = open(fn, 'r')
                buf = ''
                done = 0
                t0 = time.time()
                size = 0
                while not eof:
                        data = fd.read(60000)
                        if not data:
                                eof = 1
                        else:
                                tx_sock.send(data)
                                size = size + len(data)
                t1 = time.time()
                fd.close()
                str.send('EOF')
                tx_sock.close()
                done = str.recv() == 'EOF'
                size = size/1024.0/1024.0
                if done:
                        try:    rcvr = gethostbyaddr(addr[0])[0]
                        except: rcvr = addr[0]
                        rate = ''
                        if t1 >= t0 and size > 0:
                                rate = ' at %f MB/sec' % (size/(t1-t0))
                        return 1,'Stored %f MB on %s%s' % (size, rcvr, rate)
                else:
                        return 0,'Transfer aborted'

        def local_put(self, str, fn):
                if fn[0] != '/':
                        fn = os.getcwd() + '/' + fn
                self.connect()
                ans = str.sendAndRecv('COPY %s' % fn)
                self.disconnect()
                if not ans:
                        return 0, 'Transfer aborted'
                if ans == 'OK':
                        return 1, 'OK'
                else:
                        return 0, ans

def long2str(x):
        if type(x) != type(1) and type(x) != type(1):
                #print type(x), type(1L)
                return '(n/a)'
        str = '%s' % x
        if str[-1] == 'L':
                str = str[:-1]
        return str

class   PingPrinter:
        def __init__(self, cfg, f, fmt, downFmt):
                self.MinT = None
                self.MaxT = None
                self.NUp = 0
                self.NDown = 0
                self.TimeSum = 0
                self.F = f
                self.NW = 0
                self.NR = 0
                self.FreeSpace = 0
                self.N10M = 0
                self.N100M = 0
                self.N1G = 0
                self.Format = fmt
                self.DownFormat = downFmt
                self.NodeList = sorted(cfg.names('cell_class','*'))
                self.NextNode = 0
                self.NodeDict = {}

        def cmpNodes(self, x, y):
                ix = len(x)
                while ix > 0:
                        if x[ix-1] in '0123456789':
                                ix = ix -1
                        else:
                                break
                px = x[:ix]
                try:    nx = int(x[ix:])
                except: nx = 0
                iy = len(y)
                while iy > 0:
                        if y[iy-1] in '0123456789':
                                iy = iy -1
                        else:
                                break
                py = y[:iy]
                try:    ny = int(y[iy:])
                except: ny = 0
                return cmp(px, py) or cmp(nx, ny)
                
        def done(self):
                for n in self.NodeList:
                        # print n, self.NodeDict.has_key(n)
                        if n not in self.NodeDict:
                                return 0
                return 1
                                                
        def pong(self, addr, cid, t, nw, nr, sts, capdct):
                #print 'pong: %s' % cid, cid in self.NodeList
                self.NUp = self.NUp + 1
                if nw != None:          self.NW = self.NW + nw
                if nr != None:          self.NR = self.NR + nr
                for psan, caps in list(capdct.items()):
                        size, lfree, pfree = caps
                        f = min(lfree, pfree)
                        self.FreeSpace = self.FreeSpace + f
                        if f >= 1024:
                                self.N1G = self.N1G + 1
                        if f >= 100:
                                self.N100M = self.N100M + 1
                        if f >= 10:
                                self.N10M = self.N10M + 1
                if t != None:           
                        self.TimeSum = self.TimeSum + t
                        if self.MinT == None or self.MinT > t:
                                self.MinT = t
                        if self.MaxT == None or self.MaxT < t:
                                self.MaxT = t
                if cid == None:
                        cid = gethostbyaddr(addr)[0]
                self.NodeDict[cid] = (t, nw, nr, sts)
                self.printCells()
                
        def printCells(self):
                while self.NextNode < len(self.NodeList) and \
                                self.NodeList[self.NextNode] in self.NodeDict:
                        cid = self.NodeList[self.NextNode]
                        self.NextNode = self.NextNode + 1
                        
                        t, nw, nr, sts = self.NodeDict[cid]
                        # del self.NodeDict[cid]
                        if t != None:
                                self.F.write(self.Format % (cid, int(t*1000 + 0.5), nw, nr, sts))
                        else:
                                self.F.write(self.DownFormat % cid)
                        #print 'Next node: %s, in dict: %d' % (
                        #       self.NodeList[self.NextNode], 
                        #       self.NodeDict.has_key(self.NodeList[self.NextNode]))

        def close(self):
                for cid in self.NodeList[self.NextNode:]:
                        try:    t, nw, nr, sts = self.NodeDict[cid]
                        except:
                                self.NDown = self.NDown + 1
                                self.F.write(self.DownFormat % cid)
                        else:
                                self.F.write(self.Format % (cid, int(t*1000 + 0.5), nw, nr, sts))                       
                                
        def getStats(self):
                if self.NUp == 0:
                        return self.NUp, self.NDown, None, None, None, None, None
                return (self.NUp, self.NDown,
                        (self.MinT * 1000 + 0.5), 
                        (self.MaxT * 1000 + 0.5), 
                        (self.TimeSum/self.NUp * 1000 + 0.5),
                        self.NW, self.NR)

class   CapacityPrinter:
        def __init__(self, cfg):
                self.MinT = None
                self.MaxT = None
                self.Capacity = 0
                self.FreeSpace = 0
                self.NUp = 0
                self.N10M = 0
                self.N100M = 0
                self.N1G = 0
                self.NodeList = cfg.names('cell_class','*')
                if not self.NodeList:   self.NodeList = []
                self.NodeList.sort()
                self.NextNode = 0
                self.NodeDict = {}

        def done(self):
                for n in self.NodeList:
                        # print n, self.NodeDict.has_key(n)
                        if n not in self.NodeDict:
                                return 0
                return 1
                                                
        def pong(self, addr, cid, t, nw, nr, sts, capdct):
                #print 'pong: %s' % cid, cid in self.NodeList
                self.NUp = self.NUp + 1
                for psan, caps in list(capdct.items()):
                        cap, lfree, pfree = caps
                        f = min(lfree, pfree)
                        self.FreeSpace = self.FreeSpace + f
                        self.Capacity = self.Capacity + cap
                        if f >= 1024:
                                self.N1G = self.N1G + 1
                        if f >= 100:
                                self.N100M = self.N100M + 1
                        if f >= 10:
                                self.N10M = self.N10M + 1
                if cid == None:
                        cid = gethostbyaddr(addr)[0]
                self.NodeDict[cid] = 1
                self.printCells()
                
        def printCells(self):
                return
                #while self.NextNode < len(self.NodeList) and \
                #               self.NodeDict.has_key(self.NodeList[self.NextNode]):
                #       cid = self.NodeList[self.NextNode]
                #       self.NextNode = self.NextNode + 1
                #       
                #       t, nw, nr, sts = self.NodeDict[cid]
                #       # del self.NodeDict[cid]
                #       if t != None:
                #               self.F.write(self.Format % (cid, int(t*1000 + 0.5), nw, nr, sts))
                #       else:
                #               self.F.write(self.DownFormat % cid)
                #       #print 'Next node: %s, in dict: %d' % (
                #       #       self.NodeList[self.NextNode], 
                #       #       self.NodeDict.has_key(self.NodeList[self.NextNode]))

        def close(self):
                pass
                #for cid in self.NodeList[self.NextNode:]:
                #       try:    t, nw, nr, sts = self.NodeDict[cid]
                #       except:
                #               self.NDown = self.NDown + 1
                #               self.F.write(self.DownFormat % cid)
                #       else:
                #               self.F.write(self.Format % (cid, int(t*1000 + 0.5), nw, nr, sts))                       
                                
        def getStats(self):
                pass
                #if self.NUp == 0:
                #       return self.NUp, self.NDown, None, None, None, None, None
                #return (self.NUp, self.NDown,
                #       (self.MinT * 1000 + 0.5), 
                #       (self.MaxT * 1000 + 0.5), 
                #       (self.TimeSum/self.NUp * 1000 + 0.5),
                #       self.NW, self.NR)


def recursiveRemoveDir(c, path):
        sts, lst = c.listFiles(path)
        if sts != 'OK':
                return 0, sts
        subdirs = []
        for lp, t, info in lst:
                fpath = path + '/' + lp
                if t == 'd':    subdirs.append(fpath)
                else:
                        #print 'deleting %s' % fpath
                        sts, err = c.delFile(fpath)
                        if not sts:
                                return sts, 'Error deleting %s: %s' % (fpath, err)
        #print 'subdirs: ', subdirs
        for subdir in subdirs:
                sts, err = recursiveRemoveDir(c, subdir)
                if not sts:
                        return sts, err
        sts, err = c.delDir(path)
        return sts, err

                
if __name__ == '__main__':
        from config import *
        import sys
        import getopt
        
        Usage = """
Usage:    dfarm <command> <args>
Commands: ls [-1] [-A|-a <attr>[,<attr>...]] [-s <attr>[,<attr>...]] 
                   [<vfs dir>|<wildcard>]
          info [-0] <vfs file>
          get [-t <timeout>] [-v] <vfs file> <local file>
          put [-t <timeout>] [-v] [-r] [-n <ncopies>] <local file> <vfs file>
          rm(=del) [-r] (<vfs path>|<wildcard>) ...
          mkdir <vfs path>
          rmdir [-r] <vfs path> ...
          chmod (r|-)(w|-)(r|-)(w|-) <vfs path>
          setattr <vfs path> <attr> <value>
          getattr <vfs path> <attr>
          ln <local vfs file> <local file>
          ping
          stat <node>
          usage <user>
          hold/release <node> ...
          repnode [-n <ncopies>] <node> ...
          repfile [-n <ncopies>] <vfs file>
          capacity [-mMGfcu]
"""
        
        cfg = ConfigFile(os.environ['DFARM_CONFIG'])
        c = DiskFarmClient(cfg)

        if len(sys.argv) < 2:
                print(Usage)
                sys.exit(2)
                                        
        cmd = sys.argv[1]
        args = sys.argv[2:]
        
        if cmd == 'ls' or cmd == 'list':
                path_only = 0
                all_attrs = 0
                print_attr = []
                select_attr = []
                opts, args = getopt.getopt(args, '1Aa:s:')
                for opt, val in opts:
                        if opt == '-1':         path_only = 1
                        elif opt == '-A':       all_attrs = 1
                        elif opt == '-a':
                                print_attr = val.split(',')
                        elif opt == '-s':
                                select_attr = val.split(',')

                if not args:
                        dir = '/'
                        prefix = '/'
                else:
                        dir = args[0]
                        prefix = ''

                sts, lst = c.listFiles(dir)
                if sts != 'OK':
                        print(sts)
                        sys.exit(1)

                lst.sort(lambda x, y: cmp(x[0],y[0]))
                for lp, t, info in lst:
                        if select_attr:
                                skip = 0
                                for a in select_attr:
                                        if not a in info.attributes():
                                                skip = 1
                                                break
                                if skip:        continue
                        lp = prefix + lp
                        if t == 'd':
                                lp = lp + '/'
                        if path_only:   print(lp, end=' ')
                        else:
                                if t == 'd':
                                        print('%1s%4s %3s %-16s %12s %14s %s' % (
                                                t, info.Prot, '-', info.Username, '-', '', lp), end=' ')
                                else:
                                        size = info.Size
                                        if info.sizeEstimated():
                                                size = None
                                        timstr = time.strftime('%m/%d %H:%M:%S', 
                                                        time.localtime(info.CTime))
                                        print('%1s%4s %3d %-16s %12s %14s %s' % (
                                                t, info.Prot, info.mult(), info.Username,
                                                long2str(size), timstr, lp), end=' ')
                        if all_attrs:
                                attrs = info.attributes()
                                attrs.sort()
                                for a in attrs:
                                        v = info[a]
                                        print('%s:%s' % (a,v), end=' ')
                        elif print_attr:
                                for a in print_attr:
                                        v = info[a]
                                        if v != None:
                                                print('%s:%s' % (a,v), end=' ')
                        print('')
                                
                sys.exit(0)

        elif cmd == 'hold':
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                sts, reason = c.holdNodes(args)
                if not sts:
                        print(reason)
                        sys.exit(1)
                sys.exit(0)

        elif cmd == 'release':
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                sts, reason = c.releaseNodes(args)
                if not sts:
                        print(reason)
                        sys.exit(1)
                sys.exit(0)

        elif cmd == 'chmod':
                if len(args) < 2:
                        print(Usage)
                        sys.exit(2)
                sts, reason = c.chmod(args[1], args[0])
                if not sts:
                        print(reason)
                        sys.exit(1)
                sys.exit(0)             

        elif cmd == 'setattr':
                if len(args) < 2:
                        print(Usage)
                        sys.exit(2)
                sts, reason = c.setAttr(args[0], args[1], args[2])
                if not sts:
                        print(reason)
                        sys.exit(1)
                sys.exit(0)             

        elif cmd == 'usage':
                # get usage statistics for the user
                if not args:
                        print(Usage)
                        sys.exit(2)
                usg, res, qta = c.getUsage(args[0])
                if usg == None:
                        # error
                        print(qta)
                        sys.exit(1)
                print('Used: %s + Reserved: %s / Quota: %s (MB)' % \
                        (long2str(usg), long2str(res), long2str(qta)))

        elif cmd == 'get':
                # get [-t <tmo>] lpath fn
                try:    opts, args = getopt.getopt(args, 't:v')
                except getopt.error as msg:
                        print(msg)
                        print(Usage)
                        sys.exit(2)
                if len(args) < 2:
                        print(Usage)
                        sys.exit(2)
                tmo = 5*60
                verbose = 0
                for opt, val in opts:
                        if opt == '-t': tmo = int(val)
                        elif opt == '-v': verbose = 1
                        
                lpath = args[0]
                dst = args[1]
                info, err = c.getInfo(lpath)
                if not info:
                        print(err)
                        sys.exit(1)
                if info.Type != 'f':
                        print('Is not a file')
                        sys.exit(2)
                try:
                        st = os.stat(dst)
                        if stat.S_ISDIR(st[stat.ST_MODE]):
                                fn = lpath.split('/')[-1]
                                dst = dst + '/' + fn
                except:
                        pass
                sts, err = c.get(lpath, dst, info, tmo = tmo)
                if not sts or verbose:
                        print(err)
                sys.exit(sts == 0)
        
        elif cmd == 'put':
                if len(args) < 2:
                        print(Usage)
                        sys.exit(2)
                ncopies = 1
                verbose = 0
                logpath = '/'
                nolocal = 0
                tmo = 5*60
                
                try:    opts, args = getopt.getopt(args, 't:n:rv')
                except getopt.error as msg:
                        print(msg)
                        print(Usage)
                        sys.exit(2)
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                
                srclst = args
                dst = '/'
                if len(args) > 1:
                        srclst = args[:-1]
                        dst = args[-1]
                        
                if dst[0] != '/':
                        dat = '/' + dst
                dstisdir = c.isDir(dst)
                if len(srclst) > 1 and not dstisdir:
                        print('Destination must be a directory')
                        sys.exit(1)

                for opt, val in opts:
                        if opt == '-n':
                                ncopies = int(val)
                        elif opt == '-v':
                                verbose = 1
                        elif opt == '-r':
                                nolocal = 1
                        elif opt == '-t':
                                tmo = int(val)
                status = 1
                for src in srclst:
                        try:    st = os.stat(src)
                        except os.error as val:
                                print('Error opening %s: %s' % (src, val.strerror))
                                status = 0
                                continue
                        if stat.S_ISDIR(st[stat.ST_MODE]):
                                print('Can not copy directory %s' % src)
                                continue

                        lpath = dst
                        if dstisdir:
                                fn = src.split('/')[-1]
                                if not fn:
                                        print('Invalid input file specification %s' % src)
                                        continue
                                lpath = '%s/%s' % (dst, fn)
                                
                        # put fn lpath
                        info = c.fileInfo(lpath, src)
                        lpath = info.Path
                        info, err = c.createFile(info, ncopies)
                        if not info:
                                print('Error creating %s: %s' % (lpath, err))
                                status = 0
                                continue
                        t0 = time.time()
                        sts, err = c.put(src, lpath, info, ncopies, nolocal, tmo = tmo)
                        status = sts
                        if not sts or verbose:
                                if len(srclst) > 1:
                                        print('%s: %s' % (src, err))
                                else:
                                        print(err)
                sys.exit(status == 0)

        elif cmd == 'info':
                # info lpath
                try:    opts, args = getopt.getopt(args, '0')
                except getopt.error as msg:
                        print(msg)
                        print(Usage)
                        sys.exit(2)
                print_info = 1
                for opt, val in opts:
                        if opt == '-0':
                                print_info = 0
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                info, err = c.getInfo(args[0])
                if not info:
                        if print_info:
                                print(err)
                        sys.exit(1)
                if print_info:
                        print('Path: %s' % args[0])
                        print('Type: %s' % info.Type)
                        print('Owner: %s' % info.Username)
                        print('Protection: %s' % info.Prot)
                        print('Attributes:')
                        for k in info.attributes():
                                v = info[k]
                                print('  %s = %s' % (k, info[k]))
                        if info.Type == 'f':
                                print('Created: %s' % time.ctime(info.CTime))
                                print('Size: %s' % long2str(info.Size))
                                print('Stored on: %s' % ','.join(info.Servers))
                else:
                        print(info.Type)
                sys.exit(0)

        elif cmd == 'getattr':
                # getattr lpath attr
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                info, err = c.getInfo(args[0])
                if not info:
                        print(err)
                        sys.exit(1)
                val = info[args[1]]
                if val == None:
                        sys.exit(1)
                print(val)
                sys.exit(0)

        elif cmd == 'del' or cmd == 'rm':
                # [-r] lpath
                recursive = 0
                opts, args = getopt.getopt(args, 'r')
                for opt, val in opts:
                        if opt == '-r': recursive = 1

                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                
                for arg in args:
                        if c.isDir(arg):
                                if not recursive:       
                                        sts, err = 0, '%s is a directory' % arg
                                else:
                                        sts, err = recursiveRemoveDir(c, arg)
                        else:
                                sts, err = c.delFile(args[0])
                                if not sts and err[:2] == 'NF':
                                        # try wildcard
                                        sts, lst = c.listFiles(args[0])
                                        if sts != 'OK':
                                                err = sts
                                        else:
                                                if not lst:
                                                        sts, err = 0, 'Not found'
                                                else:
                                                        sts, err = 1, ''
                                                        for lp, t, info in lst:
                                                                if t == 'd':
                                                                        if recursive:
                                                                                sts, err = recursiveRemoveDir(c, info.Path)
                                                                                if not sts:
                                                                                        break
                                                                        else:
                                                                                print('%s is not a file' % info.Path)
                                                                else:
                                                                        sts, err = c.delFile(info.Path)
                                                                        if not sts:
                                                                                print('Error deleting %s: %s' % (info.Path, err))
                                                        if sts: err = ''
                        if not sts:
                                print(err)
                                sys.exit(1)
                sys.exit(0)

        elif cmd == 'mkdir':
                # info lpath
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                lpath = args[0]
                info = c.dirInfo(lpath)
                sts, err = c.makeDir(lpath, info)
                if not sts:
                        print(err)
                        sys.exit(1)
                sys.exit(0)

        elif cmd == 'rmdir':
                # [-r] lpath
                recursive = 0
                opts, args = getopt.getopt(args, 'r')
                for opt, val in opts:
                        if opt == '-r': recursive = 1

                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)

                for arg in args:
                        if recursive:   
                                sts, err = recursiveRemoveDir(c, arg)
                        else:
                                sts, err = c.delDir(arg)
                        if not sts:
                                print(err)
                                sys.exit(1)
                sys.exit(0)

        elif cmd in ['repnodes','repnode']:
                opts, args = getopt.getopt(args, 'n:')
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                mult = 1
                for opt, val in opts:
                        if opt == '-n':
                                mult = int(val)
                sts, err = c.replicateNodes(mult, args)
                if not sts:
                        print(err)
                        sys.exit(1)
                sys.exit(0)
                
        elif cmd == 'repfile':
                opts, args = getopt.getopt(args, 'n:')
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                mult = 1
                for opt, val in opts:
                        if opt == '-n':
                                mult = int(val)
                if len(args) != 1:
                        print(Usage)
                        sys.exit(2)
                sts, err = c.replicateFile(args[0], mult)
                if not sts:
                        print(err)
                        sys.exit(1)
                sys.exit(0)

        elif cmd == 'ping':
                pw = PingPrinter(cfg, sys.stdout, '%20s %5dms %4dw %4dr %s\n', '%20s -- not responding --\n')
                lst = c.pingParallel(pw.pong, pw.done)
                pw.close()
                nup, ndown, mint, maxt, avgt, tput, tget = \
                        pw.getStats()
                if nup or ndown:
                        print('--- %d/%d nodes up ---------------------------' % \
                                (nup, nup+ndown))
                        if nup:
                                print('%20s %5s   %4dw %4dr' % ('total','',tput, tget))
                                print('%20s %5dms' % ('min',mint))
                                print('%20s %5dms' % ('avegare',avgt))
                                print('%20s %5dms' % ('max',maxt))
                sys.exit(0)

        elif cmd == 'old_ping':
                pw = PingPrinter(cfg, sys.stdout, '%20s %5dms %4dw %4dr %s\n', '%20s -- not responding --\n')
                lst = c.ping(pw.pong, pw.done)
                pw.close()
                nup, ndown, mint, maxt, avgt, tput, tget = \
                        pw.getStats()
                if nup or ndown:
                        print('--- %d/%d nodes up ---------------------------' % \
                                (nup, nup+ndown))
                        if nup:
                                print('%20s %5s   %4dw %4dr' % ('total','',tput, tget))
                                print('%20s %5dms' % ('min',mint))
                                print('%20s %5dms' % ('avegare',avgt))
                                print('%20s %5dms' % ('max',maxt))
                sys.exit(0)

        elif cmd == 'capacity':
                pw = CapacityPrinter(cfg)
                lst = c.pingParallel(pw.pong, pw.done)
                pw.close()
                opts, args = getopt.getopt(args, 'mMGfcu')
                show_all = 1
                show_u = 0
                show_10 = 0
                show_100 = 0
                show_g = 0
                show_f = 0
                show_c = 0
                for opt, val in opts:
                        if opt == '-m':         show_all, show_10       =               (0, 1)
                        if opt == '-M':         show_all, show_100      =               (0, 1)
                        if opt == '-G':         show_all, show_g        =               (0, 1)
                        if opt == '-f':         show_all, show_f        =               (0, 1)
                        if opt == '-c':         show_all, show_c        =               (0, 1)
                        if opt == '-u':         show_all, show_u        =               (0, 1)
                if show_all or show_u:          print('Nodes up:            ', pw.NUp)
                if show_all or show_10:         print('Nfree > 10M:         ', pw.N10M)
                if show_all or show_100:        print('Nfree > 100M:        ', pw.N100M)
                if show_all or show_g:          print('Nfree > 1G:          ', pw.N1G)
                if show_all or show_f:          print('Total free (MB):     ', pw.FreeSpace)
                if show_all or show_c:          print('Total capacity (MB): ', pw.Capacity)
                sys.exit(0)

        elif cmd == 'stat':
                if len(args) < 1:
                        print(Usage)
                        sys.exit(2)
                ci = c.cellInfo(args[0])
                if ci == None:
                        print('time-out')
                        sys.exit(1)
                print('%16s %10s %10s %10s %10s' % ('Area','Size','Used','Reserved','Free'))
                print('%16s %10s %10s %10s %10s' % (16*'-',10*'-',10*'-',10*'-',10*'-',))
                for psn, size, used, rsrvd, free in ci.PSAs:
                        print('%16s %10d %10d %10d %10d' % (psn, size, used, rsrvd, free))
                print('%8s %6s %8s' % ('Txn type', 'Status','VFS Path'))
                print('%8s %6s %8s' % (8*'-', 6*'-', 8*'-'))
                for tt, sts, lpath in ci.Txns:
                        print('%8s %6s %s' % (tt, sts, lpath))
                sys.exit(0)
        elif cmd == 'ln':
                if len(args) < 2:
                        print(Usage)
                        sys.exit(2)
                info, err = c.getInfo(args[0])
                if not info:
                        print(err)
                        sys.exit(1)
                lpath = args[0]
                dpath = c.localDataPath(lpath, info)
                if not dpath:
                        print('Time-out or non-local file')
                        sys.exit(1)
                dst = args[1]
                try:
                        st = os.stat(dst)
                        if stat.S_ISDIR(st[stat.ST_MODE]):
                                fn = lpath.split('/')[-1]
                                dst = dst + '/' + fn
                except:
                        pass
                try:
                        os.symlink(dpath, dst)
                except os.error as val:
                        print(val)
                        sys.exit(1)
                sys.exit(0)
        else:
                print(Usage)
                sys.exit(2)
