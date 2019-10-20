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
import os, sys
import time
import stat
import pwd
from socket import *
from config import ConfigFile

from py3 import to_str, to_bytes

class DiskFarmError(Exception):
    def __init__(self, value):
        self.Value = value

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
                        self.Sock.sendto(to_bytes(msg), self.DFC.CAddr)
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
                        try:    self.Sock.sendto(to_bytes(msg), self.DAddr)
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
                if isinstance(cfg, str):
                    from dfconfig import DFConfig
                    cfg = DFConfig(cfg, 'DFARM_CONFIG')
                else:
                    assert isinstance(cfg, dict)
                self.Cfg = cfg
                self.CSock = socket(AF_INET, SOCK_DGRAM)
                self.CSock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
                self.CAddr = (cfg["cell"]['broadcast'], cfg["cell"]['listen_port'])         
                self.DAddr = (cfg['VFSServer']['host'],cfg['VFSServer']['api_port'])
                self.FarmName = cfg['cell']['farm_name']
                self.NodeList = list(cfg['cell_class'].keys())
                if not self.NodeList:   self.NodeList = []
                self.NodeAddrMap = {}
                domain = cfg['cell']['domain']
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
                ans = self.DStr.sendAndRecv('HOLD %s' % " ".join(nlst))
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
                ans = self.DStr.sendAndRecv('RELEASE %s' % " ".join(nlst))
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
                ans = self.DStr.sendAndRecv('REPNODE %d %s' % (mult,  " ".join(nlst)))
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
                words = ans.split()
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
                        msg = to_bytes('DPATH %s %s %s' % (self.FarmName, lpath, 
                                                info.CTime)
                        )
                        try:    sock.sendto(msg, ('127.0.0.1', self.CAddr[1]))
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
                sock.sendto(to_bytes('STATPSA %s' % self.FarmName, (node, self.CAddr[1])))
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
                sock.sendto(to_bytes('PING %s' % self.FarmName, self.CAddr))
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
                                msgs.append((msg, addr, time.time()))
                        while nwait >= maxping or \
                                        (nwait > 0 and itosend >= len(self.NodeList)):
                                # wait for answers if necessary
                                r,w,e = select.select([sock],[],[],3)
                                if r:
                                        msg, addr = sock.recvfrom(100000)
                                        msgs.append((msg, addr, time.time()))
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
                                try:    sock.sendto(pingmsg, addr)
                                except: raise
                                sent_times[n] = time.time()
                        for msg, addr, t in msgs:
                                msg = to_str(msg)
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
                try:    self.CSock.sendto(to_bytes(msg), self.CAddr)
                except: pass            
                
        def sendSendBcast(self, lpath, info, ctl_sock, ctladdr, nolocal=0, tmo=None):
                if nolocal:
                        bcast = 'SENDR'
                else:
                        bcast = 'SEND'
                bcast = bcast + ' %s %s %s %s %s' % (self.FarmName, lpath, info.CTime,
                        ctladdr[0], ctladdr[1])         
                self.CSock.sendto(to_bytes(bcast), self.CAddr)

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
                        path = msg[5:].strip()
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
                self.CSock.sendto(to_bytes(bcast), self.CAddr)
                
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
                        try:    
                                sts, err = self.remote_put(addr[0] == ctl_host, str, fn)
                        except:
                                sts, err = 0, 'remote_put error: %s %s' % (sys.exc_info()[0], sys.exc_info()[1])
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
                fd = open(fn, 'rb')
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

                
