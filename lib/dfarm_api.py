
from SockStream import SockStream
from VFSFileInfo import VFSFileInfo, VFSDirInfo
from DataClient import DataClient
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
                                reply = to_str(reply)
                                words = reply.split()
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
                                words = reply.split(':', 1)
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

        def recursiveRemoveDir(self, path):
                sts, lst = self.listFiles(path)
                if sts != 'OK':
                        return 0, sts
                subdirs = []
                for lp, t, info in lst:
                        fpath = path + '/' + lp
                        if t == 'd':    subdirs.append(fpath)
                        else:
                                #print 'deleting %s' % fpath
                                sts, err = self.delFile(fpath)
                                if not sts:
                                        return sts, 'Error deleting %s: %s' % (fpath, err)
                #print 'subdirs: ', subdirs
                for subdir in subdirs:
                        sts, err = self.recursiveRemoveDir(subdir)
                        if not sts:
                                return sts, err
                sts, err = self.delDir(path)
                return sts, err

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
                return to_str(ans)
                
        def cellInfo(self, node):
                sock = socket(AF_INET, SOCK_DGRAM)
                sock.sendto(to_bytes('STATPSA %s' % self.FarmName), (node, self.CAddr[1]))
                r,w,e = select.select([sock],[],[],30)
                if not r:
                        sock.close()
                        return None
                ans, addr = sock.recvfrom(100000)
                ans = to_str(ans)
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
                        msg = to_str(msg)
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
                                msg = to_str(msg)
                                msgs.append((msg, addr, time.time()))
                        while nwait >= maxping or \
                                        (nwait > 0 and itosend >= len(self.NodeList)):
                                # wait for answers if necessary
                                r,w,e = select.select([sock],[],[],1)
                                if r:
                                        msg, addr = sock.recvfrom(100000)
                                        msg = to_str(msg)
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

        def get(self, info, fn, nolocal = True, tmo = None):
            data_client = DataClient(self.CAddr, self.FarmName)
            return data_client.get(info, fn, nolocal, tmo)
            
        def put(self, info, fn, ncopies = 1, nolocal = True, tmo = None):
            data_client = DataClient(self.CAddr, self.FarmName)
            return data_client.put(info, fn, ncopies, nolocal, tmo)

        def open(self, info, mode, ncopies = 1, nolocal = True, tmo = None):
            assert mode in ("r", "w")
            data_client = DataClient(self.CAddr, self.FarmName)
            if mode == "r":
                return data_client.openRead(info, nolocal = nolocal, tmo = tmo)
            elif mode == "w":
                return data_client.openWrite(info, ncopies = ncopies, nolocal = nolocal, tmo = tmo)
                
def long2str(x):
        if type(x) != type(1) and type(x) != type(1):
                #print type(x), type(1L)
                return '(n/a)'
        str = '%s' % x
        if str[-1] == 'L':
                str = str[:-1]
        return str

                
