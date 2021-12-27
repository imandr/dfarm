from fcslib import TCPServerThread, TCPClientConnectionTask
from VFSFileInfo import VFSFileInfo, VFSDirInfo, VFSCanonicPath
import sys
import vfssrv_global
import time
from logs import Logged

from py3 import to_str, to_bytes

def _long2str(x):
        str = '%s' % x
        if str[-1] == 'L':
                str = str[:-1]
        return str

class   VFSClientConnection(TCPClientConnectionTask, Logged):
        MAX_DIR_TO_SEND = 100
        def __init__(self, usrv, sock, addr):
                self.USrv = usrv
                self.ClientAddr = addr
                self.ClientSocket = sock
                self.ClientAddress = addr
                TCPClientConnectionTask.__init__(self, sock, addr)
                self.Username = None
                self.DirList = None

        def __str__(self):
            return "VFSClient[%s@%s]" % (self.Username, self.ClientAddr)


        def doWrite(self, fd, sel):
                if fd != self.ClientSocket.fileno():    return
                if self.DirList:
                        n = min(self.MAX_DIR_TO_SEND, len(self.DirList))
                        retlst = []
                        for fn, typ, info in self.DirList[:n]:
                                if self.FullDirPath and fn[0] != '/':
                                        fn = '/' + fn
                                istr = ''
                                if info:
                                        istr = info.serialize()
                                str = '%s %s %s' % (fn, typ, istr)
                                retlst.append(str)
                        self.Str.send(retlst)
                        self.DirList = self.DirList[n:]
                if not self.DirList:
                        self.DirList = None
                        self.Str.send('.')
                        sel.unregister(wr=self.ClientSocket.fileno())

        def doDelFile(self, cmd, args, msg):
                if not self.Username:
                        return 'ERR Say HELLO first'
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info == None:
                        return 'NF Not found'
                sts, reason = self._delfile(info)
                if sts:
                        self.log('Deleted file %s %s' % (lpath, info.CTime))
                        return 'OK'
                else:
                        return reason


        def _delfile(self, info):
                if not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[1] != 'w':
                                        return 0, 'PERM Permission denied'
                        else:
                                if info.Prot[3] != 'w':
                                        return 0, 'PERM Permission denied'
                lpath = info.Path
                sts, reason = vfssrv_global.G_VFSDB.rmfile(lpath)
                if sts:
                        vfssrv_global.G_QuotaMgr.delFile(
                                '%s %s' % (lpath, info.CTime),
                                info.Username, info.sizeMB(),
                                info.mult())
                        #self.log('Deleted file %s %s' % (lpath, info.CTime))
                        return 1, 'OK'
                else:
                        return 0, 'ERR %s' % reason
                

        def doDelDir(self, cmd, args, msg):
                if not self.Username:
                        return 'ERR Say HELLO first'
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                info = vfssrv_global.G_VFSDB.getDirInfo(lpath)
                if not info:
                        return 'NF Directory not found'

                if not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[1] != 'w':
                                        return 'PERM Permission denied'
                        else:
                                if info.Prot[3] != 'w':
                                        return 'PERM Permission denied'
                
                sts, reason = vfssrv_global.G_VFSDB.rmdir(lpath)
                if sts:
                        self.log('Deleted dir %s' % (lpath,))
                        return 'OK'
                else:
                        return 'ERR %s' % reason
                
        def doList(self, cmd, args, msg):
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                arg = VFSCanonicPath(args[0])
                if vfssrv_global.G_VFSDB.isDir(arg):
                        self.doListDirContents(arg)
                elif vfssrv_global.G_VFSDB.isFile(arg):
                        self.doListFile(arg)
                else:
                        self.doListPattern(arg)

        def doListFile(self, lpath):
                info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info and not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[0] != 'r':
                                        return 'PERM Permission denied'
                        else:
                                if info.Prot[2] != 'r':
                                        return 'PERM Permission denied'
                info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info and not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[0] != 'r':
                                        return 'PERM Permission denied'
                        else:
                                if info.Prot[2] != 'r':
                                        return 'PERM Permission denied'
                self.Str.send('OK')
                self.Str.send('%s f %s' % (lpath, info.serialize()))
                self.Str.send('.')
                return None

        def sendList(self, lst):
            for lp, typ, info in lst:
                self.Str.send("%s %s %s" % (lp, typ, info.serialize()))
            self.Str.send(".")

        def doListDirContents(self, lpath):
                info = vfssrv_global.G_VFSDB.getDirInfo(lpath)
                if info and not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[0] != 'r':
                                        return 'PERM Permission denied'
                        else:
                                if info.Prot[2] != 'r':
                                        return 'PERM Permission denied'
                nlist = vfssrv_global.G_VFSDB.glob(lpath, '*')
                self.Str.send('OK')
                self.sendList(nlist)
                return None

        def doListPattern(self, ptrn):
                nlist = vfssrv_global.G_VFSDB.glob(ptrn)
                self.Str.send('OK')
                self.sendList(nlist)
                return None

        def doGetInfo(self, cmd, args, msg):
                if not self.Username:
                        return 'ERR Say HELLO first'
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                typ = vfssrv_global.G_VFSDB.getType(lpath)
                info = None
                if typ == 'd':
                        info = vfssrv_global.G_VFSDB.getDirInfo(lpath)
                elif typ == 'f':
                        info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info == None:
                        return 'NF File not found'
                if not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[0] != 'r':
                                        return 'PERM Permission denied'
                        else:
                                if info.Prot[2] != 'r':
                                        return 'PERM Permission denied'
                
                self.log('Got info <%s>' % lpath)
                return 'OK %s %s' % (typ, info.serialize())

        def doGetType(self, cmd, args, msg):
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                typ = vfssrv_global.G_VFSDB.getType(VFSCanonicPath(args[0]))
                if typ:
                        return 'OK %s' % typ
                else:
                        return 'NF not found'

        def doMkFile(self, cmd, args, msg):
                # MF <lpath> <mult> <info>
                if not self.Username:
                        return 'ERR Say HELLO first'
                if len(args) < 3:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                mult = int(args[1])
                dirinfo = vfssrv_global.G_VFSDB.getDirInfo(
                        vfssrv_global.G_VFSDB.parentPath(lpath))

                if dirinfo == None:
                        return 'ERR Parent directory does not exist'

                if not self.USrv.isAdmin(self.Username):
                        if dirinfo.Username == self.Username:
                                if dirinfo.Prot[1] != 'w':
                                        return 'PERM Permission denied'
                        else:
                                if dirinfo.Prot[3] != 'w':
                                        return 'PERM Permission denied'

                info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info != None:
                        sts, reason = self._delfile(info)
                        if not sts:
                                return 'ERR Can not remove existing file: %s' % reason

                info = VFSFileInfo(lpath, msg.split(None,3)[3])
                info.Username = self.Username   # do not trust them
                info.CTime = int(time.time())
                if vfssrv_global.G_QuotaMgr.wouldExceedQuota(
                        info.Username, 
                        info.sizeMB(),
                        mult):
                        return 'ERR Quota exceeded'
                vfssrv_global.G_QuotaMgr.makeReservation(
                        info.Username,
                        '%s %s' % (lpath, info.CTime),
                        info.sizeMB(), mult)
                for k, v in list(dirinfo.Attrs.items()):
                        if k[0] == '_':
                                info.Attrs[k] = v
                sts, reason = vfssrv_global.G_VFSDB.mkfile(info)
                if sts:
                        self.log('Created file %s %s' % (lpath, info.CTime))
                        return 'OK %s' % info.serialize()
                else:
                        return 'ERR %s' % reason

        def doRereplicate(self, cmd, args, msg):
                # RR <lpath> <extra mult> 
                if not self.Username:
                        return 'ERR Say HELLO first'
                if len(args) < 2:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                mult = int(args[1])
                info = vfssrv_global.G_VFSDB.getFileInfo(lpath)

                if info == None:
                        return 'ERR File does not exist'

                if not self.USrv.isAdmin(self.Username):
                        if info.Username == self.Username:
                                if info.Prot[1] != 'w':
                                        return 'PERM Permission denied'
                        else:
                                if info.Prot[3] != 'w':
                                        return 'PERM Permission denied'
                
                if vfssrv_global.G_QuotaMgr.wouldExceedQuota(
                        info.Username, 
                        info.sizeMB(),
                        mult):
                        return 'ERR Quota exceeded'
                vfssrv_global.G_QuotaMgr.makeReservation(
                        info.Username,
                        '%s %s' % (lpath, info.CTime),
                        info.sizeMB(), mult)
                sts, reason = vfssrv_global.G_CellIF.rereplicate(lpath, info.Servers, mult)
                if sts:
                        self.log('Replicated +%s %s' % (mult, lpath))
                        return 'OK'
                else:
                        return 'ERR %s' % reason

        def doMkDir(self, cmd, args, msg):
                # MD <lpath> <info>
                if not self.Username:
                        return 'ERR Say HELLO first'
                if len(args) < 2:
                        return 'ERR Protocol syntax error: <%s>' % msg
                args = msg.split(None, 2)[1:]
                lpath = VFSCanonicPath(args[0])
                dirinfo = vfssrv_global.G_VFSDB.getDirInfo(
                        vfssrv_global.G_VFSDB.parentPath(lpath))

                if not dirinfo: return 'ERR Parent directory not found'

                if not self.USrv.isAdmin(self.Username):
                        if dirinfo.Username == self.Username:
                                if dirinfo.Prot[1] != 'w':
                                        return 'PERM Permission denied'
                        else:
                                if dirinfo.Prot[3] != 'w':
                                        return 'PERM Permission denied'
                
                info = VFSDirInfo(lpath, args[1])
                info.Username = self.Username   # do not trust them
                for k, v in list(dirinfo.Attrs.items()):
                        if k[0] == '_':
                                info.Attrs[k] = v
                sts, reason = vfssrv_global.G_VFSDB.mkdir(info)
                if sts:
                        self.log('Created directory %s' % (lpath,))
                        return 'OK'
                else:
                        return 'ERR %s' % reason

        def doGetUsage(self, cmd, args, msg):
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                u, r, q = vfssrv_global.G_QuotaMgr.getUsage(args[0])
                self.log('Got usage %s' % args[0])
                return 'OK %s %s %s' % (u, r, q)        

        def doChMod(self, cmd, args, msg):
                # CHMOD <lpath> <prot>
                if len(args) < 2:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                prot = args[1]
                if len(prot) != 4:
                        return 'ERR Invalid protection mask'
                if not prot[0] in 'r-' or not prot[2] in 'r-' or \
                                not prot[1] in 'w-' or not prot[3] in 'w-':
                        return 'ERR Invalid protection mask'
                typ = vfssrv_global.G_VFSDB.getType(lpath)
                info = None
                if typ == 'd':
                        info = vfssrv_global.G_VFSDB.getDirInfo(lpath)
                elif typ == 'f':
                        info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info == None:
                        return 'NF not found'
                if not self.USrv.isAdmin(self.Username) and \
                                                self.Username != info.Username and \
                                info.Prot[3] != 'w':
                        return 'PERM Permission denied'
                info.Prot = prot
                if typ == 'd':
                        vfssrv_global.G_VFSDB.storeDirInfo(info)
                else:
                        vfssrv_global.G_VFSDB.storeFileInfo(info)
                self.log('chmod %s %s' % (prot, info.Path))
                return 'OK'

        def doSetAttr(self, cmd, args, msg):
                # SATTR <lpath> <attr> <value>
                if len(args) < 3:
                        return 'ERR Protocol syntax error: <%s>' % msg
                lpath = VFSCanonicPath(args[0])
                attr = args[1]
                value = ''
                if len(args) >= 3:
                        value = args[2]
                typ = vfssrv_global.G_VFSDB.getType(lpath)
                info = None
                if typ == 'd':
                        info = vfssrv_global.G_VFSDB.getDirInfo(lpath)
                elif typ == 'f':
                        info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                if info == None:
                        return 'NF not found'
                if not self.USrv.isAdmin(self.Username) and \
                                                self.Username != info.Username and \
                                info.Prot[3] != 'w':
                        return 'PERM Permission denied'
                info[attr] = value
                if typ == 'd':
                        vfssrv_global.G_VFSDB.storeDirInfo(info)
                else:
                        vfssrv_global.G_VFSDB.storeFileInfo(info)
                self.log('setattr %s: %s=%s' % (lpath, attr, value))
                return 'OK'

        def doHold(self, cmd, args, msg):
                # HOLD <node> [...]
                if not self.USrv.isAdmin(self.Username):
                        return 'PERM Permission denied'
                errlist = ''
                for nname in args:
                        sts, reason = vfssrv_global.G_CellIF.holdNode(nname)
                        if not sts:
                                errlist = errlist + ('%s: %s, ' % (nname, reason))
                if errlist:
                        return 'ERR %s' % errlist
                else:
                        self.log('Held nodes %s' % (",".join(args),))
                        return 'OK'

        def doRelease(self, cmd, args, msg):
                # RELEASE <node> [...]
                if not self.USrv.isAdmin(self.Username):
                        return 'PERM Permission denied'
                errlist = ''
                for nname in args:
                        sts, reason = vfssrv_global.G_CellIF.releaseNode(nname)
                        if not sts:
                                errlist = errlist + ('%s: %s, ' % (nname, reason))
                if errlist:
                        return 'ERR %s' % errlist
                else:
                        self.log('Released nodes %s' % (",".join(args),))
                        return 'OK'

        def doRepNode(self, cmd, args, msg):
                # REPNODE <nrep> <node> [...]
                if not self.USrv.isAdmin(self.Username):
                        return 'PERM Permission denied'
                if len(args) < 2:
                        return 'ERR Syntax error: %s' % msg
                errlist = ''
                nfrep = int(args[0])
                for nname in args[1:]:
                        sts, reason = vfssrv_global.G_CellIF.replicateNode(nname, nfrep)
                        if not sts:
                                errlist = errlist + ('%s: %s, ' % (nname, reason))
                if errlist:
                        return 'ERR %s' % errlist
                else:
                        self.log('Replicate nodes %d %s' % (nfrep, ",".join(args[1:])))
                        return 'OK'
                

        def doHello(self, cmd, args, msg):
                # HELLO <username>
                if not args:
                        return 'ERR Protocol syntax error: <%s>' % msg
                self.Username = args[0]
                #self.log('Hello %s' % self.Username)
                return 'OK'

        def processMsg(self, cmd, args, msg):
                msg = to_str(msg)
                #print 'processMsg(<%s>)' % msg
                if not self.Username and cmd != 'HELLO':
                        ans = 'ERR Say HELLO first'
                else:
                        try:    
                                fcn = self.MsgDispatch[cmd]
                        except:
                                #print sys.exc_type, sys.exc_value
                                return None
                        ans = fcn(self, cmd, args, msg)
                #print 'processMsg() -> %s' % ans
                return ans
        
        MsgDispatch = {
                        'HELLO' :       doHello,
                        'DF'    :       doDelFile,
                        'CHMOD' :       doChMod,
                        'SATTR' :       doSetAttr,
                        'LIST'  :       doList,
                        'GET'   :       doGetInfo,
                        'GETT'  :       doGetType,
                        'MF'    :       doMkFile,
                        'MD'    :       doMkDir,
                        'DD'    :       doDelDir,
                        'REPNODE'       :       doRepNode,
                        'RR'    :       doRereplicate,
                        'HOLD'  :       doHold,
                        'RELEASE'       :       doRelease,
                        'USAGE' :       doGetUsage                      
                }

        def eof(self):
                #self.log('connection closed')                          
                pass

class   VFSServer(TCPServerThread, Logged):
        def __init__(self, cfg):
                self.Cfg = cfg
                self.Port = cfg['api_port']
                TCPServerThread.__init__(self, self.Port, 
                    max_clients=10, queue_capacity=100)
                lst = []
                self.AdminList = cfg.get('admins', [])

        def isAdmin(self, user):
                return user in self.AdminList
                
        def createClientInterface(self, sock, addr):
                #self.log('New client at %s' % (addr,))
                return VFSClientConnection(self, sock, addr)
