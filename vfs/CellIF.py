#
# @(#) $Id: CellIF.py,v 1.9 2002/08/16 19:18:28 ivm Exp $
#
# Cell Manager interface to VFS DB
#
# $Log: CellIF.py,v $
# Revision 1.9  2002/08/16 19:18:28  ivm
# Implemented size estimates for ftpd
#
# Revision 1.8  2002/08/12 16:29:43  ivm
# Implemented cell indeces
# Kerberized ftpd
#
# Revision 1.7  2002/04/30 20:07:15  ivm
# Implemented and tested:
#       node replication
#       node hold/release
#
# Revision 1.6  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.5  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.4  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.3  2001/05/08 22:17:46  ivm
# Fixed some bugs
#
# Revision 1.2  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

from TCPServer import *
from TCPClientConnection import *
import sys
import vfssrv_global
import glob

from py3 import to_str, to_bytes

class   StorageCellConnection(TCPClientConnection):
                def __init__(self, scif, sock, addr, sel):
                        self.SCIF = scif
                        TCPClientConnection.__init__(self, sock, addr, sel)
                        self.Name = None
                        self.Synchronized = 0
                        self.FileList = {}

                def log(self, msg):
                        msg = 'CellIF[%s]: %s' % (self.Name, msg)
                        if vfssrv_global.G_LogFile:
                                vfssrv_global.G_LogFile.log(msg)
                        else:
                                print(msg)
                                sys.stdout.flush()
                        
                def processMsg(self, cmd, args, msg):
                        #raise ValueError("processMsg: %s %s" % (repr(cmd), args))
                        msg = to_str(msg)
                        if self.Name:
                                fcn = self.MsgDispatch[cmd]
                                return fcn(self, cmd, args, msg)
                        else:
                                if cmd != 'HELLO':
                                        self.disconnect('Say HELLO first')
                                        return None
                                if not args:
                                        self.disconnect('HELLO syntax error: <%s>' % msg)
                                        return None
                                self.Name = args[0]
                                if self.SCIF.cellConnected(self.Name):
                                        self.disconnect('Already connected')
                                        return None
                                self.SCIF.addCellConnection(self.Name, self)
                                if self.SCIF.nodeIsHeld(self.Name):
                                        return 'HOLD'
                                else:
                                        return 'OK'

                def doIHave(self, cmd, args, msg):
                        # IHAVE <path> <ctime> [<size>]
                        if len(args) < 2:
                                return 'ERR Syntax error: <%s>' % msg
                        lpath = args[0]
                        ct = args[1]
                        try:    ct = int(ct)
                        except:
                                return 'ERR Syntax error: <%s>' % msg
                        actSize = None
                        if len(args) > 2:
                                actSize = int(args[2])
                        self.log(msg)
                        info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                        if info == None or info.CTime != ct:
                                self.log('Out-of-date %s' % lpath)
                                self.sendDel(lpath)
                        else:
                                need_update = 0
                                if actSize and info.sizeEstimated():
                                        info.setActualSize(actSize)
                                        vfssrv_global.G_QuotaMgr.updateReservation(
                                                '%s %s' % (lpath, info.CTime), info.sizeMB())
                                        need_update = 1
                                if not info.isStoredOn(self.Name):
                                        vfssrv_global.G_QuotaMgr.instanceCreated(
                                                '%s %s' % (lpath, info.CTime),
                                                info.Username, 
                                                info.sizeMB())
                                        vfssrv_global.G_VFSDB.addServer(info, self.Name)
                                        need_update = 0
                                if need_update:
                                        vfssrv_global.G_VFSDB.storeFileInfo(info)
                                if not self.Synchronized:
                                        self.FileList[lpath] = 1
                        return None

                def doSync(self, cmd, args, msg):
                        flst = vfssrv_global.G_VFSDB.listCellFiles(self.Name)
                        for lpath in flst:
                                if lpath not in self.FileList:
                                        info = vfssrv_global.G_VFSDB.getFileInfo(lpath)
                                        if info and info.isStoredOn(self.Name):
                                                self.log('sync: removing instance of %s' % lpath)
                                                vfssrv_global.G_VFSDB.removeServer(info, self.Name)
                                                vfssrv_global.G_QuotaMgr.instanceDeleted(
                                                '%s %s' % (lpath, info.CTime),
                                                info.Username, 
                                                info.sizeMB())
                        self.FileList = {}
                        self.log('Synchronized')
                        return 'SYNC'

                MsgDispatch = {
                                'IHAVE' :       doIHave,
                                'SYNC'  :       doSync
                        }

                def sendDel(self, lpath):
                        self.send('DEL %s' % lpath)             

                def sendHold(self):
                        self.send('HOLD')

                def sendRelease(self):
                        self.send('RELEASE')

                def eof(self):
                        if self.Name:
                                self.SCIF.deleteCellConnection(self.Name)

                def rereplicate(self, lpath, mult):
                        self.send('REPLICATE %s %d' % (lpath, mult))
                        return 1, 'OK'                                                          

class   CellHoldList:
        def __init__(self, dir):
                self.CfgDir = dir
                self.Dict = {}
                for fn in glob.glob1(self.CfgDir, '*.hold'):
                        self.Dict[fn[:-5]] = 1

        def hold(self, nname):
                f = open(self.CfgDir + '/%s.hold' % nname, 'w')
                f.close()
                self.Dict[nname] = 1
                
        def release(self, nname):
                try:    os.remove(self.CfgDir + '/%s.hold' % nname)
                except: pass
                self.Dict[nname] = 0
                
        def isHeld(self, nname):
                return nname in self.Dict and self.Dict[nname]

        def list(self):
                return list(self.Dict.keys())

class   StorageCellIF(TCPServer):
        def __init__(self, cfg, sel):
                self.Port = cfg['cellif_port']
                TCPServer.__init__(self, self.Port, sel)
                self.Sock.listen(100)           # to be raplaced with
                                                                        # TCPServer.enableServer(backlog=100)
                self.CellMap = {}
                self.HoldList = CellHoldList(cfg['lock_dir'])

        def nodeIsHeld(self, nname):
                return self.HoldList.isHeld(nname)

        def log(self, msg):
                msg = 'CellIF: %s' % (msg,)
                if vfssrv_global.G_LogFile:
                        vfssrv_global.G_LogFile.log(msg)
                else:
                        print(msg)
                        sys.stdout.flush()
                        
        def deleteCellConnection(self, nname):
                self.log('cell disconnected: %s' % nname)
                try:    del self.CellMap[nname]
                except: pass
                
        def addCellConnection(self, nname, conn):
                self.log('new cell: %s' % nname)
                self.CellMap[nname] = conn
                
        def cellConnected(self, nname):
                return nname in self.CellMap
                
        def createClientInterface(self, sock, addr, sel):
                StorageCellConnection(self, sock, addr, sel)            

        def delFile(self, lpath, cells):
                for c in cells:
                        if c in self.CellMap:
                                self.CellMap[c].sendDel(lpath)

        def holdNode(self, nname):
                if nname not in self.CellMap:
                        return 0, 'Not connected'
                self.CellMap[nname].sendHold()
                self.HoldList.hold(nname)
                return 1, 'OK'
        
        def releaseNode(self, nname):
                if nname not in self.CellMap:
                        return 0, 'Not connected'
                self.CellMap[nname].sendRelease()
                self.HoldList.release(nname)
                return 1, 'OK'
                
        def replicateNode(self, nname, nfrep):
                if nname not in self.CellMap:
                        return 0, 'Not connected'
                self.CellMap[nname].rereplicate('*', nfrep)
                return 1, 'OK'
                
        def rereplicate(self, lpath, cells, mult):
                if not cells:
                        return 0, 'File has no replicas'
                error = 'No replicas available'
                for cn in cells:
                        if cn in self.CellMap:
                                sts, reason = self.CellMap[cn].rereplicate(lpath, mult)
                                if sts:
                                        return 1, 'OK'
                                else:
                                        error = reason
                return 0, error
                
