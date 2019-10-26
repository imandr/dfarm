from TCPServer import *
from TCPClientConnection import *
import sys
import vfssrv_global
import glob
from logs import Logged

class   StorageCellConnection(TCPClientConnection, Logged):
                def __init__(self, scif, sock, addr, sel):
                        self.SCIF = scif
                        TCPClientConnection.__init__(self, sock, addr, sel)
                        self.Name = None
                        self.Synchronized = 0
                        self.FileList = {}
                        self.CAddr = addr
                        self.debug("connection created")

                def __str__(self):
                    return "CellIF[%s@%s]" % (self.Name, self.CAddr)

                def processMsg(self, cmd, args, msg):
                        self.debug("processMsg: cmd:%s args:%s" % (cmd, args))
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
                    # msg: IHAVE <path> <ctime> <size>,...
                    ihave, args = msg.split(" ", 1)
                    assert ihave == "IHAVE"
                    lst = args.split(",")
                    for finfo in lst:
                        self.processSingleIHave(finfo.split())

                def processSingleIHave(self, args):
                        # <path> <ctime> [<size>]
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
                        self.log("singeIHave: %s %s %s" % (lpath, ct, actSize))
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

class   StorageCellIF(TCPServer, Logged):
        def __init__(self, cfg, sel):
                self.Cfg = cfg
                self.Port = cfg['cellif_port']
                TCPServer.__init__(self, self.Port, sel)
                self.Sock.listen(100)           # to be raplaced with
                                                                        # TCPServer.enableServer(backlog=100)
                self.CellMap = {}
                hold_dir = cfg['hold_dir']
                self.HoldList = CellHoldList(hold_dir)
                self.log('Hold list: %s' % ",".join(self.HoldList.list()))

        def nodeIsHeld(self, nname):
                return self.HoldList.isHeld(nname)

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
                self.debug("createClientInterface: addr=%s" % (addr,))
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
                
