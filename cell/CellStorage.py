from txns import HasTxns

from txns import *
import os
try:    import statfs
except: pass

import glob
import stat
import cellmgr_global
from logs import Logged
import pwd
from VFSFileInfo import *
import sys
import errno


def free_MB(path):
        try:    
                tup = os.statvfs(path)
                return int(float(tup[0])*float(tup[4])/(1024*1024))
        except: 
                return statfs.free_mb(path)
        
class   GetTxn(DLTxn, Logged):
        def __init__(self, psa, lpath):
                DLTxn.__init__(self, 0, psa)
                self.PSA = psa
                self.LPath = lpath
                self.CAddr = None
                
        def __str__(self):
            return "GetTxn[%s]" % (self.LPath,)
            
        def dataPath(self):
                return self.PSA.fullDataPath(self.LPath)

        def isPutTxn(self):
                return 0
                
        def isGetTxn(self):
                return 1

class   PutTxn(ULTxn, Logged):
        def __init__(self, size, psa, lpath, info):
                ULTxn.__init__(self, size, psa)
                self.PSA = psa
                self.LPath = lpath
                self.Info = info
                self.CAddr = None

        def __str__(self):
            return "PutTxn[%s]" % (self.LPath,)

        def dataPath(self):
                return self.PSA.fullDataPath(self.LPath)

        def rollback(self):
                self.debug("rollback")
                self.PSA.receiveAborted(self.LPath)
                Transaction.rollback(self)

        def commit(self):
                self.debug("commit")
                actSize = 0
                actSizeMB = 0
                try:
                        st = os.stat(self.dataPath())
                        actSize = int(st[stat.ST_SIZE])
                        actSizeMB = int(float(actSize)/1024/1024+0.5)
                except Exception as e:
                    self.log("Error getting file size for %s: %s" % (
                        self.dataPath(), e))
                self.debug("actual size:%s" % (actSize,))
                self.Info.setActualSize(actSize)
                cellmgr_global.VFSSrvIF.sendIHave(self.LPath, self.Info)
                self.PSA.receiveComplete(self.LPath, self.Info)
                Transaction.commit(self, actSizeMB)
                if self.NFRep > 0:
                        cellmgr_global.ReplicationManager.replicate(self.NFRep, self.dataPath(),
                                        self.LPath, self.Info)

        def isPutTxn(self):
                return 1
                
        def isGetTxn(self):
                return 0

        def attractor(self, sclass):
                return self.PSA.attractor(sclass)

class   PSA(HasTxns, Logged):

        def __init__(self, name, root, size, attractors):       # size in mb
                self.Name = name
                self.Root = root
                self.Attractors = attractors
                self.DataRoot = self.Root + '/data'
                self.InfoRoot = self.Root + '/info'
                HasTxns.__init__(self, size, 0)
                self.Used = 0
                self.LastPrune = 0

        def __str__(self):
            return "PSA[%s @%s]" % (self.Name, self.Root)

        def fullDataPath(self, lpath):
                if not lpath or lpath[0] != '/':
                        lpath = '/' + lpath
                return self.DataRoot + lpath

        def fullInfoPath(self, lpath):
                if not lpath or lpath[0] != '/':
                        lpath = '/' + lpath
                return self.InfoRoot + lpath

        def logPath(self, fpath):
                if fpath[:len(self.DataRoot)] == self.DataRoot:
                        tail = fpath[len(self.DataRoot):]
                elif fpath[:len(self.InfoRoot)] == self.InfoRoot:
                        tail = fpath[len(self.InfoRoot):]
                if not tail:    tail = '/'
                return tail

        def dirPath(self, path):
                dp = '/'.join(path.split('/')[:-1])
                #if not dp:     dp = '/'
                return dp

        def canonicPath(self, path):
                # replace repearing '/' with singles
                while path and '//' in path:
                    path = path.replace("//", '/')
                if not path or path[0] != '/':
                        path = '/' + path
                return path

        def calcUsed(self):
                used = 0
                for lp, i in self.listFiles():
                        if i:
                                used = used + i.sizeMB()
                return used

        def spaceUsage(self):
                return float(self.Used + self.reservedByTxns())/float(self.Size)

        def free(self):         # calculate actual physical space available
                return min(free_MB(self.Root), self.Size - self.Used) - self.reservedByTxns()
                
        def freeMB(self):
                return free_MB(self.Root)

        def storeFileInfo(self, lpath, info):
                ipath = self.fullInfoPath(lpath)
                try:    os.makedirs(self.dirPath(ipath),0o711)
                except: pass
                f = open(ipath,'w')
                f.write(info.serialize())
                f.close()
                # set file owner here
                                
        def getFileInfo(self, lpath):
                ipath = self.fullInfoPath(lpath)
                dpath = self.fullDataPath(lpath)
                try:    st = os.stat(dpath)
                except: 
                        self.delFile(lpath)
                        return None
                if stat.S_ISDIR(st[stat.ST_MODE]):
                        try:    os.rmdir(dpath)
                        except: pass
                        try:    os.rmdir(ipath)
                        except: pass
                        return None
                try:    f = open(ipath,'r')
                except:
                        #self.delFile(lpath)
                        return None
                else:
                        str = f.read()
                        f.close()
                        info = VFSFileInfo(lpath, str)
                return info

        def listRec(self, list, dir):
                # run recursively through info records
                for fn in glob.glob1(dir,'*'):
                        fpath = dir + '/' + fn
                        try:    st = os.stat(fpath)
                        except: continue
                        if stat.S_ISDIR(st[stat.ST_MODE]):
                                list = self.listRec(list, fpath)
                        else:
                                lpath = self.logPath(fpath)
                                info = self.getFileInfo(lpath)
                                list.append((lpath, info))
                return list

        def listFiles(self):
                list = []
                return self.listRec(list, self.InfoRoot)

        def init(self):
                # create PSA directories
                try:    os.makedirs(self.DataRoot, 0o711)
                except OSError as val:
                        if val.errno == errno.EEXIST:
                                st = os.stat(self.DataRoot)
                                if not stat.S_ISDIR(st[stat.ST_MODE]):
                                        raise OSError(val)
                        else:
                                raise OSError(val)
                try:    os.makedirs(self.InfoRoot, 0o711)
                except OSError as val:
                        if val.errno == errno.EEXIST:
                                st = os.stat(self.InfoRoot)
                                if not stat.S_ISDIR(st[stat.ST_MODE]):
                                        raise OSError(val)
                        else:
                                raise OSError(val)
                # remove data entries which do not have infos
                data_lst = []
                data_lst = self.listRec(data_lst, self.DataRoot)
                for lpath, info in data_lst:
                        if not info:
                                self.log('CellStorage.init: deleted data for: %s' % lpath)
                                self.delFile(lpath)

                # calc how much space is in use
                self.Used = self.calcUsed()

        def delFile(self, lpath):
                lpath = self.canonicPath(lpath)
                self.debug("delFile(%s)" % (lpath,))
                try:    
                        st = os.stat(self.fullDataPath(lpath))
                except:
                        pass
                else:
                        sizemb = int(float(st[stat.ST_SIZE])/1024/1024 + 0.5)
                        self.Used = self.Used - sizemb
                try:    os.remove(self.fullDataPath(lpath))
                except: pass
                try:    os.remove(self.fullInfoPath(lpath))
                except: pass
                dp = self.dirPath(lpath)
                while dp and dp != '/':
                        try:    os.rmdir(self.fullDataPath(dp))
                        except: pass
                        try:    os.rmdir(self.fullInfoPath(dp))
                        except: pass
                        dp = self.dirPath(dp)
                
        def canReceive(self, lpath, info):
                return self.free() >= info.sizeMB() and self.attractor(info.dataClass()) > 0

        def receive(self, lpath, info):
                dpath = self.fullDataPath(lpath)
                try:    os.makedirs(self.dirPath(dpath), 0o711)
                except: pass            # it may already exist

                try:    
                        os.close(os.open(dpath,os.O_CREAT, 0o744))
                except:
                        return None
                return PutTxn(info.sizeMB(), self, lpath, info)

        def receiveComplete(self, lpath, info):
                self.log('Received: %s' % lpath)
                self.storeFileInfo(lpath, info)

        def receiveAborted(self, lpath):
                self.log('Receive aborted: %s' % lpath)
                try:    os.remove(self.fullDataPath(lpath))
                except: pass
                
        def send(self, lpath):
                return GetTxn(self, lpath)

        def hasFile(self, lpath):
                return self.getFileInfo(lpath)

        def status(self):
                # returns size, used, reserved, physically free
                return self.Size, self.Used, self.reservedByTxns(), self.freeMB()

        def fileBeingReceived(self, lpath):
                for txn in self.txnList():
                        if txn.isPutTxn() and txn.LPath == lpath:
                                return 1

        def replicate(self, nfrep):
                for lpath, info in self.listFiles():
                        if info != None:
                                cellmgr_global.ReplicationManager.replicate(nfrep, 
                                        self.fullDataPath(lpath),
                                        lpath, info)

        def idle(self):
                if time.time() > self.LastPrune + 3600:
                        self.prune()
                self.LastPrune = time.time()

        def prune(self):
                # remove empty directories from Info and Data area
                self.log("Pruning")
                self.pruneRec(self.DataRoot)
                self.pruneRec(self.InfoRoot)
                
        def pruneRec(self, dir):
                for fn in glob.glob1(dir,'*'):
                        fpath = dir + '/' + fn
                        try:    st = os.stat(fpath)
                        except: continue
                        if stat.S_ISDIR(st[stat.ST_MODE]):
                                self.pruneRec(fpath)
                                try:    os.rmdir(fpath)
                                except:
                                        self.log("can not remove directory <%s>: %s %s" %
                                                (fpath, sys.exc_info()[0], sys.exc_info()[1]))
                                else:
                                        self.log("removed directory <%s>" % fpath)

        def attractor(self, sclass):
                if sclass in self.Attractors:
                        return self.Attractors[sclass]
                if '*' in self.Attractors:
                        return self.Attractors['*']
                return 0

class   CellStorageMgr(Logged):
        def __init__(self, myid, myclass, cfg):
                self.PSAs = {}
                for psan, params in cfg.items():
                        if psan in ['max_get','max_put','max_txn','max_rep']:
                                continue
                        if not isinstance(params, list) or len(params) < 2:       continue
                        attractors = {}
                        root, size = params[:2]
                        params = params[2:]
                        if not params:
                                params = ['*:50']
                        for word in params:
                                try:
                                        c, v = word.split(':')
                                        v = int(v)
                                except:
                                        continue
                                attractors[c] = v
                        self.PSAs[psan] = PSA(psan, root, size, attractors)
                        try:    self.PSAs[psan].init()
                        except OSError as val:
                                self.log("Can not initialize PSA %s at %s: %s\n" %
                                        (psan, root, val))
                                raise OSError(val)
                self.PSAList = list(self.PSAs.keys())           # list for round-robin
                self.IsHeld = 0         

        def status(self):
                ret = ''
                if self.IsHeld:
                        ret = '(held)'
                else:
                        ret = '(OK)'
                for psan in self.listPSAs():
                        size, used, rsrvd, free = self.getPSA(psan).status()
                        ret = ret + (" %s:%s:%s:%s" % 
                                                (psan, size, size - used - rsrvd, free - rsrvd))
                return ret
                        

        def hold(self):
                self.IsHeld = 1
                self.log('held')
                
        def release(self):
                self.IsHeld = 0
                self.log('released')

        def listFiles(self):
                lst = []
                for psa in self.PSAs.values():
                        lst = lst + psa.listFiles()
                return lst
                
        def findPSA(self, lpath, info):
                i = 0
                for psan in self.PSAList:
                        psa = self.PSAs[psan]
                        if psa.canReceive(lpath, info):
                                return psa
                return None
                
        def receiveFile(self, lpath, info):
                # find available PSA
                # allocate space there
                # create and return the txn
                if self.IsHeld or self.fileBeingReceived(lpath):
                        return None, None
                psa, existing_info = self.findFile(lpath)
                if psa != None:
                    #if existing_info.CTime >= info.CTime:
                        return None, None
                    #psa.delFile(lpath)
                psa = self.findPSA(lpath, info)
                if psa:
                    n = psa.Name
                    self.PSAList.remove(n)
                    self.PSAList.append(n)
                    return psa.receive(lpath, info), psa.attractor(info.dataClass())
                return None, None

        def fileBeingReceived(self, lpath):
                for psa in list(self.PSAs.values()):
                        if psa.fileBeingReceived(lpath):
                                return psa
                return None

        def findFile(self, lpath):
                for psa in list(self.PSAs.values()):
                        info = psa.hasFile(lpath)
                        if info:        return psa, info
                return None, None
                
        def delFile(self, lpath):
                done = 0
                while not done:
                        psa, info = self.findFile(lpath)
                        if psa:
                                psa.delFile(lpath)
                        else:
                                done = 1

        def sendFile(self, lpath):
                psa, info = self.findFile(lpath)
                if psa:
                        return psa.send(lpath)
                else:
                        return None

        def getPSA(self, psan):
                return self.PSAs[psan]
                
        def listPSAs(self):
                return list(self.PSAs.keys())

        def replicateAll(self, nfrep):
                for psa in self.PSAs.values():
                        psa.replicate(nfrep)

        def replicateFile(self, lpath, nfrep):
                psa, info = self.findFile(lpath)
                if psa == None or info == None:
                        return 0, 'File not found'
                cellmgr_global.ReplicationManager.replicate(nfrep, 
                        psa.fullDataPath(lpath),
                        lpath, info)

        def idle(self):
                for psa in self.PSAs.values():
                        psa.idle()
