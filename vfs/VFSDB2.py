import os
import stat
from VFSFileInfo import *
import sys
import vfssrv_global
import errno
import time
from VFSDBStorage import VFSDBStorage
import fnmatch
from pythreader import Primitive, synchronized
from logs import Logged

class   _Cache:
        def __init__(self, low, high):
                self._Low = low
                self._High = high
                self._Dict = {}                 # key -> (access_time, val)

        def has_key(self, key):
                return key in self._Dict

        def keys(self):
                return self._Dict.keys()
                
        def __getitem__(self, key):
                if key in self._Dict:
                        at, v = self._Dict[key]
                        self.touch(key)         # renew
                else:
                        v = self.readItem(key)
                        if v is not None:
                            self._Dict[key] = (int(time.time()), v)
                            self.cleanUp()
                return v

        def touch(self, k):
                t, v = self._Dict[k]
                self._Dict[k] = (int(time.time()), v)
                
        def __setitem__(self, key, val):
                self._Dict[key] = (int(time.time()), val)
                self.writeItem(key, val)
                self.cleanUp()

        def __delitem__(self, key):
                try:    del self._Dict[key]
                except: pass

        def cleanUp(self):
                if len(self._Dict) < self._High:        return
                lst = list(self._Dict.items())
                lst = sorted(lst, key=lambda x: x[1][0])
                self._Dict = dict(lst[:self._Low])

        # overridables
        def readItem(self, key):
                #print 'dummy readItem(%s)' % (key,)
                return None
                
        def writeItem(self, key, value):
                pass

class   VFSCache(_Cache):
        CacheHigh = 500
        CacheLow = 400
        CacheWriteBackLimit = 0

        def __init__(self, db):
                _Cache.__init__(self, self.CacheLow, self.CacheHigh)
                self.DB = db

        def has_key(self, key):
                return _Cache.has_key(self, VFSCanonicPath(key))
                
        def __getitem__(self, key):
                v = _Cache.__getitem__(self, VFSCanonicPath(key))
                return v
                
        def __setitem__(self, key, val):
                _Cache.__setitem__(self, VFSCanonicPath(key), val)

        def __delitem__(self, key):
                _Cache.__delitem__(self, VFSCanonicPath(key))

        def clearUnder(self, key):
                key = VFSCanonicPath(key)
                for k in list(self.keys())[:]:
                        if k.startswith(key):
                                del self[k]

        def readItem(self, key):
                return self.DB.readItem(key)
                
        def writeItem(self, key, val):
                return self.DB.writeItem(key, val)              


class   CellIndex(Logged):
        def __init__(self, cellname):
                self.CellName = cellname
                self.Files = {}
                
        def read(self, f):
                self.Files = {}
                l = f.readline()
                while l:
                        l = l.strip()
                        words = l.split()
                        try:
                                lp = words[0]
                                fid = int(words[1])
                        except:
                                pass
                        else:
                                self.Files[lp] = fid
                        l = f.readline()

        def write(self, f):
                for lp, fid in list(self.Files.items()):
                        f.write('%s %s\n' % (lp, fid))

        def addFile(self, lp, fid):
                self.Files[lp] = fid

        def removeFile(self, lp):
                try:    del self.Files[lp]
                except: pass

        def hasFile(self, lp, fid=None):
                return lp in self.Files and (
                        fid == None or self.Files[lp] == fid)

        def getFid(self, lp):
                return self.Files[lp]

        def files(self):
                return list(self.Files.keys())

        def set(self, dct):
                self.Files = {}
                for lp, fid in list(dct.items()):
                        self.Files[lp] = fid

class CellIndexDB(_Cache, Logged):
        CacheHigh = 500
        CacheLow = 400
        CacheWriteBackLimit = 100

        def __init__(self, root):
                self.Root = root
                _Cache.__init__(self, self.CacheLow, self.CacheHigh)

        def readItem(self, cname):
                inx = CellIndex(cname)
                try:
                        f = open(self.fpath(cname), 'r')
                        inx.read(f)
                        f.close()
                except:
                        pass
                return inx
                
        def writeItem(self, cname, inx):
                f = open(self.fpath(cname), 'w')
                inx.write(f)
                f.close()

        def fpath(self, cname):
                return '%s/%s.inx' % (self.Root, cname)

class   VFSFileLister(Logged):
        def __init__(self, db, str, prefix, iterable):
                self.DB = db
                self.Str = str
                self.PathPrefix = prefix
                if not self.PathPrefix or self.PathPrefix[-1] != '/':
                        self.PathPrefix = self.PathPrefix + '/'
                self.PathList = sorted(list(iterable))      # list of tuples: (name, type, info)

        def isEmpty(self):
                return len(self.PathList) == 0

        def doWrite(self, fd, sel):
                if self.isEmpty():
                        self.Str.send('.')
                        sel.unregister(wr=fd)
                        return
                lst = self.getNext()
                msglst = []
                #self.DB.Debug=1
                for lp, typ, info in lst:
                        msglst.append('%s %s %s' % (lp, typ, info.serialize()))
                #self.DB.Debug=0
                self.Str.send(msglst)
                
        def getNext(self, n=100):
                lst = self.PathList[:n]
                self.PathList = self.PathList[len(lst):]
                return lst

class   VFSDB(Primitive, Logged):

        def __init__(self, root):
            Primitive.__init__(self)
            self.Root = root
            self.Cache = VFSCache(self)             # lpath -> (entry_time, type, info)
            self.CellInxDB = CellIndexDB(self.Root + '/.cellinx')
            self.LastFlush = 0
            self.Debug = 0
            
            info = VFSDirInfo('/')
            info.Prot = 'rwrw'
            info.Username = 'root'
            self.DB = VFSDBStorage(self.Root + "/vfs.db", info.serialize())
            self.FlushInterval = 30.0
            
            self.debug("root info: %s" % (self.getDirInfo('/'),))
                
        def parentPath(self, path):
                dp = '/'.join(path.split('/')[:-1])
                if not dp:      dp = '/'
                return dp

        def relPath(self, lpath, dpath):
                if not dpath or dpath[-1] != '/':
                        dpath = dpath + '/'
                if lpath[:len(dpath)] == dpath:
                        lpath = lpath[len(dpath):]
                return lpath

        def fileName(self, lpath):
                lst = lpath.split('/')
                if not lst: return ''
                else:
                        return lst[-1]

        def walkTreeRec(self, start, downfirst, fcn, arg, carry):
                # walks through directory tree calling fcn
                # for each item (file or directory)
                # downfirst controls whether it goes down into subdirs first or
                # after visiting every file
                #print 'walkTreeRec(%s, %s): ...' % (start, downfirst),
                dlist = []
                prepath = start
                if prepath[-1] != '/':
                        prepath = prepath + '/'
                nfiles = 0
                ndirs = 0
                for rpath, typ, info in self.glob(start):
                        apath = prepath + rpath
                        carry = fcn(apath, typ, info, arg, carry)
                        if typ == 'd':
                                ndirs = ndirs + 1
                                if downfirst:
                                        #print 'walkTreeRec(%s, %s): calling walkTreeRec(%s, %s)' % (
                                        #       start, downfirst, apath, downfirst)
                                        carry = self.walkTreeRec(apath, downfirst, fcn, arg, carry)
                                else:
                                        dlist.append(apath)
                        else:
                                nfiles = nfiles + 1
                #print '%d files, %d subdirectories' % (nfiles, ndirs)
                for subdir in dlist:
                        carry = self.walkTreeRec(subdir, downfirst, fcn, arg, carry)
                return carry

        def glob(self, dirp, ptrn = '*'):
                dirp = VFSCanonicPath(dirp)
                for name, typ, info in self.DB.listItems(dirp):
                        if name[0] != '.' and fnmatch.fnmatch(name, ptrn):
                                if typ == 'f':
                                        info = VFSFileInfo(dirp + '/' + name, info)
                                else:
                                        info = VFSDirInfo(dirp + '/' + name, info)
                                yield name, typ, info
        
        @synchronized
        def listCellFiles(self, cname):
                return self.CellInxDB[cname].files()

        def cellInxWalk(self, lpath, typ, info, inxdict, carry):
                if typ == 'f':
                        for srv in info.Servers:
                                inx = {}
                                try:    inx = inxdict[srv]
                                except KeyError:
                                        inxdict[srv] = inx
                                inx[lpath] = info.CTime
                return carry
                
        @synchronized
        def recreateCellInxDB(self):
                dict = {}
                self.walkTreeRec('/', 0, self.cellInxWalk, dict, None)
                for cname, dct in list(dict.items()):
                        inx = self.CellInxDB[cname]
                        inx.set(dct)
                        self.CellInxDB[cname] = inx

        def inventoryCallback(self, lpath, typ, info, params, nfiles):
                inxdict, qmgr = params
                self.cellInxWalk(lpath, typ, info, inxdict, None)
                qmgr.inventoryCallback(lpath, typ, info, None, None)
                nfiles = nfiles + 1
                return nfiles           

        @synchronized
        def fileInventory(self, quotamgr):
                dict = {}
                quotamgr.initInventory()
                nfiles = self.walkTreeRec('/', 0, self.inventoryCallback, (dict, quotamgr), 0)
                for cname, dct in list(dict.items()):
                        inx = self.CellInxDB[cname]
                        inx.set(dct)
                        self.CellInxDB[cname] = inx
                #self.Debug = 1
                return nfiles
        
        def isDir(self, lpath):
                return self.getType(lpath) == 'd'
                
        def isFile(self, lpath):
                return self.getType(lpath) == 'f'

        def exists(self, lpath):
                return self.getType(lpath) != None

        def readItem(self, lpath):
                typ, info = self.DB.getItem(lpath)
                if typ == 'd':
                        return 'd', VFSDirInfo(lpath, info)
                if typ == 'f':
                        return 'f', VFSFileInfo(lpath, info)
                else:
                        return None

        @synchronized
        def writeItem(self, lpath, data):
                typ, info = data
                self.DB.putItem(lpath, typ, info.serialize())
                                
        def getType(self, lpath):
                tup = self.Cache[lpath]
                return None if tup is None else tup[0]

        def getFileInfo(self, lpath):
                tup = self.Cache[lpath]
                if tup is None: return None
                t, info = tup
                if t != 'f':    return None
                return info

        def getDirInfo(self, lpath):
                tup = self.Cache[lpath]
                if tup is None: return None
                t, info = tup
                if t != 'd':    return None
                return info

        def getInfo(self, lpath):
                tup = self.Cache[lpath]
                if tup is None: return None
                t, info = tup
                return info
        
        def storeFileInfo(self, info):
                self.Cache[info.Path] = ('f',info)
        
        def storeDirInfo(self, info):
                self.Cache[info.Path] = ('d',info)

        @synchronized
        def addServer(self, info, cellname):
                if info.Type != 'f':    return
                if not info.isStoredOn(cellname):
                        info.addServer(cellname)
                        self.storeFileInfo(info)
                        inx = self.CellInxDB[cellname]
                        if not inx.hasFile(info.Path, info.CTime):
                                inx.addFile(info.Path, info.CTime)
                                self.CellInxDB[cellname] = inx

        @synchronized
        def removeServer(self, info, cellname):
                if info.Type != 'f':    return
                if info.isStoredOn(cellname):
                        info.removeServer(cellname)
                        self.storeFileInfo(info)
                        inx = self.CellInxDB[cellname]
                        if inx.hasFile(info.Path):
                                inx.removeFile(info.Path)
                                self.CellInxDB[cellname] = inx
        
        @synchronized
        def mkfile(self, info):
                lpath = info.Path
                fn = self.fileName(lpath)
                if not fn or fn[0] == '.':
                        return 0, 'Invalid file name'
                if not self.isDir(self.parentPath(lpath)):
                        return 0, 'Parent directory not found'
                if self.exists(lpath):
                        if self.isFile(lpath):
                                sts, reason = self.rmfile(lpath)
                                if not sts:
                                        return 0, 'Can not remove existing file: %s' % reason
                        else:
                                return 0, '%s is a directory' % lpath
                try:    self.storeFileInfo(info)
                except:
                        return 0, 'Error: %s %s' % (sys.exc_info()[0], sys.exc_info()[1])
                return 1, 'OK'
                
        @synchronized
        def mkdir(self, info):
                lpath = info.Path
                fn = self.fileName(lpath)
                if not fn or fn[0] == '.':
                        return 0, 'Invalid directory name'
                if self.exists(lpath):
                        return 0, 'Already exists'
                if not self.isDir(self.parentPath(lpath)):
                        return 0, 'Parent directory not found'
                self.DB.mkdir(lpath, info.serialize())
                return 1, 'OK'

        @synchronized
        def rmfile(self, lpath):
                if not self.exists(lpath):
                        return 1, 'Already deleted'
                if not self.isFile(lpath):
                        return 0, 'Is not a file'
                info = self.getFileInfo(lpath)
                try:
                    self.DB.delItem(lpath)
                except: return 0, 'Error: %s %s' % (sys.exc_info()[0], sys.exc_info()[1])
                vfssrv_global.G_CellIF.delFile(lpath, info.Servers)
                del self.Cache[lpath]
                return 1, 'OK'

        def _rmdir_rec(self, path):
                if path == '/': return          # do not delete root
                self.DB.deleteItemsUnder(path, typ='f')
                for name, typ, _ in self.DB.listItems(path):
                        assert typ == 'd'
                        self._rmdir_rec(path + "/" + name)
                self.DB.deleteItemsUnder(path, typ='d')

        @synchronized
        def rmdir(self, lpath, recursive=False):
                if lpath == '/':        return
                assert self.getType(lpath) == 'd'
                if recursive:
                        self._rmdir_rec(lpath)
                else:
                        empty = True
                        for _ in self.DB.listItems(lpath):
                                empty = False
                                break
                        if not empty:
                                return False, "Directory %s is not empty" % (lpath,)
                self.DB.delItem(lpath)
                self.Cache.clearUnder(lpath)
                return True, "OK"
                
        def idle(self):
            pass
            #    if time.time() > self.LastFlush + self.FlushInterval:
            #           self.flush()
            #           self.LastFlush = time.time()

        @synchronized
        def flush(self):
                #self.Cache.flush()
                self.CellInxDB.flush()
        
