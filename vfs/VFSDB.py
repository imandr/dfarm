#
# @(#) $Id: VFSDB.py,v 1.14 2003/12/09 16:33:29 ivm Exp $
#
# $Log: VFSDB.py,v $
# Revision 1.14  2003/12/09 16:33:29  ivm
# Fixed bug with rmdir
#
# Revision 1.12  2003/11/25 20:37:02  ivm
# Implemented write-back cache
#
# Revision 1.11  2003/03/25 17:36:46  ivm
# Implemented non-blocking directory listing transmission
# Implemented single inventory walk-through
# Implemented re-tries on failed connections to VFS Server
#
# Revision 1.10  2003/01/30 17:33:11  ivm
# .
#
# Revision 1.9  2002/08/12 16:29:43  ivm
# Implemented cell indeces
# Kerberized ftpd
#
# Revision 1.8  2002/04/30 20:07:16  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.7  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.6  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.5  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.4  2001/05/08 22:17:46  ivm
# Fixed some bugs
#
# Revision 1.3  2001/04/24 16:44:59  ivm
# Implemented "dummy" client authentication and permission validation
#
# Revision 1.2  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

import os
import stat
import string
from VFSFileInfo import *
import glob
import sys
import vfssrv_global
import errno
import time
import bsddb
import fnmatch

class	_Cache:
	def __init__(self, low, high, autoflush_level = 0):
		self._Low = low
		self._High = high
		self._Dict = {} 		# key -> (access_time, val)
		self._DirtyKeys = {}
		self._AutoFlush = autoflush_level

	def has_key(self, key):
		return self._Dict.has_key(key)
		
	def __getitem__(self, key):
		if self._Dict.has_key(key):
			at, v = self._Dict[key]
			self.touch(key)		# renew
		else:
			v = self.readItem(key)
			self._Dict[key] = (int(time.time()), v)
			self.cleanUp()
		return v

	def touch(self, k):
		t, v = self._Dict[k]
		self._Dict[k] = (int(time.time()), v)
		
	def __setitem__(self, key, val):
		self._Dict[key] = (int(time.time()), val)
		self._DirtyKeys[key] = 1
		self.cleanUp()			

	def __delitem__(self, key):
		try:	del self._Dict[key]
		except:	pass
		try:	del self._DirtyKeys[key]
		except: pass

	def cleanUp(self):
		if len(self._DirtyKeys) > self._AutoFlush:
			self.flush()
		if len(self._Dict) < self._High:	return
		lst = self._Dict.items()
		lst.sort(lambda x,y: y[1][0] - x[1][0])
		self._Dict = {}
		for key, x in lst[:self._Low]:
			self._Dict[key] = x

	def flush(self):
		for k in self._DirtyKeys.keys():
			self.writeItem(k, self._Dict[k][1])
		self._DirtyKeys = {}

	# overridables
	def readItem(self, key):
		#print 'dummy readItem(%s)' % (key,)
		return None
		
	def writeItem(self, key, value):
		pass

class	VFSCache(_Cache):
	CacheHigh = 500
	CacheLow = 400
	CacheWriteBackLimit = 0

	def __init__(self, db):
		_Cache.__init__(self, self.CacheLow, self.CacheHigh, self.CacheWriteBackLimit)
		self.DB = db

	def has_key(self, key):
		return _Cache.has_key(self, VFSCanonicPath(key))
		
	def __getitem__(self, key):
		#if self.DB.Debug:
		#	if self.has_key(key):
		#		#print '__getitem__(%s): in cache' % key
		#	else:
		#		#print '__getitem__(%s): not in cache' % key
		v = _Cache.__getitem__(self, VFSCanonicPath(key))
		#if self.DB.Debug:
		#	print '__getitem__: value=%s' % (v,)
		return v
		
	def __setitem__(self, key, val):
		_Cache.__setitem__(self, VFSCanonicPath(key), val)

	def __delitem__(self, key):
		_Cache.__delitem__(self, VFSCanonicPath(key))

	def readItem(self, key):
		return self.DB.readItem(key)
		
	def writeItem(self, key, val):
		return self.DB.writeItem(key, val)		

class	CellIndex:
	def __init__(self, cellname):
		self.CellName = cellname
		self.Files = {}
		
	def read(self, f):
		self.Files = {}
		l = f.readline()
		while l:
			l = string.strip(l)
			words = string.split(l)
			try:
				lp = words[0]
				fid = int(words[1])
			except:
				pass
			else:
				self.Files[lp] = fid
			l = f.readline()

	def write(self, f):
		for lp, fid in self.Files.items():
			f.write('%s %s\n' % (lp, fid))

	def addFile(self, lp, fid):
		self.Files[lp] = fid

	def removeFile(self, lp):
		try:	del self.Files[lp]
		except: pass

	def hasFile(self, lp, fid=None):
		return self.Files.has_key(lp) and (
			fid == None or self.Files[lp] == fid)

	def getFid(self, lp):
		return self.Files[lp]

	def files(self):
		return self.Files.keys()

	def set(self, dct):
		self.Files = {}
		for lp, fid in dct.items():
			self.Files[lp] = fid

class CellIndexDB(_Cache):
	CacheHigh = 500
	CacheLow = 400
	CacheWriteBackLimit = 100

	def __init__(self, root):
		self.Root = root
		_Cache.__init__(self, self.CacheLow, self.CacheHigh, 
				self.CacheWriteBackLimit)

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

class	VFSFileLister:
	def __init__(self, db, str, prefix, lst):
		self.DB = db
		self.Str = str
		self.PathPrefix = prefix
		if not self.PathPrefix or self.PathPrefix[-1] != '/':
			self.PathPrefix = self.PathPrefix + '/'
		self.PathList = lst
		self.PathList.sort()

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
		for lp in lst:
			lpath = self.PathPrefix + lp
			typ = ''
			if self.DB.isFile(lpath):
				info = self.DB.getFileInfo(lpath)
				typ = 'f'
			elif self.DB.isDir(lpath):
				info = self.DB.getDirInfo(lpath)
				typ = 'd'
			else:
				continue
			#print 'Lister: type for %s is %s' % (lp, typ)
			msglst.append('%s %s %s' % (lp, typ, info.serialize()))
		#self.DB.Debug=0
		self.Str.send(msglst)
		
	def getNext(self, n=10):
		lst = self.PathList[:n]
		self.PathList = self.PathList[len(lst):]
		return lst

class	VFSDB:

	FlushInterval = 600
	__DirInfoKey = '..info..'
	__DirIndexName = 'index.db'
	
	def __init__(self, root):
		self.Root = root
		self.Cache = VFSCache(self) 		# lpath -> (entry_time, type, info)
		self.CellInxDB = CellIndexDB(self.Root + '/.cellinx')
		self.LastFlush = 0
		self.Debug = 0
		if self.getDirInfo('/') == None:
			info = VFSDirInfo('/')
			info.Prot = 'rwrw'
			info.Username = 'root'
			self.storeDirInfo(info)
		
	def debug(self, str):
		if self.Debug:	print str

	def fileInfoPath(self, lpath):
		return self.fullPath(lpath)

	def fullPath(self, lpath):
		if not lpath or lpath[0] != '/':
			lpath = '/' + lpath
		return self.Root + lpath

	def dirIndexPath(self, lpath):
		if not lpath or lpath[0] != '/':
			lpath = '/' + lpath
		return self.Root + lpath + '/' + self.__DirIndexName
		
	def logPath(self, fpath):
		tail = fpath[len(self.Root):]
		if not tail:	tail = '/'
		if self.fileName(tail) == self.__DirIndexName:
			tail = self.parentPath(tail)
		return tail

	def parentPath(self, path):
		dp = string.join(string.split(path,'/')[:-1],'/')
		if not dp:	dp = '/'
		return dp

	def relPath(self, lpath, dpath):
		if not dpath or dpath[-1] != '/':
			dpath = dpath + '/'
		if lpath[:len(dpath)] == dpath:
			lpath = lpath[len(dpath):]
		return lpath

	def fileName(self, lpath):
		lst = string.split(lpath,'/')
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
		for rpath, typ, info in self.glob2(start):
			apath = prepath + rpath
			carry = fcn(apath, typ, info, arg, carry)
			if typ == 'd':
				ndirs = ndirs + 1
				if downfirst:
					#print 'walkTreeRec(%s, %s): calling walkTreeRec(%s, %s)' % (
					#	start, downfirst, apath, downfirst)
					carry = self.walkTreeRec(apath, downfirst, fcn, arg, carry)
				else:
					dlist.append(apath)
			else:
				nfiles = nfiles + 1
		#print '%d files, %d subdirectories' % (nfiles, ndirs)
		for subdir in dlist:
			carry = self.walkTreeRec(subdir, downfirst, fcn, arg, carry)
		return carry

	def glob(self, ptrn):
		# returns unsorted list of absolute logical paths matching the pattern
		dirptrn = self.parentPath(ptrn)
		fnptrn = self.fileName(ptrn)
		dirlst = glob.glob(self.fullPath(dirptrn))
		dirlst = filter(lambda x: string.find(x, '/.') < 0, dirlst)
		lst = []
		for dp in map(lambda x, s=self: s.logPath(x), dirlst):
			lst = lst + map(lambda x, d=dp: d + '/' + x,
					self.glob1(dp, fnptrn))
		return lst
		
	def glob1(self, dirp, ptrn):
		# returns unsorted list of items in the directory matching the pattern
		db = bsddb.btopen(self.dirIndexPath(dirp), 'c')
		lst = db.keys()
		db.close()
		#print 'glob1: lst1(%d): %s...' % (len(lst), lst[:5])
		lst = filter(lambda fn, pt=ptrn, s=self: fn != s.__DirInfoKey and 
				fnmatch.fnmatch(fn, pt),
				lst)
		#print 'glob1: lst2(%d): %s...' % (len(lst), lst[:5])
		fullp = self.fullPath(dirp)
		for fn in glob.glob1(fullp, '*'):
			if not fn or fn[0] == '.':	continue
			st = os.stat(fullp + '/' + fn)
			if stat.S_ISDIR(st[stat.ST_MODE]):
				lst.append(fn)
		#print 'glob1: lst3(%d): ...%s' % (len(lst), lst[-5:])
		return lst

	def glob2(self, dirp, ptrn = '*'):
		#print 'glob2: opening %s' % dirp
		db = bsddb.btopen(self.dirIndexPath(dirp), 'c')
		files = filter(lambda fn, pt=ptrn, s=self: fn != s.__DirInfoKey and 
					fnmatch.fnmatch(fn, pt), db.keys())
		lst = []
		for fn in files:
			info = VFSFileInfo(dirp + '/' + fn, db[fn])
			lst.append((fn, 'f', info))
		db.close()
		fullp = self.fullPath(dirp)
		for fn in glob.glob1(fullp, '*'):
			if not fn or fn[0] == '.':	continue
			st = os.stat(fullp + '/' + fn)
			if stat.S_ISDIR(st[stat.ST_MODE]):
				lp = dirp + '/' + fn
				info = self.getDirInfo(lp)
				if info != None:
					lst.append((fn, 'd', info))
		return lst

	def listDir(self, dir, ptrn = '*'):
		# always returns list of relative paths
		dir = VFSCanonicPath(dir)
		lst1 = []
		db = bsddb.btopen(self.dirIndexPath(dir), 'r')
		for fn in self.glob1(dir, ptrn):
			lpath = dir + '/' + fn
			typ = self.getType(lpath)
			info = None
			if typ == 'f':
				info = VFSFileInfo(lpath, db[fn])
			elif typ == 'd':
				info = self.getDirInfo(lpath)
			# remove dir from fn
			lst1.append((fn, typ, info))
		#print 'listDir(%s, %s): lst1(%d): %s' % (dir, ptrn, len(lst1), lst1[:5])
		db.close()
		return lst1

	def listCellFiles(self, cname):
		return self.CellInxDB[cname].files()

	def cellInxWalk(self, lpath, typ, info, inxdict, carry):
		if typ == 'f':
			for srv in info.Servers:
				inx = {}
				try:	inx = inxdict[srv]
				except KeyError:
					inxdict[srv] = inx
				inx[lpath] = info.CTime
		return carry
		
	def recreateCellInxDB(self):
		dict = {}
		self.walkTreeRec('/', 0, self.cellInxWalk, dict, None)
		for cname, dct in dict.items():
			inx = self.CellInxDB[cname]
			inx.set(dct)
			self.CellInxDB[cname] = inx

	def inventoryCallback(self, lpath, typ, info, params, nfiles):
		inxdict, qmgr = params
		self.cellInxWalk(lpath, typ, info, inxdict, None)
		qmgr.inventoryCallback(lpath, typ, info, None, None)
		nfiles = nfiles + 1
		return nfiles		

	def fileInventory(self, quotamgr):
		dict = {}
		quotamgr.initInventory()
		nfiles = self.walkTreeRec('/', 0, self.inventoryCallback, (dict, quotamgr), 0)
		for cname, dct in dict.items():
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
		t = None
		info = None
		self.debug('readItem(%s)...' % (lpath,))

		try:
			fpath = self.fullPath(lpath)
			st = os.stat(fpath)
			if stat.S_ISDIR(st[stat.ST_MODE]):	t = 'd'
		except:
			# it's a file
			t = 'f'
			#self.debug('readItem: stat(%s): %s %s' % (fpath, sys.exc_type, sys.exc_value))
			pass
			
		self.debug('readItem: type for <%s> is %s' % (lpath, t))
		if t == 'f':
			try:	
				db = bsddb.btopen(self.dirIndexPath(self.parentPath(lpath)), 'r')
				str = db[self.fileName(lpath)]
			except:
				self.debug('readItem: db key for %s not found' % (lpath,))
				return None, None
			info = VFSFileInfo(lpath, str)
			db.close()
		elif t == 'd':
			#print 'readItem: opening %s' % self.dirIndexPath(lpath)
			try:	
				db = bsddb.btopen(self.dirIndexPath(lpath), 'r')
				str = db[self.__DirInfoKey]
			except:
				return None, None
			info = VFSDirInfo(lpath, str)
			db.close()
		return t, info		

	def writeItem(self, lpath, data):
		typ, info = data
		if info.Type == 'f':
			db = bsddb.btopen(self.dirIndexPath(self.parentPath(lpath)),'c')
			db[self.fileName(lpath)] = info.serialize()
			db.sync()
			db.close()
		else:	# assume 'd'
			db = bsddb.btopen(self.dirIndexPath(lpath),'c')
			db[self.__DirInfoKey] = info.serialize()
			db.sync()
			db.close()
				
	def getType(self, lpath):
		t, info = self.Cache[lpath]
		self.debug('getType(%s) -> %s' % (lpath, t))
		return t

	def getFileInfo(self, lpath):
		t, info = self.Cache[lpath]
		if t != 'f':	return None
		return info

	def getDirInfo(self, lpath):
		t, info = self.Cache[lpath]
		if t != 'd':	return None
		return info

	def getInfo(self, lpath):
		t, info = self.Cache[lpath]
		return info
	
	def storeFileInfo(self, info):
		self.Cache[info.Path] = ('f',info)
	
	def storeDirInfo(self, info):
		self.Cache[info.Path] = ('d',info)

	def addServer(self, info, cellname):
		if info.Type != 'f':	return
		if not info.isStoredOn(cellname):
			info.addServer(cellname)
			self.storeFileInfo(info)
			inx = self.CellInxDB[cellname]
			if not inx.hasFile(info.Path, info.CTime):
				inx.addFile(info.Path, info.CTime)
				self.CellInxDB[cellname] = inx

	def removeServer(self, info, cellname):
		if info.Type != 'f':	return
		if info.isStoredOn(cellname):
			info.removeServer(cellname)
			self.storeFileInfo(info)
			inx = self.CellInxDB[cellname]
			if inx.hasFile(info.Path):
				inx.removeFile(info.Path)
				self.CellInxDB[cellname] = inx
	
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
		try:	self.storeFileInfo(info)
		except:
			return 0, 'Error: %s %s' % (sys.exc_type, sys.exc_value)
		return 1, 'OK'
		
	def mkdir(self, info):
		lpath = info.Path
		fn = self.fileName(lpath)
		if not fn or fn[0] == '.':
			return 0, 'Invalid directory name'
		if self.exists(lpath):
			return 0, 'Already exists'
		if not self.isDir(self.parentPath(lpath)):
			return 0, 'Parent directory not found'
		fpath = self.fullPath(lpath)
		try:	os.mkdir(fpath)
		except:
			return 0, 'Error: %s %s' % (sys.exc_type, sys.exc_value)
		try:	self.storeDirInfo(info)
		except:
			return 0, 'Error: %s %s' % (sys.exc_type, sys.exc_value)
		return 1, 'OK'

	def rmfile(self, lpath):
		if not self.exists(lpath):
			return 1, 'Already deleted'
		if not self.isFile(lpath):
			return 0, 'Is not a file'
		info = self.getFileInfo(lpath)
		try:
			db = bsddb.btopen(self.dirIndexPath(self.parentPath(lpath)), 'c')
			del db[self.fileName(lpath)]
			db.sync()
			db.close()
		except: return 0, 'Error: %s %s' % (sys.exc_type, sys.exc_value)
		vfssrv_global.G_CellIF.delFile(lpath, info.Servers)
		del self.Cache[lpath]
		return 1, 'OK'

	def rmdir(self, lpath):
		try:	
			os.remove(self.dirIndexPath(lpath))
		except:
			return 0, 'Error removing directory index file: %s %s' % (sys.exc_type, sys.exc_value)

		try:	
			os.rmdir(self.fullPath(lpath))
		except os.error, val:
			if val.errno == errno.EEXIST:
				return 0, 'Directory not empty'
			else:
				return 0, val.strerror		
		except: 
			return 0, 'Error removing the directory: %s %s' % (sys.exc_type, sys.exc_value)
		del self.Cache[lpath]
		return 1, 'OK'

	def idle(self):
		if time.time() > self.LastFlush + self.FlushInterval:
			self.flush()
			self.LastFlush = time.time()

	def flush(self):
		self.Cache.flush()
		self.CellInxDB.flush()
	
