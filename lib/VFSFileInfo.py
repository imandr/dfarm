#
# @(#) $Id: VFSFileInfo.py,v 1.7 2002/08/16 19:18:28 ivm Exp $
#
# $Log: VFSFileInfo.py,v $
# Revision 1.7  2002/08/16 19:18:28  ivm
# Implemented size estimates for ftpd
#
# Revision 1.6  2002/07/16 18:44:40  ivm
# Implemented data attractions
# v2_1
#
# Revision 1.5  2002/05/07 23:02:35  ivm
# Implemented attributes and info -0
#
# Revision 1.4  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.3  2001/05/26 15:31:09  ivm
# Improved cell stat
#
# Revision 1.2  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

import time
import serialize

def VFSCanonicPath(path):
	if not path:
		path = '/'
	if path[0] != '/':
		path = '/' + path
	while '//' in path:
		path = path.replace('//', '/')
	if path != "/" and path.endswith('/'):
		path = path[:-1]
	return path		

class	VFSItemInfo:
	Version = '2.1'
	def __init__(self, path, typ = None, str = None):
		self.Type = typ
		self.Path = VFSCanonicPath(path)
		self.Username = None
		self.Prot = 'rwr-'
		self.Attrs = {}
		self.Flags = 0
		if str != None:
			self.deserialize(str)

	def __getitem__(self, attr):
		try:				return	self.Attrs[attr]
		except KeyError:	return None

	def __setitem__(self, attr, val):
		self.Attrs[attr] = val

	def __delitem__(self, attr):
		try:	del self.Attrs[attr]
		except KeyError:	pass
	
	def attributes(self):
		return list(self.Attrs.keys())

	def dataClass(self):
		return self['__data_class'] or '*'

class	VFSFileInfo(VFSItemInfo):
	FLAG_ESTIMATE_SIZE = 1

	def __init__(self, path, str = None):
		self.CTime = int(time.time())
		self.Servers = []
		self.Size = None
		VFSItemInfo.__init__(self, path, 'f', str)

	def sizeMB(self):
		if self.Size and self.Size > 0:
			return int(float(self.Size)/1024/1024 + 0.5)
		else:
			return 0

	def sizeEstimated(self):
		return (self.Flags & self.FLAG_ESTIMATE_SIZE) != 0

	def setActualSize(self, size):
		self.Flags = self.Flags & ~self.FLAG_ESTIMATE_SIZE
		self.Size = int(size)
	
	def setSizeEstimate(self, size):
		self.Size = int(size)
		self.Flags = self.Flags | self.FLAG_ESTIMATE_SIZE
	
	def isStoredOn(self, srv):
		return srv in self.Servers
		
	def addServer(self, srv):
		if not srv in self.Servers:
			self.Servers.append(srv)

	def removeServer(self, srv):
		while srv in self.Servers:
			self.Servers.remove(srv)
	
	def mult(self):
		return len(self.Servers)
		
	def isStored(self):
		return self.mult() > 0

	def serialize(self, short=0):
		ct = self.CTime
		if type(ct) == type(1.0):
			ct = int(ct)
		dict = {}
		for n in ['CTime','Username','Size','Prot','Flags']:
			dict[n] = self.__dict__[n]
		dict['CTime'] = ct
		if not short:
			for n in ['Servers','Attrs']:
				dict[n] = self.__dict__[n]
		return '[%s] %s' % (self.Version, serialize.serialize(dict))
		
	def deserialize(self, str):
		words = str.split()
		if not words:	return
		if words[0][0] == '[':
			v = words[0][1:-1]
			words = words[1:]
		else:
			v = '0'
		if v == '0':
			try:	ct = int(words[0])
			except: ct = None
			self.CTime = ct
			self.Username = words[1]
			self.Size = int(eval(words[2]))
			self.Servers = words[3:]
		elif v == '1':
			try:	ct = int(words[0])
			except: ct = None
			self.CTime = ct
			self.Username = words[1]
			self.Size = int(eval(words[2]))
			self.Prot = words[3]
			self.Servers = words[4:]
		else:	# v >= '2'
			info = str.split(None,1)[1]
			dict, rest = serialize.deserialize(info)
			for n in ['CTime','Username','Size','Prot','Servers',
						'Flags','Attrs']:
				if n in dict:
					self.__dict__[n] = dict[n]
			
		
class	VFSDirInfo(VFSItemInfo):
	def __init__(self, path, str = None):
		VFSItemInfo.__init__(self, path, 'd', str)
		
	def serialize(self):
		dict = {}
		for n in ['Username','Prot','Attrs','Flags']:
			dict[n] = self.__dict__[n]
		return '[%s] %s' % (self.Version, serialize.serialize(dict))

	def deserialize(self, str):
		lst = str.split()
		if len(lst) >= 2:
			if lst[0][0] == '[':
				v = lst[0][1:-1]
				lst = lst[1:]
			else:
				v = '0'
			if v < '2':
				self.Username = lst[0]
				self.Prot = lst[1]
			else:	# >= 2
				info = str.split(None, 1)[1]
				dict, rest = serialize.deserialize(info)
				for n in ['Username','Prot','Attrs','Flags']:
					if n in dict:
						self.__dict__[n] = dict[n]

