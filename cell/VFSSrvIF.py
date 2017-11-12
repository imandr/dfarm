#
# @(#) $Id: VFSSrvIF.py,v 1.9 2002/08/12 16:29:43 ivm Exp $
#
# Cell Manager interface to VFS server
#
# $Log: VFSSrvIF.py,v $
# Revision 1.9  2002/08/12 16:29:43  ivm
# Implemented cell indeces
# Kerberized ftpd
#
# Revision 1.8  2002/07/16 18:44:40  ivm
# Implemented data attractions
# v2_1
#
# Revision 1.6  2002/04/30 20:07:15  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.5  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.4  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.3  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.2  2001/04/12 16:02:31  ivm
# Fixed Makefiles
# Fixed for fcslib 2.0
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

import cellmgr_global
from socket import *
from SockStream import SockStream
import string
import time
import sys
import whrandom

class	VFSSrvIF:
	def __init__(self, myid, cfg, sel):
		self.ID = myid
		self.Sel = sel
		self.DSAddr = \
			(cfg.getValue('vfssrv','*','host'),
			 cfg.getValue('vfssrv','*','cellif_port'))
		self.Connected = 0
		self.Reconciled = 0
		self.LastIdle = 0
		self.NextReconnect = 0
		self.NextProbeTime = 0
		self.connect()
	
	def log(self, msg):
		msg = 'VFSSrvIF: %s' % (msg,)
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()
				
	def connect(self):
		self.Sock = socket(AF_INET, SOCK_STREAM)
		try:	self.Sock.connect(self.DSAddr)
		except: 
			self.log('can not connect to VFS Server')
			return
		self.Str = SockStream(self.Sock)
		ans = self.Str.sendAndRecv('HELLO %s' % self.ID)
		self.log('connect: HELLO -> %s' % ans)
		if ans == 'HOLD':
			cellmgr_global.CellStorage.hold()
		elif ans != 'OK':
			if ans == 'EXIT':
				self.log('Shot down by VFS Server')
				sys.exit(3)
			self.disconnect()
			return
		self.Sel.register(self, rd=self.Sock.fileno())
		self.Connected = 1
		self.reconcile()
		self.Reconciled = 1
	
	def reconcile(self):
		if not self.Connected:	return
		for lp, info in cellmgr_global.CellStorage.listFiles():
			#self.log('reconcile: %s %s' % (lp, info))
			if info:
				sizestr = '%s' % info.Size
				if sizestr[-1] == 'L':
					sizestr = sizestr[:-1]
				ct = info.CTime
				self.log('reconcile: sending IHAVE %s %s %s' % (lp, ct, sizestr))
				self.Str.send('IHAVE %s %s %s' % (lp, ct, sizestr))
		self.Str.send('SYNC')
		
	def doRead(self, fd, sel):
		if fd != self.Sock.fileno():
			return
		self.Str.readMore()
		while self.Str and self.Str.msgReady() and not self.Str.eof():
			msg = self.Str.getMsg()
			self.log('doRead: msg:<%s>' % msg)
			if not msg: continue
			words = string.split(msg)
			if not words:	continue
			if words[0] == 'SYNC':
				self.Reconciled = 1
				self.log('reconciled')
				continue
			elif words[0] == 'DEL':
				if not words[1:]:
					self.disconnect()
				lp = words[1]
				cellmgr_global.CellStorage.delFile(lp)
				continue
			elif words[0] == 'HOLD':
				self.doHold()
				continue
			elif words[0] == 'RELEASE':
				self.doRelease()
				continue
			elif words[0] == 'REPLICATE':
				self.doReplicate(words[1:])
				continue
		if not self.Str or self.Str.eof():
			self.disconnect()
			
	def doHold(self):
		cellmgr_global.CellStorage.hold()
		
	def doRelease(self):
		cellmgr_global.CellStorage.release()

	def doReplicate(self, args):
		# args: (<path>|*) <nfrep>
		if len(args) < 2:	return
		path = args[0]
		mult = int(args[1])
		if path == '*':
			cellmgr_global.CellStorage.replicateAll(mult)
		else:
			cellmgr_global.CellStorage.replicateFile(path, mult)
			
		
	def disconnect(self):
		self.Reconciled = 0
		if self.Str:
			self.Sel.unregister(self.Str.fileno())
			self.Sock.close()
			self.Sock = None
			self.Str = None
		self.Connected = 0
		
	def reconnect(self):
		if self.Connected:	return
		if time.time() < self.NextReconnect:	return
		self.connect()
		self.NextReconnect = time.time() + whrandom.randint(5,20)

	def probe(self):
		if not self.Connected or time.time() < self.NextProbeTime:	return
		self.Str.probe()
		self.NextProbeTime = time.time() + 300

	def sendIHave(self, lpath, info):
		if self.Connected:
			sizestr = '%s' % info.Size
			if sizestr[-1] == 'L':
				sizestr = sizestr[:-1]
			self.Str.send('IHAVE %s %s %s' % (lpath, info.CTime, sizestr))
			

	def idle(self):
		self.reconnect()
		self.probe()
