#
# @(#) $Id: DataMover.py,v 1.17 2003/12/04 16:52:28 ivm Exp $
#
# $Log: DataMover.py,v $
# Revision 1.17  2003/12/04 16:52:28  ivm
# Implemented BSD DB - based VFS DB
# Use connect with time-out for data communication
#
# Revision 1.16  2002/10/31 17:52:33  ivm
# v2_3
#
# Revision 1.15  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.14  2002/08/23 18:11:36  ivm
# Implemented Kerberos authorization
#
# Revision 1.13  2002/07/26 19:09:09  ivm
# Bi-directional EOF confirmation. Tested.
#
# Revision 1.12  2002/07/09 18:48:11  ivm
# Implemented purging of empty directories in PSA
# Implemented probing of VFS Server by Cell Managers
#
# Revision 1.11  2002/04/30 20:07:15  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.10  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.9  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.8  2001/06/18 18:05:52  ivm
# Implemented disconnect-on-time-out in SockRcvr
#
# Revision 1.7  2001/05/26 15:31:09  ivm
# Improved cell stat
#
# Revision 1.6  2001/05/23 19:52:50  ivm
# Use 127.0.0.1 for local uploads
#
# Revision 1.4  2001/04/12 16:02:31  ivm
# Fixed Makefiles
# Fixed for fcslib 2.0
#
# Revision 1.3  2001/04/04 20:19:03  ivm
# Get replication working
#
# Revision 1.2  2001/04/04 18:05:57  ivm
# *** empty log message ***
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

from txns import *
from SockStream import SockStream
from socket import *
from Replicator import Replicator
import sys
import string
import os
import time
import cellmgr_global
import select

def connectSocket(addr, tmo = -1):
        # returns either connected socket or None on timeout
        # -1 means infinite
        s = socket(AF_INET, SOCK_STREAM)        # create a socket
        if tmo < 0:
                s.connect(addr)         # wait forever
                return s

        s.setblocking(0)
        if s.connect_ex(addr) == 0:
                s.setblocking(1)        # done immediately
                return s
        #print 'selecting...'
        r,w,x = select.select([], [s], [], tmo)
        if not s in w:
                # timed out
                s.close()
                raise IOError, 'timeout'
        if s.connect_ex(addr) == 0:
                s.setblocking(1)
                return s
        try:    s.getpeername()
        except:
                # connection refused
                s.close()
                raise IOError, 'connection refused'
        s.setblocking(1)
        return s



class	SocketRcvr:
	def __init__(self, txn, caddr, sel, delay):
		self.Txn = txn
		self.CAddr = caddr
		self.Sel = sel
		self.DAddr = None
		self.EofReceived = 0
		self.DataConnected = 0
		self.TheFile = None
		self.CSock = None
		self.DSock = None
		self.Str = None
		self.TimeoutEvent = None
		if delay > 0.0:
			self.ActivateEvent = cellmgr_global.Timer.addEvent(
				time.time() + delay,
				0, 1, self.activate, None)
		else:
			self.activate(time.time(), None)
		
	def activate(self, t, arg):				
		try:	self.CSock = connectSocket(self.CAddr, 5)
		except:
			self.log('ctl connect: %s %s' % (sys.exc_type, sys.exc_value))
			self.Txn.rollback()
			return
		self.log('ctl connected')
		self.Str = SockStream(self.CSock)
		self.Str.send('SEND')
		self.DSock = socket(AF_INET, SOCK_STREAM)
		self.Sel.register(self, rd=self.CSock.fileno())
		self.TimeoutEvent = cellmgr_global.Timer.addEvent(time.time() + 600,
			0, 1, self.timeoutAbort, None)

	def log(self, msg):
		msg = 'SockRcvr[t=%s, lp=%s, c=%s, d=%s]: %s' % (
			self.Txn.ID, self.Txn.LPath, self.CAddr, self.DAddr, msg)
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()
		
	def doRead(self, fd, sel):
		if fd == self.CSock.fileno():
			self.Str.readMore()
			while self.Str.msgReady() and not self.Str.eof():
				msg = self.Str.getMsg()
				if not msg: continue
				words = string.split(msg)
				if not words:	continue
				if words[0] == 'RCVFROM':
					if len(words) < 3:
						self.abort()
						break
					addr = (words[1], string.atoi(words[2]))
					self.DAddr = addr
					try:	self.DSock.connect(addr)
					except:
						self.abort()
						break
					#self.log('data connected')
					self.Sel.register(self, rd=self.DSock.fileno())
					self.DataConnected = 1
				elif words[0] == 'EOF':
					#self.log('EOF received')
					#self.Str.send('OK')
					self.EofReceived = 1
			if not self.Str or self.Str.eof():
				self.ctlClosed()
		elif fd == self.DSock.fileno():
			self.TimeoutEvent.NextT = time.time() + 600
			try:	data = self.DSock.recv(100000)
			except: data = ''
			if not data:
				self.dataClosed()
				return
			if self.TheFile == None:
					self.TheFile = open(self.Txn.dataPath(),'w')
			try:	self.TheFile.write(data)
			except: self.abort()			
			
	def ctlClosed(self):
		#self.log('ctl closed')
		if self.CSock == None:	return
		self.Sel.unregister(rd=self.CSock.fileno())
		if not self.EofReceived:
			self.abort()
		elif not self.DataConnected:
			self.commit()
			
	def dataClosed(self):
		#self.log('data closed')
		#if not self.EofReceived:
		#	self.abort()
		#else:
		#	self.commit()
		if self.Str:
			self.Str.send('EOF')
		self.Sel.unregister(rd=self.DSock.fileno())
		if self.EofReceived:
			self.commit()
		self.DataConnected = 0
		
	def abort(self):
		if self.TheFile != None:
			self.TheFile.close()
			os.remove(self.Txn.dataPath())
		self.log('rollback')
		self.Txn.rollback()
		self.Sel.unregister(rd=self.CSock.fileno())
		self.Sel.unregister(rd=self.DSock.fileno())
		self.CSock.close()
		self.DSock.close()
		self.Str = None
		self.DSock = None
		self.CSock = None		
		if self.TimeoutEvent:
			cellmgr_global.Timer.removeEvent(self.TimeoutEvent)
			self.TimeoutEvent = None
		
	def commit(self):
		if self.TheFile != None:
			self.TheFile.close()
		self.Txn.commit()
		self.log('committed')
		self.Sel.unregister(rd=self.CSock.fileno())
		self.Sel.unregister(rd=self.DSock.fileno())
		self.Str.send('OK')
		self.CSock.close()
		self.DSock.close()
		self.Str = None
		self.DSock = None
		self.CSock = None		
		if self.TimeoutEvent:
			cellmgr_global.Timer.removeEvent(self.TimeoutEvent)
			self.TimeoutEvent = None

	def timeoutAbort(self, t, arg):
		self.log('time-out')
		self.abort()

class	LocalRcvr:
	def __init__(self, txn, caddr, sel, delay):
		self.Txn = txn
		self.SrcFile = None
		self.DstFile = None
		self.CSock = None
		self.CAddr = caddr
		self.Sel = sel
		self.Str = None
		if delay > 0.0:
			self.ActivateEvent = cellmgr_global.Timer.addEvent(
				time.time() + delay,
				0, 1, self.activate, None)
		else:
			self.activate(time.time(), None)
				
	def activate(self, t, arg):
		try:	self.CSock = connectSocket(self.CAddr, 5)
		except:
			self.log('ctl connect: %s %s' % (sys.exc_type, sys.exc_value))
			self.Txn.rollback()
			return
		self.log('ctl connected')
		self.Str = SockStream(self.CSock)
		self.Str.send('LOCAL')
		self.Sel.register(self, rd=self.CSock.fileno())
		self.ActivateEvent = None
		
	def log(self, msg):
		msg = 'LocalRcvr[t=%s, lp=%s, c=%s]: %s' % (
			self.Txn.ID, self.Txn.LPath, self.CAddr, msg)
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()
		
	def doRead(self, fd, sel):
		if fd == self.CSock.fileno():
			self.doReadCtl(sel)
		elif self.SrcFile != None and fd == self.SrcFile.fileno():
			self.doReadData(sel)
			
	def doReadCtl(self, sel):
		self.Str.readMore()
		while self.Str.msgReady() and not self.Str.eof():
			msg = self.Str.getMsg()
			if not msg: continue
			words = string.split(msg)
			if not words:	continue
			if words[0] == 'COPY':
				if len(words) < 2:
					self.abort()
					break
				path = words[1]
				try:	self.SrcFile = open(path, 'r')
				except:
					self.abort('ERR Can not open source file')
					break
				try:	self.DstFile = open(self.Txn.dataPath(), 'w')
				except:
					self.abort('ERR Can not open destination file')
					break
				sel.register(self, rd=self.SrcFile.fileno())

	def doReadData(self, sel):
		data = self.SrcFile.read(1000000)
		if not data:
			# EOF
			self.commit('OK')
		else:
			try:	self.DstFile.write(data)
			except: self.abort('Error writing to disk')
			
	def abort(self, msg = None):
		self.log('rollback: %s' % msg)
		if self.DstFile:
			self.DstFile.close()
			os.remove(self.Txn.dataPath())			
		self.Txn.rollback()
		self.Sel.unregister(rd=self.CSock.fileno())
		if self.SrcFile:
			self.Sel.unregister(rd=self.SrcFile.fileno())
			self.SrcFile.close()
		if msg:
			self.Str.send(msg)
		self.CSock.close()
		self.Str = None
				
	def commit(self, msg = None):
		self.log('commit: %s' % msg)
		if self.DstFile:
			self.DstFile.close()
		self.Txn.commit()
		self.Sel.unregister(rd=self.CSock.fileno())
		if self.SrcFile:
			self.Sel.unregister(rd=self.SrcFile.fileno())
			self.SrcFile.close()
		if msg:
			self.Str.send(msg)
		self.CSock.close()
		self.Str = None
			
class	SocketSndr:
	def __init__(self, txn, caddr, sel, delay):
		self.Txn = txn
		self.CSock = None
		self.CAddr = caddr
		self.DAddr = None
		self.Str = None
		self.Sel = sel
		self.TheFile = None
		self.DSock = None
		if delay > 0.0:
			self.ActivateEvent = cellmgr_global.Timer.addEvent(
				time.time() + delay,
				0, 1, self.activate, None)
		else:
			self.activate(time.time(), None)
		
	def activate(self, t, arg):
		self.CSock = socket(AF_INET, SOCK_STREAM)
		try:	self.CSock.connect(self.CAddr)
		except:
			self.log('ctl connect: %s %s' % (sys.exc_type, sys.exc_value))
			self.Txn.rollback()
			return
		self.log('ctl connected')
		self.Str = SockStream(self.CSock)
		self.Str.send('RECV')
		self.DSock = socket(AF_INET, SOCK_STREAM)
		self.Sel.register(self, rd=self.CSock.fileno())
		self.TheFile = open(self.Txn.dataPath(),'r')

	def log(self, msg):
		msg = 'SockSndr[t=%s, lp=%s, c=%s, d=%s]: %s' % (
			self.Txn.ID, self.Txn.LPath, self.CAddr, self.DAddr, msg)
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()
		
	def doRead(self, fd, sel):
		if fd != self.CSock.fileno():
			return
		self.Str.readMore()
		while self.Str.msgReady() and not self.Str.eof():
			msg = self.Str.getMsg()
			if not msg: continue
			words = string.split(msg)
			if not words:	continue
			if words[0] == 'SENDTO':
				if len(words) < 3:
					self.abort()
					break
				addr = (words[1], string.atoi(words[2]))
				self.DAddr = addr
				try:	self.DSock.connect(addr)
				except:
					self.abort()
					break
				self.log('data connected')
				sel.register(self, wr=self.DSock.fileno())
		if self.Str.eof():
			self.abort()
			
	def doWrite(self, fd, sel):
		if fd != self.DSock.fileno():
			return
		try:	data = self.TheFile.read(100000)
		except: self.abort()
		if not data:
			# end of file
			self.Str.send('EOF')
			self.commit()
			return
		try:	self.DSock.send(data)
		except: self.abort()

	def abort(self):
		self.log('abort')
		if self.TheFile:
			self.TheFile.close()
		self.Txn.rollback()
		self.Sel.unregister(rd=self.CSock.fileno())
		self.Sel.unregister(rd=self.DSock.fileno())
		self.Sel.unregister(wr=self.DSock.fileno())
		self.CSock.close()
		self.DSock.close()
		
	def commit(self):
		self.log('commit')
		self.TheFile.close()
		self.Txn.commit()
		self.Sel.unregister(rd=self.CSock.fileno())
		self.Sel.unregister(rd=self.DSock.fileno())
		self.Sel.unregister(wr=self.DSock.fileno())
		self.CSock.close()
		self.DSock.close()

class	LocalSndr:
	def __init__(self, txn, caddr, sel, delay):
		self.Txn = txn
		self.CSock = socket(AF_INET, SOCK_STREAM)
		self.CAddr = caddr
		self.Sel = sel
		self.Str = None
		if delay > 0.0:
			self.ActivateEvent = cellmgr_global.Timer.addEvent(
				time.time() + delay,
				0, 1, self.activate, None)
		else:
			self.activate(time.time(), None)
		
	def activate(self, t, arg):
		try:	self.CSock.connect(self.CAddr)
		except:
			self.log('ctl connect: %s %s' % (sys.exc_type, sys.exc_value))
			self.Txn.rollback()
			return
		self.log('ctl connected')
		self.Str = SockStream(self.CSock)
		self.Str.send('LOCAL %s' % self.Txn.dataPath())
		self.Sel.register(self, rd=self.CSock.fileno())

	def log(self, msg):
		msg = 'LocalSndr[t=%s, lp=%s, c=%s]: %s' % (
			self.Txn.ID, self.Txn.LPath, self.CAddr, msg)
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()
		
	def doRead(self, fd, sel):
		if fd != self.CSock.fileno():
			return
		self.Str.readMore()
		while self.Str.msgReady() and not self.Str.eof():
			msg = self.Str.getMsg()
			if msg != 'OK':
				continue
			self.commit()
		if self.Str.eof():
			self.abort()
					
	def abort(self):
		self.log('rollback')
		self.Txn.rollback()
		self.Sel.unregister(rd=self.CSock.fileno())
		self.CSock.close()
		
	def commit(self):
		self.log('commit')
		self.Txn.commit()
		self.Sel.unregister(rd=self.CSock.fileno())
		self.CSock.close()

class	FileHandle:
	def __init__(self, info, mode, txn, caddr, sel):
		self.Txn = txn
		self.CSock = socket(AF_INET, SOCK_DGRAM)
		self.CAddr = caddr
		self.CSock.sendto('OK')
		self.Sel = sel
		sel.register(self, rd=self.CSock.fileno())
		self.TimeoutEvent = cellmgr_global.Timer.addEvent(
			time.time() + 60, 0, 1, self.timeoutClose, None)
		self.File = open(self.Txn.dataPath(), 'r')
		self.LPath = info.Path
		self.Mode = mode
		self.Info = info
		self.Id = '%s:%s' % (info.CTime, self.LPath)

	def myId(self):
		return self.Id		

	def doRead(self, fd, sel):
		if fd != self.CSock.fileno():	return
		try:	msg, addr = self.CSock.recvfrom(10000)
		except:
			self.errorClose()
			return
		self.TimeoutEvent.NextT = time.time() + 60
		words = string.split(msg)
		if not words:	return
		cmd, args = words[0], words[1:]
		if cmd == 'READ':
			# READ <offset> <size>
			if len(args) < 2:	return
			off = long(args[0])
			size = int(args[1])
			size = min(size, 100*1024)
			self.File.seek(off)
			data = self.File.read(size)
			self.CSock.sendto(('%s:' % off) + data, self.CAddr)

	def timeoutClose(self):
		if self.TimeoutEvent:
			cellmgr_global.Timer.removeEvent(self.TimeoutEvent)
			self.TimeoutEvent = None
		self.Sel.unregister(rd=self.CSock.fileno())
		self.CSock.close()
		self.cellmgr_global.DataMover.closeHandle(self.myId())

	def errorClose(self):
		self.timeoutClose()

class	DataMover(HasTxns):
	def __init__(self, myid, cfg, sel):
		HasTxns.__init__(self)
		self.Sel = sel
		self.Cfg = cfg
		self.MyID = myid
		self.MaxGet = cfg.getValue('storage',self.MyID,'max_get',3)
		self.MaxPut = cfg.getValue('storage',self.MyID,'max_put',1)
		self.MaxRep = cfg.getValue('storage',self.MyID,'max_rep',2)
		self.MaxTxn = cfg.getValue('storage',self.MyID,'max_txn',5)
		self.Replicators = []
		self.Handles = {}

	def log(self, msg):
		msg = 'DataMover: %s' % msg
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()
		
	def canSend(self, lpath):
		if self.txnCount() >= self.MaxTxn or \
					self.txnCount('D') >= self.MaxGet:
			return 0
		for t in self.txnList():
			if t.type() == 'U' and t.LPath == lpath:
				return 0
		return 1			

	def canReceive(self, lpath):
		if self.txnCount() >= self.MaxTxn or \
					self.txnCount('U') >= self.MaxPut:
			return 0
		for t in self.txnList():
			if t.type() == 'U' and t.LPath == lpath:
				return 0
		return 1

	def canOpenFile(self, lpath, mode):
		if mode == 'r':
			return self.canSend(lpath)
		else:
			return self.canReceive(lpath)
		
	def sendSocket(self, txn, caddr, delay):
		txn.notify(self)
		#self.log('sendSocket: initiating socket send txn #%s to %s' %\
		#	(txn.ID, caddr))
		SocketSndr(txn, caddr, self.Sel, delay)

	def recvSocket(self, txn, caddr, delay):
		txn.notify(self)
		#self.log('recvSocket: initiating socket recv txn #%s from %s' %\
		#	(txn.ID, caddr))
		SocketRcvr(txn, caddr, self.Sel, delay)

	def sendLocal(self, txn, caddr, delay):
		txn.notify(self)
		#self.log('sendLocal: initiating local send txn #%s to %s' %\
		#	(txn.ID, caddr))
		LocalSndr(txn, caddr, self.Sel, delay)
		

	def recvLocal(self, txn, caddr, delay):
		txn.notify(self)
		#self.log('recvLocal: initiating local recv txn #%s from %s' %\
		#	(txn.ID, caddr))
		LocalRcvr(txn, caddr, self.Sel, delay)

	def openFile(self, txn, caddr, info, mode):
		txn.notify(self)
		FileHandle(info, mode, txn, caddr, self.Sel)
		
	def putTxns(self):
		#print 'putTxns: %d' % self.txnCount('U')
		return self.txnCount('U')
		
	def getTxns(self):
		#print 'getTxns: %d' % self.txnCount('D')
		return self.txnCount('D')
		
	def replicate(self, nfrep, lfn, lpath, info):
		if nfrep > 2:
			# make 2 replicators
			n1 = nfrep/2
			n2 = nfrep - n1
			r = Replicator(self.Cfg, lfn, lpath, info, n1, self.Sel)
			#self.log('replicator created: %s' % r)
			self.Replicators.append(r)
			r = Replicator(self.Cfg, lfn, lpath, info, n2, self.Sel)
			#self.log('replicator created: %s' % r)
			self.Replicators.append(r)
		elif nfrep > 0:
			r = Replicator(self.Cfg, lfn, lpath, info, nfrep, self.Sel)
			self.Replicators.append(r)
			#self.log('replicator created: %s' % r)

	def idle(self):
		nactive = 0
		for r in self.Replicators:
			if r.isInProgress():
				nactive = nactive + 1
		if nactive < self.MaxRep:
			for r in self.Replicators:
				if not r.isInProgress() and r.Retry and not r.Done:
					if time.time() > r.RetryAfter:
						r.init()
						nactive = nactive + 1
						if nactive >= self.MaxRep:
							break
		newlst = []
		for r in self.Replicators:
			if not r.Done and (r.isInProgress() or r.Retry):
				newlst.append(r)
		self.Replicators = newlst

	def statTxns(self):
		str = ''
		for t in self.txnList():
			if t.type() == 'U':
				str = str + 'WR * %s\n' % t.LPath
			elif t.type() == 'D':
				str = str + 'RD * %s\n' % t.LPath
		for r in self.Replicators:
			sts = '*'
			if not r.isInProgress():
				sts = 'I'
			str = str + 'RP %s %s\n' % (sts, r.LogPath)
		return str + '.\n'
