#
# @(#) $Id: Replicator.py,v 1.14 2002/07/26 19:09:09 ivm Exp $
#
# $Log: Replicator.py,v $
# Revision 1.14  2002/07/26 19:09:09  ivm
# Bi-directional EOF confirmation. Tested.
#
# Revision 1.13  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.12  2001/06/27 14:27:36  ivm
# Introduced farm_name parameter
#
# Revision 1.11  2001/06/18 18:05:52  ivm
# Implemented disconnect-on-time-out in SockRcvr
#
# Revision 1.10  2001/06/15 22:12:25  ivm
# Fixed bug with replication stall
#
# Revision 1.9  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.8  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.7  2001/05/26 15:31:09  ivm
# Improved cell stat
#
# Revision 1.6  2001/05/22 13:27:19  ivm
# Fixed some bugs
# Implemented non-blocking send in Replicator
# Implemented ACCEPT Remote
#
# Revision 1.5  2001/04/12 16:02:31  ivm
# Fixed Makefiles
# Fixed for fcslib 2.0
#
# Revision 1.4  2001/04/04 20:19:03  ivm
# Get replication working
#
# Revision 1.3  2001/04/04 18:05:58  ivm
# *** empty log message ***
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

from SockStream import SockStream
import select
import socket
import errno
import sys
import time
import os
import cellmgr_global
import whrandom

class	Replicator:
	def __init__(self, cfg, fn, lpath, info, nrep, sel):
		# constructor is incomplete.
		# init() must be called
		self.LocalFN = fn
		self.FD = None
		self.LogPath = lpath
		self.FileInfo = info
		self.Sel = sel
		self.BCAddr = (cfg.getValue('cell','*','broadcast'),
			cfg.getValue('cell','*','listen_port'))
		self.FarmName = cfg.getValue('cell','*','farm_name','*')
		self.CtlSrvSock = None
		self.MoverCtlSock = None
		self.TxSock = None
		self.NRep = nrep
		self.Host = socket.gethostbyname(socket.gethostname())
		self.Initialized = 0
		self.Connected = 0
		self.Done = 0
		self.Failed = 0
		self.FailureReason = ''
		self.RetryAfter = time.time() + 0
		self.Retry = 1
		self.DataBuf = ''
		self.MinBuf = 10000	
		self.AcceptCancelEvent = None
		self.DataSrvSock = None
		self.ResendBcastEvent = None

	def __str__(self):
		return '<Replicator[%s -> %s *%s]>' %\
			(self.LocalFN, self.LogPath, self.NRep)
			
	def log(self, msg):
		msg = 'Replicator[%s -> %s *%s]: %s' % \
			(self.LocalFN, self.LogPath, self.NRep, msg)
		if cellmgr_global.LogFile:
			cellmgr_global.LogFile.log(msg)
		else:
			print msg
			sys.stdout.flush()

	def init(self):
		try:	os.stat(self.LocalFN)
		except:
			# File has been deleted already
			self.abort('File deleted')
			self.Retry = 0
			return			
		self.CtlSrvSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.CtlSrvSock.bind(('',0))
		ctl_port = self.CtlSrvSock.getsockname()[1]
		self.CtlSrvSock.listen(1)
		self.MoverCtlSock = None
		self.Sel.register(self, rd=self.CtlSrvSock.fileno())

		usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		usock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		bcast = 'ACCEPTR %s %s %s %s %s %s' % (self.FarmName, 
			self.NRep - 1, self.LogPath, 
			self.Host, ctl_port, self.FileInfo.serialize())
		usock.sendto(bcast, self.BCAddr)
		usock.close()
		
		self.Initialized = 1
		self.Done = 0
		self.Failed = 0
		self.FailureReason = ''
		self.log('initialized, ctl sock at %s %s' % (self.Host, ctl_port))

		self.AcceptCtlTmoEvent = cellmgr_global.Timer.addEvent(
			time.time() + 60, 0, 1, self.cancelCtlAccept, None)
		self.AcceptDataTmoEvent = None
		self.ResendBcastEvent = cellmgr_global.Timer.addEvent(
			time.time() + 9, 9, 5, self.resendBcast, bcast)

	def resendBcast(self, t, bcast):
		usock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		usock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
		usock.sendto(bcast, self.BCAddr)
		#self.log('Broadcast resent')
		usock.close()
	
	def cancelCtlAccept(self, t, arg):
		self.abort('Broadcast time-out')
		self.AcceptCtlTmoEvent = None

	def isInProgress(self):
		return self.Initialized and not self.Done and not self.Failed

	def doRead(self, fd, sel):
		if self.CtlSrvSock and fd == self.CtlSrvSock.fileno():
			self.acceptCtlConn()
		elif self.MoverCtlSock and fd == self.MoverCtlSock.fileno():
			self.doMoverCtlRead()
		elif self.DataSrvSock and fd == self.DataSrvSock.fileno():
			self.acceptDataConn()

	def doWrite(self, fd, sel):
		if self.TxSock and fd == self.TxSock.fileno():
			if len(self.DataBuf) < self.MinBuf and self.FD:
				data = self.FD.read(self.MinBuf*3)
				if not data:
					self.FD.close()
					self.FD = None
				self.DataBuf = self.DataBuf + data
			if not self.DataBuf:
				self.MoverStr.send('EOF')
				sel.register(self, rd=self.MoverCtlSock.fileno())
				sel.unregister(wr=self.TxSock.fileno())
				self.TxSock.close()
				self.TxSock = None
				#self.log('End of data file')
			else:
				n = 0
				try:	n = self.TxSock.send(self.DataBuf)
				except socket.error, val:
					errn, msg = val.args
					if errn == errno.EWOULDBLOCK:
						pass	#ignore
					else:
						self.abort('Socket error %s' % val)
				except: self.abort('Mover closed data connection')
				else:
					if n > 0:
						self.DataBuf = self.DataBuf[n:]

	def doMoverCtlRead(self):
		self.MoverStr.readMore()
		while not self.MoverStr.eof() and \
					self.MoverStr.msgReady():
			msg = self.MoverStr.getMsg()
			#self.log('doMoverCtlRead: msg: <%s>' % msg)
			if not msg:
				#self.abort('Mover closed connection')
				return
			elif msg == 'EOF':
				self.Done = 1
				self.Sel.unregister(rd=self.MoverCtlSock.fileno())
				self.MoverCtlSock.close()
				self.MoverCtlSock = None
				self.log('Replication complete')
				return
		if self.MoverStr.eof():
			self.abort('Mover closed control connection')
				

	def acceptCtlConn(self):
		# accept connection on the data socket
		if self.AcceptCtlTmoEvent:
			cellmgr_global.Timer.removeEvent(self.AcceptCtlTmoEvent)
			self.AcceptCtlTmoEvent = None
		if self.ResendBcastEvent:
			cellmgr_global.Timer.removeEvent(self.ResendBcastEvent)
			self.ResendBcastEvent = None
			
		self.Sel.unregister(rd=self.CtlSrvSock.fileno())
		try:	self.MoverCtlSock, addr = self.CtlSrvSock.accept()
		except:
			self.abort('Accept on ctl socket failed')
			return
		self.CtlSrvSock.close()
		self.CtlSrvSock = None
		self.MoverStr = SockStream(self.MoverCtlSock)
		msg = self.MoverStr.recv()
		#self.log('acceptCtlConn: addr: <%s> msg: <%s>' % (addr,msg))
		if msg == 'SEND':
			# initiate ransfer
			data_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			data_srv.bind((socket.gethostname(),0))
			data_port = data_srv.getsockname()[1]
			data_srv.listen(1)
			self.MoverStr.send('RCVFROM %s %s' % (self.Host, data_port))
			self.Sel.register(self, rd=data_srv.fileno())
			self.DataSrvSock = data_srv
			self.AcceptDataTmoEvent = cellmgr_global.Timer.addEvent(
				time.time() + 10, 0, 1, self.cancelDataAccept, None)
		else:
			self.abort('Transfer initiation protocol error: <%s>' % msg)


	
	def cancelDataAccept(self, t, arg):
		self.abort('Data port accept time-out')
		self.AcceptDataTmoEvent = None

	def acceptDataConn(self):
		if self.AcceptDataTmoEvent:
			cellmgr_global.Timer.removeEvent(self.AcceptDataTmoEvent)
			self.AcceptDataTmoEvent = None
		self.Sel.unregister(rd=self.DataSrvSock.fileno())
		try:	self.TxSock, addr = self.DataSrvSock.accept()
		except:
			self.abort('Accept on data socket failed')
			return
		self.DataSrvSock.close()
		self.DataSrvSock = None
		try:	self.FD = open(self.LocalFN, 'r')
		except:
			# file disappeared
			self.abort('File deleted')
			self.Retry = 0
		else:
			self.TxSock.setblocking(0)
			self.Sel.register(self, wr=self.TxSock.fileno())		
			self.Connected = 1

	def abort(self, msg):
		self.log('aborting: %s' % msg)
		self.Failed = 1
		self.FailureReason = msg
		self.RetryAfter = time.time() + 5
		if self.TxSock:
			self.Sel.unregister(wr=self.TxSock.fileno())
			self.TxSock.close()
			self.TxSock = None
		if self.CtlSrvSock:
			self.Sel.unregister(rd=self.CtlSrvSock.fileno())
			self.CtlSrvSock.close()
			self.CtlSrvSock = None
		if self.DataSrvSock:
			self.Sel.unregister(rd=self.DataSrvSock.fileno())
			self.DataSrvSock.close()
			self.DataSrvSock = None
		if self.MoverCtlSock:
			self.Sel.unregister(rd=self.MoverCtlSock.fileno())
			self.MoverCtlSock.close()
			self.MoverCtlSock = None
		if self.FD:
			self.FD.close()
			self.FD = None
