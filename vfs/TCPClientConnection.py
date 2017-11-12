#
# @(#) $Id: TCPClientConnection.py,v 1.3 2003/11/25 20:37:02 ivm Exp $
#
# $Log: TCPClientConnection.py,v $
# Revision 1.3  2003/11/25 20:37:02  ivm
# Implemented write-back cache
#
# Revision 1.2  2001/05/22 13:27:20  ivm
# Fixed some bugs
# Implemented non-blocking send in Replicator
# Implemented ACCEPT Remote
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

from SockStream import *
import sys

class	TCPClientConnection:
	def __init__(self, sock, addr, sel):
		self.Str = SockStream(sock)
		self.Sock = sock
		self.Sel = sel
		self.Sel.register(self, rd=self.Str.fileno())
		self.Disconnected = 0
			
	def doRead(self, fd, sel):
		if fd != self.Str.fileno():
			return
		self.Str.readMore()
		while not self.Disconnected and self.Str.msgReady():
			msg = self.Str.getMsg()
			if not msg: continue
			words = string.split(msg)
			if not words:	continue
			#print words
			ans = self.processMsg(words[0], words[1:], msg)
			#print 'ans=<%s>' % ans
			if ans != None and self.Str !=  None:
				self.Str.send(ans)
		if not self.Disconnected and self.Str.eof():
			self.disconnect()
			
	def disconnect(self, msg = None):
		if not self.Disconnected:
			if msg:
				self.send(msg)
			self.Disconnected = 1
			self.Sel.unregisterFD(self.Sock.fileno())
			self.Sock.close()
			self.Str = None
			self.eof()

	def processMsg(self, cmd, args, msg):
		return None
		
	def send(self, msg):
		if not self.Disconnected:
			self.Str.send(msg)

	def sendAndRecv(self, msg):
		if not self.Disconnected:
			return self.Str.sendAndRecv(msg)
		else:
			return None

	def eof(self):
		# overridable
		pass
