#
# @(#) $Id: ftpd.py,v 1.11 2003/06/06 17:31:37 ivm Exp $
#
# $Log: ftpd.py,v $
# Revision 1.11  2003/06/06 17:31:37  ivm
# Asynchronous processing of STOR and RETR in ftpd
#
# Revision 1.10  2003/03/26 17:22:31  ivm
# Improved absolute path calculation in ftpd
#
# Revision 1.9  2003/03/11 18:23:42  ivm
# Implemented passive mode
#
# Revision 1.8  2003/01/30 17:32:44  ivm
# Added defaults for user profiles
#
# Revision 1.7  2003/01/03 20:03:29  ivm
# Pass port number from start script
#
# Revision 1.5  2002/08/23 18:11:36  ivm
# Implemented Kerberos authorization
#
# Revision 1.4  2002/08/16 19:18:28  ivm
# Implemented size estimates for ftpd
#
# Revision 1.3  2002/08/12 16:29:43  ivm
# Implemented cell indeces
# Kerberized ftpd
#
# Revision 1.1  2002/07/30 20:27:19  ivm
# Added FTPD
#
#

from socket import *
from dfarm_api import DiskFarmClient
from TCPServer import TCPServer
from Selector import Selector
from SockStream import SockStream
from config import ConfigFile
import string
import time
import gss
import base64
import sys
import select
from LogFile import LogFile
from Timer import Timer

def long2str(x):
	str = '%s' % x
	if str[-1] == 'L':
		str = str[:-1]
	return str

def encode64(str):
	return string.join(
			string.split(base64.encodestring(str),'\n'),
			'')

def decode64(str):
	return base64.decodestring(str)

class	GFTPEDataChannel:
	EOD_BIT = 8
	EOF_BIT = 64

	def __init__(self, sock):
		self.Buf = ''
		self.EODReceived = 0
		self.EODSent = 0
		self.EOF = 0
		self.Sock = sock
		self.Offset = None
		self.Size = None
		self.Flags = 0
		self.BytesRcvd = 0L
		self.EODCReceived = 0
		self.NDC = None
		self.Header = ''
		self.Data = ''
		
	def get64(self, str8):
		#print 'get64: str8 = ', repr(str8)
		x = 0L
		for i in range(8):
			x = x * 256 + ord(str8[i])
		#print 'get64: %s -> %s' % (repr(str8), x)
		return x

	def put64(self, x):
		str = ''
		for i in range(8):
			str = chr(x % 256) + str
			x = x / 256
		return str

	def dataReady(self):
		return self.Size != None and len(self.Data) == self.Size

	def hdrReady(self):
		return len(self.Header) == 17

	def doRead(self):
		if self.EOF:	return
		if not self.hdrReady():
			data = self.Sock.recv(17 - len(self.Header))
			if not data:	
				self.EOF = 1
				return
			self.Header = self.Header + data
			if self.hdrReady():
				self.Flags = ord(self.Header[0])
				self.Size = int(self.get64(self.Header[1:9]))
				self.Offset = self.get64(self.Header[9:17])
				self.EODReceived = 1 and (self.Flags & self.EOD_BIT)
				if self.Flags and self.EOF_BIT:
					self.EODCReceived = 1
					self.NDC = self.Offset
			return
			
		if not self.dataReady():
			data = self.Sock.recv(self.Size - len(self.Data))
			if not data:	
				self.EOF = 1
				return
			self.Data = self.Data + data
	
	def getBlock(self):
		o, d = self.Offset, self.Data
		self.Offset = None
		self.Size = None
		self.Data = ''
		self.Header = ''
		return o, d

	def sendBlock(self, off, block, eod = 0):
		# make header
		#print 'sendBlock(%s, %d, %d) ...' % (off, len(block), eod)
		hdr = chr(0)
		if eod: 	
			hdr = chr(self.EOD_BIT)
			self.EODSent = 1
		hdr = hdr + self.put64(long(len(block))) + \
			self.put64(long(off))
		# send it
		self.Sock.send(hdr)
		self.Sock.send(block)
		#print 'sent'

	def sendEODC(self, ndc):
		# make header
		#print 'sendBlock(%s, %d, %d) ...' % (off, len(block), eod)
		hdr = chr(self.EOF_BIT) + self.put64(0L) + self.put64(long(ndc))
		# send it
		self.Sock.send(hdr)
		#print 'sent'

	def eof(self):
		return self.EOF
		
	def eodc(self):
		return self.EODCReceived
		
	def eod(self):
		return self.EODReceived or self.EODSent

	def aborted(self):
		return self.eof() and not self.eod() and self.BytesRcvd > 0

	def sendLine(self, str):
		try:	self.Sock.send(str + '\r\n')
		except: pass
		
	def recvLine(self):
		#print 'recvLine...'
		line = ''
		while string.find(line, '\n') < 0:
			try:	data = self.Sock.recv(1000)
			except: data = ''
			#print 'recvLine: received %s' % repr(data)
			if not data:
				return ''
			line = line + data
		return string.strip(string.split(line, '\n')[0])

	def close(self):
		self.Sock.close()
		self.EOF = 1
		self.Buf = ''

class	FTPPassiveAdapter:
	def __init__(self):
		self.SockA = socket(AF_INET, SOCK_STREAM)
		self.SockA.bind(('',0))
		self.SockA.listen(1)
		self.SockB = socket(AF_INET, SOCK_STREAM)
		self.SockB.bind(('',0))
		self.SockB.listen(1)
		self.DstSock = None
		self.DstAddr = None
		self.SrcSock = None
		self.SrcAddr = None
		self.Sel = None
		self.Connected = 0
		self.Mode = 'r'
		self.EOF = 0
		self.Done = 0

	def addrForClient(self):
		return gethostbyname(gethostname()), self.SockA.getsockname()[1]		

	def addrForCell(self):
		return gethostbyname(gethostname()), self.SockB.getsockname()[1]		

	def start(self, mode, sel):
		self.Sel = sel
		self.Mode = mode
		self.Sel.register(self, rd=self.SockA.fileno())
		self.Sel.register(self, rd=self.SockB.fileno())

	def run(self):
		while not self.Done:
			self.Sel.select(10)

	def doRead(self, fd, sel):
		if self.SockA and fd == self.SockA.fileno():
			sock, addr = self.SockA.accept()
			self.SockA.listen(0)
			self.Sel.unregister(rd=self.SockA.fileno())
			self.SockA.close()
			self.SockA = None
			if self.Mode == 'r':
				self.DstSock, self.DstAddr = sock, addr
			else:
				self.SrcSock, self.SrcAddr = sock, addr
		elif self.SockB and fd == self.SockB.fileno():
			sock, addr = self.SockB.accept()
			self.SockB.listen(0)
			self.Sel.unregister(rd=self.SockB.fileno())
			self.SockB.close()
			self.SockB = None
			if self.Mode == 'w':
				self.DstSock, self.DstAddr = sock, addr
			else:
				self.SrcSock, self.SrcAddr = sock, addr
		if not self.Connected and self.DstSock and self.SrcSock:
			self.Sel.register(self, rd=self.SrcSock.fileno())
			self.Connected = 1
		if self.SrcSock and fd == self.SrcSock.fileno():
			r,w,e = select.select([],[self.DstSock],[],1)
			if not w:	return
			data = self.SrcSock.recv(100000)
			if not data:
				# EOF
				self.eofReceived()
				return
			self.DstSock.send(data)

	def eofReceived(self):
		self.Sel.unregister(rd=self.SrcSock.fileno())
		self.SrcSock.close()
		self.SrcSock = None
		self.DstSock.close()
		self.DstSock = None
		self.Done = 1

	def clientSocket(self):
		# this is for LIST, NLST, so A socket is always destination
		self.DstSock, self.DstAddr = self.SockA.accept()
		self.SockA.close()
		self.SockA = None
		return self.DstSock

class	DFTPClientConnection:
	HelloMsg = 'DFarm FTP Server is ready'
	DefaultSize = 500000000L
	
	def __init__(self, ftpd, sock, addr, sel):
		self.FTPD = ftpd
		self.Sel = sel
		self.CtlAddr = addr
		self.Sock = sock
		self.Input = self.Sock.makefile()
		self.Sel.register(self, rd=self.Sock.fileno())
		self.DataAddr = None
		self.FileSize = self.DefaultSize
		self.FileReplicas = 1
		self.FilePath = None
		self.KnownSize = 0
		self.PWD = '/'
		self.MyCred = None
		self.ClientCred = None
		self.ClientName = None
		self.UserName = None
		self.GSSCtx = None
		self.AuthRequired = 0
		self.ReplyType = 'clear'
		self.TransferMode = 'S'
		self.PassiveAdapter = None
		self.PassiveMode = 0
		self.MoverSock = None
		self.MoverStr = None
		self.MoverDirection = 'r'
		self.SavedReplyType = 'clear'
		self.WaitForConnectEvent = None
		self.TransferInProgress = 0
		self.reply(200, self.HelloMsg)
		
	def log(self, msg):
		self.FTPD.log('[%s@%s] %s' % (self.UserName, self.CtlAddr, msg))
						
	def doRead(self, fd, sel):
		if self.MoverSock != None and fd == self.MoverSock.fileno():
			self.moverDoRead(fd, sel)
			return

		if fd != self.Sock.fileno():
			return
			
		line = self.Input.readline()
		if not line:
			self.log('disconnected')
			self.close()
			return
		
		line = string.strip(line)
		if not line:	return
		#print 'RCVD: <%s>' % line
		self.processCommand(line, 1)

	def moverDoRead(self, fd, sel):
		if self.MoverDirection == 'r':
			self.doEndRetr()
		else:
			self.doEndStor()
		
	def processCommand(self, line, setReply):
		if not line: return
		self.log('processCommand(<%s>, %s)' % (line, setReply))
		words = string.split(line)
		if not words:	return
		cmd = string.lower(words[0])
		args = words[1:]
		if setReply:	self.ReplyType = "clear"
		if self.TransferInProgress:
			self.reply(400, 'Transfer is in progress. Try again later')
		if cmd == 'port':
			self.doPort(args)
		elif cmd == 'retr':
			self.doRetr(args)
		elif cmd == 'stor':
			self.doStor(args)
		elif cmd == 'site':
			self.doSite(args)
		elif cmd == 'pass':
			self.reply(200, 'OK (%s ignored)' % cmd)
		elif cmd == 'user':
			self.doUser(args)
		elif cmd == 'auth':
			self.doAuth(args)
		elif cmd == 'adat':
			self.doAdat(args)
		elif cmd == 'list':
			self.doList(args)
		elif cmd == 'nlst':
			self.doNlst(args)
		elif cmd == 'type':
			self.reply(200, 'OK (ignored, will use I)')
		elif cmd == 'quit':
			self.reply(200, 'Bye-bye')
			self.close()
		elif cmd == 'pwd':
			self.reply(257, '"%s" is current directory.' % self.PWD)
		elif cmd == 'cwd':
			self.doCWD(args)
		elif cmd == 'cdup':
			self.doCDUp(args)
		elif cmd == 'auth':
			self.doAuth(args)
		elif cmd == 'mode':
			self.doMode(args)
		elif cmd == 'pasv':
			self.doPasv(args)
		elif cmd == 'syst':
			self.reply(215, 'UNIX Type: DFarm')
		elif cmd in ['mic','enc','conf']:
			self.secureCommand(cmd, args[0])
		else:
			self.badCommand(words)
		#self.ReplyType = 'clear'

	def secureCommand(self, sectype, data):
		self.ReplyType = sectype
		line = decode64(data)
		line, conf, qop = self.GSSCtx.unwrap(line)
		line = string.strip(line)
		inx = string.find(line, '\0')
		if inx >= 0:
			line = line[:inx]
		self.processCommand(line, 0)
		
	def reply(self, code, line):
		self.log('reply(%s, %s, %s)' % (self.ReplyType, code, line))
		if self.ReplyType != 'clear':
			#print 'ENC(%s) %s %s' % (self.ReplyType, code, line)
			data, conf = self.GSSCtx.wrap(0,0,'%s %s' % (code, line))
			code = {'mic':631, 'conf':632, 'enc':633}[self.ReplyType]
			line = encode64(data)
		#print 'SEND: %s %s' % (code, line)
		try:
			self.Sock.send('%s %s\r\n' % (code, line))
		except:
			self.log('Error sending reply: %s %s' % (sys.exc_type, sys.exc_value))
			self.close()			
		
	def badCommand(self, words):
		self.reply(500, 'Command <%s> not recognized' % words[0])

	def clearContext(self):
		self.FileSize = self.DefaultSize
		self.FileReplicas = 1
		self.FilePath = None
		self.DataAddr = None
		self.KnownSize = 0

	def makeAbsPath(self, lpath):
		# deal with '//', '/./', '..'
		if not lpath or lpath[0] != '/':
			# relative path, make it absolute
			if self.PWD[-1] == '/':
				lpath = self.PWD + lpath
			else:
				lpath = self.PWD + '/' + lpath
		# remove repeating slashes
		elements = string.split(lpath, '/')[1:] # first element is always empty because now path starts with '/'
		while '' in elements:
			elements.remove('')
		while '.' in elements:
			elements.remove('.')
		while '..' in elements:
			inx = elements.index('..')
			if inx > 0:
				elements = elements[:inx-1] + elements[inx+1:]
			else:
				elements = elements[inx+1:]
		return '/'+string.join(elements,'/')
		
	def doPasv(self, args):
		self.PassiveAdapter = FTPPassiveAdapter()
		self.PassiveMode = 1
		self.DataAddr = self.PassiveAdapter.addrForCell()
		addr = self.PassiveAdapter.addrForClient()
		host = tuple(string.split(addr[0],'.'))
		port = (addr[1]/256, addr[1]%256)
		addr = host + port
		self.reply(227, 'OK (%s,%s,%s,%s,%s,%s)' % addr)

	def doMode(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		if args[0] in 'sS':
			self.TransferMode = 'S'
		#elif args[0] in 'eE':
		#	self.TransferMode = 'E'
		else:
			self.reply(500, 'Unknown mode')
			return
		self.reply(200, 'Transfer mode set to %s' % self.TransferMode)				

	def doAuth(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		if args[0] != 'GSSAPI':
			self.reply(500, 'Unsupported authentication mechanism')
			return
		my_name = gss.gssName()
		my_name.import_name('ftp@%s' % gethostname(), gss.GSS_NT_SERVICE_NAME)
		self.MyCred = gss.gssCred()
		self.MyCred.acquire(my_name, 3600, [], gss.GSS_PY_ACCEPT)
		self.GSSCtx = gss.gssContext()
		self.reply(300, 'ADAT must follow')
		
	def doAdat(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		if self.GSSCtx == None:
			self.reply(503, 'Send AUTH first')
			return
		adat = args[0]
		token = decode64(adat)

		cb = None
		try:	
			# use channel bindings with Kerberos but not GSI
			x = gss.GSS_MECH_KRB5
			cb = (self.Sock.getpeername()[0], self.Sock.getsockname()[0])
		except:
			pass

		try:
			cont_needed, iname, imechs, otoken, ret_flags, time_rec, icred = \
				self.GSSCtx.accept(self.MyCred, cb, token)
		except:
			self.reply(535, 'Security context initiation failed: %s %s' % (
				sys.exc_type, sys.exc_value))
			self.GSSCtx = None
			self.MyCred = None
			return
		code = 235
		if cont_needed: 
			code = 335
		else:
			src_name, targ_name, time_rec, mech_type, context_flags = \
				self.GSSCtx.inquire()
			self.ClientCred = icred
			self.ClientName = src_name.display()				
		msg = 'OK (%s)' % self.ClientName
		if otoken:	msg = ('ADAT=%s' % encode64(otoken))
		self.reply(code, msg)

	def doUser(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		if self.UserName != None:
			self.reply(200, 'Already logged in as <%s>' % self.UserName)
			return
		if self.AuthRequired and self.ClientName == None:
			self.reply(400, 'Use AUTH first')
			return
		if not self.FTPD.authorize(self.ClientName, args[0]):
			self.reply(400, 'Authorization denied')
			return
		self.UserName = args[0]
		self.reply(200, '<%s> logged in as <%s>' % 
						(self.ClientName, self.UserName)) 
		
	def doList(self, args):
		self.sendList(args, 1)

	def doNlst(self, args):
		self.sendList(args, 0)

	def sendList(self, args, longList):
		if self.UserName == None:
			self.reply(400, 'Log in first')
			self.clearContext()
			self.resetPassive()
			return
		dfc = DiskFarmClient()
		dfc.Username = self.UserName
		dir = self.makeAbsPath('')
		if args:
			dir = self.makeAbsPath(args[0])
		sts, lst = dfc.listFiles(dir)
		if sts != 'OK':
			self.reply(400, 'Error: %s' % sts)
			self.clearContext()
			self.resetPassive()
			return
		if not self.DataAddr:
			self.reply(400, 'Unknown data socket')
			self.clearContext()
			self.resetPassive()
			return
		self.reply(100, 'Opening data connection for the list (%s)' % dir)
		if self.PassiveMode:
			s = self.PassiveAdapter.clientSocket()
		else:
			s = socket(AF_INET, SOCK_STREAM)
			try:	s.connect(self.DataAddr)
			except:
				self.reply(400, 'Error opening data connection: %s %s' %
					(sys.exc_type, sys.exc_value))
				self.clearContext()
				self.resetPassive()
				return
		for fn, ft, info in lst:
			if not longList:
				line = '%s\r\n' % fn
			elif ft == 'd':
				line = '%1s%4s %3s %-16s %12s %14s %s\r\n' % (
						ft, info.Prot, '-', info.Username, '-', '', fn)
			else:
				timstr = time.strftime('%m/%d %H:%M:%S', 
						time.localtime(info.CTime))
				line = '%1s%4s %3d %-16s %12s %14s %s\r\n' % (
					ft, info.Prot, info.mult(), info.Username,
					long2str(info.Size), timstr, fn)
			s.send(line)
		s.close()
		self.reply(200, 'OK (list)')
		self.clearContext()
		self.resetPassive()

	def doCDUp(self, args):
		return self.doCWD(['..'])

	def doCWD(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		if self.UserName == None:
			self.reply(400, 'Log in first')
			self.clearContext()
			return

		lpath = self.makeAbsPath(args[0])
		dfc = DiskFarmClient()
		dfc.Username = self.UserName

		info, err = dfc.getInfo(lpath)
		if not info:
			self.reply(500, 'Directory not found or can not be open: %s' % err)
			return
		if info.Type != 'd':
			self.reply(500, 'Not a directory')
			return
		self.PWD = lpath
		self.reply(200, 'OK, CWD is <%s>' % self.PWD)
							
	def doPort(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		words = string.split(args[0], ',')
		if len(words) != 6:
			self.reply(500, 'Wrong syntax: <%s>' % args[0])
			return
		host = string.join(words[:4], '.')
		port = int(words[4])*256 + int(words[5])
		self.DataAddr = (host, port)
		self.PassiveMode = 0
		if self.PassiveAdapter: 
			self.PassiveAdapter.close()
			self.PassiveAdapter = None
		self.reply(200, 'OK (port)')

	def resetPassive(self):
		self.PassiveMode = 0
		self.PassiveAdapter = None

	def doStor(self, args):
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			self.resetPassive()
			return
		#if self.FileSize == None:
		#	self.reply(400, 'Use SITE FINFO first')
		#	return
		if self.UserName == None:
			self.reply(400, 'Log in first')
			self.clearContext()
			self.resetPassive()
			return

		lpath = self.makeAbsPath(args[0])			
		dfc = DiskFarmClient()
		dfc.Username = self.UserName
		info = dfc.fileInfo(lpath, None)
		if self.KnownSize:
			info.setSizeEstimate(self.FileSize)
		else:
			info.setSizeEstimate(self.DefaultSize)
		info, err = dfc.createFile(info, self.FileReplicas)
		if not info:
			self.reply(400, 'Error creating file <%s>: %s' % (lpath, err))
			self.clearContext()
			self.resetPassive()
			return
		self.TransferInProgress = 1
		ctlsock = socket(AF_INET, SOCK_STREAM)
		ctlsock.bind(('',0))
		ctlsock.listen(1)
		ctlport = ctlsock.getsockname()[1]
		ctlhost = gethostbyname(gethostname())
		dfc.sendAcceptBcast(lpath, info, ctlsock, (ctlhost, ctlport),
			self.FileReplicas)
		dfc.waitForConnect(ctlsock, 2)
		retry = 5
		self.WaitForConnectEvent = self.FTPD.scheduleEvent(
				time.time(), 
				self.storCheckForConnect, 
				(retry, lpath, info, dfc, ctlsock, ctlhost, ctlport)
		)

	def storCheckForConnect(self, t, arg):
		self.WaitForConnectEvent = None
		retry, lpath, info, dfc, ctlsock, ctlhost, ctlport = arg
		done = dfc.waitForConnect(ctlsock, 0)

		if not done:
			retry = retry - 1
			if retry > 0:
				dfc.sendAcceptBcast(lpath, info, ctlsock, (ctlhost, ctlport),
					self.FileReplicas)
				self.WaitForConnectEvent = self.FTPD.scheduleEvent(
						time.time() + 5, 
						self.storCheckForConnect, 
						(retry, lpath, info, dfc, ctlsock, ctlhost, ctlport)
				)				
			else:
				self.reply(400, 'Request timed out')
				#self.clearContext()
				self.resetPassive()
				self.TransferInProgress = 0
			return

		mover_ctl, addr = ctlsock.accept()
		ctlsock.close()
		str = SockStream(mover_ctl)
		msg = str.recv()
		if msg != 'SEND':
			self.reply(400, 'Internal error')
			self.resetPassive()
			self.TransferInProgress = 0
			return
		if self.PassiveMode:
			self.PassiveAdapter.start('w',self.Sel)
		str.send('RCVFROM %s %s' % self.DataAddr)
		self.reply(100, 'Opening data connection for <%s>' % lpath)
		self.MoverSock = mover_ctl
		self.MoverStr = str
		self.MoverDirection = 'w'
		self.Sel.register(self, rd=self.MoverSock.fileno())

	def doEndStor(self):
		self.Sel.unregister(rd=self.MoverSock.fileno())
		str = self.MoverStr
		mover_ctl = self.MoverSock
		msg = str.recv()
		if msg != 'EOF':
			self.reply(400, 'Transfer aborted')
			self.clearContext()
			mover_ctl.close()
			self.resetPassive()
			self.TransferInProgress = 0
			return
		str.send('EOF')
		self.reply(200, 'Transfer complete')
		mover_ctl.close()
		self.resetPassive()
		self.TransferInProgress = 0
				
	def doSite(self, args):
		# SITE FINFO <path> <size, bytes, xxxL for long> [<replicas>]
		if len(args) < 1:
			self.reply(500, 'Wrong syntax')
			return
		kw = string.lower(args[0])
		if kw != 'finfo':
			self.reply(500, 'Unknown SITE subcommand <%s>' % args[0])
			return
		args = args[1:]
		if len(args) < 2 or len(args) > 3:
			self.reply(500, 'Wrong syntax of SITE FINFO command')
			return
		lpath = args[0]
		if args[1][-1] == 'L':	args[1] = args[1][:-1]
		fsize = long(args[1])
		nrep = 1
		if len(args) > 2:
			nrep = int(args[2])
		self.FilePath = lpath
		self.FileSize = fsize
		self.FileReplicas = nrep
		self.KnownSize = 1
		self.reply(200, 'OK (site)')

	def doRetr(self, args):
		if len(args) != 1:
			self.reply(500, 'Wrong syntax')
			self.resetPassive()
			return
		if self.UserName == None:
			self.reply(400, 'Log in first')
			self.clearContext()
			self.resetPassive()
			return
		lpath = self.makeAbsPath(args[0])
		dfc = DiskFarmClient()
		dfc.Username = self.UserName
		info, err = dfc.getInfo(lpath)
		if not info:
			self.reply(500, 'Error: %s' % err)
			self.clearContext()
			self.resetPassive()
			return
		if info.Type != 'f':
			self.reply(500, '%s is not a file' % lpath)
			self.clearContext()
			self.resetPassive()
			return
		if not self.DataAddr:
			self.reply(400, 'Data socket unspecified')
			self.clearContext()
			self.resetPassive()
			return
		ctlsock = socket(AF_INET, SOCK_STREAM)
		ctlsock.bind(('',0))
		ctlsock.listen(1)
		ctlport = ctlsock.getsockname()[1]
		ctlhost = gethostbyname(gethostname())
		self.TransferInProgress = 1
		dfc.sendSendBcast(lpath, info, ctlsock, (ctlhost, ctlport),
			nolocal=1)
		self.log('SendBroadcast sent')
		dfc.waitForConnect(ctlsock, 2)
		retry = 5
		self.WaitForConnectEvent = self.FTPD.scheduleEvent(
				time.time(), 
				self.retrCheckForConnect, 
				(retry, lpath, info, dfc, ctlsock, ctlhost, ctlport))
		self.log('retrCheck event scheduled')

	def retrCheckForConnect(self, t, arg):
		self.log('retrCheck event triggered')
		self.WaitForConnectEvent = None
		retry, lpath, info, dfc, ctlsock, ctlhost, ctlport = arg
		done = dfc.waitForConnect(ctlsock, 0)
		if not done:
			retry = retry - 1
			if retry > 0:
				dfc.sendSendBcast(lpath, info, ctlsock, (ctlhost, ctlport),
					nolocal=1)
				self.WaitForConnectEvent = self.FTPD.scheduleEvent(
						time.time() + 5, 
						self.retrCheckForConnect, 
						(retry, lpath, info, dfc, ctlsock, ctlhost, ctlport))				
			else:
				self.reply(400, 'Request timed out')
				#self.clearContext()
				self.resetPassive()
				self.TransferInProgress = 0
			return
		mover_ctl, addr = ctlsock.accept()
		ctlsock.close()
		str = SockStream(mover_ctl)
		msg = str.recv()
		if msg != 'RECV':
			self.reply(400, 'Internal error')
			mover_ctl.close()
			self.clearContext()
			self.resetPassive()
			self.TransferInProgress = 0
			return
		if self.PassiveMode:
			self.PassiveAdapter.start('r',self.Sel)
		str.send('SENDTO %s %s' % self.DataAddr)
		self.reply(100, 'Opening data connection for <%s>' % lpath)
		self.MoverSock = mover_ctl
		self.MoverStr = str
		self.MoverDirection = 'r'
		self.Sel.register(self, rd=self.MoverSock.fileno())

	def doEndRetr(self):
		self.Sel.unregister(rd=self.MoverSock.fileno())
		str = self.MoverStr
		mover_ctl = self.MoverSock
		msg = str.recv()
		if msg != 'EOF':
			self.reply(400, 'Unexpected end of file')
			mover_ctl.close()
			self.clearContext()
			self.resetPassive()
			self.TransferInProgress = 0
			return
		self.reply(200, 'Transfer complete')
		mover_ctl.close()
		self.clearContext()
		self.resetPassive()
		self.TransferInProgress = 0

	def close(self):
		self.log('close')
		self.Sel.unregister(rd=self.Sock.fileno())
		self.Sock.close()
		if self.WaitForConnectEvent != None:
			self.FTPD.cancelEvent(self.WaitForConnectEvent)

class	DFTPServer(TCPServer):
	def __init__(self, port, cfg):
		self.Cfg = cfg
		self.LogFile = None
		logpath = cfg.getValue('ftpd', '*', 'log')
		if logpath:
			interval = cfg.getValue('ftpd', '*', 'log_interval', '1d')
			self.LogFile = LogFile(logpath, interval)
		self.Timer = Timer()
		self.Sel = Selector()
		TCPServer.__init__(self, port, self.Sel)
		self.log('FTPD initialized at port %s' % port)

	def scheduleEvent(self, t, fcn, arg):
		return self.Timer.addEvent(t, 0, 1, fcn, arg)
		
	def cancelEvent(self, event):
		self.Timer.removeEvent(event)

	def log(self, msg):
		if self.LogFile:
			self.LogFile.log(msg)
		else:
			print msg

	def createClientInterface(self, sock, addr, sel):
		DFTPClientConnection(self, sock, addr, sel)

	def authorize(self, name, target_user):
		self.Cfg.reReadConfig()
		unames = self.Cfg.getValueList('user_profile', target_user, 'names')
		if not unames:	unames = []
		dnames = []
		if '+' in unames:
			while '+' in unames:
				unames.remove('+')
			dnames = self.Cfg.getValueList('user_profile', '*', 'names')
			if not dnames:	dnames = []
			unames = unames + dnames
		unames = map(lambda x, t=target_user: 
					string.replace(x, '%u', t), unames)
		return name in unames

	def run(self):
		t = self.Timer.nextt()
		if t:
			t = min(20, t)
		else:
			t = 20
		self.Sel.select(t)
		self.Timer.run()

if __name__ == '__main__':
	import os
	import getopt

	opts, args = getopt.getopt(sys.argv[1:], 'p:')
	port = 6789
	cfg = ConfigFile(os.environ['DFARM_CONFIG'])
	port = cfg.getValue('ftpd','*','port',port)
	for opt, val in opts:
		if opt == '-p': port = int(val)
	srv = DFTPServer(port, cfg)
	while 1:
		srv.run()
