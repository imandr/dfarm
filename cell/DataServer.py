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
#       node replication
#       node hold/release
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
import os
import time
import cellmgr_global
import select

class FileMover(Task):
    
        def __init__(self, txn, caddr, delay):
                Task.__init__(self)
                self.CAddr = caddr
                self.Delay = delay
                self.Txn = txn
                
        def log(self, msg):
                msg = '%s: %s' % (self, msg)
                if cellmgr_global.LogFile:
                        cellmgr_global.LogFile.log(msg)
                else:
                        print(msg)
                        sys.stdout.flush()
                
        def connectSocket(self, addr, tmo = -1):
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
                        raise IOError('timeout')
                if s.connect_ex(addr) == 0:
                        s.setblocking(1)
                        return s
                try:    s.getpeername()
                except:
                        # connection refused
                        s.close()
                        raise IOError('connection refused')
                s.setblocking(1)
                return s


        def init_transfer(self, caddr):
            try:    csock = self.connectSocket(caddr)
            except:
                raise RuntimeError("control socket connection failure")
            
            self.log('control socket connected')
            stream = SockStream(csock)
            msg = stream.recv()
            if not msg or not msg.startswith("DATA "):
                csock.close()
                raise RuntimeError('bad DATA message: "%s"' % (msg,))
            
            words = msg.split()
            if len(words) < 3:
                csock.close()
                raise RuntimeError('bad DATA message: "%s"' % (msg,))

            daddr = (words[1], int(words[2]))
            try:    
                dsock = connectSocket(daddr)
            except:
                csock.close()
                raise RuntimeError("data socket connection failure")
            
            return stream, csock, dsock

        def run(self):
            if self.Delay and self.Delay > 0.0:
                time.sleep(self.Delay)
    
            try:    cstream, csock, dsock = self.init_transfer(self.CAddr)
            except: 
                return self.failed()
            
            self.log("data socket connected to %s" % (addr,))
            
            try:    nbytes = self.transfer(cstream, csock, dsock)
            except: 
                csock.close()
                return self.failed("transfer failed")
            finally:
                csock.shitdown(SHUT_RDWR)
                dsock.close()

            msg = cstream.recv()
            count = None
            try:
                words = msg.split()
                assert words[0] == "EOF"
                count = int(words[1])
            except:
                return self.failed("EOF message not recognized (%s)" % (msg,))
            finally:
                csock.shitdown(SHUT_RDWR)
                csock.close()
                
            if count != nbytes:
                return self.failed("Incorrect byte count: transfer count: %d, from EOF message: %d" % (nbytes, count))

            self.succeeded()
                
        def failed(self, reason):
            self.log(reason)
            self.Txn.rollback()
            
        def succeeded(self):
            self.Txn.commit()
                
class SocketSender(FileMover):
    
        def __str__(self):
                return 'SocketSender[lp=%s, c=%s]' % (
                        self.Txn.LPath, self.CAddr)
                
        def transfer(self, cstream, csock, dsock):
            
            if self.Delay and self.Delay > 0.0:
                time.sleep(self.Delay)
    
            whith open(self.Txn.dataPath(),'rb') as f:
                done = False
                nbytes = 0
                while not done:
                    data = f.read(1024*1024*10)
                    if not data:
                            stream.send('EOF %d' % (nbytes,))
                            done = True
                    else:
                        dsock.sendall(data)
                        nbytes += len(data)
                        
            return nbytes

class SocketReceiver(Task):
    
        def __str__(self):
                return 'SocketReceiver[lp=%s, c=%s]' % (
                        self.Txn.LPath, self.CAddr)

        def transfer(self, cstream, csock, dsock):

            if self.Delay and self.Delay > 0.0:
                time.sleep(self.Delay)
    
            whith open(self.Txn.dataPath(),'wb') as f:
                while not done:
                    data = dsock.recv(1024*1024*10)
                    if not data:
                        done = True
                    else:
                        f.write(data)
                        nbytes += len(data)

            return nbytes


class   DataServer(Primitive):
        def __init__(self, myid, cfg):
                HasTxns.__init__(self)
                self.Cfg = cfg
                self.MyID = myid
                self.MaxGet = cfg.get('max_get',3)
                self.MaxPut = cfg.get('max_put',1)
                self.MaxRep = cfg.get('max_rep',2)
                self.MaxTxn = cfg.get('max_txn',5)
                
                self.GetMovers = TaskQueue(self.MaxGet)
                self.PutMovers = TaskQueue(self.MaxPut)
                self.RepMovers = TaskQueue(self.MaxRep)
                
                self.ReplicatorsToRetry = []
                
        @synchronized
        def txnCount(self):
            return len(self.GetMovers.activeTasks()) + len(self.PutMovers.activeTasks()) \
                    + len(self.RepMovers.activeTasks())
            
        def log(self, msg):
                msg = 'DataMover: %s' % msg
                if cellmgr_global.LogFile:
                        cellmgr_global.LogFile.log(msg)
                else:
                        print(msg)
                        sys.stdout.flush()
                
        @synchronized
        def canSend(self, lpath):
                if self.txnCount() >= self.MaxTxn or \
                                        len(self.self.GetMovers.activeTasks()) >= self.MaxGet:
                        return False
                for t in self.self.PutMovers.activeTasks() + self.self.PutMovers.waitingTasks():
                        if t.LPath == lpath:
                                return False
                return True                        

        @synchronized
        def canReceive(self, lpath):
                if self.txnCount() >= self.MaxTxn or \
                                        len(self.self.PutMovers.activeTasks()) >= self.MaxPut:
                        return False
                for t in self.self.PutMovers.activeTasks() + self.self.PutMovers.waitingTasks():
                        if t.LPath == lpath:
                                return False
                return True                        

        def canOpenFile(self, lpath, mode):
                if mode == 'r':
                        return self.canSend(lpath)
                else:
                        return self.canReceive(lpath)
                
        def sendSocket(self, txn, caddr, delay):
                self.GetMovers.addTask(SocketSender(txn, caddr, delay))

        def recvSocket(self, txn, caddr, delay):
                txn.notify(self)
                #self.log('recvSocket: initiating socket recv txn #%s from %s' %\
                #       (txn.ID, caddr))
                SocketRcvr(txn, caddr, self.Sel, delay)

        def sendLocal(self, txn, caddr, delay):
                txn.notify(self)
                #self.log('sendLocal: initiating local send txn #%s to %s' %\
                #       (txn.ID, caddr))
                LocalSndr(txn, caddr, self.Sel, delay)
                

        def recvLocal(self, txn, caddr, delay):
                txn.notify(self)
                #self.log('recvLocal: initiating local recv txn #%s from %s' %\
                #       (txn.ID, caddr))
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
