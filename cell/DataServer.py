#from txns import *
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
                
        def connectSocket(self, addr, tmo = None):
                # returns either connected socket or None on timeout
                # None means infinite
                s = socket(AF_INET, SOCK_STREAM)        # create a socket
                s.settimeout(tmo)
                try:    s.connect(addr)
                except timeout:
                    s.close()
                    raise IOError('timeout')
                except Exception as e:
                    s.close()
                    raise e
                try:    s.getpeername()
                except:
                        s.close()
                        raise IOError('connection refused')
                s.settimeout(None)
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
                return self.failed("failed to initiate the transfer")
            
            self.log("data socket connected to %s" % (addr,))
            
            try:    nbytes = self.transfer(cstream, csock, dsock)
            except Exception as e:
                return self.failed("transfer failed: %s" % (e,))
            finally:
                dsock.shitdown(SHUT_RDWR)
                dsock.close()
                csock.shitdown(SHUT_RDWR)
                csock.close()

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
            
            nbytes = 0

            whith open(self.Txn.dataPath(),'rb') as f:
                done = False
                while not done:
                    data = f.read(1024*1024*10)
                    if not data:
                        done = True
                    else:
                        dsock.sendall(data)
                        nbytes += len(data)
                        
            ok = cstream.sendAndRecv("EOF %d" % (nbytes,))
            if ok != "OK":
                self.log("Expected OK from the sender, got '%s'" % (ok,))         
            return nbytes

class SocketReceiver(Task):
    
        def __str__(self):
                return 'SocketReceiver[lp=%s, c=%s]' % (
                        self.Txn.LPath, self.CAddr)

        def transfer(self, cstream, csock, dsock):

            nbytes = 0
    
            whith open(self.Txn.dataPath(),'wb') as f:
                while not done:
                    data = dsock.recv(1024*1024*10)
                    if not data:
                        done = True
                    else:
                        f.write(data)
                        nbytes += len(data)

            msg = cstream.recv()
            words = msg.split()
            if len(words) < 2 or words[0] != "EOF":
                raise RuntimeError("Bad EOF message: [%s]" % (msg,))
            count = int(words[1])
            if count != nbytes:
                raise RuntimeError("Incorrect byte count: from EOF message: %d, actually received: %d" % (
                    count, nbytes))
            cstream.send("OK")
            return nbytes


class   DataServer(PyThread):
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
                self.GetMovers.addTask(SocketReceiver(txn, caddr, delay))

        """ comment out
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
        """
                
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
