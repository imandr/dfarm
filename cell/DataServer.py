#from txns import *
from SockStream import SockStream
from socket import *
from Replicator import Replicator
import sys
import os
import time
import cellmgr_global
import select
from pythreader import Task, synchronized, Primitive, TaskQueue
from logs import Logged

class FileMover(Task, Logged):
    
        def __init__(self, txn, caddr, delay):
                Task.__init__(self)
                self.CAddr = caddr
                self.Delay = delay
                self.Txn = txn

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
            
            self.debug('control socket connected')
            stream = SockStream(csock)
            stream.zing()
            self.debug("stream.recv...")
            msg = stream.recv()
            self.debug("msg from control: [%s]" % (msg,))
            if not msg or not msg.startswith("DATA "):
                csock.close()
                raise RuntimeError('bad DATA message: "%s"' % (msg,))
            
            words = msg.split()
            if len(words) < 3:
                csock.close()
                raise RuntimeError('bad DATA message: "%s"' % (msg,))

            daddr = (words[1], int(words[2]))
            self.log("data address: %s" % (daddr,))
            try:    
                dsock = self.connectSocket(daddr)
            except Exception as e:
                csock.close()
                raise RuntimeError("data socket connection failure: %s" % (e,))
            
            return stream, csock, dsock

        def run(self):
            if self.Delay and self.Delay > 0.0:
                time.sleep(self.Delay)
    
            try:    cstream, csock, dsock = self.init_transfer(self.CAddr)
            except Exception as e: 
                self.debug("failed to initiate the transfer: %s" % (e,))
                return self.failed("failed to initiate the transfer: %s" % (e,))
            
            self.debug("transfer initialized with remote data socket %s" % (dsock.getpeername(),))
            
            try:    
                nbytes = self.transfer(cstream, csock, dsock)
            except Exception as e:
                self.debug("transfer failed: %s" % (e,))
                return self.failed("transfer failed: %s" % (e,))
            finally:
                dsock.close()
                csock.close()

            self.succeeded()
            self.debug("done")
                
        def failed(self, reason):
            self.log(reason)
            self.Txn.rollback()
            
        def succeeded(self):
            self.Txn.commit()
            self.log("tracsaction committed")
                
class SocketSender(FileMover):
    
        def __str__(self):
                return 'SocketSender[lp=%s -> c=%s]' % (
                        self.Txn.LPath, self.CAddr)
                
        def transfer(self, cstream, csock, dsock):
            
            nbytes = 0

            with open(self.Txn.dataPath(),'rb') as f:
                done = False
                while not done:
                    data = f.read(1024*1024*10)
                    if not data:
                        done = True
                    else:
                        dsock.sendall(data)
                        nbytes += len(data)
                        self.debug("sent %d bytes" % (len(data),))
            dsock.close()
            self.debug("senfing EOF...")
            ok = cstream.sendAndRecv("EOF %d" % (nbytes,))
            self.debug("response to EOF: %s" % (ok,))
            if ok != "OK":
                self.log("Expected OK from the sender, got '%s'" % (ok,))         
            self.debug("transfer complete")
            return nbytes

class SocketReceiver(FileMover):
    
        def __str__(self):
                return 'SocketReceiver[lp=%s <- c=%s]' % (
                        self.Txn.LPath, self.CAddr)

        def transfer(self, cstream, csock, dsock):

            nbytes = 0
    
            with open(self.Txn.dataPath(),'wb') as f:
                done = False
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


class   DataServer(Primitive, Logged):
        def __init__(self, myid, cfg):
            Primitive.__init__(self)
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
        def canSend(self, lpath):
                if self.txnCount() >= self.MaxTxn or \
                                        self.getTxns() >= self.MaxGet:
                        return False
                for t in self.GetMovers.activeTasks() + self.GetMovers.waitingTasks():
                        if t.Txn.LPath == lpath:
                                return False
                return True                        

        @synchronized
        def canReceive(self, lpath):
                if self.txnCount() >= self.MaxTxn or \
                                        self.putTxns() >= self.MaxPut:
                        return False
                for t in self.PutMovers.activeTasks() + self.PutMovers.waitingTasks():
                        if t.Txn.LPath == lpath:
                                return False
                return True                        

        def canOpenFile(self, lpath, mode):
                if mode == 'r':
                        return self.canSend(lpath)
                else:
                        return self.canReceive(lpath)
                
        def sendSocket(self, txn, caddr, delay):
                self.debug("add read task: %s" % (txn.LPath,))
                self.GetMovers.addTask(SocketSender(txn, caddr, delay))

        def recvSocket(self, txn, caddr, delay):
                self.debug("add write task: %s" % (txn.LPath,))
                self.PutMovers.addTask(SocketReceiver(txn, caddr, delay))
                
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
                return len(self.PutMovers.activeTasks())
                
        def getTxns(self):
                #print 'getTxns: %d' % self.txnCount('D')
                return len(self.GetMovers.activeTasks())
                
        @synchronized
        def txnCount(self):
            return self.getTxns() + self.putTxns()
            
        def statTxns(self):
            pending, active = self.GetMovers.tasks()
            stats = [
                "RD * %s" % (x.Txn.LPath,) for x in active
            ] + [
                "RD I %s" % (x.Txn.LPath,) for x in pending
            ]
            pending, active = self.PutMovers.tasks()
            stats += [
                "WR * %s" % (x.Txn.LPath,) for x in active
            ] + [
                "WR I %s" % (x.Txn.LPath,) for x in pending
            ]

            return "\n".join(stats)
