import cellmgr_global
from socket import *
from SockStream import SockStream
import time
import sys
import random
from pythreader import PyThread

class   VFSSrvIF(PyThread):
        def __init__(self, myid, cfg, storage):
                PyThread.__init__(self)
                self.ID = myid
                self.DSAddr = (cfg['host'], cfg['cellif_port'])
                self.Connected = 0
                self.Reconciled = 0
                self.LastIdle = 0
                self.NextReconnect = 0
                self.NextProbeTime = 0
                self.connect()
                self.CellStorage = storage
        
        def log(self, msg):
                msg = 'VFSSrvIF: %s' % (msg,)
                if cellmgr_global.LogFile:
                        cellmgr_global.LogFile.log(msg)
                else:
                        print(msg)
                        sys.stdout.flush()
                                
        def connect(self):
                self.Sock = socket(AF_INET, SOCK_STREAM)
                try:    self.Sock.connect(self.DSAddr)
                except: 
                        self.log('can not connect to VFS Server')
                        return False
                self.Str = SockStream(self.Sock)
                ans = self.Str.sendAndRecv('HELLO %s' % self.ID)
                self.log('connect: HELLO -> %s' % ans)
                if ans == 'HOLD':
                        self.CellStorage.hold()
                elif ans != 'OK':
                        if ans == 'EXIT':
                                self.log('Shot down by VFS Server')
                                sys.exit(3)
                        return False
                return True
                
        def reconcile(self):
                for lp, info in self.CellStorage.listFiles():
                        #self.log('reconcile: %s %s' % (lp, info))
                        if info:
                                sizestr = '%s' % info.Size
                                if sizestr[-1] == 'L':
                                        sizestr = sizestr[:-1]
                                ct = info.CTime
                                self.log('reconcile: sending IHAVE %s %s %s' % (lp, ct, sizestr))
                                self.Str.send('IHAVE %s %s %s' % (lp, ct, sizestr))
                self.Str.send('SYNC')
                return True
                
        def run(self):
        
            while True:
        
                if not self.connect():
                    time.sleep(0.5 + random.random())
                    continue
                self.log("connected")
                
                if not self.reconcile():
                    self.disconnect()
                    time.sleep(0.5 + random.random())
                    continue
                self.log("reconciled")
                    
                eof = False

                while not eof:
                    msg = self.Str.recv()        
                    self.log('doRead: msg:<%s>' % msg)
                    if not msg: 
                        eof = True
                    else:
                        words = msg.split()
                        if words[0] == 'SYNC':
                                self.Reconciled = 1
                                self.log('reconciled')
                        elif words[0] == 'DEL':
                                if not words[1:]:
                                    self.disconnect()
                                lp = words[1]
                                self.CellStorage.delFile(lp)
                        elif words[0] == 'HOLD':
                                self.CellStorage.hold()
                        elif words[0] == 'RELEASE':
                                self.CellStorage.release()
                        elif words[0] == 'REPLICATE':
                                self.doReplicate(words[1:])
                        else:
                            # ???
                            eof = True
                self.disconnect()
                

        def doReplicate(self, args):
                # args: (<path>|*) <nfrep>
                if len(args) < 2:       return
                path = args[0]
                mult = int(args[1])
                if path == '*':
                        self.CellStorage.replicateAll(mult)
                else:
                        self.CellStorage.replicateFile(path, mult)
                        
                
        def disconnect(self):
                self.Reconciled = False
                if self.Str:
                        self.Sock.close()
                        self.Sock = None
                        self.Str = None
                self.Connected = False
                
        def probe(self):
                if not self.Connected or time.time() < self.NextProbeTime:      return
                self.Str.probe()
                self.NextProbeTime = time.time() + 300

        def sendIHave(self, lpath, info):
                if self.Connected:
                        sizestr = '%s' % info.Size
                        if sizestr[-1] == 'L':
                                sizestr = sizestr[:-1]
                        self.Str.send('IHAVE %s %s %s' % (lpath, info.CTime, sizestr))
                        

