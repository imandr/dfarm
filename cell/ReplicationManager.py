from pythreader import Task, TaskQueue, synchronized, Primitive
from DataClient import DataClient
from logs import Logged
import os, time

class   Replicator(Task, Logged):
    def __init__(self, fn, lpath, info, nrep, dclient, rmanager):
        Task.__init__(self)
        self.Manager = rmanager
        self.LocalFN = fn
        self.LogPath = lpath
        self.FileInfo = info
        self.NRep = nrep
        self.DClient = dclient

    def __str__(self):
        return "Replicator[%s *%d]" % (self.LogPath, self.NRep)
        
    def reinit(self):
        pass
            
    def run(self):
        try:    f = open(self.LocalFN, "rb")
        except:
            return self.Manager.done(self)
            
        ok, reason = self.DClient.put(self.FileInfo, f, self.NRep, True, 5.0)
        if ok:
            self.Manager.done(self)
        else:
            self.Manager.retry(self)

class ReplicationManager(Primitive, Logged):
    
    def __init__(self, cfg):
        self.Replicators = TaskQueue(cfg.get('max_rep',2))
        self.DClient = DataClient(
            (cfg["broadcast"], cfg["listen_port"]),
            cfg["farm_name"]
        )

    def replicate(self, nfrep, lfn, lpath, info):
            if nfrep > 2:
                    # make 2 replicators
                    n1 = nfrep/2
                    n2 = nfrep - n1
                    r = Replicator(lfn, lpath, info, n1, self.DClient, self)
                    self.debug('replicator created: %s *%d' % (lpath, n1))
                    self.Replicators.addTask(r)
                    r = Replicator(lfn, lpath, info, n2, self.DClient, self)
                    self.debug('replicator created: %s *%d' % (lpath, n2))
                    self.Replicators.addTask(r)
            elif nfrep > 0:
                    r = Replicator(lfn, lpath, info, nfrep, self.DClient, self)
                    self.Replicators.addTask(r)
                    self.debug('replicator created: %s *%d' % (lpath, nfrep))

    def done(self, rep):
        pass
        
    def retry(self, rep):
        rep.reinit()
        self.Replicators.addTask(rep)
        
    def statTxns(self):
            pending, active = self.Replocators.tasks()
            stats = [
                "RP * %s" % (x.LogPath,) for x in active
            ] + [
                "RP I %s" % (x.LogPath,) for x in pending
            ]
            
            return '\n'.join(stats)+".\n"

        
        
