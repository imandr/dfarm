
from VFSSrvIF import VFSSrvIF
from CellStorage import CellStorageMgr
import cellmgr_global
from Selector import Selector
from DataServer import DataServer
from CellListenerThread import CellListener
from ReplicationManager import ReplicationManager
from socket import *
from dfconfig import DFConfig
from Timer import Timer
from LogFile import LogFile
import os
import time, getopt, sys
import logs

if __name__ == '__main__':
        open("cellmgr.pid", "w").write("%d" % (os.getpid(),))
        sel = Selector()
        opts, args = getopt.getopt(sys.argv[1:], "c:d")
        opts = dict(opts)
        cfg = DFConfig(opts.get("-c"), 'DFARM_CONFIG')
        debug = "-d" in opts
        logs.set_debug(debug)
        myid = gethostname()
        domain = cfg['cell'].get('domain','')
        dot_dom = '.' + domain
        ld = len(dot_dom)
        if myid[-ld:] == dot_dom:
                myid = myid[:-ld]
        myid = myid.lower()
        print("My id:", myid)
        print("cell class config:", cfg["cell_class"])
        myclass = cfg['cell_class'][myid]
        print("My class:", myclass)
        if myclass == None:
                print('Can not determine cell class. My ID = <%s>' % myid)
                sys.exit(1)
        class_section = cfg['class:%s' % (myclass,)]
        logpath = class_section['log']
        interval = '1d'
        if logpath:
                if isinstance(logpath, list):
                        logpath, interval = tuple(logpath)
        logs.open_log(logpath, interval)
        cellmgr_global.ReplicationManager = ReplicationManager(cfg["cell"])
        cellmgr_global.DataServer = data_server = DataServer(myclass, class_section)
        cellmgr_global.CellStorage = cell_storage = CellStorageMgr(myid, myclass, class_section)
        cellmgr_global.VFSSrvIF = vfs_srv_if = VFSSrvIF(myid, cfg["VFSServer"], cell_storage)
        cell_listener = CellListener(myid, cfg["cell"], data_server, cell_storage, vfs_srv_if)
        cell_listener.enable()
        cell_listener.start()
        vfs_srv_if.start()
        

        if cellmgr_global.LogFile:
                cellmgr_global.LogFile.log('Cell Manager started, pid=%d' % os.getpid())

        cell_listener.join()
