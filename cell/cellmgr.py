from VFSSrvIF import VFSSrvIF
from CellStorage import CellStorageMgr
import cellmgr_global
from Selector import Selector
from DataServer import DataServer
from CellListener import CellListener
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
        opts, args = getopt.getopt(sys.argv[1:], "c:d:")
        opts = dict(opts)

        cfg = DFConfig(opts.get("-c"), 'DFARM_CONFIG')


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

        if "-d" in opts:
            debug = "-d" in opts
            debug_file = opts.get("-d")
            if debug_file == "-":   debug_file = None
        else:
            debug = class_section.get("debug_enabled", False)
            debug_file = class_section.get("debug_file")

        logs.set_debug(debug)
        if debug and debug_file:
            logs.open_debug(debug_file)            


        logpath = class_section['log']
        interval = '1d'
        if logpath:
                if isinstance(logpath, list):
                        logpath, interval = tuple(logpath)
        logs.open_log(logpath, interval)
        cellmgr_global.DataServer = data_server = DataServer(myclass, cfg["cell"], class_section)
        cellmgr_global.CellStorage = cell_storage = CellStorageMgr(myid, myclass, class_section)
        cellmgr_global.VFSSrvIF = vfs_srv_if = VFSSrvIF(myid, cfg["VFSServer"], cell_storage)
        cell_listener = CellListener(myid, cfg["cell"], data_server, cell_storage, vfs_srv_if)
        cell_listener.enable()
        cell_listener.start()
        vfs_srv_if.start()
        

        if cellmgr_global.LogFile:
                cellmgr_global.LogFile.log('Cell Manager started, pid=%d' % os.getpid())

        cell_listener.join()
