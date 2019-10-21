#
# @(#) $Id: cellmgr.py,v 1.11 2002/09/10 17:37:56 ivm Exp $
#
# Cell Manager "main" module
#
# $Log: cellmgr.py,v $
# Revision 1.11  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.10  2002/07/09 18:48:11  ivm
# Implemented purging of empty directories in PSA
# Implemented probing of VFS Server by Cell Managers
#
# Revision 1.9  2002/04/30 20:07:15  ivm
# Implemented and tested:
#       node replication
#       node hold/release
#
# Revision 1.8  2001/10/24 20:50:53  ivm
# Avoid sleeping in Timer.run()
#
# Revision 1.7  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.6  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.5  2001/06/18 18:05:52  ivm
# Implemented disconnect-on-time-out in SockRcvr
#
# Revision 1.4  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.3  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.2  2001/04/04 18:05:58  ivm
# *** empty log message ***
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

from VFSSrvIF import VFSSrvIF
from CellStorage import CellStorageMgr
import cellmgr_global
from Selector import Selector
from DataMover import DataMover
from CellListener import CellListener
from socket import *
from dfconfig import DFConfig
from Timer import Timer
from LogFile import LogFile
import os
import time, getopt, sys

def idle_tasks(t, arg):
        cellmgr_global.DataMover.idle()
        cellmgr_global.VFSSrvIF.idle()
        cellmgr_global.CellStorage.idle()

if __name__ == '__main__':
        sel = Selector()
        opts, args = getopt.getopt(sys.argv[1:], "c:")
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
        logpath = class_section['log']
        interval = '1d'
        if logpath:
                if isinstance(logpath, list):
                        logpath, interval = tuple(logpath)
                cellmgr_global.LogFile = LogFile(logpath, interval)
        cellmgr_global.DataServer = data_server = DataServer(myclass, class_section)
        cellmgr_global.CellStorage = cell_storage = CellStorageMgr(myid, myclass, class_section)
        cellmgr_global.VFSSrvIF = vfs_srv_if = VFSSrvIF(myid, cfg["VFSServer"], cell_storage)
        cell_listener = CellListener(myid, cfg["cell"], data_server, cell_storage, vfs_srv_if)
        cell_listener.enable()
        cell_listener.start()
        

        if cellmgr_global.LogFile:
                cellmgr_global.LogFile.log('Cell Manager started, pid=%d' % os.getpid())

        cell_listener.join()
