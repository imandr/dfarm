#
# @(#) $Id: vfssrv.py,v 1.9 2003/12/04 16:52:28 ivm Exp $
#
# $Log: vfssrv.py,v $
# Revision 1.9  2003/12/04 16:52:28  ivm
# Implemented BSD DB - based VFS DB
# Use connect with time-out for data communication
#
# Revision 1.7  2003/03/25 17:36:46  ivm
# Implemented non-blocking directory listing transmission
# Implemented single inventory walk-through
# Implemented re-tries on failed connections to VFS Server
#
# Revision 1.6  2002/08/12 16:29:43  ivm
# Implemented cell indeces
# Kerberized ftpd
#
# Revision 1.5  2002/04/30 20:07:16  ivm
# Implemented and tested:
#       node replication
#       node hold/release
#
# Revision 1.4  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.3  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.2  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

from VFSServer import VFSServer
from Selector import Selector
from VFSDB import VFSDB
from CellIF import StorageCellIF
from QuotaMgr import QuotaManager
from LogFile import LogFile
import vfssrv_global
#from config import ConfigFile
import yaml, getopt
import signal
import os, sys
import time

GotStopSignal = 0

def handle_signal(signo, env):
        global GotStopSignal
        GotStopSignal = signo
        set_signals()

def set_signals():
        signal.signal(signal.SIGINT, handle_signal)
        #signal.signal(signal.SIGKILL, handle_signal)
        signal.signal(signal.SIGHUP, handle_signal)

if __name__ == '__main__':

    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    cfgfile = opts.get("-c") or os.environ['DFARM_VFS_CONFIG']
    cfg = yaml.load(open(cfgfile, "r"), Loader=yaml.SafeLoader)["VFSServer"]
    
    sel = Selector()
    logpath = cfg.get('log')
    interval = '1d'
    if logpath:
            vfssrv_global.G_LogFile = LogFile(logpath, interval)
    set_signals()
    vfssrv_global.G_VFSServer = VFSServer(cfg, sel)
    vfssrv_global.G_VFSDB = VFSDB(cfg['db_root'])
    vfssrv_global.G_CellIF = StorageCellIF(cfg, sel)        # fix
    vfssrv_global.G_QuotaMgr = QuotaManager(cfg)

    if vfssrv_global.G_LogFile:
            vfssrv_global.G_LogFile.log('VFS Server started at %s, pid=%s' %
                    (time.ctime(time.time()), os.getpid()))

    if vfssrv_global.G_LogFile:
            vfssrv_global.G_LogFile.log('Begin inventory...')

    nfiles = 0
    nfiles = vfssrv_global.G_VFSDB.fileInventory(vfssrv_global.G_QuotaMgr)

    if vfssrv_global.G_LogFile:
            vfssrv_global.G_LogFile.log('End inventory. %d' % nfiles)

    while not GotStopSignal:
            sel.select(20)
            vfssrv_global.G_QuotaMgr.idle()
            vfssrv_global.G_VFSDB.idle()

    print('Interrupted with signal #%d' % GotStopSignal)
    vfssrv_global.G_LogFile.log('Interrupted with signal #%d' % GotStopSignal)
    vfssrv_global.G_VFSDB.flush()
    vfssrv_global.G_LogFile.log('Databases flushed')
