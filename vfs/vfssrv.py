from VFSServer2 import VFSServer
#from Selector import Selector
from VFSDB2 import VFSDB
from CellIF2 import StorageCellIF
from QuotaMgr import QuotaManager
from LogFile import LogFile
import vfssrv_global
#from config import ConfigFile
import yaml, getopt
import signal
import os, sys
import time
import logs

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
    from dfconfig import DFConfig

    opts, args = getopt.getopt(sys.argv[1:], "c:d")
    opts = dict(opts)
    cfg = DFConfig(opts.get("-c"), 'DFARM_VFS_CONFIG')["VFSServer"]
    
    logpath = cfg.get('log')
    interval = '1d'
    logs.open_log(logpath, interval)
    logs.set_debug("-d" in opts)
    set_signals()
    vfssrv_global.G_VFSServer = VFSServer(cfg).start()
    vfssrv_global.G_VFSDB = VFSDB(cfg['db_root'])
    vfssrv_global.G_CellIF = StorageCellIF(cfg).start()
    vfssrv_global.G_QuotaMgr = QuotaManager(cfg)

    logs.log('VFS Server started at %s, pid=%s' %
                    (time.ctime(time.time()), os.getpid()))

    logs.log('Begin inventory...')

    nfiles = 0
    nfiles = vfssrv_global.G_VFSDB.fileInventory(vfssrv_global.G_QuotaMgr)

    logs.log('End inventory. %d' % nfiles)

    while not GotStopSignal:
            time.sleep(20)
            vfssrv_global.G_QuotaMgr.idle()
            vfssrv_global.G_VFSDB.idle()

    print('Interrupted with signal #%d' % GotStopSignal)
    logs.log('Interrupted with signal #%d' % GotStopSignal)
    vfssrv_global.G_VFSDB.flush()
    logs.G_LogFile.log('Databases flushed')
