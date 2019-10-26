from LogFile import LogFile
import sys

_LogFile = None
_DebugFile = None
_DebugEnabled = False

def open_log(fn, *params, **args):
        global _LogFile
        _LogFile = LogFile(fn, *params, **args)

def open_debug(fn, *params, **args):
        global _DebugFile
        _DebugFile = LogFile(fn, *params, **args)

def set_debug(enabled=False):
        global _DebugEnabled
        _DebugEnabled = enabled
        debug("debug %s" % ("enabled" if enabled else "disabled"), forced=True)
        

def log(msg):
        if _LogFile is not None:
                _LogFile.log(msg)

def debug(msg, forced = False):
        #print("debug(%s), enabled=%s" % (msg, _DebugEnabled))
        if forced or _DebugEnabled:
                if _DebugFile is not None:
                        _DebugFile.log(msg)
                else:
                        print(msg)
                        sys.stdout.flush()

class Logged(object):

        def __str__(self):
            # default
            return "[%s]" % (self.__class__.__name__,)

        def log(self, msg):
                msg = "%s: %s" % (self, msg)
                log(msg)

        def debug(self, msg):
                msg = "%s: %s" % (self, msg)
                debug(msg)




