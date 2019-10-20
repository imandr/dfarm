#
# @(#) $Id: LogFile.py,v 1.3 2009/07/29 18:59:36 rreitz Exp $
#

import time
import os
import datetime

from threading import RLock, Thread, Event

def synchronized(method):
    def smethod(self, *params, **args):
        self._Lock.acquire()
        try:    
            return method(self, *params, **args)
        finally:
            self._Lock.release()
    return smethod


class   LogFile:
        def __init__(self, path, interval = '1d', keep = 10, timestamp=1, append=True):
                # interval = 'midnight' means roll over at midnight
                self._Lock = RLock()
                self.Path = path
                self.File = None
                self.CurLogBegin = 0
                if type(interval) == type(''):
                        mult = 1
                        if interval[-1] == 'd' or interval[-1] == 'D':
                                interval = interval[:-1]
                                mult = 24 * 3600
                                interval = int(interval) * mult
                        elif interval[-1] == 'h' or interval[-1] == 'H':
                                interval = interval[:-1]
                                mult = 3600
                                interval = int(interval) * mult
                        elif interval[-1] == 'm' or interval[-1] == 'M':
                                interval = interval[:-1]
                                mult = 60
                                interval = int(interval) * mult
                self.Interval = interval
                self.Keep = keep
                self.timestamp = timestamp
                self.LineBuf = ''
                self.LastLog = None
                if append:
                    self.File = open(self.Path, 'a')
                    self.CurLogBegin = time.time()
                        
        def newLog(self):
                if self.File != None:
                        self.File.close()
                try:    os.remove('%s.%d' % (self.Path, self.Keep))
                except: pass
                for i in range(self.Keep - 1):
                        inx = self.Keep - i
                        old = '%s.%d' % (self.Path, inx - 1)
                        new = '%s.%d' % (self.Path, inx)
                        try:    os.rename(old, new)
                        except: pass
                try:    os.rename(self.Path, self.Path + '.1')
                except: pass
                self.File = open(self.Path, 'w')
                self.CurLogBegin = time.time()
                
        @synchronized
        def log(self, msg):
                t = time.time()
                if self.timestamp:
                        tm = time.localtime(t)
                        msg = time.strftime('%D %T: ', tm) + msg
                if self.Interval == 'midnight':
                        if datetime.date.today() != self.LastLog:
                                self.newLog()
                elif isinstance(self.Interval,int):
                        if t > self.CurLogBegin + self.Interval:
                                self.newLog()
                self.File.write(msg + '\n');
                self.File.flush()
                self.LastLog = datetime.date.today()

        @synchronized
        def     write(self, msg):
                self.LineBuf = self.LineBuf + msg
                lines = self.LineBuf.split("\n")
                if len(lines) > 1:
                        for line in lines[:-1]:                 # assume the last line may be incomplete
                                self.log(line)
                        self.LineBuf = lines[-1]

        @synchronized
        def     flush(self):
                if self.LineBuf:
                        self.log(self.LineBuf)
                        self.LineBuf = ''
                
