import time

class   _TimerEvent:

        def __init__(self, nextt, interval, count, action, arg):
                if nextt <= 0:
                        nextt = time.time()
                self.NextT = nextt
                self.Interval = interval
                self.Count = count
                self.Action = action
                self.Arg = arg
                if self.Interval == None:
                        self.Count = 1
                        self.Interval = 0
                
        def trigger(self, t):
                if self.Count != None and self.Count <= 0:
                        return
                try:    self.Action(t, self.Arg)
                except: pass
                self.NextT = t + self.Interval
                if self.Count != None:
                        self.Count = self.Count - 1

class   Timer:
        def __init__(self):
                self.Schedule = []              # sorted list of _SDBTEvent
                self.Append = []
                self.LastRun = 0
                
        def run(self):
                self.Schedule = self.Schedule + self.Append
                self.Append = []
                # handle possible manual time shifts
                now = time.time()
                shift = 0
                if now < self.LastRun:
                        # time was shifted back
                        shift = self.LastRun - now
                if self.Schedule:
                        newscd = []
                        for event in self.Schedule:
                                tev = event.NextT
                                if shift > 0 and tev > now:
                                        tev = tev - shift
                                if event.Count == None or event.Count > 0:
                                        if now >= tev:
                                                event.trigger(now)
                                        newscd.append(event)
                        self.Schedule = newscd
                self.LastRun = now

        def nextt(self):
                mint = None
                for e in self.Schedule + self.Append:
                        if mint == None or e.NextT < mint:
                                if e.Count == None or e.Count > 0:
                                        mint = e.NextT
                return mint

        def addEvent(self, t, interval, count, action, arg):
                event = _TimerEvent(t, interval, count, action, arg)
                self.Append.append(event)
                return event

        def removeEvent(self, event):
                newscd = []
                for i in range(len(self.Schedule)):
                        if not self.Schedule[i] is event:
                                newscd.append(self.Schedule[i])
                self.Schedule = newscd
                newapp = []
                for i in range(len(self.Append)):
                        if not self.Append[i] is event:
                                newapp.append(self.Append[i])
                self.Append = newapp
