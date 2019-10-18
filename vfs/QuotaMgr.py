#
# @(#) $Id: QuotaMgr.py,v 1.8 2003/03/25 17:36:46 ivm Exp $
#
# $Log: QuotaMgr.py,v $
# Revision 1.8  2003/03/25 17:36:46  ivm
# Implemented non-blocking directory listing transmission
# Implemented single inventory walk-through
# Implemented re-tries on failed connections to VFS Server
#
# Revision 1.7  2002/08/16 19:18:28  ivm
# Implemented size estimates for ftpd
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
# Revision 1.4  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.3  2001/05/30 20:34:28  ivm
# Implemented new QuotaManager
# Fixed bug in Replicator.abort()
# Increased Replication and api.get() time-outs
#
# Revision 1.2  2001/05/08 22:17:46  ivm
# Fixed some bugs
#
# Revision 1.1  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
#

import vfssrv_global
import time

class   QuotaDict:
        def __init__(self):
                self.Quota = {}
                
        def __getitem__(self, key):
                if key in self.Quota:
                        return self.Quota[key]
                elif '*' in self.Quota:
                        return self.Quota['*']
                else:
                        # generate appropriate exception
                        return self.Quota[key]

        def __setitem__(self, key, val):
                self.Quota[key] = val
                
        def __delitem__(self, key):
                del self.Quota[key]

        def has_key(self, key):
                return key in self.Quota or '*' in self.Quota
                
class   ResTicket:
        def     __init__(self, key, user, size, nrep, expt):
                self.Key = key
                self.Size = int(size)
                self.NRep = nrep
                self.ExpTime = expt
                self.User = user

        def     renew(self, expt):
                self.ExpTime = expt

        def     expired(self):
                return time.time() > self.ExpTime

        def     tearoff(self, n=1):
                self.NRep = self.NRep - n
                if self.NRep < 0:
                        self.NRep = 0

        def     reserved(self):
                return self.NRep * self.Size

        def __str__(self):
                return '<ResTicket <%s> %s*%s %s %s>' % (
                        self.Key, self.Size, self.NRep, time.ctime(self.ExpTime),
                        self.User) 

class   DictWithDef:
        def     __init__(self, dflt):
                self.Default = dflt
                self.Dict = {}
                
        def     __getitem__(self, key):
                if key in self.Dict:
                        return self.Dict[key]
                else:
                        return self.Default

        def     __setitem__(self, key, val):
                if val == self.Default:
                        try:    del self.Dict[key]
                        except: return
                else:
                        self.Dict[key] = val
                        
        def     __delitem__(self, key):
                del self.Dict[key]

        def     has_key(self, key):
                return key in self.Dict

class   QuotaManager:
        def __init__(self, cfg):
                self.Cfg = cfg
                self.Usage = DictWithDef(0)
                self.Quota = QuotaDict()
                self.Tickets = {}       # key -> ResTicket
                self.Quota = cfg.get("quota", {})
                self.TicketLifetime = 10*60     # 10 min
                self.LastIdle = 0
                                                                
        def calcUsage(self):
                vfssrv_global.G_VFSDB.walkTreeRec('/', 1, self.addUsageFcn, None, 0)

        def addUsageFcn(self, apath, typ, info, arg, summ):
                if typ == 'f':
                        siz = info.sizeMB() * info.mult()
                        user = info.Username
                        self.Usage[user] = self.Usage[user] + siz
                return summ

        def initInventory(self):
                self.Usage = DictWithDef(0)
                
        def inventoryCallback(self, apath, typ, info, arg, carry):
                if typ == 'f':
                        siz = info.sizeMB() * info.mult()
                        user = info.Username
                        self.Usage[user] = self.Usage[user] + siz
                return carry
                        
        def wouldExceedQuota(self, user, fsize = 0, mult = 1):
                u = self.Usage[user]
                r = self.calcReserved(user)
                #print 'wouldExceedQuota: u = %s, r = %s, size = %s, mult = %s' %\
                #       (u,r,fsize,mult)
                return user in self.Quota and \
                        self.Quota[user] - r < u + fsize*mult

        def calcReserved(self, user):
                r = 0
                for t in list(self.Tickets.values()):
                        if t.User == user:
                                        r = r + t.reserved()
                return r

        def createTicket(self, user, key, fsize, nrep):
                if key in self.Tickets:
                                del self.Tickets[key]
                tkt = ResTicket(key, user, fsize, nrep,
                        time.time() + self.TicketLifetime)
                self.Tickets[key] = tkt

        def cleanTickets(self):
                now = time.time()
                for k, t in list(self.Tickets.items()):
                        if t.expired():
                                del self.Tickets[k]

        def renewTicket(self, key):
                try:
                        self.Tickets.renew(time.time() + self.TicketLifefime)
                except KeyError:
                        pass

        def makeReservation(self, user, key, size, nrep):
                self.createTicket(user, key, size, nrep)

        def instanceDeleted(self, key, user, size):
                self.Usage[user] = self.Usage[user] - size
                if self.Usage[user] < 0:
                        self.Usage[user] = 0

        def instanceCreated(self, key, user, size):
                try:
                        tkt = self.Tickets[key]
                except KeyError:
                        self.Usage[user] = self.Usage[user] + size
                else:
                        user = tkt.User
                        size = tkt.Size
                        self.Usage[user] = self.Usage[user] + size
                        tkt.tearoff()
                        if tkt.reserved() > 0:
                                tkt.renew(time.time() + self.TicketLifetime)

        def updateReservation(self, key, size):
                try:
                        tkt = self.Tickets[key]
                        tkt.Size = size
                except KeyError:
                        return
                

        def delFile(self, key, user, fsize, mult = 1):
                try:    del self.Tickets[key]
                except: pass
                self.Usage[user] = self.Usage[user] - fsize * mult
                if self.Usage[user] < 0:
                        self.Usage[user] = 0

        def getUsage(self, user):
                u, r, q = 0, 0, None
                if user in self.Usage:
                        u = self.Usage[user]
                if user in self.Quota:
                        q = self.Quota[user]
                r = self.calcReserved(user)
                return u, r, q

        def idle(self):
                if time.time() < self.LastIdle + 60:    return
                self.cleanTickets()
                self.LastIdle = time.time()     
