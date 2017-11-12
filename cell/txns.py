#
# @(#) $Id: txns.py,v 1.1 2001/04/04 14:25:48 ivm Exp $
#
# $Log: txns.py,v $
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

class	Transaction:
	_NextID = 1

	def __init__(self, type, size):
		self.ID = Transaction._NextID
		Transaction._NextID = (Transaction._NextID % 100000) + 1
		self.Type = type
		self.Size = size
		self.ActualSize = size
		self.NotifyList = []
						
	def type(self):
		return self.Type
		
	def size(self):
		return self.Size

	def actualSize(self):
		return self.ActualSize

	def client(self):
		return self.Client

	def notify(self, subj):
		self.NotifyList.append(subj)
		subj.openTxn(self.ID, self)
		
	def commit(self, actSize = None):
		if actSize != None:
			self.ActualSize = actSize
		for subj in self.NotifyList:
			subj.commitTxn(self.ID)
		self.NotifyList = []
		
	def rollback(self):
		for subj in self.NotifyList:
			subj.rollbackTxn(self.ID)
		self.NotifyList = []
	
class	ULTxn(Transaction):
	def __init__(self, size, psa):
		Transaction.__init__(self, 'U', size)
		self.notify(psa)

class	DLTxn(Transaction):
	def __init__(self, size, psa):
		Transaction.__init__(self, 'D', size)
		self.notify(psa)

class	HasTxns:
	def __init__(self, size = 0, used = 0):
		self.Size = size
		self.Txnd = {}
		self.Used = used

	def reservedByTxns(self):
		# calculate how much space is still unused and unresreved
		rtxns = 0
		for tid, txn in self.Txnd.items():
			if txn.type() == 'U':
				rtxns = rtxns + txn.size()
		return rtxns

	def openTxn(self, tid, txn):
		self.Txnd[tid] = txn

	def rollbackTxn(self, tid):
		if self.Txnd.has_key(tid):
			del self.Txnd[tid]
	
	def commitTxn(self, tid):
		if self.Txnd.has_key(tid):
			txn = self.Txnd[tid]
			del self.Txnd[tid]
			if txn.type() == 'U':
				self.Used = self.Used + txn.actualSize()

	def used(self): 	# can be overriden
		return self.Used

	def free(self): 	# can be overriden
		# calculate how much space is still unused and unresreved
		f = self.Size - self.used() - self.reservedByTxns()

	def txnCount(self, type='*'):
		#print 'txnCount: type=%s' % type
		if type == '*':
			return len(self.Txnd)
		n = 0
		for tid, txn in self.Txnd.items():
			#print tid, txn.type(), txn.Type
			if txn.type() == type:
				n = n + 1
		#print 'returning %d' % n
		return n

	def txnList(self):
		return self.Txnd.values()
