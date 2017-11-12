#
# @(#) $Id: Makefile,v 1.2 2002/09/10 17:37:56 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.2  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.1  2001/04/04 15:34:39  ivm
# Added Makefiles
#
#

DSTBIN = $(DSTROOT)/bin

BINFILES = CellListener.py    Replicator.py      cellmgr.sh \
	CellStorage.py     VFSSrvIF.py        cellmgr_global.py \
	DataMover.py       cellmgr.py         txns.py \
	Repeater.py repeater.sh

upstar: $(DSTBIN)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod ugo+rx $(BINFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	

