#
# @(#) $Id: Makefile,v 1.4 2003/12/04 16:52:28 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.4  2003/12/04 16:52:28  ivm
# Implemented BSD DB - based VFS DB
# Use connect with time-out for data communication
#
# Revision 1.3  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.2  2001/04/11 20:59:50  ivm
# Fixed some bugs
#
# Revision 1.1  2001/04/04 15:34:40  ivm
# Added Makefiles
#
#

DSTBIN = $(DSTROOT)/bin
DSTLIB = $(DSTROOT)/lib

BINFILES = CellIF.py                         vfssrv.sh \
	TCPClientConnection.py  VFSServer.py            vfssrv_global.py \
	VFSDB.py                vfssrv.py	QuotaMgr.py \
	cvtdb.py


LIBFILES = VFSFileInfo.py

upstar: $(DSTBIN)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod ugo+rx $(BINFILES)
	cp $(LIBFILES) $(DSTLIB)
	cd $(DSTLIB); chmod ugo+rx $(LIBFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	
$(DSTLIB):	
	mkdir -p $(DSTLIB)
	

