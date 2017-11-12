#
# @(#) $Id: Makefile,v 1.20 2003/12/04 17:03:13 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.20  2003/12/04 17:03:13  ivm
# v3_0
#
# Revision 1.19  2003/03/25 17:36:46  ivm
# Implemented non-blocking directory listing transmission
# Implemented single inventory walk-through
# Implemented re-tries on failed connections to VFS Server
#
# Revision 1.18  2003/01/03 18:14:27  ivm
# Fixed channel bindings
#
# Revision 1.17  2002/10/31 17:52:32  ivm
# v2_3
#
# Revision 1.16  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.14  2002/08/16 19:18:28  ivm
# Implemented size estimates for ftpd
#
# Revision 1.13  2002/08/12 16:29:42  ivm
# Implemented cell indeces
# Kerberized ftpd
#
# Revision 1.12  2002/07/16 18:44:39  ivm
# Implemented data attractions
# v2_1
#
# Revision 1.11  2002/05/02 17:53:49  ivm
# v2_0, tested, fixed minor bugs in API
#
# Revision 1.10  2002/04/30 20:07:15  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.9  2001/10/24 20:50:53  ivm
# Avoid sleeping in Timer.run()
#
# Revision 1.8  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.7  2001/06/29 18:53:52  ivm
# Implemented get/put timeouts, v1_5
#
# Revision 1.6  2001/06/15 22:12:24  ivm
# Fixed bug with replication stall
#
# Revision 1.5  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.4  2001/05/26 15:31:08  ivm
# Improved cell stat
#
# Revision 1.3  2001/04/23 22:21:12  ivm
# Implemented quota, file ownership and protection
#
# Revision 1.2  2001/04/04 20:19:03  ivm
# Get replication working
#
# Revision 1.1  2001/04/04 15:34:39  ivm
# Added Makefiles
#
#

VERSION = v3_0

DSTBIN = $(DSTROOT)/bin
DSTUPS = $(DSTROOT)/ups
DSTDOC = $(DSTROOT)/doc
DSTLIB = $(DSTROOT)/lib 

DSTDIRS = $(DSTBIN) $(DSTLIB) $(DSTUPS) $(DSTDOC)

UPSTAR = /tmp/dfarm_$(VERSION)_$(OS).tar

upstar:
	@DSTROOT=`pwd`/dstroot; OS=`uname`; export DSTROOT OS;\
	make _upstar
	
_upstar:	makedirs tarfile cleanup

tarfile:
	cd cell;	make upstar
	cd ftp;		make upstar
	cd vfs; 	make upstar
	cd api; 	make upstar
	cd ui;		make upstar
	cd cfg; 	make upstar
	cd scripts; make upstar
	cd ups; 	make upstar
	cd c-src;	make upstar
	cd doc;		make upstar
	cd misc;	make upstar
	#cd pygss;	make TARGET=$(DSTROOT)/bin install
	cd $(DSTROOT); tar cf $(UPSTAR) .
	@echo -------------------------------------
	@echo   UPS tarfile $(UPSTAR) is ready
	@echo -------------------------------------
	
	
makedirs:	cleanup
	mkdir -p $(DSTDIRS)
	
cleanup:
	rm -rf $(DSTROOT)
	
clean:
	cd pygss;	make clean
