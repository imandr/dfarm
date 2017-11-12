#
# @(#) $Id: Makefile,v 1.7 2003/12/04 16:52:28 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.7  2003/12/04 16:52:28  ivm
# Implemented BSD DB - based VFS DB
# Use connect with time-out for data communication
#
# Revision 1.6  2002/09/10 17:37:56  ivm
# Added ftpd startup scripts
#
# Revision 1.5  2001/11/02 16:37:51  ivm
# Added db-backup.cron.sh and kill_all.sh
#
# Revision 1.4  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.3  2001/06/26 18:18:24  ivm
# Added system boot time script templates
#
# Revision 1.2  2001/04/04 20:51:06  ivm
# Added scripts, statfsmodule
#
# Revision 1.1  2001/04/04 15:34:40  ivm
# Added Makefiles
#
#

DSTBIN = $(DSTROOT)/bin

BINFILES = start_cellmgr.sh start_vfssrv.sh \
	vfs_startup_template.sh cell_startup_template.sh \
	kill_cellmgr.sh kill_vfssrv.sh restart_cellmgr.sh \
	db-backup.cron.sh kill_all.sh start_repeater.sh \
	start_ftpd.sh cvtdb.sh

upstar: $(DSTBIN)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod ugo+rx $(BINFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	

