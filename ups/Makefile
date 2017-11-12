#
# @(#) $Id: Makefile,v 1.3 2002/04/30 20:07:15 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.3  2002/04/30 20:07:15  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.2  2001/11/05 16:32:49  ivm
# Added setup_dfarm.csh, .sh templates, tailor.sh
#
# Revision 1.1  2001/04/04 15:34:40  ivm
# Added Makefiles
#
#

DSTUPS = $(DSTROOT)/ups

UPSFILES = dfarm.table setup_dfarm.csh.template setup_dfarm.sh.template \
	tailor.sh

upstar: $(DSTUPS)
	cp $(UPSFILES) $(DSTUPS)
	cd $(DSTUPS); chmod ugo+rx $(UPSFILES)

$(DSTUPS):	
	mkdir -p $(DSTUPS)
	

