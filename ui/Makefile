#
# @(#) $Id: Makefile,v 1.1 2001/04/04 15:34:40 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.1  2001/04/04 15:34:40  ivm
# Added Makefiles
#
#

DSTBIN = $(DSTROOT)/bin

BINFILES = dfarm

upstar: $(DSTBIN)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod ugo+r $(BINFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	

