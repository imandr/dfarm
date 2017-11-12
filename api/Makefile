#
# @(#) $Id: Makefile,v 1.1 2001/04/04 15:34:39 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.1  2001/04/04 15:34:39  ivm
# Added Makefiles
#
#

DSTLIB = $(DSTROOT)/lib 

LIBFILES = dfarm_api.py

upstar: $(DSTLIB)
	cp $(LIBFILES) $(DSTLIB)
	cd $(DSTLIB); chmod ugo+r $(LIBFILES)

$(DSTLIB):	
	mkdir -p $(DSTLIB)
	

