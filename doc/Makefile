#
# @(#) $Id: Makefile,v 1.2 2001/09/27 20:37:24 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.2  2001/09/27 20:37:24  ivm
# Fixed some bugs
# Introduced cell class in configuration for heterogenous dfarms
#
# Revision 1.1  2001/04/04 15:34:39  ivm
# Added Makefiles
#
#

DSTDOC = $(DSTROOT)/doc

DOCFILES = README ABSTRACT WEB_LINKS

upstar: $(DSTDOC)
	cp $(DOCFILES) $(DSTDOC)
	cd $(DSTDOC); chmod ugo+r $(DOCFILES)

$(DSTDOC):	
	mkdir -p $(DSTDOC)
	

