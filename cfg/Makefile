#
# @(#) $Id: Makefile,v 1.1 2001/04/04 15:34:39 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.1  2001/04/04 15:34:39  ivm
# Added Makefiles
#
#

DSTCFG = $(DSTROOT)/cfg

CFGFILES = dfarm.cfg

upstar: $(DSTCFG)
	cp $(CFGFILES) $(DSTCFG)
	cd $(DSTCFG); chmod ugo+r $(CFGFILES)

$(DSTCFG):	
	mkdir -p $(DSTCFG)
	

