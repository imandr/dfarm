#
# @(#) $Id: Makefile,v 1.5 2002/04/30 20:07:15 ivm Exp $
#
# $Log: Makefile,v $
# Revision 1.5  2002/04/30 20:07:15  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.4  2001/04/12 16:02:31  ivm
# Fixed Makefiles
# Fixed for fcslib 2.0
#
# Revision 1.3  2001/04/11 20:59:50  ivm
# Fixed some bugs
#
# Revision 1.2  2001/04/04 20:51:05  ivm
# Added scripts, statfsmodule
#
# Revision 1.1  2001/04/04 15:34:39  ivm
# Added Makefiles
#
#

DSTBIN = $(DSTROOT)/bin

#OBJS = statfsmodule.so

upstar:
	. build_env.sh; make _upstar

_upstar: $(TARGETS) $(BINDIR)
	if [ ! -z "$(TARGETS)" ]; then cp $(TARGETS) $(DSTBIN); \
		cd $(DSTBIN); chmod ugo+rx $(TARGETS) ; fi

$(DSTBIN):	
	mkdir -p $(DSTBIN)

statfsmodule.so:	statfsmodule.o
	ld -shared $(N32) -o statfsmodule.so statfsmodule.o
	

statfsmodule.o: statfsmodule.c
	cc -c $(N32) -o statfsmodule.o \
		-I$(PYTHON_DIR)/include/python$(PYTHONVERSION) statfsmodule.c

