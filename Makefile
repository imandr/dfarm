VERSION = 3.0

DSTROOT = $(HOME)/build/dfarm

DSTBIN = $(DSTROOT)/bin
DSTUPS = $(DSTROOT)/ups
DSTDOC = $(DSTROOT)/doc
DSTLIB = $(DSTROOT)/lib 
DSTCFG = $(DSTROOT)/cfg
TARDIR = /tmp/$(USER)

DSTDIRS = $(DSTBIN) $(DSTLIB) $(DSTUPS) $(DSTDOC) $(TARDIR)

UPSTAR = $(TARDIR)/dfarm_$(VERSION).tar

all: clean makedirs build tarfile

build:
	cd cell;	make DSTBIN=$(DSTBIN) DSTLIB=$(DSTLIB) build
	#cd ftp;		make upstar
	cd vfs; 	make DSTBIN=$(DSTBIN) DSTLIB=$(DSTLIB) build
	cd lib; 	make DSTBIN=$(DSTBIN) DSTLIB=$(DSTLIB) build
	cd ui;		make DSTBIN=$(DSTBIN) DSTLIB=$(DSTLIB) build
	cd cfg; 	make DSTCFG=$(DSTCFG) build
	cd scripts; 	make DSTBIN=$(DSTBIN) DSTLIB=$(DSTLIB) build
	cd ups; 	make DSTBIN=$(DSTBIN) DSTLIB=$(DSTLIB) DSTUPS=$(DSTUPS) build
	#cd c-src;	make upstar
	#cd doc;		make upstar
	#cd misc;	make upstar
	#cd pygss;	make TARGET=$(DSTROOT)/bin install

tarfile:	$(TARDIR)
	cd $(DSTROOT); tar cf $(UPSTAR) .
	@echo -------------------------------------
	@echo   UPS tarfile $(UPSTAR) is ready
	@echo -------------------------------------
	
	
makedirs:	
	mkdir -p $(DSTDIRS)
	
clean:
	rm -rf $(DSTROOT)

	
