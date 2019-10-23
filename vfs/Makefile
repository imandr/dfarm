BINFILES = CellIF.py                         vfssrv.sh \
	VFSServer.py            vfssrv_global.py \
	VFSDB.py                vfssrv.py	QuotaMgr.py \
	cvtdb.py


#LIBFILES = 		# VFSFileInfo.py

build: $(DSTBIN) $(DSTLIB)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod ugo+rx $(BINFILES)
	#cp $(LIBFILES) $(DSTLIB)
	#cd $(DSTLIB); chmod ugo+rx $(LIBFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	
$(DSTLIB):	
	mkdir -p $(DSTLIB)
	

