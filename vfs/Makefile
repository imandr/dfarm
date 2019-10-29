BINFILES = CellIF2.py                         vfssrv.sh \
	VFSServer2.py            vfssrv_global.py \
	VFSDB2.py VFSDBStorage.py vfssrv.py	QuotaMgr.py 


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
	

