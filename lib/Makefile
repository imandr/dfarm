LIBFILES = DataClient.py  dfarm_api.py  dfconfig.py  LogFile.py  py3.py  Timer.py  VFSFileInfo.py logs.py

build: $(DSTBIN) $(DSTLIB)
	cp $(LIBFILES) $(DSTLIB)
	cd $(DSTLIB); chmod ugo+rx $(LIBFILES)

$(DSTLIB):	
	mkdir -p $(DSTLIB)
	

