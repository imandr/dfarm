LIBFILES = dfarm_api.py

build: $(DSTBIN) $(DSTLIB)
	cp $(LIBFILES) $(DSTLIB)
	cd $(DSTLIB); chmod ugo+rx $(LIBFILES)

$(DSTLIB):	
	mkdir -p $(DSTLIB)
	

