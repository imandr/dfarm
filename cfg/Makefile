CFGFILES = dfarm.cfg

build: $(DSTCFG)
	cp $(CFGFILES) $(DSTCFG)
	cd $(DSTCFG); chmod ugo+r $(CFGFILES)

$(DSTCFG):	
	mkdir -p $(DSTCFG)
	

