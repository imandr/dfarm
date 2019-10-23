UPSFILES = dfarm.table setup_dfarm.csh.template setup_dfarm.sh.template \
	tailor.sh

build: $(DSTUPS)
	cp $(UPSFILES) $(DSTUPS)
	cd $(DSTUPS); chmod ugo+rx $(UPSFILES)

$(DSTUPS):	
	mkdir -p $(DSTUPS)
	

