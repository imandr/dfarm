BINFILES = CellListener.py    cellmgr.sh \
	CellStorage.py     VFSSrvIF.py        cellmgr_global.py \
	DataServer.py       cellmgr.py         txns.py \
	Repeater.py repeater.sh 

build: $(DSTBIN)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod ugo+rx $(BINFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	

