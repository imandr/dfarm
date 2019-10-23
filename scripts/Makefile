BINFILES = start_cellmgr.sh start_vfssrv.sh \
	vfs_startup_template.sh cell_startup_template.sh 


	#kill_cellmgr.sh kill_vfssrv.sh restart_cellmgr.sh \
	#db-backup.cron.sh kill_all.sh start_repeater.sh \
	#start_ftpd.sh cvtdb.sh

build: $(DSTBIN)
	cp $(BINFILES) $(DSTBIN)
	cd $(DSTBIN); chmod +rx $(BINFILES)

$(DSTBIN):	
	mkdir -p $(DSTBIN)
	

