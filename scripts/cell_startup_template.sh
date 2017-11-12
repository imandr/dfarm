#!/bin/sh -f

# Disk Farm Cell startup script template.
# Administrator: edit the following definitions:

#------------------------------------------------
dfarm_root=/home/farms/dfarm_root
#------------------------------------------------

case "$1" in

'start')
	# start cell manager
	${dfarm_root}/bin/start_cellmgr.sh
	;;
	
'stop')
	;;
	
*)
	echo "usage: $0 {start|stop}"
	;;
esac

exit 0
