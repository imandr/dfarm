#!/bin/sh -f

#
# Disk Farm VFS Server and broadcast repeater startup script template.
# Administrator: edit the following definitions:
#
#------------------------------------------------
dfarm_root=/home/farms/dfarm_root
dfarm_user=farms
#--------------- end of definitions -------------

case "$1" in

'start')
	# start cell manager
	su ${dfarm_user} -c ${dfarm_root}/bin/start_vfssrv.sh
	
	# start broadcast repeater
	#su ${dfarm_user} -c ${dfarm_root}/bin/start_repeater.sh

	# start FTP server
	#su ${dfarm_user} -c ${dfarm_root}/bin/start_ftpd.sh
	;;
	
'stop')
	;;
	
*)
	echo "usage: $0 {start|stop}"
	;;
esac

exit 0
