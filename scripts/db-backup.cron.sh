#!/bin/sh -f
#
# @(#) $Id: db-backup.cron.sh,v 1.1 2001/11/02 16:38:18 ivm Exp $
#
#

# Edit the following lines: --------------
tardir=/home/farms/dfarm_root/backup	# where to store VFS DB backup files
dbdir=/home/farms/dfarm_root/db 	# location of VFS DB
purge_days=7						# how long backup files should be kept
#-----------------------------------------

#------------------------------------------------
#	purge old backup files
#------------------------------------------------
find $tardir -type f -name 'db.*' -mtime +$purge_days -exec rm -f {} \;

#------------------------------------------------
#	create new backup file
#------------------------------------------------
tarfile=${tardir}/db.`date +%y-%m-%d-%H:%M:%S`.tar
cd $dbdir
tar cf $tarfile .
compress $tarfile

