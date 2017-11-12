#!/bin/sh -f
#
# @(#) $Id: start_ftpd.sh,v 1.2 2003/01/03 20:03:30 ivm Exp $
#

# -- Edit these lines ------------------------------------------
upsdir=/fnal/ups/etc	# location of setups.sh
dfarm_user=farms		# user to run VFS server as
stdout=/dev/null		# stdout and
stderr=/dev/null		# stderr of VFS Server
# --------------------------------------------------------------

. ${upsdir}/setups.sh

if ( ups exist -q local_python dfarm ) ;then
	setup -q local_python dfarm
else
	setup dfarm
fi

$DFARM_DIR/bin/df_ftpd.sh "$@" </dev/null >$stdout 2>$stderr &
