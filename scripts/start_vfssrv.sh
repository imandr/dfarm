#!/bin/sh -f
#
# @(#) $Id: start_vfssrv.sh,v 1.4 2002/04/30 20:07:15 ivm Exp $
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

$DFARM_DIR/bin/vfssrv.sh </dev/null >$stdout 2>$stderr &
