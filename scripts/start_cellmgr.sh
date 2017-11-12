#!/bin/sh -f
#
# @(#) $Id: start_cellmgr.sh,v 1.4 2002/04/30 20:07:15 ivm Exp $
#

# -- Edit these lines ------------------------------------------------
upsdir=/fnal/ups/etc               # location of setups.sh
stdout=/dev/null                   # stdout and
stderr=/var/log/cellmgr.err        # stderr of cell manager
# --------------------------------------------------------------------

. ${upsdir}/setups.sh


if ( ups exist -q local_python dfarm ) ;then
	setup -q local_python dfarm
else
	setup dfarm
fi


$DFARM_DIR/bin/cellmgr.sh </dev/null >$stdout 2>$stderr &
