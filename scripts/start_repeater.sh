#!/bin/sh -f
#
# @(#) $Id: start_repeater.sh,v 1.1 2002/09/03 16:34:50 ivm Exp $
#

# -- Edit these lines ------------------------------------------------
upsdir=/fnal/ups/etc               # location of setups.sh
stdout=/dev/null                   # stdout and
stderr=/var/log/repeater.err       # stderr of repeater
# --------------------------------------------------------------------

. ${upsdir}/setups.sh


if ( ups exist -q local_python dfarm ) ;then
	setup -q local_python dfarm
else
	setup dfarm
fi


$DFARM_DIR/bin/repeater.sh </dev/null >$stdout 2>$stderr &
