#!/bin/sh -f
#
# @(#) $Id: df_ftpd.sh,v 1.2 2003/01/03 20:03:29 ivm Exp $
#

cd $DFARM_DIR/bin
exec python ftpd.py "$@"
