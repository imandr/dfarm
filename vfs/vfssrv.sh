#!/bin/sh -f
#
# @(#) $Id: vfssrv.sh,v 1.2 2002/04/30 20:07:16 ivm Exp $
#
# $Log: vfssrv.sh,v $
# Revision 1.2  2002/04/30 20:07:16  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#


cd $DFARM_DIR/bin
python vfssrv.py
