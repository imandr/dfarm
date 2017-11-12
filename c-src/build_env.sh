#!/bin/sh -f
#
# @(#) $Id: build_env.sh,v 1.2 2002/04/30 20:07:15 ivm Exp $
#
# $Log: build_env.sh,v $
# Revision 1.2  2002/04/30 20:07:15  ivm
# Implemented and tested:
# 	node replication
# 	node hold/release
#
# Revision 1.1  2001/04/04 20:51:05  ivm
# Added scripts, statfsmodule
#
# Revision 1.1  2000/08/14 16:02:00  ivm
# Initial versions
#
#

TARGETS=""
N32=""

if [ -z "$PYTHON_DIR" ] ;then
	echo Python is not set up
	exit 1
fi

PYTHONVERSION=1.5
if [ -r $PYTHON_DIR/include/python2.1 ] ;then
	PYTHONVERSION=2.1
fi

case `uname` in
	IRIX*)
		N32="-n32"
		;;
	Linux*)
		TARGETS="statfsmodule.so"
		;;
	*)
		;;
esac

export TARGETS N32 PYTHONVERSION
