#!/bin/sh
#
# @(#) $Id: kill_all.sh,v 1.1 2001/11/02 16:37:51 ivm Exp $
#
# kill_all.sh will kill all processes running command
# (including arguments) having given string as a substring.
#
# Usage:
#
#       kill_all.sh [-signal] string
#
#       Note that string may be grep-compatible regular expression
#

if [ "${#}" = "2" ] ;then
        sig="$1"
        ptrn="$2"
elif [ "${#}" = "1" ] ;then
		sig=""
		ptrn="$1"
else
        echo "Usage: kill_all [-sig] pattern"
        exit 1
fi

os=`uname`
case $os in
"IRIX"|"SunOS")
	pids=`ps -ef | grep -e "$ptrn" | grep -v grep | grep -v kill_all | cut -c 9-14`
	;;

"IRIX64")
	pids=`ps -ef | grep -e "$ptrn" | grep -v grep | grep -v kill_all | cut -c 9-19`
	;;

"Linux")
	pids=`ps axw | grep -e "$ptrn" | grep -v grep | grep -v kill_all | cut -c 1-5`
	;;

"OSF1")
	pids=`ps axw | grep -e "$ptrn" | grep -v grep | grep -v kill_all | cut -c 9-15`
	;;
esac

if [ ! -z "$pids" ]; then
	kill $sig $pids
fi

