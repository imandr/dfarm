#!/bin/sh 
#

machine=`uname`
LFLAG=""
N32=""
WOFF=""
CC="cc"
TARGETS="gsscmodule.so"
BINFILES=""
LIBFILES="gsscmodule.so"
LDSHARED="ld -shared"
PYTHONVERSION=1.5

if [ -z "$PYTHON_DIR" ] ;then
	echo ------------------------------------------------------
	echo "|" Error: PYTHON_DIR is not set
	echo ------------------------------------------------------
	exit 1
fi

if [ -r ${PYTHON_DIR}/include/python2.1 ] ;then
	PYTHONVERSION=2.1
fi


echo ------------------------------------------------------
echo "|" PYTHONVERSION is set to $PYTHONVERSION
echo ------------------------------------------------------
sleep 1

case $machine in
"IRIX"*)
    N32="-n32"
   ;;
"OSF1"*)
    LFLAG='-expect_unresolved  "*"'
   ;;
"Linux"*) 
    WOFF="-w"
    ;;
"SunOS"*|"SUN"*)
	LDSHARED="ld -G"
	;;
esac

export LFLAG N32 WOFF CC TARGETS BINFILES LIBFILES LDSHARED
export PYTHONVERSION
