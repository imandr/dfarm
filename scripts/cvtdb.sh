#!/bin/sh -f

if [ -z "${DFARM_CONFIG}" ] ;then
	echo DFARM_CONFIG is not defined
	echo Use "setup dfarm"
	exit 1
fi

if [ -z "${DFARM_DIR}" ] ;then
	echo DFARM_DIR is not defined
	echo Use "setup dfarm"
	exit 1
fi

python $DFARM_DIR/bin/cvtdb.py
