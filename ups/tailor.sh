#!/bin/sh -f
#
# @(#) $Id: tailor.sh,v 1.1 2001/11/05 16:32:49 ivm Exp $
#

root=${UPS_OPTIONS}

if [ -z "$root" ] ; then
	echo "Usage: ups tailor [-dct] -O <dfarm root path> <product>"
	exit 1
fi

sed -e s+_ROOT_+${root}+g < ${UPS_PROD_DIR}/ups/setup_dfarm.sh.template \
		> ${UPS_PROD_DIR}/bin/setup_dfarm.sh

sed -e s+_ROOT_+${root}+g < ${UPS_PROD_DIR}/ups/setup_dfarm.csh.template \
		> ${UPS_PROD_DIR}/bin/setup_dfarm.csh

chmod ugo+rx ${UPS_PROD_DIR}/bin/setup_dfarm.csh
chmod ugo+rx ${UPS_PROD_DIR}/bin/setup_dfarm.sh
