#
# @(#) $Id: cellmgr_global.py,v 1.3 2001/10/12 21:12:02 ivm Exp $
#
# $Log: cellmgr_global.py,v $
# Revision 1.3  2001/10/12 21:12:02  ivm
# Fixed bug with double-slashes
# Redone remove-on-put
# Implemented log files
#
# Revision 1.2  2001/06/15 19:54:25  ivm
# Implemented full cell-vfsdb synchronization
# Implemented "ln"
#
# Revision 1.1  2001/04/04 14:25:48  ivm
# Initial CVS deposit
#
#

CellStorage = None
DataMover = None
DataServer = None
CellListener = None
VFSSrvIF = None
Timer = None
LogFile = None
