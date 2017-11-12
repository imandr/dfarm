#!/usr/bin/python

# Python GSS-API
# Copyright (C) 2000, David Margrave
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


import gss
import string
import common
import holygrail
from socket import *

s = socket(AF_INET, SOCK_STREAM)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind('', 9999)
s.listen(1)

vcred = gss.gssCred()
vcred.acquire(None, 3600,[gss.GSS_MECH_KRB5],gss.GSS_PY_ACCEPT)

vctx = gss.gssContext()

conn, addr = s.accept()

print 'Connect from', addr

while (1):
  itoken = common.recv_msg(conn)
  (cont_needed, iname, imechs, otoken, ret_flags, time_rec, icred) = vctx.accept(vcred, itoken) 
  common.send_msg(conn, otoken)
  if not cont_needed: break

print "\ncontext info"
print "-=-=-=-=-=-=-=-=-"
ctxinfo = vctx.inquire()
(src_name,targ_name, time_rec, mech_type, context_flags) = ctxinfo
print "initator: %s" % src_name.display()
print "acceptor: %s" % targ_name.display()
print "Context time: %d" % time_rec
print "mech: %s" % common.b2a(mech_type)
print "context flags: %04x" % context_flags

for i in xrange(0, len(holygrail.quotes)):
  msgtoken = common.recv_msg(conn)
  (token, conf_state, qop_state)= vctx.unwrap(msgtoken)
  print "received message: %s" % token
  print "conf_state: %d" % conf_state
  print "qop_state: %d" % qop_state
  message = holygrail.quotes[token]
  print "wrapping message: %s" % message
  (token, conf_state) = vctx.wrap(1,0,message)
  common.send_msg(conn, token)

 
 


conn.close()

