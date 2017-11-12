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
import common
import holygrail
from socket import *

s = socket(AF_INET, SOCK_STREAM)
s.connect('linuxbox.linux.premier1.net', 9999)

icred = gss.gssCred()
icred.acquire(None,3600,[gss.GSS_MECH_KRB5],gss.GSS_PY_INITIATE)

target = gss.gssName()
target.import_name('host@linuxbox', gss.GSS_NT_SERVICE_NAME)

ictx = gss.gssContext()
itoken=gss.GSS_PY_NO_BUFFER
while (1):
  (cont_needed, otoken) = ictx.init(icred,
                       target,
                       gss.GSS_MECH_KRB5,
                       gss.GSS_PY_MUTUAL_FLAG |
                        gss.GSS_PY_CONF_FLAG | 
                        gss.GSS_PY_INTEG_FLAG,
                       0,
                       itoken)
  if not cont_needed: break
  common.send_msg(s,otoken)
  itoken = common.recv_msg(s)

print "\ncontext info"
print "-=-=-=-=-=-=-=-=-"
ctxinfo = ictx.inquire()
(src_name,targ_name, time_rec, mech_type, context_flags) = ctxinfo
print "initator: %s" % src_name.display()
print "acceptor: %s" % targ_name.display()
print "Context time: %d" % time_rec
print "mech: %s" % common.b2a(mech_type)
print "context flags: %04x" % context_flags


# wrap test

for message in holygrail.quotes.keys():
  print "wrapping message: %s" % message
  (token, conf_state) = ictx.wrap(1,0,message)
  common.send_msg(s, token)
  resptoken = common.recv_msg(s)
  (token, conf_state, qop_state)= ictx.unwrap(resptoken)
  print "received reply: %s" %  token
  print "conf_state: %d" % conf_state
  print "qop_state: %d" % qop_state



s.close()
