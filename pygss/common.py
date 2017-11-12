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


import socket
import string

# convert string to ascii octets
def b2a(s):
    bytes = map(lambda x: '%.2x' % x, map(ord, s))
    disp=[]
    for i in xrange(0, len(bytes)/16):
      disp.append(string.join(bytes[i*16:i*16+16]))
    disp.append(string.join(bytes[(len(bytes)/16)*16:]))
    return string.join(disp, '\n')

# convert string to integer
def s2i(s):
  x=ord(s[0])
  for i in xrange(1, len(s)):
    x=(x<<8)+ord(s[i])
  return x

# convert integer to string
def i2s(x):
  s=[]
  while (1):
    s.append(chr(x%256))
    x=x>>8
    if (not x):
      break
  return string.join(s,'')
  

def send_msg(sock, data):
  if (len(data)>pow(2, 16)):
    raise socket.error, "data too large for send_msg()"
  s = i2s(socket.htons(len(data)))
  print "send_msg:\n%s" % b2a(s + data)
  sock.send(s + data)

def recv_msg(sock):
  s = sock.recv(2)
  if not s:
    raise socket.error, "recv() len failed"
  len = socket.ntohs(s2i(s))
  data = sock.recv(len)
  if not data:
    raise socket.error, "recv() len failed"
  print "recv_msg:\n%s" % b2a(s + data)
  return data 

