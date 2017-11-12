
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
#

SWIG=$(SWIG_DIR)/bin/swig
# -DSWIG_GLOBAL allows me to use SWIG_MakePtr in my code, say for
# returning a list object with one of its members that is a gssName

PYTHON_INCLUDES=-I$(PYTHON_DIR)/include/python$(PYTHONVERSION)
GSS_INCLUDES=-I$(KERBEROS_DIR)/include/gssapi
GSS_LIBS=-L$(KERBEROS_DIR)/lib -lgssapi_krb5 -lkrb5 -lcom_err
CFLAGS = -g -DSWIG_GLOBAL $(N32)
OBJS= name.o cred.o context.o exception.o error.o constants.o

PYMODULES = gss.py constants.py common.py

LIBDIR = $(TARGET)



default: install

install:
	. build-env.sh; make _install
	

build: $(TARGETS) $(PYMODULES)

_install: $(LIBDIR) build
	cp $(LIBFILES) $(PYMODULES) $(LIBDIR)
	cp README $(LIBDIR)
	cd $(LIBDIR)
	chmod ugo+rx $(LIBFILES) $(PYMODULES) README

gsscmodule.so: gss_wrap.o $(OBJS)
	$(LDSHARED) $(N32) gss_wrap.o $(OBJS) $(GSS_LIBS) -o gsscmodule.so

.c.o:
	$(CC) $(CFLAGS) $(PYTHON_INCLUDES) $(GSS_INCLUDES) -c $<

gss_wrap.c: gss.i typemaps.i constants.i
	$(SWIG) -python gss.i

clean:
	rm -f *.o *.pyc gsscmodule.so core gss_wrap.c

$(LIBDIR):
	mkdir -p $(LIBDIR)
