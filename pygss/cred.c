
/*

Python GSS-API 
Copyright (C) 2000, David Margrave

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

*/


#include <Python.h>
#include <gssapi.h>
#include <string.h>
#include "gss.h"

gssCred *new_gssCred()
{
  gssCred *self;

  self = (gssCred *)malloc(sizeof(gssCred));
  self->cred=NULL;
  
  return self;
}

void delete_gssCred(gssCred *self)
{
  OM_uint32 gMin;

  if ((self->cred)!=NULL)
    gss_release_cred(&gMin, (&self->cred));
  free(self);
}

PyObject *gssCred_acquire(gssCred *self,
                          gssName *desired_name,
                          int time_req,
                          gss_OID_set desired_mechs,
                          gss_cred_usage_t cred_usage)
{
  OM_uint32 gMin, gMaj;
  PyObject *outTuple, *oidList;
  gss_OID_set actual_mechs;
  OM_uint32 time_rec;
  int i;
  

  gMaj = gss_acquire_cred(&gMin,
                          desired_name->name,
                          time_req,
                          desired_mechs,
                          cred_usage,
                          &(self->cred),
                          &actual_mechs,
                          &time_rec);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
    return NULL;
  }

  
  oidList = PyList_New(actual_mechs->count);
  for (i=0;i<actual_mechs->count;i++) {
    PyList_SetItem(oidList, 
                   i,
                   Py_BuildValue("s#",
                                 actual_mechs->elements[i].elements,
                                 actual_mechs->elements[i].length));
  }

  outTuple=PyTuple_New(2);
  PyTuple_SetItem(outTuple, 0, Py_BuildValue("i", time_rec));
  PyTuple_SetItem(outTuple, 1, oidList);

  gss_release_oid_set(&gMin, &actual_mechs);

  return outTuple;

}


/* return a list rather than tuple, so that the shadow class can 
   manipulate the gss_name_t that we return */
PyObject *gssCred_inquire(gssCred *self)
{
  OM_uint32 gMin, gMaj;
  PyObject *outList, *oidList;
  gss_OID_set actual_mechs;
/*
  gss_name_t name;
*/
  int i;
  OM_uint32 lifetime;
  gss_cred_usage_t cred_usage;
  gss_OID_set mechs;
  /* borrowed from swig, to turn our name_t into a gssName */
  PyObject * _resultobj;
  gssName *name;
  char _ptemp[128];

  name = (gssName *)malloc(sizeof(gssName));

  gMaj = gss_inquire_cred(&gMin,
                          self->cred,
                          &(name->name),
                          &lifetime,
                          &cred_usage,
                          &mechs);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  oidList = PyList_New(mechs->count);
  for (i=0;i<mechs->count;i++) {
    PyList_SetItem(oidList, 
                   i,
                   Py_BuildValue("s#",
                                 mechs->elements[i].elements,
                                 mechs->elements[i].length));
  }
  gss_release_oid_set(&gMin, &mechs);


  outList=PyList_New(4);
  SWIG_MakePtr(_ptemp, (char *) name,"_gssName_p");
  PyList_SetItem(outList, 0, Py_BuildValue("s", _ptemp));
  PyList_SetItem(outList, 1, Py_BuildValue("i", lifetime));
  PyList_SetItem(outList, 2, Py_BuildValue("i", cred_usage));
  PyList_SetItem(outList, 3, oidList);

  return outList;

}
