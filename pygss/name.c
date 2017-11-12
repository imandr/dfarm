
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

gssName NO_NAME={GSS_C_NO_NAME};

gssName *new_gssName(gss_buffer_t input_name,
                     gss_OID name_type)
{
  gssName *self;
  OM_uint32 gMaj, gMin;

  self = (gssName *)malloc(sizeof(gssName));
  self->name=NULL;

#if 0
  gMaj = gss_import_name(&gMin, input_name, name_type, &(self->name));
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }
#endif
  
  return self;
}

void delete_gssName(gssName *self)
{
  OM_uint32 gMin;

  if ((self->name)!=NULL)
    gss_release_name(&gMin, &(self->name));
  free(self);
}

void gssName_import_name(gssName *self,
                         gss_buffer_t input_name,
                         gss_OID name_type)
{
  OM_uint32 gMaj, gMin;

  gMaj = gss_import_name(&gMin, input_name, name_type, &(self->name));
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }
}


/*
gss_buffer_t gssName_display(gssName *self)
*/
PyObject *gssName_display(gssName *self)
{
  OM_uint32 gMaj, gMin;
  gss_buffer_t output_name_buffer;
  gss_OID output_name_type;
  PyObject *output_name;

  output_name_buffer = (gss_buffer_desc *)malloc(sizeof(gss_buffer_desc));

  gMaj = gss_display_name(&gMin,
                          self->name,
                          output_name_buffer,
                          &output_name_type);
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
    return NULL;
  }

  gss_release_oid(&gMin, &output_name_type);
/*
  return output_name_buffer;
*/

  output_name = Py_BuildValue("s#",
                              output_name_buffer->value,
                              output_name_buffer->length);

  gss_release_buffer(&gMin, output_name_buffer);
  free (output_name_buffer);
  return output_name;
}


PyObject *gssName_export(gssName *self)
{
  OM_uint32 gMaj, gMin;
  gss_buffer_t output_name_buffer;
  PyObject *output_name;

  output_name_buffer = (gss_buffer_desc *)malloc(sizeof(gss_buffer_desc));


  gMaj = gss_export_name(&gMin,
                         self->name,
                         output_name_buffer);
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
    return NULL;
  }

  output_name = Py_BuildValue("s#",
                              output_name_buffer->value,
                              output_name_buffer->length);
  gss_release_buffer(&gMin,output_name_buffer);
  free (output_name_buffer);
  return output_name;
}


int gssName_compare(gssName *self, gssName *other)
{
  OM_uint32 gMin, gMaj;
  int name_equal;

  gMaj = gss_compare_name(&gMin,
                          self->name,
                          other->name,
                          &name_equal);
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
    return 0;
  }

  if (name_equal)
    return 1;
  return 0;
}


gssName *gssName_duplicate(gssName *self)
{
  OM_uint32 gMin, gMaj;
  gssName *other;

  other = (gssName *)malloc(sizeof(gssName));

  gMaj = gss_duplicate_name(&gMin,
                            self->name,
                            &(other->name));
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
    return NULL;
  }
  return other;
}


gssName *gssName_canonicalize(gssName *self, gss_OID mech_type)
{
  OM_uint32 gMin, gMaj;
  gssName *other;

  other = (gssName *)malloc(sizeof(gssName));

  gMaj = gss_canonicalize_name(&gMin,
                               self->name,
                               mech_type,
                               &(other->name));
  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
    return NULL;
  }
  return other;
}
