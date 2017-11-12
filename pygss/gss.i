
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

%module gssc

%{
#include <gssapi.h>
#include "gss.h"
%}


%include typemaps.i

typedef void *gss_name_t;
typedef void *gss_cred_id_t;
typedef void *gss_ctx_id_t;
typedef unsigned int       gss_qop_t;
typedef int             gss_cred_usage_t;


typedef unsigned int      OM_uint32;

%init %{
  /* d is the dictionary for the current module */
  PYGSS_init_errors(d);

%}



%except (python) {
  int err;
  clear_exception();
  $function
  if ((err = check_exception())) {
    printf ("setting python error\n");
    /* convert a GSS error into a python exception */
    PYGSS_set_error(err, get_exception_message());
    return NULL;
  }
}



typedef struct gss_OID_desc_struct {
      OM_uint32 length;
      void       *elements;
} gss_OID_desc,  *gss_OID;

typedef struct gss_OID_set_desc_struct  {
      size_t  count;
      gss_OID elements;
} gss_OID_set_desc,  *gss_OID_set;


typedef struct gss_buffer_desc_struct {
      size_t length;
      void  *value;
} gss_buffer_desc,  *gss_buffer_t;

/*

typedef struct gss_channel_bindings_struct {
      OM_uint32 initiator_addrtype;
      gss_buffer_desc initiator_address;
      OM_uint32 acceptor_addrtype;
      gss_buffer_desc acceptor_address;
      gss_buffer_desc application_data;
} *gss_channel_bindings_t;
*/





/* we use import_name because the keyword import is reserved */

typedef struct {
  gss_name_t name; 
  %addmethods {
    gssName();
    ~gssName();
    void import_name(gss_buffer_t input_name,
                     gss_OID name_type);
    PyObject *display();
    PyObject *export();
    int compare(gssName *other);
    gssName *duplicate();
    gssName *canonicalize(gss_OID mech_type);
  }
} gssName;


typedef struct {
  gss_cred_id_t cred;
  %addmethods {
    gssCred();
    ~gssCred();
    PyObject *acquire(gssName *desired_name,
                      int time_req,
                      gss_OID_set desired_mechs,
                      gss_cred_usage_t cred_usage);
    PyObject *inquire();
  }
} gssCred;

typedef struct {
  gss_ctx_id_t ctx;
  %addmethods {
    gssContext();
    ~gssContext();
    PyObject *init(gssCred *initiator_cred_handle, 
              gssName *target_name,
              gss_OID mech_type,
              OM_uint32 req_flags,
              OM_uint32 time_req,
			  PyObject *chan,
              gss_buffer_t input_token);
    PyObject *accept(gssCred *acceptor_cred_handle,
				PyObject *chan,
                gss_buffer_t input_token_buffer);
    void process_token(gss_buffer_t token_buffer);
    int time();
    int wrap_size_limit(int conf_req_flag,
                        gss_qop_t qop_req,
                        OM_uint32 req_output_size);
    PyObject *inquire();
    PyObject *wrap(int conf_req_flag,
                   gss_qop_t qop_req,
                   gss_buffer_t input_message_buffer);
    PyObject *unwrap(gss_buffer_t input_message_buffer);
    gss_buffer_t get_mic(gss_qop_t qop_req,
                         gss_buffer_t message_buffer);
    int verify_mic(gss_buffer_t message_buffer,
                   gss_buffer_t message_token);

  }
} gssContext;



%include constants.i


