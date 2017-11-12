
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


#include <gssapi.h>

/* the gssName class */
typedef struct {
  gss_name_t name;
} gssName;

gssName *new_gssName();
void delete_gssName(gssName *self);
void gssName_import_name(gssName *self,
                         gss_buffer_t input_name,
                         gss_OID name_type);
PyObject *gssName_display(gssName *self);
PyObject *gssName_export(gssName *self);
int gssName_compare(gssName *self, gssName *other);
gssName *gssName_duplicate(gssName *self);
gssName *gssName_canonicalize(gssName *self, gss_OID mech_type);


/* the gssCred class */
typedef struct {
  gss_cred_id_t cred;
} gssCred;

gssCred *new_gssCred();
void delete_gssCred(gssCred *self);
PyObject *gssCred_acquire(gssCred *self,
                          gssName *desired_name,
                          int time_req,
                          gss_OID_set desired_mechs,
                          gss_cred_usage_t cred_usage);
PyObject *gssCred_inquire(gssCred *self);


/* gssContext class */
typedef struct {
  gss_ctx_id_t ctx;
} gssContext;

gssContext *new_gssContext();
void delete_gssContext(gssContext *self);
PyObject *gssContext_init(gssContext *self,
                     gssCred *initiator_cred_handle, 
                     gssName *target_name,
                     gss_OID mech_type,
                     OM_uint32 req_flags,
                     OM_uint32 time_req,
					 PyObject *chan,
                     gss_buffer_t input_token);
PyObject *gssContext_accept(gssContext *self,
                       gssCred *acceptor_cred_handle,
					 PyObject *chan,
                       gss_buffer_t input_token_buffer);
void gssContext_process_token(gssContext *self,
                              gss_buffer_t token_buffer);
int gssContext_time(gssContext *self);
int gssContext_wrap_size_limit(gssContext *self,
                               int conf_req_flag,
                               gss_qop_t qop_req,
                               OM_uint32 req_output_size);
PyObject *gssContext_inquire(gssContext *self);
PyObject *gssContext_wrap(gssContext *self,
                          int conf_req_flag,
                          gss_qop_t qop_req,
                          gss_buffer_t input_message_buffer);
PyObject *gssContext_unwrap(gssContext *self,
                            gss_buffer_t input_message_buffer);
gss_buffer_t gssContext_get_mic(gssContext *self,
                               gss_qop_t qop_req,
                               gss_buffer_t message_buffer);
int gssContext_verify_mic(gssContext *self,
                               gss_buffer_t message_buffer,
                               gss_buffer_t message_token);


/* exception handling */

void throw_exception(OM_uint32 major_code, OM_uint32 minor_code);
void clear_exception();
int check_exception();
char *get_exception_message();

