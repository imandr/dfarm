
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

gssContext *new_gssContext()
{
  gssContext *self;

  self = (gssContext *)malloc(sizeof(gssContext));
  self->ctx=NULL;

  return self;
}

void delete_gssContext(gssContext *self)
{
  OM_uint32 gMin;
  gss_buffer_desc outputToken;

  if ((self->ctx)!=NULL)
    gss_delete_sec_context(&gMin, (&self->ctx), &outputToken);
  free(self);
}

static unsigned long inetaddr(char *str, int *len)
{
	int d1, d2, d3, d4;
	char ch;
	
	if (sscanf(str, "%d.%d.%d.%d%c", &d1, &d2, &d3, &d4, &ch) == 4 &&
            0 <= d1 && d1 <= 255 && 0 <= d2 && d2 <= 255 &&
            0 <= d3 && d3 <= 255 && 0 <= d4 && d4 <= 255) {
				*len = 4;
                return htonl(
                        ((long) d1 << 24) | ((long) d2 << 16) |
                        ((long) d3 << 8) | ((long) d4 << 0));
    }
	else
	{
		*len = 0;
		return 0;
	}
}
	


PyObject *gssContext_init(gssContext *self,
                     gssCred *initiator_cred_handle,
                     gssName *target_name,
                     gss_OID mech_type,
                     OM_uint32 req_flags,
                     OM_uint32 time_req,
					 PyObject *chantup,
                     gss_buffer_t input_token)
{
  OM_uint32 gMin, gMaj;
  PyObject *outList;
  OM_uint32 ret_flags, time_rec;
  gss_OID actual_mech_type;
  gss_buffer_desc output_token;
  struct gss_channel_bindings_struct chan, *chptr = GSS_C_NO_CHANNEL_BINDINGS;
  unsigned long init_addr, accept_addr;
  int	init_addr_len = 0, accept_addr_len = 0;
  void	*chan_data;
  int chan_data_len = 0;
  
  if( chantup != Py_None && PyTuple_Check(chantup) )
  {
  		int tuplen = PyTuple_Size(chantup);
		if ( tuplen > 0 )
		{
			PyObject *p = PyTuple_GetItem(chantup, 0);
			init_addr = inetaddr(PyString_AsString(p), &init_addr_len);
		}
		if ( tuplen > 1 )
		{
			PyObject *p = PyTuple_GetItem(chantup, 1);
			accept_addr = inetaddr(PyString_AsString(p), &accept_addr_len);
		}
		if ( tuplen > 2 )
		{
			PyObject *str = PyTuple_GetItem(chantup, 2);
			chan_data = (void*)PyString_AsString(str);
			chan_data_len = PyString_Size(str);
			if( chan_data_len <= 0 )
				chan_data = 0;
		}
		if( init_addr_len || accept_addr_len || chan_data_len )
		{
			chan.initiator_addrtype =  
				chan.acceptor_addrtype = GSS_C_AF_INET; /* OM_uint32 */
			chan.initiator_address.length = init_addr_len;
			chan.initiator_address.value = &init_addr;
			chan.acceptor_address.length = accept_addr_len;
			chan.acceptor_address.value = &accept_addr;
			chan.application_data.length = chan_data_len;
			chan.application_data.value = chan_data;
			chptr = &chan;
		}
  		
	}
	
	gMaj = gss_init_sec_context(&gMin,
                              initiator_cred_handle->cred,
                              &(self->ctx),
                              target_name->name,
                              mech_type,
                              req_flags,
                              time_req,
                              chptr,
                              input_token,
                              &actual_mech_type,
                              &output_token,
                              &ret_flags,
                              &time_rec);

  if ((gMaj!=GSS_S_COMPLETE) && (gMaj!=GSS_S_CONTINUE_NEEDED)) {
    throw_exception(gMaj, gMin);
  }

  outList=PyList_New(2);
  PyList_SetItem(outList, 0, Py_BuildValue("i", gMaj));
  PyList_SetItem(outList, 1, Py_BuildValue("s#",
                                           output_token.value,
                                           output_token.length));

  return outList;
}


PyObject *gssContext_accept(gssContext *self,
                       gssCred *acceptor_cred_handle,
					 PyObject *chantup,
                       gss_buffer_t input_token_buffer)
{
  OM_uint32 gMin, gMaj;
  PyObject *outList;
  gss_name_t src_name;
  gss_OID mech_type;
  gss_buffer_desc output_token;
  OM_uint32 ret_flags,time_rec;
  gss_cred_id_t delegated_cred_handle;
  char _ptemp[128];
  struct gss_channel_bindings_struct chan, *chptr = GSS_C_NO_CHANNEL_BINDINGS;
  unsigned long init_addr, accept_addr;
  int	init_addr_len = 0, accept_addr_len = 0;
  void	*chan_data;
  int chan_data_len = 0;

  if( chantup != Py_None && PyTuple_Check(chantup) )
  {
  		int tuplen = PyTuple_Size(chantup);
		if ( tuplen > 0 )
		{
			PyObject *p = PyTuple_GetItem(chantup, 0);
			init_addr = inetaddr(PyString_AsString(p), &init_addr_len);
		}
		if ( tuplen > 1 )
		{
			PyObject *p = PyTuple_GetItem(chantup, 1);
			accept_addr = inetaddr(PyString_AsString(p), &accept_addr_len);
		}
		if ( tuplen > 2 )
		{
			PyObject *str = PyTuple_GetItem(chantup, 2);
			chan_data = (void*)PyString_AsString(str);
			chan_data_len = PyString_Size(str);
			if( chan_data_len <= 0 )
				chan_data = 0;
		}
		if( init_addr_len || accept_addr_len || chan_data_len )
		{
			chan.initiator_addrtype =  
				chan.acceptor_addrtype = GSS_C_AF_INET; /* OM_uint32 */
			chan.initiator_address.length = init_addr_len;
			chan.initiator_address.value = &init_addr;
			chan.acceptor_address.length = accept_addr_len;
			chan.acceptor_address.value = &accept_addr;
			chan.application_data.length = chan_data_len;
			chan.application_data.value = chan_data;
			chptr = &chan;
		}
  		
	}

  gMaj = gss_accept_sec_context(&gMin,
                                &(self->ctx),
                                acceptor_cred_handle->cred,
                                input_token_buffer,
                                chptr,
                                &src_name,
                                &mech_type,
                                &output_token,
                                &ret_flags,
                                &time_rec,
                                &delegated_cred_handle);

  if ((gMaj!=GSS_S_COMPLETE) && (gMaj!=GSS_S_CONTINUE_NEEDED)) {
    throw_exception(gMaj, gMin);
  }

  outList=PyList_New(7);
  PyList_SetItem(outList, 0, Py_BuildValue("i", gMaj));
  SWIG_MakePtr(_ptemp, (char *)src_name,"_gssName_p");
  PyList_SetItem(outList, 1, Py_BuildValue("s", _ptemp));
  if( mech_type )
  	PyList_SetItem(outList, 2, Py_BuildValue("s#",
                                           mech_type->elements,
                                           mech_type->length));
  else
  	PyList_SetItem(outList, 2, Py_BuildValue("s", ""));
  	
  PyList_SetItem(outList, 3, Py_BuildValue("s#",
                                           output_token.value,
                                           output_token.length));
  PyList_SetItem(outList, 4, Py_BuildValue("i", ret_flags));
  PyList_SetItem(outList, 5, Py_BuildValue("i", time_rec));
  SWIG_MakePtr(_ptemp, (char *)delegated_cred_handle,"_gssCred_p");
  PyList_SetItem(outList, 6, Py_BuildValue("s", _ptemp));

  return outList;

}

void gssContext_process_token(gssContext *self,
                              gss_buffer_t token_buffer)
{
  OM_uint32 gMin, gMaj;

  gMaj = gss_process_context_token(&gMin,
                                   self->ctx,
                                   token_buffer);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }
  
}


int gssContext_time(gssContext *self)
{
  OM_uint32 gMin, gMaj;
  OM_uint32 time_rec;

  gMaj = gss_context_time(&gMin,
                          self->ctx,
                          &time_rec);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  return time_rec;
}


int gssContext_wrap_size_limit(gssContext *self,
                               int conf_req_flag,
                               gss_qop_t qop_req,
                               OM_uint32 req_output_size)
{
  OM_uint32 gMin, gMaj;
  OM_uint32 max_input_size;

  gMaj = gss_wrap_size_limit(&gMin,
                             self->ctx,
                             conf_req_flag,
                             qop_req,
                             req_output_size,
                             &max_input_size);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }
  return max_input_size;
}



PyObject *gssContext_inquire(gssContext *self)
{
  OM_uint32 gMin, gMaj;
  PyObject *outList;
  gssName *src_name, *targ_name;
  OM_uint32 lifetime_rec;
  gss_OID mech_type;
  OM_uint32 ctx_flags;
  int locally_initated;
  int open;
  char _ptemp[128];

  src_name = (gssName *)malloc(sizeof(gssName));
  targ_name = (gssName *)malloc(sizeof(gssName));


  gMaj = gss_inquire_context(&gMin,
                             self->ctx,
                             &(src_name->name),
                             &(targ_name->name),
                             &lifetime_rec,
                             &mech_type,
                             &ctx_flags,
                             &locally_initated,
                             &open);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  outList=PyList_New(5);
  SWIG_MakePtr(_ptemp, (char *)src_name,"_gssName_p");
  PyList_SetItem(outList, 0, Py_BuildValue("s", _ptemp));
  SWIG_MakePtr(_ptemp, (char *)targ_name,"_gssName_p");
  PyList_SetItem(outList, 1, Py_BuildValue("s", _ptemp));

  PyList_SetItem(outList, 2, Py_BuildValue("i", lifetime_rec));
  PyList_SetItem(outList, 3, Py_BuildValue("s#",
                                           mech_type->elements,
                                           mech_type->length));
  PyList_SetItem(outList, 4, Py_BuildValue("i", ctx_flags));

  return outList;
  
}


PyObject *gssContext_wrap(gssContext *self,
                          int conf_req_flag,
                          gss_qop_t qop_req,
                          gss_buffer_t input_message_buffer)
{
  OM_uint32 gMin, gMaj;
  int conf_state;
  gss_buffer_desc output_message_buffer;
  PyObject *outTuple;
  

  gMaj = gss_wrap(&gMin,
                  self->ctx,
                  conf_req_flag,
                  qop_req,
                  input_message_buffer,
                  &conf_state,
                  &output_message_buffer);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  outTuple=PyTuple_New(2);

  PyTuple_SetItem(outTuple, 0, Py_BuildValue("s#",
                                           output_message_buffer.value,
                                           output_message_buffer.length));
  PyTuple_SetItem(outTuple, 1, Py_BuildValue("i", conf_state));

  return outTuple;

}


PyObject *gssContext_unwrap(gssContext *self,
                            gss_buffer_t input_message_buffer)
{
  OM_uint32 gMin, gMaj;
  gss_buffer_desc output_message_buffer;
  int conf_state;
  gss_qop_t qop_state;
  PyObject *outTuple;

  gMaj = gss_unwrap(&gMin,
                    self->ctx,
                    input_message_buffer,
                    &output_message_buffer,
                    &conf_state,
                    &qop_state);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  outTuple=PyTuple_New(3);
  PyTuple_SetItem(outTuple, 0, Py_BuildValue("s#",
                                           output_message_buffer.value,
                                           output_message_buffer.length));
  PyTuple_SetItem(outTuple, 1, Py_BuildValue("i", conf_state));
  PyTuple_SetItem(outTuple, 2, Py_BuildValue("i", qop_state));

  return outTuple;
}

gss_buffer_t gssContext_get_mic(gssContext *self,
                               gss_qop_t qop_req,
                               gss_buffer_t message_buffer)
{
  OM_uint32 gMin, gMaj;
  gss_buffer_t message_token;

  message_token=(gss_buffer_t)malloc(sizeof(gss_buffer_desc));

  gMaj = gss_get_mic(&gMin,
                     self->ctx,
                     qop_req,
                     message_buffer,
                     message_token);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  return message_token;

}

int gssContext_verify_mic(gssContext *self,
                               gss_buffer_t message_buffer,
                               gss_buffer_t message_token)

{
  OM_uint32 gMin, gMaj;
  gss_qop_t qop_state;

  gMaj = gss_verify_mic(&gMin,
                        self->ctx,
                        message_buffer,
                        message_token,
                        &qop_state);

  if (gMaj!=GSS_S_COMPLETE) {
    throw_exception(gMaj, gMin);
  }

  return qop_state;
}
