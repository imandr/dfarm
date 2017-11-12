
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
#include <Python.h>

static PyObject *gssError;
static PyObject *error_objects[3][32];

#define calling_err(x) x >> GSS_C_CALLING_ERROR_OFFSET
#define routine_err(x) x >> GSS_C_ROUTINE_ERROR_OFFSET
#define supplementary_info(x) x >>  GSS_C_SUPPLEMENTARY_OFFSET

static OM_uint32 EXC_CALL_INACCESSIBLE_READ =
   calling_err (GSS_S_CALL_INACCESSIBLE_READ);
static OM_uint32 EXC_CALL_INACCESSIBLE_WRITE =
   calling_err (GSS_S_CALL_INACCESSIBLE_WRITE);
static OM_uint32 EXC_CALL_BAD_STRUCTURE =
   calling_err (GSS_S_CALL_BAD_STRUCTURE);

static OM_uint32 EXC_BAD_MECH = routine_err(GSS_S_BAD_MECH);
static OM_uint32 EXC_BAD_NAME = routine_err(GSS_S_BAD_NAME);
static OM_uint32 EXC_BAD_NAMETYPE = routine_err(GSS_S_BAD_NAMETYPE);
static OM_uint32 EXC_BAD_BINDINGS = routine_err(GSS_S_BAD_BINDINGS);
static OM_uint32 EXC_BAD_STATUS = routine_err(GSS_S_BAD_STATUS);
static OM_uint32 EXC_BAD_SIG = routine_err(GSS_S_BAD_SIG);
static OM_uint32 EXC_NO_CRED = routine_err(GSS_S_NO_CRED);
static OM_uint32 EXC_NO_CONTEXT = routine_err(GSS_S_NO_CONTEXT);
static OM_uint32 EXC_DEFECTIVE_TOKEN = routine_err(GSS_S_DEFECTIVE_TOKEN);
static OM_uint32 EXC_DEFECTIVE_CREDENTIAL =
  routine_err(GSS_S_DEFECTIVE_CREDENTIAL);
static OM_uint32 EXC_CREDENTIALS_EXPIRED =
  routine_err(GSS_S_CREDENTIALS_EXPIRED);
static OM_uint32 EXC_CONTEXT_EXPIRED = routine_err(GSS_S_CONTEXT_EXPIRED);
static OM_uint32 EXC_FAILURE = routine_err(GSS_S_FAILURE);
static OM_uint32 EXC_BAD_QOP = routine_err(GSS_S_BAD_QOP);
static OM_uint32 EXC_UNAUTHORIZED = routine_err(GSS_S_UNAUTHORIZED);
static OM_uint32 EXC_UNAVAILABLE = routine_err(GSS_S_UNAVAILABLE);
static OM_uint32 EXC_DUPLICATE_ELEMENT = routine_err(GSS_S_DUPLICATE_ELEMENT);
static OM_uint32 EXC_NAME_NOT_MN = routine_err(GSS_S_NAME_NOT_MN);

static OM_uint32 EXC_CONTINUE_NEEDED =
  supplementary_info(GSS_S_CONTINUE_NEEDED);
static OM_uint32 EXC_DUPLICATE_TOKEN =
  supplementary_info(GSS_S_DUPLICATE_TOKEN);
static OM_uint32 EXC_OLD_TOKEN = supplementary_info(GSS_S_OLD_TOKEN);
static OM_uint32 EXC_UNSEQ_TOKEN = supplementary_info(GSS_S_UNSEQ_TOKEN);
static OM_uint32 EXCS_GAP_TOKEN = supplementary_info(GSS_S_GAP_TOKEN);


/* thank ldap module author david leonard (David.Leonard@csee.uq.edu.au)
   for these handy macros */
#if 0
#define seterrobj2(n, e, o) \
    PyDict_SetItemString(d, \
                         #n, (error_objects[e] = o)); \
    Py_INCREF(error_objects[e])

#define seterrobj(n) \
    seterrobj2(n, \
               ETFILE_ERROR_OFFSET + n, \
               PyErr_NewException("etfilec." #n, \
                                  ETFileError, \
                                  NULL))
#endif

#define seterrobj2(n, t, o) \
    PyDict_SetItemString(d, #n, (error_objects[t][n] =  o)); \
    Py_INCREF(o)

#if 0
#define seterrobj(n, t, e) \
    seterrobj2(n, t, e, \
               PyErr_NewException("gssc." #n, \
                                  gssError, \
                                  NULL))
#endif

#define setcallingerrobj(n) \
    seterrobj2(n, 0, \
               PyErr_NewException("gssc." #n, \
                                  gssError, \
                                  NULL))

#define setroutineerrobj(n) \
    seterrobj2(n, 1, \
               PyErr_NewException("gssc." #n, \
                                  gssError, \
                                  NULL))

#define setsupplementaryerrobj(n) \
    seterrobj2(n, 2, \
               PyErr_NewException("gssc." #n, \
                                  gssError, \
                                  NULL))


int PYGSS_init_errors(PyObject *d)
{
  int i;

  /* the base class */
  gssError = PyErr_NewException("gssc.error", NULL, NULL);
  PyDict_SetItemString(d, "error", gssError);
  Py_INCREF(gssError);

#if 0
  /* initialize the array to all-NULL values */
  for (i=0;i<NUM_ETFILE_ERRORS;i++) {
    error_objects[i]=NULL;
  }
#endif

  /* GSS toolkit error codes */
  /* calling errors */
  setcallingerrobj(EXC_CALL_INACCESSIBLE_READ);
  setcallingerrobj(EXC_CALL_INACCESSIBLE_WRITE); 
  setcallingerrobj(EXC_CALL_BAD_STRUCTURE);

  /* routine errors */
  setroutineerrobj(EXC_BAD_MECH);
  setroutineerrobj(EXC_BAD_NAME);
  setroutineerrobj(EXC_BAD_NAMETYPE);
  setroutineerrobj(EXC_BAD_BINDINGS);
  setroutineerrobj(EXC_BAD_STATUS);
  setroutineerrobj(EXC_BAD_SIG);
  setroutineerrobj(EXC_NO_CRED);
  setroutineerrobj(EXC_NO_CONTEXT);
  setroutineerrobj(EXC_DEFECTIVE_TOKEN);
  setroutineerrobj(EXC_DEFECTIVE_CREDENTIAL);
  setroutineerrobj(EXC_CREDENTIALS_EXPIRED);
  setroutineerrobj(EXC_CONTEXT_EXPIRED);
  setroutineerrobj(EXC_FAILURE);
  setroutineerrobj(EXC_BAD_QOP);
  setroutineerrobj(EXC_UNAUTHORIZED);
  setroutineerrobj(EXC_UNAVAILABLE);
  setroutineerrobj(EXC_DUPLICATE_ELEMENT);
  setroutineerrobj(EXC_NAME_NOT_MN);

  setsupplementaryerrobj(EXC_CONTINUE_NEEDED);
  setsupplementaryerrobj(EXC_DUPLICATE_TOKEN);
  setsupplementaryerrobj(EXC_OLD_TOKEN);
  setsupplementaryerrobj(EXC_UNSEQ_TOKEN);
  setsupplementaryerrobj(EXCS_GAP_TOKEN);

  return 0;
}


int PYGSS_set_error(int error_code, char *error_message)
{
  char errbuf[256];

  if (GSS_CALLING_ERROR(error_code)) {
    printf ("a calling error of some type\n");
    switch (GSS_CALLING_ERROR(error_code)) {
      case GSS_S_CALL_INACCESSIBLE_READ:
      case GSS_S_CALL_INACCESSIBLE_WRITE:
      case GSS_S_CALL_BAD_STRUCTURE:
        PyErr_SetString(error_objects[0][calling_err(error_code)],
                        error_message);
        return 0;
      default:
        snprintf(errbuf,
                 256,
                 "Unknown GSS calling error: %d",
                 calling_err(error_code));
        PyErr_SetString (PyExc_Exception, errbuf);
        return 0;
    }
  }
  else if (GSS_ROUTINE_ERROR(error_code)) {
    switch (GSS_ROUTINE_ERROR(error_code)) {
      case GSS_S_BAD_MECH:
      case GSS_S_BAD_NAME:
      case GSS_S_BAD_NAMETYPE:
      case GSS_S_BAD_BINDINGS:
      case GSS_S_BAD_STATUS:
      case GSS_S_BAD_SIG:
      case GSS_S_NO_CRED:
      case GSS_S_NO_CONTEXT:
      case GSS_S_DEFECTIVE_TOKEN:
      case GSS_S_DEFECTIVE_CREDENTIAL:
      case GSS_S_CREDENTIALS_EXPIRED:
      case GSS_S_CONTEXT_EXPIRED:
      case GSS_S_FAILURE:
      case GSS_S_BAD_QOP:
      case GSS_S_UNAUTHORIZED:
      case GSS_S_UNAVAILABLE:
      case GSS_S_DUPLICATE_ELEMENT:
      case GSS_S_NAME_NOT_MN:
        PyErr_SetString(error_objects[1][routine_err(error_code)],
                        error_message);
        return 0;
      default:
        snprintf(errbuf,
                 256,
                 "Unknown GSS routine error: %d",
                 routine_err(error_code));
        PyErr_SetString (PyExc_Exception, errbuf);
        return 0;
    }
  }
  else if (GSS_SUPPLEMENTARY_INFO(error_code)) {
    switch (GSS_SUPPLEMENTARY_INFO(error_code)) {
      case GSS_S_CONTINUE_NEEDED:
      case GSS_S_DUPLICATE_TOKEN:
      case GSS_S_OLD_TOKEN:
      case GSS_S_UNSEQ_TOKEN:
      case GSS_S_GAP_TOKEN:
        PyErr_SetString(error_objects[2][supplementary_info(error_code)],
                        error_message);
        return 0;
      default:
        snprintf(errbuf,
                 256,
                 "Unknown GSS supplementary error: %d",
                 supplementary_info(error_code));
        PyErr_SetString (PyExc_Exception, errbuf);
        return 0;
    }
  }
  else {
    snprintf(errbuf,
             256,
             "Invalid or Unknown GSS error: %d",
             error_code);
    PyErr_SetString (PyExc_Exception, errbuf);
    return 0;
  }

  /* well, it didn't work out.  Set the top-level exception and include
     some (hopefully) informative error text */
/*
  snprintf(errbuf,
           256,
           "Unknown gss toolkit error: %s",
           "blah");
  PyErr_SetString(gssError, errbuf);
  return 0;
*/
}

