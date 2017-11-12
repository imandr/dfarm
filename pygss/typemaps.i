
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


/* let functions return raw python objects */
%typemap(python, out) PyObject * {
  $target = $source;
}

%typemap(python, in) PyObject * {
  $target = $source;
}


/* typemaps for global variables */
%typemap(python, varout) gss_OID {
  $target = Py_BuildValue("s#",
                          $source->elements,
                          $source->length);
}

/*
%typemap(python, varout) gss_buffer_t {
  $target = Py_BuildValue("s#",
                          $source->value,
                          $source->length);
}
*/



/* gss_buffer_t typemaps */
%typemap (python, out) char *{
  OM_uint32 gMin;
  PyObject *str;

  /* buffer_t typemap */

  if (!(str = Py_BuildValue("s", $source))) {
    PyErr_SetString(PyExc_TypeError,"string conversion failed.");
    return NULL;
  }

  /* we can free the source */
  free($source);
  $target = str;
}


%typemap (python, in) gss_buffer_t {
  /* check if it is a string */
  if (PyString_Check($source)) {
    $target = (gss_buffer_desc *)malloc(sizeof(gss_buffer_desc));
    $target->length = PyString_Size($source);
    if (!($target->value = PyString_AsString($source))) {
      PyErr_SetString(PyExc_TypeError,"string conversion failed.");
      return NULL;
    }
  }
  else {
    PyErr_SetString(PyExc_TypeError, "not a string");
    return NULL;
  }
}

%typemap (python, freearg) gss_buffer_t {
  free ($source);
}


%typemap (python, out) gss_buffer_t {
  PyObject *str;
  OM_uint32 gMin;

  if (!(str = Py_BuildValue("s#", $source->value, $source->length))) {
      PyErr_SetString(PyExc_TypeError,"string conversion failed.");
      return NULL;
  }
  $target = str;

  
  gss_release_buffer(&gMin, $source);
  free ($source);
}


%typemap (python, in) gss_OID {
  /* check if it is a string */
  if (PyString_Check($source)) {
    $target = (gss_OID_desc *)malloc(sizeof(gss_OID_desc));
    $target->length = PyString_Size($source);
    if (!($target->elements = PyString_AsString($source))) {
      PyErr_SetString(PyExc_TypeError,"string conversion failed.");
      return NULL;
    }
  }
  else {
    PyErr_SetString(PyExc_TypeError, "not a string");
    return NULL;
  }
}

%typemap (python, freearg) gss_OID {
  OM_uint32 gMin;
  free ($source);
/*
  do not use this, because source->elements is was created by PyString_AsString
  gss_release_oid(&gMin, &$source);
*/
}

%typemap (python, in) gss_OID_set {
  int i, size;
  PyObject *listItem;
  gss_OID oid;

  /* check if it is a list  */
  if (!PyList_Check($source)) {
    PyErr_SetString(PyExc_TypeError, "not a list");
    return NULL;
  }


 
  size = PyList_Size($source);
  /* if it is a zero-length list, set $target =  GSS_C_NO_OID_SET */
  if (size==0) {
    $target = GSS_C_NO_OID_SET;
  }
  else {
    $target = (gss_OID_set_desc *)malloc(sizeof(gss_OID_set_desc));
    $target->count = size;
    $target->elements = (gss_OID_desc *)malloc(size * sizeof(gss_OID_desc));

    for (i=0;i<size;i++) {
      listItem = PyList_GetItem($source, i);
      /* check if it is a string */ 
      if (PyString_Check(listItem)) {
        oid = &$target->elements[i];
/*
        oid->elements = (gss_OID_desc *)malloc(sizeof(gss_OID_desc));
*/
        oid->length = PyString_Size(listItem);
        if (!(oid->elements = PyString_AsString(listItem))) {
          PyErr_SetString(PyExc_TypeError,"string conversion failed.");
          return NULL;
        }
      }
      else {
        PyErr_SetString(PyExc_TypeError, "list item is not a string");
        return NULL;
      }
    }
  }
}


%typemap (python, freearg) gss_OID_set {
  OM_uint32 gMin;

  if( $source != GSS_C_NO_OID_SET )
  {
  	free ($source->elements);
  	free ($source);
  }

  

/*
  free ($source);
  do not use this, because source->elements is was created by PyString_AsString
  gss_release_oid(&gMin, &$source);
  gss_release_oid_set(&gMin, &$source);
*/
}
