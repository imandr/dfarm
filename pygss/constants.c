
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
#include "gss.h"


/*
gssName NO_NAME={GSS_C_NO_NAME};
const gss_name_t PYGSS_C_NO_NAME = ((gss_name_t)0);
*/

const gssName GSS_PY_NO_NAME = {GSS_C_NO_NAME};
/*
gss_buffer_desc GSS_PY_NO_BUFFER_DESC = {0, NULL};
const gss_buffer_t GSS_PY_NO_BUFFER  = &GSS_PY_NO_BUFFER_DESC;
*/

/*
const gss_OID GSS_PY_NO_OID = ((gss_OID) 0);
const gss_OID_set GSS_PY_NO_OID_SET  = ((gss_OID_set) 0);
*/
