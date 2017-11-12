
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


/* mechanisms */
extern gss_OID gss_mech_krb5;
extern gss_OID gss_mech_krb5_old;
extern gss_OID gss_mech_krb5_v2;
extern gss_OID_set gss_mech_set_krb5_old;
extern gss_OID_set gss_mech_set_krb5_both;
extern gss_OID_set gss_mech_set_krb5_v2;
extern gss_OID_set gss_mech_set_krb5_v1v2;
/*
const gss_OID GSS_MECH_KRB5 = gss_mech_krb5;
const gss_OID GSS_MECH_KRB5_OLD = gss_mech_krb5_old;
const gss_OID GSS_MECH_KRB5_V2 = gss_mech_krb5_v2;
*/


/*
 * Various Null values.
 */
extern gssName GSS_PY_NO_NAME;

extern gss_OID gss_nt_user_name;
extern gss_OID gss_nt_machine_uid_name;
extern gss_OID gss_nt_string_uid_name;
extern gss_OID gss_nt_service_name;
extern gss_OID gss_nt_exported_name;
extern gss_OID gss_nt_service_name_v2;

/*
const gss_OID GSS_NT_USER_NAME = gss_nt_user_name;
const gss_OID GSS_NT_MACHINE_UID_NAME = gss_nt_machine_uid_name;
const gss_OID GSS_NT_STRING_UID_NAME = gss_nt_string_uid_name;
const gss_OID GSS_NT_SERVICE_NAME = gss_nt_service_name;
const gss_OID GSS_NT_EXPORTED_NAME = gss_nt_exported_name;
const gss_OID GSS_NT_SERVICE_NAME_V2 = gss_nt_service_name_v2;
*/



const gss_cred_usage_t GSS_PY_BOTH=0;
const gss_cred_usage_t GSS_PY_INITIATE=1;
const gss_cred_usage_t GSS_PY_ACCEPT=2;

const unsigned int GSS_PY_DELEG_FLAG=1;
const unsigned int GSS_PY_MUTUAL_FLAG = 2;
const unsigned int GSS_PY_REPLAY_FLAG = 4;
const unsigned int GSS_PY_SEQUENCE_FLAG = 8;
const unsigned int GSS_PY_CONF_FLAG = 16;
const unsigned int GSS_PY_INTEG_FLAG = 32;
const unsigned int GSS_PY_ANON_FLAG = 64;
const unsigned int GSS_PY_PROT_READY_FLAG = 128;
const unsigned int GSS_PY_TRANS_FLAG = 256;

