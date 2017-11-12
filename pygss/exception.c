
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


#include <stdlib.h>
#include <gssapi.h>

static char error_message[256];
static int error_code = 0;
static int error_status = 0;

/*
void throw_exception(int errorcode, char *msg) {
*/
void throw_exception(OM_uint32 major_code, OM_uint32 minor_code) 
{
     char major_msg[80], minor_msg[80];
     OM_uint32 maj_stat, min_stat;
     gss_buffer_desc msg;
     OM_uint32 msg_ctx;

     error_code = major_code;
     error_status = 1;

     msg_ctx = 0;
     while (1) {
          maj_stat = gss_display_status(&min_stat, major_code,
                                       GSS_C_GSS_CODE, GSS_C_NULL_OID,
                                       &msg_ctx, &msg);
           snprintf(major_msg, 80, "%s", (char *)msg.value);
          (void) gss_release_buffer(&min_stat, &msg);

          if (!msg_ctx)
               break;
     }

     while (1) {
          maj_stat = gss_display_status(&min_stat, minor_code,
                                       GSS_C_MECH_CODE, GSS_C_NULL_OID,
                                       &msg_ctx, &msg);
           snprintf(minor_msg, 80, "%s", (char *)msg.value);
          (void) gss_release_buffer(&min_stat, &msg);

          if (!msg_ctx)
               break;
     }

     snprintf(error_message, 256, "GSS:%s, MECH:%s\n",
              (char *)major_msg,
              (char *)minor_msg);
}

void clear_exception()
{
        error_status = 0;
}

int check_exception()
{
        if (error_status) {
          return error_code;
        }
        else {
          return 0;
        }
}

char *get_exception_message()
{
    if (error_status) {
      return error_message;
    }
    else return NULL;
}

