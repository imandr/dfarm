# This file was created automatically by SWIG.
import gssc
import types
from constants import *

class gss_OID_descPtr :
    def __init__(self,this):
        self.this = this
        self.thisown = 0
    def __setattr__(self,name,value):
        if name == "length" :
            gssc.gss_OID_desc_length_set(self.this,value)
            return
        if name == "elements" :
            gssc.gss_OID_desc_elements_set(self.this,value)
            return
        self.__dict__[name] = value
    def __getattr__(self,name):
        if name == "length" : 
            return gssc.gss_OID_desc_length_get(self.this)
        if name == "elements" : 
            return gssc.gss_OID_desc_elements_get(self.this)
        raise AttributeError,name
    def __repr__(self):
        return "<C gss_OID_desc instance>"
class gss_OID_desc(gss_OID_descPtr):
    def __init__(self,this):
        self.this = this




class gss_OID_set_descPtr :
    def __init__(self,this):
        self.this = this
        self.thisown = 0
    def __setattr__(self,name,value):
        if name == "count" :
            gssc.gss_OID_set_desc_count_set(self.this,value)
            return
        if name == "elements" :
            gssc.gss_OID_set_desc_elements_set(self.this,value)
            return
        self.__dict__[name] = value
    def __getattr__(self,name):
        if name == "count" : 
            return gssc.gss_OID_set_desc_count_get(self.this)
        if name == "elements" : 
            return gssc.gss_OID_set_desc_elements_get(self.this)
        raise AttributeError,name
    def __repr__(self):
        return "<C gss_OID_set_desc instance>"
class gss_OID_set_desc(gss_OID_set_descPtr):
    def __init__(self,this):
        self.this = this




class gss_buffer_descPtr :
    def __init__(self,this):
        self.this = this
        self.thisown = 0
    def __setattr__(self,name,value):
        if name == "length" :
            gssc.gss_buffer_desc_length_set(self.this,value)
            return
        if name == "value" :
            gssc.gss_buffer_desc_value_set(self.this,value)
            return
        self.__dict__[name] = value
    def __getattr__(self,name):
        if name == "length" : 
            return gssc.gss_buffer_desc_length_get(self.this)
        if name == "value" : 
            return gssc.gss_buffer_desc_value_get(self.this)
        raise AttributeError,name
    def __repr__(self):
        return "<C gss_buffer_desc instance>"
class gss_buffer_desc(gss_buffer_descPtr):
    def __init__(self,this):
        self.this = this




class gssNamePtr :
    def __init__(self,this):
        self.this = this
        self.thisown = 0
    def __del__(self):
        if self.thisown == 1 :
            gssc.delete_gssName(self.this)
    def import_name(self,arg0,arg1):
        val = gssc.gssName_import_name(self.this,arg0,arg1)
        return val
    def display(self):
        val = gssc.gssName_display(self.this)
        return val
    def export(self):
        val = gssc.gssName_export(self.this)
        return val
    def compare(self,arg0):
        val = gssc.gssName_compare(self.this,arg0.this)
        return val
    def duplicate(self):
        val = gssc.gssName_duplicate(self.this)
        val = gssNamePtr(val)
        return val
    def canonicalize(self,arg0):
        val = gssc.gssName_canonicalize(self.this,arg0)
        val = gssNamePtr(val)
        return val
    def __setattr__(self,name,value):
        if name == "name" :
            gssc.gssName_name_set(self.this,value)
            return
        self.__dict__[name] = value
    def __getattr__(self,name):
        if name == "name" : 
            return gssc.gssName_name_get(self.this)
        raise AttributeError,name
    def __repr__(self):
        return "<C gssName instance>"
class gssName(gssNamePtr):
    def __init__(self) :
        self.this = gssc.new_gssName()
        self.thisown = 1




class gssCredPtr :
    def __init__(self,this):
        self.this = this
        self.thisown = 0
    def __del__(self):
        if self.thisown == 1 :
            gssc.delete_gssCred(self.this)
    def acquire(self,arg0,arg1,arg2,arg3):
        if type(arg0)==types.NoneType:
          arg0 = gssc.cvar.GSS_PY_NO_NAME
        else:
          arg0 = arg0.this
        val = gssc.gssCred_acquire(self.this,arg0,arg1,arg2,arg3)
        return val
    def inquire(self):
        val = gssc.gssCred_inquire(self.this)
        # name
        val[0]=gssNamePtr(val[0])
        val[0].thisown=1
        return tuple(val)
    def __setattr__(self,name,value):
        if name == "cred" :
            gssc.gssCred_cred_set(self.this,value)
            return
        self.__dict__[name] = value
    def __getattr__(self,name):
        if name == "cred" : 
            return gssc.gssCred_cred_get(self.this)
        raise AttributeError,name
    def __repr__(self):
        return "<C gssCred instance>"
class gssCred(gssCredPtr):
    def __init__(self) :
        self.this = gssc.new_gssCred()
        self.thisown = 1




class gssContextPtr :
    def __init__(self,this):
        self.this = this
        self.thisown = 0
    def __del__(self):
        if self.thisown == 1 :
            gssc.delete_gssContext(self.this)
    def init(self,arg0,arg1,arg2,arg3,arg4,arg5):
        val = gssc.gssContext_init(self.this,arg0.this,arg1.this,arg2,arg3,arg4,arg5)
        return val
    def accept(self,arg0,arg1):
        val = gssc.gssContext_accept(self.this,arg0.this,arg1)
        # src_name
        val[0]=gssNamePtr(val[0])
        val[0].thisown=1
        # deleg cred
        val[5]=gssCredPtr(val[5])
        return tuple(val)
    def process_token(self,arg0):
        val = gssc.gssContext_process_token(self.this,arg0)
        return val
    def time(self):
        val = gssc.gssContext_time(self.this)
        return val
    def wrap_size_limit(self,arg0,arg1,arg2):
        val = gssc.gssContext_wrap_size_limit(self.this,arg0,arg1,arg2)
        return val
    def wrap(self,arg0,arg1,arg2):
        val = gssc.gssContext_wrap(self.this,arg0,arg1,arg2)
        return val
    def unwrap(self,arg0):
        val = gssc.gssContext_unwrap(self.this,arg0)
        return val
    def inquire(self):
        val = gssc.gssContext_inquire(self.this)
        # src_name
        val[0]=gssNamePtr(val[0])
        val[0].thisown=1
        # targ_name
        val[1]=gssNamePtr(val[1])
        val[1].thisown=1
        return tuple(val)
    def __setattr__(self,name,value):
        if name == "ctx" :
            gssc.gssContext_ctx_set(self.this,value)
            return
        self.__dict__[name] = value
    def __getattr__(self,name):
        if name == "ctx" : 
            return gssc.gssContext_ctx_get(self.this)
        raise AttributeError,name
    def __repr__(self):
        return "<C gssContext instance>"
class gssContext(gssContextPtr):
    def __init__(self) :
        self.this = gssc.new_gssContext()
        self.thisown = 1






#-------------- FUNCTION WRAPPERS ------------------



#-------------- VARIABLE WRAPPERS ------------------

cvar = gssc.cvar
GSS_PY_NO_NAME = gssNamePtr(gssc.cvar.GSS_PY_NO_NAME)
GSS_PY_BOTH = gssc.GSS_PY_BOTH
GSS_PY_INITIATE = gssc.GSS_PY_INITIATE
GSS_PY_ACCEPT = gssc.GSS_PY_ACCEPT
GSS_PY_DELEG_FLAG = gssc.GSS_PY_DELEG_FLAG
GSS_PY_MUTUAL_FLAG = gssc.GSS_PY_MUTUAL_FLAG
GSS_PY_REPLAY_FLAG = gssc.GSS_PY_REPLAY_FLAG
GSS_PY_SEQUENCE_FLAG = gssc.GSS_PY_SEQUENCE_FLAG
GSS_PY_CONF_FLAG = gssc.GSS_PY_CONF_FLAG
GSS_PY_INTEG_FLAG = gssc.GSS_PY_INTEG_FLAG
GSS_PY_ANON_FLAG = gssc.GSS_PY_ANON_FLAG
GSS_PY_PROT_READY_FLAG = gssc.GSS_PY_PROT_READY_FLAG
GSS_PY_TRANS_FLAG = gssc.GSS_PY_TRANS_FLAG
