from configparser import ConfigParser
import os

def cvt(v):
    if v in ("yes", "true"):  v = True
    elif v in ("no", "false"): v = False
    else:
        v = v.strip()
        if " " in v:
            v = v.split()
        try:    v = int(v)
        except:
            try:    v = float(v)
            except:   pass
    return v

def DFConfig(path=None, envVar=None):
    path = path or os.environ[envVar]
    cfg = ConfigParser()
    cfg.read_file(open(path, "r"))
    cfg_dict = {}
    for sn in cfg.sections():
        sdict = {}
        section = cfg[sn]
        for k, v in list(section.items()):
            v = v.strip()
            if " " in v:
                v = [cvt(x) for x in v.split()]
            else:
                v = cvt(v)
            sdict[k] = v
        cfg_dict[sn] = sdict
    return cfg_dict
