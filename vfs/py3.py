import sys

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2


def to_bytes(s):
    if isinstance(s, str):
        s = bytes(s, "utf-8")
    return s
    
def to_str(b):
    if isinstance(b, bytes):
        b = b.decode("utf-8")
    return b
