from py3 import to_str, to_bytes

sysopen = open

class TDB(object):
    def __init__(self, f):
        self.F = f
        self.Items = self.read()
        print (self.Items)

    @staticmethod        
    def open(path, create=False):
        try:    return TDB(sysopen(path, "r+b"))
        except Exception as e:
            if create:
                return TDB.create(path)
            else:
                raise e
        
    @staticmethod        
    def create(path):
        return TDB(sysopen(path, "w+b"))
        
    def __enter__(self):
        pass
        
    def __exit__(self, *params):
        self.sync()
        self.close()
        
    def save(self):
        data = [to_bytes("%s %s" % (k, v)) for k, v in self.Items.items()]
        self.F.seek(0)
        print ("data:", data)
        self.F.write(b'\n'.join(data))
        self.F.truncate()
        
    sync = save

    def read(self):
        items = {}
        for line in self.F.read().split(b'\n'):
            if line:
                key, value = line.split(b" ", 1)
                items[to_str(key)] = to_str(value)
        return items
    
    def close(self):
        print ("closed")
        if self.F is not None:
            self.F.close()
            self.F = None
            
    def __del__(self):
        print ("__del__", self.F)
        if self.F is not None:
            self.close()

    def __getitem__(self, name):
        return self.Items[name]
        
    def __setitem__(self, name, value):
        self.Items[name] = value
        
    def keys(self):
        return self.Items.keys()
        
    def items(self):
        return self.Items.items()
        
    def __contains__(self, key):
        return key in self.Items
        
    def __delitem__(self, key):
        del self.Items[key]

    def get(self, key, *params):
        return self.Items.get(key, *params)   
            
def open(path, create=False):
    return TDB.open(path, create)

def create(path):
    return TDB.create(path)
                    
if __name__ == "__main__":
    t = create("/tmp/tdb")
    t["a"] = "aa"
    t["b"] = "bb"
    
    t.save()
    
    t = open("/tmp/tdb")
    for k, v in t.items():
        print(k,v)
        t[k] = v+v
        
    t.save()

    t = open("/tmp/tdb")
    del t["a"]
        
    t.save()

     
        
        
