from pythreader import Primitive, synchronized
import threading
from sqlite_connection import SQLiteDB

class VFSDBStorage(Primitive):

    def __init__(self, dbfile, root_info):
        Primitive.__init__(self)
        self.DB = SQLiteDB(dbfile)
        with self.DB.transaction() as t:
            t.execute("""
                create table if not exists vfs (
                    dpath text,
                    key text,
                    type text,
                    info text,
                    primary key (dpath, key)
                );
                """)

        if self.type("/") != 'd':
                self.mkdir("/", root_info)
                
    def split_path(self, path):
        assert len(path) > 0 and path[0] == '/', "split_path: invalid path: '%s'" % (path,)
        if path == '/':
                return '/', '.'
        parent, name = path.rsplit('/', 1)
        if not parent: parent = '/'
        return parent, name

    @synchronized        
    def listItems(self, dpath):
        with self.DB.transaction() as c:
            c.execute("select key, type, info from vfs where dpath=?", (dpath,))
        while True:
            tup = c.fetchone()
            if tup is None:
                break
            yield tup
            
    @synchronized        
    def getItem(self, path):
        parent, key = self.split_path(path)
        with self.DB.transaction() as c:
            c.execute("select type, info from vfs where dpath=? and key=?", (parent, key))
            tup = c.fetchone()
            if tup is None: return None, None
            else: return tup
        
    def type(self, path):
        typ, info = self.getItem(path)
        return typ

    @synchronized
    def delItemsUnder(self, path, typ='f'):
        with self.DB.transaction() as c:
            c.execute("delete from vfs where dpath=? and type=?", (path, typ))
        
    @synchronized
    def delItem(self, path):
        if path == '/.':        return  # do not delete root
        if type(path) == 'd':
                empty = True
                for _ in self.listItems(path):
                        empty = False
                        break
                if not empty:
                        raise RuntimeError("Directory %s not empty" % (path,))
        parent, key = self.split_path(path)
        with self.DB.transaction() as c:
            c.execute("delete from vfs where dpath=? and key = ?", (parent, key))
    

    @synchronized
    def delItem(self, path, recursive=True):
        if type(path) == 'd':
                empty = True
                for _ in self.listItems(path):
                        empty = False
                        break
                if not empty:
                        raise RuntimeError("Directory %s not empty" % (path,))
        parent, key = self.split_path(path)
        with self.DB.transaction() as c:
            c.execute("delete from vfs where dpath=? and key = ?", (parent, key))
        
    @synchronized
    def putItem(self, path, typ, info):
        existing_type = self.type(path)
        if existing_type is not None and typ != existing_type:
                raise ValueError("Can not change item type from %s to %s for %s" % (existing_type, typ, path))
        parent, key = self.split_path(path)
        with self.DB.transaction() as c:
            c.execute("""
                insert or replace into vfs (dpath, key, type, info) values (?, ?, ?, ?)""", (parent, key, typ, info))

    def mkdir(self, path, info):
        self.putItem(path, 'd', info)
        
    @synchronized
    def sync(self):
        self.DB.commit()

    @synchronized
    def close(self):
        print("VFSDBStorage.close()")
        self.DB.close()


        
            
        
        
