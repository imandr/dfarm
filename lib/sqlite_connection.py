import sqlite3
from threading import RLock

class ConnectionStub(object):
    def __init__(self, accessor, connection):
        self.Accessor = accessor
        self.Connection = connection
        self.Closed = False
    
    def __getattr__(self, name):
        return getattr(self.Connection, name)
        
    def close(self):
        self.Connection.close()
        self.Accessor.closed(self)
        self.Connection = None
        
    def __del__(self):
        if self.Connection is not None:
            self.close()
            
class SQLiteTransaction(object):
    
    def __init__(self, connection, auto_commit):
        self.Connection = connection
        self.Cursor = connection.cursor()
        self.AutoCommit = auto_commit
        self.Closed = False
        
    def commit(self):
        if not self.Closed:
            self.Connection.commit()
            #print("commit")
        
    def rollback(self):
        if not self.Closed:
            self.Connection.rollback()
            #print("rollback")
        
    def close(self):
        if self.AutoCommit:
            self.commit()
        self.Connection.close()
        self.Closed = True

    def __del__(self):
        if self.AutoCommit:
            self.commit()

    def __getattr__(self, name):
        return getattr(self.Cursor, name)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()

class SQLiteDB(object):

    def __init__(self, path):
        self.Path = path
        self.ConnectionLock = RLock()
        
    def connect(self):
        self.ConnectionLock.acquire()
        conn = sqlite3.connect(self.Path)
        return ConnectionStub(self, conn)
        
    def closed(self, stub):
        self.ConnectionLock.release()

    def transaction(self, auto_commit=True):
        return SQLiteTransaction(self.connect(), auto_commit)
    