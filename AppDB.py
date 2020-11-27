import os
import colored
import sqlite3
import dataset

from flask import current_app, g

def get_db():
    if 'db' not in g:
        g.DATABASE_PATH = 'sqlite:///' + current_app.config['DATABASE']

        # https://flask.palletsprojects.com/en/1.1.x/patterns/sqlite3/#sqlite3
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    if 'ds' not in g:
        g.ds = dataset.connect(g.DATABASE_PATH)

    return (g.db, g.ds)

def init_db():
    db, ds = get_db()
    table = ds['files']
    table = ds['dirs']

def close_db(e=None):
    ds = g.pop('ds', None)
    db = g.pop('db', None)

    if db is not None:
        db.close()

    return db

def drop_db():
    db = close_db()

    DATABASE_PATH = current_app.config['DATABASE']

    try:
        os.remove( DATABASE_PATH )
    except:
        pass
    try:
        os.rmdir( os.path.dirname(DATABASE_PATH) )
    except:
        pass

class Node():
    def __init__(self, abs_path):
        self.id = abs_path
        path, name = os.path.split(abs_path)
        self.path = path
        self.name = name or "/" ### Check: If name is none, then path is "/" and we're root
        self.color = ""

    def __repr__(self):
        return self.color + os.path.join(self.path, self.name) + colored.attr('reset')

    def __delete__(self, instance):
        pass ## eventually delete from DB or Filesystem depending on...

class File_Node(Node):
    def __init__(self, abs_path):
        Node.__init__(self, abs_path)

        self.sha1 = None
        self.size = os.path.getsize(abs_path)
        self.atime = os.path.getatime(abs_path)
        self.mtime = os.path.getmtime(abs_path)
        self.islink = os.path.islink(abs_path)

        self.color = colored.bg('blue')

        self.parent = Dir_Node(self.path)

    def get_hash(self):
        if self.sha1:
            return self.sha1
        else: ##maybe rather than recalculate query DB to see if we're already stored
            self.sha1 = calculate_hash()
        return self.sha1

    def calculate_hash(self):
        BLOCKSIZE = 65536
        hasher = hashlib.sha1()

        with open(self.id, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        h = hasher.hexdigest()
        return h

class Dir_Node(Node):
    def __init__(self, abs_path):
        Node.__init__(self, abs_path)
        self.islink = os.path.islink(abs_path)
        self.ismount = os.path.ismount(abs_path)

        self.parent = None

        p, d = os.path.split(abs_path)
        if d: self.parent = Dir_Node(p) # If d is None then we're at the top

        self.color = colored.bg('dark_olive_green_3a')