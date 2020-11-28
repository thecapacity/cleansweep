import os
import sys
import json
import click
import colored
import sqlite3
import dataset
import hashlib

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
        self.abs_path = abs_path
        path, name = os.path.split(abs_path)
        self.table_name = None
        self.path = path
        self.name = name or "/" ### Check: If name is none, then path is "/" and we're root
        self.color = ""

    def __repr__(self):
        return self.color + os.path.join(self.path, self.name) + colored.attr('reset')

    def db_delete(self):
        """ d = AppDB.DirNode(row['path'])
            d.db_delete()
        """
        db, ds = get_db()

        try:
            statement = 'DELETE FROM dirs WHERE abs_path = :abs_path'
            for row in ds.query(statement, abs_path=self.abs_path):
                print(row)
        except:
            click.echo( "Trying to DELETE: %s" % (self.abs_path) )
            click.echo( "Unexpected error: %s" % (sys.exc_info()[0]) )

class FileNode(Node):
    def __init__(self, abs_path):
        Node.__init__(self, abs_path)

        self.table_name = 'files'
        self.sha1 = None
        self.size = os.path.getsize(abs_path)
        self.atime = os.path.getatime(abs_path)
        self.mtime = os.path.getmtime(abs_path)
        self.islink = os.path.islink(abs_path)

        self.color = colored.bg('blue')

        self.parent = DirNode(self.path)
        self.blessed = False

    ### FIXME: Probably a more elegant way to have the base class filter and add
    def db_add(self):
        """ d = AppDB.FileNode(row['path'])
            d.db_add()
            ### Will CREATE or UPDATE based on abs_path as unique key
        """
        db, ds = get_db()
        table = ds[self.table_name]

        ### FIXME: Maybe we'll want these attributes another day
        if not self.sha1: self.get_hash()

        entry = self.__dict__.copy()
        entry.pop('parent') ### This MUST be deleted as it's an obj type that can't be stored
        entry.pop('color')
        entry.pop('mtime')
        entry.pop('atime')
        entry.pop('size')
        entry.pop('islink')
        entry.pop('table_name')

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Trying to ADD FILE: %s" % (self.abs_path) )

    def get_hash(self):
        if self.sha1:
            return self.sha1
        else: ##maybe rather than recalculate query DB to see if we're already stored
            self.sha1 = self.calculate_hash()
        return self.sha1

    def calculate_hash(self):
        BLOCKSIZE = 65536
        hasher = hashlib.sha1()

        with open(self.abs_path, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        h = hasher.hexdigest()
        return h

class DirNode(Node):
    def __init__(self, abs_path, sub_dirs = None):
        Node.__init__(self, abs_path)
        self.islink = os.path.islink(abs_path)
        self.ismount = os.path.ismount(abs_path)

        self.table_name = 'dirs'
        self.parent = None
        self.sub_dirs = sub_dirs

        p, d = os.path.split(abs_path)
        if d: self.parent = DirNode(p) # If d is None then we're at the top

        self.color = colored.bg('dark_olive_green_3a')
