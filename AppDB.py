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
        db, ds = get_db()

        try:
            statement = 'DELETE FROM dirs WHERE abs_path = :abs_path'
            for row in ds.query(statement, abs_path=self.abs_path):
                print(row)
        except:
            click.echo( "Trying to DELETE: %s" % (self.abs_path) )
            click.echo( "Unexpected error: %s" % (sys.exc_info()[0]) )

    ### FIXME: Probably a more elegant way to have the base class filter and add
    def db_add(self):
        pass

class DirNode(Node):
    def __init__(self, info):

        if isinstance(info, str): # if we get a string we're loading via filesystem
            abs_path = info
            Node.__init__(self, abs_path)
        else: #Otherwise assume we're loading an OrderedDict from the DB
            abs_path = info['abs_path']
            Node.__init__(self, abs_path)

        self.table_name = 'dirs'
        self.parent = None

        p, d = os.path.split(abs_path)
        if d: self.parent = DirNode(p) # If d is None then we're at the top, i.e. '/'

        self.color = colored.bg('dark_olive_green_3a')

    def db_add(self):
        """ d = AppDB.DirNode(row['path'])
            d.db_add()
            ### Will CREATE or UPDATE based on abs_path as unique key
        """
        db, ds = get_db()
        table = ds[self.table_name]

        ## FIXME: Not needed at present - creates full dir tree (back to '/' if we do)
        #if self.parent: self.parent.db_add()

        ### FIXME: Maybe we'll want these attributes another day
        entry = self.__dict__.copy()
        entry.pop('parent') ### This MUST be deleted as obj type can't be stored in DB
        entry.pop('color')
        entry.pop('table_name')

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD DIR: %s" % (self.abs_path) )

class FileNode(Node):
    def __init__(self, abs_path):
        Node.__init__(self, abs_path)

        self.table_name = 'files'
        self.sha1 = None
        self.size = os.path.getsize(abs_path)

        self.blessed = False
        self.color = colored.bg('blue')

        self.parent = DirNode(self.path)

    def bless(self):
        self.blessed = True
        self.color = colored.bg('gold_3a')

    def db_add(self):
        """ d = AppDB.FileNode(row['path'])
            d.db_add()
            ### Will CREATE or UPDATE based on abs_path as unique key
        """
        db, ds = get_db()
        table = ds[self.table_name]

        if not self.sha1: self.get_hash()
        if self.parent: self.parent.db_add()

        ### FIXME: Maybe we'll want these attributes another day
        entry = self.__dict__.copy()
        entry.pop('parent') ### This MUST be deleted as obj type can't be stored in DB
        entry.pop('color')
        entry.pop('table_name')

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD FILE: %s" % (self.abs_path) )

    def get_hash(self):
        db, ds = get_db()

        if self.sha1:
            return self.sha1
        else: ##rather than just recalculate - query DB to see if we're already stored
            db_entry = ds[self.table_name].find_one(abs_path=self.abs_path)

            if db_entry:
                self.sha1 = db_entry['sha1']
            else:
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

    ## FIXME: Consider if this should happen automatically e.g. on `__repr__(..)`
    def test_unique(self):
        my_sha1 = self.get_hash()
        my_name = self.name

        ## Cases to consider
        ##      * Same Hash and Same Name as blessed file -> Mark as red for deletion
        ##      * Same Hash and Diff Name as blessed file -> Mark as orange for review
        ##      * Diff Hash and Same Name as blessed file -> Mark as purple for review
        ##      * Diff Hash and Diff Name as blessed file -> Mark as green for inclusion

        db, ds = get_db()
        table = ds[self.table_name]

        hash_match = False
        name_match = False

        if hash_match and name_match:
            self.color = colored.bg('red')

        elif hash_match and not name_match:
            self.color = colored.bg('dark_orange_3a')

        elif not hash_match and name_match:
            self.color = colored.bg('purple_1b')

        elif not hash_match and not name_match:
            self.color = colored.bg('green')

        else: ### Should Never get here
            click.echo("test_unique() UNKNOWN CONDITION")

        ## FIXME: Future expansion, look for file collisions among non-blessed files
