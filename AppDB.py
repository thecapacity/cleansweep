import os
import sys
import json
import click
import colored
import sqlite3
import dataset
import hashlib

from flask import current_app, g


class Node():
    def __init__(self, abs_path):
        self.abs_path = abs_path
        path, name = os.path.split(abs_path)
        self.table_name = None
        self.path = path
        self.name = name or "/" ### If name is none, then path is "/" and we're root
        self.color = ""

        return self

    def __repr__(self):
        return self.color + os.path.join(self.path, self.name) + colored.attr('reset')

    def db_delete(self):
        db, ds = get_db()

        statement = 'DELETE FROM %s WHERE abs_path = "%s"' % (self.table_name, self.abs_path)
        for row in ds.query(statement):
            print("DELETING: %s" % (row) ) ## Doesn't happen because DELETE query returns None

    def db_add(self):
        pass


class FileNode(Node):
    def __init__(self, info):
        self.table_name = 'files'

        if isinstance(info, str): # if we get a string we're loading via filesystem
            abs_path = info
            Node.__init__(self, abs_path)

            self.status = "unknown"
            self.set_status(self.status)

            self.size = os.path.getsize(abs_path)
            self.sha1 = None ## Don't auto hash for sha1, rely on get_hash() call

        else: #Otherwise assume we're loading an OrderedDict from the DB
            abs_path = info['abs_path']
            Node.__init__(self, abs_path)

            self.status = "unknown"
            self.set_status( info['status'] )

            self.size = info['size']
            self.sha1 = info['sha1']

        return self

    def set_status(self, state = None):
        self.status = state

        ## FIXME: Maybe add a check to keep BLESSED and CURSED fixed no matter what
        if "BLESSED" in state: 
            ## purple = 'protect'
            self.color = colored.bg('purple_1b')
        elif "CURSED" in state: 
            ## red = 'ready to nuke'
            self.color = colored.bg('red_3a') + colored.attr(5) ## attr(5) is blink
        elif "NUKE" in state:
            ## red = 'ready to nuke'
            self.color = colored.bg('red_3a')
        elif "CHECK" in state:
            ## orange = 'OR-ange you sure it's not a match'
            self.color = colored.bg('dark_orange_3a')
        elif "NOTSURE" in state:
            ## new or navy = 'not sure | maybe unique - e.g. name seems to match'
            self.color = colored.bg('navy_blue')
        elif "GOOD" in state: ## Note, this means new to the DB - NOT to the full dir!!
            ## green = 'good to store'
            self.color = colored.bg('green')
        elif "unknown" in state: # Default 'blue' for unknown
            self.color = colored.bg('blue')
        else: #default blue + blink for unlikely case of bad status flag
            self.color = colored.bg('blue') + colored.attr(5)

    def db_add(self):
        ### Will CREATE or UPDATE based on abs_path as unique key
        ##
        ## FIXME: May overwrite "BLESSED" / "CURSED" e.g. with something like "unknown"
        ## FIXME: Consider only saving BLESSED | CURSED | unknown states even if "GOOD"
        db, ds = get_db()
        table = ds[self.table_name]

        if not self.sha1: self.get_hash()

        entry = self.__dict__.copy()
        entry.pop('color')
        entry.pop('table_name')

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD FILE: %s" % (self.abs_path) )

    def db_delete(self):
        Node.db_delete(self)

    def get_hash(self):
        if not self.sha1: ## Try to load from SHA1 HASH BB - definitely a speed hack
            try:
                if 'hash_cache' not in g:
                    HASH_DB_PATH = 'sqlite:///' + current_app.config['CACHE']
                    g.hash_cache = dataset.connect(HASH_DB_PATH)

                hash_ds = g.hash_cache['hashes']

                db_entry = hash_ds.find_one(abs_path=self.abs_path)

                if db_entry:
                    self.sha1 = db_entry['sha1']
                    return self.sha1
            ## FIXME: Doesn't nicely close HASHES_CACHE DB (add to close_db?)
            ##        NOTE: dataset library may not need it
            except:
                pass ## Fall through if we can't find via HASHES_CACHE DB or throw error

            ## rather than just recalculate - query DB to see if we're already stored
            ## FIXME: Potential bug if DB file differs from filesystem version
            ##        Based on the use case I'm willing to accept this risk
            db, ds = get_db()
            db_entry = ds[self.table_name].find_one(abs_path=self.abs_path)

            if db_entry and 'size' in db_entry.keys() and self.size != db_entry['size']:
                ## FIXME: This is an impartial / test to see if files are not ==
                click.echo("get_hash: BAD SIZE + HASH for: %s" % (self.abs_path) )
                self.sha1 = None
            elif db_entry and 'sha1' in db_entry.keys():
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

### Database functions to get pointers / close, and drop
def close_db(e=None):
    ds = g.pop('ds', None)
    db = g.pop('db', None)

    if db is not None:
        db.close()

    return db

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
    return db, ds

def drop_db():
    db = close_db()

    DATABASE_PATH = current_app.config['DATABASE']

    try: ## Try to remove the actual database file - but NOT the hashes CACHE
        os.remove( DATABASE_PATH )
    except:
        pass

    try: ## Separate try block in case dir exists but not db file
        os.rmdir( os.path.dirname(DATABASE_PATH) )
    except:
        pass

