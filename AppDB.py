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

