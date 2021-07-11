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

    def __repr__(self):
        return self.color + os.path.join(self.path, self.name) + colored.attr('reset')

    def db_delete(self):
        db, ds = get_db()

        statement = 'DELETE FROM %s WHERE abs_path = "%s"' % (self.table_name, self.abs_path)
        for row in ds.query(statement):
            print("DELETING: %s" % (row) ) ## Doesn't happen because DELETE query returns None

    def db_add(self):
        pass

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

