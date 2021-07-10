import os
import sys
import json
import click
import colored
import sqlite3
import dataset
import hashlib

from flask import current_app, g

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

