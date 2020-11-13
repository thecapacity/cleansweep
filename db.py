import os
import json
import sqlite3
import time
import shutil
import hashlib

import dataset
import click
from flask import current_app, g
from flask.cli import with_appcontext

def hash_func(file_name):
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()

    with open(file_name, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
        return hasher.hexdigest()

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

    return g.db

def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

    return db

def init_db():
    db = get_db()
    table = db['files']
    table = db['dirs']

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

@click.command('init-db')
@with_appcontext
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database: %s' % (g.DATABASE_PATH))

@click.command('drop-db')
@with_appcontext
def drop_db_command():
    """Drop the database file, if it exists."""
    drop_db()
    click.echo('Database Deleted')

@click.command('db-ls-files')
@with_appcontext
def db_ls_files_command():
    """List files in the database."""
    DATABASE_PATH = 'sqlite:///' + current_app.config['DATABASE']
    ds = dataset.connect(DATABASE_PATH)

    for d in ds['files'].all():
        click.echo('%s' % click.format_filename(json.dumps(d)) )
    return

@click.command('db-ls-dirs')
@with_appcontext
def db_ls_dirs_command():
    """List dirs in the database."""
    DATABASE_PATH = 'sqlite:///' + current_app.config['DATABASE']
    ds = dataset.connect(DATABASE_PATH)

    for d in ds['dirs'].all():
        click.echo('%s' % click.format_filename(json.dumps(d)) )
    return

def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    app.cli.add_command(drop_db_command)
