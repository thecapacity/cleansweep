import os
import sys
import json
import sqlite3
import time
import shutil
import hashlib
from pathlib import Path

import dataset
import colored
import click
from flask import current_app, g
from flask.cli import with_appcontext

from . import AppDB

def check_file(f):
    if isinstance(f, str):
        f = Path(f)
        return f.is_file() and not f.name.startswith('.') and os.path.getsize(f) > 0
    else: #already a file object
        return f.is_file() and not f.name.startswith('.') and os.path.getsize(f) > 0

def check_dir(d):
    if isinstance(d, str):
        d = Path(d)
        return d.is_dir() and not d.name.startswith('.') and not d.is_symlink() and not d.is_mount()
    else: #already a dir object
        return d.is_dir() and not d.name.startswith('.')



### Database functions to get pointers / close, and drop
def close_db_command(e = None):
    """Close the database"""
    try:
        AppDB.close_db(e)
    except:
        click.echo('Error closing the database')

@click.command('init-db')
@with_appcontext
def init_db_command():
    """Clear the existing data and create new tables."""
    AppDB.init_db()
    click.echo('Initialized the database: %s' % (g.DATABASE_PATH))

@click.command('drop-db')
@with_appcontext
def drop_db_command():
    """Drop the database file, if it exists."""
    AppDB.init_db() ## get it first so we can reference g.* values
    click.echo('Deleting the database: %s' % (g.DATABASE_PATH))
    AppDB.drop_db()
