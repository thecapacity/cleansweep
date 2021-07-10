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
