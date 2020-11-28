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

def close_db_command(e = None):
    """Close the database"""
    AppDB.close_db(e)

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
    AppDB.drop_db()
    click.echo('Deleted the database')

@click.command('db-ls')
@click.option('--files/--no-files', default=True)
@click.option('--dirs/--no-dirs', default=False)
@click.option('--hashes/--no-hashes', default=False)
@with_appcontext
def db_ls_command(files = True, dirs = False, hashes = False):
    """List entries in the database."""
    db, ds = AppDB.get_db()

    if files:
        #click.echo('Listing <files> stored in the database: %s' % (g.DATABASE_PATH))
        for d in ds['files'].all():
            #click.echo('%s' % click.format_filename(json.dumps(d)) )
            Node = AppDB.FileNode(d['abs_path'])
            click.echo('%s' % (Node) )
        click.echo()

    if dirs:
        #click.echo('Listing <dirs> stored in the database: %s' % (g.DATABASE_PATH))
        for d in ds['dirs'].all():
            #click.echo('%s' % click.format_filename(json.dumps(d)) )
            Node = AppDB.DirNode(d['abs_path'])
            click.echo('%s' % (Node) )
        click.echo()

    if hashes:
        return ## FIXME: Future expansion
        #click.echo('Listing <hashes> stored in the database: %s' % (g.DATABASE_PATH))
        for d in ds['hashes'].all(): ##FIXME: maybe not a seprate table just return file #
            click.echo('%s' % click.format_filename(json.dumps(d)) )
        click.echo()

@click.command('db-ls-files')
@with_appcontext
def db_ls_files_command():
    """List files in the database."""
    db, ds = AppDB.get_db()

    for n in ds['files'].all():
        if n['blessed']:
            click.echo('%s * %s' % (click.format_filename(n['abs_path']), n['sha1']) )
        else:
            click.echo('%s . %s' % (click.format_filename(n['abs_path']), n['sha1']) )
#        click.echo('\t%s' % click.format_filename(json.dumps(n)) )
    return

@click.command('db-ls-dirs')
@with_appcontext
def db_ls_dirs_command():
    """List dirs in the database."""
    db, ds = AppDB.get_db()

    for n in ds['dirs'].all():
        click.echo('%s' % click.format_filename(n['abs_path']) )
#        click.echo('\t%s' % click.format_filename(json.dumps(n)) )
    return

@click.command('db-ls-dir-top')
@click.option('-n', '--num', default=2)
@click.option('--name/--no-name', default=False)
@with_appcontext
def db_dir_top_command(num = None, name = None):
    """Return top `n` dirs based on n_sub_dirs."""
    if not num: num = 2

    db, ds = AppDB.get_db()
    table = ds.load_table('dirs')

    if not name:
        click.echo('Top %s dir(s) by number of sub-directories...' % (num) )
        for d in table.find(order_by='-n_sub_dirs', _limit=num):
            click.echo('\t > %s' % click.format_filename( json.dumps(d['path']) ) )
    elif name: ## FIXME: Will error out if database has no data
        click.echo('Top %s dir(s) with the same names...' % num)

        statement = 'SELECT path, name, COUNT(*) c FROM dirs GROUP BY name \
                                        ORDER BY c DESC LIMIT :max_num'

        for row in ds.query(statement, max_num=num):
            click.echo('\t %i > %s @ %s' % (row['c'], 
                            click.format_filename(row['name']),
                            click.format_filename(row['path'])) )
    else:
        click.echo('Top %s dir(s) UNKNOWN ERROR...' % (num) )

@click.command('bless-dir')
@click.option('--dir_name', default=False)
@click.option('--dups-allowed/--no-dups-allowed', default=True) ##FIXME: Future option
@with_appcontext
def bless_command(dir_name = None, **kw):
    """Populate the database wih confirmed files - IGNORES hidden .* files"""
    if not dir_name: dir_name = os.getcwd()

    ## It is possible to get files with the same hash this way
    ##    that should be ok - but worth noting that DB HASHES may not be unique
    for r, subs, files in os.walk(dir_name):
        ## Skip directories that don't pass - e.g. are mounts, links, or start with '.'
        if not check_dir(r): continue
        click.echo('Blessing / %s' % click.format_filename(r))

        for f in files:
            if not check_file( os.path.join(r, f) ): continue
            click.echo('\t   *> %s' % click.format_filename(f) )

            fNode = AppDB.FileNode( os.path.join(r, f) )
            fNode.blessed = True
            fNode.db_add()

    #files.create_index(['path', 'name', 'parent', 'f_hash'])
    #dirs.create_index(['path', 'name', 'parent', 'n_sub_dirs'])

## FIXME: This needs to actually do something w/ the database and comparisons
@click.command('ls')
@with_appcontext
def ls_fs_command():
    """List files on the filesystem based on database."""
    file_list, dirs = get_files()

    for f in file_list:
#        click.echo('\t*> %s' % click.format_filename(f.path) ) 

        file_node = AppDB.FileNode(f.path)
        click.echo('%s' % (file_node) )
        click.echo('\t abs  > %s' % click.format_filename(file_node.abs_path) ) 
        click.echo('\t name > %s' % click.format_filename(file_node.name) ) 
        click.echo('\t path > %s' % click.format_filename(file_node.path) ) 
        click.echo('\t hash > %s' % (file_node.sha1) ) 
        click.echo('\t dir  > %s' % (file_node.parent) ) 
        click.echo(' ')
