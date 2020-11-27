import os
import sys
import json
import sqlite3
import time
import shutil
import hashlib

import dataset
import colored
import click
from flask import current_app, g
from flask.cli import with_appcontext

from . import AppDB

def check_file(f):
    return f.is_file() and not f.name.startswith('.') and os.path.getsize(f) > 0

def check_dir(d):
    return d.is_dir() and not d.name.startswith('.')

def get_files(dir_name = None):
    if not dir_name: dir_name = os.getcwd()

    ### Note, this ignores the top_level_directory (i.e. current working directory)
    sub_dirs = [ d for d in os.scandir(dir_name) if check_dir(d) ]
    child_files = [ f for f in os.scandir(dir_name) if check_file(f) ]

    for d in sub_dirs:
        try:
#            click.echo("\t > %s" % (d.path) )

            child_files.extend( [f for f in os.scandir(d.path) if check_file(f)] )
            child_dirs = [ d for d in os.scandir(d) if check_dir(d) ]
            if len(child_dirs): sub_dirs.extend(child_dirs)

        except:
            if d: click.echo( "EXCEPTION FOR: %s" % click.format_filename(d.path) )
#            print( "Unexpected error: %s" % (sys.exc_info()[0]) )
            continue

    return child_files, sub_dirs

    ## FIXME: eventually consider yield vs. building a full list
    """for f in os.listdir(path):
        if check_file( os.path.join(path, f) ):
            yield f
    """

def hash_func(file_name):
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()

    with open(file_name, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
        return hasher.hexdigest()

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

@click.command('db-ls-files')
@with_appcontext
def db_ls_files_command():
    """List files in the database."""
    db, ds = AppDB.get_db()

    click.echo('Listing <files> stored in the database: %s' % (g.DATABASE_PATH))
    for d in ds['files'].all():
        click.echo('%s' % click.format_filename(json.dumps(d)) )
    return

@click.command('db-ls-dirs')
@with_appcontext
def db_ls_dirs_command():
    """List dirs in the database."""
    db, ds = AppDB.get_db()

    click.echo('Listing <dirs> stored in the database: %s' % (g.DATABASE_PATH))
    for d in ds['dirs'].all():
        click.echo('%s' % click.format_filename(json.dumps(d)) )
    return

@click.command('db-ls-dir-top')
@click.option('-n', '--num', default=2)
@click.option('--name/--no-name', default=False)

@with_appcontext
def db_dir_top_command(num = None, name = None):
    """Return top `n` dirs based on n_sub_dirs."""
    if not num: num = 2
    ### FIXME: --name requires an argument but isn't used should be made a flag

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
@with_appcontext
def bless_command(dir_name = None):
    """Populate the database wih confirmed files - IGNORES hidden .* files"""
    if not dir_name: dir_name = os.getcwd()

    click.echo('Blessing / %s' % click.format_filename(dir_name))

    db, ds = AppDB.get_db()
    files = ds['files']
    dirs = ds['dirs']

    ### Note, this ignores the top_level_directory and does NOT add it to the database
    sub_dirs = [ d for d in os.scandir(dir_name) if check_dir(d) ]
    child_files = [ f for f in os.scandir(dir_name) if check_file(f) ]

    for d in sub_dirs:
        try:
            click.echo("\t > %s" % (d.path) )

            child_files.extend( [f for f in os.scandir(d.path) if check_file(f)] )
            child_dirs = [ d for d in os.scandir(d) if check_dir(d) ]
            if len(child_dirs): sub_dirs.extend(child_dirs)

        except:
            if d: click.echo( "EXCEPTION FOR: %s" % click.format_filename(d.path) )
#            print( "Unexpected error: %s" % (sys.exc_info()[0]) )
            continue

    for f in child_files:
        click.echo('\t*> %s' % click.format_filename(f.path) )
        ## FIXME: change to use File and Dir Objects
        files.upsert( { 'name': f.name,
                        'f_hash': hash_func(f.path),
                        'blessed': True,
                        'parent': os.path.dirname(f.path),
                        #'updated_at': int(time.time()),
                        'path': f.path, }, ['path'] )

    for d in sub_dirs:
        click.echo('\t \ %s' % click.format_filename(d.path) )
        ## FIXME: change to use File and Dir Objects
        dirs.upsert( { 'name': d.name,
                        'blessed': True,
                        'parent': os.path.dirname(d.path),
                        'sub_dirs': json.dumps( [s.path for s in sub_dirs] ),
                        'n_sub_dirs': len( child_dirs ),
                        #'updated_at': int(time.time()),
                        'path': d.path, }, ['path'] )
    #
    #files.create_index(['path', 'name', 'parent', 'f_hash'])
    #dirs.create_index(['path', 'name', 'parent', 'n_sub_dirs'])

@click.command('ls')
@with_appcontext
def ls_fs_command():
    """List files on the filesystem based on database."""
    file_list, dirs = get_files()

    for f in file_list:
#        click.echo('\t*> %s' % click.format_filename(f.path) ) 

        file_node = AppDB.File_Node(f.path)
        click.echo('%s' % (file_node) )
        click.echo('\t id   > %s' % click.format_filename(file_node.id) ) 
        click.echo('\t name > %s' % click.format_filename(file_node.name) ) 
        click.echo('\t path > %s' % click.format_filename(file_node.path) ) 
        click.echo('\t hash > %s' % (file_node.sha1) ) 
        click.echo('\t dir  > %s' % (file_node.parent) ) 
        click.echo(' ')
