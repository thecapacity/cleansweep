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
            #click.echo('%s\n' % (d) )
            Node = AppDB.FileNode(d)
            click.echo('%s' % (Node) )

    if dirs:
        #click.echo('Listing <dirs> stored in the database: %s' % (g.DATABASE_PATH))
        for d in ds['dirs'].all():
            #click.echo('%s' % click.format_filename(json.dumps(d)) )
            Node = AppDB.DirNode(d['abs_path'])
            click.echo('%s' % (Node) )

    if hashes:
        #click.echo('Listing <hashes> stored in the database: %s' % (g.DATABASE_PATH))

        for h in ds['files'].distinct('sha1'):
            click.echo('%s' % (h['sha1']))
            for f in ds['files'].all(sha1=h['sha1']):
#            for f in ds['files'].find(sha1=h['sha1'], order_by='abs_path'):
                click.echo('\t / %s' % (click.format_filename(f['abs_path'])) )

@click.command('db-ls-files')
@with_appcontext
def db_ls_files_command():
    """List files in the database."""
    db, ds = AppDB.get_db()

    for n in ds['files'].all():
        if "BLESSED" in n['status']:
            click.echo('%s *\n\t%s' % (click.format_filename(n['abs_path']), n['sha1']) )
        else:
            click.echo('%s . %s' % (click.format_filename(n['abs_path']), n['sha1']) )
        click.echo('\t%s\n' % click.format_filename(json.dumps(n)) )
    return

@click.command('db-ls-dirs')
@with_appcontext
def db_ls_dirs_command():
    """List dirs in the database."""
    db, ds = AppDB.get_db()

    for n in ds['dirs'].all():
        click.echo('%s' % click.format_filename(n['abs_path']) )
        click.echo('\t%s\n' % click.format_filename(json.dumps(n)) )
    return

@click.command('bless')
@click.argument('status', required=False, default = "BLESSED")
@with_appcontext
def bless_command(**kw):
    """Populate the database wih confirmed files - IGNORES hidden .* files"""
    dir_name = os.getcwd()

    if 'status' in kw and not "BLESSED" in kw['status']:
        status = "unknown" ## Only permit bless to clear (not CURSE)
    else:
        status = "BLESSED"

    ## It is possible to get files with the same hash this way
    ##    that should be ok - but worth noting that DB HASHES may not be unique
    for r, subs, files in os.walk(dir_name):
        ## Skip directories that don't pass - e.g. are mounts, links, or start with '.'
        if not check_dir(r): continue
        click.echo('Blessing %s' % click.format_filename(r))

        for f in files:
            if not check_file( os.path.join(r, f) ): continue
            fNode = AppDB.FileNode( os.path.join(r, f) )
            fNode.bless(status)
            fNode.db_add()

            click.echo('\t *> %s' % (fNode) )

@click.command('ls')
@with_appcontext
def fs_ls_command():
    """List files on the filesystem based on database."""
    dir_name = os.getcwd()

    for r, subs, files in os.walk(dir_name):
        ## Skip directories that don't pass - e.g. are mounts, links, or start with '.'
        if not check_dir(r): continue

        for f in files:
            if not check_file( os.path.join(r, f) ): continue

            fNode = AppDB.FileNode( os.path.join(r, f) )
            fNode.is_unique()
            click.echo('%s' % (fNode) )

## FIXME: Placeholder - NEEDS TO BE COMPLETED
@click.command('clean')
@with_appcontext
def fs_clean_command(**kw):
    """Clean - aka DELETE - files on the filesystem based on database."""
    dir_name = os.getcwd()


    pass

@click.command('sweep')
@click.option('--dst-name', default=False)
@with_appcontext
def fs_sweep_command(**kw):
    """Sweap files on the filesystem """
    dir_name = os.getcwd()
    if not kw['dst_name']: kw['dst_name'] = current_app.config['DST_DIR_NAME']

    replace_dir, _ = os.path.split(dir_name)

    for r, subs, files in os.walk(dir_name):
        ## Skip directories that don't pass - e.g. are mounts, links, or start with '.'
        if not check_dir(r): continue

        for f in files:
            if not check_file( os.path.join(r, f) ): continue

            abs_src = os.path.join(r, f)
            fNode = AppDB.FileNode(abs_src)

            if fNode.is_unique():
                click.echo('%s' % (fNode) )
                new_dst = os.path.join(r, f).replace(replace_dir, kw['dst_name'])
                click.echo('\t%s' % (new_dst) )

                ## FIXME: Need to figure out what to do w/ node 
                ##          - e.g. delete old, make new, store new?

                ## green means it's not in DB and is new 
                ##          - purple means it is and should be deleted
