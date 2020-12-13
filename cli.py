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
        for d in ds['files'].all():
            Node = AppDB.FileNode(d)
            click.echo('%s' % (Node) )

    if dirs:
        for d in ds['dirs'].all():
            Node = AppDB.DirNode(d['abs_path'])
            click.echo('%s' % (Node) )

    if hashes:
        for h in ds['files'].distinct('sha1'):
            click.echo('%s' % (h['sha1']))
            for f in ds['files'].find(sha1=h['sha1'], order_by='abs_path'):
                click.echo('\t[%7s] . %s' % (f['status'], 
                                            click.format_filename(f['abs_path'])) )

@click.command('db-ls-files')
@with_appcontext
def db_ls_files_command():
    """List files in the database."""
    db, ds = AppDB.get_db()

    for n in ds['files'].all():
        click.echo('[%7s] %s\n\t%s' % (n['status'], click.format_filename(n['abs_path']), n['sha1']) )
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


@click.argument('file_name', type=click.Path(exists=True, file_okay=True, 
                 dir_okay=True, resolve_path=True), required=True)
@click.command('rm')
@with_appcontext
def db_rm_command(file_name, **kw):
    """ REMOVE file(s) from the database """

    if file_name and os.path.isfile(file_name):
        fNode = AppDB.FileNode(file_name)
        fNode.score = fNode.test_unique() # Checks against DB by default
        click.echo('Removing: [%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
        fNode.db_delete()
        return

    ## Means we passed in '.' and we'll recursively remove all files from DB
    dir_name = os.getcwd()
    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue ## Skip directories that don't pass

        for f in files:
            if not check_file( os.path.join(r, f) ): continue
            fNode = AppDB.FileNode( os.path.join(r, f) )
            fNode.score = fNode.test_unique() # Checks against DB by default
            click.echo('Removing: [%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
            fNode.db_delete()
     
@click.argument('file_name', type=click.Path(exists=True, file_okay=True, 
                 dir_okay=False, resolve_path=True), required=False)
@click.command('curse')
@with_appcontext
def curse_command(file_name = False, **kw):
    """CURSE the database wih known BAD files - IGNORES hidden .* files"""

    if file_name:
        fNode = AppDB.FileNode(file_name)
        fNode.score = fNode.test_unique() # Checks against DB by default
        click.echo('[%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
        fNode.set_status("CURSED")
        fNode.score = fNode.test_unique() # Checks against DB by default
        click.echo('[%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
        fNode.db_add()
        return

    dir_name = os.getcwd()
    ## It is possible to store files with the same hash into the DB this way
    ##    that should be ok - but worth noting that DB HASHES may not be unique
    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue ## Skip directories that don't pass
        click.echo('CURSING %s' % click.format_filename(r))

        for f in files:
            if not check_file( os.path.join(r, f) ): continue
            fNode = AppDB.FileNode( os.path.join(r, f) )
            fNode.set_status("CURSED") ## NOTE: Can overwrite previously BLESSED files
            fNode.db_add()

            click.echo('\t[%7s] %s' % (fNode.status, fNode) )

@click.argument('file_name', type=click.Path(exists=True, file_okay=True, 
                 dir_okay=False, resolve_path=True), required=False)
@click.command('bless')
@with_appcontext
def bless_command(file_name = False, **kw):
    """Populate the database wih confirmed files - IGNORES hidden .* files"""

    if file_name:
        fNode = AppDB.FileNode(file_name)
        fNode.score = fNode.test_unique()
        click.echo('[%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
        fNode.set_status("BLESSED")
        fNode.score = fNode.test_unique()
        click.echo('[%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
        fNode.db_add()
        return

    dir_name = os.getcwd()
    ## It is possible to store files with the same hash into the DB this way
    ##    that should be ok - but worth noting that DB HASHES may not be unique
    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue ## Skip directories that don't pass
        click.echo('Blessing %s' % click.format_filename(r))

        for f in files:
            if not check_file( os.path.join(r, f) ): continue
            fNode = AppDB.FileNode( os.path.join(r, f) )
            fNode.set_status("BLESSED") ## NOTE: Can overwrite previously CURSED files
            fNode.db_add()

            click.echo('\t[%s] %s' % (fNode.status, fNode) )

## Via: https://click.palletsprojects.com/en/7.x/api/#click.Path
@click.argument('file_name', type=click.Path(exists=True, file_okay=True, 
                   dir_okay=False, resolve_path=True), required=False)
@click.command('ls')
@with_appcontext
def fs_ls_command(file_name = False):
    """List files on the filesystem based on database."""

    if file_name:
        fNode = AppDB.FileNode(file_name)
        fNode.score = fNode.test_unique()
        click.echo('[%7s] @ [%5s] %s' % (fNode.status, fNode.score, fNode) )
        return

    file_list = [ ]
    dir_name = os.getcwd()

    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue ## Skip directories that don't pass

        for f in files:
            if not check_file( os.path.join(r, f) ): continue ## Skip files that don't pass

            fNode = AppDB.FileNode( os.path.join(r, f) )
            file_list.append(fNode)

    ## Running this check vs in previous loop in case we wanted to do something else
    for fNode in file_list: 
        fNode.score = fNode.test_unique() # Checks against DB by default

    dup_scores = [n.score for n in file_list if n.score > 0] or [0]
    min_score  = min(dup_scores)
    max_score  = max(dup_scores)
    ave_score  = sum(dup_scores) / len(dup_scores)
    lower_T = int(ave_score * .8) ## arbitrary
    upper_T = int(ave_score * 1.2) ## arbitrary

    click.echo("\t[%s [%s - %s] %s]\n" % (min_score, lower_T, upper_T, max_score) )
    for fNode in file_list:

        ## NOTE: The conditionals are meant for each node to have one exclusive action
        ##
        if "BLESSED" in fNode.status:
            pass ## Take no actions - these nodes are already in the DB
        elif "CURSED" in fNode.status:
            pass ## Take no actions - these nodes are already in the DB
        #
        # We know these won't be BLESSED or CURSED because of earlier checks
        # There may still be duplicates in the list - e.g. among other filesystem matches
        #
        elif fNode.score < 0: 
            fNode.set_status("GOOD")   # this only means unique vs DB

        elif fNode.score >= upper_T:
            fNode.set_status("NUKE")   # Strong guess this is duplicate vs. DB
        
        elif fNode.score >= lower_T:   # May want to rethink another level/test because same
            fNode.set_status("CHECK")  # action for: score == min_score and score == lower_T

        else:
            fNode.set_status("NOTSURE")# > 0 but < lower_T - likely name match only

        click.echo('[%5s] %s' % (fNode.score, fNode) )

    """ 
    ## Possible way to look / check for dups in FS before thinking about db_add()
    click.echo("")
    ## In this case the highest score(s) is actually a good thing
    ##     e.g. store the highest score and you match a lot of the filesystem dups
    for fNode in file_list:
        if fNode.score > 0 or "BLESSED" in fNode.status: continue
        if not "GOOD" in fNode.status: continue ## Another way to filter
        fNode.score = fNode.test_unique(file_list)
        click.echo("%5s > %s" % (fNode.score, fNode) )
    """

@click.command('hunt')
@with_appcontext
def fs_hunt_command(**kw):
    """ HUNT for files on the filesystem based on BLESSED files in database."""
    db, ds = AppDB.get_db()
    table = ds['files']

    file_list = [ ]
    dir_name = os.getcwd()

    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue ## Skip directories that don't pass

        for f in files:
            if not check_file( os.path.join(r, f) ): continue ## Skip some files

            fNode_fs = AppDB.FileNode( os.path.join(r, f) )
            fNode_db = table.find_one('abs_path' != fNode_fs.abs_path, name=fNode_fs.name, sha1=fNode_fs.get_hash(), status="BLESSED")

            if fNode_db:
                fNode_fs.score = fNode_fs.test_unique()
                click.echo('NUKE: [%5s] %s' % (fNode_fs.score, fNode_fs) )

## FIXME: Placeholder - NEEDS TO BE COMPLETED
@click.command('clean')
@with_appcontext
def fs_clean_command(**kw):
    """Clean - aka DELETE - files on the filesystem based on database."""
    dir_name = os.getcwd()

    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue # Skip directories that don't pass

        for f in files:
            if not check_file( os.path.join(r, f) ): continue

            abs_src = os.path.join(r, f)
            fNode = AppDB.FileNode(abs_src)

            if not fNode.is_unique():
                click.echo('NUKE: %s' % (fNode) )
            else:
                click.echo('%s' % (fNode) )

## FIXME: Placeholder - NEEDS TO BE COMPLETED
@click.command('sweep')
@click.option('--dst-name', default=False)
@with_appcontext
def fs_sweep_command(**kw):
    """Sweap files on the filesystem """
    dir_name = os.getcwd()
    if not kw['dst_name']: kw['dst_name'] = current_app.config['DST_DIR_NAME']

    replace_dir, _ = os.path.split(dir_name)

    for r, subs, files in os.walk(dir_name):
        if not check_dir(r): continue # Skip directories that don't pass

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

                ## Maybe green means it's not in DB and is new 
                ##       purple means it is and should be deleted
