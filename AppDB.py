import os
import sys
import json
import click
import colored
import sqlite3
import dataset
import hashlib

from flask import current_app, g

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
    table = ds['files']
    table = ds['dirs']

    files.create_index(['path', 'name', 'sha1'])
    dirs.create_index(['path', 'name'])

def close_db(e=None):
    ds = g.pop('ds', None)
    db = g.pop('db', None)

    if db is not None:
        db.close()

    return db

def drop_db():
    db = close_db()

    DATABASE_PATH = current_app.config['DATABASE']

    try:
        os.remove( DATABASE_PATH )
    except:
        pass
    try: ## Separate try block in case dir exists but not db file
        os.rmdir( os.path.dirname(DATABASE_PATH) )
    except:
        pass

class Node():
    def __init__(self, abs_path):
        self.abs_path = abs_path
        path, name = os.path.split(abs_path)
        self.table_name = None
        self.path = path
        self.name = name or "/" ### Check: If name is none, then path is "/" and we're root
        self.color = ""

    def __repr__(self):
        return self.color + os.path.join(self.path, self.name) + colored.attr('reset')

    def db_delete(self):
        db, ds = get_db()

        statement = 'DELETE FROM %s WHERE abs_path = "%s"' % (self.table_name, self.abs_path)
        for row in ds.query(statement):
            print("DELETING: %s" % (row) ) ## Doesn't happen because query returns nothing

    def db_add(self):
        pass

class DirNode(Node):
    def __init__(self, info):

        if isinstance(info, str): # if we get a string we're loading via filesystem
            abs_path = info
            Node.__init__(self, abs_path)
        else: #Otherwise assume we're loading an OrderedDict from the DB
            abs_path = info['abs_path']
            Node.__init__(self, abs_path)

        self.table_name = 'dirs'

        ## FIXME: For now being used for brevity's sake
        self.parent = None # Intended as object version of path string
        #p, d = os.path.split(abs_path)
        #if d: self.parent = DirNode(p) # If d is None then we're at the top, i.e. '/'

        self.color = colored.bg('dark_olive_green_3a')

    def db_add(self):
        """ d = AppDB.DirNode(row['path'])
            d.db_add()
            ### Will CREATE or UPDATE based on abs_path as unique key
        """
        db, ds = get_db()
        table = ds[self.table_name]

        ## Not needed at present - creates full dir tree (back to '/' if we do)
        #if self.parent: self.parent.db_add()

        entry = self.__dict__.copy()
        entry.pop('color')
        entry.pop('table_name')
        entry.pop('parent') ### This MUST be deleted as obj type can't be stored in DB
            ## We also don't need to save it because self.path is the text representation

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD DIR: %s" % (self.abs_path) )

    def db_delete(self):
        ## FIXME: This might leave dangling files if we delete Dir of multiple files
        ##      DirNode Delete should be to delete the DirNode if no files point to it
        if self.parent: self.parent.db_delete() ## FIXME: Right now self.parent = None
        Node.db_delete(self)
        ## FIXME: Test gets complicated because DirNode needs to know about FileNode 
            ##  (i.e. files table) to run query

class FileNode(Node):
    def __init__(self, info):
        if isinstance(info, str): # if we get a string we're loading via filesystem
            abs_path = info
            Node.__init__(self, abs_path)

            self.sha1 = None ## Don't auto hash for sha1, rely on explicit get_hash() call
            self.status = "unknown"
            self.color = colored.bg('blue')
            self.size = os.path.getsize(abs_path)

        else: #Otherwise assume we're loading an OrderedDict from the DB
            abs_path = info['abs_path']
            Node.__init__(self, abs_path)

            self.sha1 = info['sha1']
            self.size = info['size']
            self.bless( info['status'] )

        self.table_name = 'files'
        self.parent = DirNode(self.path) ## Convenience helper object vs. self.path string

    def bless(self, state = "BLESSED"):
        self.status = state
        if "BLESSED" in state:
            self.color = colored.bg('gold_3a')
        elif "CURSED" in state:
            self.color = colored.bg('red_3a') + colored.attr(5) ## attr(5) is blink
        elif "unknown" in state:
            self.color = colored.bg('blue')
        else:
            self.color = colored.bg('blue') + colored.attr(5)

    def db_add(self):
        """ d = AppDB.FileNode(row['path'])
            d.db_add()
            ### Will CREATE or UPDATE based on abs_path as unique key
        """
        db, ds = get_db()
        table = ds[self.table_name]

        if not self.sha1: self.get_hash()
        if self.parent: self.parent.db_add()

        entry = self.__dict__.copy()
        entry.pop('color')
        entry.pop('table_name')
        entry.pop('parent') ### This MUST be deleted as obj type can't be stored in DB
            ## We also don't need to save it because self.path is the text representation

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD FILE: %s" % (self.abs_path) )

    def db_delete(self):
        Node.db_delete(self)
        ## Need to delete the file (i.e. self) first so DirNode can see if it should be deleted
        if self.parent: self.parent.db_delete() ## Child should suggest to parent to delete

    def get_hash(self):
        db, ds = get_db()

        if self.sha1:
            return self.sha1
        else: ##rather than just recalculate - query DB to see if we're already stored
            db_entry = ds[self.table_name].find_one(abs_path=self.abs_path)

            if db_entry:
                self.sha1 = db_entry['sha1']
            else:
                self.sha1 = self.calculate_hash()

        return self.sha1

    def calculate_hash(self):
        BLOCKSIZE = 65536
        hasher = hashlib.sha1()

        with open(self.abs_path, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            while len(buf) > 0:
                hasher.update(buf)
                buf = afile.read(BLOCKSIZE)
        h = hasher.hexdigest()
        return h

    ## Maybe this should happen on `__repr__(..)` - good enough for now
    ## FIXME: Make this work with the test_unique()
    def is_unique(self):
        ## Return Unique if it's green or purple or gold
        ##  note !unique is not the same as saying it's a duplicate
        ##  e.g. orange or blue are unknown

        self.test_unique()

        if self.color == colored.bg('red_3a'):
            return False

        elif self.color == colored.bg('dark_orange_3a'):
            return False ## Beware binary logic _may_ be unique or dup

        elif self.color == colored.bg('navy_blue'):
            return False ## Beware binary logic _may_ be unique or dup

        elif self.color == colored.bg('green'):
            return True

        elif self.color == colored.bg('purple_1b'):
            return True

        elif self.color == colored.bg('gold_3a'):
            return True

        else: ### Would get here if file hasn't been checked: colored.bg('blue')
            click.echo("is_unique() UNKNOWN CONDITION")
            click.echo("\t with: %s" % (self.abs_path) )

            return False

    ## FIXME: This isn't quite right - e.g. confusing dups for uniques, etc. 
    ##             depends on the circumstances - e.g. ordering of adding - of events
    def test_unique(self):
        """ Shouldn't be called directly, call `is_unique()` to give change to re-test """
        my_sha1 = self.get_hash()
        my_name = self.name

        db, ds = get_db()
        table = ds[self.table_name]

        ## find_one will return an OrderedDict Object - i.e. the whole row / object
        hash_match = table.find_one(sha1=my_sha1)
        name_match = table.find_one(name=my_name)

        ## Cases to consider
        ##      * Same Hash and Same ABS_PATH (thus name) -> Mark as red for deletion
        ##      * Same Hash and Diff Name as blessed file -> Mark as orange for review
        ##      * Diff Hash and Same Name as blessed file -> Mark as blue for review
        ##      * Diff Hash and Diff Name as blessed file -> Mark as green for inclusion
        ##      * Same Hash and Same ABS_PATH as blessed file -> Mark as purple for protection

        if self.color == colored.bg('gold_3a'):
            ##      gold = 'golden master' - we've blessed this - ignore others
            pass    ## Added to prevent over writing - e.g. with 'purple'

        elif hash_match and name_match and self.abs_path != name_match['abs_path']:
            ##      red = 'ready to nuke'
            self.color = colored.bg('red_3a')

            ## THIS COULD FAIL badly - i.e. mark a blessed file as nuke-able!!!
            ##      so check to make sure returned file is not itself blessed
            if "BLESSED" in name_match['status']: # and self.abs_path == name_match['abs_path']:
                click.echo("%s" % (self.abs_path) )
                click.echo("\t%s" % (json.dumps(name_match) ) )
                self.color = colored.bg('gold_3a')
                #self.color = colored.bg('purple_1b')
            ### FIXME: the above is still not right - prevents nuking a blessed file
            ##      but is also marking a non-blessed dup as safe

        elif hash_match and not name_match: # HASH MATCH BUT NOT NAME
            ##      orange = 'OR-ange you sure it's not a match'
            self.color = colored.bg('dark_orange_3a')

        elif not hash_match and name_match: # NAME MATCH BUT NOT HASH
            ##      navy = 'name matches something'
            self.color = colored.bg('navy_blue')

        elif not hash_match and not name_match: # NO MATCH - unique file
            ##      green = 'good to store'
            self.color = colored.bg('green')

        elif hash_match and name_match and self.abs_path == name_match['abs_path']:
            ### THIS FILE IS A GOLDEN MASTER FILE - DO NOT DELETE!!!
            ##      purple = 'protect'
            self.color = colored.bg('purple_1b')

        else: ### Should Never get here
            click.echo("test_unique() UNKNOWN CONDITION")
            click.echo("\t with: %s" % (self.abs_path) )
