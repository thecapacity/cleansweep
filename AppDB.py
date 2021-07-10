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

    files.create_index(['abs_path', 'name', 'sha1'])
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
        self.name = name or "/" ### If name is none, then path is "/" and we're root
        self.color = ""

    def __repr__(self):
        return self.color + os.path.join(self.path, self.name) + colored.attr('reset')

    def db_delete(self):
        db, ds = get_db()

        statement = 'DELETE FROM %s WHERE abs_path = "%s"' % (self.table_name, self.abs_path)
        for row in ds.query(statement):
            print("DELETING: %s" % (row) ) ## Doesn't happen because query returns None

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
        #self.parent = None # Intended as object version of path string
        #p, d = os.path.split(abs_path)
        #if d: self.parent = DirNode(p) # If d is None then we're at the top, i.e. '/'

        self.color = colored.bg('dark_olive_green_3a')

    def db_add(self):
        ### Will CREATE or UPDATE based on abs_path as unique key - OVERWRITE RISK!

        db, ds = get_db()
        table = ds[self.table_name]

        ## Not needed at present - creates full dir tree (back to '/' if we do)
        #if self.parent: self.parent.db_add()

        entry = self.__dict__.copy()
        entry.pop('color')
        entry.pop('table_name')
        entry.pop('parent') ### This MUST be deleted as obj type can't be stored in DB
            ## We don't need to save it because self.path is the text representation

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD DIR: %s" % (self.abs_path) )

    def db_delete(self):
        ## FIXME: This might leave dangling files if we delete Dir of multiple files
        ##      DirNode Delete should be to delete the DirNode if no files point to it
        #if self.parent: self.parent.db_delete() ## FIXME: Right now self.parent = None
        Node.db_delete(self)
        ## FIXME: Test gets complicated because DirNode needs to know about FileNode 
            ##  (i.e. files table) to run query

class FileNode(Node):
    def __init__(self, info):
        if isinstance(info, str): # if we get a string we're loading via filesystem
            abs_path = info
            Node.__init__(self, abs_path)

            self.sha1 = None ## Don't auto hash for sha1, rely on get_hash() call
            self.status = "unknown"
            self.set_status(self.status)
            self.size = os.path.getsize(abs_path)

        else: #Otherwise assume we're loading an OrderedDict from the DB
            
            abs_path = info['abs_path']
            Node.__init__(self, abs_path)
            self.status = "unknown"
            self.sha1 = info['sha1']
            self.size = info['size']
            self.set_status( info['status'] )

        self.table_name = 'files'
        ## FIXME: Not being used at the moment
        #self.parent = DirNode(self.path) ## Convenience object vs. self.path string

    def set_status(self, state = "BLESSED"):
        self.status = state

        ## FIXME: Add a check to keep BLESSED and CURSED nodes static no matter what
        if "BLESSED" in state: 
            ## purple = 'protect'
            self.color = colored.bg('purple_1b')
        elif "CURSED" in state: 
            ## red = 'ready to nuke'
            self.color = colored.bg('red_3a') + colored.attr(5) ## attr(5) is blink
        elif "NUKE" in state:
            ## red = 'ready to nuke'
            self.color = colored.bg('red_3a')
        elif "CHECK" in state:
            ## orange = 'OR-ange you sure it's not a match'
            self.color = colored.bg('dark_orange_3a')
        elif "NOTSURE" in state:
            ## new or navy = 'not sure | maybe unique - e.g. name seems to match'
            self.color = colored.bg('navy_blue')
        elif "GOOD" in state: ## Note, this means new to the DB - NOT to the full dir!!
            ## green = 'good to store'
            self.color = colored.bg('green')
        elif "unknown" in state: # Default 'blue' for unknown
            self.color = colored.bg('blue')
        else: #default blue + blink for unlikely case of bad status flag
            self.color = colored.bg('blue') + colored.attr(5)

        ## Another idea for status
        ## gold = 'golden master' - we've blessed this - ignore others
        ## self.color == colored.bg('gold_3a'):

    def db_add(self):
        ### Will CREATE or UPDATE based on abs_path as unique key
        ##
        ## FIXME: May overwrite "BLESSED" / "CURSED" e.g. with something like "unknown"
        ## FIXME: Consider only saving BLESSED | CURSED | unknown states even if "GOOD"
        db, ds = get_db()
        table = ds[self.table_name]

        if not self.sha1: self.get_hash()
        #if self.parent: self.parent.db_add()

        entry = self.__dict__.copy()
        entry.pop('color')
        entry.pop('table_name')
        #entry.pop('parent') ### This MUST be deleted as obj type can't be stored in DB
            ## self.path is also the text representation so we don't need to save

        try:
            table = ds[self.table_name]
            table.upsert(entry, ['abs_path'])
        except:
            click.echo( "Error trying to ADD FILE: %s" % (self.abs_path) )

    def db_delete(self):
        Node.db_delete(self)
        ## Delete the file (i.e. self) first so DirNode can see if it should be deleted
        #if self.parent: self.parent.db_delete() ## Child should suggest to parent 

    def get_hash(self):
        db, ds = get_db()

        if self.sha1:
            return self.sha1
        else: 

            try:
                HASH_DB_PATH = 'sqlite:///' + \
                               '/Users/wjhuie/bin/instance/cleansweep_hashes.sqlite'

                db = dataset.connect(HASH_DB_PATH)
                hash_ds = db['hashes']

                db_entry = hash_ds.find_one(abs_path=self.abs_path)

                if db_entry:
                    self.sha1 = db_entry['sha1']
                    return self.sha1
            except:
                pass ## If we can't find ourselves in special HASHES DB try files

            ##rather than just recalculate - query DB to see if we're already stored
            ## FIXME: Potential bug if DB file differs from Filesystem version
            ##      Based on the use case I'm willing to accept this risk
            db_entry = ds[self.table_name].find_one(abs_path=self.abs_path)

            if db_entry and 'size' in db_entry.keys() and self.size != db_entry['size']: 
                ## FIXME: This is an impartial sub-HASH test
                click.echo("get_hash: BAD SIZE + HASH for: %s" % (self.abs_path) )
                self.sha1 = None
            elif db_entry and 'sha1' in db_entry.keys():
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

    def shade_unique(self, lower_T = 400, upper_T = 900):
        try:
            self.score
        except:
        #    self.score = self.test_unique()
            self.score = -1

        if "BLESSED" in self.status:
            pass ## Take no actions - these nodes are already in the DB
        elif "CURSED" in self.status:
            pass ## Take no actions - these nodes are already in the DB
        #
        # We know these won't be BLESSED or CURSED because of earlier checks
        # There may still be duplicates in the list - e.g. among other filesystem matches
        #
        elif self.score < 0:
            self.set_status("GOOD")   # this only means unique vs DB

        elif self.score >= upper_T:
            self.set_status("NUKE")   # Strong guess this is duplicate vs. DB

        elif self.score >= lower_T:   # May want to rethink another level/test because same
            self.set_status("CHECK")  # action for: score == min_score and score == lower

        else:
            self.set_status("NOTSURE")# > 0 but < lower_T - likely name match only

    def test_unique(self, file_list = None):
        ### Set color based on uniqueness logic - also return [<0, 0 , >0] depending 
        ###     <0 => Assume  unique, >0 => Assume NOT unique, =0 => Unsure         
        ## Cases to consider
        ##      * Same Hash and Same ABS_PATH (thus name) -> Mark as red for deletion
        ##      * Same Hash and Diff Name as blessed file -> Mark as orange for review
        ##      * Diff Hash and Same Name as blessed file -> Mark as blue for review
        ##      * Diff Hash and Diff Name as blessed file -> Mark as green for inclusion
        ##      * Same Hash and Same ABS_PATH as blessed file -> Purple for protection
        ##
        ## NOTE: THIS ONLY CHECKS THE DB - not against other files in the same dir
        ##       meaning multiple files may NOT be 'NEW / UNIQUE' in the FS vs. DB scan
        ## NOTE All numerical "scoring" values are arbitary 

        ### Used as a wrapper to DB or to list([ FileNode ]) to make logic consistent
        class file_source():
            def __init__(self, file_list = None):
                if isinstance(file_list, list): # if we get a list
                    self.table = file_list
                else: #Otherwise assume we're using the DB
                    db, ds = get_db()
                    self.table = ds['files']

            def abs_match(self, abs_path):
                if isinstance(self.table, list): # if we get a list
                    for fNode in self.table:
                        if fNode.abs_path == abs_path: return fNode
                else: #Otherwise assume we're using the DB
                    match = self.table.find_one(abs_path=abs_path)
                    if match: return FileNode(match)
                return None

            def sha1_match(self, sha1):
                if isinstance(self.table, list): # if we get a list
                    for fNode in self.table:
                        if fNode.sha1 == sha1: yield fNode
                else: #Otherwise assume we're using the DB
                    for match in self.table.find(sha1=sha1):
                        yield FileNode(match)
                return None

            def name_match(self, name):
                if isinstance(self.table, list): # if we get a list
                    for fNode in self.table:
                        if fNode.name == name: yield fNode
                else: # Otherwise assume we're using the DB
                    for match in self.table.find(name=name):
                        yield FileNode(match)
                return None

        self.get_hash()

        FILES = file_source(file_list)

        ## Good is < 0 so we can set pruning thresholds > 0
        ## numbers are arbitrary and used in CLI logic to set thresholds
        STATUS_OPTIONS = { "BLESSED": -5000, "CURSED": 5000, "unknown": 0 }

        ### These should be the easy cases - don't rely on color for testing unique
        ## FIXME: Consider adding more points for name or SHA1 collisions too
        if "CURSED" in self.status:
            self.set_status("CURSED")
            return STATUS_OPTIONS.get(self.status, 0) 
        elif "BLESSED" in self.status:
            self.set_status("BLESSED")
            return STATUS_OPTIONS.get(self.status, 0) 

        abs_match = FILES.abs_match(self.abs_path)
        if abs_match: # likely we found ourself - let's trust it (FIXME: BUG RISK)

            ## NOTE: This may set status as unknown even for BLESSED | CURSED FILES
            ##         e.g. if the file is in the DB not quite what we want 
            ##              but is kind of a safe bet it won't happen based on usage
            if self.size != abs_match.size: ## FIXME: An impartial sub-HASH test
                click.echo("test_unique: BAD SIZE and HASH for: %s" % (self.abs_path) )
                self.set_status( "unknown" )
                return 0

            if self.sha1 and self.sha1 != abs_match.sha1: # No use testing `None`
                click.echo("test_unique: BAD HASH for: %s" % (self.abs_path) )
                self.set_status( "unknown" )
                return 0 ## FIXME: Should we _do_ anything else here to recover?

            ## FIXME: Consider comparing actual file SHA1 and DB
            ## FIXME: `get_hash()` could have BAD side effect of setting a bad hash
            ##            if DB and file don't match contents but do match in abs_path
            ##            i.e. - it will ignore the file hash and use DB['sha1']
            ##            This means we _could_ get an abs_path with the wrong hash
            ##            This is acceptable for our planned use case
            ## NOTE: IF we don't have a sha1 get_hash() can/will overwrite with DB #'s
            self.get_hash()
            self.set_status( abs_match.status ) # Make status match 

            ## return if known BLESSED | CURSED otherwise fall through
            if "BLESSED" in self.status or "CURSED" in self.status: 
                return STATUS_OPTIONS.get(self.status, 0)

 
        ## OK, no match based on absolute path - let's try other ideas
        ## FIXME: Consider comparing actual file SHA1 and DB
        ## FIXME: `get_hash()` could have BAD side effect of setting a bad hash
        ##            if DB and file don't match contents but do match in abs_path
        ##            i.e. - it will ignore the file hash and use DB['sha1']
        ##            This means we _could_ get an abs_path with the wrong hash
        ##            This is acceptable for our planned use case
        ##
        ## NOTE: IF we don't have a sha1 get_hash() can/will overwrite with DB #'s
        if not self.sha1: self.get_hash()
        ret = -1 # using < 0 so as to default in CLI to saving vs. unknown

        ## This will be a true hash match - NOT an abs_path match which happens above
        ## NOTE: if hash matches then size is almost certainly a match so not checking
        for hash_match in FILES.sha1_match(self.sha1):
            if self.abs_path == hash_match.abs_path: continue # don't count self
            ret += 1000  # Arbitrary threshold / heuristic
            if "CURSED" in hash_match.status: ret *= 2 # BAD IF WE MATCH CURSED

        for name_match in FILES.name_match(self.name):
            if self.abs_path == name_match.abs_path: continue # don't count self
            ret += 200
            if "CURSED" in name_match.status: ret *= 2 # BAD IF WE MATCH CURSED

        return ret

        ## FIXME: Do something special and test if directory chain looks similar
        #for name_match in table.find(name=self.name):
        #    if self.abs_path == name_match['abs_path']: continue #don't count self
        #    ret += 100
        #    #my_p, my_d = os.path.split(self.path)
        #    #n_p, n_d = os.path.split(name_match['path'])

        click.echo("test_unique: UNKNOWN TEST for: %s" % (self.abs_path) )
        return 0
