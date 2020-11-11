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

    with open(file_name,'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
        return hasher.hexdigest()

def init_app(app):
	pass
