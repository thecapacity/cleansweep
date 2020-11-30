import os
import json

from flask import Flask, url_for, render_template, request, redirect, g, current_app
from markupsafe import escape

def create_app(test_config=None):
    from . import cli

    app = Flask(__name__, instance_relative_config=True)

    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'cleansweep.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/')
    def index(directory = None):
        if not directory: directory = request.args.get('directory')
        if not directory: directory = os.getcwd()
        app.logger.debug("Scanning Directory: %s" % (directory) )
        ret = { }
 
        for d in [ d for d in os.scandir(directory) if d.is_dir() ]:
            app.logger.debug("\t > %s" % (d.path) )

            ret[d.path] = {'path': d.path, 'name': d.name }
            try:
                ret[d.path]['sub_dirs'] = [ s.name for s in os.scandir(d) if os.path.isdir(s) ]
                ret[d.path]['n_sub_dirs'] = len( ret[d.path]['sub_dirs'] ) 
            except:
                continue

        return json.dumps(ret), {'Content-Type': 'application/json'}

    def init_app(app):
        app.teardown_appcontext(cli.close_db_command)
        app.cli.add_command(cli.init_db_command)
        app.cli.add_command(cli.drop_db_command)

        app.cli.add_command(cli.db_ls_command) #More flexible list <x> in database
        app.cli.add_command(cli.db_ls_files_command) #list files in database
        app.cli.add_command(cli.db_ls_dirs_command) #list dirs in database - currently none
#        app.cli.add_command(cli.db_dir_top_command) #list top `n` dirs in database by subdir

        app.cli.add_command(cli.bless_command) # Recursively scan dir and add all non-zero files
        app.cli.add_command(cli.fs_ls_command) # Recursively scan dir and add all non-zero files
        app.cli.add_command(cli.fs_clean_command) # Recursively delete files
        app.cli.add_command(cli.fs_sweep_command) # Recursively move unique files

    init_app(app)

    return app
