import os
import json
import dataset

from flask import Flask, url_for, render_template, request, redirect, jsonify
from markupsafe import escape


def create_app(test_config=None):
    from . import cli

    app = Flask(__name__, instance_relative_config=True)

    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'cleansweep.sqlite'),
        CACHE=os.path.join(app.instance_path, 'cleansweep_hashes.sqlite'),
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

    @app.route('/', methods=['GET', 'POST'])
    def index(**kwargs):
        data = { }
        examples = [ ]

        try: print("Command:", request.form['search_string'])
        except: pass

        try:
            data['HASH_DB_PATH'] = 'sqlite:///' + \
                                    os.path.join(app.instance_path, 'cleansweep_hashes.sqlite')
            hash_cache = dataset.connect( data['HASH_DB_PATH'] )
            hash_ds = hash_cache['hashes']

            data['status'] = "SUCCESS GETTING A CACHE"
            data['tables'] = hash_cache.tables
            data['columns'] = hash_ds.columns
            data['entries'] = len(hash_cache['hashes'])

            result = hash_cache.query('SELECT * FROM hashes LIMIT 10')
            examples = [row for row in result]

            #data['files'] = hash_ds['hashes'].all()
            #db_entry = hash_ds.find_one(abs_path=self.abs_path)
            #data['files'] = db_entry.__dict__.copy()

        except:
            data = { 'status': "ERROR getting HASH CACHE" }

        return render_template('index.html', data=data, examples=examples)


    @app.route('/scan', methods=['GET'])
    def scan(directory = None, **kwargs):
        ret = { }

        if not directory: directory = request.args.get('dir')
        if not directory: directory = os.getcwd()

        app.logger.debug("Scanning Directory: %s" % (directory) )

        ret['dir'] = directory

        return jsonify(ret)
#        return json.dumps(ret), {'Content-Type': 'application/json'}
#        return jsonify(ret), {'Content-Type': 'application/json'}



    def init_app(app):
        app.teardown_appcontext(cli.close_db_command)
        app.cli.add_command(cli.init_db_command)
        app.cli.add_command(cli.drop_db_command)

    init_app(app)

    return app
