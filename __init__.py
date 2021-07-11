import os
import json

from flask import Flask, url_for, render_template, request, redirect, jsonify
from markupsafe import escape


def create_app(test_config=None):
    from . import cli

    app = Flask(__name__, instance_relative_config=True)

    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'cleansweep.sqlite'),
        CACHE=os.path.join(app.instance_path, 'cleansweep_cache.sqlite'),
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

        try:
            print("Command:", request.form['search_string'])
        except:
            pass

        return render_template('search.html', data=data)


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
