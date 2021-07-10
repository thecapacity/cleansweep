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

    #endpoint for search
    @app.route('/', methods=['GET'])
    @app.route('/search', methods=['GET', 'POST'])
    def search(**kwargs):
        data = { }

        try:
            print("Search:", request.form['search_string'])
        except:
            pass

        return render_template('search.html', data=data)

    def init_app(app):
        pass

    init_app(app)

    return app
