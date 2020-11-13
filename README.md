# cleansweep
Creating a command line tool to help me clean up a bunch of redundant files

Per: https://flask.palletsprojects.com/en/1.1.x/installation/

### Initial Setup:
```
$ python3 -m venv cleansweep

$ . cleansweep/bin/activate
$ export FLASK_APP=~/bin/cleansweep
$ export FLASK_ENV=development
$ flask run
```

### Commands:
```  
  db-ls-dirs   List dirs in the database.
  db-ls-files  List files in the database.
  drop-db      Drop the database file, if it exists.
  init-db      Clear the existing data and create new tables.
```
