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

### Approach:
    Recursively scan a directory subdirectories that do not start with `.*` and 
        capture `sha1` hash for files that are `> 0` in size and not hidden (i.e. don't start with `.*`)

    "Bless" a directory, associated subdirectories and files become references for future scans.


### Commands:
```  
  db-ls-dirs   List dirs in the database.
  db-ls-files  List files in the database.
  drop-db      Drop the database file, if it exists.
  init-db      Clear the existing data and create new tables.

  bless-dir    Recursively scan a directory and subdirs and add all files as 'blessed'.
```
