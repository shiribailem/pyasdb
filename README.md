# Python Yet-Another-Simple-Database

I kept using SQLite for local databases and too often it was overkill. Inflexible and too much work to maintain for it's
benefit, even when using an abstraction.

The goal with PYASDB (I pronounce it Py-As-DB) is to be a barebones simple no-sql, unstructured local database.

While I try to optimize it's performance, it's probably less efficient than sqlite as the size of the data increases.

Additionally this is built using only internal libraries, meaning no dependencies should need to be installed.

Disclaimer: Due to it's use of shelve which is backed further by pickle, this is not advisable to load untrusted
databases.

### Quickstart

```python
from src.pyasdb.pyasdb import DB

# This will create a file 'test.db' in the local folder. 
# The name is passed directly to shelve so accepts paths, but will always append '.db' at the end. 
db = DB('test')

# Tables are created just by referencing them.
table = db['table1']

# You add data to tables just like you would with shelve.
table['123'] = {'field1': "testdata", 'x': 456}

# You don't have to handle the tables as separate objects though, they do pass changes back to the database.
db['table1']['123'] = {'field1': "testdata2", 'x': 789}

# You can iterate over the database and tables
for table in db:
    for item in table:
        print(item)

# Rudimentary querying is supported using lambdas
# The query will only search entries with the given field, so you can safely assume that the field entry exists.
query = db['table1'].query(field='x', func=lambda x: x == 789)

# When iterated over it will return the keys only
for item in query:
    print(item)

# But when referenced it'll access the table entries
# (these entries do still accept updates)
for item in query:
    print(query[item])

# You can reference query results by both key and index, so getting the first entry is as simple as:
print(query[0])

# The query result object does support subqueries
query.query('z', lambda x: x < 10)
```

### Features / TODO

- [ ] Indexes (for better performance)
- [x] Writeback Support
- [ ] Convenience queries (ie. dedicated functions for =, <, >, != to cut down on repetitive lambdas)
- [ ] SUM and similar
- [x] Query Type Checking (to prevent TypeError if field has been assigned different types)
- [ ] Subfield queries (your lambda can reference subfields, but the query function doesn't check to make sure they 
exist)
- [ ] Key type agnosticism (the key in the backend must be a string, but it can be handy to just str() all keys to allow
different types)
- [x] Available on PyPi (pip install pyasdb)
- [x] Add help() data
- [x] Add thread safety (automatically includes locks to secure writes)
- [x] Update Routine that connects to dict.update
- [x] pyasdb.import_tools contains tools for automatically importing and converting raw data (CSV only at the time of 
  writing)
- [x] added support for forcing a different dbm backend to shelve, defaults to dumbdbm now for stability reasons
  (this will also help avoid issues if anyone installs a different natively supported dbm after opening the database as
  shelves' default behavior automatically picks a db and could change when a new one is available)

### PyDoc
```
Python Library Documentation: module pyasdb

NAME
    pyasdb

CLASSES
    builtins.object
        DB
        Query
        Table
    
    class DB(builtins.object)
     |  DB(filename, flag='c', writeback=False)
     |  
     |  A simple offline local pythonic database backed by shelve/pickle
     |  
     |  Methods defined here:
     |  
     |  __getitem__(self, key)
     |      Returns a Table, will create a new Table if one does not already exist.
     |      :param key:
     |      :return:
     |  
     |  __init__(self, filename, flag='c', writeback=False)
     |      Database constructor
     |      :param filename: Path and filename of the database file to use (will append .db to end)
     |      :param flag: flag passed through to Shelve.open
     |      :param writeback: Whether to enable writeback mode
     |  
     |  __iter__(self)
     |  
     |  __next__(self)
     |  
     |  keys(self)
     |      :return: List of all tables in the database
     |  
     |  sync(self)
     |      If writeback enabled, manually sync
     |
     |  backup(self, filename=None, flag='n', backend=None)
     |      Creates a backup of the current database
     |      :param filename: Path and filename of the database file to use (ignored if backend provided)
     |      :param flag: flag passed through to Shelve.open
     |      :param backend: (alternative) Accepts open DBM handler (overrides all other arguments)
     |      :writeback: Defaults to True to help protect against corruption in mid-copy
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)
    
    class Query(builtins.object)
     |  Query(table, results)
     |  
     |  Query Class For Making And Managing Results of Queries.
     |  
     |  Methods defined here:
     |  
     |  __getitem__(self, key)
     |      :param key(str): primary key of result
     |      :param key(int): list index of result
     |      :return:
     |  
     |  __init__(self, table, results)
     |      Query Constructor
     |      :param table: Table object the results are for
     |      :param results: list of keys
     |  
     |  __iter__(self)
     |  
     |  __next__(self)
     |  
     |  __repr__(self)
     |      Return repr(self).
     |  
     |  __setitem__(self, key, value)
     |      :param key(str): primary key of result
     |      :param key(int): list index of result
     |      :return:
     |
     |  def update(self, key, obj)
     |      Applies partial updates without erasing data via the dict.update mechanism
     |      :param key: primary key of entry
     |      :param obj: dictionary containing new values to be merged with entry
     |
     |  def __delitem__(self, key)
     |
     |  def __contains__(self, key)
     |  
     |  query(self, field, func, checktype=None)
     |      Make a sub-query and return a new narrower Query object
     |      :param field: the field being searched, will only parse entries that have this field
     |      :param func: a function reference applied to a filter query (ie. lambda x: x > 5)
     |      :param checktype: if passed a type will automatically narrow results to that type to prevent TypeError
     |      :return: A new query object containing the results of the given query
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)
    
    class Table(builtins.object)
     |  Table(parent, name)
     |  
     |  Table class for managing individual database tables
     |  
     |  Methods defined here:
     |  
     |  __getitem__(self, key)
     |  
     |  __init__(self, parent, name)
     |      Table Constructor
     |      :param parent: handle of the DB
     |      :param name: name of the Table in the DB
     |  
     |  __iter__(self)
     |  
     |  __next__(self)
     |  
     |  __repr__(self)
     |      Return repr(self).
     |  
     |  __setitem__(self, key, value, sync=False)
     |      :param key: primary key of entry
     |      :param value: new contents
     |      :param sync: boolean specifying to immediately sync if in writeback mode
     |
     |  def update(self, key, obj)
     |      Applies partial updates without erasing data via the dict.update mechanism
     |      :param key: primary key of entry
     |      :param obj: dictionary containing new values to be merged with entry
     |
     |  def __delitem__(self, key)
     |
     |  def __contains__(self, key)
     |  
     |  keys(self)
     |      Returns a list of primary keys in the Table
     |      :return: list of keys
     |  
     |  query(self, field, func)
     |      Generates an initial query and returns a Query object.
     |      :param field: the field being searched, will only parse entries that have this field
     |      :param func: a function reference applied to a filter query (ie. lambda x: x > 5)
     |      :return: A new Query object containing the results of the given query
     |  
     |  sync(self)
     |      If writeback enabled on database, initiate a sync
     |  
     |  ----------------------------------------------------------------------
     |  Data descriptors defined here:
     |  
     |  __dict__
     |      dictionary for instance variables (if defined)
     |  
     |  __weakref__
     |      list of weak references to the object (if defined)

FILE
    /home/shiri/PycharmProjects/pyasdb/pyasdb.py


```