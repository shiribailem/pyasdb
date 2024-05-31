# Python Yet-Another-Simple-Database

I kept using SQLite for local databases and too often it was overkill. Inflexible and too much work to maintain for it's
benefit, even when using an abstraction.

The goal with PYASDB (I pronounce it Py-As-DB) is to be a barebones simple no-sql, unstructured local database.

While I try to optimize it's performance, it's probably less efficient than sqlite as the size of the data increases.

As a side benefit of it's simplicity: the only import (at the time of writing) is the internal library shelve.

Disclaimer: Due to it's use of shelve which is backed further by pickle, this is not advisable to load untrusted
databases.

### Quickstart

```python
from pyasdb import DB

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
- [ ] Query Type Checking
- [ ] Subfield queries (your lambda can reference subfields, but the query function doesn't check to make sure they 
exist)
- [ ] Key type agnosticism (the key in the backend must be a string, but it can be handy to just str() all keys to allow
different types)
- [ ] Convert to package (and upload to pip)
- [ ] Add help() data
- [ ] Add thread safety