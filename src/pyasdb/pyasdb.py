import shelve
import atexit
from threading import Lock

class Query:
    """
    Query Class For Making And Managing Results of Queries.

    """
    def __init__(self, table, results):
        """
        Query Constructor
        :param table: Table object the results are for
        :param results: list of keys
        """
        self.table = table
        self.results = results

    def query(self, field, func, checktype=None):
        """
        Make a sub-query and return a new narrower Query object
        :param field: the field being searched, will only parse entries that have this field
        :param func: a function reference applied to a filter query (ie. lambda x: x > 5)
        :param checktype: if passed a type will automatically narrow results to that type to prevent TypeError
        :return: A new query object containing the results of the given query
        """
        field = str(field)
        return Query(self.table, list(
            filter(
                lambda key:
                    field in self.table[key].keys() and
                    (
                        checktype is None or
                        isinstance(self.table[key], checktype)
                    ) and
                    func(self.table[key][field]), self.results)
            )
        )

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index == len(self.results):
            raise StopIteration
        else:
            x = self.results[self.index]
            self.index += 1
            return x

    def __getitem__(self, key):
        """
        :param key(str): primary key of result
        :param key(int): list index of result
        :return:
        """
        if isinstance(key, int):
            return self.table[self.results[key]]
        elif isinstance(key, str):
            return self.table[key]
        else:
            raise KeyError

    def __setitem__(self, key, value):
        """
        :param key(str): primary key of result
        :param key(int): list index of result
        :return:
        """
        if not isinstance(value, dict):
            raise TypeError("Value must be a dictionary")
        if isinstance(key, int):
            self.table[self.results[key]] = value
        elif isinstance(key, str):
            self.table[key] = value
        else:
            raise KeyError

    def update(self, key, obj):
        self.table.update(key, obj)

    def __delitem__(self, key):
        self.table.__delitem__(key)

    def __contains__(self, key):
        return key in self.results

    def __repr__(self):
        return "<pyasdb.DB.Table.Query: " + self.table.name + " - " + ','.join(self.results) + ">"


class Table:
    """Table class for managing individual database tables"""
    def __init__(self, parent, name):
        """
        Table Constructor
        :param parent: handle of the DB
        :param name: name of the Table in the DB
        """
        self.parent = parent
        self.name = name

    def keys(self):
        """
        Returns a list of primary keys in the Table
        :return: list of keys
        """
        return list(
            set(
                map(
                    lambda table: '.'.join(table.split('.')[1:]),
                    filter(
                        lambda key: key.split('.')[0] == self.name, list(self.parent.shelf)
                    )
                )
            )
        )

    def sync(self):
        """
        If writeback enabled on database, initiate a sync
        """
        self.parent.sync()

    def __getitem__(self, key):
        return self.parent.shelf['.'.join((self.name, key))]

    def __setitem__(self, key, value, sync=False):
        """
        :param key: primary key of entry
        :param value: new contents
        :param sync: boolean specifying to immediately sync if in writeback mode
        """
        if not isinstance(value, dict):
            raise TypeError("Value must be a dictionary")
        with self.parent.lock:
            self.parent.shelf['.'.join((self.name, key))] = value
            if sync:
                self.parent.sync()

    def update(self, key, obj):
        """
        Applies partial updates without erasing data via the dict.update mechanism
        :param key: primary key of entry
        :param obj: dictionary containing new values to be merged with entry
        """
        tmpx = self[key]
        tmpx.update(obj)
        self[key] = tmpx

    def __delitem__(self, key):
        del self.parent.shelf['.'.join((self.name, key))]

    def __contains__(self, key):
        return '.'.join((self.name, key)) in self.parent.shelf

    def __iter__(self):
        self.index = 0
        self.parent.tables[self.name] = self
        self.__keycache = self.keys()
        return self

    def __next__(self):
        if self.index == len(self.__keycache):
            raise StopIteration
        else:
            x = self[self.__keycache[self.index]]
            self.index += 1
            return x

    def __repr__(self):
        return "<pyasdb.DB.Table: " + self.name + ">"

    def query(self, field, func):
        """
        Generates an initial query and returns a Query object.
        :param field: the field being searched, will only parse entries that have this field
        :param func: a function reference applied to a filter query (ie. lambda x: x > 5)
        :return: A new Query object containing the results of the given query
        """
        query = Query(self, self.keys())
        return query.query(field, func)


class DB:
    """
    A simple offline local pythonic database backed by shelve/pickle
    """
    def __init__(self, filename, flag='c', writeback=False):
        """
        Database constructor
        :param filename: Path and filename of the database file to use (will append .db to end)
        :param flag: flag passed through to Shelve.open
        :param writeback: Whether to enable writeback mode
        """
        self.shelf = shelve.open(filename, flag, writeback=writeback)
        self.writeback = writeback
        self.lock = Lock()

        tableNames = list(set(map(lambda key: key.split('.')[0], list(self.shelf))))
        self.tables = {}

        for table in tableNames:
            self.tables[table] = Table(self, table)

        atexit.register(self.close)

    def keys(self):
        """
        :return: List of all tables in the database
        """
        return list(self.tables.keys())

    def sync(self):
        """
        If writeback enabled, manually sync
        """
        with self.lock:
            if self.writeback:
                self.shelf.sync()

    def __getitem__(self, key):
        """
        Returns a Table, will create a new Table if one does not already exist.
        :param key:
        :return:
        """
        if not key in self.tables.keys():
            self.tables[key] = Table(self, key)
        return self.tables[key]

    def __iter__(self):
        self.index = 0
        self.__keycache = self.keys()
        return self

    def __next__(self):
        if self.index == len(self.__keycache):
            raise StopIteration
        else:
            x = self[self.__keycache[self.index]]
            self.index += 1
            return x

    def close(self):
        self.sync()
        self.shelf.close()