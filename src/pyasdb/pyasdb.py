import shelve
import atexit
from threading import Lock
from dbm import dumb as dumbdbm
from contextlib import nullcontext
import logging

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
        self.results = list(results)

    def query(self, field, func, checktype=None, compare=None, count=None):
        """
        Make a sub-query and return a new narrower Query object
        :param field: the field being searched, will only parse entries that have this field
        :param func: a function reference applied to a filter query (ie. lambda x: x > 5)
        :param checktype: if passed a type will automatically narrow results to that type to prevent TypeError
        :param compare: if function takes an extra argument, use this argument
        :param count: specify the maximum number of results to return
        :return: A new query object containing the results of the given query
        """
        try:
            hash(field)
        except TypeError:
            field = str(field)

        if field in self.table.index_keys:
            results = set()
            for key in self.table.index[field].keys():
                if compare and func(key, compare):
                    results.update(self.table.index[field][key])
                elif not compare and func(key):
                    results.update(self.table.index[field][key])

            if count:
                return Query(self.table, list(filter(lambda x: x in results, self.results))[0:count])
            else:
                return Query(self.table, list(filter(lambda x: x in results, self.results)))

        if compare:
            results = filter(
                    lambda key:
                    field in self.table[key].keys() and
                    (
                            checktype is None or
                            isinstance(self.table[key], checktype)
                    ) and
                    func(self.table[key][field], compare), self.results)

        else:
            results = filter(
                lambda key:
                    field in self.table[key].keys() and
                    (
                        checktype is None or
                        isinstance(self.table[key], checktype)
                    ) and
                    func(self.table[key][field]), self.results)

        if count:
            new_results = list()
            for i in range(count):
                try:
                    new_results.append(next(results))
                except StopIteration:
                    break
            return Query(self.table, new_results)

        return Query(self.table, list(results))

    def query_none(self, field, count=None):
        """
        A query type that returns entries that are undefined or None
        :param field: the field being searched
        """
        field = str(field)

        results = filter(
            lambda key: field not in self.table[key].keys() or self.table[key] is None, self.results)

        if count:
            new_results = []
            for _ in range(count):
                try:
                    new_results.append(next(results))
                except StopIteration:
                    break
            return Query(self.table, new_results)

        return Query(self.table, list(results))

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
            return self.table[str(key)]

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
            return self.table[str(key)]

    def update(self, key, obj):
        key = str(key)
        self.table.update(key, obj)

    def __delitem__(self, key):
        key = str(key)
        self.table.__delitem__(key)

    def __contains__(self, key):
        key = str(key)
        return key in self.results

    def __repr__(self):
        return "<pyasdb.DB.Table.Query: " + self.table.name + " - " + ','.join(self.results) + ">"


class Table:
    """Table class for managing individual database tables"""
    def __init__(self, parent, name, meta=False):
        """
        Table Constructor
        :param parent: handle of the DB
        :param name: name of the Table in the DB
        """
        self.parent = parent
        self.name = name
        self.__meta = meta

        if not self.__meta:
            self.index = Table(self.parent, self.name + '__index', meta=True)
            self.index_keys = list(self.index.keys())

    def create_indexes(self, keys):
        """
        Creates an index on the given key
        :param keys: keys to index on
        """
        refresh_keys = set()
        for key in keys:
            if key not in self.index_keys:
                self.index[key] = {}
                self.index_keys.append(key)
                refresh_keys.update((key,))

        if refresh_keys:
            self.refresh_indexes(refresh_keys)

    def remove_index(self, key):
        """
        Removes an index on the given key
        :param key: key to remove index on
        """
        if key in self.index_keys:
            del self.index[key]
            self.index_keys.remove(key)
        else:
            raise KeyError("Index does not exist")

    def refresh_indexes(self, keys):
        """
        Clear and rebuild indexes
        :param keys: which indexes to refresh
        """
        for key in keys:
            if key not in self.index_keys:
                raise KeyError("Index does not exist")
            self.index[key] = {}

        for line in self.keys():
            for key in keys:
                if key in self[line].keys():
                    if self[line][key] in self.index[key]:
                        self.index[key][self[line][key]].update((line,))
                    else:
                        self.index[key][self[line][key]] = {line}

    def refresh_all_indexes(self):
        if self.index_keys:
            self.refresh_indexes(self.index_keys)

    def keys(self):
        """
        Returns a list of primary keys in the Table
        :return: list of keys
        """
        keys = list(
            set(
                map(
                    lambda table: '.'.join(table.split('.')[1:]),
                    filter(
                        lambda key: key.split('.')[0] == self.name, list(self.parent.raw_keys())
                    )
                )
            )
        )
        return keys

    def sync(self):
        """
        If writeback enabled on database, initiate a sync
        """
        self.parent.sync()

    def __getitem__(self, key):
        # Replace Key Errors With Blank Values
        comp_key = '.'.join((self.name, str(key)))
        self.parent.logger.debug("pyasdb: Getting key: " + comp_key)
        return self.parent.raw_get(comp_key)

    def __setitem__(self, key, value, sync=False):
        """
        :param key: primary key of entry
        :param value: new contents
        :param sync: boolean specifying to immediately sync if in writeback mode
        """
        key = str(key)
        if not isinstance(value, dict) and not self.__meta:
            raise TypeError("Value must be a dictionary")

        # if empty then no need to check for keys that match the index
        if not self.__meta and self.index_keys:
            # store the old value in memory
            old_value = self[key]

            # Iterate over the keys in the new value
            for field in value.keys():
                # If it's one of the easy stringable types don't hash it, otherwise hash to increase versatility
                if type(field) in (str, int, float):
                    field_hash = field
                else:
                    field_hash = hash(field)

                # Check if there's an index on the field before continuing
                if field_hash in self.index_keys:
                    # Use try except to combine a few different checks and hopefully minimize performance impact
                    try:
                        # Has the value changed?
                        update = old_value[field] != value[field]
                        # Do we need to clean up old index entries?
                        old_exists = True
                    except KeyError:
                        update = True
                        old_exists = False

                    # Obviously if it hasn't changed then the index shouldn't have changed either
                    if update:
                        # Check to see if the value is unique
                        new_index = value[field] not in self.index[field_hash]

                        # If an old value existed, we need to remove it from now incorrect indexes
                        if old_exists:
                            try:
                                self.index[field_hash][old_value[field]].remove(key)
                            except KeyError:
                                # Soft fail
                                self.parent.logger.warning(
                                    "pyasdb: Failed to remove key from index, update index required")

                        # If it doesn't exist we can just create it, otherwise we need to update the existing set
                        if new_index:
                            # store as a set because the keys are always going to be unique
                            self.index[field_hash][value[field]] = {key}
                        else:
                            index = self.index[field_hash][value[field]]
                            index.update((key,))
                            self.index[field_hash][value[field]] = index

        comp_key = '.'.join((self.name, key))
        self.parent.logger.debug("pyasdb: Setting key: " + comp_key)
        self.parent.raw_write(comp_key, value)

    def update(self, key, obj):
        """
        Applies partial updates without erasing data via the dict.update mechanism
        :param key: primary key of entry
        :param obj: dictionary containing new values to be merged with entry
        """
        key = str(key)
        tmpx = self.__getitem__(key)
        tmpx.update(obj)
        self.__setitem__(key, tmpx)

    def __delitem__(self, key):
        key = str(key)
        comp_key = '.'.join((self.name, key))
        self.parent.logger.debug("pyasdb: Deleting key: " + comp_key)
        self.parent.raw_delete(comp_key)

    def __contains__(self, key):
        return key in self.keys()

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

    def __repr__(self):
        return "<pyasdb.DB.Table: " + self.name + ">"

    def query(self, *args, **kwargs):
        """
        Generates Query object and runs initial query.
        See Query.query for inputs.
        """
        query = Query(self, self.keys())
        return query.query(*args, **kwargs)

    def query_none(self, *args, **kwargs):
        """
        Generates an initial query_none and returns a Query object.
        :param field: the field being search
        """
        query = Query(self, self.keys())
        return query.query_none(*args, **kwargs)


class DB:
    """
    A simple offline local pythonic database backed by shelve/pickle
    """
    def __init__(self, filename=None, flag='c', writeback=False, backend=None):
        """
        Database constructor
        :param filename: Path and filename of the database file to use (ignored if backend provided)
        :param flag: flag passed through to Shelve.open
        :param writeback: Whether to enable writeback mode
        :param backend: (alternative) Accepts open DBM handler or dict object (overrides all other arguments)
        """

        self.logger = logging.getLogger('pyasdb')

        if backend is None:
            self.dbm = dumbdbm.open(filename, flag)
        else:
            self.dbm = backend

        self.shelf = shelve.Shelf(self.dbm, writeback=False)
        self.raw_dict = {}

        self.writeback = writeback
        self.lock = Lock()
        self.__nulllock = nullcontext(True)
        self.threadsafe = True
        self.__keycache = []
        self.__bulkcache = ()

        self.filename = filename

        self.tables = {}

        atexit.register(self.close)

    def keys(self):
        """
        :return: List of all tables in the database
        """

        keys = list(set(map(lambda key: key.split('.')[0], list(self.shelf))))
        for key in self.raw_dict.keys():
            if key not in keys:
                keys.append(key)
        return keys

    def raw_keys(self):
        """
        :return: the threadsafe combined keys of the writeback cache and the underlying shelf
        """

        # __bulkcache should only be set when doing a bulk operation
        # When set use it to avoid a RunTime error on .keys()
        if self.__bulkcache:
            return self.__bulkcache

        keys = list(self.shelf.keys())
        for key in self.raw_dict.keys():
            if key not in keys:
                keys.append(key)
        return keys

    def get_bulk_lock(self):
        """
        :return: Lock object for bulk operations
        """
        self.threadsafe = False
        # Set __bulkcache so .keys() can be used while doing a bulk operation
        self.__bulkcache = tuple(self.raw_keys())
        return self.lock

    def release_bulk_lock(self):
        """
        Releases bulk lock
        """
        # Clear __bulkcache so the database resumes normal key usage
        self.__bulkcache = ()
        self.threadsafe = True

    def sync(self, lock=True):
        """
        If writeback enabled, manually sync
        """
        if lock:
            lockhandle = self.lock
        else:
            lockhandle = nullcontext(True)

        with lockhandle:
            keylist = tuple(self.raw_dict.keys())
            for key in keylist:
                self.shelf[key] = self.raw_dict[key]
                del self.raw_dict[key]

    def backup(self, filename=None, flag='n', backend=None, writeback=True):
        """
        Creates a backup of the current database
        :param filename: Path and filename of the database file to use (ignored if backend provided)
        :param flag: flag passed through to Shelve.open
        :param backend: (alternative) Accepts open DBM handler (overrides all other arguments)
        :writeback: Defaults to True to help protect against corruption in mid-copy
        """

        if backend is None:
            backend = dumbdbm.open(filename, flag)

        backupshelf = shelve.Shelf(backend, writeback=writeback)

        keylist = []

        for key in self.shelf.keys():
            backupshelf[key] = self.shelf[key]
            keylist.append(key)

        for key in backupshelf.keys():
            if not key in keylist:
                del backupshelf[key]

        backupshelf.sync()
        backupshelf.close()
        backend.close()

    def __getitem__(self, key):
        """
        Returns a Table, will create a new Table if one does not already exist.
        :param key:
        :return:
        """
        key = str(key)
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

    def raw_get(self, key):
        # Perform a check of the check before pulling to avoid KeyErrors
        # Not using a try here as the key should *usually* be absent
        # RuntimeError will occur during threading sometimes while using .keys()
        self.logger.debug(f'pyasdb: raw_get({key})')
        try:
            if key in self.raw_dict.keys():
                return self.raw_dict[key]
        except RuntimeError:
            self.logger.warning('pyasdb: RuntimeError while searching raw_dict.keys()')

        try:
            return self.shelf[key]
        except KeyError:
            self.logger.debug(f'pyasdb: KeyError {key}')
            return {}

    def raw_write(self, key, value):
        self.logger.debug(f'pyasdb: raw_write({key}, {value})')
        if self.threadsafe:
            lock = self.lock
        else:
            lock = self.__nulllock

        with lock:
            self.raw_dict[key] = value
            if not self.writeback:
                self.sync(lock=False)

    def raw_delete(self, key):
        self.logger.debug(f'pyasdb: raw_delete({key})')
        if self.threadsafe:
            lock = self.lock
        else:
            lock = self.__nulllock

        with lock:
            if key in self.raw_dict[key].keys():
                del self.raw_dict[key]
            if key in self.shelf.keys():
                del self.shelf[key]

    def close(self):
        self.logger.debug('pyasdb: close()')
        self.sync()
        self.shelf.close()
        if 'close' in dir(self.dbm):
            self.dbm.close()
