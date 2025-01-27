import shelve
import atexit
from threading import Lock
from dbm import dumb as dumbdbm
from contextlib import nullcontext
import logging
import typing
from copy import deepcopy
from time import sleep


class Special:
    """
    This is a parent primitive for special entry operations.

    This contains no behavior on its own but by inheritance allows entries to recognize
    special values.

    When encountered in an entry, instead of passing this object directly the entry will
    return the results of call() with the current entry provided as an argument.
    """
    def __init__(self):
        pass

    def call(self, entry):
        pass


class Join(Special):
    """
    Special Join

    This is a One-To-One join function.
    """
    def __init__(self, table, field=None):
        super().__init__()
        self.table = table
        self.field = field

    def call(self, entry):
        if self.field:
            return self.table[entry[self.field]]
        else:
            return self.table[entry.key]


class OneToManyJoin(Special):
    """
    Special Simple Join Using Queries

    This is a special One-To-Many join function.

    It is recommended for there to be an index on the queryField.

    Will return a list of entries that match.
    """
    def __init__(self, table, queryField, field=None):
        super().__init__()
        self.table = table
        self.field = field
        self.queryField = queryField

    def call(self, entry):
        if self.field:
            return self.table.query(self.queryField, lambda x: x == entry[self.field]).entries()
        else:
            return self.table.query(self.queryField, lambda x: x == entry.key).entries()


class TranslationJoin(Special):
    """
    Special Join Using Translation Tables

    This is a modified One-To-One join function.

    This join expects a specially formatted table in addition to normal join parameters.

    The reference table entries must contain a 'reference_key' value, which will be used
    to pull the key from the given table.

    This join allows for multiple different keys to all reference the same entry.
    Great for imported reports where the names might be potentially entered in different
    formats.
    """
    def __init__(self, table, translationtable, field=None):
        super().__init__()
        self.table = table
        self.field = field
        self.translationtable = translationtable

    def call(self, entry):
        key = entry[self.field] if self.field else entry.key
        data = self.translationtable[key]
        if data:
            return self.table[data['reference_key']]
        else:
            return None


class Entry:
    """
    Represents an Entry in a data store, providing structured access to its content.

    Entry objects act as intermediaries for accessing and manipulating the content
    of a data store. They can work with dictionary-like or list-like structures and
    maintain a handle to the underlying data store. The class supports features like
    auto-updating the data store and using default values for missing keys.

    Attributes:
        handle (Table | Entry | dict): The data source or upper-level entry this entry is associated with.
        key (str): The key of the entry in the data store.
        value (dict | list): The content of this entry, which must be either a dictionary or a list.
        auto_update (bool): Whether changes to the entry automatically reflect in the data store.
        defaults (dict | list | None): A dictionary or list containing default values, of the same type
            as the value.

    Raises:
        TypeError: If the handle is not a Table, Entry, or dict.
        TypeError: If value is not a dictionary or list.
        TypeError: If defaults are provided but are not of the same type as value.

    """
    def __init__(self, handle, key, value, auto_update=False, defaults=None):
        self.handle = handle
        if isinstance(self.handle, Table):
            self.top_level = True
        elif isinstance(self.handle, Entry):
            self.top_level = False
        elif isinstance(self.handle, dict):
            self.top_level = True
        else:
            raise TypeError("Handle Object must be Table, Entry, or dict")
        self.key = key
        self.value = value
        self.auto_update = auto_update
        if isinstance(value, dict):
            self.list = False
        elif isinstance(value, list):
            self.list = True
        else:
            raise TypeError("Value must be a dictionary or list")

        if defaults:
            if not isinstance(defaults, type(value)):
                raise TypeError("Defaults must be same type as value")

        self.defaults = defaults

    def db_write(self):
        """
        Writes the current object's data into the database handle.

        This method ensures the current object's data is appropriately written to the provided
        database handle. If the object is marked as top-level, the associated key-value pair
        is directly written to the handle. If not a top-level object, the method delegates the
        writing operation to the handle's internal write mechanism.

        Returns:
            None

        """
        if self.top_level:
            self.handle[self.key] = self.value
        else:
            self.handle.write()

    def recursive_get(self, keys):
        try:
            if not isinstance(keys, tuple):
                return self[keys]
            if len(keys) == 1:
                return self[keys[0]]
            else:
                return self[keys[0]].recursive_get(keys[1:])
        except KeyError:
            return None

    def __getitem__(self, key):
        if self.list and not isinstance(key, int):
            raise KeyError("Entry is a list, key must be an integer")
        elif not self.list and not isinstance(key, typing.Hashable):
            raise KeyError("Entry is a dictionary, key must be a Hashable")

        value = None

        if self.defaults:
            if key in self.defaults.keys():
                if isinstance(self.defaults[key], Special):
                    return self.defaults[key].call(self)
                if key not in self.value:
                    self.value[key] = deepcopy(self.defaults[key])
        value = self.value[key]

        # If somehow an entry does make it into the database, fix it and throw a message.
        # This shouldn't render a database unreadable, it's mostly just a problem to use the entry as-is
        # so you're not accidentally mixing versions of the object.
        if isinstance(value, Entry):
            value = value.value
            print("WARNING: Entry object found in database.")

        if isinstance(value, dict) or isinstance(value, list):
            try:
                value = Entry(self.handle, key, value, self.auto_update, self.defaults[key])
            except TypeError:
                value = Entry(self.handle, key, value, self.auto_update)

        return value

    def __setitem__(self, key, value):
        # Avoid accidentally writing an entry object to the database
        if isinstance(value, Entry):
            value = value.value

        if isinstance(value, Special):
            raise ValueError("Special Objects Can Not Be Assigned To Entries, They Should Only Be Assigned To Defaults")

        if self.defaults and key in self.defaults.keys():
            if isinstance(self.defaults[key], Special):
                raise ValueError("Special Objects Can Not Be Changed Outside of Defaults Definitions")

        self.value[key] = value
        if self.auto_update or not self.top_level:
            self.db_write()

    def __repr__(self):
        return f"<Entry {self.key}: {self.value}>"

    def __getattr__(self, key):
        result = getattr(self.value, key)
        if self.auto_update:
            self.db_write()
        return result

    def __delitem__(self, key):
        del self.value[key]
        if self.auto_update or not self.top_level:
            if self.auto_update:
                self.db_write()

    def __bool__(self):
        if self.value:
            return True
        else:
            return False


class Query:
    """
    Query Class For Making And Managing Results of Queries.

    """
    # TODO: Rewrite to work with entries rather than keys
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

        if type(field) in (str, int, float):
            field = field
        elif not isinstance(field, tuple):
            field = hash(field)

        # Check if there's an index related to this search, if so use that index
        if field in self.table.index_keys and len(self.table.index[field].keys()) < len(self.results):
            # Use a set for results because it'll quickly eliminate duplicates and allow
            # intersection update with existing results
            results = set()
            for key in self.table.index[field].keys():
                if compare and func(key, compare):
                    results.update(self.table.index[field][key])
                elif not compare and func(key):
                    results.update(self.table.index[field][key])

            # This merges the new search with existing results, returning only entries that are in both
            results.intersection_update(self.results)

            # If count is set, return only that many entries
            if count:
                return Query(self.table, list(results)[0:count])
            else:
                return Query(self.table, results)

        if compare:
            results = filter(
                    lambda key:
                    self.table[key].recursive_get(field) is not None and
                    (
                            checktype is None or
                            isinstance(self.table[key].recursive_get(field), checktype)
                    ) and
                    func(self.table[key].recursive_get(field), compare), self.results)

        else:
            results = filter(
                lambda key:
                    self.table[key].recursive_get(field) is not None and
                    (
                        checktype is None or
                        isinstance(self.table[key].recursive_get(field), checktype)
                    ) and
                    func(self.table[key].recursive_get(field)), self.results)

        # not 100% certain this works, in theory the filter returns an iterable object and on each iteration
        # it runs comparisons until it finds a match then returns just that entry. Which means this will run
        # comparisons only as long as it takes to get to count, saving time.
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
        :param count: specify the maximum number of results to return
        """
        if type(field) in (str, int, float):
            field = field
        elif not isinstance(field, tuple):
            field = hash(field)

        results = filter(
            lambda key: self.table[key].recursive_get(field) is None, self.results)

        # not 100% certain this works, in theory the filter returns an iterable object and on each iteration
        # it runs comparisons until it finds a match then returns just that entry. Which means this will run
        # comparisons only as long as it takes to get to count, saving time.
        if count:
            new_results = []
            for _ in range(count):
                try:
                    new_results.append(next(results))
                except StopIteration:
                    break
            return Query(self.table, new_results)

        return Query(self.table, list(results))

    def entries(self):
        values = []
        for key in self.results:
            values.append(self.table[key])
        return values

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
    def __init__(self, parent, name, meta=False, defaults=None):
        """
        Table Constructor
        :param parent: handle of the DB
        :param name: name of the Table in the DB
        """
        self.parent = parent
        self.name = name
        self.__meta = meta
        self.defaults = defaults

        # When set to true the Entries entered by the table will be set to auto_update mode.
        self.synchronous_entries = False

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
        tmp_index = {}
        for key in keys:
            if key not in self.index_keys:
                raise KeyError("Index does not exist")
            tmp_index[key] = {}

        for line in self.keys():
            for key in keys:
                if key in self[line].keys():
                    if self[line][key] in tmp_index[key]:
                        tmp_index[key][self[line][key]].update((line,))
                    else:
                        tmp_index[key][self[line][key]] = {line}

        for key in self.index_keys:
            self.index[key] = tmp_index[key]

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

        # Wrap results in an Entry object to support advanced functions
        if not self.__meta:
            return Entry(self, key, self.parent.raw_get(comp_key),
                         defaults=self.defaults, auto_update=self.synchronous_entries)
        return self.parent.raw_get(comp_key)

    def __setitem__(self, key, value, sync=False):
        """
        :param key: primary key of entry
        :param value: new contents
        :param sync: boolean specifying to immediately sync if in writeback mode
        """
        key = str(key)

        # Entry objects should never be saved to the database
        if isinstance(value, Entry):
            value = value.value

        if not isinstance(value, dict) and not self.__meta:
            raise TypeError("Value must be a dict or Entry object")

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
    def __init__(self, filename=None, flag='c', writeback=False, backend=None, needsshelf=True):
        """
        Database constructor
        :param filename: Path and filename of the database file to use (ignored if backend provided)
        :param flag: flag passed through to Shelve.open
        :param writeback: Whether to enable writeback mode
        :param backend: (alternative) Accepts open DBM handler or dict object (overrides all other arguments)
        :param needsshelf: For alternate backends that don't need shelf inbetween. Set False to assign values
         directly to the backend. Default True.
        """

        self.logger = logging.getLogger('pyasdb')
        self.needsshelf = needsshelf

        if backend is None:
            self.dbm = dumbdbm.open(filename, flag)
        else:
            self.dbm = backend

        if needsshelf:
            self.shelf = shelve.Shelf(self.dbm, writeback=False)
        else:
            self.shelf = backend
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

        keys = list(set(map(lambda key: key.split('.')[0], list(self.shelf.keys()))))
        for key in self.raw_dict.keys():
            if key not in keys:
                keys.append(key)

        for key in list(keys):  # Use list to copy keys, because we modify the list during iteration
            if key.endswith('__index'):
                keys.remove(key)

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
        # Use a cache to limit iteration activity
        raw_keys = list(self.raw_dict.keys())

        for key in raw_keys:
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
            try:
                self.shelf.sync()
            except AttributeError:
                # sync isn't on all backends
                pass

    def backup(self, filename=None, flag='n', backend=None, writeback=True, needsshelf=True):
        """
        Creates a backup of the current live database.
        :param filename: Path and filename of the database file to use (ignored if backend provided)
        :param flag: flag passed through to Shelve.open
        :param backend: (alternative) Accepts open DBM handler (overrides all other arguments)
        :writeback: Defaults to True to help protect against corruption in mid-copy
        """

        if backend is None:
            backend = dumbdbm.open(filename, flag)

        if needsshelf:
            backupshelf = shelve.Shelf(backend, writeback=writeback)
        else:
            backupshelf = backend

        for key in self.shelf.keys():
            backupshelf[key] = self.shelf[key]

        removed_keys = set(backupshelf.keys())
        removed_keys.difference_update(set(self.shelf.keys()))

        for key in removed_keys:
            try:
                del backupshelf[key]
            except KeyError:
                # No need to do anything if for some reason it's not there anyways?
                pass

        try:
            backupshelf.sync()
        except AttributeError:
            # Not all backends have sync
            pass
        try:
            backupshelf.close()
        except AttributeError:
            # Not all backends have close (ie. in memory dict)
            pass

        try:
            backend.close()
        except:
            # Sometimes the backend is already closed by prior close.
            pass

    def set_table_defaults(self, table, defaults):
        """
        Sets default values for a specific table in the database. If the table does not
        exist in the current context, it creates a new table instance with the provided
        default values. If the table already exists, it updates the existing table's
        default values.

        Parameters
        ----------
        table : Any
            The identifier of the table for which defaults are to be set. It can be any
            supported type that represents a table name/key.
        defaults : dict
            A dictionary containing default values to be associated with the specified
            table.

        Raises
        ------
        TypeError
            If the `defaults` argument is not a dictionary.
        """
        if not isinstance(defaults, dict):
            raise TypeError("Defaults must be a dictionary")

        if table not in self.tables.keys():
            self.tables[table] = Table(self, table, defaults=defaults)
        else:
            self.tables[table].defaults = defaults

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
            if key in self.raw_dict.keys():
                del self.raw_dict[key]
            if key in self.shelf.keys():
                del self.shelf[key]

    def close(self):
        self.logger.debug('pyasdb: close()')
        self.sync()
        try:
            self.shelf.close()
        except AttributeError:
            pass
        if 'close' in dir(self.dbm):
            try:
                self.dbm.close()
            except:
                # Sometimes the close just fails and can't assume all errors
                pass
