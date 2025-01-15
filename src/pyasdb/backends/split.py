from dbm import dumb as dumbdbm
import os


def decode(key):
    if isinstance(key, bytes):
        key = key.decode('utf-8')

    try:
        table, key = key.split('.', 1)
    except ValueError:
        table = '__no_table__'
        print(f'WARNING: KEY WITHOUT TABLE {key}')
    return table, key


class SplitDBM:
    def __init__(self, directory, backend=dumbdbm, flag='c', debug=False):
        self.directory = directory
        self.backend = backend
        self.flag = flag
        self.debug = debug

        # Ensure directory exists
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            
        self.tables = {}

        for file in os.listdir(self.directory):
            tablename = os.path.splitext(file)[0]  # Remove the extension to get the table name
            if self.debug: print(f'Prepopulating table for {tablename}')
            self.table(tablename)

    def close(self):
        for key in self.tables:
            if not self.tables[key]:
                for file in os.listdir(self.directory):
                    if file.startswith(key):
                        if self.debug: print(f'Removing file {file}')
                        os.remove(os.path.join(self.directory, file))
                
            self.tables[key].close()

    def table(self, tablename):
        if tablename not in self.tables.keys():
            self.tables[tablename] = self.backend.open(os.path.join(self.directory, tablename), self.flag)
        return self.tables[tablename]

    def __getitem__(self, key):
        tablename, key = decode(key)

        table = self.table(tablename)

        return table[key]

    def __setitem__(self, key, value):
        tablename, key = decode(key)

        table = self.table(tablename)

        table[key] = value

    def __delitem__(self, key):
        tablename, key = decode(key)

        table = self.table(tablename)

        del table[key]

    def __contains__(self, key):
        tablename, key = decode(key)
        table = self.table(tablename)
        return key in table

    def keys(self):
        keys = []
        for key in self.tables.keys():
            keys.extend(map(lambda x: (key + '.' + x.decode('utf-8')).encode(), self.tables[key].keys()))
        return keys

