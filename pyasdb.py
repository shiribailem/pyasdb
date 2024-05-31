import shelve


class Query:
    def __init__(self, table, results):
        self.table = table
        self.results = results

    def query(self, field, func):
        field = str(field)
        return Query(self.table, list(
            filter(
                lambda key: field in self.table[key].keys() and func(self.table[key][field]), self.results)
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
        if isinstance(key, int):
            return self.table[self.results[key]]
        elif isinstance(key, str):
            return self.table[key]
        else:
            raise KeyError

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.table[self.results[key]] = value
        elif isinstance(key, str):
            self.table[key] = value
        else:
            raise KeyError

    def __repr__(self):
        return "<pyasdb.DB.Table.Query: " + self.table.name + " - " + ','.join(self.results) + ">"


class Table:
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name

    def keys(self):
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
        self.parent.sync()

    def __getitem__(self, key):
        return self.parent.shelf['.'.join((self.name, key))]

    def __setitem__(self, key, value, sync=False):
        self.parent.shelf['.'.join((self.name, key))] = value
        if sync:
            self.parent.sync()

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
        query = Query(self, self.keys())
        return query.query(field, func)


class DB:
    def __init__(self, filename, flag='c', writeback=False):
        self.shelf = shelve.open(filename, flag, writeback=writeback)
        self.writeback = writeback

        tableNames = list(set(map(lambda key: key.split('.')[0], list(self.shelf))))
        self.tables = {}

        for table in tableNames:
            self.tables[table] = Table(self, table)

    def keys(self):
        return list(self.tables.keys())

    def sync(self):
        if self.writeback:
            self.shelf.sync()

    def __getitem__(self, key):
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
