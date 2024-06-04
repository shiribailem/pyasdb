try:
    import dateutil
except ImportError:
    dateutil = None

from csv import DictReader
from ast import literal_eval


def csv_import(file, table, index, autoconvert=True):
    data = DictReader(file)

    row: dict
    for row in data:
        if index not in row.keys():
            continue

        if autoconvert:
            removekeys = []
            for key in row:
                if row[key] == '' or row[key] is None:
                    removekeys.append(key)
                else:
                    try:
                        row[key] = literal_eval(row[key])
                    except ValueError:
                        if dateutil:
                            try:
                                row[key] = dateutil.parser.parse(row[key])
                            except dateutil.parser.ParserError:
                                pass
            for key in removekeys:
                row.pop(key)

        table[str(row[index])] = row
