try:
    import dateutil.parser
except ImportError:
    dateutil = None

from csv import DictReader
from ast import literal_eval


def csv_import(file, db, tablename, index, autoconvert=True, debug=False, hints=None):
    if hints is None:
        hints = {}

    if debug:
        print(f'Importing file into db')
    data = DictReader(file)
    if debug:
        print(f'file mapped')
        print("Getting Bulk Lock")

    with db.get_bulk_lock():
        if debug:
            print("Bulk Lock acquired")
        table = db[tablename]
        if debug:
            print("Table acquired")

        row: dict
        for row in data:
            if index not in row.keys():
                continue

            if debug:
                backspace = '\b'
                print(f'{ backspace * 20 } Row: { row[index] }', end='')

            if autoconvert:
                removekeys = []
                for key in row:
                    if row[key] == '' or row[key] is None:
                        removekeys.append(key)
                    elif key not in hints.keys() or hints[key] != str:
                        if row[key].isdigit():
                            row[key] = int(row[key])
                        else:
                            try:
                                row[key] = float(row[key])
                            except ValueError:
                                try:
                                    row[key] = literal_eval(row[key])
                                except (ValueError, SyntaxError):
                                    if dateutil:
                                        try:
                                            row[key] = dateutil.parser.parse(row[key])
                                        except (ValueError, OverflowError):
                                            pass

                for key in removekeys:
                    row.pop(key)

            for key in row:
                if key in hints.keys() and type(row[key]) is not hints[key]:
                    row.pop(key)

            table[str(row[index])] = row
    db.release_bulk_lock()
    if debug:
        print("\nBulk Lock released")