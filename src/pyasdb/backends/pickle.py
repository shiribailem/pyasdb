import os
import pickle
from hashlib import md5
from datetime import datetime


class PickleDBM:
    def __init__(self, filename, debug=False):
        self.filename = filename
        self.debug = debug
        self.closed = False

        try:
            with open(filename, 'rb') as file:
                data = file.read()
        except FileNotFoundError:
            data = b""

        self.checksum = None

        # If filename.md5sum exists load it and check it
        if os.path.exists(filename + '.md5sum'):
            with open(filename + '.md5sum', 'r') as file:
                self.checksum = file.read()
            if md5(data).hexdigest() != self.checksum:
                raise ValueError('MD5SUM MISMATCH')
        elif data:
            self.checksum = md5(data).hexdigest()
        else:
            self.checksum = b''

        if data:
            self.data = pickle.loads(data)
            if not self.data:
                self.data = dict()
        else:
            self.data = dict()

        self.updated = False
        self.journal = []

        if os.path.exists(filename + '.journal'):
            self.recovery()

    def close(self):
        self.flush()
        self.closed = True
        self.data = None

    # Updates the journal with all current activities
    def write(self):
        if not self.closed and len(self.journal) > 0:
            with open(self.filename + '.journal', 'ab') as file:
                for line in self.journal:
                    data = pickle.dumps(line)
                    size = len(data).to_bytes(4)
                    checksum = md5(size+data).digest()
                    file.write(checksum + size + data)
                self.journal = []

    # This is for when the journal isn't needed, write everything to the file
    # and erase the no longer needed journal
    def flush(self):
        if not self.closed and self.updated:
            data = pickle.dumps(self.data)
            new_checksum = md5(data).hexdigest()
            # If the checksum matches then why are we rewriting an identical file?
            if self.checksum != new_checksum:
                with open(self.filename + '_new', 'wb') as file:
                    file.write(data)

                with open(self.filename + '.md5sum', 'w') as file:
                    file.write(new_checksum)
                    self.checksum = new_checksum

                # Delete original and swap in new file
                try:
                    os.remove(self.filename)
                except FileNotFoundError:
                    # If the file is gone then nothing to worry about
                    pass
                os.rename(self.filename + '_new', self.filename)

                # If this point is reached then the data was successfully written
                # and the journal is no longer needed.
                if os.path.exists(self.filename + '.journal'):
                    os.remove(self.filename + '.journal')
            self.updated = False

    def recovery(self):
        if os.path.exists(self.filename + '.journal'):
            with open(self.filename + '.journal', 'rb') as file:
                while True:
                    checksum = file.read(16)
                    if not checksum:
                        break
                    size_raw = file.read(4)
                    size = int.from_bytes(size_raw, 'big')
                    data = file.read(size)
                    if checksum != md5(size_raw+data).digest():
                        raise ValueError(f'MD5SUM MISMATCH')
                    segment = pickle.loads(data)

                    if segment['action'] == 'set':
                        self.data[segment['key']] = segment['value']
                    elif segment['action'] == 'del':
                        del self.data[segment['key']]

    # Now deprecated routine replaced by write
    def sync(self):
        self.write()

    def __getattr__(self, key):
        return getattr(self.data, key)

    def __getitem__(self, key):
        if self.closed:
            raise ValueError('PickleDBM Backend No Longer Open.')
        return self.data[key]

    def __setitem__(self, key, value):
        if self.closed:
            raise ValueError('PickleDBM Backend No Longer Open.')
        self.data[key] = value
        self.journal.append({'action': 'set', 'key': key, 'value': value, 'time': datetime.now()})
        self.updated = True

    def __delitem__(self, key):
        if self.closed:
            raise ValueError('PickleDBM Backend No Longer Open.')
        del self.data[key]
        self.journal.append({'action': 'del', 'key': key, 'time': datetime.now()})
        self.updated = True
