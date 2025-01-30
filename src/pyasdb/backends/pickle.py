import os
import pickle
from hashlib import md5


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

    def close(self):
        self.sync()
        self.closed = True
        self.data = None

    def sync(self):
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
        self.updated = True

    def __delitem__(self, key):
        if self.closed:
            raise ValueError('PickleDBM Backend No Longer Open.')
        del self.data[key]
        self.updated = True
