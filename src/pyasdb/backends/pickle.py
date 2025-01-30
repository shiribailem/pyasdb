import os
import pickle


class PickleDBM:
    def __init__(self, filename, debug=False):
        self.filename = filename
        self.debug = debug
        self.closed = False

        try:
            with open(filename, 'rb') as file:
                data = file.read()
        except FileNotFoundError:
            data = ""

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
            with open(self.filename + '_new', 'wb') as file:
                file.write(pickle.dumps(self.data))
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
