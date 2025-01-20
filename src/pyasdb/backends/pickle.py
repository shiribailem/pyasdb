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

    def close(self):
        if not self.closed:
            with open(self.filename, 'wb') as file:
                file.write(pickle.dumps(self.data))
            self.closed = True
            self.data = None

    def sync(self):
        pass

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
