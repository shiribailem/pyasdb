import os
import pytest
from src.pyasdb.pyasdb import DB, Table, Entry
import src.pyasdb.queries as queries
from src.pyasdb.backends import PickleDBM


@pytest.fixture
def db_instance():
    db = DB(backend={})
    yield db


@pytest.fixture
def db_file_instance():
    db = DB(backend=PickleDBM('test.pickle', debug=True), needsshelf=False)
    yield db
    db.close()
    try:
        os.remove('test.pickle')
        os.remove('test.pickle.md5sum')
    except FileNotFoundError:
        pass
    try:
        os.remove('backup.pickle')
        os.remove('backup.pickle.md5sum')
    except FileNotFoundError:
        pass


@pytest.fixture
def db_table_instance():
    db = DB(backend={}, needsshelf=False)
    yield db['table']


@pytest.fixture
def db_sample_data():
    db = DB(backend={}, needsshelf=False)
    table = db['table']
    for i in range(5):
        table[f'row{i}'] = {'key': f'value{i}', 'deep': {'key': 10}}

    for i in range(5):
        table[f'int-row{i}'] = {'key': i, 'deep': {'key': -10}}

    table["difference_line"] = {'test': "alt data"}
    yield table


def test_db_created(db_instance):
    assert isinstance(db_instance, DB)


def test_db_file_created(db_file_instance):
    assert isinstance(db_file_instance, DB)


def test_initial_keys(db_instance):
    assert db_instance.keys() == []


def test_raw_get_without_existent_key(db_instance):
    assert db_instance.raw_get('nonsense') == {}


def test_raw_write_and_raw_get(db_instance):
    db_instance.raw_write('key', 'value')
    assert db_instance.raw_get('key') == 'value'


def test_raw_delete(db_instance):
    db_instance.raw_write('key', 'value')
    db_instance.raw_delete('key')
    assert db_instance.raw_get('key') == {}


def test_sync(db_instance):
    db_instance.raw_write('key', 'value')
    db_instance.sync()
    assert not 'key' in db_instance.raw_dict.keys()


def test_backup(db_file_instance):
    db_file_instance.raw_write('key', 'value')
    db_file_instance.backup(backend=PickleDBM('backup.pickle'), needsshelf=False)
    assert os.path.exists('backup.pickle')


def test_close(db_instance):
    db_instance.close()
    with pytest.raises(ValueError):
        db_instance.raw_get('key')


def test_journal_creation(db_file_instance):
    db_file_instance.raw_write('key', 'value')
    assert os.path.exists('test.pickle.journal')


def test_journal_recovery(db_file_instance):
    db_file_instance.raw_write('key', 'value')
    assert os.path.exists('test.pickle.journal')
    new_db = DB(backend=PickleDBM('test.pickle', debug=True), needsshelf=False)
    assert new_db.raw_get('key') == 'value'
    new_db.close()
    #os.remove('test.pickle')
    #os.remove('test.pickle.md5sum')


def test_get_table(db_table_instance):
    assert isinstance(db_table_instance, Table)


def test_table_row_creation(db_table_instance):
    assert isinstance(db_table_instance['row'], Entry)


def test_table_write(db_table_instance):
    db_table_instance['row'] = {'key': 'entry'}
    assert db_table_instance['row']['key'] == 'entry'


def test_basic_query_function(db_sample_data):
    assert db_sample_data.query('key', queries.eq, checktype=int, compare=2).results == ['int-row2']


def test_query_none_function(db_sample_data):
    assert db_sample_data.query_none('key').results == ['difference_line']


def test_deep_query_function(db_sample_data):
    results = db_sample_data.query(('deep', 'key'), queries.eq, checktype=int, compare=10).results
    results.sort()
    assert results == ['row0', 'row1', 'row2', 'row3', 'row4']