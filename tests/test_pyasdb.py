import os
import pytest
from src.pyasdb.pyasdb import DB, Table, Entry


@pytest.fixture
def db_instance():
    db = DB(backend={})
    yield db


@pytest.fixture
def db_file_instance():
    db = DB('test_db')
    yield db
    db.close()
    os.remove('test_db.dat')
    os.remove('test_db.bak')
    os.remove('test_db.dir')
    try:
        os.remove('backup_db.dat')
        os.remove('backup_db.bak')
        os.remove('backup_db.dir')
    except FileNotFoundError:
        pass


@pytest.fixture
def db_table_instance():
    db = DB(backend={})
    yield db['table']


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
    db_file_instance.backup('backup_db')
    assert os.path.exists('backup_db.dat')


def test_close(db_instance):
    db_instance.close()
    with pytest.raises(ValueError):
        db_instance.raw_get('key')


def test_get_table(db_table_instance):
    assert isinstance(db_table_instance, Table)


def test_table_row_creation(db_table_instance):
    assert isinstance(db_table_instance['row'], Entry)


def test_table_write(db_table_instance):
    db_table_instance['row'] = {'key': 'entry'}
    assert db_table_instance['row']['key'] == 'entry'


