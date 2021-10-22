import sqlite3
from datetime import datetime
from http_session_sqlite.store import SQLiteStore
from freezegun import freeze_time


def test_store():
    store = SQLiteStore(sqlite3.connect(":memory:"), 300)
    store.set('test', {'this': 'is a session'})
    assert store.get('test') == {'this': 'is a session'}
    assert store.get('nothing') == {}


def test_timeout():
    store = SQLiteStore(sqlite3.connect(":memory:"), 300)

    with freeze_time('2021-10-22 21:00:00'):
        store.set('test', {'this': 'is a session'})
        assert store.get('test') == {'this': 'is a session'}

    with freeze_time('2021-10-22 21:05:01'):
        assert store.get('test') == {}


def test_touch():
    store = SQLiteStore(sqlite3.connect(":memory:"), 300)

    with freeze_time('2021-10-22 21:00:00'):
        store.set('test', {'this': 'is a session'})
        assert store.get('test') == {'this': 'is a session'}

    sessions = list(store)
    assert len(sessions) == 1
    assert sessions[0]['mtime'] == 1634936400

    with freeze_time('2021-10-22 21:04:00'):
        store.touch('test')

    sessions = list(store)
    assert len(sessions) == 1
    assert sessions[0]['mtime'] == 1634936640

    with freeze_time('2021-10-22 21:05:01'):
        assert store.get('test') == {'this': 'is a session'}


def test_flush():
    store = SQLiteStore(sqlite3.connect(":memory:"), 300)

    with freeze_time('2021-10-22 21:00:00'):
        store.set('test', {'this': 'is a session'})

    with freeze_time('2021-10-22 21:01:00'):
        store.set('another test', {'this': 'is another session'})

    with freeze_time('2021-10-22 21:01:30'):
        store.set('foo', {'bar': 'qux'})

    sessions = list(store)
    assert len(sessions) == 3

    with freeze_time('2021-10-22 21:06:01'):
        store.flush_expired_sessions()

    sessions = list(store)
    assert len(sessions) == 1
    assert sessions[0]['sid'] == 'foo'
