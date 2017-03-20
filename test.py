from nose.tools import (
    assert_in,
    assert_is_instance,
    assert_is_not_none,
)
from hbasepy import Connection
import six

HAPPYBASE_HOST = '192.168.3.83'
HAPPYBASE_PORT = 9090

TEST_TABLE_NAME = 'students'

connection_kwargs = dict(
    host=HAPPYBASE_HOST,
    port=HAPPYBASE_PORT,
)

connection = table = None


def test_connection():
    global connection, table
    connection = Connection(**connection_kwargs)
    assert_is_not_none(connection)


def test_table_listing(table_name):
    names = connection.tables()
    assert_is_instance(names, list)
    print names
    assert_in(table_name, names)


def test_create_table(table_name):
    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions': 1},
    }
    connection.create_table(table_name, families=cfs)

    test_table_listing(table_name)


def test_families(table_name):
    table = connection.table(table_name)
    assert_is_not_none(table)
    families = table.families()
    for name, fdesc in six.iteritems(families):
        print name, fdesc

if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.DEBUG)
    test_connection()
    #test_table_listing()
    #test_create_table('table2')
    test_families('students')

