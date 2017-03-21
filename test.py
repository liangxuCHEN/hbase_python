# encoding=utf-8
from nose.tools import (
    assert_in,
    assert_is_instance,
    assert_is_not_none,
    assert_raises,
)
from hbasepy import Connection
import six

HAPPYBASE_HOST = 'master'
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
    print (names)
    assert_in(table_name, names)


def test_create_table(table_name):
    cfs = {
        'cf1': {},
        'cf2': None,
        'cf3': {'max_versions': 1},
    }
    connection.create_table(table_name, families=cfs)

    test_table_listing(table_name)


def test_invalid_table_create():
    with assert_raises(ValueError):
        connection.create_table('sometable', families={})
    with assert_raises(TypeError):
        connection.create_table('sometable', families=0)
    with assert_raises(TypeError):
        connection.create_table('sometable', families=[])


def test_families(table_name):
    table_tmp = connection.table(table_name)
    assert_is_not_none(table_tmp)
    families = table_tmp.families()
    for name, fdesc in six.iteritems(families):
        print (name, fdesc)


def test_get_row(table_name, row, column):
    table_tmp = connection.table(table_name)
    print (table_tmp.row(row, columns=column))


def test_enable_table(table_name):
    print (connection.is_table_enabled(table_name))
    connection.disable_table(table_name)
    print (connection.is_table_enabled(table_name))
    connection.enable_table(table_name)
    print (connection.is_table_enabled(table_name))


def test_delete_table(table_name):
    test_table_listing(table_name)
    connection.disable_table(table_name)
    connection.delete_table(table_name)
    test_table_listing(table_name)


def test_table_regions(table_name):
    table_tmp = connection.table(table_name)
    regions = table_tmp.regions()
    assert_is_instance(regions, list)
    print (regions)


def test_put(table_name):
    table_tmp = connection.table(table_name)
    # table_tmp.put(b'Jim', {b'basicInfo:age': b'16', b'moreInfo:score': b'85', b'basicInfo:class': b'c6'})
    table_tmp.put(b'May', {b'moreInfo:health': b'good'})


def test_atomic_counters():
    """
    创建一个计数器统计column
    第一次创建的时候，可以用counter_set,设定初始值，
    后面counter值使用counter_dec 或 counter_inc 改变
    """

    row = b'May'
    column = b'basicInfo:counter'
    table_name = 'students'
    table_tmp = connection.table(table_name)

    # print(table_tmp.counter_dec(row, column))
    # print (table_tmp.counter_get(row, column))
    print (table_tmp.counter_inc(row, column, 3))


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    test_connection()
    # test_table_listing()
    # test_create_table('table2')
    # test_invalid_table_create()
    # test_families('students')
    # test_get_row('students', b'Tom', [b'basicInfo:age'])
    # test_enable_table('table2')
    # test_delete_table('table2')
    #test_table_regions('students')
    # test_put('students')
    test_atomic_counters()



