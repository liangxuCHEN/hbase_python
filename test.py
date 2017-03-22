# encoding=utf-8
import random
import threading
from nose.tools import (
    assert_in,
    assert_is_instance,
    assert_is_not_none,
    assert_raises,
    assert_equal
)
from hbasepy import Connection, ConnectionPool, NoConnectionsAvailable
import six

HBASE_HOST = 'master'
HBASE_PORT = 9090
HbASE_COMPAT = '1.24'
TEST_TABLE_NAME = 'students'

connection_kwargs = dict(
    host=HBASE_HOST,
    port=HBASE_PORT,
    compat=HbASE_COMPAT,
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
        'cf3': {'max_versions': 3},
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


def test_batch(table_name):
    table_tmp = connection.table(table_name)
    b = table_tmp.batch()
    b.put(b'row1', {b'cf:col1': b'value1',
                    b'cf:col2': b'value2'})
    b.send()

    # b = table_tmp.batch(timestamp=1490078461188)
    b.put(b'row2', {b'cf:col5': b'value5'})
    b.send()

    b.delete(b'row2')
    b.delete(b'row1', [b'cf1:col2'])
    b.send()


def test_batch_context_managers(table_name):
    table_tmp = connection.table(table_name)

    with table_tmp.batch() as b:
        b.put(b'row4', {b'cf:col3': b'value3'})
        b.put(b'row5', {b'cf:col4': b'value4'})
        b.put(b'row', {b'cf:col1': b'value1'})
        b.delete(b'row', [b'cf:col1'])
        b.put(b'row', {b'cf:col2': b'value2'})

    with table_tmp.batch(timestamp=1490082327736) as b:
        b.put(b'row', {b'cf:col2': b'somevalue'})
        b.delete(b'row', [b'cf:c3'])

    with assert_raises(ValueError):
        with table_tmp.batch(transaction=True) as b:
            b.put(b'fooz', {b'cf1:bar': b'baz'})
            raise ValueError

    print (table_tmp.row(b'foo', [b'cf:bar']))

    with table_tmp.batch(batch_size=5) as b:
        for i in range(10):
            b.put(('row-batch1-%03d' % i).encode('ascii'),
                  {b'cf:': str(i).encode('ascii')})

    res = list(table_tmp.scan())
    print(res)
    res = list(table_tmp.scan(row_prefix='row-batch1'))
    print(res)


def test_cells(table_name):
    table_tmp = connection.table(table_name)
    row_key = b'cell-test'
    col = b'cf1:col1'

    # table_tmp.put(row_key, {col: b'old'}, timestamp=1234)
    # table_tmp.put(row_key, {col: b'supnew'})

    with assert_raises(TypeError):
        table_tmp.cells(row_key, col, versions='invalid')

    with assert_raises(TypeError):
        table_tmp.cells(row_key, col, versions=3, timestamp='invalid')

    with assert_raises(ValueError):
        table_tmp.cells(row_key, col, versions=0)

    results = table_tmp.cells(row_key, col, versions=1)
    assert_equal(len(results), 1)
    print(results)

    results = table_tmp.cells(row_key, col)
    print(results)
    # 时间之前的版本
    results = table_tmp.cells(row_key, col, timestamp=1490091432045, include_timestamp=True)
    print(results)


def test_scan(table_name):
    table_tmp = connection.table(table_name)
    with assert_raises(ValueError):
        list(table_tmp.scan(limit=0))

    scanner = table_tmp.scan(row_start=b'row-batch1-000', row_stop=b'row-batch1-005')
    print(list(scanner))

    scanner = table_tmp.scan(row_prefix=b'row-batch1-', reverse=True)
    key, value = next(scanner)
    print(key, value)

    key, value = list(scanner)[-1]
    print(key, value)


def test_scan_filter_and_batch_size(table_name):
    table_tmp = connection.table(table_name)
    filter = b"SingleColumnValueFilter ('basicInfo', 'age', =, 'binary:16')"
    for k, v in table_tmp.scan(filter=filter):
        print(k, v)


def test_delete(table_name):
    row_key = b'May'
    table_tmp = connection.table(table_name)

    table_tmp.put(row_key, {b'moreInfo:teacher': b'Alie', b'basicInfo:sex': b'man'})
    print(table_tmp.row(row_key))

    table_tmp.delete(row_key, [b'moreInfo:teacher'])
    print(table_tmp.row(row_key))

    table_tmp.delete(row_key, timestamp=12345)
    print(table_tmp.row(row_key))

    table_tmp.delete(row_key)
    print(table_tmp.row(row_key))


def test_connection_pool():

    from thrift.Thrift import TException

    def run():
        name = threading.current_thread().name
        print("Thread %s starting" % name)

        def inner_function():
            # Nested connection requests must return the same connection
            with pool.connection() as another_connection:
                assert connection is another_connection

                # Fake an exception once in a while
                if random.random() < .25:
                    print("Introducing random failure")
                    connection.transport.close()
                    raise TException("Fake transport exception")

        for i in range(50):
            with pool.connection() as connection:
                connection.tables()

                try:
                    inner_function()
                except TException:
                    # This error should have been picked up by the
                    # connection pool, and the connection should have
                    # been replaced by a fresh one
                    pass

                connection.tables()

        print("Thread %s done" % name)

    N_THREADS = 10

    pool = ConnectionPool(size=3, **connection_kwargs)
    threads = [threading.Thread(target=run) for i in range(N_THREADS)]

    for t in threads:
        t.start()

    while threads:
        for t in threads:
            t.join(timeout=.1)

        # filter out finished threads
        threads = [t for t in threads if t.is_alive()]
        print("%d threads still alive" % len(threads))


def test_pool_exhaustion():
    pool = ConnectionPool(size=1, **connection_kwargs)

    def run():
        with assert_raises(NoConnectionsAvailable):
            with pool.connection(timeout=0.3) as connection:
                connection.tables()

    with pool.connection():
        # At this point the only connection is assigned to this thread,
        # so another thread cannot obtain a connection at this point.

        t = threading.Thread(target=run)
        t.start()
        t.join()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    test_connection()
    # test_table_listing()
    # test_create_table('table2')
    # test_invalid_table_create()
    # test_families('table2')
    # test_get_row('students', b'Tom', [b'basicInfo:age'])
    # test_enable_table('table2')
    # test_delete_table('table2')
    # test_table_regions('students')
    # test_put('students')
    # test_atomic_counters()
    # test_batch('mytable')
    # test_batch_context_managers('mytable')
    # test_cells('table2')
    # test_scan('mytable')
    # test_scan_filter_and_batch_size('students')
    # test_delete('students')
    # test_connection_pool()
    # test_pool_exhaustion()


