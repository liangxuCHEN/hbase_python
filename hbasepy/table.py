"""
hbasepy table module.
"""
import logging
from numbers import Integral
from struct import Struct
from six import iteritems
from .tool import thrift_type_to_dict, bytes_increment, OrderedDict
from .batch import Batch

logger = logging.getLogger(__name__)

pack_i64 = Struct('>q').pack

def make_row(cell_map, include_timestamp):
    """Make a row dict for a cell mapping like ttypes.TRowResult.columns."""
    return {
        name: (cell.value, cell.timestamp) if include_timestamp else cell.value
        for name, cell in iteritems(cell_map)
    }


def make_ordered_row(sorted_columns, include_timestamp):
    """Make a row dict for sorted column results from scans."""
    od = OrderedDict()
    for column in sorted_columns:
        if include_timestamp:
            value = (column.cell.value, column.cell.timestamp)
        else:
            value = column.cell.value
        od[column.columnName] = value
    return od


class Table(object):
    """HBase table abstraction class.

    This class cannot be instantiated directly; use :py:meth:`Connection.table`
    instead.
    """

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def __repr__(self):
        return '<%s.%s name=%r>' % (
            __name__,
            self.__class__.__name__,
            self.name,
        )

    def families(self):
        """Retrieve the column families for this table.

        :return: Mapping from column family name to settings dict
        :rtype: dict
        """
        descriptors = self.connection.client.getColumnDescriptors(self.name)
        families = dict()
        for name, descriptor in descriptors.items():
            name = name.rstrip(b':')
            families[name] = thrift_type_to_dict(descriptor)
        return families

    def row(self, row, columns=None, timestamp=None, include_timestamp=False):
        """Retrieve a single row of data.
        :param str row: the row key
        :param list_or_tuple columns: list of columns (optional)
        :param int timestamp: timestamp (optional)
        :param bool include_timestamp: whether timestamps are returned

        :return: Mapping of columns (both qualifier and family) to values
        :rtype: dict
        """
        if columns is not None and not isinstance(columns, (tuple, list)):
            raise TypeError("'columns' must be a tuple or list")

        if timestamp is None:
            rows = self.connection.client.getRowWithColumns(
                self.name, row, columns)
        else:
            if not isinstance(timestamp, Integral):
                raise TypeError("'timestamp' must be an integer")
            rows = self.connection.client.getRowWithColumnsTs(
                self.name, row, columns, timestamp)

        if not rows:
            return {}

        return make_row(rows[0].columns, include_timestamp)

    def regions(self):
        """Retrieve the regions for this table.

        :return: regions for this table
        :rtype: list of dicts
        """
        regions = self.connection.client.getTableRegions(self.name)
        return [thrift_type_to_dict(r) for r in regions]

    def put(self, row, data, timestamp=None):
        """Store data in the table.

        This method stores the data in the `data` argument for the row
        specified by `row`. The `data` argument is dictionary that maps columns
        to values. Column names must include a family and qualifier part, e.g.
        ``b'cf:col'``, though the qualifier part may be the empty string, e.g.
        ``b'cf:'``.

        Note that, in many situations, :py:meth:`batch()` is a more appropriate
        method to manipulate data.

        :param str row: the row key
        :param dict data: the data to store
        :param int timestamp: timestamp (optional)
        :param wal bool: whether to write to the WAL (optional)
        """
        with self.batch(timestamp=timestamp) as batch:
            batch.put(row, data)

    def batch(self, timestamp=None, batch_size=None, transaction=False):
        """Create a new batch operation for this table.

        This method returns a new :py:class:`Batch` instance that can be used
        for mass data manipulation. The `timestamp` argument applies to all
        puts and deletes on the batch.

        If given, the `batch_size` argument specifies the maximum batch size
        after which the batch should send the mutations to the server. By
        default this is unbounded.

        The `transaction` argument specifies whether the returned
        :py:class:`Batch` instance should act in a transaction-like manner when
        used as context manager in a ``with`` block of code. The `transaction`
        flag cannot be used in combination with `batch_size`.

        :param bool transaction: whether this batch should behave like
                                 a transaction (only useful when used as a
                                 context manager)
        :param int batch_size: batch size (optional)
        :param int timestamp: timestamp (optional)

        :return: Batch instance
        :rtype: :py:class:`Batch`
        """
        kwargs = locals().copy()
        del kwargs['self']
        return Batch(table=self, **kwargs)

    def counter_get(self, row, column):
        """Retrieve the current value of a counter column.

        This method retrieves the current value of a counter column. If the
        counter column does not exist, this function initialises it to `0`.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param str row: the row key
        :param str column: the column name

        :return: counter value
        :rtype: int
        """
        # Don't query directly, but increment with value=0 so that the counter
        # is correctly initialised if didn't exist yet.
        return self.counter_inc(row, column, value=0)

    def counter_set(self, row, column, value=0):
        """Set a counter column to a specific value.

        This method stores a 64-bit signed integer value in the specified
        column.

        Note that application code should *never* store a incremented or
        decremented counter value directly; use the atomic
        :py:meth:`Table.counter_inc` and :py:meth:`Table.counter_dec` methods
        for that.

        :param str row: the row key
        :param str column: the column name
        :param int value: the counter value to set
        """
        self.put(row, {column: pack_i64(value)})

    def counter_inc(self, row, column, value=1):
        """Atomically increment (or decrements) a counter column.

        This method atomically increments or decrements a counter column in the
        row specified by `row`. The `value` argument specifies how much the
        counter should be incremented (for positive values) or decremented (for
        negative values). If the counter column did not exist, it is
        automatically initialised to 0 before incrementing it.

        :param str row: the row key
        :param str column: the column name
        :param int value: the amount to increment or decrement by (optional)

        :return: counter value after incrementing
        :rtype: int
        """
        return self.connection.client.atomicIncrement(
            self.name, row, column, value)

    def counter_dec(self, row, column, value=1):
        """Atomically decrement (or increments) a counter column.

        This method is a shortcut for calling :py:meth:`Table.counter_inc` with
        the value negated.

        :return: counter value after decrementing
        :rtype: int
        """
        return self.counter_inc(row, column, -value)