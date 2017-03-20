"""
hbasepy table module.
"""
import logging
from numbers import Integral
from struct import Struct
from six import iteritems
from .tool import thrift_type_to_dict, bytes_increment, OrderedDict

logger = logging.getLogger(__name__)


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