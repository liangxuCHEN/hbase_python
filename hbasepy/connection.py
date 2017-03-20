# coding: UTF-8

"""
HappyBase connection module.
"""

import logging

from thrift import Thrift
import six
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol, TCompactProtocol

from hbase import Hbase
from hbase.ttypes import *
from .tool import *

from .table import Table

logger = logging.getLogger(__name__)

STRING_OR_BINARY = (six.binary_type, six.text_type)

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9090
DEFAULT_PROTOCOL = 'binary'


class Connection(object):
    """Connection to an HBase Thrift server.
    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, autoconnect=True, protocol=DEFAULT_PROTOCOL):

        # Allow host and port to be None, which may be easier for
        # applications wrapping a Connection instance.
        self.host = host or DEFAULT_HOST
        self.port = port or DEFAULT_PORT
        self._protocol = protocol
        self._refresh_thrift_client()
        self._transport_is_open = False

        if autoconnect:
            self.open()

        self._initialized = True

    def _refresh_thrift_client(self):
        """Refresh the Thrift socket, transport, and client."""
        socket = TSocket.TSocket(host=self.host, port=self.port)
        self.transport = TTransport.TBufferedTransport(socket)

        if self._protocol == 'binary':
            protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        else:
            protocol = TCompactProtocol.TCompactProtocol(self.transport)
        self.client = Hbase.Client(protocol)

    def open(self):
        """Open the underlying transport to the HBase instance.

        This method opens the underlying Thrift transport (TCP connection).
        """
        if self._transport_is_open:
            return

        logger.debug("Opening Thrift transport to %s:%d", self.host, self.port)
        self.transport.open()
        self._transport_is_open = True

    def close(self):
        """Close the underyling transport to the HBase instance.

        This method closes the underlying Thrift transport (TCP connection).
        """
        if not self._transport_is_open:
            return

        if logger is not None:
            # If called from __del__(), module variables may no longer
            # exist.
            logger.debug(
                "Closing Thrift transport to %s:%d",
                self.host, self.port)

        self.transport.close()
        self._transport_is_open = False

    def __del__(self):
        try:
            self._initialized
        except AttributeError:
            # Failure from constructor
            return
        else:
            self.close()

    def table(self, name):
        """
        Return a table object.
        :param str name:the name of the table
        :return:py:class:`Table`
        """
        return Table(name, self)




    def tables(self):
        """Return a list of table names available in this HBase instance.

        :return: The table names
        :rtype: List of strings
        """
        names = self.client.getTableNames()
        return names

    def create_table(self, name, families):
        """
            connection.create_table(
                'mytable',
                {'cf1': dict(max_versions=10),
                 'cf2': dict(max_versions=1, block_cache_enabled=False),
                 'cf3': dict(),  # use defaults
                 }
            )
        :param name: String
        :param families: dict
        """

        if not isinstance(families, dict):
            raise TypeError("'families' arg must be a dictionary")

        if not families:
            raise ValueError(
                "Cannot create table %r (no column families specified)" % name)

        column_descriptors = []
        for cf_name, options in six.iteritems(families):
            if options is None:
                options = dict()

            kwargs = dict()
            for option_name, value in six.iteritems(options):
                kwargs[pep8_to_camel_case(option_name)] = value

            if not cf_name.endswith(':'):
                cf_name += ':'
            kwargs['name'] = cf_name

            column_descriptors.append(ColumnDescriptor(**kwargs))

        self.client.createTable(name, column_descriptors)