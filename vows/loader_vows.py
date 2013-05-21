#se!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/globocom/thumbor/wiki

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2011 globo.com timehome@corp.globo.com

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase, ttypes


from pyvows import Vows, expect
from thumbor_hbase.storage import Storage
import thumbor_hbase.loader as loader
from thumbor.context import Context
from thumbor.config import Config
from fixtures.storage_fixture import IMAGE_URL, IMAGE_BYTES, get_server

class HbaseDBContext(Vows.Context):
    def setup(self):
        transport = TBufferedTransport(TSocket(host='localhost', port=9090))
        transport.open()
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.connection = Hbase.Client(protocol)
        self.table='thumbor-test'
        self.family='images:'

        columns = []
        col = ttypes.ColumnDescriptor()
        col.name = self.family
        col.maxVersions = 1
        columns.append(col)
        try:
            self.connection.disableTable(self.table)
            self.connection.deleteTable(self.table)
        except ttypes.IOError:
            pass
        self.connection.createTable(self.table, columns)


@Vows.batch
class HbaseLoaderVows(HbaseDBContext):
    class CanLoadImage(Vows.Context):
        @Vows.async_topic
        def topic(self,callback ):
            config = Config(HBASE_STORAGE_TABLE=self.parent.table,HBASE_STORAGE_SERVER_PORT=9090)
            context = Context(config=config, server=get_server('ACME-SEC'))
            storage = Storage(context)

            storage.put(IMAGE_URL % '1', IMAGE_BYTES)
            return loader.load(context, IMAGE_URL % '1', callback)

        def should_not_be_null(self, topic):
            expect(topic).not_to_be_null()
            expect(topic).not_to_be_an_error()

        def should_have_proper_bytes(self, topic):
            expect(topic[0]).to_equal(IMAGE_BYTES)

