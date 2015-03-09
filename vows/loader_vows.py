#se!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/globocom/thumbor/wiki

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2011 globo.com timehome@corp.globo.com

import happybase

from pyvows import Vows, expect
from thumbor_hbase.storage import Storage
import thumbor_hbase.loader as loader
from thumbor.context import Context
from thumbor.config import Config
from fixtures.storage_fixture import IMAGE_URL, IMAGE_BYTES, get_server

table = 'thumbor-test'
family = 'images:'


class HbaseDBContext(Vows.Context):

    def setup(self):

        self.pool = happybase.ConnectionPool(size=10)
        with self.pool.connection() as connection:
            try:
                connection.delete_table(table, disable=True)
            except happybase.hbase.ttypes.IOError:
                None

            connection.create_table(table, { family: {'max_versions': 1} })


@Vows.batch
class HbaseLoaderVows(HbaseDBContext):
    class CanLoadImage(Vows.Context):
        @Vows.async_topic
        def topic(self, callback):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            context = Context(config=config, server=get_server('ACME-SEC'))
            storage = Storage(context)

            storage.put(IMAGE_URL % '1', IMAGE_BYTES)
            loader.load(context, IMAGE_URL % '1', callback)

        def should_not_be_null(self, topic):
            expect(topic.args[0]).not_to_be_null()
            expect(topic.args[0]).not_to_be_an_error()

        def should_have_proper_bytes(self, topic):
            expect(topic.args[0]).to_equal(IMAGE_BYTES)

