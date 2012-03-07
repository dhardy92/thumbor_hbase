#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/dhardy92/thumbor_hbase/

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license

from json import loads, dumps
from datetime import datetime, timedelta
from hashlib import md5

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import Mutation

from thumbor.storages import BaseStorage

class Storage(BaseStorage):
    crypto_col = 'crypto'
    detector_col = 'detector'
    image_col = 'raw'

    def __init__(self,context):
        self.context=context
        self.table = self.context.config.HBASE_STORAGE_TABLE
        self.data_fam = self.context.config.HBASE_STORAGE_FAMILY
        transport = TBufferedTransport(TSocket(host=self.context.config.HBASE_STORAGE_SERVER_HOST, port=self.context.config.HBASE_STORAGE_SERVER_PORT))
        transport.open()
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        self.storage = Hbase.Client(protocol)

    def put(self, path, bytes):
        path = path.encode('utf-8')
        r = [Mutation(column=self.data_fam + ':' + self.image_col, value=bytes)]
        self.storage.mutateRow(self.table, md5(path).hexdigest() + '-' + path, r)
        return path

    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        if not self.context.config.SECURITY_KEY:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        path = path.encode('utf-8')
        r = [Mutation(column=self.data_fam + ':' + self.crypto_col, value=self.context.config.SECURITY_KEY)]
        self.storage.mutateRow(self.table, md5(path).hexdigest() + '-' + path, r)

    def put_detector_data(self, path, data):
        path = path.encode('utf-8')
        r = [Mutation(column=self.data_fam + ':' + self.detector_col, value=dumps(data))]
        self.storage.mutateRow(self.table, md5(path).hexdigest() + '-' + path, r)

    def get_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return None

        path = path.encode('utf-8')
        crypto = self.storage.get(self.table, md5(path).hexdigest() + '-' + path, self.data_fam + ':' + self.crypto_col)

        if not crypto:
            return None
        return crypto[0].value

    def get_detector_data(self, path):
        path = path.encode('utf-8')
        data = self.storage.get(self.table, md5(path).hexdigest() + '-' + path, self.data_fam + ':' + self.detector_col)

        try:
            return loads(data[0].value)
        except IndexError:
            return None

    def get(self, path):
        path = path.encode('utf-8')
        r = self.storage.get(self.table, md5(path).hexdigest() + '-' + path, self.data_fam + ':' + self.image_col)
        try:
            return r[0].value
        except IndexError: 
          return None

    def exists(self, path):
        path = path.encode('utf-8')
        r = self.storage.get(self.table, md5(path).hexdigest() + '-' + path, self.data_fam + ':' + self.image_col)

        return len(r) != 0

    def remove(self,path):
        path = path.encode('utf-8')
        r = [Mutation(column=self.data_fam + ':' + self.image_col, isDelete=True)]
        self.storage.mutateRow(self.table, md5(path).hexdigest() + '-' + path, r)

    def resolve_original_photo_path(self,filename):
        return filename
