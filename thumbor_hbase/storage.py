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
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import Mutation

from thumbor.storages import BaseStorage

class Storage(BaseStorage):
    crypto_col = 'crypto'
    detector_col = 'detector'
    image_col = 'raw'
    storage = None

    def __init__(self,context):
        self.context=context
        self.table = self.context.config.HBASE_STORAGE_TABLE
        self.data_fam = self.context.config.HBASE_STORAGE_FAMILY
        try:
            self.connect()
        except TTransportException:
            None

    def connect(self):
        transport = TBufferedTransport(TSocket(host=self.context.config.HBASE_STORAGE_SERVER_HOST, port=self.context.config.HBASE_STORAGE_SERVER_PORT))
        transport.open()
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.storage = Hbase.Client(protocol)

    def put(self, path, bytes):
        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')
        
        r = [Mutation(column=self.data_fam + ':' + self.image_col, value=bytes)]
        if self.storage is None:
            self.connect()
        self.storage.mutateRow(self.table, key, r)

        return path

    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        if not self.context.config.SECURITY_KEY:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        r = [Mutation(column=self.data_fam + ':' + self.crypto_col, value=self.context.config.SECURITY_KEY)]

        if self.storage is None:
            self.connect()
        self.storage.mutateRow(self.table, key, r)
 
    def put_detector_data(self, path, data):
        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        r = [Mutation(column=self.data_fam + ':' + self.detector_col, value=dumps(data))]
        if self.storage is None:
            self.connect()
        self.storage.mutateRow(self.table, key, r)

    def get_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return None

        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        if self.storage is None:
            self.connect()
        crypto = self.storage.get(self.table, key, self.data_fam + ':' + self.crypto_col)

        if not crypto:
            return None
        return crypto[0].value

    def get_detector_data(self, path):
        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        if self.storage is None:
            self.connect()
        data = self.storage.get(self.table, key, self.data_fam + ':' + self.detector_col)

        try:
            return loads(data[0].value)
        except IndexError:
            return None

    def get(self, path):
        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        if self.storage is None:
            self.connect()
        r = self.storage.get(self.table, key, self.data_fam + ':' + self.image_col)

        try:
            return r[0].value
        except IndexError: 
          return None

    def exists(self, path):
        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        if self.storage is None:
            self.connect()
        r = self.storage.get(self.table, key, self.data_fam + ':' + self.image_col)

        return len(r) != 0

    def remove(self,path):
        try:
            key = md5(path).hexdigest() + '-' + path
        except UnicodeEncodeError:
            key = md5(path.encode('utf-8')).hexdigest() + '-' + path.encode('utf-8')

        r = [Mutation(column=self.data_fam + ':' + self.image_col, isDelete=True)]

        if self.storage is None:
            self.connect()
        self.storage.mutateRow(self.table, key, r)

    def resolve_original_photo_path(self,filename):
        return filename
