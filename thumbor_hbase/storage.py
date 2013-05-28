#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/dhardy92/thumbor_hbase/

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license

from json import loads, dumps
from hashlib import md5
import time
import re

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
            self._connect()
        except TTransportException:
            None

    # put image content
    def put(self, path, bytes):
        self._put(path, self.image_col, bytes )

        return path

    # put crypto key for signature
    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        if not self.context.config.SECURITY_KEY:
            raise RuntimeError("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        self._put(path, self.crypto_col,self.context.config.SECURITY_KEY)

    # put detector Json
    def put_detector_data(self, path, data):
        self._put(path, self.detector_col, dumps(data))

    # get signature key
    def get_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return None

        crypto = self._get(path, self.crypto_col)

        if not crypto:
            return None
        return crypto[0].value

    # get detector Json
    def get_detector_data(self, path):
        data = self._get(path, self.detector_col)

        try:
            return loads(data[0].value)
        except IndexError:
            return None

    # get image content
    def get(self, path):
        r = self._get(path, self.image_col)

        try:
            return r[0].value
        except IndexError: 
          return None

    # test image exists
    def exists(self, path):
        r = self._get(path, self.image_col)

        return len(r) != 0

    # remove image entries
    def remove(self,key):
        ts = None
        try:
            if (self.context.request_handler.request.arguments['ts']):
                ts=int(self.context.request_handler.request.arguments['ts'][0])
                key=re.sub(r'(\?|&)ts=\d+','',key)
        except (AttributeError, KeyError):
            None

        try:
            key = md5(key).hexdigest() + '-' + key
        except UnicodeEncodeError:
            key = md5(key.encode('utf-8')).hexdigest() + '-' + key.encode('utf-8')

        if self.storage is None:
            self._connect()
        self.storage.deleteAllRow(self.table, key)

    def resolve_original_photo_path(self,filename):
        return filename

    # GET a Cell value in HBase
    def _get(self,key,col):

        ts = None
        try:
            if (self.context.request_handler.request.arguments['ts']):
                ts=int(self.context.request_handler.request.arguments['ts'][0])
                key=re.sub(r'(\?|&)ts=\d+','',key)
        except (AttributeError, KeyError):
            None

        try:
            key = md5(key).hexdigest() + '-' + key
        except UnicodeEncodeError:
            key = md5(key.encode('utf-8')).hexdigest() + '-' + key.encode('utf-8')

        if self.storage is None:
            self._connect()

        #get specific version if ?ts= parameter is used
        try:
            if ts != None:
                r = self.storage.getRowWithColumnsTs(self.table, key, [self.data_fam + ':' + col], ts)[0]
            else:
                r = self.storage.get(self.table, key, self.data_fam + ':' + col)
        except IndexError:
	    r = [] 

        return r

    # PUT value in a Cell of HBase
    def _put(self, key, col, value):
        ts = None

        try:
            if (self.context.request_handler.request.arguments['ts']):
                ts=int(self.context.request_handler.request.arguments['ts'][0])
                key=re.sub(r'(\?|&)ts=\d+','',key)
            else:
                ts=int(time.time())
        except (AttributeError, KeyError):
            ts=int(time.time())

        try:
            key = md5(key).hexdigest() + '-' + key
        except UnicodeEncodeError:
            key = md5(key.encode('utf-8')).hexdigest() + '-' + key.encode('utf-8')

        r = [Mutation(column=self.data_fam + ':' + col, value=value)]

        if self.storage is None:
            self._connect()

        self.storage.mutateRowTs(self.table, key, r, ts)

    def _connect(self):
        transport = TBufferedTransport(TSocket(host=self.context.config.HBASE_STORAGE_SERVER_HOST, port=self.context.config.HBASE_STORAGE_SERVER_PORT))
        transport.open()
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.storage = Hbase.Client(protocol)
