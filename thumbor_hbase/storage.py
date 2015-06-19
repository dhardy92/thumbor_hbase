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
from tornado.concurrent import return_future

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
    @return_future
    def get_crypto(self, pathi, callback):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            callback(None)

        r = self._get(path, self.crypto_col)
        if not r:
            callback(None)
        else:
            callback(r.value)

    # get detector Json
    @return_future
    def get_detector_data(self, path, callback):
        r = self._get(path, self.detector_col)

        if r is not None:
            callback(loads(r))
        else:
            callback(None)

    # get image content
    @return_future
    def get(self, path, callback):
        r = self._get(path, self.image_col)

        if r is not None:
             callback(r.value)
        else:
             callback(None)

    # test image exists
    @return_future
    def exists(self, path, callback):
        r = self._get(path, self.image_col)

        if r is not None:
            callback(len(r) != 0)
        else:
            callback(False)

    # remove image entries
    def remove(self,key):
        ts = int(time.time())
        try:
            if (self.context.request_handler.request.arguments['ts']):
                ts=int(self.context.request_handler.request.arguments['ts'][0])
                key=re.sub(r'(\?|&)ts=\d+','',key)
        except (AttributeError, KeyError):
            ts = int(time.time())

        try:
            key = md5(key).hexdigest() + '-' + key
        except UnicodeEncodeError:
            key = md5(key.encode('utf-8')).hexdigest() + '-' + key.encode('utf-8')

        if self.storage is None:
            self._connect()
        self.storage.deleteAllRowTs(self.table, key, ts)

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
                r = self.storage.getRowWithColumnsTs(self.table, key, [self.data_fam + ':' + col], ts+1)[0].columns.values()[0]
                # due to bug HBASE-7924 timestamp is an upper limit to timerange (lower java Long.MIN_VALUE)
                # resulting in getting last value of the cell until the timestamp and preventing from geting updates
                # this is a hack to handle it
                if r.timestamp < ts:
                    r = None
            else:
                r = self.storage.get(self.table, key, self.data_fam + ':' + col)[0]
        except IndexError:
            r = None 

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
