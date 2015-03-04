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

from thrift.transport.TTransport import TTransportException
import happybase

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
          self.pool = happybase.ConnectionPool(size=200, host=self.context.config.HBASE_STORAGE_SERVER_HOST, port=self.context.config.HBASE_STORAGE_SERVER_PORT)
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

        r = self._get(path, self.crypto_col)
        return r

    # get detector Json
    def get_detector_data(self, path):
        r = self._get(path, self.detector_col)

        if r is not None:
            return loads(r)
        else:
            return None

    # get image content
    def get(self, path):
        r = self._get(path, self.image_col)
        return r

    # test image exists
    def exists(self, path):
        r = self._get(path, self.image_col)
        if r is not None:
            return len(r) != 0
        else:
            return False

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

        with self.pool.connection() as connection:
            table = connection.table(self.table)
            table.delete(key,timestamp=ts)

    def resolve_original_photo_path(self,filename):
        return filename

    # GET a Cell value in HBase
    def _get(self,key,col):

        ts = None
        try:
            if (self.context.request_handler.request.arguments['ts']):
                ts=int(self.context.request_handler.request.arguments['ts'][0])
                key=re.sub(r'(\?|&)ts=\d+','',key)
                # due to bug HBASE-7924 timestamp is an upper limit to timerange (lower java Long.MIN_VALUE)
                # resulting in getting last value of the cell until the timestamp and preventing from geting updates
                # this is a hack to handle it
                ts += 1
        except (AttributeError, KeyError):
            None

        try:
            key = md5(key).hexdigest() + '-' + key
        except UnicodeEncodeError:
            key = md5(key.encode('utf-8')).hexdigest() + '-' + key.encode('utf-8')

        with self.pool.connection() as connection:
            table = connection.table(self.table)
            r = table.row(key,[self.data_fam + ':' + col], timestamp=ts)

        #get specific version if ?ts= parameter is used
        if ts is not None and r.timestamp < ts:
            return None

        try:
            if r is not None:
              return r['%s:%s' % (self.context.config.HBASE_STORAGE_FAMILY, col)]
        except KeyError:
            None

        return None

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

        with self.pool.connection() as connection:
            table = connection.table(self.table)
            r = table.put(key, {self.data_fam + ':' + col: value}, timestamp=ts)
