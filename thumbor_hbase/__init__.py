#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/globocom/thumbor/wiki

# Licensed under the MIT license: 
# http://www.opensource.org/licenses/mit-license

# HBASE STORAGE OPTIONS
from thumbor.config import Config
Config.define('HBASE_STORAGE_SERVER_HOST', 'localhost','Thrift Hbase interface Host for Storage', 'HBase Storage')
Config.define('HBASE_STORAGE_SERVER_PORT', 9090, 'Thrift Hbase interface Port for Storage', 'HBase Storage')
Config.define('HBASE_STORAGE_TABLE', 'thumbor', 'Thrift Hbase Table for Storage', 'HBase Storage')
Config.define('HBASE_STORAGE_FAMILY', 'images', 'Thrift Hbase column family for Storage', 'HBase Storage')

__version__ = "0.9"
