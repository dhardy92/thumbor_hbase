#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/globocom/thumbor/wiki

# Licensed under the MIT license: 
# http://www.opensource.org/licenses/mit-license

# HBASE STORAGE OPTIONS
from thumbor.config import Config
Config.define('HBASE_STORAGE_SERVER_HOST', 'localhost')
Config.define('HBASE_STORAGE_SERVER_PORT', 9090)
Config.define('HBASE_STORAGE_TABLE', 'thumbor')
Config.define('HBASE_STORAGE_FAMILY', 'images')

__version__ = "0.1"
