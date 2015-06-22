#!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/globocom/thumbor/wiki

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2011 globo.com timehome@corp.globo.com

from thumbor_hbase.storage import Storage
from thumbor.context import Context
from thumbor.config import Config

import pprint

def load(context, path, callback):
    storage = Storage(context)
    callback(storage.get(path))
