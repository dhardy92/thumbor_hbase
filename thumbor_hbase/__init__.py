# HBASE STORAGE OPTIONS
from thumbor.config import Config
Config.define('HBASE_STORAGE_SERVER_HOST', 'localhost')
Config.define('HBASE_STORAGE_SERVER_PORT', 9090)
Config.define('HBASE_STORAGE_TABLE', 'thumbor')
Config.define('HBASE_STORAGE_FAMILY', 'images')

