#se!/usr/bin/python
# -*- coding: utf-8 -*-

# thumbor imaging service
# https://github.com/globocom/thumbor/wiki

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2011 globo.com timehome@corp.globo.com


import happybase
from tornado_pyvows.context import TornadoHTTPContext as TornadoHTTPContext

from pyvows import Vows, expect
from hashlib import md5

from thumbor.app import ThumborServiceApp
from thumbor_hbase.storage import Storage
from thumbor.importer import Importer
from thumbor.context import Context, ServerParameters
from thumbor.config import Config
from fixtures.storage_fixture import IMAGE_URL, IMAGE_BYTES, get_server
import time

table='thumbor-test'
family='images'

def hbasekey(key):
    try:
        key = md5(key).hexdigest() + '-' + key
    except UnicodeEncodeError:
        key = md5(key.encode('utf-8')).hexdigest() + '-' + key.encode('utf-8')
    return key


def get_app(table):
        cfg = Config(HBASE_STORAGE_TABLE=table,
                     HBASE_STORAGE_SERVER_PORT=9090,
                     STORAGE='thumbor_hbase.storage')
        importer = Importer(cfg)
        importer.import_modules()
        server = ServerParameters(8888, 'localhost', 'thumbor.conf', None, 'info', None)
        ctx = Context(server, cfg, importer)
        application = ThumborServiceApp(ctx)

        return application

class HbaseDBContext(Vows.Context):
    def setup(self):
        self.connection = happybase.Connection()
        self.connection.delete_table(table, disable=True)
        self.connection.create_table(table, { family: {'max_versions': 1} })

class HbaseStorageVows(Vows.Context):
    @Vows.batch
    class CanStartWithoutThriftServer(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_SERVER_HOST='dummyserver',HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090,SECURITY_KEY='ACME-SEC')
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

        def does_not_raise_exception(self, topic):
            expect(topic).Not.to_be_an_error()

    @Vows.batch
    class CanStoreImage(HbaseDBContext):
        def topic(self):
            thumborId = IMAGE_URL % '1'
            
            config = Config(HBASE_STORAGE_TABLE=table, HBASE_STORAGE_SERVER_PORT=9090, SECURITY_KEY='ACME-SEC')
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
            store = storage.put(thumborId, IMAGE_BYTES)
            result = self.connection.table(table).row(hbasekey(thumborId), [family])
            return (store , result[family + ':raw'])

        def should_be_in_catalog(self, topic):
            expect(topic[0]).to_equal(IMAGE_URL % '1')
            expect(topic[1]).not_to_be_null()
            expect(topic[1]).not_to_be_an_error()
            expect(topic[1]).to_equal(IMAGE_BYTES)

    @Vows.batch
    class CanStoreUnicodeImage2(HbaseDBContext):
        def topic(self):
            thumborId = IMAGE_URL % 'àé'

            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090,SECURITY_KEY='ACME-SEC')
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
            store = storage.put(thumborId, IMAGE_BYTES)
            result = self.connection.table(table).row(hbasekey(thumborId), [family])
            return (store , result[family + ':raw'])

        def should_be_in_catalog(self, topic):
            expect(topic[0]).to_equal(IMAGE_URL % 'àé')
            expect(topic[1]).not_to_be_null()
            expect(topic[1]).not_to_be_an_error()
            expect(topic[1]).to_equal(IMAGE_BYTES)

    @Vows.batch
    class CanStoreUnicodeImage(HbaseDBContext):
        def topic(self):
            thumborId = IMAGE_URL % u'àé'.encode('utf-8')

            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090,SECURITY_KEY='ACME-SEC')
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
            store = storage.put(thumborId, IMAGE_BYTES)
            result = self.connection.table(table).row(hbasekey(thumborId), [family])
            return (store , result[family + ':raw'])

        def should_be_in_catalog(self, topic):
            expect(topic[0]).to_equal(IMAGE_URL % u'àé')
            expect(topic[1]).not_to_be_null()
            expect(topic[1]).not_to_be_an_error()
            expect(topic[1]).to_equal(IMAGE_BYTES)

    @Vows.batch
    class CanStoreAndGetUnicodeURLencodedImage(HbaseDBContext):
        def topic(self):
            thumborId = IMAGE_URL % '%C3%A0%C3%A9'

            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090,SECURITY_KEY='ACME-SEC')
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
            store = storage.put(thumborId, IMAGE_BYTES)
            result = self.connection.table(table).row(hbasekey(thumborId), [family])
            return (store , result[family + ':raw'])

        def should_be_in_catalog(self, topic):
            expect(topic[0]).to_equal(IMAGE_URL % '%C3%A0%C3%A9'.encode('utf-8'))
            expect(topic[1]).not_to_be_null()
            expect(topic[1]).not_to_be_an_error()
            expect(topic[1]).to_equal(IMAGE_BYTES)

    @Vows.batch
    class CanGetImage(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

            storage.put(IMAGE_URL % '2', IMAGE_BYTES)
            return storage.get(IMAGE_URL % '2')

        def should_not_be_null(self, topic):
            expect(topic).not_to_be_null()
            expect(topic).not_to_be_an_error()

        def should_have_proper_bytes(self, topic):
            expect(topic).to_equal(IMAGE_BYTES)

    @Vows.batch
    class CanGetImageExistance(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

            storage.put(IMAGE_URL % '8', IMAGE_BYTES)
            return storage.exists(IMAGE_URL % '8')

        def should_exists(self, topic):
            expect(topic).to_equal(True)

    @Vows.batch
    class CanGetImageInexistance(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

            return storage.exists(IMAGE_URL % '9999')

        def should_not_exists(self, topic):
            expect(topic).to_equal(False)

    @Vows.batch
    class CanRemoveImage(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

            storage.put(IMAGE_URL % '9', IMAGE_BYTES)
            created = storage.exists(IMAGE_URL % '9')
            storage.remove(IMAGE_URL % '9')
            return storage.exists(IMAGE_URL % '9') != created

        def should_be_put_and_removed(self, topic):
            expect(topic).to_equal(True)

    @Vows.batch
    class CanRemovethenPutImage(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

            storage.put(IMAGE_URL % '10', IMAGE_BYTES)
            storage.remove(IMAGE_URL % '10')
            time.sleep(1)  
            created = storage.exists(IMAGE_URL % '10')
            time.sleep(1)
            storage.put(IMAGE_URL % '10', IMAGE_BYTES)
            return storage.exists(IMAGE_URL % '10') != created

        def should_be_put_and_removed(self, topic):
            expect(topic).to_equal(True)

    @Vows.batch
    class CanReturnPath(Vows.Context):
        def topic(self):
            config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

            return storage.resolve_original_photo_path("toto")

        def should_return_the_same(self, topic):
            expect(topic).to_equal("toto")

    @Vows.batch
    class CryptoVows(Vows.Context):
        class RaisesIfInvalidConfig(Vows.Context):
            def topic(self):
                config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090, SECURITY_KEY='', STORES_CRYPTO_KEY_FOR_EACH_IMAGE=True)
                storage = Storage(Context(config=config, server=get_server('')))
                storage.put(IMAGE_URL % '3', IMAGE_BYTES)
                storage.put_crypto(IMAGE_URL % '3')

            def should_be_an_error(self, topic):
                expect(topic).to_be_an_error_like(RuntimeError)
                expect(topic).to_have_an_error_message_of("STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no SECURITY_KEY specified")

        class GettingCryptoForANewImageReturnsNone(Vows.Context):
            def topic(self):
                config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090, STORES_CRYPTO_KEY_FOR_EACH_IMAGE=True)
                storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
                return storage.get_crypto(IMAGE_URL % '9999')

            def should_be_null(self, topic):
                expect(topic).to_be_null()

        class DoesNotStoreIfConfigSaysNotTo(Vows.Context):
            def topic(self):
                config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
                storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
                storage.put(IMAGE_URL % '5', IMAGE_BYTES)
                storage.put_crypto(IMAGE_URL % '5')
                return storage.get_crypto(IMAGE_URL % '5')

            def should_be_null(self, topic):
                expect(topic).to_be_null()

        class CanStoreCrypto(Vows.Context):
            def topic(self):
                config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090, SECURITY_KEY='ACME-SEC', STORES_CRYPTO_KEY_FOR_EACH_IMAGE=True)
                storage = Storage(Context(config=config, server=get_server('ACME-SEC')))

                storage.put(IMAGE_URL % '6', IMAGE_BYTES)
                storage.put_crypto(IMAGE_URL % '6')
                return storage.get_crypto(IMAGE_URL % '6')

            def should_not_be_null(self, topic):
                expect(topic).not_to_be_null()
                expect(topic).not_to_be_an_error()

            def should_have_proper_key(self, topic):
                expect(topic).to_equal('ACME-SEC')

    @Vows.batch
    class DetectorVows(Vows.Context):
        class CanStoreDetectorData(Vows.Context):
            def topic(self):
                config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
                storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
                storage.put(IMAGE_URL % '7', IMAGE_BYTES)
                storage.put_detector_data(IMAGE_URL % '7', 'some-data')
                return storage.get_detector_data(IMAGE_URL % '7')

            def should_not_be_null(self, topic):
                expect(topic).not_to_be_null()
                expect(topic).not_to_be_an_error()

            def should_equal_some_data(self, topic):
                expect(topic).to_equal('some-data')

        class ReturnsNoneIfNoDetectorData(Vows.Context):
            def topic(self):
                config = Config(HBASE_STORAGE_TABLE=table,HBASE_STORAGE_SERVER_PORT=9090)
                storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
                return storage.get_detector_data(IMAGE_URL % '10000')

            def should_not_be_null(self, topic):
                expect(topic).to_be_null()

######################################################################
# TODO : correct this test (Async operation timed out after 5 seconds)
######################################################################
#    class UseWrongTimeStamp(TornadoHTTPContext):
#
#        def get_app(self):
#            return get_app('thumbor-test')
#
#        def topic(self):
#            config = Config(HBASE_STORAGE_TABLE=table, HBASE_STORAGE_SERVER_PORT=9090)
#            storage = Storage(Context(config=config, server=get_server('ACME-SEC')))
#            storage.put(IMAGE_URL % '8', IMAGE_BYTES)
#            response = self.get('/unsafe/' + IMAGE_URL % '8' + '?ts=123458')
#            return response.code
#
#        def should_not_be_found_with_wrong_timestamp(self, topic):
#            expect(topic).to_equal(404)
