# -*- coding: utf-8 -*-

import unittest
import logging

from tornado.ioloop import IOLoop
from tornado.testing import AsyncTestCase, gen_test

from http2 import SimpleAsyncHTTP2Client

log = logging.getLogger('http2_test_client')


class TestSimpleAsyncHTTP2Client(AsyncTestCase):
    def setUp(self):
        self.io_loop = IOLoop.current()

    def tearDown(self):
        pass

    def _init_client(self, host, port, secure=True, max_streams=1000):
        return SimpleAsyncHTTP2Client(
            host=host,
            port=port,
            secure=secure,
            # enable_push=True,
            # connect_timeout=20,
            # request_timeout=20,
            max_streams=max_streams
        )

    def _callback(self, response):
        log.setLevel(logging.DEBUG)
        log.debug(response)

        self.assertIsNotNone(response)
        self.stop()

    # def get_new_ioloop(self):
    #     return IOLoop.current()

    def test_init_client(self):
        host = '127.0.0.1'
        client = self._init_client(host, port=None, secure=False)
        self.assertIsNotNone(client)

    def test_fetch(self):
        host = 'http2.akamai.com'
        port = 80
        url = '/'

        client = self._init_client(host=host, port=port, secure=False)
        client.fetch(url, method='GET', callback=self._callback, raise_error=False)
        self.wait()

    @gen_test
    def test_fetch_coroutine(self):
        host = 'www.google.com'
        port = 80
        url = '/'

        client = self._init_client(host=host, port=port, secure=False)
        res = yield client.fetch(url, method='GET', raise_error=False)

        self.assertIsNotNone(res)

    @gen_test
    def test_apns(self):
        host = 'api.development.push.apple.com'
        port = 443
        url = '/3/device/AAA123'

        client = self._init_client(host=host, port=port, secure=True)
        res = yield client.fetch(url, method='POST', body='{}', raise_error=False)

if __name__ == '__main__':
    unittest.main()
