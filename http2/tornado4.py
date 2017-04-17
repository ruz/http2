# -*- coding: utf-8 -*-

import io
import ssl
import sys
import copy
import base64
import socket
import httplib
import urlparse
import functools
import contextlib
import collections

from h2.errors import ErrorCodes
import h2.events
from h2.settings import SettingCodes
import h2.connection
import h2.exceptions

from hpack.struct import HeaderTuple, NeverIndexedHeaderTuple

from tornado import (
    httputil, log, stack_context,
    simple_httpclient, netutil
)
from tornado.escape import _unicode, utf8
from tornado.util import GzipDecompressor
from tornado.httpclient import (
    HTTPResponse, HTTPError, HTTPRequest, _RequestProxy
)

logger = log.gen_log
__all__ = [
    'HTTP2Response', 'HTTP2Error', 'HTTP2ConnectionTimeout',
    'HTTP2ConnectionClosed', 'SimpleAsyncHTTP2Client',
]


class HTTP2Response(HTTPResponse):
    def __init__(self, *args, **kwargs):
        self.pushed_responses = kwargs.pop('pushed_responses', [])
        self.new_request = kwargs.pop('new_request', None)

        super(HTTP2Response, self).__init__(*args, **kwargs)
        reason = kwargs.pop('reason', None)
        self.reason = reason or httputil.responses.get(self.code, "Unknown")


class HTTP2Error(HTTPError):
    pass


class HTTP2ConnectionTimeout(HTTP2Error):
    def __init__(self, time_cost=None):
        super(HTTP2ConnectionTimeout, self).__init__(599)
        self.time_cost = time_cost


class HTTP2ConnectionClosed(HTTP2Error):
    def __init__(self, reason=None):
        super(HTTP2ConnectionClosed, self).__init__(599)
        self.reason = reason


class _RequestTimeout(Exception):
    pass


class SimpleAsyncHTTP2Client(simple_httpclient.SimpleAsyncHTTPClient):
    MAX_CONNECTION_BACKOFF = 10
    CONNECTION_BACKOFF_STEP = 1
    CLIENT_REGISTRY = {}

    def __new__(cls, *args, **kwargs):
        force_instance = kwargs.pop('force_instance', False)
        host = kwargs['host']
        client_key = kwargs.get('http_client_key')

        if not client_key:
            client_key = host

        if force_instance or client_key not in cls.CLIENT_REGISTRY:
            client = simple_httpclient.SimpleAsyncHTTPClient.__new__(cls, *args, force_instance=True, **kwargs)
            cls.CLIENT_REGISTRY.setdefault(client_key, client)
        else:
            client = cls.CLIENT_REGISTRY[client_key]

        return client

    def initialize(self, io_loop, host, port=None, max_streams=200,
                   hostname_mapping=None, max_buffer_size=104857600,
                   resolver=None, defaults=None, secure=True,
                   cert_options=None, enable_push=False, connect_timeout=20,
                   request_timeout=20, initial_window_size=65535, **conn_kwargs):
        # initially, we disables stream multiplexing and wait the settings frame
        super(SimpleAsyncHTTP2Client, self).initialize(
            io_loop=io_loop, max_clients=1,
            hostname_mapping=hostname_mapping, max_buffer_size=max_buffer_size,
            resolver=resolver, defaults=defaults, max_header_size=None,
        )
        self.host = host
        if port is None:
            port = 443 if secure else 80
        self.port = port
        self.secure = secure
        self.max_streams = max_streams
        self.enable_push = bool(enable_push)
        self.initial_window_size = initial_window_size

        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.connection_factory = _HTTP2ConnectionFactory(
            io_loop=self.io_loop, host=host, port=port,
            max_buffer_size=self.max_buffer_size, secure=secure,
            cert_options=cert_options, connect_timeout=self.connect_timeout,
            tcp_client=self.tcp_client,
        )

        # open connection
        self.connection = None
        self.io_stream = None

        # back-off
        self.next_connect_time = 0
        self.connection_backoff = self.CONNECTION_BACKOFF_STEP

        self.connection_factory.make_connection(
            self._on_connection_ready, self._on_connection_close)

    def _adjust_settings(self, event):
        logger.debug('settings updated: %r', event.changed_settings)
        settings = event.changed_settings.get(SettingCodes.MAX_CONCURRENT_STREAMS)
        if settings:
            self.max_clients = min(settings.new_value, self.max_streams)
            if settings.new_value > settings.original_value:
                self._process_queue()

    def _on_connection_close(self, io_stream, reason):
        if self.io_stream is not io_stream:
            return

        connection = self.connection
        self.io_stream = None
        self.connection = None

        if connection is not None:
            connection.on_connection_close(io_stream.error)

        # schedule back-off
        now_time = self.io_loop.time()
        self.next_connect_time = max(
            self.next_connect_time,
            now_time + self.connection_backoff)

        self.connection_backoff = min(
            self.connection_backoff + self.CONNECTION_BACKOFF_STEP,
            self.MAX_CONNECTION_BACKOFF)

        if io_stream is None:
            logger.info(
                'Connection to %s:%u failed due: %r. Reconnect in %.2f seconds',
                self.host, self.port, reason, self.next_connect_time - now_time)
        else:
            logger.info(
                'Connection to %s:%u closed due: %r. Reconnect in %.2f seconds',
                self.host, self.port, reason, self.next_connect_time - now_time)

        self.io_loop.add_timeout(
            self.next_connect_time, functools.partial(
                self.connection_factory.make_connection,
                self._on_connection_ready, self._on_connection_close
            ))

        # move active request to pending
        for key, (request, callback) in self.active.items():
            req = _HTTP2Stream.prepare_request(request, self.host)
            self.queue.appendleft((key, req, callback))
            self.waiting[key] = (None, None, None)  # @NOTICE: in this case we need only key in waiting

        self.active.clear()

    def _connection_terminated(self, event):
        self._on_connection_close(
            self.io_stream, 'Server requested, code: 0x%x' % event.error_code)

    def _on_connection_ready(self, io_stream):
        # reset back-off, prevent reconnect within back-off period
        self.next_connect_time += self.connection_backoff
        self.connection_backoff = 0

        self.io_stream = io_stream
        self.connection = _HTTP2ConnectionContext(
            io_stream=io_stream, secure=self.secure,
            enable_push=self.enable_push,
            max_buffer_size=self.max_buffer_size,
            initial_window_size=self.initial_window_size,
        )
        self.connection.add_event_handler(
            h2.events.RemoteSettingsChanged, self._adjust_settings
        )
        self.connection.add_event_handler(
            h2.events.ConnectionTerminated, self._connection_terminated
        )
        self._process_queue()

    def fetch_impl(self, request, callback):
        request = _HTTP2Stream.prepare_request(request, self.host)
        super(SimpleAsyncHTTP2Client, self).fetch_impl(request, callback)

    def _process_queue(self):
        if not self.connection:
            return

        super(SimpleAsyncHTTP2Client, self)._process_queue()

    def _handle_request(self, request, release_callback, final_callback):
        with self.connection.handle_exception():
            stream_id = self.connection.h2_conn.get_next_available_stream_id()
            stream = _HTTP2Stream(
                io_loop=self.io_loop, context=self.connection,
                request=request, stream_id=stream_id,
                release_callback=release_callback,
                final_callback=final_callback,
            )
            stream.send_request(request)


class _HTTP2ConnectionFactory(object):
    def __init__(self, io_loop, host, port, max_buffer_size, tcp_client,
                 secure=True, cert_options=None, connect_timeout=None):
        self.io_loop = io_loop
        self.max_buffer_size = max_buffer_size
        self.tcp_client = tcp_client
        self.cert_options = collections.defaultdict(lambda: None, **cert_options or {})
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.ssl_options = self._get_ssl_options(self.cert_options) if secure else None

    def make_connection(self, ready_callback, close_callback):
        if self.connect_timeout:
            timed_out = [False]
            start_time = self.io_loop.time()

            def _on_timeout():
                timed_out[0] = True
                close_callback(
                    io_stream=None,
                    reason=HTTP2ConnectionTimeout(self.io_loop.time() - start_time)
                )

            def _on_connect(io_stream):
                if timed_out[0]:
                    io_stream.close()
                    return
                self.io_loop.remove_timeout(timeout_handle)
                self._on_connect(io_stream, ready_callback, close_callback)

            timeout_handle = self.io_loop.add_timeout(
                start_time + self.connect_timeout, _on_timeout)

        else:
            _on_connect = functools.partial(
                self._on_connect,
                ready_callback=ready_callback,
                close_callback=close_callback,
            )

        logger.info('Establishing HTTP/2 connection to %s:%s...', self.host, self.port)
        with stack_context.ExceptionStackContext(
                functools.partial(self._handle_exception, close_callback)):
            self.tcp_client.connect(
                self.host, self.port, af=socket.AF_UNSPEC,
                ssl_options=self.ssl_options,
                max_buffer_size=self.max_buffer_size,
                callback=_on_connect)

    @classmethod
    def _handle_exception(cls, close_callback, typ, value, tb):
        close_callback(io_stream=None, reason=value)
        return True

    @classmethod
    def _get_ssl_options(cls, cert_options):
        ssl_options = {}
        if cert_options['validate_cert']:
            ssl_options["cert_reqs"] = ssl.CERT_REQUIRED
        if cert_options['ca_certs'] is not None:
            ssl_options["ca_certs"] = cert_options['ca_certs']
        else:
            ssl_options["ca_certs"] = simple_httpclient._default_ca_certs()
        if cert_options['client_key'] is not None:
            ssl_options["keyfile"] = cert_options['client_key']
        if cert_options['client_cert'] is not None:
            ssl_options["certfile"] = cert_options['client_cert']

        # according to REC 7540:
        # deployments of HTTP/2 that use TLS 1.2 MUST
        # support TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
        ssl_options["ciphers"] = "ECDH+AESGCM"
        ssl_options["ssl_version"] = ssl.PROTOCOL_TLSv1_2
        ssl_options = netutil.ssl_options_to_context(ssl_options)
        ssl_options.set_alpn_protocols(['h2'])
        return ssl_options

    def _on_connect(self, io_stream, ready_callback, close_callback):
        io_stream.set_close_callback(lambda: close_callback(io_stream, io_stream.error))
        self.io_loop.add_callback(functools.partial(ready_callback, io_stream))
        io_stream.set_nodelay(True)


class _HTTP2ConnectionContext(object):
    """maintenance a http/2 connection state on specific io_stream
    """
    def __init__(self, io_stream, secure, enable_push,
                 max_buffer_size, initial_window_size):
        self.io_stream = io_stream
        self.schema = 'https' if secure else 'http'
        self.enable_push = enable_push
        self.initial_window_size = initial_window_size
        self.max_buffer_size = max_buffer_size
        self.is_closed = False

        # h2 contexts
        self.stream_delegates = {}
        self.event_handlers = {}  # connection level event, event -> handler
        self.reset_stream_ids = collections.deque(maxlen=50)
        self.h2_conn = h2.connection.H2Connection()
        self.h2_conn.initiate_connection()
        self.h2_conn.update_settings({
            SettingCodes.ENABLE_PUSH: int(self.enable_push),
            SettingCodes.INITIAL_WINDOW_SIZE: self.initial_window_size,
        })
        self.flow_control = collections.deque([])

        self.add_event_handler(h2.events.WindowUpdated, self.window_updated)

        self._setup_reading()
        self._flush_to_stream()

    def on_connection_close(self, reason):
        if self.is_closed:
            return

        self.is_closed = True
        for delegate in self.stream_delegates.values():
            delegate.on_connection_close(reason)

    @contextlib.contextmanager
    def handle_exception(self):
        try:
            yield
        except Exception as err:
            exc_info = sys.exc_info()
            logger.error('Unexpected exception: %r', err, exc_info=exc_info)
            try:
                self.io_stream.close(exc_info)
            finally:
                self.on_connection_close(err)

    # h2 related
    def _on_connection_streaming(self, data):
        """handles streaming data"""
        if self.is_closed:
            return

        with self.handle_exception():
            events = self.h2_conn.receive_data(data)
            if events:
                self._process_events(events)
                self._flush_to_stream()

    def _flush_to_stream(self):
        """flush h2 connection data to IOStream"""
        data_to_send = self.h2_conn.data_to_send()
        if data_to_send:
            self.io_stream.write(data_to_send)

    def window_updated(self, event):
        logger.debug('window updated on connection')

        while len(self.flow_control) > 0:
            stream = self.flow_control.popleft()
            stream.send_body(append=False)

            if self.h2_conn.outbound_flow_control_window <= 0:
                break

    def set_stream_delegate(self, stream_id, stream_delegate):
        self.stream_delegates[stream_id] = stream_delegate

    def remove_stream_delegate(self, stream_id):
        del self.stream_delegates[stream_id]

    def add_event_handler(self, event_type, event_handler):
        self.event_handlers[event_type] = event_handler

    def remove_event_handler(self, event_type):
        del self.event_handlers[event_type]

    def reset_stream(self, stream_id, reason=ErrorCodes.REFUSED_STREAM, flush=False):
        if self.is_closed:
            return

        try:
            self.h2_conn.reset_stream(stream_id, reason)
        except h2.exceptions.StreamClosedError:
            return
        else:
            if flush:
                self._flush_to_stream()

    def _process_events(self, events):
        stream_inbounds = collections.defaultdict(int)

        for event in events:
            if isinstance(event, h2.events.DataReceived):
                stream_inbounds[event.stream_id] += event.flow_controlled_length

            if isinstance(event, h2.events.PushedStreamReceived):
                stream_id = event.parent_stream_id
            else:
                stream_id = getattr(event, 'stream_id', None)

            if stream_id is not None and stream_id != 0:
                if stream_id in self.stream_delegates:
                    stream_delegate = self.stream_delegates[stream_id]

                    with stack_context.ExceptionStackContext(stream_delegate.handle_exception):
                        stream_delegate.handle_event(event)
                else:
                    # FIXME: our nginx server will simply reset stream,
                    # without increase the window size which consumed by
                    # queued data frame which was belongs to the stream we're resetting
                    # self.reset_stream(stream_id)
                    if stream_id in self.reset_stream_ids:
                        if isinstance(event, h2.events.StreamEnded):
                            self.reset_stream_ids.remove(stream_id)
                    else:
                        logger.warning('Unexpected stream: %s, event: %r', stream_id, event)

                continue

            event_type = type(event)
            if event_type in self.event_handlers:
                try:
                    self.event_handlers[event_type](event)
                except Exception as err:
                    logger.exception('Exception while handling event: %r', err)

                continue

            logger.debug('ignored event: %r, %r', event, event.__dict__)

        for stream_id, stream_inbound in stream_inbounds.items():
            self.h2_conn.acknowledge_received_data(stream_inbound, stream_id)

    def _setup_reading(self, *_):
        if self.is_closed:
            return

        with stack_context.NullContext():
            self.io_stream.read_bytes(
                num_bytes=65535, callback=self._setup_reading,
                streaming_callback=self._on_connection_streaming)


class _HTTP2Stream(httputil.HTTPMessageDelegate):
    _SUPPORTED_METHODS = set(["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])

    def __init__(
            self, io_loop, context, request, stream_id,
            release_callback=None, final_callback=None):
        self.io_loop = io_loop
        self.start_time = self.io_loop.time()
        self.context = context
        self.release_callback = release_callback
        self.final_callback = final_callback

        self.chunks = []
        self.headers = None
        self.code = None
        self.reason = None

        self._timeout = None
        self._pushed_streams = {}
        self._pushed_responses = {}
        self._stream_ended = False
        self._finalized = False
        self._decompressor = None
        self._pending_body = None

        self.stream_id = stream_id
        self.request = request
        self.context.set_stream_delegate(self.stream_id, self)

        if request.request_timeout:
            with stack_context.ExceptionStackContext(self.handle_exception):
                self._timeout = self.io_loop.add_timeout(
                    self.start_time + request.request_timeout, self._on_timeout)

    @classmethod
    def build_http_headers(cls, headers):
        http_headers = httputil.HTTPHeaders()
        for name, value in headers:
            http_headers.add(name, value)

        return http_headers

    def send_request(self, request):
        http2_headers = []
        http2_headers.append(HeaderTuple(':authority', request.headers.pop('Host')))
        http2_headers.append(NeverIndexedHeaderTuple(':path', request.url))
        http2_headers.append(HeaderTuple(':scheme', self.context.schema))
        http2_headers.append(HeaderTuple(':method', request.method))

        sensitive_headers = ('apns-topic', 'authorization')

        for key, value in request.headers.iteritems():
            if key.lower() in sensitive_headers:
                new = NeverIndexedHeaderTuple(key, value)
            else:
                new = HeaderTuple(key, value)
            http2_headers.append(new)

        self.context.h2_conn.send_headers(self.stream_id, http2_headers, end_stream=not request.body)
        self.context._flush_to_stream()

        if request.body:
            self._pending_body = request.body
            self.send_body()

    def send_body(self, append=True):
        if self._pending_body is None:
            return
        h2_conn = self.context.h2_conn
        window_size = h2_conn.local_flow_control_window(self.stream_id)
        frame_size = h2_conn.max_outbound_frame_size
        bytes_to_send = min(window_size, len(self._pending_body))

        end = False
        while bytes_to_send > 0:
            chunk_size = min(bytes_to_send, frame_size)
            chunk_data = self._pending_body[:chunk_size]
            end = chunk_size == len(self._pending_body)
            h2_conn.send_data(self.stream_id, chunk_data, end_stream=end)
            bytes_to_send -= chunk_size
            self._pending_body = self._pending_body[chunk_size:]

        if not end:
            if h2_conn.outbound_flow_control_window <= 0:
                if append:
                    self.context.flow_control.append(self)
                else:
                    self.context.flow_control.appendleft(self)
        else:
            self._pending_body = None

        self.context._flush_to_stream()

    def from_push_stream(self, event):
        headers = self.build_http_headers(event.headers)

        method = headers.pop(':method')
        scheme = headers.pop(':scheme')
        authority = headers.pop(':authority')
        path = headers.pop(':path')

        full_url = '%s://%s%s' % (scheme, authority, path)
        request = HTTPRequest(url=full_url, method=method, headers=headers)
        return _HTTP2Stream(
            io_loop=self.io_loop, context=self.context,
            request=request, stream_id=event.pushed_stream_id,
            final_callback=functools.partial(
                self.finish_push_stream, event.pushed_stream_id)
        )

    def finish_push_stream(self, stream_id, response):
        if self._finalized:
            return

        self._pushed_responses[stream_id] = response
        if not self._stream_ended:
            return

        if len(self._pushed_streams) == len(self._pushed_responses):
            self.finish()

    @classmethod
    def prepare_request(cls, request, default_host):
        parsed = urlparse.urlsplit(_unicode(request.url))
        if (request.method not in cls._SUPPORTED_METHODS and
                not request.allow_nonstandard_methods):
            raise KeyError("unknown method %s" % request.method)
        request.follow_redirects = False
        for key in ('network_interface',
                    'proxy_host', 'proxy_port',
                    'proxy_username', 'proxy_password',
                    'expect_100_continue', 'body_producer',
                    ):
            if getattr(request, key, None):
                raise NotImplementedError('%s not supported' % key)

        request.headers.pop('Connection', None)
        if "Host" not in request.headers:
            if not parsed.netloc:
                request.headers['Host'] = default_host
            elif '@' in parsed.netloc:
                request.headers["Host"] = parsed.netloc.rpartition('@')[-1]
            else:
                request.headers["Host"] = parsed.netloc
        username, password = None, None
        if parsed.username is not None:
            username, password = parsed.username, parsed.password
        elif request.auth_username is not None:
            username = request.auth_username
            password = request.auth_password or ''
        if username is not None:
            if request.auth_mode not in (None, "basic"):
                raise ValueError("unsupported auth_mode %s",
                                 request.auth_mode)
            auth = utf8(username) + b":" + utf8(password)
            request.headers["Authorization"] = (
                b"Basic " + base64.b64encode(auth))
        if request.user_agent:
            request.headers["User-Agent"] = request.user_agent
        if not request.allow_nonstandard_methods:
            # Some HTTP methods nearly always have bodies while others
            # almost never do. Fail in this case unless the user has
            # opted out of sanity checks with allow_nonstandard_methods.
            body_expected = request.method in ("POST", "PATCH", "PUT")
            body_present = (request.body is not None or
                            request.body_producer is not None)
            if ((body_expected and not body_present) or
                (body_present and not body_expected)):
                raise ValueError(
                    'Body must %sbe None for method %s (unless '
                    'allow_nonstandard_methods is true)' %
                    ('not ' if body_expected else '', request.method))
        if request.body is not None:
            # When body_producer is used the caller is responsible for
            # setting Content-Length (or else chunked encoding will be used).
            request.headers["Content-Length"] = str(len(
                request.body))
        if (request.method == "POST" and
                "Content-Type" not in request.headers):
            request.headers["Content-Type"] = "application/x-www-form-urlencoded"
        if request.decompress_response:
            request.headers["Accept-Encoding"] = "gzip"

        request.url = (
            (parsed.path or '/') +
            (('?' + parsed.query) if parsed.query else '')
        )
        return request

    def headers_received(self, first_line, headers):
        if self.request.decompress_response \
                and headers.get("Content-Encoding") == "gzip":
            self._decompressor = GzipDecompressor()

            # Downstream delegates will only see uncompressed data,
            # so rename the content-encoding header.
            headers.add("X-Consumed-Content-Encoding",
                        headers["Content-Encoding"])
            del headers["Content-Encoding"]

        self.headers = headers
        self.code = first_line.code
        self.reason = first_line.reason

        if self.request.header_callback is not None:
            # Reassemble the start line.
            self.request.header_callback('%s %s %s\r\n' % first_line)
            for k, v in self.headers.get_all():
                self.request.header_callback("%s: %s\r\n" % (k, v))
            self.request.header_callback('\r\n')

    def _run_callback(self, response):
        if self._finalized:
            return

        with stack_context.NullContext():
            self.io_loop.add_callback(functools.partial(
                self.final_callback, response
            ))

            if self.release_callback is not None:
                self.io_loop.add_callback(functools.partial(
                    self.release_callback
                ))

        self._finalized = True

    def handle_event(self, event):
        if isinstance(event, h2.events.ResponseReceived):
            headers = self.build_http_headers(event.headers)
            status_code = int(headers.pop(':status'))
            start_line = httputil.ResponseStartLine(
                'HTTP/2.0', status_code, httplib.responses[status_code]
            )
            self.headers_received(start_line, headers)
        elif isinstance(event, h2.events.DataReceived):
            self.data_received(event.data)
        elif isinstance(event, h2.events.StreamEnded):
            self._stream_ended = True
            if self._pending_body is not None:
                # we still have data to send, server responded earlier
                self._pending_body = None
                self.context.h2_conn.end_stream( self.stream_id )
                self.context._flush_to_stream()
            self.context.remove_stream_delegate(self.stream_id)
            if len(self._pushed_responses) == len(self._pushed_streams):
                self.finish()
        elif isinstance(event, h2.events.PushedStreamReceived):
            stream = self.from_push_stream(event)
            self._pushed_streams[event.pushed_stream_id] = stream
        elif isinstance(event, h2.events.StreamReset):
            self.context.reset_stream(self.stream_id)
        elif isinstance(event, h2.events.WindowUpdated):
            self.window_updated()
        else:
            logger.warning('ignored event: %r, %r', event, event.__dict__)

    def finish(self):
        self._remove_timeout()
        self._unregister_unfinished_streams()

        if self._decompressor:
            self._data_received(self._decompressor.flush())

        data = b''.join(self.chunks)
        original_request = getattr(self.request, "original_request",
                                   self.request)
        new_request = None
        if (self.request.follow_redirects and
            self.request.max_redirects > 0 and
                self.code in (301, 302, 303, 307)):
            assert isinstance(self.request, _RequestProxy)
            new_request = copy.copy(self.request.request)
            new_request.url = urlparse.urljoin(self.request.url,
                                               self.headers["Location"])
            new_request.max_redirects = self.request.max_redirects - 1
            del new_request.headers["Host"]
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.3.4
            # Client SHOULD make a GET request after a 303.
            # According to the spec, 302 should be followed by the same
            # method as the original request, but in practice browsers
            # treat 302 the same as 303, and many servers use 302 for
            # compatibility with pre-HTTP/1.1 user agents which don't
            # understand the 303 status.
            if self.code in (302, 303):
                new_request.method = "GET"
                new_request.body = None
                for h in ["Content-Length", "Content-Type",
                          "Content-Encoding", "Transfer-Encoding"]:
                    try:
                        del self.request.headers[h]
                    except KeyError:
                        pass
            new_request.original_request = original_request
        if self.request.streaming_callback:
            buff = io.BytesIO()
        else:
            buff = io.BytesIO(data)  # TODO: don't require one big string?
        response = HTTP2Response(
            original_request, self.code, reason=self.reason,
            headers=self.headers, request_time=self.io_loop.time() - self.start_time,
            buffer=buff, effective_url=self.request.url,
            pushed_responses=self._pushed_responses.values(),
            new_request=new_request,
        )
        self._run_callback(response)

    def _data_received(self, chunk):
        if self.request.streaming_callback is not None:
            self.request.streaming_callback(chunk)
        else:
            self.chunks.append(chunk)

    def data_received(self, chunk):
        if self._decompressor:
            compressed_data = chunk
            decompressed = self._decompressor.decompress(compressed_data, 0)
            if decompressed:
                self._data_received(decompressed)
        else:
            self._data_received(chunk)

    def handle_exception(self, typ, error, tb):
        if isinstance(error, _RequestTimeout):
            if self._stream_ended:
                self.finish()
                return True
            else:
                error = HTTPError(599, "Timeout")

        self._remove_timeout()
        self._unregister_unfinished_streams()
        if hasattr(self, 'stream_id'):
            self.context.remove_stream_delegate(self.stream_id)

            # FIXME: our nginx server will simply reset stream,
            # without increase the window size which consumed by
            # queued data frame which was belongs to the stream we're resetting
            # self.context.reset_stream(self.stream_id, flush=True)
            self.context.reset_stream_ids.append(self.stream_id)

        error.__traceback__ = tb
        response = HTTP2Response(
            self.request, 599, error=error,
            request_time=self.io_loop.time() - self.start_time,
        )
        self._run_callback(response)
        return True

    def _unregister_unfinished_streams(self):
        for stream_id in self._pushed_streams:
            if stream_id not in self._pushed_responses:
                self.context.remove_stream_delegate(stream_id)

    def _remove_timeout(self):
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def _on_timeout(self):
        self._timeout = None
        self.connection_timeout = True
        self._pending_body = None
        if not self._stream_ended:
            self.context.reset_stream(self.stream_id, reason=ErrorCodes.CANCEL, flush=True)
        raise _RequestTimeout()

    def window_updated(self):
        logger.debug('window updated on stream #%d', self.stream_id)

        if self._pending_body is None:
            return

        if self in self.context.flow_control:
            return

        self.send_body(append=False)

    def on_connection_close(self, reason=None):
        try:
            raise HTTP2ConnectionClosed(reason)
        except Exception:
            self.handle_exception(*sys.exc_info())
