#!/usr/bin/env python3

import aiohttp
import asyncio
import asyncio_dgram
import os
import signal
import socket
import subprocess
import sys
import timeit

from collections import namedtuple
from contextlib import closing
from setproctitle import setproctitle
from sanic import Sanic, response
from sanic.log import error_logger, logger as log, LOGGING_CONFIG_DEFAULTS
from sanic.server import HttpProtocol
from sanic.models.server_types import ConnInfo
from sanic.touchup.meta import TouchUpMeta
from vod import VodData, VodLoop, VodSetup, find_free_port, COVER_URL, IMAGENIO_URL, UA


setproctitle('movistar_u7d')

HOME = os.getenv('HOME', '/home')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
GUIDE = os.path.join(HOME, 'guide.xml')
MIME_TS = 'video/MP2T;audio/mp3'
MP4_OUTPUT = bool(os.getenv('MP4_OUTPUT', False))
SANIC_EPG_URL = 'http://127.0.0.1:8889'
SANIC_HOST = os.getenv('LAN_IP', '0.0.0.0')
SANIC_PORT = int(os.getenv('SANIC_PORT', '8888'))
SANIC_THREADS = int(os.getenv('SANIC_THREADS', '4'))

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

PREFIX = ''

app = Sanic('movistar_u7d', log_config=LOG_SETTINGS)
app.config.update({'REQUEST_TIMEOUT': 1, 'RESPONSE_TIMEOUT': 1})
app.ctx.vod_client = None

VodArgs = namedtuple('Vod', ['channel', 'broadcast', 'iptv_ip',
                             'client_ip', 'client_port', 'start'])

_channels = _iptv = _session = _session_logos = None


@app.listener('after_server_start')
async def after_server_start(app, loop):
    log.debug('after_server_start')
    global PREFIX, _channels, _iptv, _session
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("172.26.23.3", 53))
    _iptv = s.getsockname()[0]
    s.close()

    conn = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1)
    _session = aiohttp.ClientSession(connector=conn)

    while True:
        try:
            async with _session.get(f'{SANIC_EPG_URL}/channels/') as r:
                if r.status == 200:
                    _channels = await r.json()
                    break
                else:
                    await asyncio.sleep(5)
        except (ConnectionRefusedError, aiohttp.client_exceptions.ClientConnectorError):
            await asyncio.sleep(5)

    if not app.ctx.vod_client:
        conn_vod = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS)
        app.ctx.vod_client = aiohttp.ClientSession(connector=conn_vod,
                                                   headers={'User-Agent': UA})


@app.get('/canales.m3u')
@app.get('/channels.m3u')
@app.get('/MovistarTV.m3u')
async def handle_channels(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(CHANNELS):
        return response.json({}, 404)
    return await response.file(CHANNELS)


@app.get('/guia.xml')
@app.get('/guide.xml')
async def handle_guide(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(GUIDE):
        return response.json({}, 404)
    log.debug(f'[{request.ip}] Return: {request.method} {request.url}')
    return await response.file(GUIDE)


@app.route('/Covers/<path>/<cover>', methods=['GET', 'HEAD'])
@app.route('/Logos/<logo>', methods=['GET', 'HEAD'])
async def handle_logos(request, cover=None, logo=None, path=None):
    log.debug(f'[{request.ip}] {request.method} {request.url}')
    if logo:
        orig_url = f'{IMAGENIO_URL}/channelLogo/{logo}'
    elif path and cover:
        orig_url = f'{COVER_URL}/{path}/{cover}'
    else:
        return response.json({'status': f'{request.url} not found'}, 404)

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type='image/jpeg', status=200)

    global _session_logos
    if not _session_logos:
        headers = {'User-Agent': 'MICA-IP-STB'}
        _session_logos = aiohttp.ClientSession(headers=headers)

    async with _session_logos.get(orig_url) as r:
        if r.status == 200:
            logo_data = await r.read()
            headers = {}
            headers.setdefault('Content-Disposition',
                               f'attachment; filename="{logo}"')
            return response.HTTPResponse(body=logo_data,
                                         content_type='image/jpeg',
                                         headers=headers,
                                         status=200)
        else:
            return response.json({'status': f'{orig_url} not found'}, 404)


@app.route('/<channel_id>/live', methods=['GET', 'HEAD'])
async def handle_channel(request, channel_id):
    _start = timeit.default_timer()
    try:
        name, mc_grp, mc_port = [_channels[channel_id][t]
                                 for t in ['name', 'address', 'port']]
    except (AttributeError, KeyError):
        return response.json({'status': f'{channel_id} not found'}, 404)

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    with closing(socket.socket(socket.AF_INET,
                               socket.SOCK_DGRAM,
                               socket.IPPROTO_UDP)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((mc_grp, int(mc_port)))
        sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(_iptv))
        sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(mc_grp) + socket.inet_aton(_iptv))

        with closing(await asyncio_dgram.from_socket(sock)) as stream:
            log.info(f'[{request.ip}] {request.raw_url} -> Playing "{name}"')
            try:
                _response = await request.respond(content_type=MIME_TS)
                await _response.send((await stream.recv())[0][28:])
                _stop = timeit.default_timer()
                log.info(f'[{request.ip}] {request.raw_url} -> {_stop - _start}s')
                while True:
                    await _response.send((await stream.recv())[0][28:])
            finally:
                log.info(f'[{request.ip}] {request.raw_url} -> Stopped "{name}"')
                try:
                    await _response.eof()
                except AttributeError:
                    pass


@app.route('/<channel_id>/<url>', methods=['GET', 'HEAD'])
async def handle_flussonic(request, channel_id, url):
    _start = timeit.default_timer()
    try:
        async with _session.get(f'{SANIC_EPG_URL}/program_id/{channel_id}/{url}') as r:
            name, program_id, duration, offset = (await r.json()).values()
    except (AttributeError, KeyError, ValueError) as ex:
        log.error(f'{repr(ex)}')
        return response.json({'status': f'{channel_id}/{url} not found {repr(ex)}'}, 404)

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    client_port = find_free_port(_iptv)
    vod_msg = f'"{name}" {program_id} [{offset}/{duration}]'

    record = request.args.get('record', 0)
    if record:
        cmd = f'{PREFIX}vod.py {channel_id} {program_id} -s {offset}' \
              f' -p {client_port} -i {request.ip} -a {_iptv}'
        record = int(record)
        record_time = record if record > 1 else duration - offset

        cmd += f' -t {record_time} -w'
        if MP4_OUTPUT or request.args.get('mp4', False):
            cmd += ' --mp4 1'
        if request.args.get('vo', False):
            cmd += ' --vo 1'

        log.info(f'[{request.ip}] {request.raw_url} -> Recording [{record_time}s] {vod_msg}')
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        subprocess.Popen(cmd.split())
        return response.json({'status': 'OK',
                              'channel_id': channel_id, 'program_id': program_id,
                              'offset': offset, 'time': record_time})

    _args = VodArgs(channel_id, program_id, _iptv, request.ip, client_port, offset)
    vod_data = await VodSetup(_args, app.ctx.vod_client)
    vod = asyncio.create_task(VodLoop(_args, vod_data))
    try:
        with closing(await asyncio_dgram.bind((_iptv, client_port))) as stream:
            log.info(f'[{request.ip}] {request.raw_url} -> Playing {vod_msg}')
            try:
                _response = await request.respond(content_type=MIME_TS)
                await _response.send((await asyncio.wait_for(stream.recv(), 1))[0])
                _stop = timeit.default_timer()
                log.info(f'[{request.ip}] {request.raw_url} -> {_stop - _start}s')
            except asyncio.exceptions.TimeoutError:
                log.error(f'NOT_AVAILABLE: {vod_msg}')
                return response.empty(404)
            try:
                while True:
                    await _response.send((await stream.recv())[0])
            finally:
                log.info(f'[{request.ip}] {request.raw_url} -> Stopped {vod_msg}')
                try:
                    await _response.eof()
                except AttributeError:
                    pass
    finally:
        try:
            vod.cancel()
            await asyncio.wait({vod})
        except (ProcessLookupError, TypeError):
            pass


@app.get('/favicon.ico')
async def handle_notfound(request):
    return response.json({}, 404)


class VodHttpProtocol(HttpProtocol, metaclass=TouchUpMeta):
    def connection_made(self, transport):
        """
        HTTP-protocol-specific new connection handler tuned for VOD.
        """
        try:
            transport.set_write_buffer_limits(low=188, high=1316)
            self.connections.add(self)
            self.transport = transport
            self._task = self.loop.create_task(self.connection_task())
            self.recv_buffer = bytearray()
            self.conn_info = ConnInfo(self.transport, unix=self._unix)
        except Exception:
            error_logger.exception("protocol.connect_made")


if __name__ == '__main__':
    try:
        app.run(host=SANIC_HOST, port=SANIC_PORT, protocol=VodHttpProtocol,
                access_log=False, auto_reload=False, debug=False, workers=SANIC_THREADS)
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        log.critical(f'{repr(ex)}')
        sys.exit(1)
