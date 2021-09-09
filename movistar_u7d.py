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
from sanic.compat import open_async
from sanic.log import error_logger, logger as log, LOGGING_CONFIG_DEFAULTS
from sanic.server import HttpProtocol
from sanic.models.server_types import ConnInfo
from sanic.touchup.meta import TouchUpMeta

from movistar_epg import get_ffmpeg_procs
from vod import VodLoop, VodSetup, find_free_port, COVER_URL, IMAGENIO_URL, RECORDINGS, UA


setproctitle('movistar_u7d')

HOME = os.getenv('HOME', '/home')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
GUIDE = os.path.join(HOME, 'guide.xml')
IPTV_BW = int(os.getenv('IPTV_BW', '85000'))
IPTV_BW = 85000 if IPTV_BW > 90000 else IPTV_BW
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
app.config.update({'GRACEFUL_SHUTDOWN_TIMEOUT': 0,
                   'REQUEST_TIMEOUT': 1,
                   'RESPONSE_TIMEOUT': 1})
app.ctx.vod_client = None

VodArgs = namedtuple('Vod', ['channel', 'broadcast', 'iptv_ip',
                             'client_ip', 'client_port', 'start'])

_channels = _iptv = _session = _session_logos = _t_tp = None
_network_fsignal = '/tmp/.u7d_bw'
_network_saturated = False
_responses = []


@app.listener('before_server_stop')
async def before_server_stop(app, loop):
    global _responses
    for req, resp, vod_msg in _responses:
        try:
            await resp.eof()
            log.info(f'[{req.ip}] {req.raw_url} -> Stopped 2 {vod_msg}')
        except AttributeError:
            pass


@app.listener('after_server_start')
async def after_server_start(app, loop):
    log.debug('after_server_start')
    global PREFIX, _channels, _iptv, _session, _t_tp
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

    if os.path.exists('/proc/net/route'):
        async with await open_async('/proc/net/route') as f:
            _route = await f.read()

        try:
            iface = list(set([u[0] for u in
                              [t.split()[0:3] for t in _route.splitlines()]
                              if u[2] == '0100400A']))[0]  # gw=10.64.0.1
        except (AttributeError, KeyError):
            return

        # let's control dynamically the number of allowed clients
        iface_rx = f'/sys/class/net/{iface}/statistics/rx_bytes'
        if os.path.exists(iface_rx):
            log.info('Setting dynamic limit of clients '
                     'from ' + iface_rx.split('/')[4] + ' bw')
            _t_tp = app.add_task(throughput(iface_rx))
            if not os.path.exists(_network_fsignal):
                with open(_network_fsignal, 'w') as f:
                    f.write('')


@app.listener('after_server_stop')
async def after_server_stop(app, loop):
    try:
        _t_tp.cancel()
        await asyncio.wait({_t_tp})
    except (AttributeError, ProcessLookupError):
        pass


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
    if _network_saturated:
        procs = await get_ffmpeg_procs()
        if procs:
            os.kill(int(procs[-1].split()[0]), signal.SIGINT)
            log.warning(f'[{request.ip}] {request.raw_url} -> Killed '
                        'ffmpeg "' + procs[-1].split(RECORDINGS)[1] + '"')
        else:
            log.warning(f'[{request.ip}] {request.raw_url} -> Network Saturated')
            return response.json({'status': 'Network Saturated'}, 503)
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
    procs = None
    if _network_saturated:
        procs = await get_ffmpeg_procs()
        if not procs:
            log.warning(f'[{request.ip}] {request.raw_url} -> Network Saturated')
            return response.json({'status': 'Network Saturated'}, 503)
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
    if procs:
        if record:
            log.warning(f'[{request.ip}] {request.raw_url} -> Network Saturated')
            return response.json({'status': 'Network Saturated'}, 503)
        else:
            os.kill(int(procs[-1].split()[0]), signal.SIGINT)
            log.warning(f'[{request.ip}] {request.raw_url} -> Killed '
                        'ffmpeg "' + procs[-1].split(RECORDINGS)[1] + '"')
    elif record:
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
                global _responses
                _responses.append((request, _response, vod_msg))
                await _response.send((await asyncio.wait_for(stream.recv(), 1))[0])
                _stop = timeit.default_timer()
                log.info(f'[{request.ip}] {request.raw_url} -> {_stop - _start}s')
            except asyncio.exceptions.TimeoutError:
                log.error(f'NOT_AVAILABLE: {vod_msg}')
                return response.empty(404)

            while True:
                await _response.send((await stream.recv())[0])
    finally:
        try:
            vod.cancel()
            await asyncio.wait({vod})
        except (ProcessLookupError, TypeError):
            pass
        _responses.remove((request, _response, vod_msg))
        log.info(f'[{request.ip}] {request.raw_url} -> Stopped 1 {vod_msg}')
        await _response.eof()


@app.get('/favicon.ico')
async def handle_notfound(request):
    return response.json({}, 404)


async def throughput(iface_rx):
    global _network_saturated
    cur = last = 0
    while True:
        async with await open_async(iface_rx) as f:
            cur = int((await f.read())[:-1])
            if last:
                tp = int((cur - last) * 8 / 1000 / 3)
                _network_saturated = tp > IPTV_BW
            last = cur
            await asyncio.sleep(3)


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
