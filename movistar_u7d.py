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
import ujson
import urllib.parse

from collections import namedtuple
from contextlib import closing
from setproctitle import setproctitle
from sanic import Sanic, exceptions, response
from sanic.compat import open_async, stat_async
from sanic.handlers import ContentRangeHandler
from sanic.log import error_logger, logger as log, LOGGING_CONFIG_DEFAULTS
from sanic.models.server_types import ConnInfo
from sanic.server import HttpProtocol
from sanic.touchup.meta import TouchUpMeta

from movistar_epg import get_ffmpeg_procs
from vod import VodLoop, VodSetup, find_free_port, COVER_URL, IMAGENIO_URL, UA


setproctitle('movistar_u7d')

HOME = os.getenv('HOME', '/home')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
CHANNELS_CLOUD = os.path.join(HOME, 'cloud.m3u')
CHANNELS_RECORDINGS = os.path.join(HOME, 'recordings.m3u')
GUIDE = os.path.join(HOME, 'guide.xml')
GUIDE_CLOUD = os.path.join(HOME, 'cloud.xml')
IPTV_BW = int(os.getenv('IPTV_BW', '85000'))
IPTV_BW = 85000 if IPTV_BW > 90000 else IPTV_BW
MIME_TS = 'video/MP2T;audio/mp3'
MIME_WEBM = 'video/webm'
MP4_OUTPUT = bool(os.getenv('MP4_OUTPUT', False))
NETWORK_FSIGNAL = '/tmp/.u7d_bw'
RECORDINGS = os.getenv('RECORDINGS', None)
SANIC_EPG_URL = 'http://127.0.0.1:8889'
SANIC_HOST = os.getenv('LAN_IP', '0.0.0.0')
SANIC_PORT = int(os.getenv('SANIC_PORT', '8888'))
SANIC_THREADS = int(os.getenv('SANIC_THREADS', '4'))

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

PREFIX = ''
_CHANNELS = {}
_IPTV = _SESSION = _SESSION_LOGOS = None
_NETWORK_SATURATED = False
_RESPONSES = []

VodArgs = namedtuple('Vod', ['channel', 'broadcast', 'iptv_ip',
                             'client_ip', 'client_port', 'start', 'cloud'])

app = Sanic('movistar_u7d', log_config=LOG_SETTINGS)
app.config.update({'FALLBACK_ERROR_FORMAT': 'json',
                   'GRACEFUL_SHUTDOWN_TIMEOUT': 0,
                   'REQUEST_TIMEOUT': 1,
                   'RESPONSE_TIMEOUT': 1})
app.ctx.vod_client = _t_tp = None


@app.listener('after_server_start')
async def after_server_start(app, loop):
    log.debug('after_server_start')
    global PREFIX, _CHANNELS, _IPTV, _SESSION, _t_tp
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("172.26.23.3", 53))
    _IPTV = s.getsockname()[0]
    s.close()

    conn = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1)
    _SESSION = aiohttp.ClientSession(connector=conn, json_serialize=ujson.dumps)

    while True:
        try:
            async with _SESSION.get(f'{SANIC_EPG_URL}/channels/') as r:
                if r.status == 200:
                    _CHANNELS = await r.json()
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
            if not os.path.exists(NETWORK_FSIGNAL):
                with open(NETWORK_FSIGNAL, 'w') as f:
                    f.write('')


@app.listener('before_server_stop')
async def before_server_stop(app, loop):
    global _RESPONSES
    for req, resp, vod_msg in _RESPONSES:
        try:
            await resp.eof()
            log.info(f'[{req.ip}] {req.raw_url.decode()} -> Stopped 2 {vod_msg}')
        except AttributeError:
            pass


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
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')
    return await response.file(CHANNELS)


@app.get('/cloud.m3u')
@app.get('/nube.m3u')
async def handle_channels_cloud(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(CHANNELS_CLOUD):
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')
    return await response.file(CHANNELS_CLOUD)


@app.get('/grabaciones.m3u')
@app.get('/recordings.m3u')
async def handle_channels_recordings(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(CHANNELS_RECORDINGS):
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')
    return await response.file(CHANNELS_RECORDINGS)


@app.get('/guia.xml')
@app.get('/guide.xml')
async def handle_guide(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(GUIDE):
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')
    return await response.file(GUIDE)


@app.get('/cloud.xml')
@app.get('/nube.xml')
async def handle_guide_cloud(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(GUIDE_CLOUD):
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')
    return await response.file(GUIDE_CLOUD)


@app.route('/Covers/<path:int>/<cover>', methods=['GET', 'HEAD'])
@app.route('/Logos/<logo>', methods=['GET', 'HEAD'])
async def handle_logos(request, cover=None, logo=None, path=None):
    log.debug(f'[{request.ip}] {request.method} {request.url}')
    if logo and logo.split('.')[0].isdigit():
        orig_url = f'{IMAGENIO_URL}/channelLogo/{logo}'
    elif path and cover and cover.split('.')[0].isdigit():
        orig_url = f'{COVER_URL}/{path}/{cover}'
    else:
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type='image/jpeg', status=200)

    global _SESSION_LOGOS
    if not _SESSION_LOGOS:
        headers = {'User-Agent': 'MICA-IP-STB'}
        _SESSION_LOGOS = aiohttp.ClientSession(headers=headers)

    async with _SESSION_LOGOS.get(orig_url) as r:
        if r.status == 200:
            logo_data = await r.read()
            headers = {}
            headers.setdefault('Content-Disposition', f'attachment; filename="{logo}"')
            return response.HTTPResponse(body=logo_data, content_type='image/jpeg',
                                         headers=headers, status=200)
        else:
            raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')


@app.route('/<channel_id:int>/live', methods=['GET', 'HEAD'])
async def handle_channel(request, channel_id):
    _start = timeit.default_timer()
    _raw_url = request.raw_url.decode()
    if _NETWORK_SATURATED:
        procs = await get_ffmpeg_procs()
        if procs:
            os.kill(int(procs[-1].split()[0]), signal.SIGINT)
            log.warning(f'[{request.ip}] {_raw_url} -> Killed '
                        'ffmpeg "' + procs[-1].split(RECORDINGS)[1] + '"')
        else:
            log.warning(f'[{request.ip}] {_raw_url} -> Network Saturated')
            raise exceptions.ServiceUnavailable('Network Saturated')
    try:
        name, mc_grp, mc_port = [_CHANNELS[str(channel_id)][t]
                                 for t in ['name', 'address', 'port']]
    except (AttributeError, KeyError):
        log.error(f'{_raw_url} not found')
        raise exceptions.NotFound(f'Requested URL {_raw_url} not found')

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((mc_grp, int(mc_port)))
        sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(_IPTV))
        sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(mc_grp) + socket.inet_aton(_IPTV))

        try:
            with closing(await asyncio_dgram.from_socket(sock)) as stream:
                _response = await request.respond(content_type=MIME_TS)
                await _response.send((await stream.recv())[0][28:])
                _lat = timeit.default_timer() - _start
                asyncio.create_task(
                    _SESSION.post(f'{SANIC_EPG_URL}/prom_event/add', json={
                        'method': 'live',
                        'endpoint': f'{name} _ {request.ip} _ ',
                        'channel_id': channel_id,
                        'msg': f'[{request.ip}] -> Playing "{name}" {_lat}s ',
                        'id': _start,
                        'lat': _lat}))
                while True:
                    await _response.send((await stream.recv())[0][28:])
        finally:
            try:
                await _response.eof()
            finally:
                await _SESSION.post(f'{SANIC_EPG_URL}/prom_event/remove', json={
                    'method': 'live',
                    'endpoint': f'{name} _ {request.ip} _ ',
                    'channel_id': channel_id,
                    'msg': f'[{request.ip}] -> Stopped "{name}" {_raw_url} ',
                    'id': _start})


@app.route('/<channel_id:int>/<url>', methods=['GET', 'HEAD'])
async def handle_flussonic(request, channel_id, url, cloud=False):
    _start = timeit.default_timer()
    _raw_url = request.raw_url.decode()
    procs = None
    if _NETWORK_SATURATED:
        procs = await get_ffmpeg_procs()
        if not procs:
            log.warning(f'[{request.ip}] {_raw_url} -> Network Saturated')
            raise exceptions.ServiceUnavailable('Network Saturated')
    try:
        async with _SESSION.get(f'{SANIC_EPG_URL}/program_id/{channel_id}/{url}' + (
                '?cloud=1' if cloud else '')) as r:
            name, program_id, duration, offset = (await r.json()).values()
    except (AttributeError, KeyError, ValueError):
        log.error(f'{_raw_url} not found')
        raise exceptions.NotFound(f'Requested URL {_raw_url} not found')

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    client_port = find_free_port(_IPTV)
    vod_msg = f'"{name}" {program_id} [{offset}/{duration}]'

    record = request.args.get('record', 0)
    if procs:
        if record:
            log.warning(f'[{request.ip}] {_raw_url} -> Network Saturated')
            raise exceptions.ServiceUnavailable('Network Saturated')
        else:
            os.kill(int(procs[-1].split()[0]), signal.SIGINT)
            log.warning(f'[{request.ip}] {_raw_url} -> Killed '
                        'ffmpeg "' + procs[-1].split(RECORDINGS)[1] + '"')
    elif record:
        cmd = f'{PREFIX}vod.py {channel_id} {program_id} -s {offset}' \
              f' -p {client_port} -i {request.ip} -a {_IPTV}'
        record = int(record)
        record_time = record if record > 1 else duration - offset

        cmd += f' -t {record_time} -w'
        if cloud:
            cmd += ' --cloud 1'
        if MP4_OUTPUT or request.args.get('mp4', False):
            cmd += ' --mp4 1'
        if request.args.get('vo', False):
            cmd += ' --vo 1'

        log.info(f'[{request.ip}] {_raw_url} -> Recording [{record_time}s] {vod_msg}')
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        subprocess.Popen(cmd.split())
        return response.json({'status': 'OK',
                              'channel_id': channel_id, 'program_id': program_id,
                              'offset': offset, 'time': record_time})

    _args = VodArgs(channel_id, program_id, _IPTV, request.ip, client_port, offset, cloud)
    vod_data = await VodSetup(_args, app.ctx.vod_client)
    if not vod_data:
        log.error(f'{_raw_url} not found')
        raise exceptions.NotFound(f'Requested URL {_raw_url} not found')
    vod = asyncio.create_task(VodLoop(_args, vod_data))
    _endpoint = _CHANNELS[str(channel_id)]['name'] + f' _ {request.ip} _ '
    try:
        with closing(await asyncio_dgram.bind((_IPTV, client_port))) as stream:
            try:
                _response = await request.respond(content_type=MIME_TS)
                global _RESPONSES
                _RESPONSES.append((request, _response, vod_msg))
                await _response.send((await asyncio.wait_for(stream.recv(), 1))[0])
                _lat = timeit.default_timer() - _start
                asyncio.create_task(
                    _SESSION.post(f'{SANIC_EPG_URL}/prom_event/add', json={
                        'method': 'catchup',
                        'endpoint': _endpoint,
                        'channel_id': channel_id,
                        'url': url,
                        'msg': f'[{request.ip}] -> Playing {vod_msg} {_lat}s ',
                        'id': _start,
                        'cloud': cloud,
                        'lat': _lat}))
            except asyncio.exceptions.TimeoutError:
                log.error(f'NOT_AVAILABLE: {vod_msg}')
                raise exceptions.NotFound(f'Requested URL {_raw_url} not found')

            while True:
                await _response.send((await stream.recv())[0])
    finally:
        try:
            await _response.eof()
        finally:
            _RESPONSES.remove((request, _response, vod_msg))
            try:
                vod.cancel()
            except (ProcessLookupError, TypeError):
                pass
            await _SESSION.post(f'{SANIC_EPG_URL}/prom_event/remove', json={
                'method': 'catchup',
                'endpoint': _endpoint,
                'channel_id': channel_id,
                'url': url,
                'msg': f'[{request.ip}] -> Stopped {vod_msg} {_raw_url} ',
                'id': _start,
                'cloud': cloud,
                'offset': timeit.default_timer() - _start})
            await asyncio.wait({vod})


@app.route('/cloud/<channel_id:int>/<url>', methods=['GET', 'HEAD'])
async def handle_flussonic_cloud(request, channel_id, url):
    if url == 'live':
        return await handle_channel(request, channel_id)
    return await handle_flussonic(request, channel_id, url, cloud=True)


@app.route('/recording/', methods=['GET', 'HEAD'])
async def handle_recording(request):
    if not RECORDINGS:
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')

    _path = urllib.parse.unquote(request.raw_url.decode().split('/recording/')[1])
    file = os.path.join(RECORDINGS, _path[1:])
    if os.path.exists(file):
        if request.method == 'HEAD':
            return response.HTTPResponse(status=200)

        ext = os.path.splitext(file)[1]
        if ext == '.jpg':
            return await response.file(file)

        elif ext in ('.avi', '.mkv', '.mp4', '.mpeg', '.mpg', '.ts'):
            try:
                _range = ContentRangeHandler(request, await stat_async(file))
            except exceptions.HeaderNotFound:
                _range = None

            return await response.file_stream(file, mime_type=MIME_WEBM, _range=_range)

    raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')


@app.get('/favicon.ico')
async def handle_notfound(request):
    return response.empty(404)


@app.get('/metrics')
async def handle_prometheus(request):
    try:
        async with _SESSION.get(f'{SANIC_EPG_URL}/metrics') as r:
            return response.text((await r.read()).decode())
    except aiohttp.client_exceptions.ClientConnectorError:
        raise exceptions.ServiceUnavailable('Not available')


async def throughput(iface_rx):
    global _NETWORK_SATURATED
    cur = last = 0
    while True:
        async with await open_async(iface_rx) as f:
            cur = int((await f.read())[:-1])
            if last:
                tp = int((cur - last) * 8 / 1000 / 3)
                _NETWORK_SATURATED = tp > IPTV_BW
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
