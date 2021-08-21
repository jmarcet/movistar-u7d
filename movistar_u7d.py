#!/usr/bin/env python3

import aiohttp
import asyncio
import asyncio_dgram
import os
import re
import signal
import socket
import struct
import subprocess
import sys

from contextlib import closing
from setproctitle import setproctitle
from sanic import Sanic, exceptions, response
from sanic.log import logger as log
from sanic.log import LOGGING_CONFIG_DEFAULTS
from threading import Thread
from vod import find_free_port, COVER_URL, IMAGENIO_URL


setproctitle('movistar_u7d')

if 'IPTV_ADDRESS' in os.environ:
    IPTV = os.getenv('IPTV_ADDRESS')
else:
    IPTV = socket.gethostbyname(socket.gethostname())

SANIC_EPG_URL = 'http://127.0.0.1:8889'
SANIC_HOST = os.getenv('LAN_IP', '0.0.0.0')
SANIC_PORT = int(os.getenv('SANIC_PORT', '8888'))
SANIC_THREADS = int(os.getenv('SANIC_THREADS', '4'))

HOME = os.getenv('HOME', '/home')
GUIDE = os.path.join(HOME, 'guide.xml')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
MIME_TS = 'video/MP2T;audio/mp3'
MP4_OUTPUT = bool(os.getenv('MP4_OUTPUT', False))
SESSION = None
SESSION_LOGOS = None
YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

PREFIX = ''

app = Sanic('movistar_u7d', log_config=LOG_SETTINGS)
app.config.update({'REQUEST_TIMEOUT': 1, 'RESPONSE_TIMEOUT': 1})


@app.listener('after_server_start')
async def after_server_start(app, loop):
    log.debug('after_server_start')
    global PREFIX, SESSION
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    conn = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1)
    SESSION = aiohttp.ClientSession(connector=conn)


@app.get('/channels.m3u')
@app.get('/MovistarTV.m3u')
async def handle_channels(request):
    log.info(f'[{request.ip}] {request.method} {request.url}')
    if not os.path.exists(CHANNELS):
        return response.json({}, 404)
    return await response.file(CHANNELS)


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

    global SESSION_LOGOS
    if not SESSION_LOGOS:
        headers = {'User-Agent': 'MICA-IP-STB'}
        SESSION_LOGOS = aiohttp.ClientSession(headers=headers)

    async with SESSION_LOGOS.get(orig_url) as r:
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
    try:
        epg_url = f'{SANIC_EPG_URL}/channel_address/{channel_id}'
        async with SESSION.get(epg_url) as r:
            if r.status != 200:
                return response.json({'status': f'{channel_id} not found'}, 404)
            r = await r.json()
            mc_grp, name, mc_port = [r[t] for t in ['address', 'name', 'port']]
    except Exception as ex:
        log.error(f"[{request.ip}] get('{epg_url}') {ex}")
        return response.json({'status': f'{channel_id}/live not found'}, 404)

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    with closing(socket.socket(socket.AF_INET,
                               socket.SOCK_DGRAM,
                               socket.IPPROTO_UDP)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((mc_grp, int(mc_port)))
        sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(IPTV))
        sock.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(mc_grp) + socket.inet_aton(IPTV))

        with closing(await asyncio_dgram.from_socket(sock)) as stream:
            log.info(f'[{request.ip}] {request.raw_url} -> Playing "{name}"')
            try:
                _response = await request.respond(content_type=MIME_TS)
                while True:
                    await _response.send((await stream.recv())[0][28:])
            finally:
                log.info(f'[{request.ip}] {request.raw_url} -> Stopped "{name}"')
                await _response.eof()


@app.route('/<channel_id>/<url>', methods=['GET', 'HEAD'])
async def handle_flussonic(request, channel_id, url):
    log.debug(f'[{request.ip}] {request.method} {request.raw_url}')
    program_id = None
    epg_url = f'{SANIC_EPG_URL}/program_id/{channel_id}/{url}'
    try:
        async with SESSION.get(epg_url) as r:
            if r.status != 200:
                return response.json({'status': f'{url} not found'}, 404)
            r = await r.json()
            name, program_id, duration, offset = [r[t] for t in ['name', 'program_id',
                                                                 'duration', 'offset']]
    except Exception as ex:
        log.error(f'{request.ip}] {ex}')

    if not program_id:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    client_port = find_free_port()
    cmd = f'{PREFIX}vod.py {channel_id} {program_id} -s {offset}'
    cmd += f' -p {client_port} -i {request.ip}'
    remaining = str(int(duration) - int(offset))
    vod_msg = '"%s" %s [%s/%s]' % (name, program_id, offset, duration)

    if request.method == 'HEAD':
        return response.HTTPResponse(content_type=MIME_TS, status=200)
    elif record := int(request.args.get('record', 0)):
        record_time = record if record > 1 else remaining
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

    try:
        vod = await asyncio.create_subprocess_exec(*(cmd.split()),)
        with closing(await asyncio_dgram.bind((IPTV, client_port))) as stream:
            log.info(f'[{request.ip}] {request.raw_url} -> Playing {vod_msg}')
            try:
                _response = await request.respond(content_type=MIME_TS)
                await _response.send((await asyncio.wait_for(stream.recv(), 1))[0])
            except asyncio.exceptions.TimeoutError:
                log.error(f'NOT_AVAILABLE: {vod_msg}')
                return response.empty(404)
            try:
                while True:
                    await _response.send((await stream.recv())[0])
            finally:
                log.debug(f'[{request.ip}] {request.raw_url} -> Stopped {vod_msg}')
                try:
                    await _response.eof()
                except AttributeError:
                    pass
    finally:
        try:
            await vod.terminate()
        except (ProcessLookupError, TypeError):
            pass


@app.get('/favicon.ico')
async def handle_notfound(request):
    return response.json({}, 404)


if __name__ == '__main__':
    try:
        app.run(host=SANIC_HOST, port=SANIC_PORT,
                access_log=False, auto_reload=True, debug=False, workers=SANIC_THREADS)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as ex:
        log.critical(f'{ex}')
        sys.exit(1)
