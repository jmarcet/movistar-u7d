#!/usr/bin/env python3

import aiohttp
import asyncio
import asyncio_dgram
import os
import signal
import socket

from contextlib import closing
from sanic import Sanic, response
from sanic.log import logger as log
from sanic.log import LOGGING_CONFIG_DEFAULTS


HOME = os.environ.get('HOME') or '/home'
SANIC_HOST = os.environ.get('SANIC_HOST') or '127.0.0.1'
SANIC_PORT = int(os.environ.get('SANIC_PORT')) or 8888
SANIC_EPG_HOST = os.environ.get('SANIC_EPG_HOST') or '127.0.0.1'
SANIC_EPG_PORT = int(os.environ.get('SANIC_EPG_PORT')) or 8889
UDPXY = os.environ.get('UDPXY') or 'http://192.168.137.1:4022/rtp'
UDPXY = UDPXY[:-1] if UDPXY.endswith('/') else UDPXY

GUIDE = os.path.join(HOME, 'guide.xml')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
IMAGENIO_URL = ('http://html5-static.svc.imagenio.telefonica.net'
                '/appclientv/nux/incoming/epg')
MIME = 'video/MP2T'
SANIC_EPG_URL = f'http://{SANIC_EPG_HOST}:{SANIC_EPG_PORT}'
SESSION = None
SESSION_LOGOS = None
YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

app = Sanic('Movistar_u7d', log_config=LOG_SETTINGS)
app.config.update({'KEEP_ALIVE': False})
host = socket.gethostbyname(socket.gethostname())


@app.listener('after_server_start')
async def notify_server_start(app, loop):
    log.info('after_server_start')
    global SESSION
    conn = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1)
    SESSION = aiohttp.ClientSession(connector=conn)


@app.listener('after_server_stop')
async def notify_server_stop(app, loop):
    log.info('after_server_stop killing u7d.py')
    p = await asyncio.create_subprocess_exec('/usr/bin/pkill', '-INT',
                                             '-f', '/app/u7d.py .+ -p ')
    await p.wait()


@app.get('/channels.m3u')
@app.get('/MovistarTV.m3u')
async def handle_channels(request):
    log.info(f'[{request.ip}] Req: {request.method} {request.url}')
    if not os.path.exists(CHANNELS):
        return response.json({}, 404)
    return await response.file(CHANNELS)


@app.get('/guide.xml')
async def handle_guide(request):
    log.info(f'[{request.ip}] Req: {request.method} {request.url}')
    await SESSION.get(f'{SANIC_EPG_URL}/reload_epg')
    if not os.path.exists(GUIDE):
        return response.json({}, 404)
    log.info(f'[{request.ip}] Return: {request.method} {request.url}')
    return await response.file(GUIDE)


@app.get('/Covers/<path>/<cover>')
@app.get('/Logos/<logo>')
async def handle_logos(request, cover=None, logo=None, path=None):
    log.debug(f'[{request.ip}] Req: {request.method} {request.url}')
    global SESSION_LOGOS
    if not SESSION_LOGOS:
        headers = {'User-Agent': 'MICA-IP-STB'}
        SESSION_LOGOS = aiohttp.ClientSession(headers=headers)

    if logo:
        orig_url = f'{IMAGENIO_URL}/channelLogo/{logo}'
    elif path and cover:
        orig_url = (f'{IMAGENIO_URL}/covers/programmeImages'
                    f'/portrait/290x429/{path}/{cover}')
    else:
        return response.json({'status': f'{request.url} not found'}, 404)

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

@app.get('/<channel_id>/rtp')
async def handle_channel(request, channel_id):
    try:
        epg_url = f'{SANIC_EPG_URL}/get_channel_address/{channel_id}'
        async with SESSION.get(epg_url) as r:
            if r.status != 200:
                return response.json({'status': f'{channel_id} not found'}, 404)
            r = await r.json()
            channel_id = r['channel_id']
            address = r['address']
            port = r['port']
        log.info(f'[{request.ip}] {request.method} {request.raw_url.decode()} => {UDPXY}/{address}:{port}')
        return response.redirect(f'{UDPXY}/{address}:{port}')
    except Exception as ex:
        log.error(f'[{request.ip}]',
                  f"aiohttp.ClientSession().get('{epg_url}')",
                  f'{repr(ex)}')


@app.get('/<channel_id>/<url>')
async def handle_flussonic(request, channel_id, url):
    log.info(f'[{request.ip}] {request.method} {request.raw_url.decode()}')
    if url.startswith('video-'):
        try:
            program_id = None
            epg_url = f'{SANIC_EPG_URL}/get_program_id/{channel_id}/{url}'
            async with SESSION.get(epg_url) as r:
                if r.status != 200:
                    return response.json({'status': f'{url} not found'}, 404)
                r = await r.json()
                channel_id = r['channel_id']
                program_id = r['program_id']
                offset = r['offset']
        except Exception as ex:
            log.error(f'[{request.ip}]',
                      f"aiohttp.ClientSession().get('{epg_url}')",
                      f'{repr(ex)}')

        if not program_id:
            return response.json({'status': f'{channel_id}/{url} not found'}, 404)

        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_port = s.getsockname()[1]

        cmd = ('/app/u7d.py', channel_id, program_id, '-s', offset, '-p', str(client_port))
        duration = int(url.split('-')[2].split('.')[0])
        remaining = str(duration - int(offset))
        u7d_msg = '%s %s [%s/%d]' % (channel_id, program_id, offset, duration)
        if record := request.query_args and request.query_args[0][0] == 'record':
            record_time = request.query_args[0][1] \
                if request.query_args[0][1].isnumeric() else remaining
            cmd += ('-t', record_time, '-w')
            log.info(f'[{request.ip}] '
                     'Record: '
                     f'[{record_time}]s '
                     '{u7d_msg}')
        cmd += ('-i', request.ip)
        u7d = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
        try:
            await asyncio.wait_for(u7d.wait(), 0.5)
            msg = (await u7d.stdout.readline()).decode().rstrip()
            log.info(f'NOT_AVAILABLE: {msg} {u7d_msg}')
            await SESSION.get(f'{SANIC_EPG_URL}/reload_epg')
            return response.json({'status': 'NOT_AVAILABLE',
                                  'msg': msg,
                                  'cmd': u7d_msg}, 404)
        except asyncio.exceptions.TimeoutError:
            pass

        if record:
            return response.json({'status': 'OK',
                                  'channel_id': channel_id,
                                  'program_id': program_id,
                                  'offset': offset,
                                  'time': record_time})

        log.info(f'[{request.ip}] Start: {u7d_msg}')
        with closing(await asyncio_dgram.bind((host, client_port))) as stream:
            timedout = False
            respond = await request.respond()
            try:
                await respond.send((await asyncio.wait_for(stream.recv(), 1))[0])
                while True:
                    await respond.send((await asyncio.wait_for(stream.recv(), 0.25))[0])
                return respond
            except Exception as ex:
                log.warning(f'[{request.ip}] {repr(ex)}')
                if isinstance(ex, TimeoutError):
                    timedout = True
            finally:
                log.info(f'[{request.ip}] End: {u7d_msg}')
                try:
                    if timedout:
                        await respond.send(end_stream=True)
                    else:
                        u7d.send_signal(signal.SIGINT)
                except Exception as ex:
                    log.warning(f'[{request.ip}] {repr(ex)}')

    else:
        return response.json({'status': 'URL not understood'}, 404)


if __name__ == '__main__':
    app.run(host=SANIC_HOST,
            port=SANIC_PORT,
            access_log=False,
            auto_reload=True,
            debug=False,
            workers=3)
