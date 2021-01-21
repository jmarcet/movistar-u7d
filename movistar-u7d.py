#!/usr/bin/env python3

import aiohttp
import asyncio
import asyncio_dgram
import glob
import json
import logging
import os
import socket
import sys
import time

from contextlib import closing
from sanic import Sanic, response
from sanic.log import logger


HOME = os.environ.get('HOME') or '/home/'
SANIC_HOST = os.environ.get('SANIC_HOST') or '127.0.0.1'
SANIC_PORT = os.environ.get('SANIC_PORT') or '8888'
UDPXY = os.environ.get('UDPXY') or 'http://192.168.137.1:4022/rtp/'

MIME = 'video/MP2T'
GUIDE = os.path.join(HOME, 'guide.xml')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')

logging_format = "%(process)d-%(levelname)s "
logging_format += "%(module)s::%(funcName)s():l%(lineno)d: "
logging_format += "%(message)s"

logging.basicConfig(
    format=logging_format,
    level=logging.DEBUG
)
log = logging.getLogger()

app = Sanic('Movistar_u7d')
app.config.update({'KEEP_ALIVE': False})

_epgdata = {}
_lastprogram = {}
_proc = {}


@app.listener('after_server_start')
@app.listener('after_server_stop')
async def notify_server(app, loop):
    global _epgdata
    p = await asyncio.create_subprocess_exec('/usr/bin/pkill', '-f', '/app/u7d.py .+ --client_port')
    await p.wait()

    epg_cache = '/home/.xmltv/cache/epg.json'
    epgs = glob.glob('/home/epg.*.json')
    if os.path.exists(epg_cache) and os.stat(epg_cache).st_size > 1024:
        epgs.append(epg_cache)
    for epg in epgs:
        day_epg = json.loads(open(epg).read())
        if _epgdata:
            _epgdata = {**_epgdata, **day_epg}
        else:
            _epgdata = day_epg


@app.get('/channels.m3u')
@app.get('/MovistarTV.m3u')
async def handle_channels(request):
    if not os.path.exists(CHANNELS):
        return response.json({}, 404)
    return await response.file(CHANNELS)

@app.get('/guide.xml')
async def handle_guide(request):
    if not os.path.exists(GUIDE):
        return response.json({}, 404)
    return await response.file(GUIDE)

@app.get('/rtp/<channel_id>/<channel_key>/<url>')
async def handle_rtp(request, channel_id, channel_key, url):
    log.info(f'Request: {request.method} {request.raw_url.decode()}')
    global _lastprogram, _proc

    if request.ip:
        log.info(f'Client: {request.ip}')

    # Nromal IPTV Channel
    if url.startswith('239'):
        await cleanup_proc(request, 'on channel change')
        if request.ip in _lastprogram.keys():
            _lastprogram.pop(request.ip, None)

        log.info(f'Redirecting to {UDPXY + url}')
        return response.redirect(UDPXY + url)

    # Flussonic catchup style url
    elif url.startswith('video-'):
        await cleanup_proc(request, 'on vod')

        start = url.split('-')[1]
        duration = url.split('-')[2].split('.')[0]
        last_event = None
        offset = '0'
        program_id = None

        if 'data' in _epgdata and channel_key in _epgdata['data'] and start in _epgdata['data'][channel_key]:
            program_id = str(_epgdata['data'][channel_key][start]['pid'])
            _lastprogram[request.ip] = (channel_id, program_id, start)
        elif request.ip in _lastprogram.keys():
            new_start = start
            channel_id, program_id, start = _lastprogram[request.ip]
            offset = str(int(new_start) - int(start))
        elif 'data' in _epgdata and channel_key in _epgdata['data']:
            for event in sorted(_epgdata['data'][channel_key].keys()):
                if int(event) > int(start):
                    break
                last_event = event
            if last_event:
                offset = str(int(start) - int(last_event))
                program_id = str(_epgdata['data'][channel_key][last_event]['pid'])
                _lastprogram[request.ip] = (channel_id, program_id, last_event)

        if not program_id:
            return response.json({'status': 'channel_id={channel_id} program_id={program_id} not found'}, 404)

        host = socket.gethostbyname(socket.gethostname())
        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_port = str(s.getsockname()[1])

        log.info(f'Exec: /app/u7d.py {channel_id} {program_id} --start {offset} --client_port {client_port}')

        _proc[request.ip] = await asyncio.create_subprocess_exec('/app/u7d.py', channel_id, program_id,
                                                                 '--start', offset, '--client_port', client_port)

        #log.info(str({'path': request.path, 'url': url,
        #              'channel_id': channel_id, 'channel_key': channel_key,
        #              'start': start, 'offset': offset, 'duration': duration}))


        async def udp_streaming(response):
            log.info(f'Stream: @{host}:{client_port}')
            stream = await asyncio_dgram.bind((host, client_port))
            while True:
                data, remote_addr = await stream.recv()
                await response.write(data)
            await cleanup_proc(request, 'after udp_streaming')

        return response.stream(udp_streaming, content_type=MIME)

    # Not recognized url
    else:
        if request.ip in _lastprogram.keys():
            _lastprogram.pop(request.ip, None)
        return response.json({'status': 'URL not understood'}, 404)

async def cleanup_proc(request, message):
    if request.ip in _proc.keys() and _proc[request.ip].pid:
        #log.info(f'_proc[{request.ip}].kill() {message}')
        try:
            _proc[request.ip].kill()
        except Exception as ex:
            log.info(f'_proc[{request.ip}].kill() EXCEPTED with {repr(ex)}')
        _proc.pop(request.ip, None)


if __name__ == '__main__':
    app.run(host=SANIC_HOST, port=SANIC_PORT, workers=1, debug=True, access_log=False)
