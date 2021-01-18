#!/usr/bin/env python3

#from asyncio import sleep
from sanic import Sanic, response
from sanic.log import logger

import aiohttp
import asyncio
import glob
import json
import logging
import os
import pgrep
import sys

HOME = os.environ.get('HOME') or '/home/'
SANIC_HOST = os.environ.get('SANIC_HOST') or '127.0.0.1'
SANIC_PORT = os.environ.get('SANIC_PORT') or '8888'
UDPXY = os.environ.get('UDPXY') or 'http://192.168.137.1:4022/rtp/'
UDPXY_VOD = os.environ.get('UDPXY_VOD') or 'http://192.168.137.1:4022/udp/239.1.0.1:6667'

CHUNK = 65536
MIME = 'video/MP2T'
GUIDE = os.path.join(HOME, 'guide.xml')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')


EPG_DATA = {}
for epg in glob.glob('/home/epg.*.json'):
    day_epg = json.loads(open(epg).read())
    if EPG_DATA:
        EPG_DATA = {**EPG_DATA, **day_epg}
    else:
        EPG_DATA = day_epg

app_settings = {
    'REQUEST_TIMEOUT': 5,
    'RESPONSE_TIMEOUT': 5,
    'KEEP_ALIVE_TIMEOUT': 7200,
    'KEEP_ALIVE': True
}

logging_format = "[%(asctime)s] %(process)d-%(levelname)s "
logging_format += "%(module)s::%(funcName)s():l%(lineno)d: "
logging_format += "%(message)s"

logging.basicConfig(
    format=logging_format,
    level=logging.DEBUG
)
log = logging.getLogger()

app = Sanic('Movistar_u7d')
app.config.update(app_settings)

_lastprogram = ()
_proc = None


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
    global _lastprogram, _proc
    if url.startswith('239'):
        if pgrep.pgrep('socat'):
            p = await asyncio.create_subprocess_exec('/usr/bin/pkill', 'socat')
            await p.wait()
        _lastprogram = ()
        _proc = None
        log.info(f'Redirecting to {UDPXY + url}')
        return response.redirect(UDPXY + url)
    elif url.startswith('video-'):
        start = url.split('-')[1]
        duration = url.split('-')[2].split('.')[0]
        offset = '0'
        program_id = None

        if 'data' in EPG_DATA and channel_key in EPG_DATA['data'] and start in EPG_DATA['data'][channel_key]:
            program_id = str(EPG_DATA['data'][channel_key][start]['pid'])
            _lastprogram = (channel_id, program_id, start)
        elif _lastprogram:
            new_start = start
            channel_id, program_id, start = _lastprogram
            offset = str(int(new_start) - int(start))

        if not program_id:
            return response.json({'status': 'URL not understood'}, 404)

        log.info(f'Found program: channel={channel_id} program_id={program_id} offset={offset}')
        log.info(f'Need to exec: /app/u7d.py {channel_id} {program_id} --start {offset}')
        if _proc:
            p = await asyncio.create_subprocess_exec('/usr/bin/pkill', 'socat')
            await p.wait()
            try:
                _proc.kill()
            except Exception as ex:
                log.info(f'EXCEPTED 2 with {repr(ex)}')
        _proc = await asyncio.create_subprocess_exec('/app/u7d.py', channel_id, program_id, '--start', offset)

        #log.info(str({'path': request.path, 'url': url,
        #              'channel_id': channel_id, 'channel_key': channel_key,
        #              'start': start, 'offset': offset, 'duration': duration}))

        log.info(f'Streaming response from udpxy')
        async def sample_streaming_fn(response):
            global _proc
            async with aiohttp.ClientSession() as session:
                async with session.get(UDPXY_VOD) as resp:
                    log.info(f'Status from udpxy = {resp.status}')
                    while True:
                        if not (data := await resp.content.read(CHUNK)):
                            break
                        await response.write(data)
            if _proc:
                log.info(f'_proc.kill() sample_streaming_fn')
                try:
                    _proc.kill()
                except Exception as ex:
                    log.info(f'EXCEPTED 3 with {repr(ex)}')
                _proc = None
        return response.stream(sample_streaming_fn, content_type=MIME)
    else:
        _lastprogram = ()
        return response.json({'status': 'URL not understood'}, 404)

if __name__ == '__main__':
    app.run(host=SANIC_HOST, port=SANIC_PORT, workers=1, debug=True, access_log=True)
