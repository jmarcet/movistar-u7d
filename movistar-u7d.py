#!/usr/bin/env python3

import aiohttp
import asyncio
import asyncio_dgram
import concurrent.futures
import glob
import json
import logging
import os
import signal
import socket
import sys
import time

from contextlib import closing
from expiringdict import ExpiringDict
from sanic import Sanic, response
from sanic.log import logger


HOME = os.environ.get('HOME') or '/home/'
SANIC_HOST = os.environ.get('SANIC_HOST') or '127.0.0.1'
SANIC_PORT = os.environ.get('SANIC_PORT') or '8888'
UDPXY = os.environ.get('UDPXY') or 'http://192.168.137.1:4022/rtp/'

CACHED_TIME = 7 * 24 * 60 * 60
EPG_CHANNELS = [ '1', '3', '4', '5', '6', '717', '477', '4911', '934', '3186', '884', '844', '3443', '1221', '3325', '744', 
                '2843', '657', '3603', '663', '935', '2863', '3184', '2', '578', '745', '743', '582', '597' ]
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
_epgdata['data'] = {}
_lastprogram = {}
_proc = {}
_reqs = ExpiringDict(max_len=100, max_age_seconds=5)


@app.listener('after_server_start')
async def notify_server_start(app, loop):
    handle_reload_epg_task()

@app.listener('after_server_stop')
async def notify_server_stop(app, loop):
    log.debug(f'after_server_stop killing u7d.py')
    p = await asyncio.create_subprocess_exec('/usr/bin/pkill', '-INT', '-f', '/app/u7d.py .+ -p ')
    await p.wait()

@app.get('/reload_epg')
async def handle_reload_epg(request):
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor() as pool:
        await loop.run_in_executor(pool, handle_reload_epg_task)
    return response.json({'status': 'EPG Updated'}, 200)

def handle_reload_epg_task():
    global _epgdata
    epg_cache = '/home/epg.cache.json'
    epg_data = '/home/.xmltv/cache/epg.json'

    if os.path.exists(epg_cache) and os.stat(epg_cache).st_size > 100:
        log.info('Loading EPG cache')
        epgs = [ epg_cache ]
    else:
        log.info('Loading logrotate EPG backups')
        epgs = glob.glob('/home/epg.*.json')
    if os.path.exists(epg_data) and os.stat(epg_data).st_size > 100:
        log.info('Loading fresh EPG data')
        epgs.append(epg_data)

    deadline = int(time.time()) - CACHED_TIME
    expired = 0
    for epg in epgs:
        with open(epg) as f:
            day_epg = json.loads(f.read())
        channels = [ channel for channel in day_epg['data'].keys() if channel in EPG_CHANNELS ]
        for channel in channels:
            if not channel in _epgdata['data']:
                _epgdata['data'][channel] = {}
            for timestamp in day_epg['data'][channel]:
                if int(day_epg['data'][channel][timestamp]['end']) > deadline:
                    _epgdata['data'][channel][timestamp] = day_epg['data'][channel][timestamp]
                else:
                    expired += 1
        nr_epg = 0
        for chan in channels:
            nr_epg += len(day_epg['data'][chan].keys())
        log.debug(f'EPG entries {epg}: {nr_epg}')

    log.debug('Total Channels=' + str(len(_epgdata['data'])))
    nr_epg = 0
    for chan in _epgdata['data'].keys():
        nr_epg += len(_epgdata['data'][chan].keys())
    log.debug(f'EPG entries Total: {nr_epg} Expired: {expired}')

    with open(epg_cache, 'w') as f:
        f.write(json.dumps(_epgdata))
    log.info('EPG: Updated')

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
    global _lastprogram, _proc, _reqs

    log.info(f'Client: {request.ip}')

    # Nromal IPTV Channel
    if url.startswith('239'):
        await cleanup_proc(request, 'on channel change')
        if request.ip in _lastprogram.keys():
            _lastprogram.pop(request.ip, None)

        log.info(f'Redirect: {UDPXY + url}')
        return response.redirect(UDPXY + url)

    # Flussonic catchup style url
    elif url.startswith('video-'):
        start = url.split('-')[1]
        duration = url.split('-')[2].split('.')[0]
        last_event = None
        offset = '0'
        program_id = None

        if channel_key in _epgdata['data'] and start in _epgdata['data'][channel_key]:
            program_id = str(_epgdata['data'][channel_key][start]['pid'])
            _lastprogram[request.ip] = (channel_id, program_id, start)
        elif request.ip in _lastprogram.keys():
            new_start = start
            channel_id, program_id, start = _lastprogram[request.ip]
            offset = str(int(new_start) - int(start))
        elif channel_key in _epgdata['data']:
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

        req_id = f'{request.ip}:{channel_id}:{program_id}:{offset}'
        if req_id in _reqs:
            client_port = _reqs[req_id]
            log.info(f'Reusing: /app/u7d.py {channel_id} {program_id} -s {offset} -p {client_port}')
        else:
            await cleanup_proc(request, 'on calling u7d.py')

            with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
                s.bind(('', 0))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                client_port = str(s.getsockname()[1])

            log.info(f'Starting: /app/u7d.py {channel_id} {program_id} -s {offset} -p {client_port}')

            _proc[request.ip] = await asyncio.create_subprocess_exec('/app/u7d.py', channel_id, program_id,
                                                                     '-s', offset, '-p', client_port)
        _reqs[req_id] = client_port

        host = socket.gethostbyname(socket.gethostname())
        async def udp_streaming(response):
            log.info(f'Stream: @{host}:{client_port}')
            stream = await asyncio_dgram.bind((host, client_port))
            while True:
                data, remote_addr = await stream.recv()
                await response.write(data)

        return response.stream(udp_streaming, content_type=MIME)

    # Not recognized url
    else:
        if request.ip in _lastprogram.keys():
            _lastprogram.pop(request.ip, None)
        return response.json({'status': 'URL not understood'}, 404)

async def cleanup_proc(request, message):
    if request.ip in _proc.keys() and _proc[request.ip].pid:
        #log.debug(f'_proc[{request.ip}].send_signal() {message}')
        try:
            _proc[request.ip].send_signal(signal.SIGINT)
            _proc.pop(request.ip, None)
        except Exception as ex:
            log.info(f'_proc[{request.ip}].send_signal() {repr(ex)}')



if __name__ == '__main__':
    app.run(host=SANIC_HOST, port=int(SANIC_PORT), workers=1, debug=True, access_log=False)
