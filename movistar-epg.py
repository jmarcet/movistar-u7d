#!/usr/bin/env python3

import asyncio
import httpx
import json
import os
import re
import time

from datetime import datetime
from sanic import Sanic, response
from sanic.compat import open_async
from sanic.log import logger as log
from sanic.log import LOGGING_CONFIG_DEFAULTS


HOME = os.getenv('HOME', '/home/')
SANIC_EPG_HOST = os.getenv('SANIC_EPG_HOST', '127.0.0.1')
SANIC_EPG_PORT = int(os.getenv('SANIC_EPG_PORT', '8889'))
SANIC_PORT = int(os.getenv('SANIC_PORT', '8888'))
SANIC_URL = f'http://{SANIC_EPG_HOST}:{SANIC_PORT}'
RECORDINGS = os.getenv('RECORDINGS')

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

PREFIX = ''

app = Sanic('Movistar_epg')
app.config.update({'KEEP_ALIVE_TIMEOUT': YEAR_SECONDS})

_channels = {}
_epgdata = {}
_t_epg1 = _t_epg2 = _t_timers = None


@app.get('/channel_address/<channel_id>/')
async def handle_channel_address(request, channel_id):
    log.debug(f'Searching Channel freqsuency: {channel_id}')

    if not channel_id in _channels:
        return response.json({'status': f'{channel_id} not found'}, 404)

    return response.json({'status': 'OK',
                          'address': _channels[channel_id]['address'],
                          'name': _channels[channel_id]['name'],
                          'port': _channels[channel_id]['port']})


@app.get('/next_program/<channel_id>/<program_id>')
async def handle_next_program(request, channel_id, program_id):
    log.info(f'Searching next EPG: /{channel_id}/{program_id}')

    if not channel_id in _channels:
        return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)

    channel_key = _channels[channel_id]['replacement'] \
        if 'replacement' in _channels[channel_id] else channel_id

    if not channel_key in _epgdata:
        return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)

    _found = False
    for event in sorted(_epgdata[channel_key]):
        _epg = _epgdata[channel_key][event]
        if _found:
            break
        elif _epgdata[channel_key][event]['pid'] == int(program_id):
            _found = True
            continue
    
    if not _found:
        return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)

    log.info(f'Found next EPG: /{channel_id}/' + str(_epg['pid']))
    return response.json({'status': 'OK',
                          'program_id': _epg['pid'],
                          'start': _epg['start'],
                          'duration': _epg['duration']}, 200)


@app.get('/program_id/<channel_id>/<url>')
async def handle_program_id(request, channel_id, url):
    log.debug(f'Searching EPG: /{channel_id}/{url}')

    x = re.search(r"\w*-?(\d{10})-?(\d+){0,1}\.\w+", url)
    if not x or not x.groups():
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    start = x.groups()[0]
    duration = int(x.groups()[1]) if x.groups()[1] else 0
    last_event = program_id = None
    offset = '0'

    if not channel_id in _channels:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    channel_key = _channels[channel_id]['replacement'] \
        if 'replacement' in _channels[channel_id] else channel_id

    if channel_key not in _epgdata or not _epgdata[channel_key]:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    channel_name = _channels[channel_id]['name']

    if start in _epgdata[channel_key]:
        program_id = str(_epgdata[channel_key][start]['pid'])
        end = str(_epgdata[channel_key][start]['end'])
        duration = str(int(end) - int(start))
        full_title = _epgdata[channel_key][start]['full_title']
        offset = '0'
    else:
        for event in sorted(_epgdata[channel_key]):
            if int(event) > int(start):
                break
            last_event = event
        if last_event:
            new_start, start = start, last_event
            offset = str(int(new_start) - int(start))
            program_id = str(_epgdata[channel_key][start]['pid'])
            end = str(_epgdata[channel_key][start]['end'])
            duration = str(int(end) - int(start))
            full_title = _epgdata[channel_key][start]['full_title']

    if not program_id:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    log.info(f'"{channel_name}" - "{full_title}" '
             f'{channel_id}/{channel_key} '
             f'{program_id} '
             f'{start} '
             f'[{offset}/{duration}]')
    return response.json({'status': 'OK', 'program_id': program_id, 'offset': offset})


@app.get('/program_name/<channel_id>/<program_id>')
async def handle_program_name(request, channel_id, program_id):
    if not channel_id in _channels:
        return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)

    channel_key = _channels[channel_id]['replacement'] \
        if 'replacement' in _channels[channel_id] else channel_id
    _found = False
    for event in sorted(_epgdata[channel_key]):
        _epg = _epgdata[channel_key][event]
        if int(program_id) == _epg['pid']:
            _found = True
            break
    
    if not _found:
        return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)

    return response.json({'status': 'OK',
                          'full_title': _epg['full_title'],
                          'duration': _epg['duration'],
                          'is_serie': _epg['is_serie'],
                          'serie': _epg['serie']
                          }, ensure_ascii=False)


@app.get('/reload_epg')
async def handle_reload_epg(request):
    return await handle_reload_epg_task()


async def handle_reload_epg_task():
    global _channels, _epgdata
    epg_data = os.path.join(HOME, '.xmltv/cache/epg.json')
    epg_metadata = os.path.join(HOME, '.xmltv/cache/epg_metadata.json')

    try:
        async with await open_async(epg_data) as f:
            epgdata = json.loads(await f.read())['data']
        _epgdata = epgdata
        log.info('Loaded fresh EPG data')
    except Exception as ex:
        log.error(f'Failed to load EPG data {repr(ex)}')
        if not _epgdata:
            raise

    try:
        async with await open_async(epg_metadata) as f:
            channels = json.loads(await f.read())['data']['channels']
        _channels = channels
        log.info('Loaded Channels metadata')
    except Exception as ex:
        log.error(f'Failed to load Channels metadata {repr(ex)}')
        if not _channels:
            raise

    log.info(f'Total Channels: {len(_epgdata)}')
    nr_epg = 0
    for channel in _epgdata:
        nr_epg += len(_epgdata[channel])
    log.info(f'Total EPG entries: {nr_epg}')
    log.info('EPG Updated')
    return response.json({'status': 'EPG Updated'}, 200)


async def handle_timers():
    log.info(f'handle_timers')

    _recordings = _timers = {}
    recordings = os.path.join(HOME, 'recordings.json')
    timers = os.path.join(HOME, 'timers.json')

    try:
        async with await open_async(timers) as f:
            _timers = json.loads(await f.read())
        async with await open_async(recordings) as f:
            _recordings = json.loads(await f.read())
    except Exception as ex:
        if not _timers:
            log.error(f'handle_timers: {ex} no timers, returning!')
            return
        log.info(f'handle_timers: no recordings saved yet')

    busy = False
    for channel in _timers['match']:
        if busy:
            break
        _key = _channels[channel]['replacement'] if \
            'replacement' in _channels[channel] else channel
        if _key not in _epgdata:
            log.info(f'Channel {channel} not found in EPG')
            continue
        for timestamp in sorted(_epgdata[_key]):
            if int(timestamp) > (int(datetime.now().timestamp()) - 900):
                break
            p = await asyncio.create_subprocess_exec('pgrep', '-f', 'ffmpeg.+udp://',
                                                     stdout=asyncio.subprocess.PIPE)
            stdout, _ = await p.communicate()
            nr_procs = len(stdout.split())
            if nr_procs > 4:
                log.info(f'Already recording {nr_procs} streams')
                busy = True
                break
            title = _epgdata[_key][timestamp]['full_title']
            for timer_match in _timers['match'][channel]:
                if re.match(timer_match, title) and \
                     (channel not in _recordings or
                      (title not in repr(_recordings[channel]) and
                      timestamp not in _recordings[channel])):
                    duration = _epgdata[_key][timestamp]['duration'] + 180
                    log.info(f'Found match! {channel} {timestamp} "{title}"')
                    sanic_url = f'{SANIC_URL}/{channel}/{timestamp}.mp4?record={duration}'
                    log.info(sanic_url)
                    async with httpx.AsyncClient() as client:
                        r = await client.get(sanic_url)
                    log.info(repr(r))
                    if r.status_code == 200:
                        if channel not in _recordings:
                            _recordings[channel] = {}
                        _recordings[channel][timestamp] = {
                            'full_title': title
                        }

    with open(recordings, 'w') as fp:
        json.dump(_recordings, fp, ensure_ascii=False, indent=4, sort_keys=True)


async def delay_update_epg():
    global _t_epg2
    delay = 3600 - (time.localtime().tm_min * 60 + time.localtime().tm_sec)
    log.info(f'Waiting for {delay}s to start updating EPG...')
    await asyncio.sleep(delay)
    _t_epg2 = asyncio.create_task(run_every(3600, handle_update_epg))


async def handle_update_epg():
    log.info(f'handle_update_epg')
    await asyncio.create_subprocess_exec('pkill', '-f', 'tv_grab_es_movistartv')
    for i in range(5):
        tvgrab = await asyncio.create_subprocess_exec(f'{PREFIX}tv_grab_es_movistartv',
                                                      '--tvheadend',
                                                      os.path.join(HOME, 'MovistarTV.m3u'),
                                                      '--output',
                                                      os.path.join(HOME, 'guide.xml'),
                                                      stdin=asyncio.subprocess.DEVNULL,
                                                      stdout=asyncio.subprocess.DEVNULL,
                                                      stderr=asyncio.subprocess.DEVNULL)
        await tvgrab.wait()
        if tvgrab.returncode != 0:
            log.error(f'Waiting 15s before trying again [{i+2}/5] to update EPG')
            await asyncio.sleep(15)
        else:
            await handle_reload_epg_task()
            break


@app.listener('after_server_start')
async def notify_server_start(app, loop):
    global PREFIX, _t_epg1, _t_timers
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    await handle_reload_epg_task()
    _t_epg1 = asyncio.create_task(delay_update_epg())
    if RECORDINGS:
        _t_timers = asyncio.create_task(run_every(900, handle_timers))


@app.listener('after_server_stop')
async def notify_server_stop(app, loop):
    for task in [_t_epg2, _t_epg1, _t_timers]:
        if task:
            task.cancel()


async def run_every(timeout, stuff):
    log.info(f'run_every {timeout} {stuff}')
    while True:
        await asyncio.gather(asyncio.sleep(timeout), stuff())


if __name__ == '__main__':
    app.run(host=SANIC_EPG_HOST,
            port=SANIC_EPG_PORT,
            access_log=False,
            auto_reload=True,
            debug=False,
            workers=1)
