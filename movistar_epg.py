#!/usr/bin/env python3

import asyncio
import httpx
import os
import re
import sys
import time
import ujson

from asyncio.subprocess import DEVNULL
from datetime import datetime
from glob import glob
from setproctitle import setproctitle
from sanic import Sanic, response
from sanic.compat import open_async
from sanic.log import logger as log, LOGGING_CONFIG_DEFAULTS
from vod import TMP_EXT


setproctitle('movistar_epg')

HOME = os.getenv('HOME', '/home/')
GUIDE = os.path.join(HOME, 'guide.xml')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
SANIC_HOST = os.getenv('LAN_IP', '127.0.0.1')
SANIC_PORT = int(os.getenv('SANIC_PORT', '8888'))
SANIC_URL = f'http://{SANIC_HOST}:{SANIC_PORT}'
PARALLEL_RECORDINGS = int(os.getenv('PARALLEL_RECORDINGS', '4'))
RECORDINGS = os.getenv('RECORDINGS', '/tmp')

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

PREFIX = ''

app = Sanic('movistar_epg')
app.config.update({'KEEP_ALIVE_TIMEOUT': YEAR_SECONDS})

epg_data = os.path.join(HOME, '.xmltv/cache/epg.json')
epg_metadata = os.path.join(HOME, '.xmltv/cache/epg_metadata.json')
recordings = os.path.join(HOME, 'recordings.json')
timers = os.path.join(HOME, 'timers.json')
recordings_lock = asyncio.Lock()
timers_lock = asyncio.Lock()

_channels = {}
_epgdata = {}
_t_epg1 = _t_epg2 = _t_timers = _t_timers_d = _t_timers_r = _t_timers_t = tvgrab = None


@app.listener('after_server_start')
async def after_server_start(app, loop):
    global PREFIX, _t_epg1, _t_timers_d
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    await reload_epg()
    _t_epg1 = asyncio.create_task(update_epg_delayed())

    if RECORDINGS:
        if os.path.exists(timers):
            _ffmpeg = str(await get_ffmpeg_procs())
            [os.remove(t) for t in glob(f'{RECORDINGS}/**/*{TMP_EXT}')
             if os.path.basename(t) not in _ffmpeg]
            _t_timers_d = asyncio.create_task(timers_check_delayed())
        else:
            log.info('No timers.json found, recordings disabled')


@app.listener('after_server_stop')
async def after_server_stop(app, loop):
    for task in [_t_epg2, _t_epg1, _t_timers, _t_timers_d, _t_timers_r, _t_timers_t, tvgrab]:
        try:
            task.kill()
            await task
        except (AttributeError, ProcessLookupError):
            pass


async def every(timeout, stuff):
    log.debug(f'run_every {timeout} {stuff}')
    while True:
        await asyncio.gather(asyncio.sleep(timeout), stuff())


async def get_ffmpeg_procs():
    p = await asyncio.create_subprocess_exec('pgrep', '-af', 'ffmpeg.+udp://',
                                             stdout=asyncio.subprocess.PIPE)
    stdout, _ = await p.communicate()
    return [t.rstrip().decode() for t in stdout.splitlines()]


def get_recording_path(channel_key, timestamp):
    if _epgdata[channel_key][timestamp]['is_serie']:
        path = os.path.join(RECORDINGS,
                            get_safe_filename(_epgdata[channel_key][timestamp]['serie']))
    else:
        path = RECORDINGS
    filename = os.path.join(path,
                            get_safe_filename(_epgdata[channel_key][timestamp]['full_title']))
    return (path, filename)


def get_safe_filename(filename):
    filename = filename.replace(':', ',').replace('...', '…')
    keepcharacters = (' ', ',', '.', '_', '-', '¡', '!')
    return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()


@app.get('/channel_address/<channel_id>/')
async def handle_channel_address(request, channel_id):
    log.debug(f'Searching channel address: {channel_id}')

    if channel_id not in _channels:
        return response.json({'status': f'{channel_id} not found'}, 404)

    address, name, port = [_channels[channel_id][t] for t in ['address', 'name', 'port']]
    return response.json({'status': 'OK', 'address': address, 'name': name, 'port': port})


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

    if channel_id not in _channels:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    channel_key = _channels[channel_id]['replacement'] \
        if 'replacement' in _channels[channel_id] else channel_id

    if channel_key not in _epgdata or not _epgdata[channel_key]:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)

    if start not in _epgdata[channel_key]:
        for event in sorted(_epgdata[channel_key]):
            if int(event) > int(start):
                break
            last_event = event
    if last_event:
        new_start, start = start, last_event
        offset = str(int(new_start) - int(start))
    elif start in _epgdata[channel_key]:
        offset = '0'
    else:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)
    program_id, end = [str(_epgdata[channel_key][start][t]) for t in ['pid', 'end']]
    duration = str(int(end) - int(start))

    name = _channels[channel_id]['name']
    # full_title = _epgdata[channel_key][start]['full_title']
    # log.info(f'"{name}/{full_title}" '
    #          f'{channel_id}/{channel_key} {program_id} {start} [{offset}/{duration}]')
    return response.json({'status': 'OK',
                          'name': name, 'program_id': program_id,
                          'duration': duration, 'offset': offset})


@app.route('/program_name/<channel_id>/<program_id>', methods=['GET', 'PUT'])
async def handle_program_name(request, channel_id, program_id):
    if channel_id not in _channels:
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

    if request.method == 'GET':
        (path, filename) = get_recording_path(channel_key, event)

        return response.json({'status': 'OK',
                              'full_title': _epg['full_title'],
                              'duration': _epg['duration'],
                              'path': path,
                              'filename': filename,
                              }, ensure_ascii=False)

    async with recordings_lock:
        try:
            async with await open_async(recordings) as f:
                _recordings = ujson.loads(await f.read())
        except (FileNotFoundError, TypeError, ValueError) as ex:
            _recordings = {}

    if channel_id not in _recordings:
        _recordings[channel_id] = {}
    _recordings[channel_id][program_id] = {
        'full_title': _epg['full_title']
    }

    async with recordings_lock:
        with open(recordings, 'w') as f:
            ujson.dump(_recordings, f, ensure_ascii=False, indent=4, sort_keys=True)

    global _t_timers_r
    _t_timers_r = asyncio.create_task(timers_check())

    log.info(f'Recording DONE: {channel_id} {program_id} "' + _epg['full_title'] + '"')
    return response.json({'status': 'Recorded OK', 'full_title': _epg['full_title'], },
                         ensure_ascii=False)


@app.get('/reload_epg')
async def handle_reload_epg(request):
    return await reload_epg()


@app.get('/check_timers')
async def handle_timers_check(request):
    global _t_timers_t
    if timers_lock.locked():
        return response.json({'status': 'Busy'}, 201)

    _t_timers_t = asyncio.create_task(timers_check())
    return response.json({'status': 'Timers check queued'}, 200)


async def reload_epg():
    global _channels, _epgdata

    if not os.path.exists(epg_data) or not os.path.exists(epg_metadata):
        log.warning('No EPG found!. Need to download it. Please be patient...')
        await update_epg()

    try:
        async with await open_async(epg_data) as f:
            epgdata = ujson.loads(await f.read())['data']
        _epgdata = epgdata
        log.info('Loaded fresh EPG data')
    except (FileNotFoundError, TypeError, ValueError) as ex:
        log.error(f'Failed to load EPG data {ex}')
        if os.path.exists(epg_data):
            os.remove(epg_data)
        return await reload_epg()

    try:
        async with await open_async(epg_metadata) as f:
            channels = ujson.loads(await f.read())['data']['channels']
        _channels = channels
        log.info('Loaded Channels metadata')
    except (FileNotFoundError, TypeError, ValueError) as ex:
        log.error(f'Failed to load Channels metadata {ex}')
        if os.path.exists(epg_metadata):
            os.remove(epg_metadata)
        return await reload_epg()

    log.info(f'Total Channels: {len(_epgdata)}')
    nr_epg = 0
    for channel in _epgdata:
        nr_epg += len(_epgdata[channel])
    log.info(f'Total EPG entries: {nr_epg}')
    log.info('EPG Updated')
    return response.json({'status': 'EPG Updated'}, 200)


async def timers_check():
    async with timers_lock:
        try:
            async with await open_async('/proc/uptime') as f:
                proc = await f.read()
            uptime = int(float(proc.split()[1]))
            if uptime < 300:
                log.info('Waiting 300s to check timers after rebooting...')
                await asyncio.sleep(300)
        except (FileNotFoundError, KeyError):
            pass

        log.info(f'Processing timers')

        _recordings = _timers = {}
        try:
            async with await open_async(timers) as f:
                _timers = ujson.loads(await f.read())
            async with recordings_lock:
                async with await open_async(recordings) as f:
                    _recordings = ujson.loads(await f.read())
        except (FileNotFoundError, TypeError, ValueError) as ex:
            if not _timers:
                log.error(f'handle_timers: {ex}')
                return

        _ffmpeg = await get_ffmpeg_procs()
        nr_procs = len(_ffmpeg)
        if not nr_procs < PARALLEL_RECORDINGS:
            log.info(f'Already recording {nr_procs} streams')
            return

        for channel in _timers['match']:
            _key = _channels[channel]['replacement'] if \
                'replacement' in _channels[channel] else channel
            if _key not in _epgdata:
                log.info(f'Channel {channel} not found in EPG')
                continue

            timers_added = []
            _time_limit = int(datetime.now().timestamp()) - (3600 * 3)
            for timestamp in reversed(_epgdata[_key]):
                if int(timestamp) > _time_limit:
                    continue
                title = _epgdata[_key][timestamp]['full_title']
                deflang = _timers['language']['default'] if (
                    'language' in _timers and 'default' in _timers['language']) else ''
                for timer_match in _timers['match'][channel]:
                    if ' ## ' in timer_match:
                            timer_match, lang = timer_match.split(' ## ')
                    else:
                        lang = deflang
                    vo = True if lang == 'VO' else False
                    (_, filename) = get_recording_path(_key, timestamp)
                    filename += TMP_EXT
                    if re.match(timer_match, title) and \
                        (title not in timers_added and
                            filename not in str(_ffmpeg) and
                            not os.path.exists(filename) and
                            (channel not in _recordings or
                             (title not in repr(_recordings[channel]) and
                              timestamp not in _recordings[channel]))):
                        log.info(f'Found match! {channel} {timestamp} {vo} "{title}"')
                        sanic_url = f'{SANIC_URL}/{channel}/{timestamp}.mp4'
                        sanic_url += f'?record=1'
                        if vo:
                            sanic_url += '&vo=1'
                        try:
                            async with httpx.AsyncClient() as client:
                                r = await client.get(sanic_url)
                            log.info(f'{sanic_url} => {r}')
                            if r.status_code == 200:
                                timers_added.append(title)
                                nr_procs += 1
                                if not nr_procs < PARALLEL_RECORDINGS:
                                    log.info(f'Already recording {nr_procs} streams')
                                    return
                        except Exception as ex:
                            log.warning(f'{ex}')


async def timers_check_delayed():
    global _t_timers
    log.info('Waiting 60s to check timers (ensuring no stale rtsp is present)...')
    await asyncio.sleep(60)
    _t_timers = asyncio.create_task(every(900, timers_check))


async def update_epg():
    global tvgrab
    log.info(f'update_epg')
    for i in range(5):
        tvgrab = await asyncio.create_subprocess_exec(f'{PREFIX}tv_grab_es_movistartv',
                                                      '--tvheadend', CHANNELS, '--output', GUIDE,
                                                      stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL)
        await tvgrab.wait()
        if tvgrab.returncode != 0:
            log.error(f'Waiting 15s before trying again [{i+2}/5] to update EPG')
            await asyncio.sleep(15)
        else:
            await reload_epg()
            break


async def update_epg_delayed():
    global _t_epg2
    delay = 3600 - (time.localtime().tm_min * 60 + time.localtime().tm_sec)
    log.info(f'Waiting {delay}s to start updating EPG...')
    await asyncio.sleep(delay)
    _t_epg2 = asyncio.create_task(every(3600, update_epg))


if __name__ == '__main__':
    try:
        app.run(host='127.0.0.1', port=8889,
                access_log=False, auto_reload=False, debug=False, workers=1)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as ex:
        log.critical(f'{ex}')
        sys.exit(1)
