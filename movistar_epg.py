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
from sanic import Sanic, exceptions, response
from sanic.compat import open_async
from sanic.log import logger as log, LOGGING_CONFIG_DEFAULTS

from vod import TMP_EXT


setproctitle('movistar_epg')

HOME = os.getenv('HOME', '/home/')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
GUIDE = os.path.join(HOME, 'guide.xml')
SANIC_HOST = os.getenv('LAN_IP', '127.0.0.1')
SANIC_PORT = int(os.getenv('SANIC_PORT', '8888'))
SANIC_URL = f'http://{SANIC_HOST}:{SANIC_PORT}'
RECORDING_THREADS = int(os.getenv('RECORDING_THREADS', '4'))
RECORDINGS = os.getenv('RECORDINGS', None)

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

PREFIX = ''

app = Sanic('movistar_epg')
app.config.update({'FALLBACK_ERROR_FORMAT': 'json',
                   'KEEP_ALIVE_TIMEOUT': YEAR_SECONDS})
flussonic_regex = re.compile(r"\w*-?(\d{10})-?(\d+){0,1}\.\w+")

epg_data = os.path.join(HOME, '.xmltv/cache/epg.json')
epg_metadata = os.path.join(HOME, '.xmltv/cache/epg_metadata.json')
recordings = os.path.join(HOME, 'recordings.json')
timers = os.path.join(HOME, 'timers.json')
recordings_lock = asyncio.Lock()
timers_lock = asyncio.Lock()

_channels = {}
_epgdata = {}
_network_fsignal = '/tmp/.u7d_bw'
_t_epg1 = _t_epg2 = _t_timers = _t_timers_d = _t_timers_r = _t_timers_t = tvgrab = None


@app.listener('after_server_start')
async def after_server_start(app, loop):
    global PREFIX, _t_epg1, _t_timers_d
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    while not os.path.exists(CHANNELS):
        log.warning(f'No {CHANNELS} found. Generating it. Please be patient...')
        tvgrab = await asyncio.create_subprocess_exec(f'{PREFIX}tv_grab_es_movistartv',
                                                      '--m3u', CHANNELS)
        await tvgrab.wait()

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
            task.cancel()
            await asyncio.wait({task})
        except (AttributeError, ProcessLookupError):
            pass


async def every(timeout, stuff):
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
    filename = filename.replace(':', ',').replace('...', '???')
    keepcharacters = (' ', ',', '.', '_', '-', '??', '!')
    return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()


@app.get('/channels/')
async def handle_channels(request):
    return response.json(_channels)


@app.get('/program_id/<channel_id>/<url>')
async def handle_program_id(request, channel_id, url):
    try:
        channel_key = _channels[channel_id]['replacement'] \
            if 'replacement' in _channels[channel_id] else channel_id
        x = flussonic_regex.match(url)

        name = _channels[channel_id]['name']
        start = x.groups()[0]
        duration = int(x.groups()[1]) if x.groups()[1] else 0
        last_event = new_start = 0

        if start not in _epgdata[channel_key]:
            for event in _epgdata[channel_key]:
                if event > start:
                    break
                last_event = event
            if not last_event:
                raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')
            start, new_start = last_event, start
        program_id, end = [_epgdata[channel_key][start][t] for t in ['pid', 'end']]
        start = int(start)
        return response.json({'name': name, 'program_id': program_id,
                              'duration': duration if duration else int(end) - start,
                              'offset': int(new_start) - start if new_start else 0})
    except (AttributeError, KeyError) as ex:
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')


@app.route('/program_name/<channel_id>/<program_id>', methods=['GET', 'PUT'])
async def handle_program_name(request, channel_id, program_id):
    if channel_id not in _channels:
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')

    channel_key = _channels[channel_id]['replacement'] \
        if 'replacement' in _channels[channel_id] else channel_id
    _found = False
    for event in sorted(_epgdata[channel_key]):
        _epg = _epgdata[channel_key][event]
        if int(program_id) == _epg['pid']:
            _found = True
            break

    if not _found:
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')

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


async def reload_epg():
    global _channels, _epgdata

    if not os.path.exists(epg_data) or not os.path.exists(epg_metadata) \
            or not os.path.exists(GUIDE):
        log.warning(f'No {GUIDE} found!. Need to download it. Please be patient...')
        await update_epg()

    try:
        async with await open_async(epg_data) as f:
            epgdata = ujson.loads(await f.read())['data']
        _epgdata = epgdata
        log.info('Loaded fresh EPG data')
    except (FileNotFoundError, TypeError, ValueError) as ex:
        log.error(f'Failed to load EPG data {repr(ex)}')
        if os.path.exists(epg_data):
            os.remove(epg_data)
        return await reload_epg()

    try:
        async with await open_async(epg_metadata) as f:
            channels = ujson.loads(await f.read())['data']['channels']
        _channels = channels
        log.info('Loaded Channels metadata')
    except (FileNotFoundError, TypeError, ValueError) as ex:
        log.error(f'Failed to load Channels metadata {repr(ex)}')
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


@app.get('/timers_check')
async def handle_timers_check(request):
    global _t_timers_t
    if timers_lock.locked():
        return response.json({'status': 'Busy'}, 201)

    _t_timers_t = asyncio.create_task(timers_check())
    return response.json({'status': 'Timers check queued'}, 200)


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

        log.info('Processing timers')

        _recordings = _timers = {}
        try:
            async with await open_async(timers) as f:
                _timers = ujson.loads(await f.read())
            async with recordings_lock:
                async with await open_async(recordings) as f:
                    _recordings = ujson.loads(await f.read())
        except (FileNotFoundError, TypeError, ValueError) as ex:
            if not _timers:
                log.error(f'handle_timers: {repr(ex)}')
                return

        _ffmpeg = await get_ffmpeg_procs()
        nr_procs = len(_ffmpeg)
        if RECORDING_THREADS and not nr_procs < RECORDING_THREADS:
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
                    if re.match(timer_match, title) and (title not in timers_added and filename
                            not in str(_ffmpeg) and not os.path.exists(filename) and (channel
                            not in _recordings or (title not in repr(_recordings[channel])
                            and timestamp not in _recordings[channel]))):
                        log.info(f'Found match! {channel} {timestamp} {vo} "{title}"')
                        sanic_url = f'{SANIC_URL}/{channel}/{timestamp}.mp4'
                        sanic_url += '?record=1'
                        if vo:
                            sanic_url += '&vo=1'
                        try:
                            async with httpx.AsyncClient() as client:
                                r = await client.get(sanic_url)
                            log.info(f'{sanic_url} => {r}')
                            if r.status_code == 200:
                                timers_added.append(title)
                                nr_procs += 1
                                if RECORDING_THREADS and not nr_procs < RECORDING_THREADS:
                                    log.info(f'Already recording {nr_procs} streams')
                                    return
                                await asyncio.sleep(3)
                            elif r.status_code == 503:
                                return
                        except Exception as ex:
                            log.warning(f'{repr(ex)}')


async def timers_check_delayed():
    global RECORDING_THREADS, _t_timers
    log.info('Waiting 60s to check timers (ensuring no stale rtsp is present)...')
    await asyncio.sleep(60)
    if os.path.exists(_network_fsignal):
        log.info('Ignoring RECORDING_THREADS, using dynamic limit')
        RECORDING_THREADS = 0
    _t_timers = asyncio.create_task(every(900, timers_check))


async def update_epg():
    global tvgrab
    for i in range(5):
        tvgrab = await asyncio.create_subprocess_exec(f'{PREFIX}tv_grab_es_movistartv',
                                                      '--tvheadend', CHANNELS, '--output', GUIDE,
                                                      stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL)
        await tvgrab.wait()
        if tvgrab.returncode != 0:
            log.error(f'Waiting 15s before trying to update EPG again [{i+2}/5]')
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
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        log.critical(f'{repr(ex)}')
        sys.exit(1)
