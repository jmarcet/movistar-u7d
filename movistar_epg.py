#!/usr/bin/env python3

import asyncio
import httpx
import os
import re
import sys
import time
import ujson
import urllib.parse

from asyncio.subprocess import DEVNULL
from datetime import datetime
from glob import glob
from setproctitle import setproctitle
from sanic import Sanic, exceptions, response
from sanic_prometheus import monitor
from sanic.compat import open_async
from sanic.log import logger as log, LOGGING_CONFIG_DEFAULTS
from xml.sax.saxutils import unescape

from vod import MVTV_URL, TMP_EXT, VID_EXT


setproctitle('movistar_epg')

HOME = os.getenv('HOME', '/home/')
CHANNELS = os.path.join(HOME, 'MovistarTV.m3u')
CHANNELS_CLOUD = os.path.join(HOME, 'cloud.m3u')
CHANNELS_RECORDINGS = os.path.join(HOME, 'recordings.m3u')
GUIDE = os.path.join(HOME, 'guide.xml')
GUIDE_CLOUD = os.path.join(HOME, 'cloud.xml')
NETWORK_FSIGNAL = '/tmp/.u7d_bw'
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
_CHANNELS = _CLOUD = _EPGDATA = _RECORDINGS = {}

app = Sanic('movistar_epg')
app.config.update({'FALLBACK_ERROR_FORMAT': 'json',
                   'KEEP_ALIVE_TIMEOUT': YEAR_SECONDS})
flussonic_regex = re.compile(r"\w*-?(\d{10})-?(\d+){0,1}\.?\w*")
title_select_regex = re.compile(r".+ T\d+ .+")
title_1_regex = re.compile(r"(.+(?!T\d)) +T(\d+)(?: *Ep?.? *(\d+))?[ -]*(.*)")
title_2_regex = re.compile(r"(.+(?!T\d))(?: +T(\d+))? *Ep?.? *(\d+)[ -]*(.*)")

cloud_data = os.path.join(HOME, '.xmltv/cache/cloud.json')
epg_data = os.path.join(HOME, '.xmltv/cache/epg.json')
epg_metadata = os.path.join(HOME, '.xmltv/cache/epg_metadata.json')
recordings = os.path.join(HOME, 'recordings.json')
timers = os.path.join(HOME, 'timers.json')

epg_lock = asyncio.Lock()
timers_lock = asyncio.Lock()
tvgrab_lock = asyncio.Lock()

_t_cloud1 = _t_cloud2 = _t_epg1 = _t_epg2 = _t_timers = _t_timers_d = \
    _t_timers_r = _t_timers_t = tv_cloud1 = tv_cloud2 = tvgrab = None


@app.listener('after_server_start')
async def after_server_start(app, loop):
    global PREFIX, _RECORDINGS, _t_cloud1, _t_epg1, _t_timers_d
    if __file__.startswith('/app/'):
        PREFIX = '/app/'

    await reload_epg()
    _t_epg1 = asyncio.create_task(update_epg_delayed())
    _t_cloud1 = asyncio.create_task(update_cloud_delayed())
    if RECORDINGS:
        await update_recordings_m3u()
        try:
            async with await open_async(recordings) as f:
                recordingsdata = ujson.loads(await f.read())
            int_recordings = {}
            for channel in recordingsdata:
                int_recordings[int(channel)] = {}
                for event in recordingsdata[channel]:
                    int_recordings[int(channel)][int(event)] = \
                        recordingsdata[channel][event]
            _RECORDINGS = int_recordings
        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f'{repr(ex)}')

        if os.path.exists(timers):
            _ffmpeg = str(await get_ffmpeg_procs())
            [os.remove(t) for t in glob(f'{RECORDINGS}/**/*{TMP_EXT}', recursive=True)
             if os.path.basename(t) not in _ffmpeg]
            _t_timers_d = asyncio.create_task(timers_check_delayed())
        else:
            log.info('No timers.json found, recordings disabled')


@app.listener('after_server_stop')
async def after_server_stop(app, loop):
    for task in [
            _t_cloud1, _t_cloud2, _t_epg2, _t_epg1, _t_timers, _t_timers_d,
            _t_timers_r, _t_timers_t, tv_cloud1, tv_cloud2, tvgrab]:
        try:
            task.cancel()
            await asyncio.wait({task})
        except (AttributeError, ProcessLookupError):
            pass


async def every(timeout, stuff):
    while True:
        await asyncio.gather(asyncio.sleep(timeout), stuff())


def get_epg(channel_id, program_id):
    if channel_id not in _CHANNELS:
        log.error(f'{channel_id} not found')
        return

    for event in sorted(_EPGDATA[channel_id]):
        _epg = _EPGDATA[channel_id][event]
        if program_id == _epg['pid']:
            return _epg, event


async def get_ffmpeg_procs():
    p = await asyncio.create_subprocess_exec(
        'pgrep', '-af', 'ffmpeg.+udp://', stdout=asyncio.subprocess.PIPE)
    stdout, _ = await p.communicate()
    return [t.rstrip().decode() for t in stdout.splitlines()]


def get_program_id(channel_id, url=None, cloud=False):
    if not url:
        url = f'{int(time.time())}'
    x = flussonic_regex.match(url)

    name = _CHANNELS[channel_id]['name']
    start = int(x.groups()[0])
    duration = int(x.groups()[1]) if x.groups()[1] else 0
    last_event = new_start = 0

    if not cloud:
        if start not in _EPGDATA[channel_id]:
            for event in _EPGDATA[channel_id]:
                if event > start:
                    break
                last_event = event
            if not last_event:
                return
            start, new_start = last_event, start
        program_id, end = [_EPGDATA[channel_id][start][t] for t in ['pid', 'end']]
    else:
        if channel_id not in _CLOUD:
            return
        if start not in _CLOUD[channel_id]:
            for event in _CLOUD[channel_id]:
                if start > event and start < event + duration:
                    start, new_start = event, start
            if not new_start:
                return
        program_id, end = [_CLOUD[channel_id][start][t] for t in ['pid', 'end']]

    return {'name': name, 'program_id': program_id,
            'duration': duration if duration else end - start,
            'offset': new_start - start if new_start else 0}


def get_recording_path(channel_id, timestamp):
    if _EPGDATA[channel_id][timestamp]['serie']:
        path = os.path.join(
            RECORDINGS, get_safe_filename(_EPGDATA[channel_id][timestamp]['serie']))
    else:
        path = RECORDINGS

    filename = os.path.join(
        path, get_safe_filename(_EPGDATA[channel_id][timestamp]['full_title']))

    return (path, filename)


def get_safe_filename(filename):
    filename = filename.replace(':', ',').replace('...', '…')
    keepcharacters = (' ', ',', '.', '_', '-', '¡', '!')
    return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()


def get_title_meta(title, serie_id=None):
    try:
        _t = unescape(title).replace('\n', ' ').replace('\r', ' ')
    except TypeError:
        _t = title.replace('\n', ' ').replace('\r', ' ')
    full_title = _t.replace(' / ', ': ').replace('/', '')

    if title_select_regex.match(full_title):
        x = title_1_regex.search(full_title)
    else:
        x = title_2_regex.search(full_title)

    is_serie = False
    season = episode = 0
    serie = episode_title = ''
    if x and x.groups():
        _x = x.groups()
        serie = _x[0].strip() if _x[0] else \
            full_title.split(' - ')[0].strip() if ' - ' in full_title else full_title
        season = int(_x[1]) if _x[1] else season
        episode = int(_x[2]) if _x[2] else episode
        episode_title = _x[3].strip() if _x[3] else episode_title
        if episode_title and '-' in episode_title:
            episode_title = \
                episode_title.replace('- ', '-').replace(' -', '-').replace('-', ' - ')
        is_serie = True if (serie and episode) else False
        if ': ' in serie and episode and not episode_title:
            episode_title = serie.split(':')[1].strip()
            serie = serie.split(':')[0].strip()
        if serie and season and episode:
            full_title = '%s S%02dE%02d' % (serie, season, episode)
            if episode_title:
                full_title += f' - {episode_title}'
    elif serie_id:
        is_serie = True
        if ' - ' in full_title:
            serie, episode_title = full_title.split(' - ', 1)
        else:
            serie = full_title

    return {'full_title': full_title, 'serie': serie, 'season': season,
            'episode': episode, 'episode_title': episode_title,
            'is_serie': is_serie}


@app.get('/channels/')
async def handle_channels(request):
    return response.json(_CHANNELS)


@app.get('/program_id/<channel_id:int>/<url>')
async def handle_program_id(request, channel_id, url):
    try:
        return response.json(get_program_id(channel_id, url, bool(request.args.get('cloud'))))
    except (AttributeError, KeyError):
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')


@app.route('/program_name/<channel_id:int>/<program_id:int>', methods=['GET', 'PUT'])
async def handle_program_name(request, channel_id, program_id):
    global _RECORDINGS
    try:
        _epg, event = get_epg(channel_id, program_id)
    except TypeError:
        raise exceptions.NotFound(f'Requested URL {request.raw_url.decode()} not found')

    if request.method == 'GET':
        path, filename = get_recording_path(channel_id, event)

        return response.json({'status': 'OK',
                              'full_title': _epg['full_title'],
                              'path': path,
                              'filename': filename,
                              }, ensure_ascii=False)

    if channel_id not in _RECORDINGS:
        _RECORDINGS[channel_id] = {}
    _RECORDINGS[channel_id][program_id] = {'full_title': _epg['full_title']}

    with open(recordings, 'w') as f:
        ujson.dump(_RECORDINGS, f, ensure_ascii=False, indent=4, sort_keys=True)

    await update_recordings_m3u()

    global _t_timers_r
    _t_timers_r = asyncio.create_task(timers_check())

    log.info(f'Recording DONE: {channel_id} {program_id} "' + _epg['full_title'] + '"')
    return response.json({'status': 'Recorded OK', 'full_title': _epg['full_title'], },
                         ensure_ascii=False)


@app.post('/prom_event/add')
async def handle_prom_event_add(request):
    try:
        _event = get_program_id(
            request.json['channel_id'],
            request.json['url'] if 'url' in request.json else None,
            request.json['cloud'] if 'cloud' in request.json else False)
        _epg, _, = get_epg(request.json['channel_id'], _event['program_id'])
        _offset = '[%d/%d]' % (_event['offset'], _event['duration'])
        request.app.metrics['RQS_LATENCY'].labels(
            request.json['method'],
            request.json['endpoint'] + _epg['full_title'] + f' _ {_offset}',
            request.json['id']).observe(float(request.json['lat']))
        _msg = request.json['msg'] + '"' + _epg['full_title'] + '"'
        if request.json['method'] == 'live':
            _msg += f' _ {_offset}'
        log.info(_msg)
    except KeyError:
        return response.empty(404)
    return response.empty(200)


@app.post('/prom_event/remove')
async def handle_prom_event_remove(request):
    try:
        _event = get_program_id(
            request.json['channel_id'],
            request.json['url'] if 'url' in request.json else None,
            request.json['cloud'] if 'cloud' in request.json else False)
        _epg, _, = get_epg(request.json['channel_id'], _event['program_id'])
        _msg = request.json['msg'] + '"' + _epg['full_title'] + '"'
        _offset = '[%d/%d]' % (_event['offset'], _event['duration'])
        if request.json['method'] == 'live':
            found = False
            for _metric in request.app.metrics['RQS_LATENCY']._metrics:
                if request.json['method'] in _metric and str(request.json['id']) in _metric:
                    found = True
                    break
            if found:
                request.app.metrics['RQS_LATENCY'].remove(*_metric)
        else:
            request.app.metrics['RQS_LATENCY'].remove(
                request.json['method'],
                request.json['endpoint'] + _epg['full_title'] + f' _ {_offset}',
                request.json['id'])
            _offset = '[%d/%d]' % (_event['offset'] + request.json['offset'], _event['duration'])
        log.info(f'{_msg} -> {_offset}')
    except KeyError:
        return response.empty(404)
    return response.empty(200)


@app.get('/reload_epg')
async def handle_reload_epg(request):
    return await reload_epg()


async def reload_epg():
    global _CHANNELS, _CLOUD, _EPGDATA

    if not os.path.exists(epg_data) or not os.path.exists(epg_metadata) \
            or not os.path.exists(CHANNELS) or not os.path.exists(GUIDE):
        log.warning(f'Missing channels data!. Need to download it. Please be patient...')
        await update_epg()

    async with epg_lock:
        try:
            async with await open_async(epg_data) as f:
                epgdata = ujson.loads(await f.read())['data']
            int_epgdata = {}
            for channel in epgdata:
                int_epgdata[int(channel)] = {}
                for event in epgdata[channel]:
                    int_epgdata[int(channel)][int(event)] = epgdata[channel][event]
            _EPGDATA = int_epgdata
            log.info('Loaded fresh EPG data')
        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f'Failed to load EPG data {repr(ex)}')
            if os.path.exists(epg_data):
                os.remove(epg_data)
            return await reload_epg()

        try:
            async with await open_async(epg_metadata) as f:
                channels = ujson.loads(await f.read())['data']['channels']
            int_channels = {}
            for channel in channels:
                int_channels[int(channel)] = channels[channel]
                int_channels[int(channel)]['id'] = int(channels[channel]['id'])
                if 'replacement' in channels[channel]:
                    int_channels[int(channel)]['replacement'] \
                        = int(channels[channel]['replacement'])
            _CHANNELS = int_channels
            log.info('Loaded Channels metadata')
        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f'Failed to load Channels metadata {repr(ex)}')
            if os.path.exists(epg_metadata):
                os.remove(epg_metadata)
            return await reload_epg()

        await update_cloud(forced=True)

        log.info(f'Total Channels: {len(_EPGDATA)}')
        nr_epg = 0
        for channel in _EPGDATA:
            nr_epg += len(_EPGDATA[channel])
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

        _timers = {}
        try:
            async with await open_async(timers) as f:
                _timers = ujson.loads(await f.read())
        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f'handle_timers: {repr(ex)}')
            return

        _ffmpeg = await get_ffmpeg_procs()
        nr_procs = len(_ffmpeg)
        if RECORDING_THREADS and not nr_procs < RECORDING_THREADS:
            log.info(f'Already recording {nr_procs} streams')
            return

        for channel_id in _timers['match']:
            channel_id = int(channel_id)
            if channel_id not in _EPGDATA:
                log.info(f'Channel {channel_id} not found in EPG')
                continue

            timers_added = []
            _time_limit = int(datetime.now().timestamp()) - (3600 * 3)
            for timestamp in reversed(_EPGDATA[channel_id]):
                if timestamp > _time_limit:
                    continue
                title = _EPGDATA[channel_id][timestamp]['full_title']
                deflang = _timers['language']['default'] if (
                    'language' in _timers and 'default' in _timers['language']) else ''
                for timer_match in _timers['match'][str(channel_id)]:
                    if ' ## ' in timer_match:
                        timer_match, lang = timer_match.split(' ## ')
                    else:
                        lang = deflang
                    vo = True if lang == 'VO' else False
                    _, filename = get_recording_path(channel_id, timestamp)
                    if re.match(timer_match, title) and (
                            title not in timers_added and filename.split('/')[-1:][0] not in
                            str(_ffmpeg)):
                        if channel_id in _RECORDINGS:
                            if (title in str(_RECORDINGS[channel_id]) or timestamp
                                in _RECORDINGS[channel_id]) \
                                    and os.path.exists(filename + VID_EXT):
                                continue
                        log.info(f'Found match! {filename + VID_EXT} {channel_id} {timestamp}'
                                 f'{" [VO]" if vo else ""}')
                        sanic_url = f'{SANIC_URL}'
                        if channel_id in _CLOUD and timestamp in _CLOUD[channel_id]:
                            sanic_url += '/cloud'
                        sanic_url += f'/{channel_id}/{timestamp}.mp4?record=1'
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
    if os.path.exists(NETWORK_FSIGNAL):
        log.info('Ignoring RECORDING_THREADS, using dynamic limit')
        RECORDING_THREADS = 0
    _t_timers = asyncio.create_task(every(900, timers_check))


async def update_cloud(forced=False):
    global cloud_data, tv_cloud1, tv_cloud2, _CLOUD, _EPGDATA

    try:
        async with await open_async(cloud_data) as f:
            clouddata = ujson.loads(await f.read())['data']
        int_clouddata = {}
        for channel in clouddata:
            int_clouddata[int(channel)] = {}
            for event in clouddata[channel]:
                int_clouddata[int(channel)][int(event)] = clouddata[channel][event]
        _CLOUD = int_clouddata
    except (FileNotFoundError, TypeError, ValueError):
        if os.path.exists(cloud_data):
            os.remove(cloud_data)

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f'{MVTV_URL}?action=recordingList&mode=0&state=2&firstItem=0&numItems=999')
            cloud_recordings = r.json()['resultData']['result']
        except KeyError:
            cloud_recordings = None
    if not cloud_recordings:
        log.info('No cloud recordings found')
        return

    new_cloud = {}
    for event in cloud_recordings:
        channel_id = event['serviceUID']
        _start = int(event['beginTime'] / 1000)
        if channel_id not in new_cloud:
            new_cloud[channel_id] = {}
        if _start not in new_cloud[channel_id]:
            if channel_id in _EPGDATA and _start in _EPGDATA[channel_id]:
                new_cloud[channel_id][_start] = _EPGDATA[channel_id][_start]
            elif channel_id in _CLOUD and _start in _CLOUD[channel_id]:
                new_cloud[channel_id][_start] = _CLOUD[channel_id][_start]
            else:
                product_id = event['productID']
                try:
                    async with httpx.AsyncClient() as client:
                        r = await client.get(
                            f'{MVTV_URL}?action=epgInfov2&'
                            f'productID={product_id}&channelID={channel_id}')
                        year = r.json()['resultData']['productionDate']
                        r = await client.get(
                            f'{MVTV_URL}?action=getRecordingData&'
                            f'extInfoID={product_id}&channelID={channel_id}&mode=1')
                        _data = r.json()['resultData']
                        meta_data = get_title_meta(
                            _data['name'],
                            _data['seriesID'] if 'seriesID' in _data else None)
                        new_cloud[channel_id][_start] = {
                            'age_rating': _data['ageRatingID'],
                            'duration': _data['duration'],
                            'end': int(str(_data['endTime'])[:-3]),
                            'episode': meta_data['episode']
                            if meta_data['episode'] else _data['episode'],
                            'full_title': meta_data['full_title'],
                            'genre': _data['theme'],
                            'is_serie': meta_data['is_serie'],
                            'pid': product_id,
                            'season': meta_data['season']
                            if meta_data['season'] else _data['season'],
                            'start': int(_start),
                            'year': year,
                            'serie': meta_data['serie']
                            if meta_data['serie'] else _data['seriesName'],
                            'serie_id': _data['seriesID'] if 'seriesID' in _data else None,
                        }
                        if meta_data['episode_title']:
                            new_cloud[channel_id][_start]['episode_title'] = \
                                meta_data['episode_title']
                except Exception as ex:
                    log.warning(
                        f'{channel_id} {product_id} not located in Cloud {repr(ex)}')
    updated = False
    if new_cloud and (not _CLOUD or set(new_cloud) != set(_CLOUD)):
        updated = True
    else:
        for id in new_cloud:
            if set(new_cloud[id]) != set(_CLOUD[id]):
                updated = True
                break

    if updated or forced:
        def merge():
            global _CLOUD
            for channel_id in new_cloud:
                for event in list(set(new_cloud[channel_id]) - set(_EPGDATA[channel_id])):
                    _EPGDATA[channel_id][event] = new_cloud[channel_id][event]
            if not forced:
                for channel_id in _CLOUD:
                    for event in list(set(_CLOUD[channel_id]) - set(new_cloud[channel_id])):
                        if event in _EPGDATA[channel_id]:
                            del _EPGDATA[channel_id][event]
        if forced:
            merge()
        else:
            async with epg_lock:
                merge()

    if updated:
        _CLOUD = new_cloud
        with open(cloud_data, 'w') as fp:
            ujson.dump({'data': _CLOUD}, fp, ensure_ascii=False, indent=6, sort_keys=True)
        log.info('Updated Cloud Recordings data')

    if updated or not os.path.exists(CHANNELS_CLOUD) or not os.path.exists(GUIDE_CLOUD):
        tv_cloud1 = await asyncio.create_subprocess_exec(
            f'{PREFIX}tv_grab_es_movistartv', '--cloud_m3u', CHANNELS_CLOUD)
        async with tvgrab_lock:
            tv_cloud2 = await asyncio.create_subprocess_exec(
                f'{PREFIX}tv_grab_es_movistartv', '--cloud_recordings', GUIDE_CLOUD)
    if forced and not updated:
        log.info('Loaded Cloud Recordings data')


async def update_cloud_delayed():
    global _t_cloud2
    await asyncio.sleep(300)
    _t_cloud2 = asyncio.create_task(every(300, update_cloud))


async def update_epg():
    global tvgrab
    for i in range(5):
        async with tvgrab_lock:
            tvgrab = await asyncio.create_subprocess_exec(
                f'{PREFIX}tv_grab_es_movistartv', '--tvheadend', CHANNELS, '--output', GUIDE,
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


async def update_recordings_m3u():
    m3u = '#EXTM3U name="Recordings" dlna_extras=mpeg_ps_pal\n'

    def dump_files(m3u, files, latest=False):
        for pair in [tuple(file.split(RECORDINGS)[1].split('/')[1:]) for file in files]:
            (path, file) = pair if len(pair) == 2 else (None, pair[0])
            name, ext = os.path.splitext(file)
            _file = f'{(path + "/") if path else ""}{name}'
            if os.path.exists(os.path.join(RECORDINGS, f'{_file}.jpg')):
                logo = f'{_file}.jpg'
            else:
                _logo = glob(os.path.join(RECORDINGS, f'{_file}*.jpg'))
                if len(_logo) and os.path.isfile(_logo[0]):
                    logo = f'{_logo[0].split(RECORDINGS)[1][1:]}'
                else:
                    logo = ''
            m3u += '#EXTINF:-1 tvg-id=""'
            m3u += f' tvg-logo="{SANIC_URL}/recording/?' if logo else ''
            m3u += (urllib.parse.quote(f'{logo}') + '"') if logo else ''
            m3u += ' group-title="'
            m3u += '# Recientes' if latest else path if path else "#"
            m3u += f'",{name}\n{SANIC_URL}/recording/?'
            m3u += urllib.parse.quote(_file + ext) + '\n'
        return m3u

    files = [file for file in glob(f'{RECORDINGS}/**', recursive=True)
             if os.path.splitext(file)[1] in
             ('.avi', '.mkv', '.mp4', '.mpeg', '.mpg', '.ts')]

    files.sort(key=os.path.getmtime, reverse=True)
    m3u = dump_files(m3u, files, latest=True)
    m3u = dump_files(m3u, sorted(files))

    log.info('Local Recordings Updated')
    with open(CHANNELS_RECORDINGS, 'w') as f:
        f.write(m3u)


if __name__ == '__main__':
    try:
        monitor(app, is_middleware=False, latency_buckets=[1.0], mmc_period_sec=None,
                multiprocess_mode='livesum').expose_endpoint()
        app.run(host='127.0.0.1', port=8889,
                access_log=False, auto_reload=False, debug=False, workers=1)
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        log.critical(f'{repr(ex)}')
        sys.exit(1)
