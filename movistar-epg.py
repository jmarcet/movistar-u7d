#!/usr/bin/env python3

import asyncio
import json
import os

from sanic import Sanic, response
from sanic.compat import open_async
from sanic.log import logger as log
from sanic.log import LOGGING_CONFIG_DEFAULTS


HOME = os.getenv('HOME', '/home/')
SANIC_EPG_HOST = os.getenv('SANIC_EPG_HOST', '127.0.0.1')
SANIC_EPG_PORT = int(os.getenv('SANIC_EPG_PORT', '8889'))

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS['formatters']['generic']['datefmt'] = \
    LOG_SETTINGS['formatters']['access']['datefmt'] = '[%Y-%m-%d %H:%M:%S]'

app = Sanic('Movistar_epg')
app.config.update({'KEEP_ALIVE_TIMEOUT': YEAR_SECONDS})

_channels = {}
_epgdata = {}


@app.listener('after_server_start')
async def notify_server_start(app, loop):
    await handle_reload_epg_task()


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
        log.error(f'Failed to EPG data {repr(ex)}')
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


@app.get('/get_next_program/<channel_id>/<program_id>')
async def handle_get_next_program(request, channel_id, program_id):
    log.info(f'Searching next EPG: /{channel_id}/{program_id}')
    if channel_id in _channels:
        channel_key = _channels[channel_id]['replacement'] \
            if 'replacement' in _channels[channel_id] else channel_id
        if channel_key in _epgdata:
            found = False
            for event in sorted(_epgdata[channel_key]):
                _epg = _epgdata[channel_key][event]
                if found:
                    log.info(f'Found next EPG: /{channel_id}/' + str(_epg['pid']))
                    return response.json({'status': 'OK',
                                          'program_id': _epg['pid'],
                                          'start': _epg['start'],
                                          'duration': _epg['duration']}, 200)
                elif _epgdata[channel_key][event]['pid'] == int(program_id):
                    found = True
                    continue
    return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)


@app.get('/get_channel_address/<channel_id>/')
async def handle_get_channel_address(request, channel_id):
    log.debug(f'Searching Channel freqsuency: {channel_id}')
    if channel_id in _channels:
        return response.json({'status': 'OK',
                              'channel_id': channel_id,
                              'address': _channels[channel_id]['address'],
                              'port': _channels[channel_id]['port']})


@app.get('/get_program_id/<channel_id>/<url>')
async def handle_get_program_id(request, channel_id, url):
    start = url.split('-')[1]
    duration = url.split('-')[2].split('.')[0]
    last_event = program_id = None
    offset = '0'

    log.debug(f'Searching EPG: /{channel_id}/{url}')
    if channel_id in _channels:
        channel_key = _channels[channel_id]['replacement'] \
            if 'replacement' in _channels[channel_id] else channel_id
        if channel_key in _epgdata:
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
                    new_start = start
                    start = last_event
                    offset = str(int(new_start) - int(start))
                    program_id = str(_epgdata[channel_key][start]['pid'])
                    end = str(_epgdata[channel_key][start]['end'])
                    duration = str(int(end) - int(start))
                    full_title = _epgdata[channel_key][start]['full_title']
            log.info(f'"{full_title}" '
                     f'{channel_id}/{channel_key} '
                     f'{program_id} '
                     f'{start} '
                     f'[{offset}/{duration}]')

    if program_id:
        return response.json({'status': 'OK',
                              'channel_id': channel_id,
                              'program_id': program_id,
                              'offset': offset})
    else:
        return response.json({'status': f'{channel_id}/{url} not found'}, 404)


@app.get('/get_program_name/<channel_id>/<program_id>')
async def handle_get_program_name(request, channel_id, program_id):
    if channel_id in _channels:
        channel_key = _channels[channel_id]['replacement'] \
            if 'replacement' in _channels[channel_id] else channel_id
        for event in sorted(_epgdata[channel_key]):
            _epg = _epgdata[channel_key][event]
            if int(program_id) == _epg['pid']:
                return response.json({'status': 'OK',
                                      'full_title': _epg['full_title'],
                                      'duration': _epg['duration'],
                                      'is_serie': _epg['is_serie'],
                                      'serie': _epg['serie']
                                      }, ensure_ascii=False)
    return response.json({'status': f'{channel_id}/{program_id} not found'}, 404)


if __name__ == '__main__':
    app.run(host=SANIC_EPG_HOST,
            port=SANIC_EPG_PORT,
            access_log=False,
            auto_reload=True,
            debug=False,
            workers=1)
