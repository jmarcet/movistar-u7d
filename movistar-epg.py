#!/usr/bin/env python3

import asyncio
import concurrent.futures
import glob
import json
import os
import time

from sanic import Sanic, response
from sanic.log import logger as log


HOME = os.environ.get('HOME') or '/home/'
SANIC_EPG_HOST = os.environ.get('SANIC_EPG_HOST') or '127.0.0.1'
SANIC_EPG_PORT = os.environ.get('SANIC_EPG_PORT') or 8889

CACHED_TIME = 7 * 24 * 60 * 60
EPG_CHANNELS = [ '1', '3', '4', '5', '6', '717', '477', '4911', '934', '3186', '884', '844', '3443', '1221', '3325', '744', 
                '2843', '657', '3603', '663', '935', '2863', '3184', '2', '578', '745', '743', '582', '597' ]

app = Sanic('Movistar_epg')
app.config.update({'KEEP_ALIVE': False})

_epgdata = {}
_epgdata['data'] = {}


@app.listener('after_server_start')
async def notify_server_start(app, loop):
    handle_reload_epg_task()

@app.get('/reload_epg')
async def handle_reload_epg(request):
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor() as pool:
        await loop.run_in_executor(pool, handle_reload_epg_task)
    return response.json({'status': 'EPG Updated'}, 200)

@app.get('/get_program_id/<channel_id>/<channel_key>/<url>')
async def handle_get_program_id(request, channel_id, channel_key, url):
    start = url.split('-')[1]
    duration = url.split('-')[2].split('.')[0]
    last_event = program_id = None
    offset = '0'

    if channel_key in _epgdata['data']:
        log.debug(f'Found channel {channel_key}')
        if start in _epgdata['data'][channel_key]:
            program_id = str(_epgdata['data'][channel_key][start]['pid'])
            end = str(_epgdata['data'][channel_key][start]['end'])
            duration = str(int(end) - int(start))
            log.info(f'Found: channel {channel_key} start {start} end {end} duration {duration}')
        else:
            for event in sorted(_epgdata['data'][channel_key].keys()):
                if int(event) > int(start):
                    break
                last_event = event
            if last_event:
                new_start = start
                start = last_event
                offset = str(int(new_start) - int(start))
                program_id = str(_epgdata['data'][channel_key][start]['pid'])
                end = str(_epgdata['data'][channel_key][start]['end'])
                duration = str(int(end) - int(start))
                log.info(f'Guessed: channel {channel_key} start {start} offset {offset} end {end} duration {duration}')

    if program_id:
        return response.json({'staus': 'OK', 'channel_id': channel_id, 'program_id': program_id, 'offset': offset})
    else:
        return response.json({'status': f'{channel_id}/{channel_key}/{url} not found'}, 404)

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
        try:
            with open(epg) as f:
                day_epg = json.loads(f.read())
        except json.decoder.JSONDecodeError:
            continue
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
        log.info(f'EPG entries {epg}: {nr_epg}')

    log.info('Total Channels=' + str(len(_epgdata['data'])))
    nr_epg = 0
    for chan in _epgdata['data'].keys():
        nr_epg += len(_epgdata['data'][chan].keys())
    log.info(f'EPG entries Total: {nr_epg} Expired: {expired}')

    with open(epg_cache, 'w') as f:
        f.write(json.dumps(_epgdata))

    log.info('EPG: Updated')


if __name__ == '__main__':
    app.run(host=SANIC_EPG_HOST, port=SANIC_EPG_PORT, access_log=False, auto_reload=True, debug=False, workers=1)
