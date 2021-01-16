#!/usr/bin/env python3

#from asyncio import sleep
from sanic import Sanic, response
from sanic.log import logger

#import asyncio
import logging
import os
import sys

CHANNELS = '/home/MovistarTV.m3u'
CHUNK = 65536
GUIDE = '/home/guide.xml'
HOME = os.environ.get('HOME') or '/home/'
MIME = 'video/MP2T'
MIN_SIZE = 10 * 1024 * 1024
#DAVFS = 'https://openwrt.marcet.info/tmp/test.ts'
SANIC_HOST = os.environ.get('SANIC_HOST') or '127.0.0.1'
SANIC_PORT = os.environ.get('SANIC_PORT') or '8888'
U7D_DIR = os.environ.get('U7D_DIR') or '/storage/timeshift/u7d/'
UDPXY = os.environ.get('UDPXY') or 'http://192.168.137.1:4022/rtp/'

app_settings = {
    'REQUEST_TIMEOUT': 60,
    'RESPONSE_TIMEOUT': 60,
    'KEEP_ALIVE_TIMEOUT': 1800,
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

testfile = os.path.join(U7D_DIR, 'test.ts')


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

@app.get('/rtp/<channel>/<url>')
async def handle_rtp(request, channel, url):
    try:
        if url.startswith('239'):
            log.info(f'Redirecting to {UDPXY + url}')
            #return response.json({'path': request.path, 'url': url, 'channel': channel}, 403)
            return response.redirect(UDPXY + url)
        elif url.startswith('video-'):
            start = url.split('-')[1]
            duration = url.split('-')[2].split('.')[0]
            dumpfile = os.path.join(U7D_DIR, channel + '-' + start + '-' + duration + '.ts')

            print(str({'path': request.path,
                        'url': url,
                    'channel': channel,
                      'start': start,
                   'duration': duration,
                   'dumpfile': dumpfile}))

            if os.path.exists(dumpfile) and os.stat(dumpfile).st_size >= MIN_SIZE:
                return await response.file_stream(dumpfile, chunk_size=CHUNK, mime_type=MIME)

            return await response.file_stream(testfile, chunk_size=CHUNK, mime_type=MIME)
        else:
            return response.json({'status': 'URL not understood'}, 302)
    except Exception as ex:
        return response.json({'status': 'URL not understood', 'exception': repr(ex)}, 400)

if __name__ == '__main__':
    app.run(host=SANIC_HOST, port=SANIC_PORT, workers=1, debug=True, access_log=True)
