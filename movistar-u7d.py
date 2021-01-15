#!/usr/bin/env python3

#from asyncio import sleep
from sanic import Sanic
from sanic.log import logger
from sanic.response import file_stream, html, json, redirect, stream, text

#import asyncio
import logging
import os

app_settings = {
    'REQUEST_TIMEOUT': 900,
    'RESPONSE_TIMEOUT': 900,
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

CHUNK = 65536
MIME = 'video/MP2T'
MIN_SIZE = 10 * 1024 * 1024
#DAVFS = 'https://openwrt.marcet.info/tmp/test.ts'
U7D = '/storage/timeshift/u7d/'
UDPXY = 'http://192.168.137.1:4022/rtp/'

app = Sanic('Movistar_u7d')
app.config.update(app_settings)

testfile = U7D + 'test.ts'


@app.get('/rtp/<channel>/<url>')
async def handle_rtp(request, channel, url):
    try:
        if url.startswith('239'):
            log.info(f'Redirecting to {UDPXY + url}')
            #return json({'path': request.path, 'url': url, 'channel': channel}, 403)
            return redirect(UDPXY + url)
        elif url.startswith('video-'):
            start = url.split('-')[1]
            duration = url.split('-')[2].split('.')[0]
            dumpfile = U7D + channel + '-' + start + '-' + duration + '.ts'

            print(str({'path': request.path,
                        'url': url,
                    'channel': channel,
                      'start': start,
                   'duration': duration,
                   'dumpfile': dumpfile}))

            if os.path.exists(dumpfile) and os.stat(dumpfile).st_size >= MIN_SIZE:
                return await file_stream(dumpfile, chunk_size=CHUNK, mime_type=MIME)

            return await file_stream(testfile, chunk_size=CHUNK, mime_type=MIME)
        else:
            return json({'status': 'URL not understood'}, 302)
    except Exception as ex:
        return json({'status': 'URL not understood', 'exception': repr(ex)}, 400)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8888, workers=1, debug=True, access_log=True)
