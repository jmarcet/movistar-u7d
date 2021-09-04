#!/usr/bin/env python3

import aiohttp
import argparse
import asyncio
import httpx
import os
import re
import signal
import socket
import subprocess
import sys
import ujson
import urllib.parse

from asyncio import CancelledError
from collections import namedtuple
from contextlib import closing
from ffmpeg import FFmpeg
from json2xml import json2xml
from threading import Thread


CACHE_DIR = os.path.join(os.getenv('HOME', '/home'), '.xmltv/cache/programs')
RECORDINGS = os.getenv('RECORDINGS', '/tmp')
SANIC_EPG_URL = 'http://127.0.0.1:8889'

IMAGENIO_URL = 'http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg'
COVER_URL = f'{IMAGENIO_URL}/covers/programmeImages/portrait/290x429'
MVTV_URL = 'http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do'

NFO_EXT = '-movistar.nfo'
TMP_EXT = '.tmp'
VID_EXT = '.mkv'
UA = 'MICA-IP-STB'
# WIDTH = 134

YEAR_SECONDS = 365 * 24 * 60 * 60

Request = namedtuple('Request', ['request', 'response'])
Response = namedtuple('Response', ['version', 'status', 'url', 'headers', 'body'])

ffmpeg = FFmpeg().option('y').option('xerror')
_ffmpeg = _log_prefix = _log_suffix = args = client = epg_url = filename = \
    full_title = get_parameter = path = session = vod_client = None
needs_position = False


class RtspClient(object):
    def __init__(self, reader, writer, url):
        self.reader = reader
        self.writer = writer
        self.url = url
        self.cseq = 1
        self.ip = None

    async def read_message(self):
        resp = (await self.reader.read(4096)).decode()
        if ' 200 OK' not in resp:
            version, status = resp.rstrip().split('\n')[0].split(' ', 1)
            return Response(version, status, self.url, {}, '')

        head, body = resp.split('\r\n\r\n')
        version, status = head.split('\r\n')[0].rstrip().split(' ', 1)

        headers = dict()
        for line in head.split('\r\n')[1:]:
            key, value = line.split(': ', 1)
            headers[key] = value

        if 'Content-Length' in headers:
            if 'a=control:rtsp:' in body:
                self.ip = body.split('\n')[-1].split(':', 1)[1].strip()
        else:
            body = ''

        return Response(version, status, self.url, headers, body)

    def serialize_headers(self, headers):
        return '\r\n'.join(map(lambda x: '{0}: {1}'.format(*x), headers.items()))

    async def send_request(self, method, headers={}):
        global needs_position
        if method == 'OPTIONS':
            url = '*'
        elif method == 'SETUP2':
            method = 'SETUP'
            self.url, self.ip = self.ip, self.url
            url = self.url
        else:
            url = self.url

        headers['CSeq'] = self.cseq
        ser_headers = self.serialize_headers(headers)
        self.cseq += 1

        if method == 'GET_PARAMETER' and needs_position:
            req = f'{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\nposition\r\n\r\n'
        else:
            req = f'{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\n'

        self.writer.write(req.encode())
        resp = await self.read_message()

        if resp.status != '200 OK' and method not in ['SETUP', 'TEARDOWN']:
            raise ValueError(f'{method} {resp}')

        return Request(req, resp)

    def print(self, req):
        return req.response
        # resp = req.response
        # _req = req.request.split('\r\n')[0].split(' ')
        # if 'TEARDOWN' not in _req:
        #     return resp
        # tmp = _req[1].split('/')
        # _req[1] = (f'{tmp[0]}//{str(tmp[2])[:40]} '
        #            f'{args.channel} {args.broadcast} '
        #            f'-s {args.start} -p {args.client_port}')
        # sys.stdout.write(f'[{args.client_ip}][VOD] Req: {_req[0]} [{_req[1]}] '
        #                  f'{_req[2]} => {resp.status}\n')
        # headers = self.serialize_headers(resp.headers)
        # sys.stderr.write('-' * WIDTH + '\n')
        # sys.stderr.write('Request: ' + req.request.split('\n')[0] + '\n')
        # sys.stderr.write(f'Response: {resp.version} {resp.status}\n{headers}\n')
        # if resp.body:
        #     sys.stderr.write(f'\n{resp.body.rstrip()}\n')
        # sys.stderr.write('-' * WIDTH + '\n')
        # return resp

    def close_connection(self):
        self.writer.close()


@ffmpeg.on('completed')
def ffmpeg_completed():
    command = _nice = ['nice', '-n', '10', 'ionice', '-c', '3']

    if os.path.exists(filename + VID_EXT):
        os.remove(filename + VID_EXT)

    if not args.mp4:
        command += ['mkvmerge', '-q', '-o', filename + VID_EXT]
        if args.vo:
            command += ['--track-order', '0:2,0:1,0:4,0:3,0:6,0:5']
            command += ['--default-track', '2:1']
        command += [filename + TMP_EXT]
        subprocess.run(command)

    else:
        proc = subprocess.run(['ffprobe', filename + TMP_EXT], capture_output=True)
        subs = [t.split()[1] for t in proc.stderr.decode().splitlines() if ' Subtitle:' in t]

        command += ['ffmpeg', '-i', filename + TMP_EXT]
        command += ['-map', '0', '-c', 'copy', '-sn']
        command += ['-movflags', '+faststart']
        command += ['-v', 'panic', '-y', '-f', 'mp4']
        command += [filename + VID_EXT]

        proc = subprocess.Popen(command)

        if len(subs):
            track, lang = re.match(r"^#([0-9:]+)[^:]*\((\w+)\):", subs[0]).groups()
            if lang == 'esp':
                lang = 'spa'
            command = _nice
            command += ['ffmpeg', '-i', filename + TMP_EXT, '-map', track]
            command += ['-c:s', 'dvbsub', '-f', 'mpegts', '-v', 'panic']
            command += ['-y', f'{filename}.{lang}.sub']
            subprocess.run(command)

        proc.wait()

    if check_recording():
        save_metadata()
        httpx.put(epg_url)
    else:
        os.remove(filename + VID_EXT)

    _cleanup()


@ffmpeg.on('error')
def ffmpeg_error(code):
    ffmpeg_completed()


# @ffmpeg.on('stderr')
# def ffmpeg_stderr(line):
#     if line.startswith('frame='):
#         return
#     sys.stderr.write(f'{_log_prefix} [ffmpeg] {line}\n')


@ffmpeg.on('terminated')
def ffmpeg_terminated():
    sys.stderr.write(f'{_log_prefix} [ffmpeg] TERMINATED: {_log_suffix}')
    ffmpeg_completed()


def _cleanup():
    if os.path.exists(filename + TMP_EXT):
        os.remove(filename + TMP_EXT)


def _handle_cleanup(signum, frame):
    raise TimeoutError()


def check_recording():
    if not os.path.exists(filename + VID_EXT):
        sys.stderr.write(f'{_log_prefix} Recording CANNOT FIND: {_log_suffix}')
        return False

    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format']
    command += [filename + VID_EXT]
    try:
        probe = ujson.loads(subprocess.run(command, capture_output=True).stdout.decode())
        duration = int(float(probe['format']['duration']))
    except KeyError:
        sys.stderr.write(f'{_log_prefix} Recording CANNOT PARSE: {_log_suffix}')
        return False

    _bad = (duration + 30) < args.time
    sys.stderr.write(f"{_log_prefix} Recording {'INCOMPLETE:' if _bad else 'COMPLETE:'} "
                     f'[{duration}s] -> '
                     f'{_log_suffix}')

    return not _bad


def find_free_port(iface=''):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind((iface, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def get_vod_url(channel_id, program_id):
    global vod_client
    params = f'action=getCatchUpUrl&extInfoID={program_id}&channelID={channel_id}' \
             f'&service=hd&mode=1'
    try:
        async with vod_client.get(f'{MVTV_URL}?{params}') as r:
            resp = await r.json()
        return resp['resultData']['url']
    except KeyError:
        return None


async def record_stream():
    global _ffmpeg, _log_suffix, ffmpeg, filename, full_title, path
    _options = {
        'map': '0',
        'c': 'copy',
        'c:a:0': 'aac',
        'c:a:1': 'aac',
        'bsf:v': 'h264_mp4toannexb',
        'metadata:s:a:0': 'language=spa',
        'metadata:s:a:1': 'language=eng',
        'metadata:s:a:2': 'language=spa',
        'metadata:s:a:3': 'language=eng',
        'metadata:s:s:0': 'language=spa',
        'metadata:s:s:1': 'language=eng'
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(epg_url)
    if resp.status_code == 200:
        data = resp.json()
        # sys.stderr.write(f'{data}\n')

        if not args.time:
            args.time = int(data['duration']) - args.start
        full_title, path, filename = [data[t] for t in ['full_title', 'path', 'filename']]
        _options['metadata:s:v'] = f'title={full_title}'
        if not os.path.exists(path):
            sys.stderr.write(f'{_log_prefix} Creating recording subdir {path}\n')
            os.mkdir(path)
    else:
        filename = os.path.join(RECORDINGS,
                                f'{args.channel}-{args.broadcast}')
    _log_suffix += f' [{args.time}s] "{filename[20:]}"\n'

    ffmpeg.input(
        f'udp://@{args.iptv_ip}:{args.client_port}',
        fifo_size=5572,
        pkt_size=1316,
        timeout=500000
    ).output(filename + TMP_EXT, _options,
             fflags='+igndts',
             t=str(args.time + 300),
             v='info',
             vsync='0',
             f='matroska')

    _ffmpeg = Thread(target=asyncio.run, args=(ffmpeg.execute(),))
    _ffmpeg.start()

    sys.stderr.write(f'{_log_prefix} Recording STARTED: {_log_suffix}')


def save_metadata():
    try:
        with open(os.path.join(CACHE_DIR, f'{args.broadcast}.json')) as f:
            metadata = ujson.loads(f.read())['data']
        _cover = metadata['cover']
        resp = httpx.get(f'{COVER_URL}/{_cover}')
        if resp.status_code == 200:
            image_ext = os.path.basename(_cover).split('.')[-1]
            _img_name = f'{filename}.{image_ext}'
            with open(_img_name, 'wb') as f:
                f.write(resp.read())
            metadata['cover'] = os.path.basename(_img_name)
        if 'covers' in metadata:
            if not os.path.exists(os.path.join(path + '/metadata')):
                os.mkdir(os.path.join(path + '/metadata'))
            for _img in metadata['covers']:
                _cover = metadata['covers'][_img]
                resp = httpx.get(_cover)
                if resp.status_code != 200:
                    continue
                image_ext = os.path.basename(_cover).split('.')[-1]
                _img_name = f'{path}/metadata/{os.path.basename(filename)}-{_img}' \
                            f'.{image_ext}'
                with open(_img_name, 'wb') as f:
                    f.write(resp.read())
        metadata['title'] = full_title
        metadata.pop('covers', None)
        metadata.pop('logos', None)
        metadata.pop('name', None)
        with open(filename + NFO_EXT, 'w') as f:
            f.write(json2xml.Json2xml(metadata, attr_type=False,
                                      pretty=True, wrapper='metadata').to_xml())

    except (FileNotFoundError, TypeError, ValueError) as ex:
        sys.stderr.write(f'{_log_prefix} No metadata found {ex}\n')


async def VodInit(VodArgs, client):
    global args, vod_client
    args = VodArgs

    vod_client = client
    try:
        await VodSetup()
    except (CancelledError, TimeoutError, ValueError):
        pass


async def VodLoop():
    global vod_client
    try:
        if __name__ == '__main__':
            conn = aiohttp.TCPConnector()
            vod_client = aiohttp.ClientSession(connector=conn, headers={'User-Agent': UA})
            await VodSetup()

            if args.write_to_file:
                await record_stream()

        while True:
            await asyncio.sleep(30)
            if __name__ == '__main__':
                if args.write_to_file and _ffmpeg and not _ffmpeg.is_alive():
                    break
            client.print(await client.send_request('GET_PARAMETER', get_parameter))

    except (AttributeError, CancelledError, TimeoutError, TypeError, ValueError):
        pass
    except Exception as ex:
        sys.stderr.write(f'{_log_prefix} Error: {ex}\n')

    finally:
        try:
            client.print(await client.send_request('TEARDOWN', session))
        except (AttributeError, OSError, RuntimeError):
            pass
        try:
            client.close_connection()
        except AttributeError:
            pass
        if __name__ == '__main__':
            if _ffmpeg:
                _ffmpeg.join()
            await vod_client.close()


async def VodSetup():
    global _log_prefix, _log_suffix, client, get_parameter, epg_url, needs_position, session

    _log_prefix = f"{'[' + args.client_ip + '] ' if args.client_ip else ''}[VOD:{os.getpid()}]"
    _log_suffix = f'{args.channel} {args.broadcast}'

    client = None
    headers = {'CSeq': '', 'User-Agent': UA}

    setup = session = play = describe = headers.copy()
    describe['Accept'] = 'application/sdp'
    setup['Transport'] = f'MP2T/H2221/UDP;unicast;client_port={args.client_port}'

    url = await get_vod_url(args.channel, args.broadcast)
    if not url:
        sys.stderr.write(f'{_log_prefix} Error: {repr(url)}\n')
        raise ValueError()

    uri = urllib.parse.urlparse(url)
    epg_url = (f'{SANIC_EPG_URL}/program_name/{args.channel}/{args.broadcast}')

    reader, writer = await asyncio.open_connection(uri.hostname, uri.port)

    signal.signal(signal.SIGHUP, _handle_cleanup)
    signal.signal(signal.SIGTERM, _handle_cleanup)

    client = RtspClient(reader, writer, url)
    client.print(await client.send_request('OPTIONS', headers))
    client.print(await client.send_request('DESCRIBE', describe))

    r = client.print(await client.send_request('SETUP', setup))
    if r.status != '200 OK':
        needs_position = True
        r = client.print(await client.send_request('SETUP2', setup))
        if r.status != '200 OK':
            sys.stderr.write(f'{r}\n')
            return

    play['Range'] = f'npt={args.start}-end'
    play.update({'Scale': '1.000', 'x-playNow': '', 'x-noFlush': ''})
    play['Session'] = session['Session'] = r.headers['Session'].split(';')[0]

    get_parameter = session.copy()
    if needs_position:
        get_parameter.update({'Content-type': 'text/parameters',
                              'Content-length': 10})

    client.print(await client.send_request('PLAY', play))


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Stream content from the Movistar VOD service.')
    parser.add_argument('channel', help='channel id')
    parser.add_argument('broadcast', help='broadcast id')
    parser.add_argument('--iptv_ip', '-a', help='iptv address')
    parser.add_argument('--client_ip', '-i', help='client ip address')
    parser.add_argument('--client_port', '-p', help='client udp port', type=int)
    parser.add_argument('--start', '-s', help='stream start offset', type=int)
    parser.add_argument('--time', '-t', help='recording time in seconds', type=int)
    parser.add_argument('--mp4', help='output split mp4 and vobsub files', type=bool, default=False)
    parser.add_argument('--vo', help='set 2nd language as main one', type=bool, default=False)
    parser.add_argument('--write_to_file', '-w', help='record', action='store_true')

    args = parser.parse_args()

    if not args.iptv_ip:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("172.26.23.3", 53))
        args.iptv_ip = s.getsockname()[0]
        s.close

    if not args.client_port:
        args.client_port = find_free_port(args.iptv_ip)

    try:
        asyncio.run(VodLoop())
        sys.exit(0)
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        sys.stderr.write(f'{ex}\n')
        sys.exit(1)
