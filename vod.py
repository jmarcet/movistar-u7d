#!/usr/bin/env python3

import argparse
import asyncio
import httpx
import json
import multiprocessing
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.parse

from collections import namedtuple
from contextlib import closing
from ffmpeg import FFmpeg
from threading import Thread


if 'IPTV_ADDRESS' in os.environ:
    IPTV = os.getenv('IPTV_ADDRESS')
else:
    IPTV = socket.gethostbyname(socket.gethostname())

MVTV_URL = 'http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do'
RECORDINGS = os.getenv('RECORDINGS', '/tmp')
SANIC_EPG_URL = f'http://127.0.0.1:8889'
TMP_EXT = '.tmp'
VID_EXT = '.mkv'
UA = 'MICA-IP-STB'
# WIDTH = 134

Request = namedtuple('Request', ['request', 'response'])
Response = namedtuple('Response', ['version', 'status', 'url', 'headers', 'body'])

ffmpeg = FFmpeg().option('y')
_ffmpeg = _log_prefix = _log_suffix = args = epg_url = filename = None
needs_position = False


class RtspClient(object):
    def __init__(self, sock, url):
        self.sock = sock
        self.url = url
        self.cseq = 1
        self.ip = None

    def read_message(self):
        resp = self.sock.recv(4096).decode()
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

    def send_request(self, method, headers={}):
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

        self.sock.send(req.encode())
        resp = self.read_message()

        if resp.status != '200 OK' and method not in ['SETUP', 'TEARDOWN']:
            raise ValueError(f'{method} {repr(resp)}')

        return Request(req, resp)

    def print(self, req, killed=None):
        # return req.response
        resp = req.response
        _req = req.request.split('\r\n')[0].split(' ')
        if 'TEARDOWN' not in _req:
            return resp
        if killed:
            tmp = _req[1].split('/')
            _req[1] = (f'{tmp[0]}//{str(tmp[2])[:26]} '
                       f'{killed.channel} {killed.broadcast} '
                       f'-s {killed.start} -p {killed.client_port}')
        sys.stdout.write(f'[{killed.client_ip}][VOD] Req: {_req[0]} [{_req[1]}] '
                         f'{_req[2]} => {resp.status}\n')
        # headers = self.serialize_headers(resp.headers)
        # sys.stderr.write('-' * WIDTH + '\n')
        # sys.stderr.write('Request: ' + req.request.split('\n')[0] + '\n')
        # sys.stderr.write(f'Response: {resp.version} {resp.status}\n{headers}\n')
        # if resp.body:
        #     sys.stderr.write(f'\n{resp.body.rstrip()}\n')
        # sys.stderr.write('-' * WIDTH + '\n')
        return resp


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def _check_recording():
    if not os.path.exists(filename + VID_EXT):
        sys.stderr.write(f'{_log_prefix} Recording CANNOT FIND: {_log_suffix}')
        return 0

    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format']
    command += [filename + VID_EXT]
    try:
        probe = json.loads(subprocess.run(command, capture_output=True).stdout.decode())
        duration = int(float(probe['format']['duration']))
    except KeyError:
        sys.stderr.write(f'{_log_prefix} Recording CANNOT PARSE: {_log_suffix}')
        return 0

    _bad = (duration + 30) < args.time
    sys.stderr.write(f"{_log_prefix} Recording {'INCOMPLETE:' if _bad else 'COMPLETE:'} "
                     f'[{duration}s] -> '
                     f'{_log_suffix}')

    return 0 if _bad else 1


def _cleanup():
    if os.path.exists(filename + TMP_EXT):
        os.remove(filename + TMP_EXT)


@ffmpeg.on('stderr')
def on_stderr(line):
    if line.startswith('frame='):
        return
    sys.stderr.write(f'{_log_prefix} [ffmpeg] {line}\n')


@ffmpeg.on('terminated')
def on_terminated():
    sys.stderr.write(f'{_log_prefix} [ffmpeg] TERMINATED: {_log_suffix}')
    on_completed()


@ffmpeg.on('error')
def on_error(code):
    if code == -1:
        on_completed()
    else:
        sys.stderr.write(f'{_log_prefix} Recording FAILED error={code}: {_log_suffix}')
        _cleanup()


@ffmpeg.on('completed')
def on_completed():
    command = _nice = ['nice', '-n', '10', 'ionice', '-c', '3']

    if os.path.exists(filename + VID_EXT):
        os.remove(filename + VID_EXT)

    if not args.mp4:
        command += ['mkvmerge', '-q', '-o', filename + VID_EXT]
        command += ['--default-language', 'spa']
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

        proc = multiprocessing.Process(target=subprocess.run, args=(command, ))
        proc.start()

        if len(subs):
            track, lang = re.search(r"^#([0-9:]+)[^:]*\((\w+)\):", subs[0]).groups()
            if lang == 'esp':
                lang = 'spa'
            command = _nice
            command += ['ffmpeg', '-i', filename + TMP_EXT, '-map', track]
            command += ['-c:s', 'dvbsub', '-f', 'mpegts', '-v', 'panic']
            command += ['-y', f'{filename}.{lang}.sub']
            subprocess.run(command)

        proc.join()

    if _check_recording():
        httpx.put(epg_url)

    _cleanup()


def record_stream():
    global _ffmpeg, _log_suffix, ffmpeg, filename
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

    resp = httpx.get(epg_url)
    if resp.status_code == 200:
        data = resp.json()
        # sys.stderr.write(f'{repr(data)}\n')

        if not args.time:
            args.time = int(data['duration']) - args.start
        title = data['full_title']
        _options['metadata:s:v'] = f'title={title}'
        if data['description']:
            _options['metadata:s:v:0'] = 'description=' + data['description']
        path = data['path']
        filename = data['filename']
        if not os.path.exists(path):
            sys.stderr.write(f'{_log_prefix} Creating recording subdir {path}\n')
            os.mkdir(path)
    else:
        filename = os.path.join(RECORDINGS,
                                f'{args.channel}-{args.broadcast}')
    _log_suffix += f' [{args.time}s] "{filename[20:]}"\n'

    ffmpeg.input(
        f'udp://@{IPTV}:{args.client_port}',
        fifo_size=5572,
        pkt_size=1316,
        timeout=500000
    ).output(filename + TMP_EXT, _options,
             fflags='+genpts+igndts',
             seek2any='1',
             max_error_rate='0.0',
             t=str(args.time + 600),
             v='panic',
             vsync='2',
             f='matroska')

    _ffmpeg = Thread(target=asyncio.run, args=(ffmpeg.execute(),))
    _ffmpeg.start()

    sys.stderr.write(f'{_log_prefix} Recording STARTED: {_log_suffix}')


def main():
    global epg_url, needs_position

    client = s = None
    headers = {'CSeq': '', 'User-Agent': UA}

    setup = session = play = describe = headers.copy()
    describe['Accept'] = 'application/sdp'
    setup['Transport'] = f'MP2T/H2221/UDP;unicast;client_port={args.client_port}'

    params = (f'action=getCatchUpUrl'
              f'&extInfoID={args.broadcast}'
              f'&channelID={args.channel}'
              f'&service=hd&mode=1')
    try:
        resp = httpx.get(f'{MVTV_URL}?{params}', headers={'User-Agent': UA})
        data = resp.json()
    except httpx.ConnectError:
        data = None

    if not data or 'resultCode' not in data or data['resultCode'] != 0:
        print(f"{'[' + args.client_ip + '] ' if args.client_ip else ''}"
              f'Error: {data} ' if data else 'Error: ',
              flush=True)
        return

    epg_url = (f'{SANIC_EPG_URL}/program_name/{args.channel}/{args.broadcast}')

    url = data['resultData']['url']
    uri = urllib.parse.urlparse(url)

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        _teardown = True
        try:
            s.connect((uri.hostname, uri.port))
            s.settimeout(10)

            client = RtspClient(s, url)
            client.print(client.send_request('OPTIONS', headers))
            client.print(client.send_request('DESCRIBE', describe))

            r = client.print(client.send_request('SETUP', setup))
            if r.status != '200 OK':
                needs_position = True
                r = client.print(client.send_request('SETUP2', setup))
                if r.status != '200 OK':
                    sys.stderr.write(f'{repr(r)}\n')
                    return

            play['Range'] = f'npt={args.start}-end'
            play.update({'Scale': '1.000', 'x-playNow': '', 'x-noFlush': ''})
            play['Session'] = session['Session'] = r.headers['Session'].split(';')[0]

            get_parameter = session.copy()
            if needs_position:
                get_parameter.update({'Content-type': 'text/parameters',
                                      'Content-length': 10})

            client.print(client.send_request('PLAY', play))

            if args.write_to_file:
                record_stream()

            def _handle_cleanup(signum, frame):
                raise TimeoutError()

            signal.signal(signal.SIGHUP, _handle_cleanup)
            if args.time:
                signal.signal(signal.SIGALRM, _handle_cleanup)
                signal.alarm(args.time)

            while True:
                time.sleep(30)
                if args.write_to_file and _ffmpeg and not _ffmpeg.is_alive():
                    break
                client.print(client.send_request('GET_PARAMETER', get_parameter))

        except TimeoutError:
            pass

        except ValueError:
            _teardown = False

        finally:
            if _teardown and client and 'Session' in session:
                client.print(client.send_request('TEARDOWN', session), killed=args)
            if args.write_to_file:
                if _ffmpeg and _ffmpeg.is_alive():
                    subprocess.run(['pkill', '-HUP', '-f',
                                   f'ffmpeg.+udp://@{IPTV}:{args.client_port}'])
                try:
                    httpx.get(f'{SANIC_EPG_URL}/check_timers')
                except Exception:
                    pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser('Stream content from the Movistar VOD service.')
    parser.add_argument('channel', help='channel id')
    parser.add_argument('broadcast', help='broadcast id')
    parser.add_argument('--client_ip', '-i', help='client ip address')
    parser.add_argument('--client_port', '-p', help='client udp port', type=int)
    parser.add_argument('--start', '-s', help='stream start offset', type=int)
    parser.add_argument('--time', '-t', help='recording time in seconds', type=int)
    parser.add_argument('--mp4', help='output split mp4 and vobsub files', type=bool, default=False)
    parser.add_argument('--vo', help='set 2nd language as main one', type=bool, default=False)
    parser.add_argument('--write_to_file', '-w', help='record', action='store_true')

    args = parser.parse_args()

    if not args.client_port:
        args.client_port = find_free_port()

    _log_prefix = f"{'[' + args.client_ip + '] ' if args.client_ip else ''}[VOD:{os.getpid()}]"
    _log_suffix = f'{args.channel} {args.broadcast}'

    try:
        main()
        sys.exit(0)
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        sys.stderr.write(f'{repr(ex)}\n')
        sys.exit(1)
