#!/usr/bin/env python3

import argparse
import httpx
import multiprocessing
import signal
import socket
import subprocess
import time
import os
import urllib.parse

from contextlib import closing
from collections import namedtuple


SANIC_EPG_HOST = os.environ.get('SANIC_EPG_HOST') or '127.0.0.1'
SANIC_EPG_PORT = int(os.environ.get('SANIC_EPG_PORT')) or 8889
STORAGE = os.environ.get('U7D_DIR') or '/tmp'

MVTV_URL = 'http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do'
SANIC_EPG_URL = f'http://{SANIC_EPG_HOST}:{SANIC_EPG_PORT}'
Request = namedtuple('Request', ['request', 'response'])
Response = namedtuple('Response', ['version', 'status', 'url', 'headers', 'body'])
UA = 'MICA-IP-STB'
# WIDTH = 134

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

        if resp.status != '200 OK' and 'SETUP' not in method:
            raise ValueError(f'{method} {repr(resp)}')

        return Request(req, resp)

    def print(self, req, killed=None):
        resp = req.response
        return resp
        _req = req.request.split('\r\n')[0].split(' ')
        _off = 90 - len(_req[0])
        if killed:
            tmp = _req[1].split('/')
            _req[1] = str(tmp[0]) + '://' + str(tmp[2])[:26] + ' '
            _req[1] += (f'[{killed.client_ip}] '
                        f'{killed.channel} '
                        f'{killed.broadcast} '
                        f'-s {killed.start} '
                        f'-p {killed.client_port}')
        _req_l = _req[0] + ' ' + _req[1][:_off]
        _req_r = ' ' * (100 - len(_req_l) - len(_req[2]))
        print(f'Req: {_req_l}{_req_r}{_req[2]}', end=' ', flush=True)
        print(f'Resp: {resp.version} {resp.status}', flush=True)
        # headers = self.serialize_headers(resp.headers)
        # print('-' * WIDTH, flush=True)
        # print('Request: ' + req.request.split('\m')[0], end='', flush=True)
        # print(f'Response: {resp.version} {resp.status}\n{headers}', flush=True)
        # if resp.body:
        #     print(f'\n{resp.body.rstrip()}', flush=True)
        # print('-' * WIDTH, flush=True)
        return resp


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def safe_filename(filename):
    keepcharacters = (' ', '.', '_')
    return "".join(c for c in filename.replace('/', '_')
                   if c.isalnum() or c in keepcharacters).rstrip()


def main(args):
    global needs_position

    client = s = None
    headers = {'CSeq': '', 'User-Agent': UA}

    setup = session = play = describe = headers.copy()
    describe['Accept'] = 'application/sdp'
    setup['Transport'] = f'MP2T/H2221/UDP;unicast;client_port={args.client_port}'

    params = (f'action=getCatchUpUrl'
              f'&extInfoID={args.broadcast}'
              f'&channelID={args.channel}'
              f'&service=hd&mode=1')
    resp = httpx.get(f'{MVTV_URL}?{params}', headers={'User-Agent': UA})
    data = resp.json()

    if data['resultCode'] != 0:
        print(f'Error: [{args.client_ip}] {repr(data)} '
              f'{args.channel} {args.broadcast} '
              f'-s {args.start}', flush=True)
        return

    url = data['resultData']['url']
    uri = urllib.parse.urlparse(url)

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
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
                    print(f'{repr(r)}', flush=True)
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
                epg_url = f'{SANIC_EPG_URL}/get_program_name/{args.channel}/{args.broadcast}'
                resp = httpx.get(epg_url)

                if resp.status_code == 200:
                    data = resp.json()
                    print(f'{repr(data)}', flush=True)

                    if not args.time:
                        args.time = int(data['duration']) - args.start
                    title = safe_filename(data['full_title'])
                    if data['is_serie']:
                        path = os.path.join(STORAGE, safe_filename(data['serie']))
                        filename = os.path.join(path, title + '.ts')
                        if not os.path.exists(path):
                            print(f'Creating recording subdir {path}', flush=True)
                            os.mkdir(path)
                    else:
                        filename = os.path.join(STORAGE, title + '.ts')
                elif args.time:
                    filename = f'{STORAGE}/{args.channel}-{args.broadcast}.ts'
                else:
                    raise ValueError('Recording time unknown')

                command = ['socat']
                command.append('-u')
                command.append(f'UDP4-LISTEN:{args.client_port}')
                command.append(f'OPEN:"{filename}",creat,trunc')

                proc = multiprocessing.Process(target=subprocess.call, args=(command, ))
                proc.start()

            if args.time:
                def _handle_timeout(signum, frame):
                    raise TimeoutError()
                signal.signal(signal.SIGALRM, _handle_timeout)
                signal.alarm(args.time)

            print('Recording:' if args.write_to_file else 'Started:',
                  f'{args.channel}',
                  f'{args.broadcast}',
                  f'[{args.time}s]'
                  f'@ "{filename}"' if args.write_to_file else '',
                  f'[{args.client_ip}]' if args.client_ip else '', flush=True)

            while True:
                time.sleep(30)
                client.print(client.send_request('GET_PARAMETER', get_parameter))

        except Exception as ex:
            if args.write_to_file and proc and proc.is_alive():
                proc.terminate()
                if proc.is_alive():
                    subprocess.call(['pkill', '-f', f"{' '.join(command[:3])}"])
            print('Finished:' if isinstance(ex, TimeoutError) else f'{repr(ex)}',
                  f'{args.channel}',
                  f'{args.broadcast}',
                  f'[{args.time}s]',
                  f'@ "{filename}"' if args.write_to_file else '',
                  f'[{args.client_ip}]' if args.client_ip else '', flush=True)
        finally:
            if client and 'Session' in session:
                client.print(client.send_request('TEARDOWN', session), killed=args)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser('Stream content from the Movistar U7D service.')
        parser.add_argument('channel', help='channel id')
        parser.add_argument('broadcast', help='broadcast id')
        parser.add_argument('--client_ip', '-i', help='client ip address')
        parser.add_argument('--client_port', '-p', help='client udp port', type=int)
        parser.add_argument('--start', '-s', help='stream start offset', type=int)
        parser.add_argument('--time', '-t', help='recording time in seconds', type=int)
        parser.add_argument('--write_to_file', '-w', help='record', action='store_true')
        args = parser.parse_args()
        if not args.start:
            args.start = 0
        if not args.client_port:
            args.client_port = find_free_port()
        main(args)
    except KeyboardInterrupt:
        pass
    except Exception as ex:
        print(f'Died: /app/u7d.py {args.channel} {args.broadcast} '
              f'-s {args.start} -p {args.client_port} {repr(ex)}', flush=True)
