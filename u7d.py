#!/usr/bin/env python3

import urllib.request
import urllib.parse
import subprocess
import argparse
import socket
import json
import time
import os

from contextlib import closing
from collections import namedtuple

Request = namedtuple('Request', ['request', 'response'])
Response = namedtuple('Response', ['version', 'status', 'headers', 'body'])
UA = 'MICA-IP-STB'
WIDTH = 134

needs_position = False

class RtspClient(object):
    def __init__(self, sock, url):
        self.sock = sock
        self.url = url
        self.cseq = 1
        self.ip = None

    def read_message(self):
        resp = self.sock.recv(4096).decode()
        if not ' 200 OK' in resp:
            version, status = resp.rstrip().split('\n')[0].split(' ', 1)
            print(f'Response: {version} {status}')
            return Response(version, status, {}, '')

        head, body = resp.split('\r\n\r\n')
        version, status = head.split('\r\n')[0].rstrip().split(' ', 1)

        headers = dict()
        for line in head.split('\r\n')[1:]:
            key, value = line.split(': ', 1)
            headers[key] = value

        if 'Content-Length' in headers.keys():
            length = headers['Content-Length']
            if 'a=control:rtsp:' in body:
                self.ip = body.split('\n')[-1].split(':', 1)[1].strip()
        else:
            body = ''

        return Response(version, status, headers, body)

    def serialize_headers(self, headers):
        return '\r\n'.join(map(lambda x: '{0}: {1}'.format(*x), headers.items()))

    def send_request(self, method, headers={}):
        global needs_position
        if method == 'OPTIONS':
            url = '*'
        elif method == 'SETUP2':
            method = 'SETUP'
            url = self.url = self.ip
            print('=' * WIDTH, flush=True)
            print(f'RollingBuffer: {url}', flush=True)
            print('=' * WIDTH, flush=True)
        else:
            url = self.url
            if method == 'SETUP':
                print('=' * WIDTH, flush=True)
                print(f'RollingBuffer: {self.url}', flush=True)
                print('=' * WIDTH, flush=True)

        headers['CSeq'] = self.cseq
        ser_headers = self.serialize_headers(headers)
        self.cseq += 1

        if method == 'GET_PARAMETER' and needs_position:
            req = f'{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\nposition\r\n\r\n'
        else:
            req = f'{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\n'

        self.sock.send(req.encode())
        resp = self.read_message()

        return Request(req, resp)

    def print(self, req):
        resp = req.response
        return resp
        headers = self.serialize_headers(resp.headers)
        print('=' * WIDTH, flush=True)
        print('Request: ' + req.request, end='', flush=True)
        print(f'Response: {resp.version} {resp.status}\n{headers}', flush=True)
        if resp.body:
            print(f'\n{resp.body}', flush=True)
        return resp

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

def main(args):
    global needs_position

    headers = {'CSeq': '', 'User-Agent': UA}
    host = socket.gethostbyname(socket.gethostname())

    params = f'action=getCatchUpUrl&extInfoID={args.broadcast}&channelID={args.channel}&service={args.quality}&mode=1'
    resp = urllib.request.urlopen(f'http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do?{params}')
    data = json.loads(resp.read())

    if data['resultCode'] != 0:
        print(f'error: {data["resultText"]}')
        return

    stream = f'udp://@{host}:{args.client_port}'
    print('=' * 134, flush=True)
    print(f'Stream: {stream}', flush=True)

    url = data['resultData']['url']
    uri = urllib.parse.urlparse(url)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((uri.hostname, uri.port))
        s.settimeout(60)

        client = RtspClient(s, url)
        client.print(client.send_request('OPTIONS', headers))

        describe = headers.copy()
        describe['Accept'] = 'application/sdp'
        client.print(client.send_request('DESCRIBE', describe))

        setup = headers.copy()
        setup.update({'Transport': f'MP2T/H2221/UDP;unicast;client_port={args.client_port}'})
        r = client.print(client.send_request('SETUP', setup))
        if r.status != '200 OK':
            needs_position = True
            r = client.print(client.send_request('SETUP2', setup))

        play = headers.copy()
        play = {'CSeq': '', 'Session': '', 'User-Agent': UA}
        play['Session'] = r.headers['Session'].split(';')[0]
        play['Range'] = 'npt={0}-end'.format(*args.start)
        play.update({'Scale': '1.000', 'x-playNow': '', 'x-noFlush': ''})
        client.print(client.send_request('PLAY', play))

        session = {'User-Agent': UA, 'Session': play['Session'], 'CSeq': ''}
        get_parameter = session.copy()
        if needs_position:
            get_parameter.update({'Content-type': 'text/parameters', 'Content-length': 10})

        try:
            while True:
                time.sleep(30)
                client.print(client.send_request('GET_PARAMETER', get_parameter))
        except Exception as ex:
            print(f'Rtsp session EXCEPTED with {repr(ex)}')
        finally:
            client.print(client.send_request('TEARDOWN', session))

if __name__ == '__main__':
    services = {'sd': 1, 'hd': 2}
    parser = argparse.ArgumentParser('Stream content from the Movistar U7D service.')
    parser.add_argument('channel', help='channel id')
    parser.add_argument('broadcast', help='broadcast id')
    parser.add_argument('--client_port', help='client udp port')
    parser.add_argument('--dumpfile', '-f', help='dump file path', default='test2.ts')
    parser.add_argument('--duration', '-t', default=[0], type=int, help='duration in seconds')
    parser.add_argument('--quality', '-q', choices=services, nargs=1, default=['hd'], help='stream quality')
    parser.add_argument('--start', '-s', metavar='seconds', nargs=1, default=[0], type=int, help='stream start offset')
    args = parser.parse_args()
    if args.client_port is not None:
        args.client_port = int(args.client_port)
    elif not args.client_port:
        args.client_port = find_free_port()
    args.quality = services.get(*args.quality)
    main(args)

