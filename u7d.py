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
Response = namedtuple('Response', ['version', 'status', 'url', 'headers', 'body'])
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
            #print(f'Response: {version} {status}', flush=True)
            return Response(version, status, self.url, {}, '')

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
        _req = req.request.split('\r\n')[0].split(' ')
        _off = 90 - len(_req[0])
        if killed:
            tmp = _req[1].split('/')
            _req[1] = str(tmp[0]) + '://' + str(tmp[2])[:26]
            _req[1] += f' [{killed.client_ip}] {killed.channel} {killed.broadcast} -s {killed.start[0]} -p {killed.client_port}'
        _req_l = _req[0] + ' ' + _req[1][:_off]
        _req_r = ' ' * (100 - len(_req_l) - len(_req[2]))
        print(f'Req: {_req_l}{_req_r}{_req[2]}', end=' ', flush=True)
        print(f'Resp: {resp.version} {resp.status}', flush=True)
        #headers = self.serialize_headers(resp.headers)
        #print('-' * WIDTH, flush=True)
        #print('Request: ' + req.request.split('\m')[0], end='', flush=True)
        #print(f'Response: {resp.version} {resp.status}\n{headers}', flush=True)
        #if resp.body:
        #    print(f'\n{resp.body.rstrip()}', flush=True)
        #print('-' * WIDTH, flush=True)
        return resp

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

def main(args):
    global needs_position

    client = s = None
    headers = {'CSeq': '', 'User-Agent': UA}

    setup = session = play = describe = headers.copy()
    describe['Accept'] = 'application/sdp'
    setup['Transport'] = f'MP2T/H2221/UDP;unicast;client_port={args.client_port}'

    host = socket.gethostbyname(socket.gethostname())
    params = f'action=getCatchUpUrl&extInfoID={args.broadcast}&channelID={args.channel}&service=hd&mode=1'
    resp = urllib.request.urlopen(f'http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do?{params}')
    data = json.loads(resp.read())

    if data['resultCode'] != 0:
        print(f'Error: [{args.client_ip}] {repr(data)} {args.channel} {args.broadcast} -s {args.start[0]}', flush=True)
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

            play['Range'] = f'npt={args.start[0]}-end'
            play.update({'Scale': '1.000', 'x-playNow': '', 'x-noFlush': ''})
            play['Session'] = session['Session'] = r.headers['Session'].split(';')[0]

            get_parameter = session.copy()
            if needs_position:
                get_parameter.update({'Content-type': 'text/parameters', 'Content-length': 10})

            client.print(client.send_request('PLAY', play))
            while True:
                time.sleep(30)
                client.print(client.send_request('GET_PARAMETER', get_parameter))

        except Exception as ex:
            print(f'[{repr(ex)}] [{args.client_ip}] {args.channel} {args.broadcast} -s {args.start[0]} -p {args.client_port}',
                  flush=True)
        finally:
            if client and 'Session' in session:
                client.print(client.send_request('TEARDOWN', session), killed=args)

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser('Stream content from the Movistar U7D service.')
        parser.add_argument('channel', help='channel id')
        parser.add_argument('broadcast', help='broadcast id')
        parser.add_argument('--client_ip', '-i', help='client ip address')
        parser.add_argument('--client_port', '-p', help='client udp port')
        #parser.add_argument('--dumpfile', '-f', help='dump file path', default='test2.ts')
        parser.add_argument('--start', '-s', metavar='seconds', nargs=1, default=[0], type=int, help='stream start offset')
        args = parser.parse_args()
        if args.client_port is not None:
            args.client_port = int(args.client_port)
        elif not args.client_port:
            args.client_port = find_free_port()
        main(args)
    except KeyboardInterrupt:
        pass
    except Exception as ex:
        print(f'Died: /app/u7d.py {args.channel} {args.broadcast} -s {args.start[0]} -p {args.client_port} {repr(ex)}', flush=True)
