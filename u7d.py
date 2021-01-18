#!/usr/bin/env python3
import collections
import urllib.request
import urllib.parse
import subprocess
import threading
import argparse
import socket
import json
import time
import os

from contextlib import closing

class RtspClient(object):
    def __init__(self, sock, url):
        self.sock = sock
        self.url = url
        self.cseq = 1

    def read_message(self, resp):
        Response = collections.namedtuple('Response', ['version', 'status', 'headers'])
        version, status = resp.readline().rstrip().split(' ', 1)
        headers = dict()
        while True:
            line = resp.readline().rstrip()
            if not line:
                break
            key, value = line.split(': ', 1)
            headers[key] = value
        return Response(version, status, headers)

    def serialize_headers(self, headers):
        return '\r\n'.join(map(lambda x: '{0}: {1}'.format(*x), headers.items()))

    def send_request(self, method, headers={}):
        Request = collections.namedtuple('Request', ['request', 'response'])
        url = '*' if method == 'OPTIONS' else self.url
        headers['CSeq'] = self.cseq
        headers = self.serialize_headers(headers)
        self.cseq += 1
        req = f'{method} {url} RTSP/1.0\r\n{headers}\r\n\r\n'
        self.sock.send(req.encode())
        with self.sock.makefile('r') as f:
            resp = self.read_message(f)
        return Request(req, resp)

    def print(self, req):
        resp = req.response
        headers = self.serialize_headers(resp.headers)
        print('-' * 100)
        print('Request: ' + req.request, end='', flush=True)
        print(f'Response: {resp.version} {resp.status}\n{headers}\n')
        return resp

def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

def main(args):
    params = f'action=getCatchUpUrl&extInfoID={args.broadcast}&channelID={args.channel}&service={args.quality}&mode=1'
    resp = urllib.request.urlopen(f'http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do?{params}')
    data = json.loads(resp.read())
    if data['resultCode'] != 0:
        print(f'error: {data["resultText"]}')
        return
    url = data['resultData']['url']
    print(f'rolling_buffer: {url}')
    uri = urllib.parse.urlparse(url)
    print('Connecting to %s:%d' % (uri.hostname, uri.port))
    headers = {'User-Agent': 'MICA-IP-STB'}
    client_port = str(find_free_port())
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((uri.hostname, uri.port))
        client = RtspClient(s, url)
        client.print(client.send_request('OPTIONS', headers))
        client.print(client.send_request('DESCRIBE', headers))
        setup = headers.copy()
        setup['Transport'] = f'MP2T/H2221/UDP;unicast;client_port={client_port}'
        r = client.print(client.send_request('SETUP', setup))
        play = headers.copy()
        play['Session'] = r.headers['Session'].split(';')[0]
        play['Range'] = 'npt={0}-end'.format(*args.start)
        client.print(client.send_request('PLAY', play))
        host = socket.gethostbyname(socket.gethostname())
        stream = f'udp://@{host}:{client_port}'
        session = headers.copy()
        session['Session'] = play['Session']
        def keep_alive():
            while True:
                time.sleep(30)
                client.print(client.send_request('GET_PARAMETER', session))
        thread = threading.Thread(target=keep_alive)
        thread.daemon = True
        thread.start()
        try:
            command = ['socat']
            command.append('-u')
            command.append(f'UDP4-LISTEN:{client_port}')
            command.append('UDP4-DATAGRAM:239.1.0.1:6667,broadcast')
            #command.append('OPEN:/storage/timeshift/u7d/test2.ts,creat,trunc')
            print(f'Opening {stream} with {command}')
            subprocess.call(command)
        except KeyboardInterrupt:
            pass
        finally:
            client.print(client.send_request('TEARDOWN', session))

if __name__ == '__main__':
    services = {'sd': 1, 'hd': 2}
    parser = argparse.ArgumentParser('Stream content from the Movistar U7D service.')
    parser.add_argument('channel', help='channel id')
    parser.add_argument('broadcast', help='broadcast id')
    parser.add_argument('--dumpfile', '-f', help='dump file path', default='test2.ts')
    parser.add_argument('--duration', '-t', default=[0], type=int, help='duration in seconds')
    parser.add_argument('--quality', '-q', choices=services, nargs=1, default=['hd'], help='stream quality')
    parser.add_argument('--start', '-s', metavar='seconds', nargs=1, default=[0], type=int, help='stream start offset')
    args = parser.parse_args()
    args.quality = services.get(*args.quality)
    main(args)
