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
        self.ip = None

    def read_message(self, resp):
        Response = collections.namedtuple('Response', ['version', 'status', 'headers'])
        temp = resp.readline().rstrip()
        print(f'read_message {temp}')
        #version, status = resp.readline().rstrip().split(' ', 1)
        version, status = temp.split(' ', 1)
        headers = dict()
        while True:
            line = resp.readline().rstrip()
            if not line:
                break
            try:
                key, value = line.split(': ', 1)
            except:
                continue
            headers[key] = value
        return Response(version, status, headers)

    def serialize_headers(self, headers):
        return '\r\n'.join(map(lambda x: '{0}: {1}'.format(*x), headers.items()))

    def send_request(self, method, headers={}):
        Request = collections.namedtuple('Request', ['request', 'response'])
        url = '*' if method == 'OPTIONS' else self.url
        headers['CSeq'] = self.cseq
        ser_headers = self.serialize_headers(headers)
        self.cseq += 1
        if method == 'GET_PARAMETER':
            req = f'{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\nposition\r\n\r\n'
        else:
            req = f'{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\n'
        self.sock.send(req.encode())
        if method == 'DESCRIBE':
            resp = self.sock.recv(4096).decode()
            print(f'send_request resp={resp}')
            recs = resp.split('\r\n')
            version, status = recs[0].rstrip().split(' ', 1)
            self.ip = recs[-1].split(':', 1)[1:][0].rstrip()
            Response = collections.namedtuple('Response', ['version', 'status', 'headers'])
            resp = Response(version, status, headers)
            print(f'send_request DESCRIBE resp={resp}')
        elif method == 'GET_PARAMETER':
            resp = self.sock.recv(4096).decode()
            #print(f'send_request GET_PARAMETER resp={resp}')
            recs = resp.split('\r\n')
            version, status = recs[0].rstrip().split(' ', 1)
            recs = recs[1:]
            headers = dict()
            for linenr in range(len(recs)):
                line = recs[linenr]
                if not line:
                    break
                key, value = line.split(': ', 1)
                headers[key] = value
            linenr += 1
            if linenr < len(recs):
                body = recs[linenr:]
            print(body)
            Response = collections.namedtuple('Response', ['version', 'status', 'headers'])
            resp = Response(version, status, headers)
        else:
            with self.sock.makefile('r') as f:
                resp = self.read_message(f)
            if method == 'SETUP' and resp.status != '200 OK':
                print(f'send_request SETUP 1 resp={resp}')
                self.url = self.ip
                headers['CSeq'] = self.cseq
                self.cseq += 1
                ser_headers = self.serialize_headers(headers)
                req = f'{method} {self.url} RTSP/1.0\r\n{ser_headers}\r\n\r\n'
                self.sock.send(req.encode())
                with self.sock.makefile('r') as f:
                    resp = self.read_message(f)
                    print(f'send_request SETUP 2 resp={resp}')
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
    headers = {'CSeq': '', 'User-Agent': 'MICA-IP-STB'}
    client_port = str(find_free_port())
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((uri.hostname, uri.port))
        client = RtspClient(s, url)
        client.print(client.send_request('OPTIONS', headers))
        describe = headers.copy()
        describe['Accept'] = 'application/sdp'
        client.print(client.send_request('DESCRIBE', describe))
        setup = headers.copy()
        setup['Transport'] = f'MP2T/H2221/UDP;unicast;client_port={client_port}'
        setup['x-mayNotify'] = ''
        r = client.print(client.send_request('SETUP', setup))
        play = headers.copy()
        play = {'CSeq': '', 'Session': '', 'User-Agent': 'MICA-IP-STB'}
        play['Session'] = r.headers['Session'].split(';')[0]
        play['Range'] = 'npt={0}-end'.format(*args.start)
        play['Scale'] = '1.000'
        play['x-playNow'] = ''
        play['x-noFlush'] = ''
        client.print(client.send_request('PLAY', play))
        host = socket.gethostbyname(socket.gethostname())
        stream = f'udp://@{host}:{client_port}'
        session = {'User-Agent': 'MICA-IP-STB', 'Session': '', 'CSeq': ''}
        session['Session'] = play['Session']
        get_parameter = session.copy()
        get_parameter['Content-type'] = 'text/parameters'
        get_parameter['Content-length'] = 10
        def keep_alive():
            while True:
                time.sleep(30)
                client.print(client.send_request('GET_PARAMETER', get_parameter))
        thread = threading.Thread(target=keep_alive)
        thread.daemon = True
        thread.start()
        try:
            command = ['socat']
            command.append('-u')
            command.append(f'UDP4-LISTEN:{client_port},tos=40')
            command.append('UDP4-DATAGRAM:239.1.0.1:6667,broadcast,keepalive,tos=40')
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
