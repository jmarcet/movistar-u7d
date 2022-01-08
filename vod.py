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

from collections import namedtuple
from contextlib import closing
from ffmpeg import FFmpeg
from glob import glob
from json2xml import json2xml
from random import randint
from threading import Thread


CACHE_DIR = os.path.join(os.getenv("HOME", "/home"), ".xmltv/cache/programs")
RECORDINGS = os.getenv("RECORDINGS", "/tmp")
SANIC_EPG_URL = "http://127.0.0.1:8889"

IMAGENIO_URL = "http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg"
COVER_URL = f"{IMAGENIO_URL}/covers/programmeImages/portrait/290x429"
MVTV_URL = "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do"

NFO_EXT = "-movistar.nfo"
TMP_EXT = ".tmp"
TMP_EXT2 = ".tmp2"
UA = "MICA-IP-STB"

YEAR_SECONDS = 365 * 24 * 60 * 60

Response = namedtuple("Response", ["version", "status", "url", "headers", "body"])
VodData = namedtuple("VodData", ["client", "get_parameter", "session"])

ffmpeg = FFmpeg().option("y").option("xerror")
_ffmpeg = _log_prefix = _log_suffix = args = epg_url = filename = full_title = path = None
_nice = ("nice", "-n", "15", "ionice", "-c", "3")


class RtspClient(object):
    def __init__(self, reader, writer, url):
        self.reader = reader
        self.writer = writer
        self.url = url
        self.cseq = 1
        self.ip = None
        self.needs_position = False

    def close_connection(self):
        self.writer.close()

    def get_needs_position(self):
        return self.needs_position

    def needs_position(self):
        self.needs_position = True

    async def print(self, req, resp):
        WIDTH = 134
        headers = self.serialize_headers(resp.headers)
        sys.stderr.write("-" * WIDTH + "\n")
        sys.stderr.write("Request: " + req.split("\n")[0] + "\n")
        sys.stderr.write(f"Response: {resp.version} {resp.status}\n{headers}\n")
        if resp.body:
            sys.stderr.write(f"\n{resp.body.rstrip()}\n")
        sys.stderr.write("-" * WIDTH + "\n")

    async def read_message(self):
        resp = (await self.reader.read(4096)).decode()
        if " 200 OK" not in resp:
            version, status = resp.rstrip().split("\n")[0].split(" ", 1)
            return Response(version, status, self.url, {}, "")

        head, body = resp.split("\r\n\r\n")
        version, status = head.split("\r\n")[0].rstrip().split(" ", 1)

        headers = dict()
        for line in head.split("\r\n")[1:]:
            key, value = line.split(": ", 1)
            headers[key] = value

        if "Content-Length" in headers:
            if "a=control:rtsp:" in body:
                self.ip = body.split("\n")[-1].split(":", 1)[1].strip()
        else:
            body = ""

        return Response(version, status, self.url, headers, body)

    async def send_request(self, method, headers={}):
        if method == "OPTIONS":
            url = "*"
        elif method == "SETUP2":
            method = "SETUP"
            self.url, self.ip = self.ip, self.url
            url = self.url
        else:
            url = self.url

        headers["CSeq"] = self.cseq
        ser_headers = self.serialize_headers(headers)
        self.cseq += 1

        if method == "GET_PARAMETER" and self.needs_position:
            req = f"{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\nposition\r\n\r\n"
        else:
            req = f"{method} {url} RTSP/1.0\r\n{ser_headers}\r\n\r\n"

        self.writer.write(req.encode())
        resp = await self.read_message()
        # asyncio.create_task(self.print(req, resp))

        if resp.status != "200 OK" and method not in ["SETUP", "TEARDOWN"]:
            raise ValueError(f"{method} {resp}")

        return resp

    def serialize_headers(self, headers):
        return "\r\n".join(map(lambda x: "{0}: {1}".format(*x), headers.items()))


@ffmpeg.on("completed")
def ffmpeg_completed():
    sys.stderr.write(f"{_log_prefix} Recording ENDED: {_log_suffix}\n")

    command = list(_nice)
    command += ["mkvmerge", "--abort-on-warnings", "-q", "-o", filename + TMP_EXT2]
    if args.vo:
        command += ["--track-order", "0:2,0:1,0:4,0:3,0:6,0:5"]
        command += ["--default-track", "2:1"]
    command += [filename + TMP_EXT]
    try:
        proc = subprocess.run(command, capture_output=True)
        _cleanup(TMP_EXT)
        if proc.stdout:
            raise ValueError(proc.stdout.decode().replace("\n", " ").strip())

        recording_data = ujson.loads(
            subprocess.run(["mkvmerge", "-J", filename + TMP_EXT2], capture_output=True).stdout.decode()
        )

        duration = int(int(recording_data["container"]["properties"]["duration"]) / 1000000000)
        bad = duration + 30 < args.time
        sys.stderr.write(
            f"{_log_prefix} Recording {'INCOMPLETE:' if bad else 'COMPLETE:'} "
            f"[{duration}s] / {_log_suffix}\n"
        )
        missing_time = args.time - duration if bad else 0

        _cleanup(VID_EXT, args.mp4)
        if args.mp4:
            command = list(_nice)
            command += ["ffmpeg", "-i", filename + TMP_EXT2]
            command += ["-map", "0", "-c", "copy", "-sn", "-movflags", "+faststart"]
            command += ["-f", "mp4", "-v", "panic", filename + VID_EXT]
            if subprocess.run(command).returncode:
                _cleanup(VID_EXT)
                raise ValueError("Failed to write " + filename + VID_EXT)

            subs = [t for t in recording_data["tracks"] if t["type"] == "subtitles"]
            for track in subs:
                filesub = "%s.%s.sub" % (filename, track["properties"]["language"])
                command = list(_nice)
                command += ["ffmpeg", "-i", filename + TMP_EXT2]
                command += ["-map", "0:%d" % track["id"], "-c:s", "dvbsub"]
                command += ["-f", "mpegts", "-v", "panic", filesub]
                if subprocess.run(command).returncode:
                    _cleanup(VID_EXT, args.mp4)
                    raise ValueError("Failed to write " + filesub)

            _cleanup(TMP_EXT2)
        else:
            os.rename(filename + TMP_EXT2, filename + VID_EXT)
    except Exception as ex:
        sys.stderr.write(f"{_log_prefix} Recording FAILED: {_log_suffix} => {repr(ex)}\n")
        httpx.put(epg_url + f"?missing={randint(1, args.time)}")
        _cleanup(TMP_EXT2)
        return

    if not missing_time:
        save_metadata()
        httpx.put(epg_url)
    else:
        resp = httpx.put(epg_url + f"?missing={missing_time}")
        if resp.status_code == 200:
            save_metadata()
        else:
            _cleanup(VID_EXT, args.mp4)


@ffmpeg.on("error")
def ffmpeg_error(code):
    ffmpeg_completed()


# @ffmpeg.on('stderr')
# def ffmpeg_stderr(line):
#     if line.startswith('frame='):
#         return
#     sys.stderr.write(f'{_log_prefix} [ffmpeg] {line}\n')


@ffmpeg.on("terminated")
def ffmpeg_terminated():
    sys.stderr.write(f"{_log_prefix} [ffmpeg] TERMINATED: {_log_suffix}\n")
    ffmpeg_completed()


def _cleanup(ext, subs=False):
    if os.path.exists(filename + ext):
        os.remove(filename + ext)
    if subs:
        [os.remove(t) for t in glob(f"{filename}.*.sub")]


def _handle_cleanup(signum, frame):
    if __name__ == "__main__":
        raise TimeoutError()
    else:
        return


def find_free_port(iface=""):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind((iface, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def get_vod_info(channel_id, program_id, cloud, vod_client):
    params = "action=getRecordingData" if cloud else "action=getCatchUpUrl"
    params += f"&extInfoID={program_id}&channelID={channel_id}&mode=1"

    async with vod_client.get(f"{MVTV_URL}?{params}") as r:
        return (await r.json())["resultData"]


async def record_stream():
    global _ffmpeg, _log_suffix, ffmpeg, filename, full_title, path
    _options = {
        "map": "0",
        "c": "copy",
        "c:a:0": "aac",
        "c:a:1": "aac",
        "bsf:v": "h264_mp4toannexb",
        "metadata:s:a:0": "language=spa",
        "metadata:s:a:1": "language=eng",
        "metadata:s:a:2": "language=spa",
        "metadata:s:a:3": "language=eng",
        "metadata:s:s:0": "language=spa",
        "metadata:s:s:1": "language=eng",
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(epg_url)
    if resp.status_code == 200:
        data = resp.json()

        full_title, path, filename = [data[t] for t in ["full_title", "path", "filename"]]
        _options["metadata:s:v"] = f"title={full_title}"

        if not os.path.exists(path):
            sys.stderr.write(f"{_log_prefix} Creating recording subdir {path}\n")
            os.mkdir(path)
    else:
        filename = os.path.join(RECORDINGS, f"{args.channel}-{args.broadcast}")

    _log_suffix = f'[{args.time}s] {_log_suffix} "{filename[20:]}"'

    ffmpeg.input(
        f"udp://@{args.iptv_ip}:{args.client_port}", fifo_size=5572, pkt_size=1316, timeout=500000
    ).output(
        filename + TMP_EXT,
        _options,
        t=str(
            args.time + 600 if args.time > 7200 else args.time + 300 if args.time > 1800 else args.time + 60
        ),
        v="panic",
        vsync="0",
        f="matroska",
    )

    _ffmpeg = Thread(target=asyncio.run, args=(ffmpeg.execute(),))
    _ffmpeg.start()

    sys.stderr.write(f"{_log_prefix} Recording STARTED: {_log_suffix}\n")


def save_metadata():
    try:
        with open(os.path.join(CACHE_DIR, f"{args.broadcast}.json")) as f:
            metadata = ujson.loads(f.read())["data"]
        _cover = metadata["cover"]
        resp = httpx.get(f"{COVER_URL}/{_cover}")
        if resp.status_code == 200:
            image_ext = os.path.basename(_cover).split(".")[-1]
            _img_name = f"{filename}.{image_ext}"
            with open(_img_name, "wb") as f:
                f.write(resp.read())
            metadata["cover"] = os.path.basename(_img_name)
        if "covers" in metadata:
            if not os.path.exists(os.path.join(path + "/metadata")):
                os.mkdir(os.path.join(path + "/metadata"))
            for _img in metadata["covers"]:
                _cover = metadata["covers"][_img]
                resp = httpx.get(_cover)
                if resp.status_code != 200:
                    continue
                image_ext = os.path.basename(_cover).split(".")[-1]
                _img_name = f"{path}/metadata/{os.path.basename(filename)}-{_img}" f".{image_ext}"
                with open(_img_name, "wb") as f:
                    f.write(resp.read())
        metadata["title"] = full_title
        metadata.pop("covers", None)
        metadata.pop("logos", None)
        metadata.pop("name", None)
        with open(filename + NFO_EXT, "w") as f:
            f.write(json2xml.Json2xml(metadata, attr_type=False, pretty=True, wrapper="metadata").to_xml())

    except (FileNotFoundError, TypeError, ValueError) as ex:
        sys.stderr.write(f"{_log_prefix} No metadata found {repr(ex)}\n")


async def VodLoop(args, vod_data=None):
    if __name__ == "__main__":
        conn = aiohttp.TCPConnector()
        vod_client = aiohttp.ClientSession(connector=conn, headers={"User-Agent": UA})
        vod_data = await VodSetup(args, vod_client)
        if not isinstance(vod_data, VodData):
            return

        if args.write_to_file:
            await record_stream()

    elif not vod_data:
        return

    try:
        while True:
            await asyncio.sleep(30)
            if __name__ == "__main__":
                if args.write_to_file and _ffmpeg and not _ffmpeg.is_alive():
                    break
            await vod_data.client.send_request("GET_PARAMETER", vod_data.get_parameter)

    except (AttributeError, TimeoutError, TypeError, ValueError):
        pass
    except Exception as ex:
        sys.stderr.write(f"{_log_prefix} Error: {repr(ex)}\n")

    finally:
        try:
            await vod_data.client.send_request("TEARDOWN", vod_data.session)
        except (AttributeError, OSError, RuntimeError):
            pass

        try:
            vod_data.client.close_connection()
        except AttributeError:
            pass

        if __name__ == "__main__":
            if _ffmpeg:
                _ffmpeg.join()
            await vod_client.close()


async def VodSetup(args, vod_client):
    global _log_prefix, _log_suffix, epg_url

    _log_prefix = f"{'[' + args.client_ip + '] ' if args.client_ip else ''}[VOD:{os.getpid()}]"
    _log_suffix = f"{args.channel} {args.broadcast}"

    client = None
    headers = {"CSeq": "", "User-Agent": UA}

    setup = session = play = describe = headers.copy()
    describe["Accept"] = "application/sdp"
    setup["Transport"] = f"MP2T/H2221/UDP;unicast;client_port={args.client_port}"

    try:
        vod_info = await get_vod_info(args.channel, args.broadcast, args.cloud, vod_client)
        uri = urllib.parse.urlparse(vod_info["url"])
    except (aiohttp.client_exceptions.ClientConnectorError, ConnectionRefusedError):
        sys.stderr.write(f"{_log_prefix} Movistar IPTV catchup service DOWN\n")
        return True
    except KeyError:
        sys.stderr.write(f"{_log_prefix} Could not get uri for: {_log_suffix}\n")
        return False

    reader, writer = await asyncio.open_connection(uri.hostname, uri.port)
    epg_url = f"{SANIC_EPG_URL}/program_name/{args.channel}/{args.broadcast}"

    signal.signal(signal.SIGHUP, _handle_cleanup)
    signal.signal(signal.SIGTERM, _handle_cleanup)

    client = RtspClient(reader, writer, vod_info["url"])
    await client.send_request("OPTIONS", headers)
    await client.send_request("DESCRIBE", describe)

    r = await client.send_request("SETUP", setup)
    if r.status != "200 OK":
        client.needs_position()
        r = await client.send_request("SETUP2", setup)
        if r.status != "200 OK":
            sys.stderr.write(f"{r}\n")
            return

    play["Range"] = f"npt={args.start}-end"
    play.update({"Scale": "1.000", "x-playNow": "", "x-noFlush": ""})
    play["Session"] = session["Session"] = r.headers["Session"].split(";")[0]

    get_parameter = session.copy()
    if client.get_needs_position():
        get_parameter.update({"Content-type": "text/parameters", "Content-length": 10})

    await client.send_request("PLAY", play)

    if __name__ == "__main__":
        if args.write_to_file and not args.time:
            args.time = vod_info["duration"]

    return VodData(client, get_parameter, session)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Stream content from the Movistar VOD service.")
    parser.add_argument("channel", help="channel id")
    parser.add_argument("broadcast", help="broadcast id")
    parser.add_argument("--iptv_ip", "-a", help="iptv address")
    parser.add_argument("--client_ip", "-i", help="client ip address")
    parser.add_argument("--client_port", "-p", help="client udp port", type=int)
    parser.add_argument("--start", "-s", help="stream start offset", type=int)
    parser.add_argument("--time", "-t", help="recording time in seconds", type=int)
    parser.add_argument("--mp4", help="output split mp4 and vobsub files", type=bool, default=False)
    parser.add_argument("--cloud", help="the event is from a cloud recording", type=bool, default=False)
    parser.add_argument("--vo", help="set 2nd language as main one", type=bool, default=False)
    parser.add_argument("--write_to_file", "-w", help="record", action="store_true")

    args = parser.parse_args()

    if not args.iptv_ip:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("172.26.23.3", 53))
        args.iptv_ip = s.getsockname()[0]
        s.close

    if not args.client_port:
        args.client_port = find_free_port(args.iptv_ip)

    if args.mp4:
        VID_EXT = ".mp4"
    else:
        VID_EXT = ".mkv"

    try:
        asyncio.run(VodLoop(args))
        sys.exit(0)
    except (AttributeError, KeyboardInterrupt, FileNotFoundError, TimeoutError, ValueError):
        sys.exit(1)
    except Exception as ex:
        sys.stderr.write(f"{repr(ex)}\n")
        sys.exit(1)
