#!/usr/bin/env python3

import aiohttp
import argparse
import asyncio
import httpx
import logging as log
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
from dict2xml import dict2xml
from ffmpeg import FFmpeg
from glob import glob
from random import randint
from time import sleep
from threading import Thread


CACHE_DIR = os.path.join(os.getenv("HOME", os.getenv("HOMEPATH")), ".xmltv/cache/programs")
DEBUG = bool(int(os.getenv("DEBUG", 0)))
RECORDINGS = os.getenv("RECORDINGS", None)
SANIC_EPG_URL = "http://127.0.0.1:8889"

IMAGENIO_URL = "http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg"
COVER_URL = f"{IMAGENIO_URL}/covers/programmeImages/portrait/290x429"
MOVISTAR_DNS = "172.26.23.3"
MVTV_URL = "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do"

NFO_EXT = "-movistar.nfo"
TMP_EXT = ".tmp"
TMP_EXT2 = ".tmp2"
UA = "MICA-IP-STB"

YEAR_SECONDS = 365 * 24 * 60 * 60

Response = namedtuple("Response", ["version", "status", "url", "headers", "body"])
VodData = namedtuple("VodData", ["client", "get_parameter", "session"])
ffmpeg = FFmpeg().option("y").option("xerror")

_args = _epg_url = _ffmpeg = _filename = _full_title = _log_prefix = _log_suffix = _path = None
_nice = ("nice", "-n", "15", "ionice", "-c", "3") if os.name != "nt" else ()


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
        log.debug("-" * WIDTH)
        log.debug("Request: " + req.split("\n")[0])
        log.debug(f"Response: {resp.version} {resp.status}\n{headers}")
        if resp.body:
            log.debug(f"\n{resp.body.rstrip()}")
        log.debug("-" * WIDTH)

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
        if DEBUG:
            asyncio.create_task(self.print(req, resp))

        if resp.status != "200 OK" and method not in ["SETUP", "TEARDOWN"]:
            raise ValueError(f"{method} {resp}")

        return resp

    def serialize_headers(self, headers):
        return "\r\n".join(map(lambda x: "{0}: {1}".format(*x), headers.items()))


@ffmpeg.on("completed")
def ffmpeg_completed():
    log.info(f"{_log_prefix} Recording ENDED: {_log_suffix}")

    def timed_out(i, ex):
        if i < 2:
            log.warning(f"{_log_prefix} Recording ARCHIVE: {_log_suffix} => {repr(ex)}")
            sleep(15)
        else:
            log.error(f"{_log_prefix} Recording ARCHIVE: {_log_suffix} => {repr(ex)}")

    command = list(_nice)
    command += ["mkvmerge", "--abort-on-warnings", "-q", "-o", _filename + TMP_EXT2]
    if _args.vo:
        command += ["--track-order", "0:2,0:1,0:4,0:3,0:6,0:5"]
        command += ["--default-track", "2:1"]
    command += [_filename + TMP_EXT]
    try:
        proc = subprocess.run(command, capture_output=True)
        cleanup(TMP_EXT)
        if proc.stdout:
            raise ValueError(proc.stdout.decode().replace("\n", " ").strip())

        recording_data = ujson.loads(
            subprocess.run(["mkvmerge", "-J", _filename + TMP_EXT2], capture_output=True).stdout.decode()
        )

        duration = int(int(recording_data["container"]["properties"]["duration"]) / 1000000000)
        bad = duration + 30 < _args.time
        log.info(
            f"{_log_prefix} Recording {'INCOMPLETE:' if bad else 'COMPLETE:'} "
            f"[{duration}s] / {_log_suffix}"
        )
        missing_time = _args.time - duration if bad else 0

        cleanup(VID_EXT, _args.mp4)
        if _args.mp4:
            command = list(_nice)
            command += ["ffmpeg", "-i", _filename + TMP_EXT2]
            command += ["-map", "0", "-c", "copy", "-sn", "-movflags", "+faststart"]
            command += ["-f", "mp4", "-v", "panic", _filename + VID_EXT]
            if subprocess.run(command).returncode:
                cleanup(VID_EXT)
                raise ValueError("Failed to write " + _filename + VID_EXT)

            subs = [t for t in recording_data["tracks"] if t["type"] == "subtitles"]
            for track in subs:
                filesub = "%s.%s.sub" % (_filename, track["properties"]["language"])
                command = list(_nice)
                command += ["ffmpeg", "-i", _filename + TMP_EXT2]
                command += ["-map", "0:%d" % track["id"], "-c:s", "dvbsub"]
                command += ["-f", "mpegts", "-v", "panic", filesub]
                if subprocess.run(command).returncode:
                    cleanup(VID_EXT, _args.mp4)
                    raise ValueError("Failed to write " + filesub)

            cleanup(TMP_EXT2)
        else:
            os.rename(_filename + TMP_EXT2, _filename + VID_EXT)
    except Exception as ex:
        log.error(f"{_log_prefix} Recording FAILED: {_log_suffix} => {repr(ex)}")
        cleanup(TMP_EXT2)
        for i in range(3):
            try:
                httpx.put(_epg_url + f"?missing={randint(1, _args.time)}")
            except (httpx.ConnectError, httpx.ReadTimeout) as ex:
                timed_out(i, ex)
        return

    resp = None
    if not missing_time:
        for i in range(3):
            try:
                resp = httpx.put(_epg_url)
                break
            except (httpx.ConnectError, httpx.ReadTimeout) as ex:
                timed_out(i, ex)
    else:
        for i in range(3):
            try:
                resp = httpx.put(_epg_url + f"?missing={missing_time}")
                break
            except (httpx.ConnectError, httpx.ReadTimeout) as ex:
                timed_out(i, ex)

    if resp and resp.status_code == 200:
        save_metadata()
    else:
        cleanup(VID_EXT, _args.mp4)


@ffmpeg.on("error")
def ffmpeg_error(code):
    ffmpeg_completed()


# @ffmpeg.on("stderr")
# def ffmpeg_stderr(line):
#     if line.startswith("frame="):
#         return
#     log.debug(f"{_log_prefix} [ffmpeg] {line}")


@ffmpeg.on("terminated")
def ffmpeg_terminated():
    log.info(f"{_log_prefix} [ffmpeg] TERMINATED: {_log_suffix}")
    ffmpeg_completed()


def check_dns():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((MOVISTAR_DNS, 53))
        return s.getsockname()[0]
    except Exception as ex:
        log.error("Unable to connect to Movistar DNS")
    finally:
        s.close()


def cleanup(ext, subs=False):
    if os.path.exists(_filename + ext):
        os.remove(_filename + ext)
    if subs:
        [os.remove(t) for t in glob(f"{_filename}.*.sub")]


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


def handle_cleanup(signum, frame):
    if __name__ == "__main__":
        raise TimeoutError()
    else:
        return


async def record_stream():
    global _ffmpeg, _filename, _full_title, _log_suffix, _path
    options = {
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
        resp = await client.get(_epg_url)
    if resp.status_code == 200:
        data = resp.json()

        _full_title, _path, _filename = [data[t] for t in ["full_title", "path", "filename"]]
        options["metadata:s:v"] = f"title={_full_title}"

        if not os.path.exists(_path):
            log.info(f"{_log_prefix} Creating recording subdir {_path}")
            os.mkdir(_path)
    else:
        _filename = os.path.join(RECORDINGS, f"{_args.channel}-{_args.broadcast}")

    _log_suffix += f' "{_filename[len(RECORDINGS) + 1:]}"'

    ffmpeg.input(
        f"udp://@{_args.iptv_ip}:{_args.client_port}", fifo_size=5572, pkt_size=1316, timeout=500000
    ).output(
        _filename + TMP_EXT,
        options,
        t=str(
            _args.time + 600
            if _args.time > 7200
            else _args.time + 300
            if _args.time > 1800
            else _args.time + 60
        ),
        v="panic",
        vsync="0",
        f="matroska",
    )

    _ffmpeg = Thread(target=asyncio.run, args=(ffmpeg.execute(),))
    _ffmpeg.start()

    log.info(f"{_log_prefix} Recording STARTED: {_log_suffix}")


def save_metadata():
    try:
        with open(os.path.join(CACHE_DIR, f"{_args.broadcast}.json")) as f:
            metadata = ujson.loads(f.read())["data"]
    except (FileNotFoundError, TypeError, ValueError) as ex:
        log.warning(f"{_log_prefix} Extended info not found: {repr(ex)}")
        return

    for t in ["beginTime", "endTime", "expDate"]:
        metadata[t] = int(metadata[t] / 1000)
    cover = metadata["cover"]
    resp = httpx.get(f"{COVER_URL}/{cover}")
    if resp.status_code == 200:
        img_ext = os.path.splitext(cover)[1]
        img_name = _filename + img_ext
        with open(img_name, "wb") as f:
            f.write(resp.read())
        metadata["cover"] = os.path.basename(img_name)
    if "covers" in metadata:
        covers = {}
        metadata_dir = os.path.join(_path, "metadata")
        if not os.path.exists(metadata_dir):
            os.mkdir(metadata_dir)
        for img in metadata["covers"]:
            cover = metadata["covers"][img]
            resp = httpx.get(cover)
            if resp.status_code != 200:
                continue
            img_ext = os.path.splitext(cover)[1]
            img_rel = f"{os.path.basename(_filename)}-{img}" + img_ext
            img_name = os.path.join(metadata_dir, img_rel)
            with open(img_name, "wb") as f:
                f.write(resp.read())
            covers[img] = os.path.join("metadata", img_rel)
        if covers:
            metadata["covers"] = covers
        else:
            metadata.pop("covers", None)
            os.rmdir(metadata_dir)
    metadata["title"] = _full_title
    metadata.pop("logos", None)
    metadata.pop("name", None)
    with open(_filename + NFO_EXT, "w", encoding="utf8") as f:
        f.write(dict2xml(metadata, wrap="metadata", indent="    "))


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
        log.error(f"{_log_prefix}: {repr(ex)}")

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
    global _epg_url, _log_prefix, _log_suffix

    _epg_url = f"{SANIC_EPG_URL}/program_name/{args.channel}/{args.broadcast}"
    _log_prefix = f"{('[' + args.client_ip + ']') if args.client_ip else ''}"

    client = None
    headers = {"CSeq": "", "User-Agent": UA}

    setup = session = play = describe = headers.copy()
    describe["Accept"] = "application/sdp"
    setup["Transport"] = f"MP2T/H2221/UDP;unicast;client_port={args.client_port}"

    try:
        vod_info = await get_vod_info(args.channel, args.broadcast, args.cloud, vod_client)
        _log_suffix = "[%ds] [%s] [%s] [%d]" % (
            vod_info["duration"],
            vod_info["channelName"],
            args.channel,
            vod_info["beginTime"] / 1000,
        )
        uri = urllib.parse.urlparse(vod_info["url"])
    except Exception as ex:
        if __name__ == "__main__":
            await vod_client.close()
        if isinstance(ex, (aiohttp.client_exceptions.ClientConnectorError, ConnectionRefusedError)):
            log.error(f"{_log_prefix} Movistar IPTV catchup service DOWN")
            if __name__ == "__main__":
                httpx.put(_epg_url + f"?missing={randint(1, args.time)}")
        else:
            log.error(f"{_log_prefix} Could not get uri for: [{args.channel}] [{args.broadcast}]: {repr(ex)}")
        return

    if os.name != "nt":
        signal.signal(signal.SIGHUP, handle_cleanup)
    signal.signal(signal.SIGTERM, handle_cleanup)

    reader, writer = await asyncio.open_connection(uri.hostname, uri.port)
    client = RtspClient(reader, writer, vod_info["url"])
    await client.send_request("OPTIONS", headers)
    await client.send_request("DESCRIBE", describe)

    r = await client.send_request("SETUP", setup)
    if r.status != "200 OK":
        client.needs_position()
        r = await client.send_request("SETUP2", setup)
        if r.status != "200 OK":
            log.error(f"{r}")
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
    parser.add_argument("--cloud", help="the event is from a cloud recording", action="store_true")
    parser.add_argument("--debug", help="enable debug logs", action="store_true")
    parser.add_argument("--mp4", help="output split mp4 and vobsub files", action="store_true")
    parser.add_argument("--vo", help="set 2nd language as main one", action="store_true")
    parser.add_argument("--write_to_file", "-w", help="record", action="store_true")

    _args = parser.parse_args()

    if _args.debug:
        DEBUG = True

    log.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] [VOD] [%(levelname)s] %(message)s",
        level=log.DEBUG if DEBUG else log.INFO,
    )

    if _args.write_to_file and not RECORDINGS:
        log.error(f"RECORDINGS path not set")
        sys.exit(1)

    if not _args.iptv_ip:
        _args.iptv_ip = check_dns()
        if not _args.iptv_ip:
            sys.exit(1)

    if not _args.client_port:
        _args.client_port = find_free_port(_args.iptv_ip)

    if _args.mp4:
        VID_EXT = ".mp4"
    else:
        VID_EXT = ".mkv"

    try:
        asyncio.run(VodLoop(_args))
        sys.exit(0)
    except (AttributeError, KeyboardInterrupt, FileNotFoundError, TimeoutError, ValueError):
        sys.exit(1)
    except Exception as ex:
        log.error(f"{repr(ex)}")
        sys.exit(1)
