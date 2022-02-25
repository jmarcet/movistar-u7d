#!/usr/bin/env python3
#
# Based on MovistarU7D by XXLuigiMario:
# Source: https://github.com/XXLuigiMario/MovistarU7D

import aiofiles
import aiohttp
import argparse
import asyncio
import logging as log
import os
import re
import signal
import socket
import subprocess
import sys
import timeit
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from collections import namedtuple
from contextlib import closing
from dict2xml import dict2xml
from ffmpeg import FFmpeg
from glob import glob
from random import randint
from time import sleep
from threading import Thread

try:
    from asyncio.exceptions import CancelledError
except ModuleNotFoundError:
    from asyncio import CancelledError

from version import _version


CACHE_DIR = os.path.join(os.getenv("HOME", os.getenv("HOMEPATH")), ".xmltv/cache/programs")
DEBUG = bool(int(os.getenv("DEBUG", 0)))
NOSUBS = bool(int(os.getenv("NOSUBS", 0)))
RECORDINGS = os.getenv("RECORDINGS", "").rstrip("/").rstrip("\\")
SANIC_EPG_URL = "http://127.0.0.1:8889"
LOCK_FILE = os.path.join(os.getenv("TMP", "/tmp"), ".u7d-pp.lock")

IMAGENIO_URL = "http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg"
COVER_URL = f"{IMAGENIO_URL}/covers/programmeImages/portrait/290x429"
MOVISTAR_DNS = "172.26.23.3"
MVTV_URL = "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do"

NFO_EXT = "-movistar.nfo"
TMP_EXT = ".tmp"
TMP_EXT2 = ".tmp2"
UA = "libcurl-agent/1.0 [IAL] WidgetManager Safari/538.1 CAP:803fd12a 1"

WIN32 = sys.platform == "win32"

YEAR_SECONDS = 365 * 24 * 60 * 60

Response = namedtuple("Response", ["version", "status", "url", "headers", "body"])
VodData = namedtuple("VodData", ["client", "get_parameter", "session"])
ffmpeg = FFmpeg().option("y").option("xerror")

_IPTV = _LOOP = _PP_DONE = _SESSION = _SESSION_CLOUD = None

_args = _epg_url = _ffmpeg = _filename = _full_title = _log_suffix = _path = _start = None
_nice = ("nice", "-n", "15", "ionice", "-c", "3", "flock", LOCK_FILE) if not WIN32 else ()


if not WIN32:
    from setproctitle import setproctitle

    setproctitle("vod %s" % " ".join(sys.argv[1:]))


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

    def does_need_position(self):
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
    global _log_suffix
    record_time = int(timeit.default_timer() - _start)
    _log_suffix = f"[~{record_time}s] / {_log_suffix}"
    log.info(f"Recording ENDED: {_log_suffix}")
    asyncio.run_coroutine_threadsafe(postprocess(), _LOOP).result()


@ffmpeg.on("error")
def ffmpeg_error(code):
    global _log_suffix
    record_time = int(timeit.default_timer() - _start)
    _log_suffix = f"[~{record_time}s] / {_log_suffix}"
    bad = record_time < (_args.time - 30)
    asyncio.run_coroutine_threadsafe(postprocess(record_time if bad else 0), _LOOP).result()


@ffmpeg.on("stderr")
def ffmpeg_stderr(line):
    if line.startswith("frame="):
        return
    log.info(f"[ffmpeg] {line}")


@ffmpeg.on("terminated")
def ffmpeg_terminated():
    log.error(f"[ffmpeg] TERMINATED: {_log_suffix}")
    ffmpeg_error()


def check_dns():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((MOVISTAR_DNS, 53))
        return s.getsockname()[0]
    finally:
        s.close()


def cleanup(ext, meta=False, subs=False):
    if os.path.exists(_filename + ext):
        os.remove(_filename + ext)
    if meta:
        [os.remove(t) for t in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*.jpg")]
    if subs:
        [os.remove(t) for t in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*.sub")]


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
    raise TimeoutError()


async def postprocess(record_time=0):
    global _PP_DONE, _log_suffix
    log.debug(f"Recording Postprocess: {_log_suffix}")

    if not WIN32:
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

    try:
        if record_time:
            missing = _args.time - record_time
            resp = await _SESSION.options(_epg_url + f"?missing={missing}")

            if resp and resp.status == 200:
                return await postprocess()
            else:
                raise ValueError(f"Too short, missing: {missing}s")

        img_mime, img_name = await save_metadata()

        command = list(_nice)
        command += ["mkvmerge", "--abort-on-warnings", "-q", "-o", _filename + TMP_EXT2]
        command += ["--attachment-mime-type", img_mime, "--attach-file", img_name]
        if _args.vo:
            command += ["--track-order", "0:2,0:1,0:4,0:3,0:6,0:5"]
            command += ["--default-track", "2:1"]
        command += [_filename + TMP_EXT]

        proc = subprocess.run(command, capture_output=True)
        cleanup(TMP_EXT)
        if proc.stdout:
            raise ValueError(proc.stdout.decode().replace("\n", " ").strip())

        recording_data = ujson.loads(
            subprocess.run(["mkvmerge", "-J", _filename + TMP_EXT2], capture_output=True).stdout.decode()
        )

        duration = int(int(recording_data["container"]["properties"]["duration"]) / 1000000000)
        bad = duration < (_args.time - 30)
        _log_suffix = f"[{duration}s] / {_log_suffix}"
        msg = f"Recording {'INCOMPLETE:' if bad else 'COMPLETE:'} {_log_suffix}"
        log.warning(msg) if bad else log.info(msg)
        missing_time = (_args.time - duration) if bad else 0
        _log_suffix = _log_suffix[_log_suffix.find(" - ") + 3 :]

        cleanup(VID_EXT, subs=_args.mp4)
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
                    cleanup(VID_EXT, meta=True, subs=True)
                    raise ValueError("Failed to write " + filesub)

            cleanup(TMP_EXT2)
        else:
            os.rename(_filename + TMP_EXT2, _filename + VID_EXT)

        resp = None
        if not missing_time:
            resp = await _SESSION.put(_epg_url)
        else:
            resp = await _SESSION.put(_epg_url + f"?missing={missing_time}")

        if resp and resp.status == 200:
            await save_metadata(extra=True)
        else:
            cleanup(VID_EXT, meta=True, subs=_args.mp4)

    except Exception as ex:
        log.error(f"Recording FAILED: {_log_suffix} => {repr(ex)}")
        if not record_time:
            try:
                async with aiohttp.ClientSession() as session:
                    await session.put(_epg_url + f"?missing={randint(1, _args.time)}")
            except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
                pass
        cleanup(TMP_EXT2)
        cleanup(VID_EXT, meta=True, subs=True)
        if os.path.exists(LOCK_FILE):
            try:
                os.remove(LOCK_FILE)
            except FileNotFoundError:
                pass

    finally:
        log.debug(f"Recording Postprocess ENDED: {_log_suffix}")
        _PP_DONE = True


async def record_stream():
    global _ffmpeg, _filename, _full_title, _log_suffix, _path, _start
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

    async with _SESSION.get(_epg_url) as resp:
        if resp.status == 200:
            _, _path, _filename = (await resp.json()).values()
            options["metadata:s:v"] = f"title={os.path.basename(_filename)}"

            if not os.path.exists(_path):
                log.debug(f"Creating recording subdir {_path}")
                os.makedirs(_path)
        else:
            _filename = os.path.join(RECORDINGS, f"{_args.channel}-{_args.broadcast}")

    _log_suffix += f' "{_filename[len(RECORDINGS) + 1:]}"'

    if _args.channel in ("578", "884", "3603") or NOSUBS:
        # matroska is not compatible with dvb_teletext subs
        # Boing, DKISS & Energy use them, so we drop them
        global ffmpeg
        log.warning(f"Recording dropping dvb_teletext subs: {_log_suffix}")
        ffmpeg = ffmpeg.option("-sn")

    ffmpeg.input(f"udp://@{_IPTV}:{_args.client_port}", fifo_size=5572, pkt_size=1316, timeout=500000).output(
        _filename + TMP_EXT,
        options,
        t=str(_args.time),
        v="panic",
        vsync="0",
        f="matroska",
    )

    _ffmpeg = Thread(target=asyncio.run, args=(ffmpeg.execute(),))
    _ffmpeg.start()

    log.info(f"Recording STARTED: {_log_suffix}")
    _start = timeit.default_timer()


async def save_metadata(extra=False):
    log.debug(f"Saving metadata: {_log_suffix}")
    cache_metadata = os.path.join(CACHE_DIR, f"{_args.broadcast}.json")
    try:
        if os.path.exists(cache_metadata):
            async with aiofiles.open(cache_metadata, encoding="utf8") as f:
                metadata = ujson.loads(await f.read())["data"]
        else:
            log.info(f"Getting extended info: {_log_suffix}")
            async with _SESSION_CLOUD.get(
                f"{MVTV_URL}?action=epgInfov2&productID={_args.broadcast}&channelID={_args.channel}&extra=1"
            ) as resp:
                metadata = (await resp.json())["resultData"]
            async with aiofiles.open(cache_metadata, "w", encoding="utf8") as f:
                await f.write(ujson.dumps({"data": metadata}, ensure_ascii=False, indent=4, sort_keys=True))
    except (TypeError, ValueError) as ex:
        log.warning(f"Extended info not found: {_log_suffix} => {repr(ex)}")
        return

    if not extra:
        for t in ["beginTime", "endTime", "expDate"]:
            metadata[t] = int(metadata[t] / 1000)
        cover = metadata["cover"]
        log.debug(f'Getting cover "{cover}": {_log_suffix}')
        async with _SESSION_CLOUD.get(f"{COVER_URL}/{cover}") as resp:
            if resp.status == 200:
                log.debug(f'Got cover "{cover}": {_log_suffix}')
                img_ext = os.path.splitext(cover)[1]
                img_name = _filename + img_ext
                log.debug(f'Saving cover "{cover}" -> "{img_name}": {_log_suffix}')
                async with aiofiles.open(img_name, "wb") as f:
                    await f.write(await resp.read())
                metadata["cover"] = os.path.basename(img_name)
            else:
                log.debug(f'Failed to get cover "{cover}": {_log_suffix}')
        img_mime = "image/jpeg" if img_ext in (".jpeg", ".jpg") else "image/png"
        return img_mime, img_name

    if "covers" in metadata:
        covers = {}
        metadata_dir = os.path.join(_path, "metadata")
        if not os.path.exists(metadata_dir):
            os.mkdir(metadata_dir)
        for img in metadata["covers"]:
            cover = metadata["covers"][img]
            log.debug(f'Getting covers "{img}": {_log_suffix}')
            async with _SESSION_CLOUD.get(cover) as resp:
                if resp.status != 200:
                    log.debug(f'Failed to get covers "{img}": {_log_suffix}')
                    continue
                log.debug(f'Got covers "{img}": {_log_suffix}')
                img_ext = os.path.splitext(cover)[1]
                img_rel = f"{os.path.basename(_filename)}-{img}" + img_ext
                img_name = os.path.join(metadata_dir, img_rel)
                log.debug(f'Saving covers "{img}" -> "{img_name}": {_log_suffix}')
                async with aiofiles.open(img_name, "wb") as f:
                    await f.write(await resp.read())
                covers[img] = os.path.join("metadata", img_rel)
        if covers:
            metadata["covers"] = covers
        else:
            metadata.pop("covers", None)
            os.rmdir(metadata_dir)
    metadata["title"] = _full_title
    for t in ("isuserfavorite", "lockdata", "logos", "name", "playcount", "resume", "watched"):
        metadata.pop(t, None)
    async with aiofiles.open(_filename + NFO_EXT, "w", encoding="utf8") as f:
        await f.write(dict2xml(metadata, wrap="metadata", indent="    "))
    log.debug(f"Metadata saved: {_log_suffix}")


async def vod_ongoing(channel_id, program_id):
    cmd = f"vod.* {channel_id} {program_id}.* -w"

    if WIN32:
        from wmi import WMI

        return len(
            [
                process.CommandLine
                for process in WMI().Win32_Process(name="vod.exe")
                if cmd in process.CommandLine
            ]
        )

    p = await asyncio.create_subprocess_exec("pgrep", "-af", cmd, stdout=asyncio.subprocess.PIPE)
    stdout, _ = await p.communicate()
    return len(stdout.splitlines())


async def VodLoop(args, vod_data=None):
    if __name__ == "__main__":
        global _SESSION_CLOUD

        _SESSION_CLOUD = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                keepalive_timeout=YEAR_SECONDS,
                resolver=AsyncResolver(nameservers=[MOVISTAR_DNS]) if not WIN32 else None,
            ),
            headers={"User-Agent": UA},
            json_serialize=ujson.dumps,
        )

        vod_data = await VodSetup(args, _SESSION_CLOUD)
        if not isinstance(vod_data, VodData):
            await _SESSION_CLOUD.close()
            return

        if args.write_to_file:
            global _LOOP, _SESSION

            _LOOP = asyncio.get_event_loop()
            _SESSION = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS),
                json_serialize=ujson.dumps,
            )

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

    except Exception as ex:
        log.debug(f"1 {repr(ex)}")

    finally:
        try:
            await vod_data.client.send_request("TEARDOWN", vod_data.session)
            if __name__ == "__main__" and not WIN32:
                signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        except Exception as ex:
            log.debug(f"2 {repr(ex)}")

        try:
            vod_data.client.close_connection()
        except Exception as ex:
            log.debug(f"3 {repr(ex)}")

        if __name__ == "__main__":
            if args.write_to_file:
                while True:
                    if _PP_DONE:
                        break
                    await asyncio.sleep(1)
                _ffmpeg.join(timeout=1)
                await asyncio.sleep(1)
                await _SESSION.close()
            await _SESSION_CLOUD.close()


async def VodSetup(args, vod_client, failed=False):
    global _epg_url

    _epg_url = f"{SANIC_EPG_URL}/program_name/{args.channel}/{args.broadcast}"
    log_prefix = f"{('[' + args.client_ip + '] ') if args.client_ip else ''}"

    if __name__ == "__main__":
        if args.write_to_file and (await vod_ongoing(args.channel, args.broadcast)) > 1:
            log.error(f"{log_prefix}Recording already ongoing: [{args.channel}] [{args.broadcast}]")
            return

    client = None
    headers = {"CSeq": "", "User-Agent": "MICA-IP-STB"}

    setup = session = play = describe = headers.copy()
    describe["Accept"] = "application/sdp"
    setup["Transport"] = f"MP2T/H2221/UDP;unicast;client_port={args.client_port}"

    try:
        vod_info = await get_vod_info(args.channel, args.broadcast, args.cloud, vod_client)
        if __name__ == "__main__" and args.write_to_file:
            global _args, _log_suffix
            if not _args.time:
                _args.time = vod_info["duration"]
            else:
                _args.time = min(
                    vod_info["duration"],
                    _args.time + 600
                    if _args.time > 7200
                    else _args.time + 300
                    if _args.time > 1800
                    else _args.time + 60,
                )
            _log_suffix = "[%ds] - [%s] [%s] [%d] [%s]" % (
                _args.time,
                vod_info["channelName"],
                args.channel,
                vod_info["beginTime"] / 1000,
                args.broadcast,
            )
        uri = urllib.parse.urlparse(vod_info["url"])
    except Exception as ex:
        if isinstance(ex, (ClientConnectorError, ConnectionRefusedError)):
            log.error(f"{log_prefix}Movistar IPTV catchup service DOWN")
            if __name__ == "__main__":
                if args.write_to_file:
                    await _SESSION.put(_epg_url + f"?missing={randint(1, args.time)}")
        elif not failed:
            log.warning(f"{log_prefix}[{args.channel}] [{args.broadcast}]: {repr(ex)}")
            return await VodSetup(args, vod_client, True)
        else:
            log.error(f"{log_prefix}Could not get uri for: [{args.channel}] [{args.broadcast}]: {repr(ex)}")
        return

    if __name__ == "__main__":
        if not WIN32:
            signal.signal(signal.SIGCHLD, handle_cleanup)
            signal.signal(signal.SIGHUP, handle_cleanup)
        else:
            signal.signal(signal.SIGBREAK, handle_cleanup)
        signal.signal(signal.SIGINT, handle_cleanup)
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

    play["Range"] = f"npt={args.start:.3f}-end"
    play.update({"Scale": "1.000", "x-playNow": "", "x-noFlush": ""})
    play["Session"] = session["Session"] = r.headers["Session"].split(";")[0]

    get_parameter = session.copy()
    if client.does_need_position():
        get_parameter.update({"Content-type": "text/parameters", "Content-length": 10})

    await client.send_request("PLAY", play)

    return VodData(client, get_parameter, session)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(f"Movistar U7D - VOD v{_version}")
    parser.add_argument("channel", help="channel id")
    parser.add_argument("broadcast", help="broadcast id")
    parser.add_argument("--client_ip", "-c", help="client ip address")
    parser.add_argument("--client_port", "-p", help="client udp port", type=int)
    parser.add_argument("--start", "-s", help="stream start offset", type=int, default=0)
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

    try:
        _IPTV = check_dns()
    except Exception:
        log.error("Unable to connect to Movistar DNS")
        sys.exit(1)

    if not _args.client_port:
        _args.client_port = find_free_port(_IPTV)

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
