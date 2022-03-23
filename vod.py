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
import socket
import sys
import timeit
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from asyncio.subprocess import PIPE, STDOUT
from collections import namedtuple
from contextlib import closing
from dict2xml import dict2xml
from glob import glob
from psutil import Process, NoSuchProcess, process_iter
from random import randint

try:
    from asyncio.exceptions import CancelledError
except ModuleNotFoundError:
    from asyncio import CancelledError

from version import _version


WIN32 = sys.platform == "win32"

if not WIN32:
    import signal
    from setproctitle import setproctitle

    setproctitle("vod %s" % " ".join(sys.argv[1:]))


CACHE_DIR = os.path.join(os.getenv("HOME", os.getenv("USERPROFILE")), ".xmltv/cache/programs")
DEBUG = bool(int(os.getenv("DEBUG", 0)))
NOSUBS = bool(int(os.getenv("NOSUBS", 0)))
RECORDINGS = os.getenv("RECORDINGS", "").rstrip("/").rstrip("\\")
SANIC_EPG_URL = "http://127.0.0.1:8889"
LOCK_FILE = os.path.join(os.getenv("TMP", "/tmp"), ".u7d-pp.lock")  # nosec B108

IMAGENIO_URL = "http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg"
COVER_URL = f"{IMAGENIO_URL}/covers/programmeImages/portrait/290x429"
MOVISTAR_DNS = "172.26.23.3"
MVTV_URL = "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do"

NFO_EXT = "-movistar.nfo"
TMP_EXT = ".tmp"
TMP_EXT2 = ".tmp2"
UA = "libcurl-agent/1.0 [IAL] WidgetManager Safari/538.1 CAP:803fd12a 1"

YEAR_SECONDS = 365 * 24 * 60 * 60

Response = namedtuple("Response", ["version", "status", "url", "headers", "body"])
VodData = namedtuple("VodData", ["client", "get_parameter", "session"])

_IPTV = _SESSION = _SESSION_CLOUD = None

_args = _epg_params = _epg_url = _filename = _full_title = _log_suffix = _mtime = _path = None
_ffmpeg_p = _ffmpeg_pp_p = _ffmpeg_pp_t = _ffmpeg_r = _ffmpeg_t = _vod_t = None

_nice = ("nice", "-n", "15", "ionice", "-c", "3", "flock", LOCK_FILE) if not WIN32 else ()


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
            log.debug(f"{method} {resp}")
            _vod_t.cancel()

        return resp

    def serialize_headers(self, headers):
        return "\r\n".join(map(lambda x: "{0}: {1}".format(*x), headers.items()))


def check_dns():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((MOVISTAR_DNS, 53))
        return s.getsockname()[0]
    finally:
        s.close()


async def check_process(process, msg):
    await process.wait()
    log.debug(f"{process}")
    if process.returncode not in (0, 1):
        raise ValueError(process.stdout.decode().replace("\n", " ").strip() if process.stdout else msg)


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
    log.debug(f"handle_cleanup: {_log_suffix}")
    if __name__ == "__main__" and _args.write_to_file:
        if _ffmpeg_p and _ffmpeg_p.returncode is None:
            _ffmpeg_t.cancel()
            _ffmpeg_p.terminate()
        elif _ffmpeg_pp_t and not _ffmpeg_pp_t.done() and _ffmpeg_pp_p and _ffmpeg_pp_p.returncode is None:
            try:
                proc = Process(_ffmpeg_pp_p.pid)
                if not WIN32:
                    proc = proc.children()[0]  # flock is the parent
                log.debug(f"Killing {proc.name()}: {_log_suffix}")
                proc.kill()
            except (IndexError, NoSuchProcess) as ex:
                log.debug(f"Could not kill {repr(ex)}: {_log_suffix}")
    else:
        _vod_t.cancel()


async def postprocess(record_time=0, timers_t=None):
    global _epg_params, _ffmpeg_pp_p, _log_suffix
    log.debug(f"Recording Postprocess: {_log_suffix}")

    try:
        if record_time:
            _epg_params["missing"] = _args.time - record_time
            resp = await _SESSION.options(_epg_url, params=_epg_params)

            if resp and resp.status == 200:
                return await postprocess()
            else:
                raise ValueError("Too short, missing: %ss" % _epg_params["missing"])

        cmd = list(_nice)
        cmd += ["mkvmerge", "--abort-on-warnings", "-q", "-o", _filename + TMP_EXT2]
        try:
            img_mime, img_name = await save_metadata()
            cmd += ["--attachment-mime-type", img_mime, "--attach-file", img_name]
        except TypeError:
            pass
        if _args.vo:
            cmd += ["--track-order", "0:2,0:1,0:4,0:3,0:6,0:5"]
            cmd += ["--default-track", "2:1"]
        cmd += [_filename + TMP_EXT]

        _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
        await check_process(_ffmpeg_pp_p, "Failed to verify recording")
        [cleanup(ext) for ext in (TMP_EXT, ".nfo")]

        cmd = ["mkvmerge", "-J", _filename + TMP_EXT2]
        res = (await (await asyncio.create_subprocess_exec(*cmd, stdout=PIPE)).communicate())[0].decode()
        recording_data = ujson.loads(res)
        duration = int(int(recording_data["container"]["properties"]["duration"]) / 1000000000)
        bad = duration < (_args.time - 30)
        _epg_params["missing"] = _args.time - duration if bad else 0
        _log_suffix = f"[{duration}s] / {_log_suffix}"

        msg = f"Recording {'INCOMPLETE:' if bad else 'COMPLETE:'} {_log_suffix}"
        log.warning(msg) if bad else log.info(msg)

        _log_suffix = _log_suffix[_log_suffix.find(" - ") + 3 :]
        cleanup(VID_EXT, subs=_args.mp4)
        if _args.mp4:
            cmd = list(_nice)
            cmd += ["ffmpeg", "-i", _filename + TMP_EXT2]
            cmd += ["-map", "0", "-c", "copy", "-sn", "-movflags", "+faststart"]
            cmd += ["-f", "mp4", "-v", "panic", _filename + VID_EXT]
            _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
            await check_process(_ffmpeg_pp_p, "Failed to remux video to mp4 container")

            subs = [t for t in recording_data["tracks"] if t["type"] == "subtitles"]
            for track in subs:
                filesub = "%s.%s.sub" % (_filename, track["properties"]["language"])
                cmd = list(_nice)
                cmd += ["ffmpeg", "-i", _filename + TMP_EXT2]
                cmd += ["-map", "0:%d" % track["id"], "-c:s", "dvbsub"]
                cmd += ["-f", "mpegts", "-v", "panic", filesub]
                _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
                await check_process(_ffmpeg_pp_p, "Failed to extract subs")
            cleanup(TMP_EXT2)
        else:
            os.rename(_filename + TMP_EXT2, _filename + VID_EXT)

        files = glob(f"{_filename.replace('[', '?').replace(']', '?')}.*")
        [os.utime(file, (-1, _mtime)) for file in files if os.access(file, os.W_OK)]

        resp = await _SESSION.put(_epg_url, params=_epg_params)
        if resp and resp.status == 200:
            await save_metadata(extra=True)
        else:
            cleanup(VID_EXT, meta=True, subs=_args.mp4)
            [os.remove(file) for file in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*")]

    except Exception as ex:
        log.error(f"Recording FAILED: {_log_suffix} => {repr(ex)}")
        if not record_time:
            _epg_params["missing"] = randint(1, _args.time)  # nosec B311
            try:
                async with aiohttp.ClientSession() as session:
                    await session.put(_epg_url, params=_epg_params)
            except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
                pass
        cleanup(NFO_EXT)
        cleanup(VID_EXT, meta=True, subs=True)
        [os.remove(file) for file in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*")]
        if os.path.exists(LOCK_FILE):
            try:
                os.remove(LOCK_FILE)
            except FileNotFoundError:
                pass
        if not os.listdir(_path):
            os.rmdir(_path)
            parent = os.path.split(_path)[0]
            if parent != RECORDINGS and not os.listdir(parent):
                os.rmdir(parent)
    finally:
        if timers_t and not timers_t.done():
            await timers_t
        log.debug(f"Recording Postprocess ENDED: {_log_suffix}")


async def reap_ffmpeg():
    global _ffmpeg_pp_t, _ffmpeg_r, _log_suffix
    start = timeit.default_timer()

    _ffmpeg_r = await _ffmpeg_p.wait()
    record_time = int(timeit.default_timer() - start)
    log.debug(f"ffmpeg retcode={_ffmpeg_r}: {_log_suffix}")

    _log_suffix = f"[~{record_time}s] / {_log_suffix}"
    log.info(f"Recording ENDED: {_log_suffix}")

    record_time = record_time if record_time < _args.time - 30 else 0
    _ffmpeg_pp_t = asyncio.create_task(postprocess(record_time, asyncio.create_task(timers_check())))


async def record_stream(opts):
    global _ffmpeg_p, _ffmpeg_t
    ffmpeg = ["ffmpeg", "-y", "-xerror", "-fifo_size", "5572", "-pkt_size", "1316", "-timeout", "500000"]
    ffmpeg.extend(["-i", f"udp://@{_IPTV}:{_args.client_port}"])
    ffmpeg.extend(["-map", "0", "-c", "copy", "-c:a:0", "aac", "-c:a:1", "aac", "-bsf:v", "h264_mp4toannexb"])
    ffmpeg.extend(["-metadata:s:a:0", "language=spa", "-metadata:s:a:1", "language=eng", "-metadata:s:a:2"])
    ffmpeg.extend(["language=spa", "-metadata:s:a:3", "language=eng", "-metadata:s:s:0", "language=spa"])
    ffmpeg.extend(["-metadata:s:s:1", "language=eng", *opts])

    if _args.channel in ("578", "884", "3603") or NOSUBS:
        # matroska is not compatible with dvb_teletext subs
        # Boing, DKISS & Energy use them, so we drop them
        log.warning(f"Recording dropping dvb_teletext subs: {_log_suffix}")
        ffmpeg.append("-sn")

    ffmpeg.extend(["-t", str(_args.time), "-v", "panic", "-vsync", "0", "-f", "matroska"])
    ffmpeg.append(_filename + TMP_EXT)

    _ffmpeg_p = await asyncio.create_subprocess_exec(*ffmpeg)
    _ffmpeg_t = asyncio.create_task(reap_ffmpeg())
    log.info(f"Recording STARTED: {_log_suffix}")


async def save_metadata(extra=False):
    log.debug(f"Saving metadata: {_log_suffix}")
    cache_metadata = os.path.join(CACHE_DIR, f"{_args.broadcast}.json")
    try:
        if os.path.exists(cache_metadata):
            async with aiofiles.open(cache_metadata, encoding="utf8") as f:
                metadata = ujson.loads(await f.read())["data"]
        else:
            log.debug(f"Getting extended info: {_log_suffix}")
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
                log.debug(f'Failed to get cover "{cover}": {resp} {_log_suffix}')
                return
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
                os.utime(img_name, (-1, _mtime))
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
    os.utime(_filename + NFO_EXT, (-1, _mtime))
    log.debug(f"Metadata saved: {_log_suffix}")


async def timers_check():
    await asyncio.sleep(3.5)
    try:
        await _SESSION.get(f"{SANIC_EPG_URL}/timers_check")
    except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
        pass


async def vod_ongoing(channel_id="", program_id=""):
    EXT = "" if not WIN32 else (".exe" if getattr(sys, "frozen", False) else ".py")
    cmd = f"vod{EXT}"
    cmd += f" {channel_id}" if channel_id else ""
    cmd += f" {program_id}" if program_id else "" + " .+ -w"
    if not WIN32:
        return [proc for proc in process_iter() if re.match(cmd, " ".join(proc.cmdline()))]
    else:
        cmd = f"{sys.executable} {cmd}" if (WIN32 and EXT == ".py") else cmd
        family = Process(Process(os.getpid()).ppid()).children(recursive=True)
        return [proc for proc in family if re.match(cmd, " ".join(proc.cmdline()))]


async def vod_task(args, vod_data):
    while True:
        if __name__ == "__main__" and args.write_to_file:
            done, pending = await asyncio.wait({_ffmpeg_t}, timeout=30)
            if _ffmpeg_t in done:
                break
        else:
            await asyncio.sleep(30)
        await vod_data.client.send_request("GET_PARAMETER", vod_data.get_parameter)


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
            global _SESSION, _filename, _full_title, _log_suffix, _path

            _SESSION = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS),
                json_serialize=ujson.dumps,
            )

            try:
                async with _SESSION.get(_epg_url, params=_epg_params) as resp:
                    if resp.status == 200:
                        _, _path, _filename = (await resp.json()).values()
                        opts = ["-metadata:s:v", f"title={os.path.basename(_filename)}"]

                        if not os.path.exists(_path):
                            log.debug(f"Creating recording subdir {_path}")
                            os.makedirs(_path)
                    else:
                        _filename = os.path.join(RECORDINGS, f"{args.channel}-{args.broadcast}")
                        opts = []
            except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
                await _SESSION_CLOUD.close()
                await _SESSION.close()
                return

            _full_title = _filename[len(RECORDINGS) + 1 :]
            _log_suffix += f' "{_full_title}"'

            await record_stream(opts)

    elif not vod_data:
        return

    global _vod_t
    try:
        _vod_t = asyncio.create_task(vod_task(args, vod_data))
        await _vod_t
    except CancelledError:
        pass
    finally:
        await vod_data.client.send_request("TEARDOWN", vod_data.session)
        vod_data.client.close_connection()

        if __name__ == "__main__":
            if args.write_to_file:
                if _ffmpeg_pp_t:
                    log.debug(f"Waiting for _ffmpeg_pp_t={_ffmpeg_pp_t}: {_log_suffix}")
                    await _ffmpeg_pp_t
                    log.debug(f"Waited for _ffmpeg_pp_t={_ffmpeg_pp_t}: {_log_suffix}")
                await _SESSION.close()
            await _SESSION_CLOUD.close()
            log.debug(f"Loop ended: {_log_suffix}")


async def VodSetup(args, vod_client, failed=False):
    global _epg_params, _epg_url, _mtime

    _epg_url = f"{SANIC_EPG_URL}/program_name/{args.channel}/{args.broadcast}"
    _epg_params = {"cloud": 1} if args.cloud else {}
    log_prefix = f"{('[' + args.client_ip + '] ') if args.client_ip else ''}"
    log_suffix = f"[{args.channel}] [{args.broadcast}]"

    if __name__ == "__main__" and args.write_to_file:
        if len(await vod_ongoing(args.channel, args.broadcast)) > 1:
            log.error(f"{log_prefix}Recording already ongoing: {log_suffix}")
            return

    client = None
    headers = {"CSeq": "", "User-Agent": "MICA-IP-STB"}

    setup = session = play = describe = headers.copy()
    describe["Accept"] = "application/sdp"
    setup["Transport"] = f"MP2T/H2221/UDP;unicast;client_port={args.client_port}"

    try:
        vod_info = await get_vod_info(args.channel, args.broadcast, args.cloud, vod_client)
        log.debug(f"{log_prefix}{log_suffix}: vod_info={vod_info}")
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
                    else _args.time + 60
                    if _args.time > 900
                    else _args.time,
                )
            _mtime = vod_info["beginTime"] / 1000 + _args.start
            _log_suffix = "[%ds] - [%s] [%s] [%s] [%d]" % (
                _args.time,
                vod_info["channelName"],
                args.channel,
                args.broadcast,
                vod_info["beginTime"] / 1000,
            )
        uri = urllib.parse.urlparse(vod_info["url"])
    except Exception as ex:
        if isinstance(ex, (ClientConnectorError, ConnectionRefusedError)):
            log.error(f"{log_prefix}Movistar IPTV catchup service DOWN: {log_suffix}")
            if __name__ == "__main__" and args.write_to_file:
                _epg_params["missing"] = str(randint(1, _args.time))  # nosec B311
                await _SESSION.put(_epg_url, params=_epg_params)
        elif not failed:
            log.warning(f"{log_prefix}{log_suffix} => {repr(ex)}")
            return await VodSetup(args, vod_client, True)
        else:
            log.error(f"{log_prefix}Could not get uri for: {log_suffix} => {repr(ex)}")
        return

    if __name__ == "__main__" and not WIN32:
        [signal.signal(sig, handle_cleanup) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

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
        log.error("RECORDINGS path not set")
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

    asyncio.run(VodLoop(_args))
    if _ffmpeg_r or (_ffmpeg_pp_p and _ffmpeg_pp_p.returncode not in (0, 1)):
        log.debug(f"Exiting {_ffmpeg_r if _ffmpeg_r else _ffmpeg_pp_p.returncode}")
        sys.exit(_ffmpeg_r if _ffmpeg_r else _ffmpeg_pp_p.returncode)
    elif not _vod_t or _vod_t.cancelled():
        log.debug("Exiting 1")
        sys.exit(1)
    log.debug("Exiting 0")
