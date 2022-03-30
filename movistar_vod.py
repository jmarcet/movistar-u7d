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
import psutil
import sys
import time
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from asyncio.exceptions import CancelledError
from asyncio.subprocess import PIPE, STDOUT
from collections import namedtuple
from datetime import timedelta
from dict2xml import dict2xml
from filelock import FileLock
from glob import glob

from mu7d import IPTV_DNS, EPG_URL, UA, URL_COVER, URL_MVTV, WIN32, YEAR_SECONDS
from mu7d import find_free_port, get_iptv_ip, mu7d_config, ongoing_vods, _version


Response = namedtuple("Response", ["version", "status", "url", "headers", "body"])
VodData = namedtuple("VodData", ["client", "get_parameter", "session"])


class RtspClient:
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
        # asyncio.create_task(self.print(req, resp))

        if resp.status != "200 OK" and method not in ["SETUP", "TEARDOWN"]:
            log.debug(f"{method} {resp}")
            _vod_t.cancel()

        return resp

    def serialize_headers(self, headers):
        return "\r\n".join(map(lambda x: "{0}: {1}".format(*x), headers.items()))


def _cleanup(ext, meta=False, subs=False):
    if os.path.exists(_filename + ext):
        _remove(_filename + ext)
    if meta:
        [_remove(t) for t in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*.jpg")]
    if subs:
        [_remove(t) for t in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*.sub")]


def _remove(item):
    try:
        if os.path.isfile(item):
            os.remove(item)
        elif os.path.isdir(item) and not os.listdir(item):
            os.rmdir(item)
    except (FileNotFoundError, PermissionError):
        pass


async def postprocess(record_time=0):
    async def _check_process(process, msg):
        proc = psutil.Process(process.pid)
        proc.nice(15 if not WIN32 else psutil.IDLE_PRIORITY_CLASS)
        proc.ionice(psutil.IOPRIO_CLASS_IDLE if not WIN32 else psutil.IOPRIO_VERYLOW)
        await process.wait()
        if process.returncode not in (0, 1):
            stdout = await process.stdout.read() if process.stdout else None
            raise ValueError(stdout.decode().replace("\n", " ").strip() if stdout else msg)

    async def _duration_ok():
        try:
            resp = await _SESSION.options(_epg_url, params=_epg_params)  # signal recording ended
            if resp.status not in (200, 201):
                raise ValueError("Too short, missing: %ss" % _epg_params["missing"])
            return resp.status
        except ClientConnectorError:
            raise ValueError("Cancelled")

    async def step_1():
        global _ffmpeg_pp_p

        cmd = ["mkvmerge", "--abort-on-warnings", "-q", "-o", _filename + TMP_EXT2]
        img_mime, img_name = await save_metadata()
        if img_mime and img_name:
            cmd += ["--attachment-mime-type", img_mime, "--attach-file", img_name]
        if _args.vo:
            cmd += ["--track-order", "0:2,0:1,0:4,0:3,0:6,0:5"]
            cmd += ["--default-track", "2:1"]
        cmd += [_filename + TMP_EXT]

        log.info(f"POSTPROCESS #1: Verifying recording: {_log_suffix}")
        _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
        await _check_process(_ffmpeg_pp_p, "#1: Failed verifying recording")

        if img_name:
            os.remove(img_name)

    async def step_2(status):
        global _epg_params, _log_suffix

        cmd = ["mkvmerge", "-J", _filename + TMP_EXT2]
        res = (await (await asyncio.create_subprocess_exec(*cmd, stdout=PIPE)).communicate())[0].decode()
        recording_data = ujson.loads(res)

        duration = int(int(recording_data["container"]["properties"]["duration"]) / 1000000000)
        bad = duration < _args.time - 30

        _epg_params["missing"] = _args.time - duration if bad else 0
        _log_suffix = f"[{duration}s] / {_log_suffix}"

        msg = f"POSTPROCESS #2: Recording is {'INCOMPLETE' if bad else 'COMPLETE'}: {_log_suffix}"
        if not bad:
            log.info(msg)
        else:
            if status == 201:
                log.warning(msg)
            else:
                log.error(msg)
                raise ValueError(msg.lstrip("POSTPROCESS "))

        return recording_data

    async def step_3(recording_data):
        global _ffmpeg_pp_p, _log_suffix

        _cleanup(".nfo")  # These are created by Jellyfin so we want them recreated
        _cleanup(TMP_EXT)
        _cleanup(VID_EXT, subs=_args.mp4)
        _log_suffix = _log_suffix[_log_suffix.find(" - ") + 3 :]

        if _args.mp4:
            cmd = ["ffmpeg", "-i", _filename + TMP_EXT2]
            cmd += ["-map", "0", "-c", "copy", "-sn", "-movflags", "+faststart"]
            cmd += ["-f", "mp4", "-v", "panic", _filename + VID_EXT]

            log.info(f"POSTPROCESS #3: Coverting remuxed recording to mp4: {_log_suffix}")
            _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
            await _check_process(_ffmpeg_pp_p, "#3: Failed converting remuxed recording to mp4")

            subs = [t for t in recording_data["tracks"] if t["type"] == "subtitles"]
            if subs:
                log.info(f"POSTPROCESS #3B: Exporting subs from recording: {_log_suffix}")
            for track in subs:
                filesub = "%s.%s.sub" % (_filename, track["properties"]["language"])
                cmd = ["ffmpeg", "-i", _filename + TMP_EXT2]
                cmd += ["-map", "0:%d" % track["id"], "-c:s", "dvbsub"]
                cmd += ["-f", "mpegts", "-v", "panic", filesub]

                _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
                await _check_process(_ffmpeg_pp_p, "#3B: Failed extracting subs from recording")

            _cleanup(TMP_EXT2)

        else:
            if WIN32 and os.path.exists(_filename + VID_EXT):
                os.remove(_filename + VID_EXT)
            os.rename(_filename + TMP_EXT2, _filename + VID_EXT)
            log.info(f"POSTPROCESS #3: Recording renamed to mkv: {_log_suffix}")

    async def step_4():
        global _ffmpeg_pp_p

        cmd = ["comskip", f"--threads={COMSKIP}", "-d", "70", _filename + VID_EXT]

        log.info(f"POSTPROCESS #4: COMSKIP: Checking recording for commercials: {_log_suffix}")
        async with aiofiles.open(COMSKIP_LOG, "ab") as f:
            start = time.time()
            _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=f, stderr=f)
            await _check_process(_ffmpeg_pp_p, "#4: COMSKIP: Failed checking recording for commercials")
            end = time.time()

        msg = (
            f"POSTPROCESS #4A: COMSKIP: Took {str(timedelta(seconds=round(end - start)))}s"
            f" => [Commercials {'not found' if _ffmpeg_pp_p.returncode else 'found'}]: {_log_suffix}"
        )
        log.warning(msg) if _ffmpeg_pp_p.returncode else log.info(msg)

        if not _args.mp4:
            if _ffmpeg_pp_p.returncode == 0:
                cmd = ["mkvmerge", "-q", "-o", _filename + TMP_EXT2]
                cmd += ["--chapters", _filename + CHP_EXT, _filename + VID_EXT]

                log.info(f"POSTPROCESS #4B: COMSKIP: Merging mkv chapters: {_log_suffix}")
                _ffmpeg_pp_p = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT)
                await _check_process(_ffmpeg_pp_p, "#4B: COMSKIP: Failed merging mkv chapters")
                if WIN32 and os.path.exists(_filename + VID_EXT):
                    os.remove(_filename + VID_EXT)
                os.rename(_filename + TMP_EXT2, _filename + VID_EXT)
            _cleanup(CHP_EXT)

    global _epg_params, _pp_lock

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_vod.lock")  # nosec B108

    # We verify with the backend if the raw recording time is OK, rounding the desired time by 30s
    _epg_params["missing"] = (_args.time - record_time) if (record_time < _args.time - 30) else 0
    status = await _duration_ok()

    _pp_lock = FileLock(lockfile)
    _pp_lock.acquire(poll_interval=5)
    log.debug(f"Recording POSTPROCESS STARTS: {_log_suffix}")

    await step_1()  # First remux and verification

    # Parse the resultant recording data and pass it to step_3 to either remux to mp4 or rename to mkv
    await step_3(await step_2(status))

    if COMSKIP:
        await step_4()  # Comskip analysis

    _pp_lock.release()
    log.debug(f"Recording POSTPROCESS ENDED: {_log_suffix}")


async def postprocess_cleanup():
    if _pp_lock and _pp_lock.is_locked:
        _pp_lock.release()

    if os.path.exists(_filename + TMP_EXT):
        [_cleanup(ext) for ext in (TMP_EXT, TMP_EXT2, ".jpg", ".png")]
    else:
        _cleanup(NFO_EXT)
        _cleanup(VID_EXT, meta=True, subs=True)
        [_remove(file) for file in glob(f"{_filename.replace('[', '?').replace(']', '?')}.*")]

    if U7D_PARENT:
        _remove(_path)
        parent = os.path.split(_path)[0]
        if parent != RECORDINGS:
            _remove(parent)


async def reap_ffmpeg():
    global _ffmpeg_pp_t, _ffmpeg_r, _log_suffix
    start = time.time()

    _ffmpeg_r = await _ffmpeg_p.wait()
    record_time = int(time.time() - start)

    _log_suffix = f"[~{record_time}s] / {_log_suffix}"
    log.info(f"Recording ENDED [ffmpeg={_ffmpeg_r}]: {_log_suffix}")

    if U7D_PARENT:
        _ffmpeg_pp_t = asyncio.create_task(postprocess(record_time))

    elif os.path.exists(_filename + TMP_EXT):
        if WIN32 and os.path.exists(_filename + VID_EXT):
            os.remove(_filename + VID_EXT)
        os.rename(_filename + TMP_EXT, _filename + VID_EXT)


async def recording_archive():
    files = glob(f"{_filename.replace('[', '?').replace(']', '?')}.*")
    [os.utime(file, (-1, _mtime)) for file in files if os.access(file, os.W_OK)]

    resp = await _SESSION.put(_epg_url, params=_epg_params)
    if resp.status == 200:
        await save_metadata(extra=True)
    else:
        await postprocess_cleanup()


async def record_stream(opts):
    global _ffmpeg_p, _ffmpeg_t
    ffmpeg = ["ffmpeg", "-y", "-xerror", "-fifo_size", "5572", "-pkt_size", "1316", "-timeout", "500000"]
    ffmpeg.extend(["-i", _vod])
    ffmpeg.extend(["-map", "0", "-c", "copy", "-c:a:0", "aac", "-c:a:1", "aac", "-bsf:v", "h264_mp4toannexb"])
    ffmpeg.extend(["-metadata:s:a:0", "language=spa", "-metadata:s:a:1", "language=eng", "-metadata:s:a:2"])
    ffmpeg.extend(["language=spa", "-metadata:s:a:3", "language=eng", "-metadata:s:s:0", "language=spa"])
    ffmpeg.extend(["-metadata:s:s:1", "language=eng", *opts])

    if _args.channel in ("578", "884", "3603") or NO_SUBS:
        # matroska is not compatible with dvb_teletext subs
        # Boing, DKISS & Energy use them, so we drop them
        log.warning(f"Recording Dropping dvb_teletext subs: {_log_suffix}")
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
                f"{URL_MVTV}?action=epgInfov2&productID={_args.broadcast}&channelID={_args.channel}&extra=1"
            ) as resp:
                metadata = (await resp.json())["resultData"]

            async with aiofiles.open(cache_metadata, "w", encoding="utf8") as f:
                await f.write(ujson.dumps({"data": metadata}, ensure_ascii=False, indent=4, sort_keys=True))

    except (TypeError, ValueError) as ex:
        log.warning(f"Extended info not found: {_log_suffix} => {repr(ex)}")
        return None, None

    # We only want the main cover, to embed in the video file
    if not extra:
        for t in ["beginTime", "endTime", "expDate"]:
            metadata[t] = int(metadata[t] / 1000)

        cover = metadata["cover"]
        log.debug(f'Getting cover "{cover}": {_log_suffix}')
        async with _SESSION_CLOUD.get(f"{URL_COVER}/{cover}") as resp:
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
                return None, None

        img_mime = "image/jpeg" if img_ext in (".jpeg", ".jpg") else "image/png"
        return img_mime, img_name

    log.debug(f"Metadata={metadata}")
    # We want to save all the available metadata
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

    for t in ("isuserfavorite", "lockdata", "logos", "name", "playcount", "resume", "watched"):
        metadata.pop(t, None)

    metadata["title"] = _full_title

    async with aiofiles.open(_filename + NFO_EXT, "w", encoding="utf8") as f:
        await f.write(dict2xml(metadata, wrap="metadata", indent="    "))

    os.utime(_filename + NFO_EXT, (-1, _mtime))
    log.debug(f"Metadata saved: {_log_suffix}")


async def vod_get_info(channel_id, program_id, cloud, vod_client):
    params = "action=getRecordingData" if cloud else "action=getCatchUpUrl"
    params += f"&extInfoID={program_id}&channelID={channel_id}&mode=1"

    async with vod_client.get(f"{URL_MVTV}?{params}") as r:
        return (await r.json())["resultData"]


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
    global _vod_t

    if __name__ == "__main__":
        global _SESSION_CLOUD

        _SESSION_CLOUD = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                keepalive_timeout=YEAR_SECONDS,
                resolver=AsyncResolver(nameservers=[IPTV_DNS]) if not WIN32 else None,
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

            if U7D_PARENT:
                try:
                    async with _SESSION.get(_epg_url, params=_epg_params) as resp:
                        if resp.status != 200:
                            raise ValueError()

                        _, _path, _filename = (await resp.json()).values()
                        opts = ["-metadata:s:v", f"title={os.path.basename(_filename)}"]

                        if not os.path.exists(_path):
                            log.debug(f"Creating recording subdir {_path}")
                            os.makedirs(_path)

                except (ClientOSError, ValueError):
                    log.error(f"Program not found: [{_args.channel}] [{_args.broadcast}]")
                    await _SESSION_CLOUD.close()
                    await _SESSION.close()
                    return

            else:
                opts = []

            _full_title = _filename[len(RECORDINGS) + 1 :]
            _log_suffix += f' "{_full_title}"'

            await record_stream(opts)

    elif not vod_data:
        return

    try:
        _vod_t = asyncio.create_task(vod_task(args, vod_data))
        await _vod_t

    except (CancelledError, ValueError):
        pass

    finally:
        try:
            # This is a crucial step or vod will be consuming bandwith for up to another 60s
            await asyncio.shield(vod_data.client.send_request("TEARDOWN", vod_data.session))

        except ValueError:
            pass

        finally:
            vod_data.client.close_connection()

            if __name__ == "__main__":
                if args.write_to_file:

                    if _ffmpeg_pp_t:
                        try:
                            await _ffmpeg_pp_t
                            await asyncio.shield(recording_archive())
                        except (CancelledError, ValueError) as ex:
                            log.error(f"Recording FAILED: {_log_suffix} => {repr(ex)}")
                            await asyncio.shield(postprocess_cleanup())

                    elif _ffmpeg_p and _ffmpeg_p.returncode is None:
                        try:
                            _ffmpeg_p.kill()
                        except psutil.NoSuchProcess:
                            pass
                        await _ffmpeg_p.wait()
                        await asyncio.shield(postprocess_cleanup())

                    await _SESSION.close()
                await _SESSION_CLOUD.close()
                log.debug(f"Loop ended: {_log_suffix}")


async def VodSetup(args, vod_client, failed=False):
    log_prefix = f"{('[' + args.client_ip + '] ') if args.client_ip else ''}"
    log_suffix = f"[{args.channel}] [{args.broadcast}]"

    if __name__ == "__main__" and args.write_to_file:
        global _epg_params, _epg_url

        _epg_params = {"cloud": 1} if args.cloud else {}
        _epg_url = f"{EPG_URL}/program_name/{args.channel}/{args.broadcast}"

        if U7D_PARENT and await ongoing_vods(args.channel, args.broadcast):
            log.error(f"{log_prefix}Recording already ongoing: {log_suffix}")
            return

    client = None
    headers = {"CSeq": "", "User-Agent": "MICA-IP-STB"}

    setup = session = play = describe = headers.copy()
    describe["Accept"] = "application/sdp"
    setup["Transport"] = f"MP2T/H2221/UDP;unicast;client_port={args.client_port}"

    try:
        vod_info = await vod_get_info(args.channel, args.broadcast, args.cloud, vod_client)
        log.debug(f"{log_prefix}{log_suffix}: vod_info={vod_info}")
        if __name__ == "__main__" and args.write_to_file:
            global _args, _filename, _log_suffix, _mtime

            if not U7D_PARENT:
                from mu7d import get_safe_filename

                _filename = os.path.join(
                    RECORDINGS, f"{vod_info['channelName']} - {get_safe_filename(vod_info['name'])}"
                )

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
                    else _args.time
                    if _args.time > 60
                    else 60,
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
        if not failed:
            log.warning(f"{log_prefix}Could not get uri for {log_suffix} => {repr(ex)}")
            return await VodSetup(args, vod_client, failed=True)
        else:
            log.error(f"{log_prefix}Could not get uri for {log_suffix} => {repr(ex)}")
            return

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

    if __name__ == "__main__" and not args.write_to_file:
        log.info(f'The VOD stream can be accesed at: "{_vod}"')

    return VodData(client, get_parameter, session)


if __name__ == "__main__":
    if not WIN32:
        import signal
        from setproctitle import setproctitle

        setproctitle("movistar_vod %s" % " ".join(sys.argv[1:]))

        def cleanup_handler(signum, frame):
            if _args.write_to_file:
                if _ffmpeg_p and _ffmpeg_p.returncode is None:
                    if not U7D_PARENT:
                        log.error(f"Recording CANCELLED: {_log_suffix}")
                    _ffmpeg_p.terminate()
                elif _ffmpeg_pp_t and not _ffmpeg_pp_t.done():
                    if _ffmpeg_pp_p and _ffmpeg_pp_p.returncode is None:
                        try:
                            psutil.Process(_ffmpeg_pp_p.pid).kill()
                        except psutil.NoSuchProcess as ex:
                            log.debug(f"Could not kill {repr(ex)}: {_log_suffix}")
                    else:
                        _ffmpeg_pp_t.cancel()
            elif _vod_t:
                _vod_t.cancel()

        [signal.signal(sig, cleanup_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    _SESSION = _SESSION_CLOUD = None
    _args = _epg_params = _epg_url = _filename = _full_title = _log_suffix = _mtime = _path = _vod = None
    _ffmpeg_p = _ffmpeg_pp_p = _ffmpeg_pp_t = _ffmpeg_r = _ffmpeg_t = _pp_lock = _vod_t = None

    _conf = mu7d_config()

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

    log.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] [VOD] [%(levelname)s] %(message)s",
        level=log.DEBUG if (_args.debug or _conf["DEBUG"]) else log.INFO,
    )
    log.getLogger("filelock").setLevel(log.INFO)

    try:
        iptv = get_iptv_ip()
    except Exception:
        log.critical("Unable to connect to Movistar DNS")
        sys.exit(1)

    if not _args.client_port:
        _args.client_port = find_free_port(get_iptv_ip())

    _vod = f"udp://@{iptv}:{_args.client_port}"

    if _args.write_to_file:
        if not _conf["RECORDINGS"]:
            log.error("RECORDINGS path not set")
            sys.exit(1)

        CACHE_DIR = os.path.join(_conf["HOME"], ".xmltv/cache/programs")
        COMSKIP = _conf["COMSKIP"]
        COMSKIP_LOG = os.path.join(_conf["HOME"], "comskip.log") if COMSKIP else None
        NO_SUBS = _conf["NO_SUBS"]
        RECORDINGS = _conf["RECORDINGS"]

        CHP_EXT = ".mkvtoolnix.chapters"
        NFO_EXT = "-movistar.nfo"
        TMP_EXT = ".tmp"
        TMP_EXT2 = ".tmp2"

        if _args.mp4:
            VID_EXT = ".mp4"
        else:
            VID_EXT = ".mkv"

    U7D_PARENT = os.getenv("U7D_PARENT")

    try:
        asyncio.run(VodLoop(_args))
    except KeyboardInterrupt:
        sys.exit(1)

    if _ffmpeg_r or (_ffmpeg_pp_p and _ffmpeg_pp_p.returncode not in (0, 1)):
        log.debug(f"Exiting {_ffmpeg_r if _ffmpeg_r else _ffmpeg_pp_p.returncode}")
        sys.exit(_ffmpeg_r if _ffmpeg_r else _ffmpeg_pp_p.returncode)
    elif not _vod_t or _vod_t.cancelled():
        log.debug("Exiting 1")
        sys.exit(1)
    log.debug("Exiting 0")
