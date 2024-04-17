#!/usr/bin/env python3
#
# Started from MovistarU7D by XXLuigiMario:
# Source: https://github.com/XXLuigiMario/MovistarU7D

import argparse
import asyncio
import logging
import os
import re
import shutil
import sys
import time
import urllib.parse
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL as NULL, PIPE, STDOUT as OUT
from contextlib import closing
from datetime import timedelta
from signal import SIGINT, SIGTERM, signal

import aiohttp
import asyncstdlib as a
import psutil
import ujson
import xmltodict
from aiofiles import open as async_open, os as aio_os
from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from asyncio_dgram import TransportClosed, bind as dgram_bind
from filelock import FileLock

from mu7d_cfg import (
    BUFF,
    CHUNK,
    CONF,
    DATEFMT,
    DIV_LOG,
    DROP_KEYS,
    FMT,
    LINUX,
    NFO_EXT,
    UA,
    URL_COVER,
    VERSION,
    WIN32,
    add_logfile,
)
from mu7d_lib import (
    IPTVNetworkError,
    find_free_port,
    get_end_point,
    get_iptv_ip,
    get_safe_filename,
    get_vod_info,
    glob_safe,
    ongoing_vods,
    remove,
    rename,
    utime,
)

log = logging.getLogger("VOD")


class RecordingError(Exception):
    """Local recording error"""


class RtspClient:
    def __init__(self, reader, writer, url):
        self.reader, self.writer, self.url = reader, writer, url
        self.cseq = 1

    def close_connection(self):
        self.writer.close()

    async def send_request(self, method, headers):
        headers["CSeq"] = self.cseq
        req = f"{method} {self.url} RTSP/1.0\r\n{self.serialize_headers(headers)}\r\n\r\n"

        self.writer.write(req.encode())
        resp = (await self.reader.read(4096)).decode().splitlines()

        # log.debug("[%d]: Req  = [%s]", self.cseq, "|".join(resp))
        # log.debug("[%d]: Resp = [%s]", self.cseq, "|".join(resp))

        self.cseq += 1

        if not resp or not resp[0].endswith("200 OK"):
            return

        if method == "SETUP":
            return resp[1].split(": ")[1].split(";")[0]

        return True

    def serialize_headers(self, headers):
        return "\r\n".join(map(lambda x: "{0}: {1}".format(*x), headers.items()))


async def _archive_recording():
    path = os.path.dirname(_filename)
    if not await aio_os.path.exists(path):
        log.debug('Making dir "%s"', path)
        await aio_os.makedirs(path)

    if not RECORDINGS_TMP:
        await rename(_tmpname + TMP_EXT, _filename + VID_EXT)

    else:
        await remove(_filename + VID_EXT, _filename + ".jpg", _filename + ".png")

        covers = [x for x in glob_safe(f"{_tmpname}.*") if x.endswith((".jpg", ".png"))]
        if covers:
            tmpcover = covers[0]
            cover_ext = os.path.splitext(tmpcover)[1]
            shutil.copy2(tmpcover, _filename + cover_ext)
            await remove(tmpcover)

        shutil.copy2(_tmpname + TMP_EXT, _filename + VID_EXT)
        await _cleanup(TMP_EXT)

        path = os.path.dirname(_tmpname)
        parent = os.path.split(path)[0]
        await remove(path)
        if parent != RECORDINGS_TMP:
            await remove(parent)


async def _cleanup(*exts):
    for ext in exts:
        if await aio_os.path.exists(_tmpname + ext):
            await remove(_tmpname + ext)


async def _cleanup_recording(exception, start=None):
    if isinstance(exception, CancelledError):
        msg = "Cancelled"
        if start:
            msg += " @ [%6ss] / [%5ss]" % ("~" + str(int(time.time() - start)), str(_args.time))
        log.error("%-17s%87s", "Recording FAILED", msg)
    else:
        log.error(f'Recording FAILED: {str(exception).split(" - ", 1)[-1]}')

    await remove(*glob_safe(os.path.join(os.path.dirname(_tmpname), f"??_show_segment{VID_EXT}")))
    if RECORDINGS_TMP or await a.any(a.map(aio_os.path.exists, (_tmpname + TMP_EXT, _tmpname + TMP_EXT2))):
        log.debug("_cleanup_recording: cleaning only TMP files")
        await _cleanup(TMP_EXT, TMP_EXT2, ".log", ".logo.txt", ".txt")
        if RECORDINGS_TMP:
            await _cleanup(".jpg", ".png")
    else:
        log.debug("_cleanup_recording: cleaning everything")
        await _cleanup(NFO_EXT)
        await remove(*(set(glob_safe(f"{_tmpname}.*")) | set(glob_safe(f"{_filename}.*"))))
        await remove(
            *glob_safe(os.path.join(os.path.dirname(_filename), "metadata", os.path.basename(_filename) + "-*"))
        )

    if U7D_PARENT:
        path = os.path.dirname(_tmpname)
        parent = os.path.split(path)[0]
        log.debug("Removing path=%s", path)
        await remove(path)
        if parent not in (RECORDINGS, RECORDINGS_TMP):
            log.debug("Removing parent=%s", parent)
            await remove(parent)

        try:
            await _SESSION.get(f"{U7D_URL}/timers_check?delay=3")
        except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
            pass


async def _open_sessions():
    global _SESSION, _SESSION_CLOUD

    _SESSION_CLOUD = aiohttp.ClientSession(headers={"User-Agent": UA}, json_serialize=ujson.dumps)

    if _args.write_to_file:
        _SESSION = aiohttp.ClientSession(json_serialize=ujson.dumps)


async def postprocess(vod_info):  # pylint: disable=too-many-statements
    async def _check_process(msg=""):
        nonlocal proc

        process = psutil.Process(proc.pid)
        process.nice(15 if not WIN32 else psutil.IDLE_PRIORITY_CLASS)
        if LINUX or WIN32:
            process.ionice(psutil.IOPRIO_CLASS_IDLE if LINUX else psutil.IOPRIO_VERYLOW)

        await proc.wait()

        if proc.returncode:
            stdout = await proc.stdout.read() if proc.stdout else ""
            if stdout:
                msg += ": " if msg else ""
                msg += '"%s"' % re.sub(r"\s+", " ", stdout.decode().replace("\n", " ").strip())

            if any((not WIN32 and proc.returncode == -9, WIN32 and proc.returncode == 15)):
                raise RecordingError(msg)

            if msg:
                log.error(msg)

    async def _get_duration(recording):
        cmd = ("ffprobe", "-i", recording, "-show_entries", "format=duration", "-v", "quiet", "-of", "json")
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=NULL)
        recording_data = ujson.loads((await proc.communicate())[0].decode())

        return int(float(recording_data.get("format", {}).get("duration", 0)))

    async def _get_language_tags(recording, vo):
        cmd = ("ffprobe", "-i", recording, "-v", "quiet", "-of", "json")
        cmd += ("-show_entries", "stream=codec_type:stream_tags=language")
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=NULL)
        recording_data = ujson.loads((await proc.communicate())[0].decode())

        sub_idx, tags = 0, []
        langs_map = {"ads": "spa", "esp": "spa", "srd": "spa", "vo": "mul", "vos": "mul"}
        for idx, stream in enumerate(recording_data["streams"][1:]):
            codec = "a" if stream["codec_type"] == "audio" else "s"
            lang = langs_map.get(stream["tags"]["language"], stream["tags"]["language"])
            sub_idx = idx if all((codec == "s", sub_idx == 0)) else sub_idx
            tags.append(f"-metadata:s:{codec}:{idx - sub_idx} language={lang}")
            if all((codec == "a", idx == 1, vo)):
                tags[0], tags[1] = tags[1].replace("s:a:1", "s:a:0"), tags[0].replace("s:a:0", "s:a:1")
        return " ".join(tags).split()

    async def _save_cover_cache(metadata):
        if metadata.get("covers", {}).get("fanart"):
            cover = os.path.join(RECORDINGS, metadata["covers"]["fanart"])
        else:
            cover = os.path.join(RECORDINGS, metadata["cover"])

        if await aio_os.path.exists(cover):
            cached_cover = cover.replace(RECORDINGS, os.path.join(RECORDINGS_TMP, "covers"))
            dirname = os.path.dirname(cached_cover)
            if not await aio_os.path.exists(dirname):
                log.debug('Making dir "%s"', dirname)
                await aio_os.makedirs(dirname)
            log.debug('Saving cover cache "%s"', cached_cover)
            shutil.copy2(cover, cached_cover)

    async def _save_metadata(duration=None, get_cover=False):
        nonlocal metadata, mtime

        # Only the main cover, to embed in the video file
        if get_cover:
            try:
                async with async_open(
                    os.path.join(CACHE_DIR, "programs", f"{_args.program}.json"), encoding="utf8"
                ) as f:
                    metadata = ujson.loads(await f.read())["data"]
            except (FileNotFoundError, OSError, PermissionError, TypeError, ValueError) as ex:
                if _args.index:
                    raise RecordingError(f"Extended info not found => {repr(ex)}") from ex
                return None, None

            cover = metadata["cover"]
            img_ext = os.path.splitext(cover)[1]
            img_name, archival_img_name = _tmpname + img_ext, _filename + img_ext
            metadata["cover"] = archival_img_name[len(RECORDINGS) + 1 :]
            log.debug('Getting cover "%s"', cover)
            for x in range(2):
                try:
                    async with _SESSION_CLOUD.get(f"{URL_COVER}/{cover}") as resp:
                        if resp.status == 200:
                            img_data = await resp.read()
                            if img_data:
                                log.debug('Got cover "%s"', cover)
                                async with async_open(img_name, "wb") as f:
                                    await f.write(await resp.read())
                                await utime(mtime, img_name)
                                img_mime = "image/png" if img_ext == ".png" else "image/jpeg"
                                return img_mime, img_name
                except (ClientConnectionError, ClientOSError, ServerDisconnectedError) as ex:
                    resp = ex

                log.warning('Failed to get cover "%s" => %s', cover, str(resp).splitlines()[0])
                if x == 0:
                    await asyncio.sleep(2)

            raise RecordingError("Failed to get cover")

        # Save all the available metadata
        log.debug('metadata="%s"', metadata)

        if metadata.get("covers"):
            covers = {}
            metadata_dir = os.path.join(os.path.dirname(_filename), "metadata")

            if not await aio_os.path.exists(metadata_dir):
                await aio_os.mkdir(metadata_dir)

            for img in metadata["covers"]:
                cover = metadata["covers"][img]
                img_ext = os.path.splitext(cover)[1]
                img_rel = f"{os.path.basename(_filename)}-{img}" + img_ext
                img_name = os.path.join(metadata_dir, img_rel)
                log.debug('Getting cover "%s"', img)
                for x in range(1 if img == "brand" else 2):  # "brand" covers seem to never be available
                    async with _SESSION_CLOUD.get(cover) as resp:
                        if resp.status == 200:
                            img_data = await resp.read()
                            if img_data:
                                log.debug('Got cover "%s"', img)
                                async with async_open(img_name, "wb") as f:
                                    await f.write(img_data)
                                covers[img] = img_name[len(RECORDINGS) + 1 :]
                                await utime(mtime, img_name)
                                break

                    msg = 'Failed to get cover "%s" => %s' % (img, str(resp).splitlines()[0])
                    log.warning(msg) if img == "fanart" else log.debug(msg)
                    if not any((x, img == "brand")):
                        await asyncio.sleep(2)

            if covers:
                metadata["covers"] = covers
            else:
                del metadata["covers"]
                await remove(metadata_dir)
        else:
            log.debug("No extended covers in metadata")

        if RECORDINGS_TMP:
            await _save_cover_cache(metadata)

        metadata = {k: v for k, v in metadata.items() if k not in DROP_KEYS}
        metadata.update({"beginTime": mtime, "duration": duration, "endTime": mtime + duration})
        metadata.update({"expDate": metadata["expDate"] // 1000})
        metadata.update({"name": os.path.basename(_args.filename)})

        program_title_url = f"{U7D_URL}/program_title/{_args.channel}/{_args.program}"
        async with _SESSION.get(program_title_url, params={"cloud": 1} if _args.cloud else {}) as resp:
            if resp.status == 200:
                _meta = await resp.json()
                if metadata["name"] != _meta["full_title"]:
                    metadata["originalName"] = _meta["full_title"]
            else:
                log.warning(f'Could not verify "full_title" => {await resp.text()}')

        xml = xmltodict.unparse({"metadata": dict(sorted(metadata.items()))}, pretty=True)
        async with async_open(_filename + NFO_EXT, "w", encoding="utf8") as f:
            log.debug("Writing XML Metadata")
            await f.write(xml)

        await utime(mtime, _filename + NFO_EXT)
        log.debug("XML Metadata saved")

    async def _step_1():
        duration = await _get_duration(_tmpname + TMP_EXT)
        bad = duration < _args.time * 95 // 100

        log_suffix = f"[{str(timedelta(seconds=duration))}s = {str(duration):>5}s] / [{str(_args.time):>5}s]"

        msg = f"POSTPROCESS #1  - Recording is {'INCOMPLETE' if bad else 'COMPLETE'}"
        msg = DIV_LOG % (msg, log_suffix)

        if bad:
            raise RecordingError(msg)
        log.info(msg)

    async def _step_2():
        global COMSKIP
        nonlocal mtime, proc, tags

        if not RECORDINGS_TRANSCODE_OUTPUT:
            log.info("POSTPROCESS #2  - Skipped. Transcoding disabled")
            return

        cmd = ["ffmpeg"] + RECORDINGS_TRANSCODE_INPUT + ["-i", _tmpname + TMP_EXT]

        _info = await get_vod_info(_SESSION_CLOUD, _END_POINT, _args.channel, _args.cloud, _args.program)
        if not _info:
            log.warning("POSTPROCESS #2  - Could not verify event has not shifted")
        else:
            new_mtime = _info["beginTime"] // 1000 + _args.start
            if mtime != new_mtime:
                msg = DIV_LOG % ("POSTPROCESS #2  - Event CHANGED", f"beginTime=[{new_mtime - mtime:+}s]")
                if new_mtime < mtime:
                    raise RecordingError(msg)
                log.info(msg)

                cmd += ["-ss", str(timedelta(seconds=new_mtime - mtime))]

        if _args.vo:
            cmd += ["-map", "0:v", "-map", "0:a:1?", "-map", "0:a:0", "-map", "0:s?"]
        else:
            cmd += ["-map", "0:v", "-map", "0:a", "-map", "0:s?"]
        tags += await _get_language_tags(_tmpname + TMP_EXT, _args.vo)

        cmd += RECORDINGS_TRANSCODE_OUTPUT

        if NO_SUBS:
            log.info("POSTPROCESS #2  - Dropping subs")
            cmd.append("-sn")

        img_mime, img_name = await _save_metadata(get_cover=True)
        if all((_args.mkv, img_mime, img_name)):
            tags += ["-attach", img_name, "-metadata:s:t:0", f"mimetype={img_mime}"]

        cmd += [*tags, "-v", "error", "-y", "-f", "matroska" if _args.mkv else "mpegts", _tmpname + TMP_EXT2]

        msg = "POSTPROCESS #2  - Remuxing/Transcoding"
        if _info and mtime != new_mtime:
            msg = DIV_LOG % (msg, f"Cutting first [{new_mtime - mtime}s]")
            mtime = new_mtime

        log.info(msg)
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

        await _check_process("Failed remuxing/transcoding, leaving as is")

        if proc.returncode:
            COMSKIP = None
            await _cleanup(TMP_EXT2)
        else:
            await rename(_tmpname + TMP_EXT2, _tmpname + TMP_EXT)

    async def _step_3():
        global COMSKIP
        nonlocal proc

        cmd = ("comskip", *COMSKIP, "--ts", _tmpname + TMP_EXT)

        log.info("POSTPROCESS #3A - COMSKIP - Checking recording for commercials")
        async with async_open(COMSKIP_LOG, "ab") as f:
            start = time.time()
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=f, stderr=f)
            await _check_process()
            end = time.time()

        COMSKIP = None if proc.returncode else COMSKIP
        msg1 = f"POSTPROCESS #3B - COMSKIP - Commercials {'NOT found' if proc.returncode else 'found'}"
        msg2 = f"In [{str(timedelta(seconds=round(end - start)))}s]"
        msg = DIV_LOG % (msg1, msg2)
        log.warning(msg) if proc.returncode else log.info(msg)

    async def _step_4():
        nonlocal proc, tags

        if COMSKIP and await aio_os.path.exists(_tmpname + CHP_EXT):
            intervals = []
            pieces = []

            async with async_open(_tmpname + CHP_EXT) as f:
                c = await f.read()

            _s = filter(lambda x: "Show Segment" in x, (" ".join(c.splitlines()).split("[CHAPTER]"))[1:])
            segments = tuple(_s)
            if not segments:
                log.warning("POSTPROCESS #4  - COMSKIP - Could not find any Show Segment")
                await _cleanup(CHP_EXT, ".log", ".logo.txt", ".txt")
                return

            if _args.comskipcut and not _args.mkv:
                for segment in segments:
                    r = re.match(r" TIMEBASE=[^ ]+ START=([^ ]+) END=([^ ]+) .+", segment)
                    start, end = map(lambda x: str(timedelta(seconds=int(x) / 100)), r.groups())
                    start, end = map(lambda x: x + (".000000" if len(x) < 9 else ""), (start, end))
                    intervals.append((start, end))

                for idx, (start, end) in enumerate(intervals, start=1):
                    tmpdir = os.path.dirname(_tmpname)
                    pieces.append(os.path.join(tmpdir, f"{idx:02}_show_segment{VID_EXT}"))
                    cmd = ("ffmpeg", "-i", _tmpname + TMP_EXT, "-ss", start, "-to", end)
                    cmd += ("-c", "copy", "-map", "0", *tags, "-v", "error", "-y", pieces[-1])

                    ch = chr(idx + 64)
                    msg1, msg2 = f"POSTPROCESS #4{ch} - Cutting Chapter [{idx:02}]", f"({start} - {end})"
                    log.info(DIV_LOG, msg1, msg2)
                    proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                    await _check_process(f"Failed Cutting Chapter [{idx:02}]")

                cmd = ("ffmpeg", "-i", f"concat:{'|'.join(pieces)}", "-c", "copy", "-map", "0")
                cmd += (*tags, "-v", "error", "-y", "-f", "mpegts", _tmpname + TMP_EXT2)

                ch = chr(ord(ch) + 1)
                log.info(f"POSTPROCESS #4{ch} - Merging recording w/o commercials")
                proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                await _check_process("Failed merging recording w/o commercials")

            elif _args.mkv:
                cmd = ("ffmpeg", "-i", _tmpname + TMP_EXT, "-i", _tmpname + CHP_EXT, *tags)
                cmd += ("-v", "error", "-y", "-f", "matroska", _tmpname + TMP_EXT2)

                log.info("POSTPROCESS #4  - COMSKIP - Merging mkv chapters")
                proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                await _check_process("Failed merging mkv chapters")

            elif RECORDINGS_TMP:
                shutil.copy2(_tmpname + CHP_EXT, _filename + CHP_EXT)

            if proc.returncode:
                await _cleanup(TMP_EXT2)
            elif await aio_os.path.exists(_tmpname + TMP_EXT2):
                await rename(_tmpname + TMP_EXT2, _tmpname + TMP_EXT)

            if _args.comskipcut or _args.mkv:
                await _cleanup(CHP_EXT)
            await _cleanup(".log", ".logo.txt", ".txt")
            await remove(*pieces)

    async def _step_5():
        nonlocal mtime

        duration = await _get_duration(_tmpname + TMP_EXT)
        length = f"[{str(timedelta(seconds=duration))}s]"
        if COMSKIP and _args.time != duration:
            cmrcls = f"{str(timedelta(seconds=_args.time - duration))}s]"
            length = f" [{str(timedelta(seconds=_args.time))}s - {cmrcls} = {length}"
        log.info(DIV_LOG, "POSTPROCESS #5  - Archiving recording", length)

        await _archive_recording()

        if _args.index:
            await _save_metadata(duration=duration)

        dirname = os.path.dirname(_filename)
        metadata_dir = os.path.join(dirname, "metadata")

        await utime(mtime, _filename + VID_EXT)
        if await aio_os.path.exists(metadata_dir):
            newest_ts = await aio_os.path.getmtime(
                sorted(glob_safe(f"{metadata_dir}/*"), key=os.path.getmtime)[-1]
            )
            await utime(newest_ts, metadata_dir)
        newest_ts = await aio_os.path.getmtime(
            sorted(glob_safe(f"{dirname}/*{VID_EXT}"), key=os.path.getmtime)[-1]
        )
        await utime(newest_ts, dirname)

        if _args.index:
            archive_url = f"{U7D_URL}/archive/{_args.channel}/{_args.program}"
            resp = await _SESSION.put(archive_url, params={"cloud": 1} if _args.cloud else {})
            if resp.status != 200:
                log.error("Failed indexing recording")

    await asyncio.sleep(0.1)  # Prioritize the main loop

    metadata = proc = None
    mtime = vod_info["beginTime"] // 1000 + _args.start
    tags = ["-metadata", 'service_name="%s"' % vod_info["channelName"]]
    tags += ["-metadata", 'service_provider="Movistar IPTV"']
    tags += ["-metadata:s:v", f"title={os.path.basename(_args.filename)}"] if _args.mkv else []

    lockfile = os.path.join(TMP_DIR, ".mu7d_vod.lock")
    pp_lock = FileLock(lockfile)

    try:
        pp_lock.acquire(poll_interval=5)
        log.debug("POSTPROCESS STARTS")

        await _step_1()  # Check actual length
        await _step_2()  # Remux/Transcode

        if COMSKIP:
            await _step_3()  # Comskip analysis
            await _step_4()  # Cut/Merge chapters

        await asyncio.shield(_step_5())  # Archive recording

        log.debug("POSTPROCESS ENDED")

    except (CancelledError, ClientConnectionError, ClientOSError, RecordingError, ServerDisconnectedError) as ex:
        await asyncio.shield(_cleanup_recording(ex))

    finally:
        if pp_lock.is_locked:
            pp_lock.release()


async def record_stream(vod_info):
    global _filename, _tmpname

    if not _args.filename:
        _args.filename = f"{vod_info['channelName']} - {get_safe_filename(vod_info['name'])}"

    log_suffix = f": [{_args.channel:4}] [{_args.program}] [{vod_info['beginTime'] // 1000}]"
    log_suffix += f' "{_args.filename}"'

    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.propagate = False
    formatter = logging.Formatter(fmt=f"{FMT[:-1]}-104s{log_suffix}", datefmt=DATEFMT)
    tuple(handler.setFormatter(formatter) for handler in log.handlers)

    ongoing = await ongoing_vods(filename=_args.filename)
    if len(ongoing) > 1:
        log.error("Recording already ongoing")
        return

    if not _args.time:
        _args.time = vod_info["duration"]
    else:
        if U7D_PARENT:
            if _args.time <= 300:
                _args.time *= 3
                _args.time //= 2
            elif _args.time <= 1800:
                _args.time *= 4
                _args.time //= 3
            else:
                _args.time *= 7
                _args.time //= 6
        _args.time = min(_args.time, vod_info["duration"])

    flags = "[COMSKIPCUT] " if _args.comskipcut else "[COMSKIP] " if _args.comskip else ""
    flags += "[MKV] " if _args.mkv else ""
    flags += "[VO] " if _args.vo else ""
    log_start = f"{flags}[{str(timedelta(seconds=_args.time)):>7}s = {_args.time:>5}s]"

    _filename = _tmpname = os.path.join(RECORDINGS, _args.filename)

    if RECORDINGS_TMP:
        _tmpname = os.path.join(RECORDINGS_TMP, _args.filename)

    path = os.path.dirname(_tmpname)
    if not await aio_os.path.exists(path):
        log.debug('Making dir "%s"', path)
        await aio_os.makedirs(path)

    buflen = BUFF // CHUNK

    async def _buffer():
        buffer = bytearray()
        for _ in range(buflen):
            buffer += (await stream.recv())[0]
        return buffer

    end = _args.time + time.time()
    log.info(DIV_LOG, "Recording STARTED", log_start)
    try:
        with closing(await dgram_bind((_IPTV, _args.client_port))) as stream:
            async with async_open(_tmpname + TMP_EXT, "wb") as f:
                if not vod_info.get("isHdtv"):
                    # 1st packet on SDTV channels is bogus and breaks ffmpeg
                    await asyncio.wait_for(stream.recv(), timeout=1.0)

                while time.time() < end:
                    await f.write(await asyncio.wait_for(_buffer(), timeout=1.0))

    except (CancelledError, TransportClosed):
        return await asyncio.shield(_cleanup_recording(CancelledError(), end - _args.time))

    except TimeoutError:
        log.debug("TIMED OUT")

    record_time = int(time.time() - end + _args.time)
    log.info(DIV_LOG, "Recording ENDED", "[%6ss] / [%5ss]" % (f"~{record_time}", f"{_args.time}"))

    if not U7D_PARENT:
        return await _archive_recording()

    return asyncio.create_task(postprocess(vod_info))


async def rtsp(vod_info):
    # Open the RTSP session
    uri = urllib.parse.urlparse(vod_info["url"])
    reader, writer = await asyncio.open_connection(uri.hostname, uri.port)
    client = RtspClient(reader, writer, vod_info["url"])

    header = {"User-Agent": "MICA-IP-STB"}
    setup = {**header, "CSeq": "", "Transport": f"MP2T/H2221/UDP;unicast;client_port={_args.client_port}"}
    session = {**header, "Session": await client.send_request("SETUP", setup), "CSeq": ""}

    play = {**session, "Range": f"npt={_args.start:.3f}-end"}
    play.update({"Scale": "1.000", "x-playNow": "", "x-noFlush": ""})

    # Start playing the VOD stream
    if not await client.send_request("PLAY", play):
        client.close_connection()
        return

    rec_t = None
    if __name__ == "__main__":
        if _args.write_to_file:
            if not WIN32:
                setproctitle(getproctitle().replace("mu7d_vod     ", "mu7d_vod REC "))
            # Start recording the VOD stream
            rec_t = asyncio.create_task(record_stream(vod_info))
        else:
            log.info(f'The VOD stream can be accesed at: f"udp://@{_IPTV}:{_args.client_port}"')

    try:
        # Start the RTSP keep alive loop
        while True:
            if rec_t:
                done, _ = await asyncio.wait({rec_t}, timeout=30)
                if rec_t in done:
                    break
            else:
                await asyncio.sleep(30)
            if not await client.send_request("GET_PARAMETER", session):
                break

    finally:
        # Close the RTSP session, reducing bandwith
        await client.send_request("TEARDOWN", session)
        client.close_connection()
        log.debug("[%4s] [%d]: RTSP loop ended", str(_args.channel), _args.program)

    if rec_t:
        if not rec_t.done():
            await rec_t

        if not WIN32:
            setproctitle(getproctitle().replace(" REC ", "     "))

        pp_t = rec_t.result()
        if pp_t:
            await pp_t


async def Vod(args=None, vod_client=None, vod_info=None):  # pylint: disable=invalid-name
    if __name__ == "__main__":
        global _END_POINT, _SESSION_CLOUD, _loop

        _loop = asyncio.get_running_loop()

        _END_POINT = await get_end_point()
        if not _END_POINT:
            return

        await _open_sessions()

    else:
        global _SESSION_CLOUD, _args

        if not all((args, vod_client)):
            return

        _SESSION_CLOUD = vod_client
        _args = args

    if not vod_info:
        vod_info = await get_vod_info(_SESSION_CLOUD, _END_POINT, _args.channel, _args.cloud, _args.program)

    try:
        if not vod_info:
            log.error(f"[{_args.channel:4}] [{_args.program}]: NOT AVAILABLE")
            return

        log.debug("[%4s] [%d]: vod_info=%s", str(_args.channel), _args.program, str(vod_info))
        # Launch the RTSP Session
        await rtsp(vod_info)

    finally:
        if __name__ == "__main__":
            await _SESSION_CLOUD.close()
            if _args.write_to_file:
                await _SESSION.close()


if __name__ == "__main__":
    if not WIN32:
        from setproctitle import getproctitle, setproctitle

        setproctitle("mu7d_vod      # %s" % " ".join(sys.argv[1:]))

        def cancel_handler(signum, frame):  # pylint: disable=unused-argument
            if _loop:
                [task.cancel() for task in asyncio.all_tasks(_loop)]

        [signal(sig, cancel_handler) for sig in (SIGINT, SIGTERM)]

    else:
        import win32api  # pylint: disable=import-error
        import win32con  # pylint: disable=import-error

        def close_handler(event):
            log.debug("close_handler(event=%d)", event)
            if _loop and event in (
                win32con.CTRL_BREAK_EVENT,
                win32con.CTRL_C_EVENT,
                win32con.CTRL_CLOSE_EVENT,
                win32con.CTRL_LOGOFF_EVENT,
                win32con.CTRL_SHUTDOWN_EVENT,
            ):
                for child in psutil.Process().children():
                    while child.is_running():
                        log.debug("close_handler(event=%d): Killing '%s'", event, child.name())
                        child.kill()
                        time.sleep(0.01)

                time.sleep(0.05)
                log.debug("Cancelling tasks")
                while not all((task.done() for task in asyncio.all_tasks(_loop))):
                    [task.cancel() for task in asyncio.all_tasks(_loop)]
                    time.sleep(0.05)
            return True

        win32api.SetConsoleCtrlHandler(close_handler, True)

    # pylint: disable=invalid-name
    _END_POINT = _IPTV = _SESSION = _SESSION_CLOUD = _filename = _loop = _tmpname = None

    if not CONF:
        log.critical("Imposible parsear fichero de configuración")
        sys.exit(1)

    DEBUG = CONF["DEBUG"]

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=DEBUG and logging.DEBUG or logging.INFO)

    if CONF["LOG_TO_FILE"]:
        add_logfile(log, CONF["LOG_TO_FILE"], DEBUG and logging.DEBUG or logging.INFO)

    parser = argparse.ArgumentParser(f"Movistar U7D - VOD v{VERSION}")
    parser.add_argument("channel", help="channel id", type=int)
    parser.add_argument("program", help="program id", type=int)

    parser.add_argument("-b", type=int, default=0)
    parser.add_argument("--client_ip", "-i", help="client ip address")
    parser.add_argument("--filename", "-o", help="output bare filename, relative to RECORDINGS path")

    parser.add_argument("--client_port", "-p", help="client udp port", type=int)
    parser.add_argument("--start", "-s", help="stream start offset", type=int, default=0)
    parser.add_argument("--time", "-t", help="recording time in seconds", type=int)

    parser.add_argument("--cloud", help="the event is from a cloud recording", action="store_true")
    parser.add_argument("--comskip", help="do comercials analysis, mark chapters", action="store_true")
    parser.add_argument("--comskipcut", help="do comercials analysis, cut chapters", action="store_true")
    parser.add_argument("--debug", help="enable debug logs", action="store_true")
    parser.add_argument("--index", help="index recording in db", action="store_true")
    parser.add_argument("--mkv", help="output recording in mkv container", action="store_true")
    parser.add_argument("--vo", help="set 2nd language as main one", action="store_true")
    parser.add_argument("--write_to_file", "-w", help="record", action="store_true")

    _args = parser.parse_args()

    DEBUG = _args.debug or DEBUG

    try:
        _IPTV = get_iptv_ip()
    except IPTVNetworkError as err:
        log.critical(err)
        sys.exit(1)

    if not _args.client_port:
        _args.client_port = find_free_port(get_iptv_ip())

    if _args.write_to_file:
        if not CONF["RECORDINGS"]:
            log.error("RECORDINGS path not set")
            sys.exit(1)

        CACHE_DIR = CONF["CACHE_DIR"]
        COMSKIP = (_args.comskip or _args.comskipcut) and CONF["COMSKIP"]
        COMSKIP_LOG = os.path.join(CONF["HOME"], "comskip.log") if COMSKIP else None
        NO_SUBS = CONF["NO_SUBS"]
        RECORDINGS = CONF["RECORDINGS"]
        RECORDINGS_TMP = CONF["RECORDINGS_TMP"]
        RECORDINGS_TRANSCODE_INPUT = CONF["RECORDINGS_TRANSCODE_INPUT"]
        RECORDINGS_TRANSCODE_OUTPUT = CONF["RECORDINGS_TRANSCODE_OUTPUT"]
        TMP_DIR = CONF["TMP_DIR"]
        U7D_URL = CONF["U7D_URL"]

        CHP_EXT = ".ffmeta"
        TMP_EXT = ".tmp"
        TMP_EXT2 = ".tmp2"
        VID_EXT = ".mkv" if _args.mkv else ".ts"

        if _args.index and not os.path.exists(os.path.join(CACHE_DIR, "programs", f"{_args.program}.json")):
            log.error(f"No metadata exists for [{_args.channel:4}] [{_args.program}]")
            sys.exit(1)

    U7D_PARENT = os.getenv("U7D_PARENT")

    del CONF

    try:
        asyncio.run(Vod())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(0)
