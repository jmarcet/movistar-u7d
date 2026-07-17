#!/usr/bin/env -S python3 -OO -X no_debug_ranges -X utf8 -u
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
import threading
import urllib.parse
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL as NULL, PIPE, STDOUT as OUT
from contextlib import closing, suppress
from datetime import timedelta
from signal import SIG_IGN, SIGINT, SIGTERM, signal
from time import time

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
    VID_EXT,
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

        # log.debug("[%d:%s]: Req  = [%s]", self.cseq, method, "|".join(resp))
        # log.debug("[%d:%s]: Resp = [%s]", self.cseq, method, "|".join(resp))

        self.cseq += 1

        if not resp or not resp[0].endswith("200 OK"):
            return

        if method == "SETUP":
            return [x for x in resp if x.startswith("Session: ")][0].split(": ")[1].split(";")[0]

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
            msg += " @ [%6ss] / [%5ss]" % ("~" + str(int(time() - start)), str(_args.time))
        log.error("%-17s%87s", "Recording FAILED", msg)
    else:
        log.error(f"Recording FAILED: {str(exception).split(' - ', 1)[-1]}")

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
        await remove(path)
        if not await aio_os.path.exists(path):
            log.debug("Removed path=%s", path)
        if parent not in (RECORDINGS, RECORDINGS_TMP):
            await remove(parent)
            if not await aio_os.path.exists(parent):
                log.debug("Removed parent=%s", parent)

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
    async def _check_process(msg="", fatal=True):
        nonlocal proc

        if not proc:
            raise RecordingError(msg)

        process = psutil.Process(proc.pid)
        process.nice(15 if not WIN32 else psutil.IDLE_PRIORITY_CLASS)
        if LINUX or WIN32:
            process.ionice(psutil.IOPRIO_CLASS_IDLE if LINUX else psutil.IOPRIO_VERYLOW)

        try:
            await proc.wait()
        except CancelledError as ex:
            with suppress(CancelledError):
                proc.terminate()
                await proc.wait()
            raise RecordingError(msg) from ex

        if proc.returncode:
            stdout = await proc.stdout.read() if proc.stdout else ""
            if stdout:
                msg += ": " if msg else ""
                msg += '"%s"' % re.sub(r"\s+", " ", stdout.decode().replace("\n", " ").strip())

            if fatal:
                raise RecordingError(msg)

            if msg:
                log.error(msg)

    async def _get_duration(recording):
        cmd = ("ffprobe", "-i", recording, "-show_entries", "format=duration", "-v", "quiet", "-of", "json")
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=NULL)
        recording_data = ujson.loads((await proc.communicate())[0].decode())

        return int(float(recording_data.get("format", {}).get("duration", 0)))

    async def _get_keyframes(recording):
        cmd = ("ffprobe", "-i", recording, "-v", "quiet", "-of", "csv", "-select_streams", "v:0")
        cmd += ("-show_entries", "format=start_time:packet=pts_time,flags")
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=NULL)

        keyframes, start_time = [], 0.0
        for line in (await proc.communicate())[0].decode().splitlines():
            kind, _, values = line.partition(",")
            if kind == "packet":
                pts, _, flags = values.partition(",")
                if "K" in flags and pts != "N/A":
                    keyframes.append(float(pts))
            elif kind == "format":
                start_time = float(values)
        return keyframes, start_time

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

    async def _save_metadata(duration):
        nonlocal metadata, mtime

        try:
            async with async_open(
                os.path.join(CACHE_DIR, "programs", f"{_args.program}.json"), encoding="utf8"
            ) as f:
                metadata = ujson.loads(await f.read())["data"]
        except (FileNotFoundError, OSError, PermissionError, TypeError, ValueError) as ex:
            if _args.index:
                raise RecordingError(f"Extended info not found => {repr(ex)}") from ex

        cover = metadata["cover"]
        img_ext = os.path.splitext(cover)[1]
        img_name = _filename + img_ext
        metadata["cover"] = img_name[len(RECORDINGS) + 1 :]
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
            except (ClientConnectionError, ClientOSError, ServerDisconnectedError) as ex:
                log.warning('Failed to get cover "%s" => %s', cover, str(ex).splitlines()[0])
                if x == 0:
                    await asyncio.sleep(2)
                else:
                    raise RecordingError("Failed to get cover") from ex

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
        nonlocal skip_start, step, tags

        duration = await _get_duration(_tmpname + TMP_EXT)
        bad = duration < _args.time * 95 // 100

        log_suffix = f"[{str(timedelta(seconds=duration))}s = {str(duration):>5}s] / [{str(_args.time):>5}s]"

        msg = f"POSTPROCESS #{step}  - Recording is {'INCOMPLETE' if bad else 'COMPLETE'}"
        msg = DIV_LOG % (msg, log_suffix)

        if bad:
            raise RecordingError(msg)

        tags += await _get_language_tags(_tmpname + TMP_EXT, _args.vo)

        log.info(msg)

        _info = await get_vod_info(_SESSION_CLOUD, _END_POINT, _args.channel, _args.cloud, _args.program)
        if not _info:
            log.warning(f"POSTPROCESS #{step}  - Could not verify event has not shifted")
        else:
            new_mtime = _info["beginTime"] // 1000 + _args.start
            if mtime != new_mtime:
                msg = DIV_LOG % (f"POSTPROCESS #{step}  - Event CHANGED", f"beginTime=[{new_mtime - mtime:+}s]")
                if new_mtime < mtime:
                    raise RecordingError(msg)
                skip_start = new_mtime - mtime
                log.info(msg)

    async def _step_2():
        global COMSKIP
        nonlocal proc, step

        step += 1

        cmd = ("comskip", *COMSKIP, "--ts", _tmpname + TMP_EXT)  # pylint: disable=used-before-assignment

        log.info(f"POSTPROCESS #{step}A - COMSKIP - Checking recording for commercials")
        async with async_open(COMSKIP_LOG, "ab") as f:
            start = time()
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=f, stderr=f)
            await _check_process(fatal=False)
            end = time()

        COMSKIP = None if any((proc.returncode, not await aio_os.path.exists(_tmpname + CHP_EXT))) else COMSKIP
        msg1 = f"POSTPROCESS #{step}B - COMSKIP - Commercials {'found' if COMSKIP else 'NOT found'}"
        msg2 = f"In [{str(timedelta(seconds=round(end - start)))}s]"
        msg = DIV_LOG % (msg1, msg2)
        log.warning(msg) if proc.returncode else log.info(msg)

    async def _step_3():
        nonlocal proc, skip_start, step, tags

        if COMSKIP:
            intervals = []
            step += 1

            async with async_open(_tmpname + CHP_EXT) as f:
                c = await f.read()

            _s = filter(lambda x: "Show Segment" in x, (" ".join(c.splitlines()).split("[CHAPTER]"))[1:])
            segments = tuple(_s)
            if not segments:
                log.warning(f"POSTPROCESS #{step}  - COMSKIP - Could not find any Show Segment")
                await _cleanup(CHP_EXT, ".log", ".logo.txt", ".txt")
                return

            if _args.comskipcut:
                for segment in segments:
                    r = re.match(r" TIMEBASE=[^ ]+ START=([^ ]+) END=([^ ]+) .+", segment)
                    intervals.append(tuple(map(lambda x: int(x) / 100, r.groups())))

                if skip_start:
                    while True:
                        if skip_start <= intervals[0][0]:
                            skip_start = None
                            break
                        if intervals[0][0] < skip_start < intervals[0][1]:
                            skip_start -= intervals[0][0]
                            break
                        intervals = intervals[1:]

                # ffmpeg filters cut points by DTS, so with reordered video every cut keeps the
                # audio of its last frames but loses their video, desyncing both streams a bit
                # more at every splice. The segment muxer instead assigns every packet to the
                # right piece, cutting at keyframes: with the cut points pre-snapped to them,
                # video and audio stay balanced within every piece, and through the final merge.
                keyframes, start_time = await _get_keyframes(_tmpname + TMP_EXT)

                cuts = []
                for idx, (start, end) in enumerate(intervals, start=1):
                    inpoint = next((k for k in keyframes if k >= start + start_time - 0.001), None)
                    outpoint = next((k for k in reversed(keyframes) if k <= end + start_time + 0.001), None)
                    if inpoint is None or outpoint is None or inpoint >= outpoint:
                        log.warning(f"POSTPROCESS #{step}  - Dropping empty Chapter [{idx:02}]")
                        continue
                    cuts.append((inpoint - start_time, outpoint - start_time))

                    ch = chr(len(cuts) + 64)
                    _t = map(lambda x: f"{timedelta(seconds=round(x, 2))}", cuts[-1])
                    start, end = map(lambda x: x + (".000000" if len(x) < 9 else ""), _t)
                    msg1, msg2 = f"POSTPROCESS #{step}{ch} - Cutting Chapter [{idx:02}]", f"({start} - {end})"
                    log.info(DIV_LOG, msg1, msg2)

                if not cuts:
                    log.warning(f"POSTPROCESS #{step}  - COMSKIP - Could not cut any Show Segment")
                    await _cleanup(CHP_EXT, ".log", ".logo.txt", ".txt")
                    return

                times = sorted({t for cut in cuts for t in cut if t > 0.001})
                pieces = [f"{_tmpname}.{i}{VID_EXT}" for i in range(len(times) + 1)]

                cmd = ("ffmpeg", "-i", _tmpname + TMP_EXT, "-map", "0", "-c", "copy", "-f", "segment")
                cmd += ("-segment_times", ",".join(f"{t:.6f}" for t in times), "-reset_timestamps", "1")
                cmd += ("-v", "error", "-y", _tmpname.replace("%", "%%") + ".%d" + VID_EXT)

                ch = chr(ord(ch) + 1)
                log.info(f"POSTPROCESS #{step}{ch} - Splitting recording at the Chapter boundaries")
                proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                await _check_process("Failed splitting recording")

                bounds = (0.0, *times, float("inf"))
                shows = filter(
                    lambda i: any(s - 0.1 <= bounds[i] and bounds[i + 1] <= e + 0.1 for s, e in cuts),
                    range(len(pieces)),
                )

                # Declaring the exact durations, the merge does not need to estimate them,
                # which would shift the splices, and the merged recording becomes seekable
                async with async_open(_tmpname + CAT_EXT, "w", encoding="utf8") as f:
                    lines = ["ffconcat version 1.0"]
                    for i in shows:
                        quoted = pieces[i].replace("'", "'\\''")
                        lines.append(f"file '{quoted}'\nduration {bounds[i + 1] - bounds[i]:.6f}")
                    await f.write("\n".join(lines) + "\n")

                cmd = ("ffmpeg", "-f", "concat", "-safe", "0", "-i", _tmpname + CAT_EXT)
                cmd += ("-map", "0", "-c", "copy", *tags, "-v", "error", "-y", "-f", "mpegts")
                cmd += (_tmpname + TMP_EXT2,)

                ch = chr(ord(ch) + 1)
                merged = round(sum(end - start for start, end in cuts))
                msg1 = f"POSTPROCESS #{step}{ch} - Merging recording w/o commercials"
                log.info(DIV_LOG, msg1, f"[{timedelta(seconds=merged)}s = {merged}s] / [{str(_args.time):>5}s]")
                proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                await _check_process("Failed merging recording w/o commercials")
                await remove(*pieces)

            elif RECORDINGS_TMP:
                shutil.copy2(_tmpname + CHP_EXT, _filename + CHP_EXT)

            if await aio_os.path.exists(_tmpname + TMP_EXT2):
                await rename(_tmpname + TMP_EXT2, _tmpname + TMP_EXT)

            if _args.comskipcut:
                await _cleanup(CAT_EXT, CHP_EXT)
            await _cleanup(".log", ".logo.txt", ".txt")

    async def _step_4():
        nonlocal mtime, proc, skip_start, step, tags

        step += 1

        if not RECORDINGS_TRANSCODE_OUTPUT:
            log.info(f"POSTPROCESS #{step}  - Skipped. Remuxing/Transcoding disabled")
            return

        cmd = ["ffmpeg"] + RECORDINGS_TRANSCODE_INPUT + ["-i", _tmpname + TMP_EXT]

        if _args.vo:
            cmd += ["-map", "0:v", "-map", "0:a:1?", "-map", "0:a:0", "-map", "0:s?"]
        else:
            cmd += ["-map", "0:v", "-map", "0:a", "-map", "0:s?"]

        cmd += RECORDINGS_TRANSCODE_OUTPUT

        if NO_SUBS:
            log.info(f"POSTPROCESS #{step}  - Dropping subs")
            cmd.append("-sn")

        cmd += [*tags, "-v", "info", "-y", "-f", "mpegts", _tmpname + TMP_EXT2]

        if re.search(r"-c[:\w]* (?!copy)", " ".join(RECORDINGS_TRANSCODE_OUTPUT)):
            _msg = "Transcoding"
        else:
            _msg = "Remuxing"

        msg = msg1 = f"POSTPROCESS #{step}A - {_msg}"
        if skip_start:
            msg = DIV_LOG % (msg1, f"Cutting first [{skip_start}s]")
            cmd += ["-ss", str(timedelta(seconds=skip_start))]

        log.info(msg)
        async with async_open(TRANSCODE_LOG, "ab") as f:
            start = time()
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=f, stderr=f)
            await _check_process(f"Failed {_msg}")
            end = time()

        if _msg == "Transcoding":
            size_orig = (await aio_os.stat(_tmpname + TMP_EXT)).st_size
            size_dest = (await aio_os.stat(_tmpname + TMP_EXT2)).st_size
            if size_dest < size_orig:

                def _h(num):
                    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
                        if abs(num) < 1024.0:
                            return f"{num:3.1f}{unit}B"
                        num /= 1024.0
                    return f"{num:.1f}YiB"

                msg1 += f" - Saved [{_h(size_orig)} - {_h(size_dest)}] ="
                msg1 += f" [{_h(size_orig - size_dest)} ({(size_orig - size_dest) / size_orig * 100:.2f}%)]"

        await rename(_tmpname + TMP_EXT2, _tmpname + TMP_EXT)

        msg1 = msg1.replace(f"#{step}A", f"#{step}B").replace("ing", "ed")
        msg = "%-84s%20s" % (msg1, f"In [{str(timedelta(seconds=round(end - start)))}s]")
        log.info(msg)

    async def _step_5():
        nonlocal mtime, step

        step += 1

        duration = await _get_duration(_tmpname + TMP_EXT)
        length = f"[{str(timedelta(seconds=duration))}s]"
        if COMSKIP and _args.time != duration:
            cmrcls = f"{str(timedelta(seconds=_args.time - duration))}s]"
            length = f" [{str(timedelta(seconds=_args.time))}s - {cmrcls} = {length}"
        log.info(DIV_LOG, f"POSTPROCESS #{step}  - Archiving recording", length)

        await _archive_recording()

        if _args.index:
            await _save_metadata(duration)

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

    step = 1
    metadata = proc = skip_start = None
    mtime = vod_info["beginTime"] // 1000 + _args.start
    tags = ["-metadata", 'service_name="%s"' % vod_info["channelName"]]
    tags += ["-metadata", 'service_provider="Movistar IPTV"']

    lockfile = os.path.join(TMP_DIR, ".mu7d_vod.lock")
    pp_lock = FileLock(lockfile)

    try:
        pp_lock.acquire(poll_interval=5)
        log.debug("POSTPROCESS STARTS")

        await _step_1()  # Check actual length

        if COMSKIP:
            await _step_2()  # Comskip analysis
            await _step_3()  # Cut/Merge chapters

        await _step_4()  # Remux/Transcode

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
        _args.time = min(_args.time, vod_info["duration"])

    flags = "[COMSKIPCUT] " if _args.comskipcut else "[COMSKIP] " if _args.comskip else ""
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

    end = _args.time + time()
    log.info(DIV_LOG, "Recording STARTED", log_start)
    try:
        with closing(await dgram_bind((_IPTV, _args.client_port))) as stream:
            async with async_open(_tmpname + TMP_EXT, "wb") as f:
                if not vod_info.get("isHdtv"):
                    # 1st packet on SDTV channels is bogus and breaks ffmpeg
                    await asyncio.wait_for(stream.recv(), timeout=1.0)

                while time() < end:
                    await f.write(await asyncio.wait_for(_buffer(), timeout=1.0))

    except (CancelledError, TransportClosed) as ex:
        await asyncio.shield(_cleanup_recording(CancelledError(), end - _args.time))
        raise CancelledError from ex

    except TimeoutError:
        log.debug("TIMED OUT")

    finally:
        if not WIN32:
            setproctitle(getproctitle().replace(" REC ", "     "))

    record_time = int(time() - end + _args.time)
    log.info(DIV_LOG, "Recording ENDED", "[%6ss] / [%5ss]" % (f"~{record_time}", f"{_args.time}"))


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
            rec_t = asyncio.create_task(record_stream(vod_info), name="recording")
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
            with suppress(CancelledError):
                await rec_t

    if rec_t and _filename:
        if U7D_PARENT:
            await postprocess(vod_info)
        else:
            await asyncio.shield(_archive_recording())


async def Vod(args=None, vod_client=None, vod_info=None):  # pylint: disable=invalid-name
    if __name__ == "__main__":
        global _END_POINT, _SESSION_CLOUD

        _END_POINT = await get_end_point()
        if not _END_POINT:
            return

        add_exit_handlers(asyncio.get_running_loop())

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

    def add_exit_handlers(loop):
        async def exit_handler(loop):
            [signal(sig, SIG_IGN) for sig in (SIGINT, SIGTERM)]
            tasks = tuple(t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop))
            log.debug("Cancelling vod tasks...")
            tuple(task.cancel() for task in tasks)
            with suppress(CancelledError):
                log.debug("Waiting for vod tasks...")
                await asyncio.gather(*tasks)
                log.debug("bye")
                sys.exit(1)

        def exit_handler_win(event):  # pylint: disable=duplicate-code
            import win32con  # pylint: disable=import-error

            log.debug("exit_handler_win(event=%d)", event)
            if event in (
                win32con.CTRL_BREAK_EVENT,
                win32con.CTRL_C_EVENT,
                win32con.CTRL_CLOSE_EVENT,
                win32con.CTRL_LOGOFF_EVENT,
                win32con.CTRL_SHUTDOWN_EVENT,
            ):
                done = threading.Event()

                def cancel_tasks():
                    tasks = tuple(t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop))
                    log.debug("Cancelling vod tasks...")
                    tuple(task.cancel() for task in tasks)

                    def check_done():
                        if all(task.done() for task in tasks):
                            done.set()
                        else:
                            loop.call_later(0.1, check_done)

                    check_done()

                log.debug("Waiting for vod tasks...")
                loop.call_soon_threadsafe(cancel_tasks)
                # Windows kills the process ~5s after a CTRL_CLOSE_EVENT, as soon as this handler returns
                done.wait(4.5)
                log.debug("bye")

            return True

        if not WIN32:
            loop.add_signal_handler(SIGINT, lambda: asyncio.create_task(exit_handler(loop)))
            loop.add_signal_handler(SIGTERM, lambda: asyncio.create_task(exit_handler(loop)))
        else:
            import win32api  # pylint: disable=import-error

            win32api.SetConsoleCtrlHandler(exit_handler_win, True)

    # pylint: disable=invalid-name
    _END_POINT = _IPTV = _SESSION = _SESSION_CLOUD = _filename = _tmpname = None

    if CONF.get("Exception"):
        log.critical(f"Imposible parsear fichero de configuración => {repr(CONF['Exception'])}")
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

        if not os.access(CONF["RECORDINGS"], os.R_OK | os.W_OK):
            log.critical(f'Cannot acceess RECORDINGS="{CONF["RECORDINGS"]}"')
            sys.exit(1)

        if CONF["RECORDINGS_TMP"] and not os.access(CONF["RECORDINGS_TMP"], os.R_OK | os.W_OK):
            log.warning(f'Cannot access RECORDINGS_TMP="{CONF["RECORDINGS_TMP"]}" => Disabling RECORDINGS_TMP')
            CONF["RECORDINGS_TMP"] = ""

        CACHE_DIR = CONF["CACHE_DIR"]
        COMSKIP = CONF["COMSKIP"] if (_args.comskip or _args.comskipcut) else None
        COMSKIP_LOG = os.path.join(CONF["HOME"], "comskip.log") if COMSKIP else None
        NO_SUBS = CONF["NO_SUBS"]
        RECORDINGS = CONF["RECORDINGS"]
        RECORDINGS_TMP = CONF["RECORDINGS_TMP"]
        RECORDINGS_TRANSCODE_INPUT = CONF["RECORDINGS_TRANSCODE_INPUT"]
        RECORDINGS_TRANSCODE_OUTPUT = CONF["RECORDINGS_TRANSCODE_OUTPUT"]
        TRANSCODE_LOG = os.path.join(CONF["HOME"], "transcode.log") if RECORDINGS_TRANSCODE_OUTPUT else None
        TMP_DIR = CONF["TMP_DIR"]
        U7D_URL = CONF["U7D_URL"]

        CAT_EXT = ".ffconcat"
        CHP_EXT = ".ffmeta"
        TMP_EXT = ".tmp"
        TMP_EXT2 = ".tmp2"

        if _args.index and not os.path.exists(os.path.join(CACHE_DIR, "programs", f"{_args.program}.json")):
            log.error(f"No metadata exists for [{_args.channel:4}] [{_args.program}]")
            sys.exit(1)

    U7D_PARENT = os.getenv("U7D_PARENT")

    del CONF

    try:
        asyncio.run(Vod())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
