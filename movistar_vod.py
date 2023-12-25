#!/usr/bin/env python3
#
# Started from MovistarU7D by XXLuigiMario:
# Source: https://github.com/XXLuigiMario/MovistarU7D

import aiofiles
import aiohttp
import argparse
import asyncio
import asyncio_dgram
import logging
import os
import psutil
import re
import shutil
import signal
import sys
import time
import ujson
import urllib.parse
import xmltodict

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL as NULL, PIPE, STDOUT as OUT
from datetime import timedelta
from filelock import FileLock
from glob import glob

from mu7d import BUFF, CHUNK, DATEFMT, DIV_LOG, DROP_KEYS, EPG_URL, FMT, IPTV_DNS
from mu7d import NFO_EXT, UA, URL_COVER, URL_MVTV, WIN32, YEAR_SECONDS
from mu7d import add_logfile, find_free_port, get_iptv_ip, glob_safe
from mu7d import mu7d_config, ongoing_vods, remove, utime, _version


log = logging.getLogger("VOD")


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

        # log.debug(f"[{self.cseq}]: Req = [{'|'.join(req.splitlines())}]")
        # log.debug(f"[{self.cseq}]: Resp = [{'|'.join(resp)}]")

        self.cseq += 1

        if not resp or not resp[0].endswith("200 OK"):
            return

        if method == "SETUP":
            return resp[1].split(": ")[1].split(";")[0]

        return True

    def serialize_headers(self, headers):
        return "\r\n".join(map(lambda x: "{0}: {1}".format(*x), headers.items()))


def _archive_recording():
    path = os.path.dirname(_filename)
    if not os.path.exists(path):
        log.debug(f'Making dir "{path}"')
        os.makedirs(path)

    if not RECORDINGS_TMP:
        _cleanup(VID_EXT)
        os.rename(_tmpname + TMP_EXT, _filename + VID_EXT)

    else:
        remove(_filename + VID_EXT, _filename + ".jpg", _filename + ".png")

        covers = [x for x in glob(f"{_tmpname}.*") if x.endswith((".jpg", ".png"))]
        if covers:
            tmpcover = covers[0]
            cover_ext = os.path.splitext(tmpcover)[1]
            shutil.copy2(tmpcover, _filename + cover_ext)
            remove(tmpcover)

        shutil.copy2(_tmpname + TMP_EXT, _filename + VID_EXT)
        _cleanup(TMP_EXT)

        path = os.path.dirname(_tmpname)
        parent = os.path.split(path)[0]
        remove(path)
        if parent != RECORDINGS_TMP:
            remove(parent)


def _cleanup(*exts, meta=False):
    for ext in exts:
        if os.path.exists(_tmpname + ext):
            remove(_tmpname + ext)
        if meta:
            remove(*glob_safe(f"{_filename}.*.jpg"))


async def _cleanup_recording(exception, start=0):
    msg = str(exception) if isinstance(exception, ValueError) else repr(exception)
    msg = "Cancelled" if isinstance(exception, CancelledError) else msg
    log_suffix = (" @ [%6ss] / [%5ss]" % ("~" + str(int(time.time() - start)), str(_args.time))) if start else ""
    msg_len = 64 if log_suffix else 87
    log.error("%-17s%87s" % ("Recording FAILED", re.sub(r"\s+", " ", msg)[:msg_len] + log_suffix))

    if any((os.path.exists(x) for x in (_tmpname + TMP_EXT, _tmpname + TMP_EXT2))):
        log.debug("_cleanup_recording: cleaning only TMP files")
        _cleanup(TMP_EXT, TMP_EXT2, ".jpg", ".log", ".logo.txt", ".png", ".txt")
    else:
        log.debug("_cleanup_recording: cleaning everything")
        _cleanup(NFO_EXT, VID_EXT, meta=True)
        remove(*glob_safe(f"{_filename}.*"))
    remove(*glob_safe(os.path.join(os.path.dirname(_tmpname), f"??_show_segment{VID_EXT}")))

    if U7D_PARENT:
        path = os.path.dirname(_tmpname)
        parent = os.path.split(path)[0]
        log.debug("Removing path=%s" % path)
        remove(path)
        if parent not in (RECORDINGS, RECORDINGS_TMP):
            log.debug("Removing parent=%s" % parent)
            remove(parent)

        try:
            await _SESSION.get(f"{EPG_URL}/timers_check?delay=3")
        except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
            pass


async def _open_sessions():
    global _SESSION, _SESSION_CLOUD

    _SESSION_CLOUD = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            keepalive_timeout=YEAR_SECONDS,
            resolver=AsyncResolver(nameservers=[IPTV_DNS]) if not WIN32 else None,
        ),
        headers={"User-Agent": UA},
        json_serialize=ujson.dumps,
    )

    if _args.write_to_file:
        _SESSION = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS),
            json_serialize=ujson.dumps,
        )


async def get_vod_info(_log=False):
    params = {"action": "getRecordingData" if _args.cloud else "getCatchUpUrl"}
    params.update({"extInfoID": _args.program, "channelID": _args.channel, "mode": 1})

    msg = f"Could not get uri for [{_args.channel:4}] [{_args.program}]:"
    try:
        async with _SESSION_CLOUD.get(URL_MVTV, params=params) as r:
            res = await r.json()
        if res.get("resultData"):
            return res["resultData"]
        if _log:
            log.error('%s "%s"' % (msg, res.get("resultText", "")))
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError, TypeError) as ex:
        log.error(f'{msg} "{repr(ex)}"')


async def postprocess(archive_params, archive_url, mtime, vod_info):
    async def _check_process(msg=""):
        nonlocal proc

        process = psutil.Process(proc.pid)
        process.nice(15 if not WIN32 else psutil.IDLE_PRIORITY_CLASS)
        process.ionice(psutil.IOPRIO_CLASS_IDLE if not WIN32 else psutil.IOPRIO_VERYLOW)

        await proc.wait()

        if proc.returncode:
            stdout = await proc.stdout.read() if proc.stdout else ""
            stdout = ': "%s"' % stdout.decode().replace("\n", " ").strip() if stdout else ""

            if any((not WIN32 and proc.returncode == -9, WIN32 and proc.returncode == 15)):
                raise ValueError(msg + stdout)

            if msg + stdout:
                log.error(msg + stdout)

    async def _get_duration(recording):
        cmd = ["ffprobe", "-i", recording, "-show_entries", "format=duration", "-v", "quiet", "-of", "json"]
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=NULL)
        recording_data = ujson.loads((await proc.communicate())[0].decode())

        if "format" not in recording_data:
            return 0

        return int(float(recording_data["format"].get("duration", 0)))

    async def _save_metadata(get_cover=False):
        nonlocal duration, metadata, newest_ts

        # Only the main cover, to embed in the video file
        if get_cover:
            cache_metadata = os.path.join(CACHE_DIR, f"{_args.program}.json")
            try:
                if os.path.exists(cache_metadata):
                    async with aiofiles.open(cache_metadata, encoding="utf8") as f:
                        metadata = ujson.loads(await f.read())["data"]

                if not metadata or int(metadata["beginTime"] / 1000 + _args.start) != mtime:
                    log.debug("Getting extended info")

                    params = {"action": "epgInfov2", "productID": _args.program, "channelID": _args.channel}
                    async with _SESSION_CLOUD.get(URL_MVTV, params=params) as resp:
                        metadata = (await resp.json())["resultData"]

                    data = ujson.dumps({"data": metadata}, ensure_ascii=False, indent=4, sort_keys=True)
                    async with aiofiles.open(cache_metadata, "w", encoding="utf8") as f:
                        await f.write(data)

            except (TypeError, ValueError) as ex:
                log.warning(f"Extended info not found => {repr(ex)}")
                return None, None

            cover = metadata["cover"]
            log.debug(f'Getting cover "{cover}"')
            async with _SESSION_CLOUD.get(f"{URL_COVER}/{cover}") as resp:
                if resp.status == 200:
                    log.debug(f'Got cover "{cover}"')
                    img_ext = os.path.splitext(cover)[1]
                    img_name, archival_img_name = _tmpname + img_ext, _filename + img_ext
                    log.debug(f'Saving cover "{cover}"')
                    async with aiofiles.open(img_name, "wb") as f:
                        await f.write(await resp.read())
                    utime(mtime, img_name)
                    metadata["cover"] = archival_img_name[len(RECORDINGS) + 1 :]
                else:
                    log.warning(f'Failed to get cover "{cover}" => {resp}')
                    return None, None

            img_mime = "image/png" if img_ext == ".png" else "image/jpeg"
            return img_mime, img_name

        if not metadata:
            return

        log.debug(f"{metadata=}")
        # Save all the available metadata
        if "covers" in metadata:
            covers = {}
            metadata_dir = os.path.join(os.path.dirname(_filename), "metadata")

            if not os.path.exists(metadata_dir):
                os.mkdir(metadata_dir)

            for img in metadata["covers"]:
                cover = metadata["covers"][img]
                log.debug(f'Getting covers "{img}"')
                async with _SESSION_CLOUD.get(cover) as resp:
                    if resp.status != 200:
                        log.debug(f'Failed to get cover "{img}" => {resp}')
                        continue
                    log.debug(f'Got cover "{img}"')
                    img_ext = os.path.splitext(cover)[1]
                    img_rel = f"{os.path.basename(_filename)}-{img}" + img_ext
                    img_name = os.path.join(metadata_dir, img_rel)
                    log.debug(f'Saving cover "{img}"')
                    async with aiofiles.open(img_name, "wb") as f:
                        await f.write(await resp.read())
                    utime(mtime, img_name)
                    covers[img] = os.path.join("metadata", img_rel)

            if covers:
                metadata["covers"] = covers
                utime(newest_ts, metadata_dir)
            else:
                del metadata["covers"]
                remove(metadata_dir)

        metadata = {k: v for k, v in metadata.items() if k not in DROP_KEYS}
        metadata.update({"beginTime": mtime, "duration": duration, "endTime": mtime + duration})
        metadata.update({"expDate": int(metadata["expDate"] / 1000)})
        metadata.update({"name": os.path.basename(_args.filename)})

        xml = xmltodict.unparse({"metadata": metadata}, pretty=True)
        async with aiofiles.open(_filename + NFO_EXT, "w", encoding="utf8") as f:
            log.debug("Metadata writing xml")
            await f.write(xml)

        utime(mtime, _filename + NFO_EXT)
        log.debug("Metadata saved")

    async def _step_0():
        nonlocal archive_params

        resp = await _SESSION.options(archive_url, params=archive_params)  # signal recording ended
        if resp.status != 200:
            raise ValueError("Too short, missing [%5ss]" % str(_args.time - archive_params["recorded"]))

    async def _step_1():
        nonlocal archive_params, duration

        duration = await _get_duration(_tmpname + TMP_EXT)
        bad = duration < _args.time * 0.99

        archive_params["recorded"] = duration if bad else 0
        log_suffix = f"[{str(timedelta(seconds=duration))}s = {str(duration):>5}s] / [{str(_args.time):>5}s]"

        msg = f"POSTPROCESS #1  - Recording is {'INCOMPLETE' if bad else 'COMPLETE'}"
        msg = DIV_LOG % (msg, log_suffix)

        if bad:
            log.warning(msg)
        else:
            log.info(msg)

    async def _step_2():
        global COMSKIP
        nonlocal mtime, proc, tags

        if not RECORDINGS_TRANSCODE_OUTPUT:
            log.info("POSTPROCESS #2  - Skipped. Transcoding disabled")
            return

        cmd = ["ffmpeg"] + RECORDINGS_TRANSCODE_INPUT + ["-i", _tmpname + TMP_EXT]

        new_vod_info = await get_vod_info(_log=True)
        if not new_vod_info:
            log.warning("POSTPROCESS #2  - Could not verify event has not shifted")
        else:
            new_mtime = new_vod_info["beginTime"] // 1000 + _args.start
            if mtime != new_mtime:
                msg = DIV_LOG % ("POSTPROCESS #2  - Event CHANGED", f"beginTime=[{new_mtime - mtime:+}s]")
                if new_mtime < mtime:
                    raise ValueError(msg)
                log.info(msg)

                cmd += ["-ss", str(timedelta(seconds=new_mtime - mtime))]

        if _args.vo:
            cmd += ["-map", "0:v", "-map", "0:a:1?", "-map", "0:a:0", "-map", "0:a:3?"]
            cmd += ["-map", "0:a:2?", "-map", "0:a:5?", "-map", "0:a:4?", "-map", "0:s?"]
        else:
            cmd += ["-map", "0:v", "-map", "0:a", "-map", "0:s?"]

        cmd += RECORDINGS_TRANSCODE_OUTPUT

        if NO_SUBS:
            log.info("POSTPROCESS #2  - Dropping subs")
            cmd.append("-sn")

        img_mime, img_name = await _save_metadata(get_cover=True)
        if _args.mkv and img_mime and img_name:
            tags += ["-attach", img_name, "-metadata:s:t:0", f"mimetype={img_mime}"]

        cmd += [*tags, "-v", "error", "-y", "-f", "matroska" if _args.mkv else "mpegts", _tmpname + TMP_EXT2]

        msg = "POSTPROCESS #2  - Remuxing/Transcoding"
        if new_vod_info and mtime != new_mtime:
            msg = DIV_LOG % (msg, f"Cutting first [{new_mtime - mtime}s]")
            mtime = new_mtime

        log.info(msg)
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

        await _check_process("Failed remuxing/transcoding, leaving as is")

        if proc.returncode:
            COMSKIP = None
            _cleanup(TMP_EXT2)
        else:
            _cleanup(TMP_EXT)
            os.rename(_tmpname + TMP_EXT2, _tmpname + TMP_EXT)

    async def _step_3():
        global COMSKIP
        nonlocal proc

        cmd = ["comskip"] + COMSKIP + ["--ts", _tmpname + TMP_EXT]

        log.info("POSTPROCESS #3  - COMSKIP - Checking recording for commercials")
        async with aiofiles.open(COMSKIP_LOG, "ab") as f:
            start = time.time()
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=f, stderr=f)
            await _check_process()
            end = time.time()

        COMSKIP = None if proc.returncode else COMSKIP
        msg1 = f"POSTPROCESS #3  - COMSKIP - Commercials {'NOT found' if proc.returncode else 'found'}"
        msg2 = f"In [{str(timedelta(seconds=round(end - start)))}s]"
        msg = DIV_LOG % (msg1, msg2)
        log.warning(msg) if proc.returncode else log.info(msg)

    async def _step_4():
        nonlocal duration, mtime, proc, tags

        if COMSKIP and os.path.exists(_tmpname + CHP_EXT):
            intervals = []
            pieces = []

            async with aiofiles.open(_tmpname + CHP_EXT) as f:
                c = await f.read()

            _s = filter(lambda x: "Show Segment" in x, (" ".join(c.splitlines()).split("[CHAPTER]"))[1:])
            segments = list(_s)
            if not segments:
                log.warning("POSTPROCESS #4  - COMSKIP - Could not find any Show Segment")
                _cleanup(CHP_EXT, ".log", ".logo.txt", ".txt")
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
                    cmd = ["ffmpeg", "-i", _tmpname + TMP_EXT, "-ss", start, "-to", end]
                    cmd += ["-c", "copy", "-map", "0", *tags, "-v", "error", "-y", pieces[-1]]

                    ch = chr(idx + 64)
                    msg1, msg2 = f"POSTPROCESS #4{ch} - Cutting Chapter [{idx:02}]", f"({start} - {end})"
                    log.info(DIV_LOG % (msg1, msg2))
                    proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                    await _check_process(f"Failed Cutting Chapter [{idx:02}]")

                cmd = ["ffmpeg", "-i", f"concat:{'|'.join(pieces)}", "-c", "copy", "-map", "0"]
                cmd += [*tags, "-v", "error", "-y", "-f", "mpegts", _tmpname + TMP_EXT2]

                ch = chr(ord(ch) + 1)
                log.info(f"POSTPROCESS #4{ch} - Merging recording w/o commercials")
                proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                await _check_process("Failed merging recording w/o commercials")

            elif _args.mkv:
                cmd = ["ffmpeg", "-i", _tmpname + TMP_EXT, "-i", _tmpname + CHP_EXT, *tags]
                cmd += ["-v", "error", "-y", "-f", "matroska", _tmpname + TMP_EXT2]

                log.info("POSTPROCESS #4  - COMSKIP - Merging mkv chapters")
                proc = await asyncio.create_subprocess_exec(*cmd, stdin=NULL, stdout=PIPE, stderr=OUT)

                await _check_process("Failed merging mkv chapters")

            elif RECORDINGS_TMP:
                shutil.copy2(_tmpname + CHP_EXT, _filename + CHP_EXT)

            if proc.returncode:
                _cleanup(TMP_EXT2)
            elif os.path.exists(_tmpname + TMP_EXT2):
                _cleanup(TMP_EXT)
                os.rename(_tmpname + TMP_EXT2, _tmpname + TMP_EXT)

            if _args.comskipcut or _args.mkv:
                _cleanup(CHP_EXT)
            _cleanup(".log", ".logo.txt", ".txt")
            remove(*pieces)

    async def _step_5():
        nonlocal archive_params, duration, mtime, newest_ts

        duration = await _get_duration(_tmpname + TMP_EXT)
        length = f"[{str(timedelta(seconds=duration))}s]"
        if COMSKIP and _args.time != duration:
            cmrcls = f"{str(timedelta(seconds=_args.time - duration))}s]"
            length = f" [{str(timedelta(seconds=_args.time))}s - {cmrcls} = {length}"

        log.info(DIV_LOG % ("POSTPROCESS #5  - Archiving recording", length))
        _archive_recording()

        newest = sorted(glob_safe(f"{os.path.dirname(_filename)}/*{VID_EXT}"), key=os.path.getmtime)[-1]
        newest_ts = os.path.getmtime(newest)

        await _save_metadata()
        if _args.index:
            resp = await _SESSION.put(archive_url, params=archive_params)
            if resp.status != 200:
                raise ValueError("Failed indexing recording")

        utime(mtime, *glob_safe(f"{_filename}.*"))
        utime(newest_ts, os.path.dirname(_filename))

    await asyncio.sleep(0.1)  # Prioritize the main loop

    duration = metadata = newest_ts = proc = None
    tags = ["-metadata", 'service_name="%s"' % vod_info["channelName"]]
    tags += ["-metadata", 'service_provider="Movistar IPTV"']
    tags += ["-metadata:s:s:0", "language=esp", "-metadata:s:s:1", "language=und"]
    tags += ["-metadata:s:v", f"title={os.path.basename(_args.filename)}"] if _args.mkv else []

    lockfile = os.path.join(TMP_DIR, ".movistar_vod.lock")
    pp_lock = FileLock(lockfile)

    try:
        await _step_0()  # Verify if raw recording time is OK

        pp_lock.acquire(poll_interval=5)
        log.debug("POSTPROCESS STARTS")

        await _step_1()  # Check actual length
        await _step_2()  # Remux/Transcode

        if COMSKIP:
            await _step_3()  # Comskip analysis
            await _step_4()  # Cut/Merge chapters

        await asyncio.shield(_step_5())  # Archive recording

        log.debug("POSTPROCESS ENDED")

    except (CancelledError, ClientConnectionError, ClientOSError, ServerDisconnectedError, ValueError) as ex:
        await asyncio.shield(_cleanup_recording(ex))

    finally:
        if pp_lock.is_locked:
            pp_lock.release()


async def record_stream(vod_info):
    global _filename, _tmpname

    if not _args.filename:
        from mu7d import get_safe_filename

        _args.filename = f"{vod_info['channelName']} - {get_safe_filename(vod_info['name'])}"

    log_suffix = f": [{_args.channel:4}] [{_args.program}] [{vod_info['beginTime'] // 1000}]"
    log_suffix += f' "{_args.filename}"'

    handler = logging.StreamHandler()
    log.addHandler(handler)
    log.propagate = False
    formatter = logging.Formatter(fmt=f"{FMT[:-1]}-104s{log_suffix}", datefmt=DATEFMT)
    for handler in log.handlers:
        handler.setFormatter(formatter)

    ongoing = await ongoing_vods(filename=_args.filename)
    if len(ongoing) > 1:
        log.error("Recording already ongoing")
        return

    if not _args.time:
        _args.time = vod_info["duration"]
    else:
        if U7D_PARENT:
            _args.time = _args.time * 7 // 6 if _args.time > 900 else _args.time if _args.time > 60 else 60
        _args.time = min(_args.time, vod_info["duration"])

    archive_params = {"cloud": 1} if _args.cloud else {}
    archive_url = f"{EPG_URL}/archive/{_args.channel}/{_args.program}"
    mtime = vod_info["beginTime"] // 1000 + _args.start

    flags = "[COMSKIPCUT] " if _args.comskipcut else "[COMSKIP] " if _args.comskip else ""
    flags += "[MKV] " if _args.mkv else ""
    flags += "[VO] " if _args.vo else ""
    log_start = f"{flags}[{str(timedelta(seconds=_args.time)):>7}s = {_args.time:>5}s]"

    _filename = _tmpname = os.path.join(RECORDINGS, _args.filename)

    if RECORDINGS_TMP:
        _tmpname = os.path.join(RECORDINGS_TMP, _args.filename)

    buflen = BUFF // CHUNK
    stream = await asyncio_dgram.bind((_IPTV, _args.client_port))

    async def _buffer():
        buffer = b""
        for _ in range(buflen):
            buffer += (await stream.recv())[0]
        return buffer

    f = None
    path = os.path.dirname(_tmpname)
    try:
        if not os.path.exists(path):
            log.debug(f'Making dir "{path}"')
            os.makedirs(path)

        f = await aiofiles.open(_tmpname + TMP_EXT, "wb")
        log.info(DIV_LOG % ("Recording STARTED", log_start))
        start = time.time()

        if vod_info.get("isHdtv"):
            await f.write((await asyncio.wait_for(stream.recv(), timeout=0.2))[0])
        else:
            # 1st packet on SDTV channels is bogus and breaks ffmpeg
            await asyncio.wait_for(stream.recv(), timeout=0.2)

        while True:
            await f.write(await asyncio.wait_for(_buffer(), timeout=1.0))
            if time.time() - start >= _args.time:
                break

    except (CancelledError, asyncio_dgram.aio.TransportClosed):
        if f:
            await f.close()
        await asyncio.shield(_cleanup_recording(CancelledError(), start))
        return

    except TimeoutError:
        log.debug("TIMED OUT")

    finally:
        stream.close()

    if f:
        await f.close()
    record_time = int(time.time() - start)
    log.info(DIV_LOG % ("Recording ENDED", "[%6ss] / [%5ss]" % (f"~{record_time}", f"{_args.time}")))

    if not U7D_PARENT:
        _archive_recording()
        return

    return asyncio.create_task(postprocess(archive_params, archive_url, mtime, vod_info))


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

    try:
        if __name__ == "__main__":
            if _args.write_to_file:
                # Start recording the VOD stream
                rec_t = asyncio.create_task(record_stream(vod_info))

                if not WIN32:
                    setproctitle(getproctitle().replace("movistar_vod     ", "movistar_vod REC "))
            else:
                log.info(f'The VOD stream can be accesed at: f"udp://@{_IPTV}:{_args.client_port}"')

        # Start the RTSP keep alive loop
        while True:
            if __name__ == "__main__" and _args.write_to_file:
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
        log.debug("[%4s] [%d]: RTSP loop ended" % (str(_args.channel), _args.program))

    if __name__ == "__main__" and _args.write_to_file:
        if U7D_PARENT and not WIN32:
            setproctitle(getproctitle().replace(" REC ", "     "))

        if not rec_t.done():
            await rec_t

        pp_t = rec_t.result()
        if pp_t:
            await pp_t


async def Vod(args=None, vod_client=None):  # noqa: N802
    if __name__ == "__main__":
        global _SESSION_CLOUD

        if WIN32:
            global _loop
            _loop = asyncio.get_running_loop()

        await _open_sessions()

    else:
        global _SESSION_CLOUD, _args

        if not all((args, vod_client)):
            return

        _SESSION_CLOUD = vod_client
        _args = args

    # Get info about the requested program from Movistar. Attempt it twice.
    vod_info = await get_vod_info()
    if not vod_info:
        vod_info = await get_vod_info(_log=True)

    try:
        if vod_info:
            log.debug("[%4s] [%d]: vod_info=%s" % (str(_args.channel), _args.program, str(vod_info)))
            # Start the RTSP Session
            rtsp_t = asyncio.create_task(rtsp(vod_info))
            await rtsp_t

    finally:
        if __name__ == "__main__":
            await _SESSION_CLOUD.close()
            if _args.write_to_file:
                await _SESSION.close()


if __name__ == "__main__":
    if not WIN32:
        from setproctitle import getproctitle, setproctitle

        setproctitle("movistar_vod     %s" % " ".join(sys.argv[1:]))

        def cancel_handler(signum, frame):
            raise CancelledError

        [signal.signal(sig, cancel_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    else:
        import win32api  # pylint: disable=import-error
        import win32con  # pylint: disable=import-error

        def cancel_handler(event):
            log.debug("cancel_handler(event=%d)" % event)
            if _loop and event in (
                win32con.CTRL_BREAK_EVENT,
                win32con.CTRL_C_EVENT,
                win32con.CTRL_CLOSE_EVENT,
                win32con.CTRL_LOGOFF_EVENT,
                win32con.CTRL_SHUTDOWN_EVENT,
            ):
                for child in psutil.Process().children():
                    while child.is_running():
                        log.debug("cancel_handler(event=%d): Killing '%s'" % (event, child.name()))
                        child.kill()
                        time.sleep(0.01)

                time.sleep(0.05)
                log.debug("Cancelling tasks")
                while not all((task.done() for task in asyncio.all_tasks(_loop))):
                    [task.cancel() for task in asyncio.all_tasks(_loop)]
                    time.sleep(0.05)

        win32api.SetConsoleCtrlHandler(cancel_handler, 1)

    _SESSION = _SESSION_CLOUD = _filename = _loop = _tmpname = None

    _conf = mu7d_config()

    parser = argparse.ArgumentParser(f"Movistar U7D - VOD v{_version}")
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

    _conf["DEBUG"] = _args.debug or _conf["DEBUG"]

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=_conf["DEBUG"] and logging.DEBUG or logging.INFO)
    if _conf["LOG_TO_FILE"]:
        add_logfile(log, _conf["LOG_TO_FILE"], _conf["DEBUG"] and logging.DEBUG or logging.INFO)

    try:
        _IPTV = get_iptv_ip()
    except Exception:
        log.critical("Unable to connect to Movistar DNS")
        sys.exit(1)

    if not _args.client_port:
        _args.client_port = find_free_port(get_iptv_ip())

    if _args.write_to_file:
        if not _conf["RECORDINGS"]:
            log.error("RECORDINGS path not set")
            sys.exit(1)

        CACHE_DIR = os.path.join(_conf["HOME"], ".xmltv/cache/programs")
        COMSKIP = (_args.comskip or _args.comskipcut) and _conf["COMSKIP"]
        COMSKIP_LOG = os.path.join(_conf["HOME"], "comskip.log") if COMSKIP else None
        NO_SUBS = _conf["NO_SUBS"]
        RECORDINGS = _conf["RECORDINGS"]
        RECORDINGS_TMP = _conf["RECORDINGS_TMP"]
        RECORDINGS_TRANSCODE_INPUT = _conf["RECORDINGS_TRANSCODE_INPUT"]
        RECORDINGS_TRANSCODE_OUTPUT = _conf["RECORDINGS_TRANSCODE_OUTPUT"]
        TMP_DIR = os.getenv("TMP", os.getenv("TMPDIR", "/tmp"))  # nosec B108

        CHP_EXT = ".ffmeta"
        TMP_EXT = ".tmp"
        TMP_EXT2 = ".tmp2"
        VID_EXT = ".mkv" if _args.mkv else ".ts"

    U7D_PARENT = os.getenv("U7D_PARENT")

    try:
        asyncio.run(Vod())
    except (CancelledError, KeyboardInterrupt, RuntimeError):
        sys.exit(1)
