#!/usr/bin/env python3
#
# Started from MovistarU7D by XXLuigiMario:
# Source: https://github.com/XXLuigiMario/MovistarU7D

import aiofiles
import aiohttp
import argparse
import asyncio
import logging
import os
import psutil
import sys
import time
import ujson
import urllib.parse
import xmltodict

from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from asyncio.subprocess import DEVNULL, PIPE
from datetime import timedelta
from filelock import FileLock

if hasattr(asyncio, "exceptions"):
    from asyncio.exceptions import CancelledError
else:
    from asyncio import CancelledError

from mu7d import CHUNK, DROP_KEYS, EPG_URL, NFO_EXT, TERMINATE, UA, URL_COVER, URL_MVTV, WIN32, YEAR_SECONDS
from mu7d import IPTV_DNS, find_free_port, get_iptv_ip, glob_safe, mu7d_config, ongoing_vods, remove, utime
from mu7d import _version


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


def _cleanup(*exts, meta=False):
    for ext in exts:
        if os.path.exists(_filename + ext):
            remove(_filename + ext)
        if meta:
            remove(*glob_safe(f"{_filename}.*.jpg"))


async def _cleanup_recording(exception, start=0):
    msg = exception if isinstance(exception, ValueError) else repr(exception)
    msg = "Cancelled" if isinstance(exception, CancelledError) else msg
    log_suffix = f" [~{int(time.time() - start)}s] / [{_args.time}s]" if start else ""
    log.error(f"Recording FAILED{log_suffix} => {msg}")

    if os.path.exists(_filename + TMP_EXT):
        log.debug("_cleanup_recording: cleaning only TMP file")
        _cleanup(TMP_EXT, TMP_EXT2, ".jpg", ".png")
    else:
        log.debug("_cleanup_recording: cleaning everything")
        _cleanup(NFO_EXT, VID_EXT, meta=True)
        remove(*glob_safe(f"{_filename}.*"))

    if U7D_PARENT:
        path = os.path.dirname(_filename)
        parent = os.path.split(path)[0]
        remove(path)
        if parent != RECORDINGS:
            remove(parent)


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


async def postprocess(archive_params, archive_url, mtime, record_time):
    async def _check_process(msg):
        nonlocal proc

        try:
            process = psutil.Process(proc.pid)
            process.nice(15 if not WIN32 else psutil.IDLE_PRIORITY_CLASS)
            process.ionice(psutil.IOPRIO_CLASS_IDLE if not WIN32 else psutil.IOPRIO_VERYLOW)

            await proc.wait()

            if proc.returncode not in (0, 1):
                stdout = await proc.stdout.read() if proc.stdout else None
                raise ValueError(stdout.decode().replace("\n", " ").strip() if stdout else msg)

        except CancelledError:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            finally:
                await proc.wait()
                raise

    def _check_terminate(fn):
        from functools import wraps

        @wraps(fn)
        async def wrapper(*args, **kwargs):
            if WIN32 and os.path.exists(TERMINATE):
                raise ValueError("Terminated")
            return await fn(*args, **kwargs)

        return wrapper

    async def _save_metadata(newest_ts=None):
        nonlocal duration, metadata

        # Only the main cover, to embed in the video file
        if not newest_ts:
            cache_metadata = os.path.join(CACHE_DIR, f"{_args.program}.json")
            try:
                if os.path.exists(cache_metadata):
                    async with aiofiles.open(cache_metadata, encoding="utf8") as f:
                        metadata = ujson.loads(await f.read())["data"]

                else:
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
                    img_name = _filename + img_ext
                    log.debug(f'Saving cover "{cover}"')
                    async with aiofiles.open(img_name, "wb") as f:
                        await f.write(await resp.read())
                    metadata["cover"] = img_name[len(RECORDINGS) + 1 :]
                else:
                    log.warning(f'Failed to get cover "{cover}" => {resp}')
                    return None, None

            img_mime = "image/jpeg" if img_ext in (".jpeg", ".jpg") else "image/png"
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
        nonlocal archive_params, resp

        resp = await _SESSION.options(archive_url, params=archive_params)  # signal recording ended
        if resp.status != 200:
            raise ValueError("Too short, missing: %ds" % (_args.time - archive_params["recorded"]))

    @_check_terminate
    async def _step_1():
        nonlocal proc

        output = _filename + TMP_EXT2 if _args.mkv else "/dev/null"
        cmd = ["mkvmerge", "--abort-on-warnings", "-q", "-o", output]
        img_mime, img_name = await _save_metadata()
        if _args.mkv and img_mime and img_name:
            cmd += ["--attachment-mime-type", img_mime, "--attach-file", img_name]
        cmd += [_filename + TMP_EXT]

        log.info(f"POSTPROCESS #1: Verifying recording [~{record_time}s] / [{_args.time}s]")
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE)
        await _check_process("#1: Failed verifying recording")

        if _args.mkv:
            _cleanup(TMP_EXT)
        else:
            os.rename(_filename + TMP_EXT, _filename + TMP_EXT2)

    @_check_terminate
    async def _step_2():
        nonlocal archive_params, duration, metadata, proc, resp

        cmd = ["ffprobe", "-i", _filename + TMP_EXT2, "-show_entries", "format=duration"]
        cmd += ["-v", "quiet", "-of", "json"]

        proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL)
        recording_data = ujson.loads((await proc.communicate())[0].decode())
        duration = round(float(recording_data["format"]["duration"]))

        bad = duration < _args.time - 30

        archive_params["recorded"] = duration if bad else 0
        log_suffix = f" [{duration}s] / [{_args.time}s]"

        msg = f"POSTPROCESS #2: Recording is {'INCOMPLETE' if bad else 'COMPLETE'}{log_suffix}"
        if bad:
            log.error(msg)
            raise ValueError(msg.lstrip("POSTPROCESS "))
        log.info(msg)

        _cleanup(VID_EXT)
        utime(mtime, _filename + TMP_EXT2)
        os.rename(_filename + TMP_EXT2, _filename + VID_EXT)

    @_check_terminate
    async def _step_3():
        nonlocal proc

        cmd = ["comskip", f"--threads={COMSKIP}", "-d", "70", _filename + VID_EXT]

        log.info("POSTPROCESS #3: COMSKIP: Checking recording for commercials")
        async with aiofiles.open(COMSKIP_LOG, "ab") as f:
            start = time.time()
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=f, stderr=f)
            try:
                await _check_process("#3: COMSKIP: Failed checking recording for commercials")
            except ValueError as exception:
                log.error(exception)
            end = time.time()

        msg = (
            f"POSTPROCESS #3: COMSKIP: Took {str(timedelta(seconds=round(end - start)))}s"
            f" => [Commercials {'not found' if proc.returncode else 'found'}]"
        )
        log.warning(msg) if proc.returncode else log.info(msg)

    @_check_terminate
    async def _step_4():
        nonlocal proc

        if _args.mkv and proc.returncode == 0 and os.path.exists(_filename + CHP_EXT):
            cmd = ["mkvmerge", "-q", "-o", _filename + TMP_EXT]
            cmd += ["--chapters", _filename + CHP_EXT, _filename + VID_EXT]

            log.info("POSTPROCESS #4: COMSKIP: Merging mkv chapters")
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE)

            try:
                await _check_process("#4: COMSKIP: Failed merging mkv chapters")

                _cleanup(VID_EXT)
                utime(mtime, _filename + TMP_EXT)
                os.rename(_filename + TMP_EXT, _filename + VID_EXT)

            except ValueError as exception:
                _cleanup(TMP_EXT)
                log.error(exception)
            finally:
                _cleanup(CHP_EXT)

    async def _step_5():
        nonlocal archive_params

        newest = sorted(glob_safe(f"{os.path.dirname(_filename)}/*{VID_EXT}"), key=os.path.getmtime)[-1]
        newest_ts = os.path.getmtime(newest)

        await _save_metadata(newest_ts)
        resp = await _SESSION.put(archive_url, params=archive_params)
        if resp.status != 200:
            raise ValueError("Failed")

        _cleanup(".log", ".logo.txt")
        utime(mtime, *glob_safe(f"{_filename}.*"))
        utime(newest_ts, os.path.dirname(_filename))

    await asyncio.sleep(0.1)  # Prioritize the main loop

    duration = metadata = proc = resp = None
    archive_params["recorded"] = record_time if record_time < _args.time - 30 else 0

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_vod.lock")  # nosec B108
    pp_lock = FileLock(lockfile)

    try:
        await _step_0()  # Verify if raw recording time is OK, w/ the desired time rounded by 30s

        pp_lock.acquire(poll_interval=5)
        log.debug("POSTPROCESS STARTS")

        await _step_1()  # Verify recording
        await _step_2()  # Check actual length

        if COMSKIP:
            await _step_3()  # Comskip analysis
            await _step_4()  # Merge chapters if recording to mkv

        await asyncio.shield(_step_5())  # Archive recording

        log.debug("POSTPROCESS ENDED")

    except (CancelledError, ClientOSError, ServerDisconnectedError, ValueError) as exception:
        await asyncio.shield(_cleanup_recording(exception))

    finally:
        if pp_lock.is_locked:
            pp_lock.release()

        return proc.returncode if proc else 1


async def record_stream(vod_info):
    global _filename

    if not _args.filename:
        from mu7d import get_safe_filename

        _args.filename = f"{vod_info['channelName']} - {get_safe_filename(vod_info['name'])}"

    if not _args.time:
        _args.time = vod_info["duration"]
    else:
        _args.time = int(_args.time * 7 / 6) if _args.time > 900 else _args.time if _args.time > 60 else 60
        _args.time = min(_args.time, vod_info["duration"])

    log_suffix = " - %s [%s] [%s]" % (vod_info["channelName"], _args.channel, _args.program)
    log_suffix += ' [%d] "%s"' % (vod_info["beginTime"] / 1000, _args.filename)
    log_suffix += " [FORCED]" if _args.force else ""

    formatter = logging.Formatter(
        datefmt="%Y-%m-%d %H:%M:%S",
        fmt=f"[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s{log_suffix}",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.propagate = False

    ongoing = await ongoing_vods(filename=_args.filename)
    if ongoing and not U7D_PARENT or len(ongoing) > 1:
        log.error("Recording already ongoing")
        return 1

    _filename = os.path.join(RECORDINGS, _args.filename)

    path = os.path.dirname(_filename)
    if not os.path.exists(path):
        log.debug("Creating recording subdir")
        os.makedirs(path)

    archive_params = {"cloud": 1} if _args.cloud else {}
    archive_url = f"{EPG_URL}/archive/{_args.channel}/{_args.program}"
    mtime = int(vod_info["beginTime"] / 1000 + _args.start)

    cmd = ["ffmpeg", "-copyts", "-fifo_size", "5572", "-pkt_size", f"{CHUNK}"]
    cmd += ["-timeout", "500000", "-t", f"{_args.time}", "-vsync", "passthrough"]
    cmd += ["-fflags", "+discardcorrupt"] if _args.force else ["-xerror"]
    cmd += ["-i", _local_url]

    if _args.vo:
        cmd += ["-map", "0:v", "-map", "0:a:1?", "-map", "0:a:0", "-map", "0:a:3?"]
        cmd += ["-map", "0:a:2?", "-map", "0:a:5?", "-map", "0:a:4?", "-map", "0:s?"]
    else:
        cmd += ["-map", "0:v", "-map", "0:a", "-map", "0:s?"]

    cmd += ["-c", "copy", "-c:a:0", "aac", "-c:a:1", "aac"]

    if _args.channel in ("578", "884", "3603") or NO_SUBS:
        # dvb_teletext subs are deprecated and problematic
        # Boing, DKISS & Energy use them, so drop them
        log.warning("Recording Dropping dvb_teletext subs")
        cmd.append("-sn")

    cmd += ["-metadata:s:s:0", "language=esp", "-metadata:s:s:1", "language=und"]
    cmd += ["-metadata:s:v", f"title={os.path.basename(_args.filename)}"]
    cmd += ["-metadata", 'service_provider="Movistar IPTV"']
    cmd += ["-metadata", 'service_name="%s"' % vod_info["channelName"]]
    cmd += ["-f", "mpegts", "-v", "fatal", "-y", _filename + TMP_EXT]

    if not vod_info["channelName"].endswith(" HD") or _args.cloud:
        await asyncio.sleep(0.2)  # Needed for ffmpeg not to bail out

    proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=DEVNULL)
    log.info(f"Recording STARTED [{_args.time}s]")
    start = time.time()

    try:
        retcode = await proc.wait()
        record_time = int(time.time() - start)
        msg = f"Recording ENDED [~{record_time}s] / [{_args.time}s]"

        if retcode not in (0, 1):
            await asyncio.shield(_cleanup_recording(CancelledError(), start))
            return retcode

        if not U7D_PARENT:
            if os.path.exists(_filename + TMP_EXT):
                _cleanup(VID_EXT)
                os.rename(_filename + TMP_EXT, _filename + VID_EXT)
            log.info(msg)
            return retcode

    except CancelledError:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        finally:
            retcode = await proc.wait()
            await asyncio.shield(_cleanup_recording(CancelledError(), start))
            return retcode

    log.info(msg)
    return asyncio.create_task(postprocess(archive_params, archive_url, mtime, record_time))


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
        return client.close_connection()

    try:
        if __name__ == "__main__":
            if _args.write_to_file:
                # Start recording the VOD stream
                rec_t = asyncio.create_task(record_stream(vod_info))
            else:
                log.info(f'The VOD stream can be accesed at: "{_local_url}"')

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
        log.debug("RTSP loop ended")

        if __name__ == "__main__" and _args.write_to_file:
            if not rec_t.done():
                await rec_t

            if isinstance(rec_t.result(), asyncio.Task):
                pp_t = rec_t.result()
                await pp_t
                return pp_t.result()

            return rec_t.result()


async def Vod(args=None, vod_client=None):  # noqa: N802
    if __name__ == "__main__":
        global _SESSION_CLOUD

        await _open_sessions()
    else:
        global _SESSION_CLOUD, _args

        if not args or not vod_client:
            return

        _SESSION_CLOUD = vod_client
        _args = args

    async def _get_info():
        params = {"action": "getRecordingData" if _args.cloud else "getCatchUpUrl"}
        params.update({"extInfoID": _args.program, "channelID": _args.channel, "mode": 1})

        try:
            async with _SESSION_CLOUD.get(URL_MVTV, params=params) as r:
                return (await r.json())["resultData"]
        except (ClientOSError, KeyError, ServerDisconnectedError, TypeError) as ex:
            log.error(f"{repr(ex)}")

    # Get info about the requested program from Movistar. Attempt it twice.
    vod_info = await _get_info()
    if not vod_info:
        msg = f"Could not get uri for [{_args.channel}] [{_args.program}]"
        log.warning(msg)
        vod_info = await _get_info()
        if not vod_info:
            log.error(msg)

    try:
        if vod_info:
            # log.debug(f"[{_args.channel}] [{_args.program}]: {vod_info=}")
            # Start the RTSP Session
            rtsp_t = asyncio.create_task(rtsp(vod_info))
            await rtsp_t

    finally:
        if __name__ == "__main__":
            await _SESSION_CLOUD.close()
            if _args.write_to_file:
                await _SESSION.close()

                # Exitcode from the last recording subprocess, to discern when the proxy has to stop on WIN32
                return rtsp_t.result() if vod_info else 1


if __name__ == "__main__":
    if not WIN32:
        import signal
        from setproctitle import setproctitle

        setproctitle("movistar_vod %s" % " ".join(sys.argv[1:]))

        def cancel_handler(signum, frame):
            raise CancelledError

        [signal.signal(sig, cancel_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    _SESSION = _SESSION_CLOUD = _filename = None

    _conf = mu7d_config()

    parser = argparse.ArgumentParser(f"Movistar U7D - VOD v{_version}")
    parser.add_argument("channel", help="channel id")
    parser.add_argument("program", help="program id")

    parser.add_argument("--client_ip", "-i", help="client ip address")
    parser.add_argument("--filename", "-o", help="output bare filename, relative to RECORDINGS path")

    parser.add_argument("--client_port", "-p", help="client udp port", type=int)
    parser.add_argument("--start", "-s", help="stream start offset", type=int, default=0)
    parser.add_argument("--time", "-t", help="recording time in seconds", type=int)

    parser.add_argument("--cloud", help="the event is from a cloud recording", action="store_true")
    parser.add_argument("--debug", help="enable debug logs", action="store_true")
    parser.add_argument("--force", help="continue recording after error", action="store_true")
    parser.add_argument("--mkv", help="output recording in mkv container", action="store_true")
    parser.add_argument("--vo", help="set 2nd language as main one", action="store_true")
    parser.add_argument("--write_to_file", "-w", help="record", action="store_true")

    _args = parser.parse_args()

    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        level=logging.DEBUG if _args.debug or _conf["DEBUG"] else logging.INFO,
    )

    logging.getLogger("asyncio").setLevel(logging.DEBUG if _args.debug or _conf["DEBUG"] else logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.DEBUG if _args.debug or _conf["DEBUG"] else logging.FATAL)

    try:
        iptv = get_iptv_ip()
    except Exception:
        log.critical("Unable to connect to Movistar DNS")
        sys.exit(1)

    if not _args.client_port:
        _args.client_port = find_free_port(get_iptv_ip())

    _local_url = f"udp://@{iptv}:{_args.client_port}"

    if _args.write_to_file:
        if not _conf["RECORDINGS"]:
            log.error("RECORDINGS path not set")
            sys.exit(1)

        CACHE_DIR = os.path.join(_conf["HOME"], ".xmltv/cache/programs")
        COMSKIP = _conf["COMSKIP"]
        COMSKIP_LOG = os.path.join(_conf["HOME"], "comskip.log") if COMSKIP else None
        NO_SUBS = _conf["NO_SUBS"]
        RECORDINGS = _conf["RECORDINGS"]

        CHP_EXT = ".mkvtoolnix.chapters" if _args.mkv else None
        TMP_EXT = ".tmp"
        TMP_EXT2 = ".tmp2"
        VID_EXT = ".mkv" if _args.mkv else ".ts"

    U7D_PARENT = os.getenv("U7D_PARENT")

    try:
        result = asyncio.run(Vod())
        if result is not None:
            log.debug(f"Exiting {result}")
            sys.exit(result)
    except (CancelledError, KeyboardInterrupt):
        log.debug("Exiting 1")
        sys.exit(1)

    log.debug("Exiting 0")
