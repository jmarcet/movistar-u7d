#!/usr/bin/env python3

import aiofiles
import aiohttp
import asyncio
import logging
import os
import re
import sys
import time
import tomli
import ujson
import unicodedata
import urllib.parse
import xmltodict

from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL, PIPE
from collections import defaultdict
from datetime import date, datetime, timedelta
from filelock import FileLock, Timeout
from glob import glob
from psutil import Process, boot_time
from sanic import Sanic, response
from sanic_prometheus import monitor
from sanic.exceptions import NotFound, ServiceUnavailable
from warnings import filterwarnings

from mu7d import DIV_ONE, DIV_TWO, DROP_KEYS, EXT, IPTV_DNS, NFO_EXT
from mu7d import UA, UA_U7D, URL_MVTV, VID_EXTS, WIN32, YEAR_SECONDS
from mu7d import find_free_port, get_iptv_ip, get_local_info, get_safe_filename, get_title_meta
from mu7d import glob_safe, launch, mu7d_config, ongoing_vods, remove, utime, _version


app = Sanic("movistar_epg")

log = logging.getLogger("EPG")


@app.listener("before_server_start")
async def before_server_start(app):
    global _IPTV, _SESSION_CLOUD, _last_epg

    app.config.FALLBACK_ERROR_FORMAT = "json"
    app.config.KEEP_ALIVE_TIMEOUT = YEAR_SECONDS

    if not WIN32:
        [signal.signal(sig, cleanup_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    app.add_task(alive())

    banner = f"Movistar U7D - EPG v{_version}"
    log.info("-" * len(banner))
    log.info(banner)
    log.info("-" * len(banner))

    if IPTV_BW_SOFT:
        app.add_task(network_saturation())
        log.info(f"Ignoring RECORDINGS_THREADS => BW: {IPTV_BW_SOFT}-{IPTV_BW_HARD} kbps / {IPTV_IFACE}")

    _SESSION_CLOUD = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            keepalive_timeout=YEAR_SECONDS,
            resolver=AsyncResolver(nameservers=[IPTV_DNS]) if not WIN32 else None,
        ),
        headers={"User-Agent": UA},
        json_serialize=ujson.dumps,
    )

    await reload_epg()

    if _CHANNELS and _EPGDATA:
        if not _last_epg:
            _last_epg = int(os.path.getmtime(GUIDE))

        if RECORDINGS:
            global _RECORDINGS

            if not os.path.exists(RECORDINGS):
                os.makedirs(RECORDINGS)
                return

            oldest_epg = 9999999999
            for channel_id in _EPGDATA:
                first = next(iter(_EPGDATA[channel_id]))
                if first < oldest_epg:
                    oldest_epg = first

            await upgrade_recordings()
            upgraded_channels = await upgrade_channel_numbers()

            if not RECORDINGS_REINDEX:
                _indexed = set()
                try:
                    async with aiofiles.open(recordings, encoding="utf8") as f:
                        recordingsdata = ujson.loads(await f.read())
                    int_recordings = {}
                    for str_channel in recordingsdata:
                        channel_id = int(str_channel)
                        int_recordings[channel_id] = {}
                        for str_timestamp in recordingsdata[str_channel]:
                            timestamp = int(str_timestamp)
                            recording = recordingsdata[str_channel][str_timestamp]
                            try:
                                filename = recording["filename"]
                            except KeyError:
                                log.warning(f'Dropping old style "{recording}" from recordings.json')
                                continue
                            for old_ch, new_ch in upgraded_channels:
                                if filename.startswith(old_ch):
                                    msg = f'Updating recording index "{filename}" => '
                                    msg += f'"{filename.replace(old_ch, new_ch)}"'
                                    log.debug(msg)
                                    filename = recording["filename"] = filename.replace(old_ch, new_ch)
                            if not does_recording_exist(filename):
                                if timestamp > oldest_epg:
                                    log.warning(f'Archived recording "{filename}" not found on disk')
                                elif channel_id in _CLOUD and timestamp in _CLOUD[channel_id]:
                                    log.warning(f'Archived Cloud Recording "{filename}" not found on disk')
                                else:
                                    continue
                            else:
                                _indexed.add(get_path(filename))
                                utime(timestamp, *get_recording_files(filename))
                            int_recordings[channel_id][timestamp] = recording
                    _RECORDINGS = int_recordings
                except (TypeError, ValueError) as ex:
                    log.error(f'Failed to parse "recordings.json". It will be reset!!!: {repr(ex)}')
                    remove(CHANNELS_LOCAL, GUIDE_LOCAL)
                except FileNotFoundError:
                    remove(CHANNELS_LOCAL, GUIDE_LOCAL)

                for file in sorted(set(glob(f"{RECORDINGS}/**/*{VID_EXT}", recursive=True)) - _indexed):
                    utime(os.path.getmtime(file), *get_recording_files(os.path.splitext(file)[0]))

                await update_recordings(True)

            else:
                await reindex_recordings()

        app.add_task(update_epg_cron())

    _IPTV = get_iptv_ip()

    if not WIN32:
        [signal.signal(sig, signal.SIG_DFL) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]


@app.listener("after_server_start")
async def after_server_start(app):
    if _last_epg and RECORDINGS:
        global _t_timers

        uptime = int(datetime.now().timestamp() - boot_time())
        if not _t_timers and int(datetime.now().replace(minute=0, second=0).timestamp()) <= _last_epg:
            delay = max(10, 180 - uptime)
            if delay > 10:
                log.info(f"Waiting {delay}s to check recording timers since the system just booted...")
            _t_timers = app.add_task(timers_check(delay))
        elif not _t_timers:
            log.warning("Delaying timers_check until the EPG is updated...")


@app.listener("before_server_stop")
async def before_server_stop(app=None):
    for task in asyncio.all_tasks():
        try:
            task.cancel()
            await task
        except CancelledError:
            pass


async def alive():
    async with aiohttp.ClientSession(headers={"User-Agent": UA_U7D}) as session:
        for _ in range(10):
            try:
                await session.get("https://openwrt.marcet.info/u7d/alive")
                break
            except Exception:
                await asyncio.sleep(6)


async def cancel_app():
    await asyncio.sleep(1)
    if not WIN32:
        os.kill(U7D_PARENT, signal.SIGTERM)
    else:
        app.stop()


def check_task(task):
    if WIN32 and task.result() not in win32_normal_retcodes:
        log.debug(f"Check task: [{task}]:[{task.result()}] Exiting!!!")
        Process().terminate()


def cleanup_handler(signum, frame):
    [task.cancel() for task in asyncio.all_tasks()]
    asyncio.get_event_loop().stop()


def does_recording_exist(filename):
    return bool(
        [
            file
            for file in glob_safe(os.path.join(RECORDINGS, f"{filename}.*"))
            if os.path.splitext(file)[1] in VID_EXTS
        ]
    )


def get_channel_dir(channel_id):
    return "%03d. %s" % (_CHANNELS[channel_id]["number"], _CHANNELS[channel_id]["name"])


def get_epg(channel_id, program_id, cloud=False):
    guide = _CLOUD if cloud else _EPGDATA
    if channel_id not in guide:
        log.error(f"{channel_id=} not found")
        return None, None

    for timestamp in sorted(guide[channel_id]):
        _epg = guide[channel_id][timestamp]
        if program_id == _epg["pid"]:
            return _epg, timestamp
    log.error(f"{channel_id=} {program_id=} not found")


def get_path(filename, bare=False):
    return os.path.join(RECORDINGS, filename + (VID_EXT if not bare else ""))


def get_program_id(channel_id, url=None, cloud=False, local=False):
    if channel_id not in _CHANNELS:
        return

    if not url:
        start = int(time.time())
    elif len(url) == 10:
        start = int(url)
    elif not url.isdigit():
        start = int(flussonic_regex.match(url).groups()[0])
    elif len(url) == 12:
        start = int(datetime.strptime(url, "%Y%m%d%H%M").timestamp())
    elif len(url) == 14:
        start = int(datetime.strptime(url, "%Y%m%d%H%M%S").timestamp())
    else:
        return

    channel = _CHANNELS[channel_id]["name"]
    last_timestamp = new_start = 0

    if not cloud and not local:
        if start not in _EPGDATA[channel_id]:
            for timestamp in _EPGDATA[channel_id]:
                if timestamp > start:
                    break
                last_timestamp = timestamp
            if not last_timestamp:
                return
            start, new_start = last_timestamp, start
        program_id, duration = [_EPGDATA[channel_id][start][t] for t in ["pid", "duration"]]

    elif cloud and local:
        return

    else:
        guide = _CLOUD if cloud else _RECORDINGS if local else {}
        if channel_id not in guide:
            return
        if start in guide[channel_id]:
            duration = guide[channel_id][start]["duration"]
        else:
            for timestamp in reversed(guide[channel_id]):
                duration = guide[channel_id][timestamp]["duration"]
                if timestamp < start <= timestamp + duration:
                    start, new_start = timestamp, start
                    break
            if not new_start:
                return
        if cloud:
            program_id = _CLOUD[channel_id][start]["pid"]
        else:
            program_id = get_path(_RECORDINGS[channel_id][start]["filename"])

    return {
        "channel": channel,
        "program_id": program_id,
        "start": start,
        "duration": duration,
        "offset": new_start - start if new_start else 0,
    }


def get_program_name(filename):
    return os.path.split(os.path.dirname(filename))[1]


def get_recording_files(fname):
    fname = fname.removesuffix(VID_EXT)
    absname = os.path.join(RECORDINGS, fname)
    basename = os.path.basename(fname)
    path = os.path.dirname(absname)

    metadata = os.path.join(path, "metadata")
    nfo = os.path.join(path, basename + NFO_EXT)

    files = glob_safe(os.path.join(path, f"{basename}.*"))
    files += glob_safe(os.path.join(metadata, f"{basename}-*"))
    files += [nfo] if os.path.exists(nfo) else []
    files += [metadata] if os.path.exists(metadata) else []
    files += [path] if path != RECORDINGS else []

    return filter(lambda file: os.access(file, os.W_OK), files)


def get_recording_name(channel_id, timestamp, cloud=False):
    guide = _CLOUD if cloud else _EPGDATA
    daily_program = not guide[channel_id][timestamp]["is_serie"] and guide[channel_id][timestamp][
        "genre"
    ].startswith("0")

    if RECORDINGS_PER_CHANNEL:
        path = os.path.join(RECORDINGS, get_channel_dir(channel_id))
    else:
        path = RECORDINGS
    if guide[channel_id][timestamp]["serie"]:
        path = os.path.join(path, get_safe_filename(guide[channel_id][timestamp]["serie"]))
    elif daily_program:
        path = os.path.join(path, get_safe_filename(guide[channel_id][timestamp]["full_title"]))
    path = path.rstrip(".").rstrip(",")

    filename = os.path.join(path, get_safe_filename(guide[channel_id][timestamp]["full_title"]))
    if daily_program:
        _time = "%Y%m%d" + ("_%H%M" if guide[channel_id][timestamp]["genre"] == "0C" else "")
        filename += f" - {datetime.fromtimestamp(timestamp).strftime(_time)}"

    return filename[len(RECORDINGS) + 1 :]


def get_siblings(channel_id, filename):
    return [
        ts
        for ts in sorted(_RECORDINGS[channel_id])
        if os.path.split(_RECORDINGS[channel_id][ts]["filename"])[0] == os.path.split(filename)[0]
    ]


@app.route("/archive/<channel_id:int>/<program_id:int>", methods=["OPTIONS", "PUT"])
async def handle_archive(request, channel_id, program_id, cloud=False):
    global _t_timers

    cloud = request.args.get("cloud") == "1" if request else cloud

    _, timestamp = get_epg(channel_id, program_id, cloud)
    if not timestamp:
        raise NotFound(f"Requested URL {request.raw_url.decode()} not found")

    filename = get_recording_name(channel_id, timestamp, cloud)

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    log_suffix = f': [{channel_id:4}] [{program_id}] [{timestamp}] "{filename}"'

    if not timers_lock.locked() and (not _t_timers or _t_timers.done()):
        _t_timers = app.add_task(timers_check(delay=5))

    recorded = int(request.args.get("recorded", 0))
    if recorded:
        if recorded < 45:
            log.error(DIV_ONE % ("Recording WRONG", log_suffix))
            return response.json({"status": "Recording WRONG"}, status=202)

        async with recordings_inc_lock:
            if channel_id not in _RECORDINGS_INC:
                _RECORDINGS_INC[channel_id] = {}
            if program_id not in _RECORDINGS_INC[channel_id]:
                _RECORDINGS_INC[channel_id][program_id] = []
            _RECORDINGS_INC[channel_id][program_id].append(recorded)
            _t = _RECORDINGS_INC[channel_id][program_id]

        if len(_t) < 2 or sorted(_t)[-1] - sorted(_t)[-2] >= 3:
            log.debug(DIV_TWO % ("Recording INCOMPLETE", "[%5ss]" % str(_t[-1]), log_suffix))
            return response.json({"status": "Recording INCOMPLETE"}, status=202)

    if request.method == "OPTIONS":
        msg = f"Recording {'INCOMPLETE ' if recorded else ''}OK"
        log.debug(DIV_ONE % (msg, log_suffix))
        return response.json({"status": msg}, status=200)

    log.debug(f"Checking for {filename}")
    if does_recording_exist(filename):
        errors = ""
        async with recordings_lock:
            if channel_id not in _RECORDINGS:
                _RECORDINGS[channel_id] = {}

            prune_duplicates(channel_id, filename)
            if channel_id in _KEEP:
                prune_expired(channel_id, filename)

            nfo = await get_local_info(channel_id, timestamp, get_path(filename, bare=True))
            if timestamp == nfo["beginTime"]:
                _RECORDINGS[channel_id][timestamp] = {"duration": nfo["duration"], "filename": filename}
            else:
                errors = ", event changed -> aborted"

        if not errors:
            async with recordings_inc_lock:
                if channel_id in _RECORDINGS_INC and program_id in _RECORDINGS_INC[channel_id]:
                    del _RECORDINGS_INC[channel_id][program_id]

            app.add_task(update_recordings(channel_id))

            msg = DIV_ONE % ("Recording ARCHIVED" + errors, log_suffix)

            log.info(msg)
            return response.json({"status": msg}, ensure_ascii=False)

    msg = DIV_ONE % ("Recording NOT ARCHIVED" + errors, log_suffix)
    log.error(msg)
    return response.json({"status": msg}, ensure_ascii=False, status=203)


@app.get("/channels/")
async def handle_channels(request):
    return response.json(_CHANNELS) if _CHANNELS else response.empty(404)


@app.get("/program_id/<channel_id:int>/<url>")
async def handle_program_id(request, channel_id, url):
    try:
        cloud, local = request.args.get("cloud", "") == "1", request.args.get("local", "") == "1"
        return response.json(get_program_id(channel_id, url, cloud, local))
    except (AttributeError, KeyError):
        raise NotFound(f"Requested URL {request.raw_url.decode()} not found")


@app.post("/prom_event/<method>")
async def handle_prom_event(request, method):
    try:
        return response.empty(200)
    finally:
        await prom_event(request, method)


@app.get("/record/<channel_id:int>/<url>")
async def handle_record_program(request, channel_id, url):
    cloud = request.args.get("cloud") == "1"
    comskip = int(request.args.get("comskip", "0"))
    index = request.args.get("index", "1") == "1"
    mkv = request.args.get("mkv") == "1"
    record_time = int(request.args.get("time", "0"))
    vo = request.args.get("vo") == "1"

    if comskip not in (0, 1, 2):
        raise NotFound(f"Requested URL {request.raw_url.decode()} not found")

    try:
        channel, program_id, start, duration, offset = get_program_id(channel_id, url, cloud).values()
    except AttributeError:
        raise NotFound(f"Requested URL {request.raw_url.decode()} not found")

    if not record_time:
        record_time = duration - offset

    msg = await record_program(channel_id, program_id, offset, record_time, cloud, comskip, index, mkv, vo)
    if msg:
        raise ServiceUnavailable(msg)

    return response.json(
        {
            "status": "OK",
            "channel": channel,
            "channel_id": channel_id,
            "program_id": program_id,
            "start": start,
            "offset": offset,
            "time": record_time,
            "cloud": cloud,
            "comskip": comskip,
            "index": index,
            "mkv": mkv,
            "vo": vo,
        }
    )


@app.get("/reload_epg")
async def handle_reload_epg(request):
    app.add_task(reload_epg())
    return response.json({"status": "EPG Reload Queued"}, 200)


@app.get("/timers_check")
async def handle_timers_check(request):
    global _t_timers

    delay = int(request.args.get("delay", 0))

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    if _NETWORK_SATURATION:
        return response.json({"status": await log_network_saturated()}, 404)

    if timers_lock.locked() or (_t_timers and not _t_timers.done()):
        raise ServiceUnavailable("Already processing timers")

    _t_timers = app.add_task(timers_check(delay=delay))

    return response.json({"status": "Timers check queued"}, 200)


@app.get("/update_epg")
async def handle_update_epg(request):
    app.add_task(update_epg())
    return response.json({"status": "EPG Update Queued"}, 200)


async def kill_vod():
    vods = await ongoing_vods()
    if not vods:
        return

    proc = vods[-1]
    proc.terminate()
    r = re.search(r"_vod.+  ?(\d+) (\d+) -b (\d+) -p .+ -o (.+) -t .+$", " ".join(proc.cmdline()).strip())
    if r and r.groups():
        channel_id, pid, timestamp, filename = r.groups()
        log_suffix = f': [{channel_id:4}] [{pid}] [{timestamp}] "{filename}"'
    log.warning(DIV_ONE % ("Recording KILLED", log_suffix if r and r.groups() else f": [{proc.pid}]"))


async def log_network_saturated(nr_procs=None, _wait=False):
    global _last_bw_warning
    level = _NETWORK_SATURATION
    msg = ""
    if level:
        msg = f"Network {'Hard' if level == 2 else 'Soft' if level == 1 else ''} Saturated. "
    if not nr_procs:
        if _wait:  # when called by network_saturation(), we wait so the recordings count is always correct
            await asyncio.sleep(1)
        nr_procs = len(await ongoing_vods())
    msg += f"Recording {nr_procs} streams." if nr_procs else ""
    if not _last_bw_warning or (time.time() - _last_bw_warning[0] > 5 or _last_bw_warning[1] != msg):
        _last_bw_warning = (time.time(), msg)
        log.warning(msg)
    return msg


async def network_bw_control():
    async with network_bw_lock:
        await log_network_saturated()
        await kill_vod()
        await asyncio.sleep(2)


async def network_saturation():
    global _NETWORK_SATURATION
    iface_rx = f"/sys/class/net/{IPTV_IFACE}/statistics/rx_bytes"
    before = last = 0

    while True:
        if not os.path.exists(iface_rx):
            last = 0
            log.error(f"{IPTV_IFACE} NOT ACCESSIBLE")
            while not os.path.exists(iface_rx):
                await asyncio.sleep(1)
            log.info(f"{IPTV_IFACE} IS now ACCESSIBLE")

        async with aiofiles.open(iface_rx) as f:
            now, cur = time.time(), int((await f.read())[:-1])

        if last:
            tp = (cur - last) * 0.008 / (now - before)
            _NETWORK_SATURATION = 2 if tp > IPTV_BW_HARD else 1 if (IPTV_BW_SOFT and tp > IPTV_BW_SOFT) else 0

            if _NETWORK_SATURATION:
                if _t_timers and not _t_timers.done():
                    _t_timers.cancel()
                    app.add_task(log_network_saturated(_wait=True))
                if _NETWORK_SATURATION == 2 and not network_bw_lock.locked():
                    app.add_task(network_bw_control())

        before, last = now, cur
        await asyncio.sleep(1)


async def prom_event(request, method):
    cloud = request.json.get("cloud", False)
    local = not request.json["lat"]

    _event = get_program_id(request.json["channel_id"], request.json.get("url"), cloud, local)
    if not local:
        _epg, _ = get_epg(request.json["channel_id"], _event["program_id"], cloud)
    else:
        path = _event["program_id"].removesuffix(VID_EXT)
        _epg = await get_local_info(request.json["channel_id"], _event["start"], path, extended=True)
    if not _epg:
        return

    if method == "add":
        offset = "[%d/%d]" % (_event["offset"], _event["duration"])

        request.app.ctx.metrics["RQS_LATENCY"].labels(
            request.json["method"],
            request.json["endpoint"] + _epg["full_title"] + f" _ {offset}",
            request.json["id"],
        ).observe(float(request.json["lat"]))

    else:
        start, end = _event["offset"], _event["offset"] + request.json["offset"]
        offset = "[%d-%d/%d]" % (start, end, _event["duration"])

        for _metric in request.app.ctx.metrics["RQS_LATENCY"]._metrics:
            if request.json["method"] in _metric and str(request.json["id"]) in _metric:
                request.app.ctx.metrics["RQS_LATENCY"].remove(*_metric)
                break

        if local:
            return

    msg = f'{request.json["msg"]} [{request.json["channel_id"]:4}] '
    msg += f'[{_event["program_id"]}] ' if not local else "[00000000] "
    msg += f'[{_event["start"]}] [{_event["channel"]}] "{_epg["full_title"]}" _ {offset}'

    log.info(msg)


def prune_duplicates(channel_id, filename):
    found = [ts for ts in _RECORDINGS[channel_id] if _RECORDINGS[channel_id][ts]["filename"] in filename]
    for ts in found:
        duplicate = _RECORDINGS[channel_id][ts]["filename"]
        old_files = sorted(set(get_recording_files(duplicate)) - set(get_recording_files(filename)))

        remove(*old_files)
        [log.warning(f'REMOVED DUPLICATED "{file}"') for file in old_files]

        del _RECORDINGS[channel_id][ts]


def prune_expired(channel_id, filename):
    program = get_program_name(filename)
    if program not in _KEEP[channel_id]:
        return

    siblings = get_siblings(channel_id, filename)
    if not siblings:
        return

    def _drop(ts):
        filename = _RECORDINGS[channel_id][ts]["filename"]
        old_files = sorted(get_recording_files(filename))
        remove(*old_files)
        del _RECORDINGS[channel_id][ts]
        [log.warning(f'REMOVED "{file}"') for file in old_files if not os.path.exists(file)]

    if _KEEP[channel_id][program] < 0:
        max_days = abs(_KEEP[channel_id][program])
        newest = date.fromtimestamp(max(siblings[-1], os.path.getmtime(get_path(filename))))
        [_drop(ts) for ts in siblings if (newest - date.fromtimestamp(ts)).days > max_days]

    elif len(siblings) >= _KEEP[channel_id][program]:
        [_drop(ts) for ts in siblings[0 : len(siblings) - _KEEP[channel_id][program] + 1]]


async def reap_vod_child(process, filename):
    retcode = await process.wait()

    if WIN32 and retcode not in win32_normal_retcodes:
        log.debug(f"Reap VOD Child: [{process}]:[{retcode}] Exiting!!!")
        Process().terminate()

    if retcode == -9 or WIN32 and retcode:
        ongoing = await ongoing_vods(filename=filename)
        if ongoing:
            log.debug('Reap VOD Child: Killing child: "%s"' % " ".join(ongoing[0].cmdline()))
            ongoing[0].terminate()

    global _t_timers
    if not timers_lock.locked() and (not _t_timers or _t_timers.done()):
        _t_timers = app.add_task(timers_check(delay=5))

    log.debug(f"Reap VOD Child: [{process}]:[{retcode}] DONE")


async def record_program(
    channel_id, pid, offset=0, time=0, cloud=False, comskip=0, index=True, mkv=False, vo=False
):
    _, timestamp = get_epg(channel_id, pid, cloud)
    if not timestamp:
        return "Event not found"

    filename = get_recording_name(channel_id, timestamp, cloud)

    ongoing = await ongoing_vods(filename=filename, _all=True)
    if ongoing:
        log_suffix = f': [{channel_id:4}] [{pid}] [{timestamp}] "{filename}"'
        if f"{channel_id} {pid} -b {timestamp} " in ongoing or f"{channel_id} {pid} -b " not in ongoing:
            if f"{channel_id} {pid} -b " in ongoing:
                msg = DIV_ONE % ("Recording ONGOING", log_suffix)
            else:
                msg = DIV_ONE % ("Found REPEATED EVENT", log_suffix)
            log.warning(msg)
            return re.sub(r"\s+:", ":", msg)

        r = re.search(f" {channel_id} {pid} -b " + r"(\d+) -p", ongoing)
        prev_ts = int(r.groups()[0])
        begin_time = f"beginTime=[{timestamp - prev_ts:+}s]"
        if timestamp < prev_ts:
            log.warning(DIV_TWO % ("Event CHANGED => KILL", begin_time, log_suffix))
            ongoing = await ongoing_vods(channel_id, pid, filename)
            ongoing[0].terminate()
            await asyncio.sleep(5)
        elif timestamp > prev_ts:
            msg = DIV_TWO % ("Event DELAYED", begin_time, log_suffix)
            log.debug(msg)
            return re.sub(r"\s+:", ":", msg)

    if _NETWORK_SATURATION:
        return await log_network_saturated()

    port = find_free_port(_IPTV)
    cmd = [sys.executable] if EXT == ".py" else []
    cmd += [f"movistar_vod{EXT}", f"{channel_id:4}", str(pid), "-b", str(timestamp), "-p", f"{port:5}", "-w"]
    cmd += ["-o", filename]
    cmd += ["-s", str(offset)] if offset else []
    cmd += ["-t", str(time)] if time else []
    cmd += ["--cloud"] if cloud else []
    cmd += ["--comskip"] if comskip == 1 else ["--comskipcut"] if comskip == 2 else []
    cmd += ["--index"] if index else []
    cmd += ["--mkv"] if mkv else []
    cmd += ["--vo"] if vo else []

    log.debug('Launching: "%s"' % " ".join(cmd))
    process = await asyncio.create_subprocess_exec(*cmd)
    app.add_task(reap_vod_child(process, filename))


async def reindex_recordings():
    log.warning("REINDEXING RECORDINGS")
    _recordings = defaultdict(dict)

    for nfo_file in sorted(glob(f"{RECORDINGS}/**/*{NFO_EXT}", recursive=True)):
        basename = nfo_file[:-13]
        if not any((os.path.exists(basename + ext) for ext in VID_EXTS)):
            log.error(f"No recording found: {basename=} {nfo_file=}")
            # remove(*get_recording_files(basename))
            continue

        try:
            async with aiofiles.open(nfo_file, encoding="utf-8") as f:
                xml = await f.read()

            nfo = xmltodict.parse(xml)["metadata"]
        except Exception as ex:
            log.error(f"Cannot read {nfo_file=} => {repr(ex)}")
            # remove(*get_recording_files(basename))
            continue

        channel_id, duration, timestamp = map(int, (nfo["serviceUID"], nfo["duration"], nfo["beginTime"]))
        filename = nfo["cover"][:-4]
        _recordings[channel_id][timestamp] = {"duration": duration, "filename": filename}

    if len(_recordings):
        global _RECORDINGS
        async with recordings_lock:
            _RECORDINGS = _recordings

        await update_recordings(True)


async def reload_epg():
    global _CHANNELS, _EPGDATA

    if not os.path.exists(CHANNELS) and all(
        map(os.path.exists, (config_data, epg_data, epg_metadata, GUIDE, GUIDE + ".gz"))
    ):
        log.warning("Missing channel list! Need to download it. Please be patient...")
        cmd = [f"movistar_tvg{EXT}", "--m3u", CHANNELS]
        async with tvgrab_lock:
            task = app.add_task(launch(cmd))
            await task
        check_task(task)
        if not os.path.exists(CHANNELS):
            return app.add_task(cancel_app())
    elif not all(map(os.path.exists, (CHANNELS, config_data, epg_data, epg_metadata, GUIDE, GUIDE + ".gz"))):
        log.warning("Missing channels data! Need to download it. Please be patient...")
        return await update_epg()

    async with epg_lock:
        int_channels, int_epgdata, services = {}, {}, {}

        try:
            async with aiofiles.open(epg_metadata, encoding="utf8") as f:
                metadata = ujson.loads(await f.read())["data"]

            async with aiofiles.open(config_data, encoding="utf8") as f:
                packages = ujson.loads(await f.read())["data"]["tvPackages"]

            for package in packages.split("|") if packages != "ALL" else metadata["packages"]:
                services = {**services, **metadata["packages"][package]["services"]}

            channels = metadata["channels"]
            for channel in [chan for chan in channels if chan in services]:
                int_channels[int(channel)] = channels[channel]
                int_channels[int(channel)]["id"] = int(channels[channel]["id"])
                int_channels[int(channel)]["number"] = int(services[channel])
                if "replacement" in channels[channel]:
                    int_channels[int(channel)]["replacement"] = int(channels[channel]["replacement"])
            _CHANNELS = int_channels

        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f"Failed to load Channels metadata {repr(ex)}")
            remove(epg_metadata)
            return await reload_epg()

        try:
            async with aiofiles.open(epg_data, encoding="utf8") as f:
                epgdata = ujson.loads(await f.read())["data"]

            for channel in epgdata:
                int_epgdata[int(channel)] = {}
                for timestamp in epgdata[channel]:
                    int_epgdata[int(channel)][int(timestamp)] = epgdata[channel][timestamp]
            _EPGDATA = int_epgdata

        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f"Failed to load EPG data {repr(ex)}")
            remove(epg_data)
            return await reload_epg()

        log.info(f"Channels  &  EPG Updated => {U7D_URL}/MovistarTV.m3u      - {U7D_URL}/guide.xml.gz")
        nr_epg = sum((len(_EPGDATA[channel]) for channel in _EPGDATA))
        log.info(f"Total: {len(_EPGDATA):2} Channels & {nr_epg:5} EPG entries")

        await update_cloud()


async def timers_check(delay=0):
    await asyncio.sleep(delay)

    def _clean(string):
        return unicodedata.normalize("NFKD", string).encode("ASCII", "ignore").decode("utf8")

    def _exit():
        global _KEEP, _t_timers_next
        nonlocal kept, next_timers
        _KEEP = kept
        if next_timers:
            title, ts = sorted(next_timers, key=lambda x: x[1])[0]
            if not _t_timers_next or _t_timers_next.get_name() != f"{ts}":
                log.info(f'Adding timers_check() @ {datetime.fromtimestamp(ts + 30)} "{title}"')
                delay = ts + 30 - datetime.now().timestamp()
                _t_timers_next = app.add_task(timers_check(delay=delay), name=f"{ts}")

    def _filter_recorded(timestamps):
        nonlocal channel_id, recs
        return filter(lambda ts: channel_id not in recs or ts not in recs[channel_id], timestamps)

    async def _record(cloud=False):
        nonlocal channel_id, comskip, keep, kept, nr_procs, ongoing
        nonlocal queued, recs, repeat, timer_match, timestamp, vo

        filename = get_recording_name(channel_id, timestamp, cloud)
        program = get_program_name(filename)

        if keep and re.search(_clean(timer_match), _clean(program), re.IGNORECASE):
            kept[channel_id][program] = keep
            if keep < 0:
                if (datetime.fromtimestamp(timestamp) - datetime.now()).days < keep:
                    log.debug('Older than     %d days: SKIPPING [%d] "%s"' % (abs(keep), timestamp, filename))
                    return
            else:
                current = list(filter(lambda x: program in x, (await ongoing_vods(_all=True)).split("|")))
                if len(current) == keep:
                    log.debug('Recording %02d programs: SKIPPING [%d] "%s"' % (keep, timestamp, filename))
                    return
                siblings = get_siblings(channel_id, filename)
                if len(current) + len(siblings) == keep and timestamp < siblings[0]:
                    log.debug('Recorded  %02d programs: SKIPPING [%d] "%s"' % (keep, timestamp, filename))
                    return

        if (channel_id, timestamp) in queued:
            return

        guide = _CLOUD if cloud else _EPGDATA
        duration, pid, title = [guide[channel_id][timestamp][t] for t in ["duration", "pid", "full_title"]]

        if filename in ongoing:
            vod = await ongoing_vods(filename=filename, _all=True)
            if f"{channel_id} {pid} -b {timestamp} " in vod or f"{channel_id} {pid} -b " not in vod:
                return

        if not cloud and timestamp > _last_epg + 3600:
            return

        if channel_id in recs:
            r = list(filter(lambda event: recs[channel_id][event]["filename"] == filename, recs[channel_id]))
            # only repeat a recording when title changed
            if r and not len(filename) > len(recs[channel_id][r[0]]["filename"]):
                # or asked to and new event is more recent than the archived
                if not repeat or timestamp <= r[0]:
                    return

        if cloud or re.search(_clean(timer_match), _clean(title), re.IGNORECASE):
            _time = 0 if cloud else duration
            now = round(datetime.now().timestamp())
            if timestamp + 30 > now:
                next_timers.append((title, timestamp))
                return
            log_suffix = f': [{channel_id:4}] [{pid}] [{timestamp}] "{filename}"'
            if 0 < _time < 300:
                log.debug(DIV_TWO % ("Skipping MATCH", f"Too short [{_time}s]", log_suffix))
                return
            if await record_program(channel_id, pid, 0, _time, cloud, comskip, True, MKV_OUTPUT, vo):
                return

            log.info(DIV_ONE % (f"Found {'Cloud ' if cloud else ''}EPG MATCH", log_suffix))

            queued.append((channel_id, timestamp))
            await asyncio.sleep(2.5 if not WIN32 else 4)

            nr_procs += 1
            if nr_procs >= RECORDINGS_THREADS:
                await log_network_saturated(nr_procs)
                return -1

    async with timers_lock:
        if not os.path.exists(timers):
            log.debug("timers_check: no timers.conf found")
            return

        if epg_lock.locked():  # If EPG is being updated, wait until it is done
            await epg_lock.acquire()
            epg_lock.release()

        log.debug("Processing timers")
        try:
            async with aiofiles.open(timers, encoding="utf8") as f:
                try:
                    _timers = tomli.loads(await f.read())
                except ValueError:
                    _timers = ujson.loads(await f.read())
        except (TypeError, ValueError) as ex:
            log.error(f"Failed to parse timers.conf: {repr(ex)}")
            return

        deflang = _timers.get("default_language", "")
        sync_cloud = _timers.get("sync_cloud", False)

        nr_procs = len(await ongoing_vods())
        if _NETWORK_SATURATION or nr_procs >= RECORDINGS_THREADS:
            await log_network_saturated(nr_procs)
            return

        async with recordings_lock:
            recs = _RECORDINGS.copy()

        ongoing = await ongoing_vods(_all=True)  # we want to check against all ongoing vods, also in pp
        log.debug(f"Ongoing VODs: [{ongoing}]")

        kept, next_timers, queued = defaultdict(dict), [], []

        for str_channel_id in _timers.get("match", {}):
            channel_id = int(str_channel_id)
            if channel_id not in _EPGDATA:
                log.warning(f"Channel [{channel_id}] not found in EPG")
                continue
            channel_name = _CHANNELS[channel_id]["name"]

            for timer_match in _timers["match"][str_channel_id]:
                comskip = 2 if _timers.get("comskipcut") else 1 if _timers.get("comskip") else 0
                days = fixed_timer = keep = repeat = None
                lang = deflang
                if " ## " in timer_match:
                    match_split = timer_match.split(" ## ")
                    timer_match = match_split[0]
                    for res in [res.lower().strip() for res in match_split[1:]]:
                        if res[0].isnumeric():
                            try:
                                hour, minute = map(int, res.split(":"))
                                _ts = datetime.now().replace(hour=hour, minute=minute, second=0).timestamp()
                                fixed_timer = int(_ts)
                                log.debug(
                                    '[%s] "%s" %02d:%02d %d'
                                    % (channel_name, timer_match, hour, minute, fixed_timer)
                                )
                            except ValueError:
                                log.warning(
                                    f'Failed to parse [{channel_name}] "{timer_match}" [{res}] correctly'
                                )

                        elif res[0] == "@":
                            days = dict(map(lambda x: (x[0], x[1] == "x"), enumerate(res[1:8], start=1)))

                        elif res == "comskip":
                            comskip = 1

                        elif res == "comskipcut":
                            comskip = 2

                        elif res.startswith("keep"):
                            keep = int(res.lstrip("keep").rstrip("d"))
                            keep = -keep if res.endswith("d") else keep

                        elif res == "nocomskip":
                            comskip = 0

                        elif res == "repeat":
                            repeat = True

                        else:
                            lang = res

                vo = lang == "VO"
                tss = filter(lambda ts: ts <= _last_epg + 3600, reversed(_EPGDATA[channel_id]))

                if fixed_timer:
                    # fixed timers are checked daily, so we want today's and all of last week
                    fixed_timestamps = [fixed_timer] if fixed_timer <= _last_epg + 3600 else []
                    fixed_timestamps += [fixed_timer - i * 24 * 3600 for i in range(1, 8)]

                    found_ts = []
                    for ts in tss:
                        for fixed_ts in fixed_timestamps:
                            if abs(ts - fixed_ts) <= 1500:
                                if not days or days.get(datetime.fromtimestamp(ts).isoweekday()):
                                    found_ts.append(ts)
                                break
                    tss = found_ts

                elif days:
                    tss = filter(lambda ts: days.get(datetime.fromtimestamp(ts).isoweekday()), tss)

                log.debug("Checking timer: [%s] [%d] [%s]" % (channel_name, channel_id, _clean(timer_match)))
                for timestamp in _filter_recorded(tss):
                    if await _record():
                        return _exit()

        if sync_cloud:
            vo = _timers.get("sync_cloud_language", "") == "VO"
            for channel_id in _CLOUD:
                channel_name = _CHANNELS[channel_id]["name"]
                for timestamp in _filter_recorded(sorted(_CLOUD[channel_id])):
                    if await _record(cloud=True):
                        return _exit()
        _exit()


async def update_cloud():
    global _CLOUD

    try:
        async with aiofiles.open(cloud_data, encoding="utf8") as f:
            clouddata = ujson.loads(await f.read())["data"]

        int_clouddata = defaultdict(dict)
        for channel in clouddata:
            for timestamp in clouddata[channel]:
                int_clouddata[int(channel)][int(timestamp)] = clouddata[channel][timestamp]
        _CLOUD = int_clouddata

    except (FileNotFoundError, TypeError, ValueError):
        remove(cloud_data)

    try:
        params = {"action": "recordingList", "mode": 0, "state": 2, "firstItem": 0, "numItems": 999}
        async with _SESSION_CLOUD.get(URL_MVTV, params=params) as r:
            cloud_recordings = (await r.json())["resultData"]["result"]

    except (ClientOSError, KeyError, ServerDisconnectedError):
        cloud_recordings = None

    if not cloud_recordings:
        log.info("No cloud recordings found")
        return

    def _fill_cloud_event():
        nonlocal data, meta, pid, timestamp, year

        return {
            "age_rating": data["ageRatingID"],
            "duration": data["duration"],
            "end": int(str(data["endTime"])[:-3]),
            "episode": meta["episode"] or data.get("episode"),
            "full_title": meta["full_title"],
            "genre": data["theme"],
            "is_serie": meta["is_serie"],
            "pid": pid,
            "season": meta["season"] or data.get("season"),
            "start": int(timestamp),
            "year": year,
            "serie": meta["serie"] or data.get("seriesName"),
            "serie_id": data.get("seriesID"),
        }

    new_cloud = defaultdict(dict)
    for _event in cloud_recordings:
        channel_id = _event["serviceUID"]
        timestamp = int(_event["beginTime"] / 1000)

        if timestamp not in new_cloud[channel_id]:
            if channel_id in _EPGDATA and timestamp in _EPGDATA[channel_id]:
                new_cloud[channel_id][timestamp] = _EPGDATA[channel_id][timestamp]

            elif channel_id in _CLOUD and timestamp in _CLOUD[channel_id]:
                new_cloud[channel_id][timestamp] = _CLOUD[channel_id][timestamp]

            else:
                pid = _event["productID"]

                params = {"action": "epgInfov2", "productID": pid, "channelID": channel_id}
                async with _SESSION_CLOUD.get(URL_MVTV, params=params) as r:
                    _data = (await r.json())["resultData"]
                    year = _data.get("productionDate")

                params = {"action": "getRecordingData", "extInfoID": pid, "channelID": channel_id, "mode": 1}
                async with _SESSION_CLOUD.get(URL_MVTV, params=params) as r:
                    data = (await r.json())["resultData"]

                if not data:  # There can be events with no data sometimes
                    continue

                meta = get_title_meta(data["name"], data.get("seriesID"))
                new_cloud[channel_id][timestamp] = _fill_cloud_event()
                if meta["episode_title"]:
                    new_cloud[channel_id][timestamp]["episode_title"] = meta["episode_title"]

    updated = False
    if new_cloud and (not _CLOUD or set(new_cloud) != set(_CLOUD)):
        updated = True
    else:
        for channel_id in new_cloud:
            if set(new_cloud[channel_id]) != set(_CLOUD[channel_id]):
                updated = True
                break

    for channel_id in new_cloud:
        if channel_id not in _EPGDATA:
            _EPGDATA[channel_id] = {}
        for timestamp in list(set(new_cloud[channel_id]) - set(_EPGDATA[channel_id])):
            _EPGDATA[channel_id][timestamp] = new_cloud[channel_id][timestamp]

    if updated:
        _CLOUD = new_cloud
        async with aiofiles.open(cloud_data, "w", encoding="utf8") as f:
            await f.write(ujson.dumps({"data": _CLOUD}, ensure_ascii=False, indent=4, sort_keys=True))

    if updated or not os.path.exists(CHANNELS_CLOUD) or not os.path.exists(GUIDE_CLOUD):
        cmd = [f"movistar_tvg{EXT}", "--cloud_m3u", CHANNELS_CLOUD, "--cloud_recordings", GUIDE_CLOUD]
        async with tvgrab_lock:
            task = app.add_task(launch(cmd))
            await task
        check_task(task)

    log.info(f"Cloud Recordings Updated => {U7D_URL}/MovistarTVCloud.m3u - {U7D_URL}/cloud.xml")
    nr_epg = sum((len(_CLOUD[channel]) for channel in _CLOUD))
    log.info(f"Total: {len(_CLOUD):2} Channels & {nr_epg:5} EPG entries")


async def update_epg(abort_on_error=False):
    global _last_epg, _t_timers
    cmd = [f"movistar_tvg{EXT}", "--m3u", CHANNELS, "--guide", GUIDE]
    for i in range(3):
        async with tvgrab_lock:
            task = app.add_task(launch(cmd))
            await task
        check_task(task)

        if task.result():
            if i < 2:
                await asyncio.sleep(3)
                log.error(f"[{i + 2} / 3]...")
            elif abort_on_error or not _CHANNELS or not _EPGDATA:
                app.add_task(cancel_app())
        else:
            await reload_epg()
            _last_epg = int(datetime.now().replace(minute=0, second=0).timestamp())
            if RECORDINGS and not timers_lock.locked() and (not _t_timers or _t_timers.done()):
                _t_timers = app.add_task(timers_check(delay=10))
            break


async def update_epg_cron():
    last_datetime = datetime.now().replace(minute=0, second=0, microsecond=0)
    if os.path.getmtime(GUIDE) < last_datetime.timestamp():
        log.warning("EPG too old. Updating it...")
        await update_epg(abort_on_error=True)
    await asyncio.sleep((last_datetime + timedelta(hours=1) - datetime.now()).total_seconds())
    while True:
        await asyncio.gather(asyncio.sleep(3600), update_epg())


async def update_epg_local():
    if not RECORDINGS:
        return

    cmd = [f"movistar_tvg{EXT}", "--local_m3u", CHANNELS_LOCAL, "--local_recordings", GUIDE_LOCAL]

    async with tvgrab_lock:
        await launch(cmd)

    log.info(f"Local Recordings Updated => {U7D_URL}/MovistarTVLocal.m3u - {U7D_URL}/local.xml")
    nr_epg = sum((len(_RECORDINGS[channel]) for channel in _RECORDINGS))
    log.info(f"Total: {len(_RECORDINGS):2} Channels & {nr_epg:5} EPG entries")


async def update_recordings(archive=False):
    def _dump_files(files, channel=False, latest=False):
        m3u = ""
        for file in files:
            filename, ext = os.path.splitext(file)
            relname = filename[len(RECORDINGS) + 1 :]
            if os.path.exists(filename + ".jpg"):
                logo = relname + ".jpg"
            else:
                _logo = glob_safe(f"{filename}*.jpg")
                if _logo and os.path.isfile(_logo[0]):
                    logo = _logo[0][len(RECORDINGS) + 1 :]
                else:
                    logo = ""
            path, name = [t.replace(",", ":").replace(" - ", ": ") for t in os.path.split(relname)]
            m3u += '#EXTINF:-1 group-title="'
            m3u += '# Recientes"' if latest else f'{path}"' if path else '#"'
            m3u += f' tvg-logo="{U7D_URL}/recording/?' if logo else ""
            m3u += (urllib.parse.quote(logo) + '"') if logo else ""
            m3u += f",{path} - " if (channel and path) else ","
            m3u += f"{name}\n{U7D_URL}/recording/?"
            m3u += urllib.parse.quote(relname + ext) + "\n"
        return m3u

    async with recordings_lock:
        if archive and _RECORDINGS:
            async with aiofiles.open(recordings + ".tmp", "w", encoding="utf8") as f:
                await f.write(ujson.dumps(_RECORDINGS, ensure_ascii=False, indent=4, sort_keys=True))
            remove(recordings)
            os.rename(recordings + ".tmp", recordings)

        await update_epg_local()

        while True:
            try:
                _files = glob(f"{RECORDINGS}/**", recursive=True)
                _files = list(filter(lambda x: os.path.splitext(x)[1] in VID_EXTS, _files))
                _files.sort(key=os.path.getmtime)
                break
            except FileNotFoundError:
                await asyncio.sleep(1)

        if RECORDINGS_PER_CHANNEL:
            if not isinstance(archive, bool):
                topdirs = [get_channel_dir(archive)]
            else:
                topdirs = sorted(
                    [
                        _dir
                        for _dir in os.listdir(RECORDINGS)
                        if re.match(r"^[0-9]+\. ", _dir) and os.path.isdir(os.path.join(RECORDINGS, _dir))
                    ]
                )
        else:
            topdirs = []

        updated_m3u = False
        for _dir in [RECORDINGS] + topdirs:
            log.debug(f'Looking for recordings in "{_dir}"')
            files = (
                _files
                if _dir == RECORDINGS
                else list(filter(lambda x: x.startswith(os.path.join(RECORDINGS, _dir)), _files))
            )
            if not files:
                continue

            if archive is True and _dir != RECORDINGS:
                for subdir in os.listdir(os.path.join(RECORDINGS, _dir)):
                    subdir = os.path.join(RECORDINGS, _dir, subdir)
                    if not os.path.isdir(subdir):
                        continue
                    try:
                        newest = os.path.getmtime(list(filter(lambda x: x.startswith(subdir), files))[-1])
                    except IndexError:
                        continue
                    utime(newest, subdir)
                    if os.path.exists(os.path.join(subdir, "metadata")):
                        utime(newest, os.path.join(subdir, "metadata"))

            if not RECORDINGS_M3U:
                continue

            m3u = f"#EXTM3U name=\"{'Recordings' if _dir == RECORDINGS else _dir}\" dlna_extras=mpeg_ps_pal\n"
            if _dir == RECORDINGS:
                m3u += _dump_files(reversed(files), latest=True)
                m3u += _dump_files(sorted(files))
                m3u_file = os.path.join(RECORDINGS, "Recordings.m3u")
            else:
                m3u += _dump_files(files, channel=True)
                m3u_file = os.path.join(os.path.join(RECORDINGS, _dir), f"{_dir}.m3u")

            async with aiofiles.open(m3u_file, "w", encoding="utf8") as f:
                await f.write(m3u)

            newest = int(os.path.getmtime(files[-1]))
            utime(newest, *(m3u_file, os.path.join(RECORDINGS, _dir)))

            if RECORDINGS_PER_CHANNEL and topdirs:
                log.info(f"Wrote m3u [{m3u_file[len(RECORDINGS) + 1 :]}]")

            updated_m3u = True

    if updated_m3u:
        log.info(f"Local Recordings' M3U Updated => {U7D_URL}/Recordings.m3u")


async def upgrade_channel_numbers():
    if not RECORDINGS_PER_CHANNEL:
        return ()

    dirs = os.listdir(RECORDINGS)
    pairs = [r.groups() for r in [re.match(r"^(\d{3}). (.+)$", dir) for dir in dirs] if r]

    stale = []
    for nr, name in pairs:
        _f = filter(lambda x: _CHANNELS[x]["name"] == name and _CHANNELS[x]["number"] != int(nr), _CHANNELS)
        try:
            channel_id = next(_f)
            stale.append((int(channel_id), int(nr), _CHANNELS[channel_id]["number"], name))
        except StopIteration:
            pass

    if not stale:
        return ()

    stale_pairs = []
    log.warning("UPGRADING CHANNEL NUMBERS")
    for channel_id, old_nr, new_nr, name in stale:
        old_channel_name = f"{old_nr:03}. {name}"
        new_channel_name = f"{new_nr:03}. {name}"
        old_channel_path = os.path.join(RECORDINGS, old_channel_name)
        new_channel_path = os.path.join(RECORDINGS, new_channel_name)

        stale_pairs.append((old_channel_name, new_channel_name))

        for nfo_file in sorted(glob(f"{old_channel_path}/**/*{NFO_EXT}", recursive=True)):
            new_nfo_file = nfo_file.replace(old_channel_name, new_channel_name)
            if os.path.exists(new_nfo_file):
                log.warning(f'SKIPPING existing "{new_nfo_file[len(RECORDINGS) + 1 :]}"')
                continue

            async with aiofiles.open(nfo_file, encoding="utf-8") as f:
                data = await f.read()

            data = data.replace(f">{old_nr:03}. ", f">{new_nr:03}. ").replace(f">{old_nr}<", f">{new_nr}<")

            async with aiofiles.open(nfo_file, "w", encoding="utf8") as f:
                await f.write(data)

            if not os.path.exists(new_channel_path):
                continue

            files = list(get_recording_files(nfo_file[:-13]))
            old_files = [f for f in files if os.path.splitext(f)[-1] in (*VID_EXTS, ".jpg", ".nfo", ".png")]
            new_files = [f.replace(old_channel_name, new_channel_name) for f in old_files]

            old_dir = os.path.dirname(files[0])
            new_dir = old_dir.replace(old_channel_name, new_channel_name)

            if not os.path.exists(new_dir):
                os.mkdir(new_dir)

            if "/metadata/" in str(files):
                meta_dir = os.path.join(new_dir, "metadata")
                if not os.path.exists(meta_dir):
                    os.mkdir(meta_dir)

            for old_file, new_file in zip(old_files, new_files):
                if os.path.exists(new_file):
                    log.warning(f'SKIPPING existing "{new_file[len(RECORDINGS) + 1 :]}"')
                else:
                    os.rename(old_file, new_file)

        if os.path.exists(new_channel_path):
            stale_channel_name = f"OLD_{old_nr:03}_{name}"
            stale_channel_path = os.path.join(RECORDINGS, stale_channel_name)

            log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{stale_channel_name}"')
            os.rename(old_channel_path, stale_channel_path)
        else:
            log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{new_channel_name}"')
            os.rename(old_channel_path, new_channel_path)

    return stale_pairs


async def upgrade_recordings():
    if not RECORDINGS_UPGRADE:
        return

    log.warning("UPGRADING RECORDINGS METADATA")
    covers = wrong = 0

    for recording in sorted(glob(f"{RECORDINGS}/**/*{VID_EXT}", recursive=True)):
        basename = os.path.splitext(recording)[0]
        nfo_file = basename + NFO_EXT

        if not os.path.exists(nfo_file):
            log.error(f"Dropping recording. Could not find {nfo_file=}")
            remove(*get_recording_files(basename))
            continue

        mtime = int(os.path.getmtime(recording))

        async with aiofiles.open(nfo_file, encoding="utf-8") as f:
            xml = await f.read()

        xml = "\n".join([line for line in xml.splitlines() if "5S_signLanguage" not in line])

        cmd = ["ffprobe", "-i", recording, "-show_entries", "format=duration"]
        cmd += ["-v", "quiet", "-of", "json"]
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL)
        recording_data = ujson.loads((await proc.communicate())[0].decode())
        duration = round(float(recording_data["format"]["duration"]))

        try:
            nfo = xmltodict.parse(xml)["metadata"]
        except Exception as ex:
            log.error(f"Metadata malformed: {nfo_file=} => {repr(ex)}")
            raise

        _start, _end, _exp = [nfo[x] for x in ("beginTime", "endTime", "expDate")]
        _start, _end, _exp = [int(int(x) / 1000) if len(x) == 13 else int(x) for x in (_start, _end, _exp)]

        nfo.update({"beginTime": mtime, "duration": duration, "endTime": mtime + duration, "expDate": _exp})
        nfo.update({"name": os.path.basename(basename)})

        [nfo.pop(key) for key in DROP_KEYS if key in nfo]

        if os.path.exists(basename + ".jpg"):
            nfo.update({"cover": basename[len(RECORDINGS) + 1 :] + ".jpg"})
            covers += 1
        else:
            log.error(f'No cover found: "{basename}"')
            remove(*get_recording_files(basename))
            continue

        if _start != mtime:
            log.debug(f"{recording=}\n{_start=} {mtime=} {duration=} {_end=}")
            wrong += 1

        try:
            xml = xmltodict.unparse({"metadata": nfo}, pretty=True)
        except Exception as ex:
            log.error(f"Metadata malformed: {nfo_file=} => {repr(ex)}")
            remove(*get_recording_files(basename))
            continue

        async with aiofiles.open(nfo_file + ".tmp", "w", encoding="utf8") as f:
            await f.write(xml)

        remove(nfo_file)
        os.rename(nfo_file + ".tmp", nfo_file)
        utime(mtime, nfo_file)

    log.info(f"Updated #{covers} covers & Fixed #{wrong} timestamps")

    await update_recordings(True)


if __name__ == "__main__":
    if not WIN32:
        import signal
        from setproctitle import setproctitle

        setproctitle("movistar_epg")

    _IPTV = _SESSION_CLOUD = None
    _NETWORK_SATURATION = 0

    _CHANNELS = {}
    _CLOUD = {}
    _EPGDATA = {}
    _KEEP = {}
    _RECORDINGS = {}
    _RECORDINGS_INC = {}

    _last_bw_warning = _last_epg = _t_timers = _t_timers_next = None

    _conf = mu7d_config()

    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] [%(name)s] [%(levelname)8s] %(message)s",
        level=logging.DEBUG if _conf["DEBUG"] else logging.INFO,
    )

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)
    logging.getLogger("sanic.error").setLevel(logging.FATAL)
    logging.getLogger("sanic.root").disabled = True

    # Ths happens when epg is killed while on before_server_start
    filterwarnings(
        action="ignore",
        category=RuntimeWarning,
        message=r"coroutine '\w+.create_server' was never awaited",
        module="sys",
    )

    CHANNELS = _conf["CHANNELS"]
    CHANNELS_CLOUD = _conf["CHANNELS_CLOUD"]
    CHANNELS_LOCAL = _conf["CHANNELS_LOCAL"]
    GUIDE = _conf["GUIDE"]
    GUIDE_CLOUD = _conf["GUIDE_CLOUD"]
    GUIDE_LOCAL = _conf["GUIDE_LOCAL"]
    IPTV_BW_HARD = _conf["IPTV_BW_HARD"]
    IPTV_BW_SOFT = _conf["IPTV_BW_SOFT"]
    IPTV_IFACE = _conf["IPTV_IFACE"]
    MKV_OUTPUT = _conf["MKV_OUTPUT"]
    RECORDINGS = _conf["RECORDINGS"]
    RECORDINGS_M3U = _conf["RECORDINGS_M3U"]
    RECORDINGS_PER_CHANNEL = _conf["RECORDINGS_PER_CHANNEL"]
    RECORDINGS_REINDEX = _conf["RECORDINGS_REINDEX"]
    RECORDINGS_THREADS = _conf["RECORDINGS_THREADS"]
    RECORDINGS_UPGRADE = _conf["RECORDINGS_UPGRADE"]
    U7D_URL = _conf["U7D_URL"]
    VID_EXT = ".mkv" if MKV_OUTPUT else ".ts"

    U7D_PARENT = int(os.getenv("U7D_PARENT", "0"))

    if not U7D_PARENT:
        log.critical("Must be run with mu7d")
        sys.exit(1)

    cloud_data = os.path.join(_conf["HOME"], ".xmltv/cache/cloud.json")
    config_data = os.path.join(_conf["HOME"], ".xmltv/cache/config.json")
    epg_data = os.path.join(_conf["HOME"], ".xmltv/cache/epg.json")
    epg_metadata = os.path.join(_conf["HOME"], ".xmltv/cache/epg_metadata.json")
    recordings = os.path.join(_conf["HOME"], "recordings.json")
    timers = os.path.join(_conf["HOME"], "timers.conf")

    epg_lock = asyncio.Lock()
    network_bw_lock = asyncio.Lock()
    recordings_lock = asyncio.Lock()
    recordings_inc_lock = asyncio.Lock()
    timers_lock = asyncio.Lock()
    tvgrab_lock = asyncio.Lock()

    flussonic_regex = re.compile(r"\w*-?(\d{10})-?(\d+){0,1}\.?\w*")
    win32_normal_retcodes = (0, 1, 2, 15, 137, 143)  # These are different to those from closing the console

    monitor(
        app,
        is_middleware=False,
        latency_buckets=[1.0],
        mmc_period_sec=None,
        multiprocess_mode="livesum",
    ).expose_endpoint()

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_epg.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            app.run(
                host="127.0.0.1",
                port=8889,
                access_log=False,
                auto_reload=False,
                debug=_conf["DEBUG"],
                workers=1,
            )
    except (CancelledError, ConnectionResetError, KeyboardInterrupt):
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
    except ValueError as err:
        log.critical(err)
        sys.exit(1)
