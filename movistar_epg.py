#!/usr/bin/env python3

import aiofiles
import aiohttp
import asyncio
import json
import logging
import os
import re
import shutil
import signal
import sys
import time
import tomli
import ujson
import unicodedata
import urllib.parse
import xmltodict

from aiohttp.client_exceptions import ClientConnectionError, ClientConnectorError
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL, PIPE
from collections import defaultdict, deque, namedtuple
from datetime import date, datetime, timedelta
from filelock import FileLock, Timeout
from glob import glob, iglob
from itertools import chain
from json import JSONDecodeError
from operator import itemgetter
from psutil import boot_time
from sanic import Sanic, response
from sanic_prometheus import monitor
from sanic.exceptions import Forbidden, NotFound
from tomli import TOMLDecodeError
from warnings import filterwarnings

from mu7d import DATEFMT, DIV_ONE, DIV_TWO, DROP_KEYS, EXT, FMT
from mu7d import NFO_EXT, VID_EXTS, WIN32, YEAR_SECONDS, IPTVNetworkError
from mu7d import add_logfile, anchored_regex, cleanup_handler, find_free_port, get_iptv_ip, get_local_info
from mu7d import get_safe_filename, glob_safe, keys_to_int, launch, mu7d_config, ongoing_vods, pp_xml, remove
from mu7d import rename, utime, _version


app = Sanic("movistar_epg")

log = logging.getLogger("EPG")


@app.listener("before_server_start")
async def before_server_start(app):
    global _IPTV, _SESSION, _last_epg

    app.config.FALLBACK_ERROR_FORMAT = "json"
    app.config.KEEP_ALIVE_TIMEOUT = YEAR_SECONDS

    if not WIN32:
        [signal.signal(sig, cleanup_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    _IPTV = get_iptv_ip()

    _SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1)
    )

    app.add_task(alive())

    banner = f"Movistar U7D - EPG v{_version}"
    log.info("-" * len(banner))
    log.info(banner)
    log.info("-" * len(banner))

    if IPTV_BW_SOFT:
        app.add_task(network_saturation())
        log.info(f"Ignoring RECORDINGS_PROCESSES => BW: {IPTV_BW_SOFT}-{IPTV_BW_HARD} kbps / {IPTV_IFACE}")

    await reload_epg()

    if _CHANNELS and _EPGDATA:
        if not _last_epg:
            _last_epg = int(os.path.getmtime(GUIDE))

        if RECORDINGS:
            if not os.path.exists(RECORDINGS):
                os.makedirs(RECORDINGS)
            else:
                if RECORDINGS_UPGRADE:
                    await upgrade_recordings()
                elif RECORDINGS_REINDEX:
                    await reindex_recordings()
                else:
                    await reload_recordings()
                if RECORDINGS_TMP and _RECORDINGS and not os.path.exists(os.path.join(RECORDINGS_TMP, "covers")):
                    await create_covers_cache()

        app.add_task(update_epg_cron())

    if not WIN32:
        [signal.signal(sig, signal.SIG_DFL) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]


@app.listener("after_server_start")
async def after_server_start(app):
    if _last_epg and RECORDINGS:
        global _t_timers

        uptime = int(datetime.now().timestamp() - boot_time())
        if not _t_timers:
            if int(datetime.now().replace(minute=0, second=0).timestamp()) <= _last_epg:
                delay = max(5, 180 - uptime)
                if delay > 10:
                    log.info(f"Waiting {delay}s to check recording timers since the system just booted...")
                _t_timers = app.add_task(timers_check(delay))
            else:
                log.warning("Delaying timers_check until the EPG is updated...")


@app.listener("before_server_stop")
async def before_server_stop(app):
    cleanup_handler()


async def alive():
    UA_U7D = f"movistar-u7d v{_version} [{sys.platform}] [{EXT}] [{_IPTV}]"
    async with aiohttp.ClientSession(headers={"User-Agent": UA_U7D}) as session:
        for _ in range(10):
            try:
                await session.get("https://openwrt.marcet.info/u7d/alive")
                break
            except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
                await asyncio.sleep(6)


async def cancel_app():
    await asyncio.sleep(1)
    if not WIN32:
        os.kill(U7D_PARENT, signal.SIGTERM)
    else:
        app.stop()


async def create_covers_cache():
    log.info("Recreating covers cache")

    covers_path = os.path.join(RECORDINGS_TMP, "covers")
    if os.path.exists(covers_path):
        shutil.rmtree(covers_path)
    os.makedirs(covers_path)

    for nfo in iglob(f"{RECORDINGS}/[0-9][0-9][0-9]. */**/*{NFO_EXT}", recursive=True):
        cover = nfo.replace(NFO_EXT, ".jpg")
        fanart = os.path.join(
            os.path.dirname(cover), "metadata", os.path.basename(cover).replace(".jpg", "-fanart.jpg")
        )

        if os.path.exists(fanart):
            cover = fanart
        elif not os.path.exists(cover):
            continue

        cached_cover = cover.replace(RECORDINGS, covers_path)
        os.makedirs(os.path.dirname(cached_cover), exist_ok=True)
        shutil.copy2(cover, cached_cover)

    log.info("Covers cache recreated")


def does_recording_exist(filename):
    return os.path.exists(os.path.join(RECORDINGS, filename + VID_EXT))


def get_channel_dir(channel_id):
    return "%03d. %s" % (_CHANNELS[channel_id]["number"], _CHANNELS[channel_id]["name"])


def get_epg(channel_id, program_id, cloud=False):
    guide = _CLOUD if cloud else _EPGDATA
    if channel_id not in guide:
        log.error(f"Not Found: {channel_id=}")
        return {}

    for timestamp in filter(lambda ts: guide[channel_id][ts]["pid"] == program_id, guide[channel_id]):
        return guide[channel_id][timestamp]

    log.error(f"Not Found: {channel_id=} {program_id=}")
    return {}


def get_path(filename, bare=False):
    return os.path.join(RECORDINGS, filename + (VID_EXT if not bare else ""))


def get_program_id(channel_id, url=None, cloud=False, local=False):
    if channel_id not in _CHANNELS:
        return

    if not url:
        start = int(time.time())
    elif not url.isdigit():
        start = int(flussonic_regex.match(url).groups()[0])
    elif len(url) == 10:
        start = int(url)
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
        program_id, duration = (_EPGDATA[channel_id][start][t] for t in ("pid", "duration"))

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


def get_recording_files(filename, cache=False):
    absname = os.path.join(RECORDINGS, filename.removesuffix(VID_EXT))
    nfo = os.path.join(absname + NFO_EXT)

    path, basename = os.path.split(absname)
    metadata = os.path.join(path, "metadata")

    files = glob_safe(f"{absname}.*")
    files += [nfo] if os.path.exists(nfo) else []
    files += [*glob_safe(os.path.join(metadata, f"{basename}-*")), metadata] if os.path.exists(metadata) else []
    files += [path] if path != RECORDINGS else []

    if cache and RECORDINGS_TMP:
        files += glob_safe(
            os.path.join(path.replace(RECORDINGS, os.path.join(RECORDINGS_TMP, "covers")), f"**/{basename}*"),
            recursive=True,
        )

    return tuple(filter(lambda file: os.access(file, os.W_OK), files))


def get_recording_name(channel_id, timestamp, cloud=False):
    guide = _CLOUD if cloud else _EPGDATA

    _program = guide[channel_id][timestamp]
    anchor = _program["genre"][0] == "0" and not _program["is_serie"]

    path = os.path.join(RECORDINGS, get_channel_dir(channel_id))
    if _program.get("serie"):
        path = os.path.join(path, get_safe_filename(_program["serie"]))
    elif anchor:
        path = os.path.join(path, get_safe_filename(_program["full_title"]))
    path = path.rstrip(".").rstrip(",")

    filename = os.path.join(path, get_safe_filename(_program["full_title"]))
    if anchor:
        filename += f' - {datetime.fromtimestamp(timestamp).strftime("%Y%m%d_%H%M")}'

    return filename[len(RECORDINGS) + 1 :]


def get_siblings(channel_id, filename):
    return tuple(
        ts
        for ts in sorted(_RECORDINGS[channel_id])
        if os.path.split(_RECORDINGS[channel_id][ts]["filename"])[0] == os.path.split(filename)[0]
    )


@app.put("/archive/<channel_id:int>/<program_id:int>")
async def handle_archive(request, channel_id, program_id):
    global _t_timers

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    cloud = request.args.get("cloud") == "1"

    timestamp = get_epg(channel_id, program_id, cloud).get("start")
    if not timestamp:
        raise NotFound(f"Requested URL {request.path} not found")

    filename = get_recording_name(channel_id, timestamp, cloud)

    log_suffix = f': [{channel_id:4}] [{program_id}] [{timestamp}] "{filename}"'

    if not _t_timers or _t_timers.done():
        _t_timers = app.add_task(timers_check(delay=3))

    log.debug('Checking for "%s"' % filename)
    if not does_recording_exist(filename):
        msg = "Recording NOT ARCHIVED"
        log.error(DIV_ONE % (msg, log_suffix))
        return response.json({"status": msg}, ensure_ascii=False, status=204)

    async with recordings_lock:
        prune_duplicates(channel_id, filename)
        if channel_id in _KEEP:
            prune_expired(channel_id, filename)

        nfo = await get_local_info(channel_id, timestamp, get_path(filename, bare=True), log)
        # Metadata's beginTime rules since it is how the recording will be found
        _RECORDINGS[channel_id][nfo["beginTime"]] = {"duration": nfo["duration"], "filename": filename}

        msg = "Recording ARCHIVED"
        log.info(DIV_ONE % (msg, log_suffix))

    app.add_task(update_recordings(channel_id))
    return response.json({"status": msg}, ensure_ascii=False)


@app.get("/epg_info/<channel_id:int>/<program_id:int>")
async def handle_epg_info(request, channel_id, program_id):
    cloud = request.args.get("cloud", "") == "1"
    epg = get_epg(channel_id, program_id, cloud)
    if epg:
        return response.json(epg)
    raise NotFound(f"Requested URL {request.path} not found")


@app.get("/program_id/<channel_id:int>/<url>")
async def handle_program_id(request, channel_id, url):
    try:
        cloud, local = request.args.get("cloud", "") == "1", request.args.get("local", "") == "1"
        return response.json(get_program_id(channel_id, url, cloud, local))
    except (AttributeError, KeyError):
        raise NotFound(f"Requested URL {request.path} not found")


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
        raise NotFound(f"Requested URL {request.path} not found")

    try:
        channel, program_id, start, duration, offset = get_program_id(channel_id, url, cloud).values()
    except AttributeError:
        raise NotFound(f"Requested URL {request.path} not found")

    if not record_time:
        record_time = duration - offset

    msg = await record_program(channel_id, program_id, offset, record_time, cloud, comskip, index, mkv, vo)
    if msg:
        raise Forbidden(msg)

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


@app.get("/reindex_recordings", name="recordings_reindex")
@app.get("/reload_recordings", name="recordings_reload")
async def handle_recordings(request):
    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    app.add_task(reindex_recordings() if request.route.path == "reindex_recordings" else reload_recordings())
    _m = "Reindex" if request.route.path == "reindex_recordings" else "Reload"
    return response.json({"status": f"Recordings {_m} Queued"}, 200)


@app.get("/reload_epg", name="epg_reload")
@app.get("/update_epg", name="epg_update")
async def handle_reload_epg(request):
    app.add_task(reload_epg() if request.route.path == "reload_epg" else update_epg())
    _m = "Reload" if request.route.path == "reload_epg" else "Update"
    return response.json({"status": f"EPG {_m} Queued"}, 200)


@app.get("/timers_check")
async def handle_timers_check(request):
    global _t_timers

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    if _NETWORK_SATURATION:
        return response.json({"status": await log_network_saturated()}, 404)

    delay = int(request.args.get("delay", 0))

    if _t_timers and not _t_timers.done():
        if delay:
            _t_timers.cancel()
        else:
            raise Forbidden("Already processing timers")

    _t_timers = app.add_task(timers_check(delay=delay))

    return response.json({"status": "Timers check queued"}, 200)


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
            _NETWORK_SATURATION = 2 if tp > IPTV_BW_HARD else 1 if tp > IPTV_BW_SOFT else 0

            if _NETWORK_SATURATION:
                if _t_timers and not _t_timers.done():
                    _t_timers.cancel()
                    app.add_task(log_network_saturated(_wait=True))
                if _NETWORK_SATURATION == 2 and not network_bw_lock.locked():
                    app.add_task(network_bw_control())

        before, last = now, cur
        await asyncio.sleep(1)


async def prom_event(request, method):
    if method not in ("add", "remove", "na"):
        return

    cloud = request.json.get("cloud", False)
    local = request.json.get("local", False)

    _event = get_program_id(request.json["channel_id"], request.json.get("url"), cloud, local)
    if not local:
        _epg = get_epg(request.json["channel_id"], _event["program_id"], cloud)
    else:
        path = _event["program_id"].removesuffix(VID_EXT)
        _epg = await get_local_info(request.json["channel_id"], _event["start"], path, log, extended=True)
    if not _epg:
        return

    if method == "add":
        offset = "[%d/%d]" % (_event["offset"], _event["duration"])

        request.app.ctx.metrics["RQS_LATENCY"].labels(
            request.json["method"],
            request.json["endpoint"] + _epg["full_title"] + f" _ {offset}",
            request.json["id"],
        ).observe(float(request.json["lat"]))

    elif method == "remove":
        for metric in filter(
            lambda _metric: request.json["method"] in _metric and str(request.json["id"]) in _metric,
            request.app.ctx.metrics["RQS_LATENCY"]._metrics,
        ):
            request.app.ctx.metrics["RQS_LATENCY"].remove(*metric)
            break

        start, end = _event["offset"], _event["offset"] + request.json["offset"]

        if not request.json["lat"] or start == end:
            return

        offset = "[%d-%d/%d]" % (start, end, _event["duration"])
        request.json["msg"] = request.json["msg"].replace("Playing", "Stopped")

    else:
        offset = "[%d/%d]" % (_event["offset"], _event["duration"])
        request.json["msg"] = request.json["msg"].replace("Playing", "NA     ")

    msg = '%-95s%9s: [%4d] [%08d] [%d] [%s] "%s" _ %s' % (
        request.json["msg"],
        f'[{request.json["lat"]:.4f}s]' if request.json["lat"] else "",
        request.json["channel_id"],
        _event["program_id"] if not local else 0,
        _event["start"],
        _event["channel"],
        _epg["full_title"],
        offset,
    )

    logging.getLogger("U7D").info(msg) if method != "na" else logging.getLogger("U7D").error(msg)


def prune_duplicates(channel_id, filename):
    def _clean(string):
        return re.sub(r"[^a-z0-9]", "", string.lower())

    for ts in tuple(
        ts
        for ts in _RECORDINGS[channel_id]
        if _clean(_RECORDINGS[channel_id][ts]["filename"]) in _clean(filename)
    ):
        duplicate = _RECORDINGS[channel_id][ts]["filename"]
        old_files = tuple(
            f
            for f in get_recording_files(duplicate, cache=True)
            if f not in get_recording_files(filename, cache=True)
        )

        remove(*old_files)
        [log.warning(f'REMOVED DUPLICATED "{file}"') for file in old_files if not os.path.exists(file)]

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
        old_files = get_recording_files(filename, cache=True)
        remove(*old_files)
        del _RECORDINGS[channel_id][ts]
        [log.warning(f'REMOVED "{file}"') for file in old_files if not os.path.exists(file)]

    if _KEEP[channel_id][program] < 0:
        max_days = abs(_KEEP[channel_id][program])
        newest = date.fromtimestamp(max(siblings[-1], os.path.getmtime(get_path(filename))))
        [_drop(ts) for ts in siblings if (newest - date.fromtimestamp(ts)).days > max_days]

    elif len(siblings) >= _KEEP[channel_id][program]:
        [_drop(ts) for ts in siblings[0 : len(siblings) - _KEEP[channel_id][program] + 1]]


async def record_program(
    channel_id, pid, offset=0, time=0, cloud=False, comskip=0, index=True, mkv=False, vo=False
):
    timestamp = get_epg(channel_id, pid, cloud).get("start")
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
                msg = DIV_ONE % ("Recording ONGOING: REPEATED EVENT", log_suffix)
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
    await asyncio.create_subprocess_exec(*cmd)


async def reindex_recordings():
    async def _write_nfo(nfo_file, nfo):
        xml = xmltodict.unparse({"metadata": dict(sorted(nfo.items()))}, pretty=True)
        async with aiofiles.open(nfo_file + ".tmp", "w", encoding="utf8") as f:
            await f.write(xml)
        rename(nfo_file + ".tmp", nfo_file)

    async with recordings_lock:
        log.info("REINDEXING RECORDINGS")

        nfos_updated = 0
        _recordings = defaultdict(dict)

        for nfo_file in sorted(
            glob(f"{RECORDINGS}/[0-9][0-9][0-9]. */**/*{NFO_EXT}", recursive=True), key=os.path.getmtime
        ):
            basename = nfo_file.removesuffix(NFO_EXT)
            if not does_recording_exist(basename):
                log.error(f'No recording found: "{basename + VID_EXT}" {nfo_file=}')
                continue

            try:
                async with aiofiles.open(nfo_file, encoding="utf-8") as f:
                    nfo = xmltodict.parse(await f.read(), postprocessor=pp_xml)["metadata"]
            except Exception as ex:
                log.error(f"Cannot read {nfo_file=} => {repr(ex)}")
                continue

            nfo_needs_update = False
            filename = nfo["cover"][:-4]
            channel_id, duration, timestamp = nfo["serviceUID"], nfo["duration"], nfo["beginTime"]

            if channel_id not in _CHANNELS:
                stale = True
                dirname = filename.split(os.path.sep)[0]
                _match = re.match(r"^(\d{3})\. (.+)$", dirname)
                if _match:
                    channel_nr = int(_match.groups()[0])
                    _ids = tuple(filter(lambda ch: _CHANNELS[ch]["number"] == channel_nr, _CHANNELS))
                    if _ids:
                        stale = False
                        log.warning(f'Replaced channel_id [{channel_id:4}] => [{_ids[0]:4}] in "{filename}"')
                        nfo["serviceUID"] = channel_id = _ids[0]
                        nfo_needs_update = True
                if stale:
                    log.warning(f'Wrong channel_id [{channel_id:4}] in nfo => Skipping "{filename}"')
                    continue

            recording_files = get_recording_files(filename)
            covers_files = tuple(
                x[len(RECORDINGS) + 1 :] for x in recording_files if "metadata" in x and os.path.isfile(x)
            )

            if len(covers_files) != len(nfo.get("covers", {})):
                log.info(f'Covers mismatch in metadata: "{nfo_file}"')
                nfo["covers"] = {}
                for _file in covers_files:
                    kind = _file.split("-")[-1].removesuffix(".jpg").removesuffix(".png")
                    nfo["covers"][kind] = _file
                nfo_needs_update = True

            if nfo_needs_update:
                nfos_updated += 1
                await _write_nfo(nfo_file, nfo)

            utime(timestamp, *recording_files)
            _recordings[channel_id][timestamp] = {"duration": duration, "filename": filename}

        if nfos_updated:
            log.info(f"Updated {nfos_updated} metadata files")
        else:
            log.info("No metadata files needed updates")

        if len(_recordings):
            global _RECORDINGS

            _RECORDINGS = _recordings
            if RECORDINGS_TMP:
                await create_covers_cache()

        log.info("RECORDINGS REINDEXED")

    await update_recordings()


async def reload_epg():
    global _CHANNELS, _EPGDATA

    if not any(map(os.path.exists, (CHANNELS, channels_data))) and all(
        map(os.path.exists, (epg_data, GUIDE, GUIDE + ".gz"))
    ):
        log.warning("Missing channel list! Need to download it. Please be patient...")

        cmd = (f"movistar_tvg{EXT}", "--m3u", CHANNELS)
        async with tvgrab_lock:
            await launch(cmd)

        if not os.path.exists(CHANNELS):
            return app.add_task(cancel_app())

    elif not all(map(os.path.exists, (CHANNELS, channels_data, epg_data, GUIDE, GUIDE + ".gz"))):
        log.warning("Missing channels data! Need to download it. Please be patient...")
        return await update_epg(abort_on_error=True)

    async with epg_lock:
        try:
            async with aiofiles.open(channels_data, encoding="utf8") as f:
                data = await f.read()
            _CHANNELS = json.loads(data, object_hook=keys_to_int)["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError) as ex:
            log.error(f"Failed to load Channels metadata {repr(ex)}")
            remove(channels_data)
            return await reload_epg()

        for _ in range(3):
            try:
                await _SESSION.post(f"{U7D_URL}/update_channels", data=data)
                break
            except (ClientConnectionError, ClientConnectorError, ClientOSError, ServerDisconnectedError):
                await asyncio.sleep(1)
        del data

        try:
            async with aiofiles.open(epg_data, encoding="utf8") as f:
                _EPGDATA = json.loads(await f.read(), object_hook=keys_to_int)["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError) as ex:
            log.error(f"Failed to load EPG data {repr(ex)}")
            remove(epg_data)
            return await reload_epg()

        log.info(f"Channels  &  EPG Updated => {U7D_URL}/MovistarTV.m3u      - {U7D_URL}/guide.xml.gz")
        nr_epg = sum((len(_EPGDATA[channel]) for channel in _EPGDATA))
        log.info(f"Total: {len(_EPGDATA):2} Channels & {nr_epg:5} EPG entries")

    await update_cloud()


async def reload_recordings():
    global _RECORDINGS

    if await upgrade_recording_channels():
        await reindex_recordings()
    else:
        async with recordings_lock:
            try:
                async with aiofiles.open(recordings_data, encoding="utf8") as f:
                    _RECORDINGS = defaultdict(dict, json.loads(await f.read(), object_hook=keys_to_int))
            except (JSONDecodeError, TypeError, ValueError) as ex:
                log.error(f'Failed to parse "recordings.json" => {repr(ex)}')
                remove(CHANNELS_LOCAL, GUIDE_LOCAL)
            except (FileNotFoundError, OSError, PermissionError):
                remove(CHANNELS_LOCAL, GUIDE_LOCAL)

        if not _RECORDINGS:
            await reindex_recordings()
            if not _RECORDINGS:
                return

        async with recordings_lock:
            for channel_id in _RECORDINGS:
                oldest_epg = min(_EPGDATA[channel_id].keys())
                for timestamp in (
                    ts
                    for ts in _RECORDINGS[channel_id]
                    if not does_recording_exist(_RECORDINGS[channel_id][ts]["filename"])
                ):
                    filename = _RECORDINGS[channel_id][timestamp]["filename"]
                    if timestamp > oldest_epg:
                        log.warning(f'Archived Local Recording "{filename}" not found on disk')
                    elif channel_id in _CLOUD and timestamp in _CLOUD[channel_id]:
                        log.warning(f'Archived Cloud Recording "{filename}" not found on disk')

    await update_epg_local()


async def timers_check(delay=0):
    global _KEEP, _t_timers_next

    Record = namedtuple("Record", ("ch_id", "ts", "cloud", "comskip", "delay", "keep", "repeat", "vo"))

    def _clean(string):
        return unicodedata.normalize("NFKD", string).encode("ASCII", "ignore").decode("utf8").strip()

    def _filter_recorded(channel_id, timestamps, stored_filenames):
        return filter(
            lambda ts: channel_id not in _RECORDINGS
            or ts not in _RECORDINGS[channel_id]
            or get_recording_name(channel_id, ts) not in stored_filenames,
            timestamps,
        )

    await asyncio.sleep(delay)

    async with epg_lock, recordings_lock:
        log.debug("Processing timers")

        if not os.path.exists(timers_data):
            log.debug("timers_check: no timers.conf found")
            return

        nr_procs = len(await ongoing_vods())
        if _NETWORK_SATURATION or nr_procs >= RECORDINGS_PROCESSES:
            await log_network_saturated(nr_procs)
            return

        try:
            async with aiofiles.open(timers_data, encoding="utf8") as f:
                _timers = tomli.loads(await f.read())
        except (AttributeError, TOMLDecodeError, TypeError, ValueError) as ex:
            log.error(f"Failed to parse timers.conf: {repr(ex)}")
            return
        except (FileNotFoundError, OSError, PermissionError) as ex:
            log.error(f"Failed to read timers.conf: {repr(ex)}")
            return

        deflang = _timers.get("default_language", "")
        sync_cloud = _timers.get("sync_cloud", False)
        kept, curr_timers, next_timers = defaultdict(dict), defaultdict(dict), []

        for str_channel_id in _timers.get("match", {}):
            channel_id = int(str_channel_id)
            if channel_id not in _EPGDATA:
                log.warning(f"Channel [{channel_id}] in timers.conf not found in EPG")
                continue
            channel_name = _CHANNELS[channel_id]["name"]

            log.debug("Checking EPG  : [%4d] [%s]" % (channel_id, channel_name))
            stored_filenames = (program["filename"] for program in _RECORDINGS[channel_id].values())

            for timer_match in _timers["match"][str_channel_id]:
                comskip = 2 if _timers.get("comskipcut") else 1 if _timers.get("comskip") else 0
                days = {}
                delay = keep = 0
                fresh = repeat = False
                fixed_timer = None
                lang = deflang
                msg = ""

                if " ## " in timer_match:
                    match_split = timer_match.split(" ## ")
                    timer_match = match_split[0]
                    for res in (res.lower().strip() for res in match_split[1:]):
                        if res[0].isnumeric():
                            try:
                                hour, minute = map(int, res.split(":"))
                                _ts = datetime.now().replace(hour=hour, minute=minute, second=0).timestamp()
                                fixed_timer = int(_ts)
                                msg = " [%02d:%02d] [%d]" % (hour, minute, fixed_timer)
                            except ValueError:
                                log.warning(
                                    f'Parsing failed: [{channel_id}] [{channel_name}] [{timer_match}] -> "{res}"'
                                )

                        elif res[0] == "@":
                            days = dict(map(lambda x: (x[0], x[1] == "x"), enumerate(res[1:8], start=1)))

                        elif res == "comskip":
                            comskip = 1

                        elif res == "comskipcut":
                            comskip = 2

                        elif res.startswith("delay"):
                            delay = 3600 * 12

                        elif res == "fresh":
                            fresh = True

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
                msg = f"[{comskip=}] [{delay=}] [{fresh=}] [{keep=}] [{repeat=}] [{days=}]{msg}"
                log.debug("Checking timer: [%4d] [%s] [%s] %s" % (channel_id, channel_name, timer_match, msg))

                if fresh:
                    tss = filter(
                        lambda ts: ts <= _last_epg + 3600 and _EPGDATA[channel_id][ts]["end"] > int(time.time()),
                        _EPGDATA[channel_id],
                    )
                else:
                    tss = filter(lambda ts: ts <= _last_epg + 3600 - delay, _EPGDATA[channel_id])

                if fixed_timer:
                    # fixed timers are checked daily, so we want today's and all of last week
                    fixed_timestamps = tuple(
                        int((datetime.fromtimestamp(fixed_timer) - timedelta(days=i)).timestamp())
                        for i in range(8)
                    )
                    tss = filter(
                        lambda ts: any(map(lambda fixed_ts: abs(ts - fixed_ts) <= 1500, fixed_timestamps))
                        and (not days or days.get(datetime.fromtimestamp(ts).isoweekday())),
                        tss,
                    )
                elif days:
                    tss = filter(lambda ts: days.get(datetime.fromtimestamp(ts).isoweekday()), tss)

                tss = tuple(tss)

                if keep:
                    for ts in filter(
                        lambda ts: re.search(
                            _clean(timer_match), _clean(_EPGDATA[channel_id][ts]["full_title"]), re.IGNORECASE
                        ),
                        tss,
                    ):
                        kept[channel_id][get_program_name(get_recording_name(channel_id, ts))] = keep
                        break

                for timestamp in filter(
                    lambda ts: re.search(
                        _clean(timer_match), _clean(_EPGDATA[channel_id][ts]["full_title"]), re.IGNORECASE
                    ),
                    _filter_recorded(channel_id, tss, stored_filenames),
                ):
                    _b, _f = "norml" if not fresh else "fresh", get_recording_name(channel_id, timestamp)
                    curr_timers[_b][_f] = Record(channel_id, timestamp, False, comskip, delay, keep, repeat, vo)

        if sync_cloud:
            comskip = 2 if _timers.get("comskipcut") else 1 if _timers.get("comskip") else 0
            vo = _timers.get("sync_cloud_language", "") == "VO"

            for channel_id in _CLOUD:
                log.debug("Checking Cloud: [%4d] [%s]" % (channel_id, _CHANNELS[channel_id]["name"]))
                stored_filenames = (program["filename"] for program in _RECORDINGS[channel_id].values())

                for timestamp in _filter_recorded(channel_id, _CLOUD[channel_id].keys(), stored_filenames):
                    _f = get_recording_name(channel_id, timestamp, cloud=True)
                    curr_timers["cloud"][_f] = Record(channel_id, timestamp, True, comskip, 0, 0, False, vo)

        _KEEP = kept
        ongoing = await ongoing_vods(_all=True)  # we want to check against all ongoing vods, also in pp

        log.debug("_KEEP=%s" % str(_KEEP))
        log.debug("Ongoing VODs: |%s|" % ongoing)

        for filename, r in chain(
            sorted(curr_timers["fresh"].items(), key=lambda x: _EPGDATA[x[1].ch_id][x[1].ts]["end"]),
            sorted(curr_timers["norml"].items(), key=lambda x: x[1].ts),
            sorted(curr_timers["cloud"].items(), key=lambda x: x[1].ts),
        ):
            guide = _EPGDATA if not r.cloud else _CLOUD
            pid = guide[r.ch_id][r.ts]["pid"]
            duration = guide[r.ch_id][r.ts]["duration"] if not r.cloud else 0
            log_suffix = f': [{r.ch_id:4}] [{pid}] [{r.ts}] "{filename}"'

            if 0 < duration < 240:
                log.info(DIV_TWO % ("Skipping MATCH", f"Too short [{duration}s]", log_suffix))
                continue

            if f"{r.ch_id} {pid} -b " in ongoing or filename in ongoing:
                continue

            _x = filter(lambda x: filename.startswith(_RECORDINGS[r.ch_id][x]["filename"]), _RECORDINGS[r.ch_id])
            recs = tuple(_x)
            # only repeat a recording when title changed
            if recs and not len(filename) > len(_RECORDINGS[r.ch_id][recs[0]]["filename"]):
                # or asked to and new event is more recent than the archived
                if not r.repeat or r.ts <= recs[0]:
                    continue

            if r.keep:
                if r.keep < 0:
                    if (datetime.fromtimestamp(r.ts) - datetime.now()).days < r.keep:
                        log.info('Older than     %d days: SKIPPING [%d] "%s"' % (abs(r.keep), r.ts, filename))
                        continue
                else:
                    program = get_program_name(filename)

                    current = tuple(filter(lambda x: program in x, (await ongoing_vods(_all=True)).split("|")))
                    if len(current) == r.keep:
                        log.info('Recording %02d programs: SKIPPING [%d] "%s"' % (r.keep, r.ts, filename))
                        continue

                    siblings = get_siblings(r.ch_id, filename)
                    if len(current) + len(siblings) == r.keep and r.ts < siblings[0]:
                        log.info('Recorded  %02d programs: SKIPPING [%d] "%s"' % (r.keep, r.ts, filename))
                        continue

            if r.ts + r.delay + 30 > round(datetime.now().timestamp()):
                next_timers.append((guide[r.ch_id][r.ts]["full_title"], r.ts + r.delay + 30))
                continue

            if await record_program(r.ch_id, pid, 0, duration, r.cloud, r.comskip, True, MKV_OUTPUT, r.vo):
                continue

            log.info(DIV_ONE % (f"Found {'Cloud ' if r.cloud else ''}EPG MATCH", log_suffix))

            nr_procs += 1
            if nr_procs >= RECORDINGS_PROCESSES:
                await log_network_saturated(nr_procs)
                break

            await asyncio.sleep(2.5 if not WIN32 else 4)

        next_title, next_ts = sorted(next_timers, key=itemgetter(1))[0] if next_timers else ("", 0)

        if _t_timers_next and (not next_ts or _t_timers_next.get_name() != f"{next_ts}"):
            if not _t_timers_next.done():
                _t_timers_next.cancel()
            _t_timers_next = None

        if next_ts and not _t_timers_next:
            log.info(f'Adding timers_check() @ {datetime.fromtimestamp(next_ts)} "{next_title}"')
            _t_timers_next = app.add_task(timers_check(delay=next_ts - time.time()), name=f"{next_ts}")


async def update_cloud():
    global _CLOUD

    async with epg_lock:
        cmd = (f"movistar_tvg{EXT}", "--cloud_m3u", CHANNELS_CLOUD, "--cloud_recordings", GUIDE_CLOUD)
        async with tvgrab_lock:
            await launch(cmd)

        try:
            async with aiofiles.open(cloud_data, encoding="utf8") as f:
                _CLOUD = json.loads(await f.read(), object_hook=keys_to_int)["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError):
            return await update_cloud()

        log.info(f"Cloud Recordings Updated => {U7D_URL}/MovistarTVCloud.m3u - {U7D_URL}/cloud.xml")
        nr_epg = sum((len(_CLOUD[channel]) for channel in _CLOUD))
        log.info(f"Total: {len(_CLOUD):2} Channels & {nr_epg:5} EPG entries")


async def update_epg(abort_on_error=False):
    global _last_epg, _t_timers
    cmd = (f"movistar_tvg{EXT}", "--m3u", CHANNELS, "--guide", GUIDE)
    for i in range(3):
        async with tvgrab_lock:
            retcode = await launch(cmd)

        if retcode:
            if i < 2:
                await asyncio.sleep(3)
                log.error(f"[{i + 2} / 3]...")
            elif abort_on_error or not _CHANNELS or not _EPGDATA:
                app.add_task(cancel_app())
        else:
            await reload_epg()
            _last_epg = int(datetime.now().replace(minute=0, second=0).timestamp())
            if RECORDINGS:
                if _t_timers and not _t_timers.done():
                    _t_timers.cancel()
                _t_timers = app.add_task(timers_check(delay=3))
            break

    if RECORDINGS and _RECORDINGS and await upgrade_recording_channels():
        await reindex_recordings()


async def update_epg_cron():
    last_datetime = datetime.now().replace(minute=0, second=0, microsecond=0)
    if os.path.getmtime(GUIDE) < last_datetime.timestamp():
        log.warning("EPG too old. Updating it...")
        await update_epg(abort_on_error=True)
    await asyncio.sleep((last_datetime + timedelta(hours=1) - datetime.now()).total_seconds())
    while True:
        await asyncio.gather(asyncio.sleep(3600), update_epg())


async def update_epg_local():
    async with recordings_lock:
        cmd = (f"movistar_tvg{EXT}", "--local_m3u", CHANNELS_LOCAL, "--local_recordings", GUIDE_LOCAL)

        async with tvgrab_local_lock:
            await launch(cmd)

        log.info(f"Local Recordings Updated => {U7D_URL}/MovistarTVLocal.m3u - {U7D_URL}/local.xml")
        nr_epg = sum((len(_RECORDINGS[channel]) for channel in _RECORDINGS))
        log.info(f"Total: {len(_RECORDINGS):2} Channels & {nr_epg:5} EPG entries")


async def update_recordings(channel_id=None):
    async with recordings_lock:
        if not _RECORDINGS:
            return

        sorted_recordings = {
            ch: dict(sorted(_RECORDINGS[ch].items())) for ch in (k for k in _CHANNELS if k in _RECORDINGS)
        }
        async with aiofiles.open(recordings_data + ".tmp", "w", encoding="utf8") as f:
            await f.write(ujson.dumps(sorted_recordings, ensure_ascii=False, indent=4))
        rename(recordings_data + ".tmp", recordings_data)

    await update_epg_local()

    if not RECORDINGS_M3U:
        if channel_id:
            utime(max(_RECORDINGS[channel_id].keys()), os.path.join(RECORDINGS, get_channel_dir(channel_id)))
        else:
            for ch_id in _RECORDINGS:
                utime(max(_RECORDINGS[ch_id].keys()), os.path.join(RECORDINGS, get_channel_dir(ch_id)))
        utime(max(max(_RECORDINGS[channel].keys()) for channel in _RECORDINGS), RECORDINGS)
        return

    translation = str.maketrans("_;[]", "/:()")

    def _dump_files(files, channel=False, latest=False):
        m3u = deque()
        for file in filter(lambda f: os.path.exists(f + VID_EXT), files):
            path, name = (urllib.parse.quote(x.translate(translation)) for x in os.path.split(file))
            _m3u = '#EXTINF:-1 group-title="'
            _m3u += '# Recientes"' if latest else f'{path}"' if path else '#"'
            _m3u += f' tvg-logo="{U7D_URL}/recording/?{urllib.parse.quote(file + ".jpg")}"'
            _m3u += f",{path} - " if (channel and path) else ","
            _m3u += f"{name}\n"
            _m3u += f"{U7D_URL}/recording/?{urllib.parse.quote(file + VID_EXT)}"
            m3u.append(_m3u)
        return m3u

    def _get_files(channel=None):
        if channel:
            return (f for f in sorted(_RECORDINGS[channel][ts]["filename"] for ts in _RECORDINGS[channel]))
        return (
            f
            for f in sorted([p[1]["filename"] for p in chain(*(_RECORDINGS[ch].items() for ch in _RECORDINGS))])
        )

    def _get_files_reversed():
        return (
            p["filename"]
            for _, p in sorted(
                tuple(chain(*(_RECORDINGS[ch].items() for ch in _RECORDINGS))), key=itemgetter(0), reverse=True
            )
        )

    channels = (channel_id,) if channel_id else tuple(ch for ch in _CHANNELS if ch in _RECORDINGS)
    channels_dirs = [(ch, get_channel_dir(ch)) for ch in channels]
    channels_dirs += [(0, RECORDINGS)]

    for ch_id, _dir in channels_dirs:
        header = f"#EXTM3U name=\"{'Recordings' if _dir == RECORDINGS else _dir}\" dlna_extras=mpeg_ps_pal\n"
        if ch_id:
            m3u_file = os.path.join(RECORDINGS, _dir, f"{_dir}.m3u")
        else:
            m3u_file = os.path.join(RECORDINGS, "Recordings.m3u")

        async with aiofiles.open(m3u_file + ".tmp", "w", encoding="utf8") as f:
            await f.write(header)
            if ch_id:
                await f.write("\n".join(_dump_files(_get_files(ch_id), channel=True)))
            else:
                await f.write("\n".join(_dump_files(_get_files_reversed(), latest=True)) + "\n")
                await f.write("\n".join(_dump_files(_get_files())))
        rename(m3u_file + ".tmp", m3u_file)

        if ch_id:
            utime(max(_RECORDINGS[ch_id].keys()), *(m3u_file, os.path.join(RECORDINGS, _dir)))
        else:
            utime(max(max(_RECORDINGS[channel].keys()) for channel in _RECORDINGS), *(m3u_file, RECORDINGS))

        log.info(f"Wrote m3u [{m3u_file[len(RECORDINGS) + 1 :]}]")

    log.info(f"Local Recordings' M3U Updated => {U7D_URL}/Recordings.m3u")


async def upgrade_recording_channels():
    changed_nr, stale = deque(), deque()

    names = tuple(_CHANNELS[x]["name"] for x in _CHANNELS)
    numbers = tuple(_CHANNELS[x]["number"] for x in _CHANNELS)

    dirs = filter(lambda _dir: os.path.isdir(os.path.join(RECORDINGS, _dir)), os.listdir(RECORDINGS))
    pairs = (r.groups() for r in (re.match(r"^(\d{3})\. (.+)$", _dir) for _dir in dirs) if r)

    for nr, name in pairs:
        nr = int(nr)
        if nr not in numbers and name in names:
            _f = filter(lambda x: _CHANNELS[x]["name"] == name and _CHANNELS[x]["number"] != nr, _CHANNELS)
            for channel_id in _f:
                changed_nr.append((channel_id, nr, _CHANNELS[channel_id]["number"], name))
        elif nr not in numbers or name not in names:
            stale.append((nr, name))

    if not changed_nr and not stale:
        return False

    log.warning("UPGRADING RECORDINGS' CHANNELS")

    for nr, name in stale:
        old_channel_name = f"{nr:03}. {name}"
        new_channel_name = f'OLD_{datetime.now().strftime("%Y%m%dT%H%M%S")}_{nr:03}_{name}'
        if nr in numbers and name not in names:
            log.warning(f'Channel "{old_channel_name}" has changed its name')
        else:
            log.warning(f'Recording dir "{old_channel_name}" not recognized')
        os.rename(os.path.join(RECORDINGS, old_channel_name), os.path.join(RECORDINGS, new_channel_name))
        log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{new_channel_name}"')

    changed_pairs = deque()
    for channel_id, old_nr, new_nr, name in changed_nr:
        old_channel_name = f"{old_nr:03}. {name}"
        new_channel_name = f"{new_nr:03}. {name}"
        old_channel_path = os.path.join(RECORDINGS, old_channel_name)
        new_channel_path = os.path.join(RECORDINGS, new_channel_name)

        changed_pairs.append((old_channel_name, new_channel_name))

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

            files = get_recording_files(nfo_file.removesuffix(NFO_EXT))
            old_files = (f for f in files if os.path.splitext(f)[-1] in (*VID_EXTS, ".jpg", ".nfo", ".png"))
            new_files = (f.replace(old_channel_name, new_channel_name) for f in old_files)

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
            changed_channel_name = f'OLD_{datetime.now().strftime("%Y%m%dT%H%M%S")}_{old_nr:03}_{name}'
            changed_channel_path = os.path.join(RECORDINGS, changed_channel_name)

            log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{changed_channel_name}"')
            os.rename(old_channel_path, changed_channel_path)
        else:
            log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{new_channel_name}"')
            os.rename(old_channel_path, new_channel_path)

    return True


async def upgrade_recordings():
    log.info(f"UPGRADING RECORDINGS METADATA: {RECORDINGS_UPGRADE}")
    covers = items = names = wrong = 0

    for nfo_file in sorted(glob(f"{RECORDINGS}/**/*{NFO_EXT}", recursive=True)):
        basename = nfo_file.split(NFO_EXT)[0]
        recording = basename + VID_EXT

        mtime = int(os.path.getmtime(recording))

        async with aiofiles.open(nfo_file, encoding="utf-8") as f:
            xml = await f.read()

        xml = "\n".join([line for line in xml.splitlines() if "5S_signLanguage" not in line])

        try:
            nfo = xmltodict.parse(xml, postprocessor=pp_xml)["metadata"]
        except Exception as ex:
            log.error(f"Metadata malformed: {nfo_file=} => {repr(ex)}")
            continue

        if abs(RECORDINGS_UPGRADE) == 2:
            cmd = ("ffprobe", "-i", recording, "-show_entries", "format=duration")
            cmd += ("-v", "quiet", "-of", "json")
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL)
            recording_data = ujson.loads((await proc.communicate())[0].decode())
            duration = round(float(recording_data.get("format", {}).get("duration", 0)))

            _start, _end, _exp = (
                nfo[x] // 1000 if nfo[x] > 10**10 else nfo[x] for x in ("beginTime", "endTime", "expDate")
            )

            nfo.update({"beginTime": mtime, "duration": duration, "endTime": mtime + duration, "expDate": _exp})

            if _start != mtime:
                wrong += 1

        [nfo.pop(key) for key in set(nfo) & set(DROP_KEYS)]

        for key in filter(lambda key: isinstance(nfo[key], dict) and "item" in nfo[key], nfo):
            new_value = tuple(nfo[key].values())[0]
            log.info(f"nfo[{key}] = {nfo[key]} => {new_value}")
            nfo[key] = new_value
            items += 1

        nfo["name"] = os.path.basename(basename)

        if not any((nfo.get("originalName"), nfo.get("is_serie"))):
            _match = anchored_regex.match(nfo["name"])
            if _match:
                names += 1
                nfo["originalName"] = _match.groups()[0]
                log.info(f'"{nfo["name"]}" => "{nfo["originalName"]}"')

        if not nfo.get("cover", "") == basename[len(RECORDINGS) + 1 :] + ".jpg":
            nfo["cover"] = basename[len(RECORDINGS) + 1 :] + ".jpg"
            covers += 1
        if not os.path.exists(basename + ".jpg"):
            log.warning('Cover not found: "%s"' % basename[len(RECORDINGS) + 1 :] + ".jpg")

        drop_covers, new_covers = [], {}
        if nfo.get("covers"):
            for cover in nfo["covers"]:
                if not nfo["covers"][cover].startswith("metadata"):
                    continue
                covers += 1
                cover_path = os.path.join(os.path.dirname(basename), nfo["covers"][cover])
                if not os.path.exists(cover_path):
                    log.warning('Cover not found: "%s"' % cover_path[len(RECORDINGS) + 1 :])
                    drop_covers.append(cover)
                    continue
                new_covers[cover] = cover_path[len(RECORDINGS) + 1 :]
        elif "covers" in nfo:
            nfo.pop("covers")

        if new_covers:
            nfo["covers"] = new_covers
        elif drop_covers:
            [nfo["covers"].pop(cover) for cover in drop_covers]

        if RECORDINGS_UPGRADE > 0:
            xml = xmltodict.unparse({"metadata": dict(sorted(nfo.items()))}, pretty=True)
            async with aiofiles.open(nfo_file + ".tmp", "w", encoding="utf8") as f:
                await f.write(xml)
            rename(nfo_file + ".tmp", nfo_file)
            utime(mtime, nfo_file)

    if any((covers, items, names, wrong)):
        msg = ("Would update", "Would fix") if RECORDINGS_UPGRADE < 0 else ("Updated", "Fixed")
        log.info(
            f"{msg[0]} #{covers} covers. {msg[1]} #{items} items, #{names} originalNames & #{wrong} timestamps"
        )
    else:
        log.info(f"No updates {'would be ' if RECORDINGS_UPGRADE < 0 else ''}carried out")

    if RECORDINGS_UPGRADE > 0:
        await update_recordings()


if __name__ == "__main__":
    if not WIN32:
        from setproctitle import setproctitle

        setproctitle("movistar_epg")

    _IPTV = _SESSION = None
    _NETWORK_SATURATION = 0

    _CHANNELS = {}
    _CLOUD = {}
    _EPGDATA = {}
    _KEEP = defaultdict(dict)
    _RECORDINGS = defaultdict(dict)

    _last_bw_warning = _last_epg = _t_timers = _t_timers_next = None

    _conf = mu7d_config()

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)
    logging.getLogger("sanic.error").setLevel(logging.FATAL)
    logging.getLogger("sanic.root").disabled = True

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=_conf.get("DEBUG") and logging.DEBUG or logging.INFO)

    if not _conf:
        log.critical("Imposible parsear fichero de configuración")
        sys.exit(1)

    if _conf["LOG_TO_FILE"]:
        add_logfile(log, _conf["LOG_TO_FILE"], _conf["DEBUG"] and logging.DEBUG or logging.INFO)

    # Ths happens when epg is killed while on before_server_start
    filterwarnings(
        action="ignore",
        category=RuntimeWarning,
        message=r"coroutine '\w+.create_server' was never awaited",
        module="sys",
    )

    U7D_PARENT = int(os.getenv("U7D_PARENT", "0"))

    if not U7D_PARENT:
        log.critical("Must be run with mu7d")
        sys.exit(1)

    CHANNELS = _conf["CHANNELS"]
    CHANNELS_CLOUD = _conf["CHANNELS_CLOUD"]
    CHANNELS_LOCAL = _conf["CHANNELS_LOCAL"]
    DEBUG = _conf["DEBUG"]
    GUIDE = _conf["GUIDE"]
    GUIDE_CLOUD = _conf["GUIDE_CLOUD"]
    GUIDE_LOCAL = _conf["GUIDE_LOCAL"]
    IPTV_BW_HARD = _conf["IPTV_BW_HARD"]
    IPTV_BW_SOFT = _conf["IPTV_BW_SOFT"]
    IPTV_IFACE = _conf["IPTV_IFACE"]
    MKV_OUTPUT = _conf["MKV_OUTPUT"]
    RECORDINGS = _conf["RECORDINGS"]
    RECORDINGS_M3U = _conf["RECORDINGS_M3U"]
    RECORDINGS_REINDEX = _conf["RECORDINGS_REINDEX"]
    RECORDINGS_PROCESSES = _conf["RECORDINGS_PROCESSES"]
    RECORDINGS_TMP = _conf["RECORDINGS_TMP"]
    RECORDINGS_UPGRADE = _conf["RECORDINGS_UPGRADE"]
    U7D_URL = _conf["U7D_URL"]
    VID_EXT = ".mkv" if MKV_OUTPUT else ".ts"

    cloud_data = os.path.join(_conf["HOME"], ".xmltv/cache/cloud.json")
    channels_data = os.path.join(_conf["HOME"], ".xmltv/cache/channels.json")
    epg_data = os.path.join(_conf["HOME"], ".xmltv/cache/epg.json")
    recordings_data = os.path.join(_conf["HOME"], "recordings.json")
    timers_data = os.path.join(_conf["HOME"], "timers.conf")

    epg_lock = asyncio.Lock()
    network_bw_lock = asyncio.Lock()
    recordings_lock = asyncio.Lock()
    tvgrab_lock = asyncio.Lock()
    tvgrab_local_lock = asyncio.Lock()

    flussonic_regex = re.compile(r"\w*-?(\d{10})-?(\d+){0,1}\.?\w*")

    monitor(
        app,
        is_middleware=False,
        latency_buckets=[1.0],
        mmc_period_sec=None,
        multiprocess_mode="livesum",
    ).expose_endpoint()

    lockfile = os.path.join(_conf["TMP_DIR"], ".movistar_epg.lock")
    del _conf
    try:
        with FileLock(lockfile, timeout=0):
            app.run(
                host="127.0.0.1",
                port=8889,
                access_log=False,
                auto_reload=False,
                debug=DEBUG,
                workers=1,
            )
    except (CancelledError, ConnectionResetError, KeyboardInterrupt):
        sys.exit(1)
    except IPTVNetworkError as err:
        log.critical(err)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
