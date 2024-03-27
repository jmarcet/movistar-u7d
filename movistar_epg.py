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
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from filelock import FileLock, Timeout
from glob import glob
from itertools import chain
from json import JSONDecodeError
from operator import itemgetter
from psutil import boot_time
from sanic import Sanic, response
from sanic_prometheus import monitor
from sanic.exceptions import Forbidden, NotFound
from tomli import TOMLDecodeError
from warnings import filterwarnings

from mu7d import DATEFMT, DIV_ONE, DIV_TWO, DROP_KEYS, EXT, FMT, NFO_EXT
from mu7d import UA_U7D, VID_EXTS, WIN32, YEAR_SECONDS, IPTVNetworkError
from mu7d import add_logfile, anchored_regex, cleanup_handler, find_free_port, get_iptv_ip, get_local_info
from mu7d import get_safe_filename, glob_safe, keys_to_int, launch, mu7d_config, ongoing_vods, pp_xml, remove
from mu7d import utime, _version


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
        if not _t_timers and int(datetime.now().replace(minute=0, second=0).timestamp()) <= _last_epg:
            delay = max(5, 180 - uptime)
            if delay > 10:
                log.info(f"Waiting {delay}s to check recording timers since the system just booted...")
            _t_timers = app.add_task(timers_check(delay))
        elif not _t_timers:
            log.warning("Delaying timers_check until the EPG is updated...")


@app.listener("before_server_stop")
async def before_server_stop(app=None):
    cleanup_handler()


async def alive():
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
    covers = set()
    paths = set()
    covers_path = os.path.join(RECORDINGS_TMP, "covers")
    rootdir = "[0-9][0-9][0-9]. */**"
    nfos = sorted(glob(f"{RECORDINGS}/{rootdir}/*{NFO_EXT}", recursive=True))

    for nfo in nfos:
        cover = nfo.replace(NFO_EXT, ".jpg")
        fanart = os.path.join(
            os.path.dirname(cover), "metadata", os.path.basename(cover).replace(".jpg", "-fanart.jpg")
        )

        if os.path.exists(fanart):
            cover = fanart
        elif not os.path.exists(cover):
            continue

        cached_cover = cover.replace(RECORDINGS, covers_path)
        path = os.path.dirname(cached_cover)
        parent = os.path.split(path)[0]

        covers.add((cover, cached_cover))
        for x in (x for x in (path, parent) if x != covers_path):
            paths.add(x)

    os.makedirs(covers_path)
    for path in sorted(paths):
        os.makedirs(path)

    for cover, cached_cover in sorted(covers, key=lambda x: os.path.getmtime(x[0])):
        shutil.copy2(cover, cached_cover)


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


def get_recording_files(fname):
    absname = os.path.join(RECORDINGS, fname.removesuffix(VID_EXT))
    nfo = os.path.join(absname + NFO_EXT)

    path, basename = os.path.split(absname)
    metadata = os.path.join(path, "metadata")

    files = glob_safe(f"{absname}.*")
    files += [nfo] if os.path.exists(nfo) else []
    files += [metadata, *glob_safe(os.path.join(metadata, f"{basename}-*"))] if os.path.exists(metadata) else []
    files += [path] if path != RECORDINGS else []

    return filter(lambda file: os.access(file, os.W_OK), files)


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
    return [
        ts
        for ts in sorted(_RECORDINGS[channel_id])
        if os.path.split(_RECORDINGS[channel_id][ts]["filename"])[0] == os.path.split(filename)[0]
    ]


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
        if channel_id not in _RECORDINGS:
            _RECORDINGS[channel_id] = {}

        prune_duplicates(channel_id, filename)
        if channel_id in _KEEP:
            prune_expired(channel_id, filename)

        nfo = await get_local_info(channel_id, timestamp, get_path(filename, bare=True), log)
        # Metadata's beginTime rules since it is how the recording will be found
        _RECORDINGS[channel_id][nfo["beginTime"]] = {"duration": nfo["duration"], "filename": filename}

        app.add_task(update_recordings(channel_id))

        msg = "Recording ARCHIVED"
        log.info(DIV_ONE % (msg, log_suffix))
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


@app.get("/reload_epg")
async def handle_reload_epg(request):
    app.add_task(reload_epg())
    return response.json({"status": "EPG Reload Queued"}, 200)


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
    cloud = request.json.get("cloud", False)
    local = not request.json["lat"]

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

    logging.getLogger("U7D").info(msg)


def prune_duplicates(channel_id, filename):
    for ts in tuple(ts for ts in _RECORDINGS[channel_id] if _RECORDINGS[channel_id][ts]["filename"] in filename):
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
    log.warning("REINDEXING RECORDINGS")
    _recordings = defaultdict(dict)

    for nfo_file in sorted(glob(f"{RECORDINGS}/[0-9][0-9][0-9]. */**/*{NFO_EXT}", recursive=True)):
        basename = nfo_file[:-13]
        if not does_recording_exist(basename):
            log.error(f'No recording found: "{basename + VID_EXT}" {nfo_file=}')
            continue

        try:
            async with aiofiles.open(nfo_file, encoding="utf-8") as f:
                nfo = xmltodict.parse(await f.read(), postprocessor=pp_xml)["metadata"]
        except Exception as ex:
            log.error(f"Cannot read {nfo_file=} => {repr(ex)}")
            continue

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
                    xml = xmltodict.unparse({"metadata": dict(sorted(nfo.items()))}, pretty=True)
                    async with aiofiles.open(nfo_file + ".tmp", "w", encoding="utf8") as f:
                        await f.write(xml)
                    remove(nfo_file)
                    os.rename(nfo_file + ".tmp", nfo_file)
            if stale:
                log.warning(f'Wrong channel_id [{channel_id:4}] in nfo => Skipping "{filename}"')
                continue

        utime(timestamp, *get_recording_files(filename))
        _recordings[channel_id][timestamp] = {"duration": duration, "filename": filename}

    if len(_recordings):
        global _RECORDINGS
        async with recordings_lock:
            _RECORDINGS = _recordings

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
                    _RECORDINGS = json.loads(await f.read(), object_hook=keys_to_int)
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
        nonlocal channel_id, stored_filenames
        return filter(
            lambda ts: channel_id not in _RECORDINGS
            or ts not in _RECORDINGS[channel_id]
            or get_recording_name(channel_id, ts) not in stored_filenames,
            timestamps,
        )

    async def _record(cloud=False):
        nonlocal channel_id, comskip, keep, kept, nr_procs, ongoing, queued, repeat, timestamp, vo

        if timestamp in queued[channel_id]:
            return

        guide = _CLOUD if cloud else _EPGDATA
        filename = get_recording_name(channel_id, timestamp, cloud)
        duration, pid, title = (guide[channel_id][timestamp][t] for t in ("duration", "pid", "full_title"))

        if filename in queued[channel_id].values():
            return

        if any(x in ongoing for x in (f"{channel_id} {pid} -b ", filename)):
            vod = await ongoing_vods(filename=filename, _all=True)
            if f"{channel_id} {pid} -b {timestamp} " in vod or f"{channel_id} {pid} -b " not in vod:
                return

        if channel_id in _RECORDINGS:
            r = tuple(
                filter(
                    lambda ts: filename.startswith(_RECORDINGS[channel_id][ts]["filename"]),
                    _RECORDINGS[channel_id],
                )
            )
            # only repeat a recording when title changed
            if r and not len(filename) > len(_RECORDINGS[channel_id][r[0]]["filename"]):
                # or asked to and new event is more recent than the archived
                if not repeat or timestamp <= r[0]:
                    return

        if keep:
            program = get_program_name(filename)
            kept[channel_id][program] = keep
            if keep < 0:
                if (datetime.fromtimestamp(timestamp) - datetime.now()).days < keep:
                    log.info('Older than     %d days: SKIPPING [%d] "%s"' % (abs(keep), timestamp, filename))
                    return
            else:
                current = tuple(filter(lambda x: program in x, (await ongoing_vods(_all=True)).split("|")))
                if len(current) == keep:
                    log.info('Recording %02d programs: SKIPPING [%d] "%s"' % (keep, timestamp, filename))
                    return
                siblings = get_siblings(channel_id, filename)
                if len(current) + len(siblings) == keep and timestamp < siblings[0]:
                    log.info('Recorded  %02d programs: SKIPPING [%d] "%s"' % (keep, timestamp, filename))
                    return

        _time = 0 if cloud else duration
        if timestamp + 30 > round(datetime.now().timestamp()):
            next_timers.append((title, timestamp))
            return
        log_suffix = f': [{channel_id:4}] [{pid}] [{timestamp}] "{filename}"'
        if 0 < _time < 300:
            log.info(DIV_TWO % ("Skipping MATCH", f"Too short [{_time}s]", log_suffix))
            return
        if await record_program(channel_id, pid, 0, _time, cloud, comskip, True, MKV_OUTPUT, vo):
            return

        log.info(DIV_ONE % (f"Found {'Cloud ' if cloud else ''}EPG MATCH", log_suffix))

        queued[channel_id][timestamp] = filename
        await asyncio.sleep(2.5 if not WIN32 else 4)

        nr_procs += 1
        if nr_procs >= RECORDINGS_PROCESSES:
            await log_network_saturated(nr_procs)
            return -1

    if not os.path.exists(timers_data):
        log.debug("timers_check: no timers.conf found")
        return

    if epg_lock.locked():  # If EPG is being updated, wait until it is done
        await epg_lock.acquire()
        epg_lock.release()

    log.debug("Processing timers")
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

    nr_procs = len(await ongoing_vods())
    if _NETWORK_SATURATION or nr_procs >= RECORDINGS_PROCESSES:
        await log_network_saturated(nr_procs)
        return

    ongoing = await ongoing_vods(_all=True)  # we want to check against all ongoing vods, also in pp
    log.debug("Ongoing VODs: |%s|" % ongoing)

    keep = repeat = False
    kept, next_timers, queued = defaultdict(dict), [], defaultdict(dict)
    async with recordings_lock:
        if sync_cloud:
            log.debug("Syncing cloud recordings")
            vo = _timers.get("sync_cloud_language", "") == "VO"
            for channel_id in _CLOUD:
                stored_filenames = (program["filename"] for program in _RECORDINGS[channel_id].values())
                channel_name = _CHANNELS[channel_id]["name"]
                log.debug("Checking Cloud: [%4d] [%s]" % (channel_id, channel_name))
                for timestamp in _filter_recorded(_CLOUD[channel_id]):
                    if await _record(cloud=True):
                        return _exit()

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
                days = delayed = fixed_timer = fresh = keep = repeat = None
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
                            delayed = True

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
                if delayed:
                    tss = filter(lambda ts: ts <= _last_epg - 3600 * 12, reversed(_EPGDATA[channel_id].keys()))
                elif fresh:
                    tss = filter(
                        lambda ts: ts <= _last_epg + 3600 and _EPGDATA[channel_id][ts]["end"] > int(time.time()),
                        reversed(_EPGDATA[channel_id].keys()),
                    )
                else:
                    tss = filter(lambda ts: ts <= _last_epg + 3600, reversed(_EPGDATA[channel_id].keys()))

                if fixed_timer:
                    # fixed timers are checked daily, so we want today's and all of last week
                    fixed_timestamps = [fixed_timer] if fixed_timer <= _last_epg + 3600 else []
                    fixed_timestamps += [fixed_timer - i * 24 * 3600 for i in range(1, 8)]

                    found_ts = deque()
                    for ts in tss:
                        for fixed_ts in fixed_timestamps:
                            if abs(ts - fixed_ts) <= 1500:
                                if not days or days.get(datetime.fromtimestamp(ts).isoweekday()):
                                    found_ts.append(ts)
                                break
                    tss = found_ts

                elif days:
                    tss = filter(lambda ts: days.get(datetime.fromtimestamp(ts).isoweekday()), tss)

                log.debug("Checking timer: [%4d] [%s] [%s]%s" % (channel_id, channel_name, timer_match, msg))
                for timestamp in _filter_recorded(tss):
                    _title = _EPGDATA[channel_id][timestamp]["full_title"]
                    if re.search(_clean(timer_match), _clean(_title), re.IGNORECASE):
                        if await _record():
                            return _exit()

    _exit()


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

        async with tvgrab_lock:
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
        remove(recordings_data)
        os.rename(recordings_data + ".tmp", recordings_data)

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
        remove(m3u_file)
        os.rename(m3u_file + ".tmp", m3u_file)

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

            files = tuple(get_recording_files(nfo_file[:-13]))
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
            remove(nfo_file)
            os.rename(nfo_file + ".tmp", nfo_file)
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
    _KEEP = {}
    _RECORDINGS = {}

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
