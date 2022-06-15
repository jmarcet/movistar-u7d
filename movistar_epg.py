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

from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from datetime import datetime, timedelta
from filelock import FileLock, Timeout
from glob import glob
from psutil import Process, boot_time
from sanic import Sanic, response
from sanic_prometheus import monitor
from sanic.exceptions import NotFound, ServiceUnavailable

if hasattr(asyncio, "exceptions"):
    from asyncio.exceptions import CancelledError
else:
    from asyncio import CancelledError

from mu7d import EXT, IPTV_DNS, UA, UA_U7D, URL_MVTV, VID_EXTS, WIN32, YEAR_SECONDS
from mu7d import find_free_port, get_iptv_ip, get_safe_filename, get_title_meta, glob_safe, ongoing_vods
from mu7d import launch, mu7d_config, remove, _version


app = Sanic("movistar_epg")

log = logging.getLogger("EPG")


@app.listener("before_server_start")
async def before_server_start(app, loop):
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
                        if not does_recording_exist(filename):
                            if timestamp > oldest_epg:
                                log.warning(f'Archived recording "{filename}" not found on disk')
                            elif channel_id in _CLOUD and timestamp in _CLOUD[channel_id]:
                                log.warning(f'Archived Cloud Recording "{filename}" not found on disk')
                            else:
                                continue
                        else:
                            _indexed.add(os.path.join(RECORDINGS, filename + VID_EXT))
                            [os.utime(file, (-1, timestamp)) for file in get_recording_files(filename)]
                        int_recordings[channel_id][timestamp] = recording
                _RECORDINGS = int_recordings
            except (TypeError, ValueError) as ex:
                log.error(f'Failed to parse "recordings.json". It will be reset!!!: {repr(ex)}')
            except FileNotFoundError:
                pass

            for file in sorted(set(glob(f"{RECORDINGS}/**/*{VID_EXT}", recursive=True)) - _indexed):
                timestamp = os.path.getmtime(file)
                [os.utime(file, (-1, timestamp)) for file in get_recording_files(os.path.splitext(file)[0])]

            await update_recordings(True)

        app.add_task(update_epg_cron())

    _IPTV = get_iptv_ip()

    if not WIN32:
        [signal.signal(sig, signal.SIG_DFL) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]


@app.listener("after_server_start")
async def after_server_start(app, loop):
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
        log.info(f"Manual timers check => {U7D_URL}/timers_check")


@app.listener("before_server_stop")
def before_server_stop(app=None, loop=None):
    [task.cancel() for task in asyncio.all_tasks()]


async def alive():
    async with aiohttp.ClientSession(headers={"User-Agent": UA_U7D}) as session:
        for i in range(10):
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
    before_server_stop()
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
        log.error(f"channel_id={channel_id} not found")
        return

    for timestamp in sorted(guide[channel_id]):
        _epg = guide[channel_id][timestamp]
        if program_id == _epg["pid"]:
            return _epg, timestamp
    log.error(f"channel_id={channel_id} program_id={program_id} not found")


def get_program_id(channel_id, url=None, cloud=False):
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

    if not cloud:
        if start not in _EPGDATA[channel_id]:
            for timestamp in _EPGDATA[channel_id]:
                if timestamp > start:
                    break
                last_timestamp = timestamp
            if not last_timestamp:
                return
            start, new_start = last_timestamp, start
        program_id, duration = [_EPGDATA[channel_id][start][t] for t in ["pid", "duration"]]
    else:
        if channel_id not in _CLOUD:
            return
        if start in _CLOUD[channel_id]:
            duration = _CLOUD[channel_id][start]["duration"]
        else:
            for timestamp in _CLOUD[channel_id]:
                duration = _CLOUD[channel_id][timestamp]["duration"]
                if start > timestamp and start < timestamp + duration:
                    start, new_start = timestamp, start
            if not new_start:
                return
        program_id = _CLOUD[channel_id][start]["pid"]

    return {
        "channel": channel,
        "program_id": program_id,
        "start": start,
        "duration": duration,
        "offset": new_start - start if new_start else 0,
    }


def get_recording_files(fname):
    basename = os.path.basename(fname)
    metadata = os.path.join(RECORDINGS, os.path.join(os.path.dirname(fname), "metadata"))

    files = glob_safe(os.path.join(RECORDINGS, f"{fname}*"))
    files += glob_safe(os.path.join(metadata, f"{basename}*"))

    return filter(lambda file: os.access(file, os.W_OK), files)


def get_recording_name(channel_id, timestamp, cloud=False):
    guide = _CLOUD if cloud else _EPGDATA
    daily_news_program = (
        not guide[channel_id][timestamp]["is_serie"] and guide[channel_id][timestamp]["genre"] == "06"
    )

    if RECORDINGS_PER_CHANNEL:
        path = os.path.join(RECORDINGS, get_channel_dir(channel_id))
    else:
        path = RECORDINGS
    if guide[channel_id][timestamp]["serie"]:
        path = os.path.join(path, get_safe_filename(guide[channel_id][timestamp]["serie"]))
    elif daily_news_program:
        path = os.path.join(path, get_safe_filename(guide[channel_id][timestamp]["full_title"]))
    path = path.rstrip(".").rstrip(",")

    filename = os.path.join(path, get_safe_filename(guide[channel_id][timestamp]["full_title"]))
    if daily_news_program:
        filename += f' - {datetime.fromtimestamp(timestamp).strftime("%Y%m%d")}'

    return filename[len(RECORDINGS) + 1 :]


@app.route("/archive/<channel_id:int>/<program_id:int>", methods=["OPTIONS", "PUT"])
async def handle_archive(request, channel_id, program_id, cloud=False):
    global _RECORDINGS, _t_timers

    cloud = request.args.get("cloud") == "1" if request else cloud

    try:
        _epg, timestamp = get_epg(channel_id, program_id, cloud)
    except TypeError:
        raise NotFound(f"Requested URL {request.raw_url.decode()} not found")

    filename = get_recording_name(channel_id, timestamp, cloud)

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    log_suffix = '[%s] [%s] [%s] [%s] "%s"' % (
        _CHANNELS[channel_id]["name"],
        channel_id,
        program_id,
        timestamp,
        filename,
    )

    if not _t_timers or _t_timers.done():
        _t_timers = app.add_task(timers_check(delay=5))

    recorded = int(request.args.get("recorded", 0))
    if recorded:
        if request.method == "OPTIONS" and recorded < 90:
            log.error(f"Recording WRONG: {log_suffix}")
            return response.json({"status": "Recording WRONG"}, status=202)

        async with recordings_inc_lock:
            if channel_id not in _RECORDINGS_INC:
                _RECORDINGS_INC[channel_id] = {}
            if program_id not in _RECORDINGS_INC[channel_id]:
                _RECORDINGS_INC[channel_id][program_id] = []
            _RECORDINGS_INC[channel_id][program_id].append(recorded)
            _t = _RECORDINGS_INC[channel_id][program_id]

        if request.method == "OPTIONS" and len(_t) > 2 and _t[-3] == _t[-2] and _t[-2] == _t[-1]:
            log.warning(f"Recording Incomplete KEEP: {log_suffix}: {_t}")
            return response.json({"status": "Recording Incomplete KEEP"}, status=201)

        elif request.method == "PUT" and len(_t) > 3 and _t[-4] == _t[-3] and _t[-3] == _t[-2]:
            log.warning(f"Recording Incomplete KEPT: {log_suffix}: {_t}")
            async with recordings_inc_lock:
                del _RECORDINGS_INC[channel_id][program_id]

        else:
            log.error(f"Recording Incomplete RETRY: {log_suffix}: {_t}")
            return response.json({"status": "Recording Incomplete RETRY"}, status=202)

    elif request.method == "OPTIONS":
        log.debug(f"Recording OK: {log_suffix}")
        return response.json({"status": "Recording OK"}, status=200)

    def _prune_duplicates():
        global _RECORDINGS

        found = [ts for ts in _RECORDINGS[channel_id] if _RECORDINGS[channel_id][ts]["filename"] in filename]
        if found:
            for ts in found:
                duplicate = _RECORDINGS[channel_id][ts]["filename"]
                old_files = sorted(set(get_recording_files(duplicate)) - set(get_recording_files(filename)))

                [remove(file) for file in old_files]
                [log.warning(f'REMOVED DUPLICATED "{file}"') for file in old_files]

                del _RECORDINGS[channel_id][ts]

    log.debug(f"Checking for {filename}")
    if does_recording_exist(filename):
        async with recordings_lock:
            if channel_id not in _RECORDINGS:
                _RECORDINGS[channel_id] = {}

            _prune_duplicates()
            _RECORDINGS[channel_id][timestamp] = {"filename": filename}

        app.add_task(update_recordings(channel_id))

        msg = f"Recording ARCHIVED: {log_suffix}"
        log.info(msg)
        return response.json({"status": msg}, ensure_ascii=False)
    else:
        msg = f"Recording NOT ARCHIVED: {log_suffix}"
        log.error(msg)
        return response.json({"status": msg}, ensure_ascii=False, status=203)


@app.get("/channels/")
async def handle_channels(request):
    return response.json(_CHANNELS) if _CHANNELS else response.empty(404)


@app.get("/program_id/<channel_id:int>/<url>")
async def handle_program_id(request, channel_id, url):
    try:
        return response.json(get_program_id(channel_id, url, request.args.get("cloud") == "1"))
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
    mp4 = request.args.get("mp4") == "1"
    vo = request.args.get("vo") == "1"

    try:
        channel, program_id, start, duration, offset = get_program_id(channel_id, url, cloud).values()
    except AttributeError:
        raise NotFound(f"Requested URL {request.raw_url.decode()} not found")

    if request.args.get("time"):
        record_time = int(request.args.get("time"))
    else:
        record_time = duration - offset

    msg = await record_program(channel_id, program_id, offset, record_time, cloud, mp4, vo)
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
        }
    )


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

    if _t_timers and not _t_timers.done():
        raise ServiceUnavailable("Already processing timers")

    _t_timers = app.add_task(timers_check())

    return response.json({"status": "Timers check queued"}, 200)


async def kill_vod():
    vods = await ongoing_vods()
    if not vods:
        return

    try:
        vod = vods[-1]
        proc = vod.children()[0]
    except IndexError:
        await asyncio.sleep(1)
        return await kill_vod()

    ffmpeg = " ".join(proc.cmdline()).split(RECORDINGS)[1][1:-4]
    log.warning(f'KILLING ffmpeg [{proc.pid}] "{ffmpeg}"')
    vod.terminate()


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

    _event = get_program_id(request.json["channel_id"], request.json.get("url"), cloud)
    _epg, _ = get_epg(request.json["channel_id"], _event["program_id"], cloud)

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
            if (request.json["method"] and request.json["id"]) in _metric:
                request.app.ctx.metrics["RQS_LATENCY"].remove(*_metric)
                break

    log.info(
        f'{request.json["msg"]} [{_event["channel"]}] [{request.json["channel_id"]}] '
        f'[{_event["program_id"]}] [{_event["start"]}] "{_epg["full_title"]}" _ {offset}'
    )


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
    if not _t_timers or _t_timers.done():
        _t_timers = app.add_task(timers_check(delay=5))

    log.debug(f"Reap VOD Child: [{process}]:[{retcode}] DONE")


async def record_program(channel_id, program_id, offset=0, record_time=0, cloud=False, mp4=False, vo=False):
    timestamp = get_epg(channel_id, program_id, cloud)[1]
    filename = get_recording_name(channel_id, timestamp, cloud)
    if await ongoing_vods(channel_id, program_id, filename):
        msg = f'Recording already ongoing: [{channel_id}] [{program_id}] "{filename}"'
        log.warning(msg)
        return msg
    elif _NETWORK_SATURATION:
        return await log_network_saturated()

    port = find_free_port(_IPTV)
    cmd = [sys.executable] if EXT == ".py" else []
    cmd += [f"movistar_vod{EXT}", str(channel_id), str(program_id), "-p", str(port), "-w"]
    cmd += ["-o", filename]
    cmd += ["-s", str(offset)] if offset else []
    cmd += ["-t", str(record_time)] if record_time else []
    cmd += ["--cloud"] if cloud else []
    cmd += ["--mp4"] if mp4 else []
    cmd += ["--vo"] if vo else []

    log.debug('Launching: "%s"' % " ".join(cmd))
    process = await asyncio.create_subprocess_exec(*cmd)
    app.add_task(reap_vod_child(process, filename))


async def reload_epg():
    global _CHANNELS, _CLOUD, _EPGDATA

    if (
        not os.path.exists(CHANNELS)
        and os.path.exists(config_data)
        and os.path.exists(epg_data)
        and os.path.exists(epg_metadata)
        and os.path.exists(GUIDE)
        and os.path.exists(GUIDE + ".gz")
    ):
        log.warning("Missing channel list! Need to download it. Please be patient...")
        cmd = [sys.executable] if EXT == ".py" else []
        cmd += [f"movistar_tvg{EXT}", "--m3u", CHANNELS]
        async with tvgrab_lock:
            task = app.add_task(launch(cmd))
            await task
        check_task(task)
        if not os.path.exists(CHANNELS):
            return app.add_task(cancel_app())
    elif (
        not os.path.exists(config_data)
        or not os.path.exists(epg_data)
        or not os.path.exists(epg_metadata)
        or not os.path.exists(GUIDE)
        or not os.path.exists(GUIDE + ".gz")
    ):
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
            if os.path.exists(epg_metadata):
                os.remove(epg_metadata)
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
            if os.path.exists(epg_data):
                os.remove(epg_data)
            return await reload_epg()

        log.info(f"Channels & EPG Updated => {U7D_URL}/MovistarTV.m3u - {U7D_URL}/guide.xml.gz")

        await update_cloud()
        nr_epg = sum([len(_EPGDATA[channel]) for channel in _EPGDATA])

        log.info(f"Total: {len(_EPGDATA)} Channels & {nr_epg} EPG entries")


async def timers_check(delay=0):
    await asyncio.sleep(delay)

    if not os.path.exists(timers):
        log.debug("timers_check: no timers.conf found")
        return

    nr_procs = len(await ongoing_vods())
    if _NETWORK_SATURATION or nr_procs >= RECORDINGS_THREADS:
        await log_network_saturated(nr_procs)
        return

    if epg_lock.locked():  # If EPG is being updated, wait until it is done
        await epg_lock.acquire()
        epg_lock.release()

    log.info("Processing timers")
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

    def _clean(string):
        return unicodedata.normalize("NFKD", string).encode("ASCII", "ignore").decode("utf8")

    async def _record(cloud=False):
        nonlocal channel_id, channel_name, ongoing, queued, nr_procs, recs, timer_match, timestamp, vo

        filename = get_recording_name(channel_id, timestamp, cloud)
        if filename in ongoing or (channel_id, timestamp) in queued:
            return

        guide = _CLOUD if cloud else _EPGDATA
        duration, pid, title = [guide[channel_id][timestamp][t] for t in ["duration", "pid", "full_title"]]

        if not cloud and timestamp + duration >= _last_epg:
            return

        if channel_id in recs and filename in str(recs[channel_id]):
            return

        if cloud or re.search(_clean(timer_match), _clean(title), re.IGNORECASE):
            if await record_program(channel_id, pid, 0, 0 if cloud else duration, cloud, MP4_OUTPUT, vo):
                return

            log.info(
                f"Found {'Cloud Recording' if cloud else 'MATCH'}: "
                f'[{channel_name}] [{channel_id}] [{pid}] [{timestamp}] "{filename}"'
                f"{' [VO]' if vo else ''}"
            )

            queued.append((channel_id, timestamp))
            await asyncio.sleep(2.5 if not WIN32 else 4)

            nr_procs += 1
            if nr_procs >= RECORDINGS_THREADS:
                await log_network_saturated(nr_procs)
                return -1

    async with recordings_lock:
        recs = _RECORDINGS.copy()

    ongoing = await ongoing_vods(_all=True)  # we want to check against all ongoing vods, also in pp
    log.debug(f"Ongoing VODs: [{ongoing}]")
    queued = []

    for str_channel_id in _timers.get("match", {}):
        channel_id = int(str_channel_id)
        if channel_id not in _EPGDATA:
            log.warning(f"Channel [{channel_id}] not found in EPG")
            continue
        channel_name = _CHANNELS[channel_id]["name"]

        for timer_match in _timers["match"][str_channel_id]:
            fixed_timer = 0
            lang = deflang
            if " ## " in timer_match:
                match_split = timer_match.split(" ## ")
                timer_match = match_split[0]
                for res in match_split[1:3]:
                    if res[0].isnumeric():
                        try:
                            hour, minute = [int(t) for t in res.split(":")]
                            fixed_timer = int(
                                datetime.now().replace(hour=hour, minute=minute, second=0).timestamp()
                            )
                            log.debug(f'[{channel_name}] "{timer_match}" {hour:02}:{minute:02} {fixed_timer}')
                        except ValueError:
                            log.warning(f'Failed to parse [{channel_name}] "{timer_match}" [{res}] correctly')
                    else:
                        lang = res
            vo = lang == "VO"

            timestamps = [ts for ts in reversed(_EPGDATA[channel_id]) if ts < _last_epg]
            if fixed_timer:
                # fixed timers are checked daily, so we want today's and all of last week
                fixed_timestamps = [fixed_timer] if fixed_timer < _last_epg else []
                fixed_timestamps += [fixed_timer - i * 24 * 3600 for i in range(1, 8)]

                found_ts = []
                for ts in timestamps:
                    for fixed_ts in fixed_timestamps:
                        if abs(ts - fixed_ts) <= 900:
                            found_ts.append(ts)
                            break
                timestamps = found_ts

            log.debug(f"Checking timer: [{channel_name}] [{channel_id}] [{_clean(timer_match)}]")
            for timestamp in [
                ts for ts in timestamps if (channel_id not in recs or ts not in recs[channel_id])
            ]:
                if await _record():
                    return

    if sync_cloud:
        vo = _timers["sync_cloud_language"] == "VO" if "sync_cloud_language" in _timers else False
        for channel_id in _CLOUD:
            for timestamp in [
                ts for ts in _CLOUD[channel_id] if (channel_id not in recs or ts not in recs[channel_id])
            ]:
                if await _record(cloud=True):
                    return


async def update_cloud():
    global _CLOUD, _EPGDATA

    try:
        async with aiofiles.open(cloud_data, encoding="utf8") as f:
            clouddata = ujson.loads(await f.read())["data"]

        int_clouddata = {}
        for channel in clouddata:
            int_clouddata[int(channel)] = {}
            for timestamp in clouddata[channel]:
                int_clouddata[int(channel)][int(timestamp)] = clouddata[channel][timestamp]
        _CLOUD = int_clouddata

    except (FileNotFoundError, TypeError, ValueError):
        if os.path.exists(cloud_data):
            os.remove(cloud_data)

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

        episode = meta["episode"] or data.get("episode")
        season = meta["season"] or data.get("season")
        serie = meta["serie"] or data.get("seriesName")

        return {
            "age_rating": data["ageRatingID"],
            "duration": data["duration"],
            "end": int(str(data["endTime"])[:-3]),
            "episode": episode,
            "full_title": meta["full_title"],
            "genre": data["theme"],
            "is_serie": meta["is_serie"],
            "pid": pid,
            "season": season,
            "start": int(timestamp),
            "year": year,
            "serie": serie,
            "serie_id": data.get("seriesID"),
        }

    new_cloud = {}
    for _event in cloud_recordings:
        channel_id = _event["serviceUID"]
        timestamp = int(_event["beginTime"] / 1000)

        if channel_id not in new_cloud:
            new_cloud[channel_id] = {}

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
        for id in new_cloud:
            if set(new_cloud[id]) != set(_CLOUD[id]):
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
        if not os.path.exists(CHANNELS_CLOUD) or not os.path.exists(GUIDE_CLOUD):
            log.warning("Missing Cloud Recordings data! Need to download it. Please be patient...")

        cmd = [sys.executable] if EXT == ".py" else []
        cmd += [f"movistar_tvg{EXT}", "--cloud_m3u", CHANNELS_CLOUD, "--cloud_recordings", GUIDE_CLOUD]
        async with tvgrab_lock:
            task = app.add_task(launch(cmd))
            await task
        check_task(task)

    log.info(f"Cloud Recordings Updated => {U7D_URL}/MovistarTVCloud.m3u - {U7D_URL}/cloud.xml")


async def update_epg(abort_on_error=False):
    global _last_epg, _t_timers
    cmd = [sys.executable] if EXT == ".py" else []
    cmd += [f"movistar_tvg{EXT}", "--m3u", CHANNELS, "--guide", GUIDE]
    for i in range(3):
        async with tvgrab_lock:
            task = app.add_task(launch(cmd))
            await task
        check_task(task)

        if task.result():
            if i < 2:
                await asyncio.sleep(3)
                log.error(f"[{i+2}/3]...")
            elif abort_on_error or not _CHANNELS or not _EPGDATA:
                app.add_task(cancel_app())
        else:
            await reload_epg()
            _last_epg = int(datetime.now().replace(minute=0, second=0).timestamp())
            if RECORDINGS and (not _t_timers or _t_timers.done()):
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
                if len(_logo) and os.path.isfile(_logo[0]):
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
            if WIN32 and os.path.exists(recordings):
                os.remove(recordings)
            os.rename(recordings + ".tmp", recordings)

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
                        dir
                        for dir in os.listdir(RECORDINGS)
                        if re.match(r"^[0-9]+\. ", dir) and os.path.isdir(os.path.join(RECORDINGS, dir))
                    ]
                )
        else:
            topdirs = []

        updated_m3u = False
        for dir in [RECORDINGS] + topdirs:
            log.debug(f'Looking for recordings in "{dir}"')
            files = (
                _files
                if dir == RECORDINGS
                else list(filter(lambda x: x.startswith(os.path.join(RECORDINGS, dir)), _files))
            )
            if not files:
                continue

            if archive is True and dir != RECORDINGS:
                for subdir in os.listdir(os.path.join(RECORDINGS, dir)):
                    subdir = os.path.join(RECORDINGS, dir, subdir)
                    if not os.path.isdir(subdir):
                        continue
                    try:
                        newest = os.path.getmtime(list(filter(lambda x: x.startswith(subdir), files))[-1])
                    except IndexError:
                        continue
                    os.utime(subdir, (-1, newest))
                    if os.path.exists(os.path.join(subdir, "metadata")):
                        os.utime(os.path.join(subdir, "metadata"), (-1, newest))

            m3u = f"#EXTM3U name=\"{'Recordings' if dir == RECORDINGS else dir}\" dlna_extras=mpeg_ps_pal\n"
            if dir == RECORDINGS:
                m3u += _dump_files(reversed(files), latest=True)
                m3u += _dump_files(sorted(files))
                m3u_file = RECORDINGS_M3U
            else:
                m3u += _dump_files(files, channel=True)
                m3u_file = os.path.join(os.path.join(RECORDINGS, dir), f"{dir}.m3u")

            async with aiofiles.open(m3u_file, "w", encoding="utf8") as f:
                await f.write(m3u)

            newest = int(os.path.getmtime(files[-1]))
            [os.utime(file, (-1, newest)) for file in (m3u_file, os.path.join(RECORDINGS, dir))]

            if RECORDINGS_PER_CHANNEL and len(topdirs):
                log.info(f"Wrote m3u [{m3u_file[len(RECORDINGS) + 1 :]}]")

            updated_m3u = True

        if updated_m3u:
            log.info(f"Local Recordings Updated => {U7D_URL}/Recordings.m3u")


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
    _RECORDINGS = {}
    _RECORDINGS_INC = {}

    _last_bw_warning = _last_epg = _t_timers = None

    _conf = mu7d_config()

    logging.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
        level=logging.DEBUG if _conf["DEBUG"] else logging.INFO,
    )
    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)
    logging.getLogger("sanic.error").setLevel(logging.FATAL)
    logging.getLogger("sanic.root").disabled = True

    if not os.getenv("U7D_PARENT"):
        log.critical("Must be run with mu7d")
        sys.exit(1)

    CHANNELS = _conf["CHANNELS"]
    CHANNELS_CLOUD = _conf["CHANNELS_CLOUD"]
    GUIDE = _conf["GUIDE"]
    GUIDE_CLOUD = _conf["GUIDE_CLOUD"]
    IPTV_BW_HARD = _conf["IPTV_BW_HARD"]
    IPTV_BW_SOFT = _conf["IPTV_BW_SOFT"]
    IPTV_IFACE = _conf["IPTV_IFACE"]
    MP4_OUTPUT = _conf["MP4_OUTPUT"]
    RECORDINGS = _conf["RECORDINGS"]
    RECORDINGS_M3U = _conf["RECORDINGS_M3U"]
    RECORDINGS_PER_CHANNEL = _conf["RECORDINGS_PER_CHANNEL"]
    RECORDINGS_THREADS = _conf["RECORDINGS_THREADS"]
    U7D_URL = _conf["U7D_URL"]
    VID_EXT = ".mkv" if not MP4_OUTPUT else ".mp4"

    U7D_PARENT = int(os.getenv("U7D_PARENT"))

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
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
