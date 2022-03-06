#!/usr/bin/env python3

import aiofiles
import aiohttp
import asyncio
import os
import re
import socket
import subprocess  # nosec B404
import sys
import time
import tomli
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectorError
from aiohttp.resolver import AsyncResolver
from datetime import datetime
from glob import glob
from random import randint
from sanic import Sanic, exceptions, response
from sanic_prometheus import monitor
from sanic.log import logger as log, LOGGING_CONFIG_DEFAULTS
from xml.sax.saxutils import unescape  # nosec B406

from version import _version
from vod import (
    MOVISTAR_DNS,
    MVTV_URL,
    TMP_EXT,
    UA,
    WIN32,
    YEAR_SECONDS,
    check_dns,
    find_free_port,
    vod_ongoing,
)


if not WIN32:
    from setproctitle import setproctitle

    setproctitle("movistar_epg")
else:
    import ctypes
    from wmi import WMI

if "LAN_IP" in os.environ:
    SANIC_HOST = os.getenv("LAN_IP")
else:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 53))
    SANIC_HOST = s.getsockname()[0]
    s.close

HOME = os.getenv("HOME", os.getenv("USERPROFILE"))
CHANNELS = os.path.join(HOME, "MovistarTV.m3u")
CHANNELS_CLOUD = os.path.join(HOME, "MovistarTVCloud.m3u")
DEBUG = bool(int(os.getenv("DEBUG", 0)))
GUIDE = os.path.join(HOME, "guide.xml")
GUIDE_CLOUD = os.path.join(HOME, "cloud.xml")
IPTV_BW = int(os.getenv("IPTV_BW", "0"))
IPTV_BW = 85000 if IPTV_BW > 90000 else IPTV_BW
IPTV_IFACE = os.getenv("IPTV_IFACE", None)
MP4_OUTPUT = bool(os.getenv("MP4_OUTPUT", False))
RECORDING_THREADS = int(os.getenv("RECORDING_THREADS", "4"))
RECORDINGS = os.getenv("RECORDINGS", "").rstrip("/").rstrip("\\")
RECORDINGS_M3U = os.path.join(RECORDINGS, "Recordings.m3u")
RECORDINGS_PER_CHANNEL = bool(int(os.getenv("RECORDINGS_PER_CHANNEL", 0)))
SANIC_PORT = int(os.getenv("SANIC_PORT", "8888"))
SANIC_URL = f"http://{SANIC_HOST}:{SANIC_PORT}"
UA_U7D = f"movistar-u7d v{_version} [{sys.platform}]"
VID_EXTS = (".avi", ".mkv", ".mp4", ".mpeg", ".mpg", ".ts")
VOD_EXEC = "vod.exe" if os.path.exists("vod.exe") else "vod.py"

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS["formatters"]["generic"]["format"] = "%(asctime)s [EPG] [%(levelname)s] %(message)s"
LOG_SETTINGS["formatters"]["generic"]["datefmt"] = LOG_SETTINGS["formatters"]["access"][
    "datefmt"
] = "[%Y-%m-%d %H:%M:%S]"

_CHANNELS = {}
_CHILDREN = {}
_CLOUD = {}
_EPGDATA = {}
_RECORDINGS = {}
_RECORDINGS_INC = {}
_SESSION = {}
_SESSION_CLOUD = {}
_TIMERS_ADDED = []

_IPTV = None
_NETWORK_SATURATED = False

app = Sanic("movistar_epg")
app.config.update({"FALLBACK_ERROR_FORMAT": "json", "KEEP_ALIVE_TIMEOUT": YEAR_SECONDS})
flussonic_regex = re.compile(r"\w*-?(\d{10})-?(\d+){0,1}\.?\w*")
title_select_regex = re.compile(r".+ T\d+ .+")
title_1_regex = re.compile(r"(.+(?!T\d)) +T(\d+)(?: *Ep?.? *(\d+))?[ -]*(.*)")
title_2_regex = re.compile(r"(.+(?!T\d))(?: +T(\d+))? *Ep?.? *(\d+)[ -]*(.*)")

cloud_data = os.path.join(HOME, ".xmltv/cache/cloud.json")
config_data = os.path.join(HOME, ".xmltv/cache/config.json")
epg_data = os.path.join(HOME, ".xmltv/cache/epg.json")
epg_metadata = os.path.join(HOME, ".xmltv/cache/epg_metadata.json")
recordings = os.path.join(HOME, "recordings.json")
timers = os.path.join(HOME, "timers.conf")

epg_lock = asyncio.Lock()
children_lock = asyncio.Lock()
recordings_lock = asyncio.Lock()
timers_lock = asyncio.Lock()
tvgrab_lock = asyncio.Lock()

_last_epg = _t_epg = _t_recs = _t_timers = _t_tp = tv_cloud = tvgrab = None


@app.listener("before_server_start")
async def before_server_start(app, loop):
    global RECORDING_THREADS, _IPTV, _SESSION, _SESSION_CLOUD, _t_epg, _t_tp

    banner = f"Movistar U7D - EPG v{_version}"
    log.info("-" * len(banner))
    log.info(banner)
    log.info("-" * len(banner))

    if not WIN32 and IPTV_IFACE:
        import netifaces

        while True:
            try:
                iptv = netifaces.ifaddresses(IPTV_IFACE)[2][0]["addr"]
                log.info(f"IPTV interface: {IPTV_IFACE}")
                break
            except (KeyError, ValueError):
                log.info(f"IPTV interface: waiting for {IPTV_IFACE} to be up...")
                await asyncio.sleep(5)

    while True:
        try:
            _IPTV = check_dns()
            if _IPTV:
                if (not WIN32 and IPTV_IFACE and iptv == _IPTV) or not IPTV_IFACE or WIN32:
                    log.info(f"IPTV address: {_IPTV}")
                    break
                else:
                    log.debug("IPTV address: waiting for interface to be routed...")
        except Exception:
            log.debug("Unable to connect to Movistar DNS")
        await asyncio.sleep(5)

    if not WIN32 and IPTV_BW and IPTV_IFACE:
        log.info(f"Ignoring RECORDING_THREADS: BW {IPTV_BW} kbps / {IPTV_IFACE}")
        _t_tp = app.add_task(network_saturated())
        RECORDING_THREADS = 0

    if not WIN32:
        await asyncio.create_subprocess_exec("pkill", "tv_grab_es_movistartv")

    _SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1),
        json_serialize=ujson.dumps,
    )

    _SESSION_CLOUD = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            keepalive_timeout=YEAR_SECONDS,
            resolver=AsyncResolver(nameservers=[MOVISTAR_DNS]) if not WIN32 else None,
        ),
        headers={"User-Agent": UA},
        json_serialize=ujson.dumps,
    )

    await reload_epg()

    delay = 3600 - (time.localtime().tm_min * 60 + time.localtime().tm_sec)
    _t_epg = asyncio.create_task(every(3600, update_epg, delay))

    if RECORDINGS:
        global _RECORDINGS, _last_epg

        if not _last_epg:
            _last_epg = int(datetime.now().replace(minute=0, second=0).timestamp())

        if not os.path.exists(RECORDINGS):
            os.makedirs(RECORDINGS)
            return

        try:
            async with aiofiles.open(recordings, encoding="utf8") as f:
                recordingsdata = ujson.loads(await f.read())
            int_recordings = {}
            for channel in recordingsdata:
                int_recordings[int(channel)] = {}
                for timestamp in recordingsdata[channel]:
                    recording = recordingsdata[channel][timestamp]
                    try:
                        filename = recording["filename"]
                    except KeyError:
                        log.warning(f'Dropping old style "{recording}" from recordings.json')
                        continue
                    if does_recording_exist(filename):
                        int_recordings[int(channel)][int(timestamp)] = recording
                    else:
                        log.warning(f'Dropping not found "{filename}" from recordings.json')
            _RECORDINGS = int_recordings
        except (TypeError, ValueError) as ex:
            log.error(f"{repr(ex)}")
        except FileNotFoundError:
            pass

        await update_recordings(True)


@app.listener("after_server_start")
async def after_server_start(app, loop):
    if RECORDINGS:
        _ffmpeg = str(await get_ffmpeg_procs())
        for t in glob(f"{RECORDINGS}/**/*{TMP_EXT}*", recursive=True):
            if os.path.basename(t) not in _ffmpeg:
                try:
                    os.remove(t)
                except FileNotFoundError:
                    pass

        if not WIN32:
            async with aiofiles.open("/proc/uptime") as f:
                uptime = int(float((await f.read()).split()[1]))
        else:
            uptime = int(str(ctypes.windll.kernel32.GetTickCount64())[:-3])

        global _t_timers
        async with timers_lock:
            if not _t_timers:
                delay = max(10, 180 - uptime)
                if delay > 10:
                    log.info(f"Waiting {delay}s to check recording timers since the system just booted...")
                _t_timers = asyncio.create_task(timers_check(delay))

        log.info(f"Manual timers check => {SANIC_URL}/timers_check")

    async with aiohttp.ClientSession(headers={"User-Agent": UA_U7D}) as session:
        for i in range(5):
            try:
                await session.get("https://openwrt.marcet.info/u7d/alive")
                break
            except Exception:
                await asyncio.sleep(5)


@app.listener("after_server_stop")
async def after_server_stop(app, loop):
    await _SESSION.close()
    await _SESSION_CLOUD.close()
    for task in [
        _t_epg,
        _t_recs,
        _t_timers,
        _t_tp,
        tv_cloud,
        tvgrab,
    ]:
        try:
            task.cancel()
            await asyncio.wait({task})
        except (AttributeError, ProcessLookupError):
            pass
    async with children_lock:
        for pid in _CHILDREN:
            try:
                _CHILDREN[pid][2].cancel()
                await asyncio.wait({_CHILDREN[pid][2]})
            except (AttributeError, KeyError, ProcessLookupError):
                pass


def does_recording_exist(filename):
    return bool(
        [
            file
            for file in glob(os.path.join(RECORDINGS, f"{filename.replace('[', '?').replace(']', '?')}.*"))
            if os.path.splitext(file)[1] in VID_EXTS
        ]
    )


async def every(timeout, stuff, delay=0):
    await asyncio.sleep(delay)
    while True:
        await asyncio.gather(asyncio.sleep(timeout), stuff())


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


async def get_ffmpeg_procs():
    if WIN32:
        return [
            process.CommandLine
            for process in WMI().Win32_Process(name="ffmpeg.exe")
            if "udp://" in process.CommandLine
        ]

    p = await asyncio.create_subprocess_exec("pgrep", "-af", "ffmpeg.+udp://", stdout=asyncio.subprocess.PIPE)
    stdout, _ = await p.communicate()
    return [t.rstrip().decode() for t in stdout.splitlines()]


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


def get_recording_path(channel_id, timestamp):
    daily_news_program = (
        not _EPGDATA[channel_id][timestamp]["is_serie"] and _EPGDATA[channel_id][timestamp]["genre"] == "06"
    )

    if RECORDINGS_PER_CHANNEL:
        path = os.path.join(RECORDINGS, get_channel_dir(channel_id))
    else:
        path = RECORDINGS
    if _EPGDATA[channel_id][timestamp]["serie"]:
        path = os.path.join(path, get_safe_filename(_EPGDATA[channel_id][timestamp]["serie"]))
    elif daily_news_program:
        path = os.path.join(path, get_safe_filename(_EPGDATA[channel_id][timestamp]["full_title"]))
    path = path.rstrip(".").rstrip(",")

    filename = os.path.join(path, get_safe_filename(_EPGDATA[channel_id][timestamp]["full_title"]))
    if daily_news_program:
        filename += f' - {datetime.fromtimestamp(timestamp).strftime("%Y%m%d")}'

    return (path, filename)


def get_safe_filename(filename):
    filename = filename.replace(":", ",").replace("...", "…").replace("(", "[").replace(")", "]")
    keepcharacters = (" ", ",", ".", "_", "-", "¡", "!", "[", "]")
    return "".join(c for c in filename if c.isalnum() or c in keepcharacters).rstrip()


def get_title_meta(title, serie_id=None):
    try:
        _t = unescape(title).replace("\n", " ").replace("\r", " ")
    except TypeError:
        _t = title.replace("\n", " ").replace("\r", " ")
    full_title = _t.replace(" / ", ": ").replace("/", "")

    if title_select_regex.match(full_title):
        x = title_1_regex.search(full_title)
    else:
        x = title_2_regex.search(full_title)

    is_serie = False
    season = episode = 0
    serie = episode_title = ""
    if x and x.groups():
        _x = x.groups()
        serie = (
            _x[0].strip()
            if _x[0]
            else full_title.split(" - ")[0].strip()
            if " - " in full_title
            else full_title
        )
        season = int(_x[1]) if _x[1] else season
        episode = int(_x[2]) if _x[2] else episode
        episode_title = _x[3].strip() if _x[3] else episode_title
        if episode_title and "-" in episode_title:
            episode_title = episode_title.replace("- ", "-").replace(" -", "-").replace("-", " - ")
        is_serie = serie and episode
        if ": " in serie and episode and not episode_title:
            episode_title = serie.split(":")[1].strip()
            serie = serie.split(":")[0].strip()
        if serie and season and episode:
            full_title = "%s S%02dE%02d" % (serie, season, episode)
            if episode_title:
                full_title += f" - {episode_title}"
    elif serie_id:
        is_serie = True
        if " - " in full_title:
            serie, episode_title = full_title.split(" - ", 1)
        else:
            serie = full_title

    return {
        "full_title": full_title,
        "serie": serie,
        "season": season,
        "episode": episode,
        "episode_title": episode_title,
        "is_serie": is_serie,
    }


@app.get("/channels/")
async def handle_channels(request):
    return response.json(_CHANNELS)


@app.get("/program_id/<channel_id:int>/<url>")
async def handle_program_id(request, channel_id, url):
    try:
        return response.json(get_program_id(channel_id, url, bool(request.args.get("cloud"))))
    except (AttributeError, KeyError):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")


@app.route("/program_name/<channel_id:int>/<program_id:int>", methods=["GET", "OPTIONS", "PUT"])
async def handle_program_name(request, channel_id, program_id, missing=0):
    global _RECORDINGS
    try:
        _epg, timestamp = get_epg(channel_id, program_id)
    except TypeError:
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")

    path, filename = get_recording_path(channel_id, timestamp)

    if request and request.method == "GET":
        return response.json(
            {
                "status": "OK",
                "path": path,
                "filename": filename,
            },
            ensure_ascii=False,
        )

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    fname = filename[len(RECORDINGS) + 1 :]
    log_suffix = '[%s] [%s] [%s] [%s] "%s"' % (
        _CHANNELS[channel_id]["name"],
        channel_id,
        timestamp,
        program_id,
        fname,
    )

    global _TIMERS_ADDED
    missing = request.args.get("missing", 0) if request else missing
    if missing:
        if channel_id not in _RECORDINGS_INC:
            _RECORDINGS_INC[channel_id] = {}
        if program_id not in _RECORDINGS_INC[channel_id]:
            _RECORDINGS_INC[channel_id][program_id] = []
        _RECORDINGS_INC[channel_id][program_id].append(missing)
        _t = _RECORDINGS_INC[channel_id][program_id]
        if request and request.method == "OPTIONS" and len(_t) > 2 and _t[-3] == _t[-2] and _t[-2] == _t[-1]:
            log.warning(f"Recording Incomplete KEEP: {log_suffix}: {_t}")
            return response.json({"status": "Recording Incomplete KEEP"}, status=200)
        elif request and request.method == "PUT" and len(_t) > 3 and _t[-4] == _t[-3] and _t[-3] == _t[-2]:
            log.warning(f"Recording Incomplete KEPT: {log_suffix}: {_t}")
            del _RECORDINGS_INC[channel_id][program_id]
        else:
            log.error(f"Recording Incomplete RETRY: {log_suffix}: {_t}")
            try:
                _TIMERS_ADDED.remove(fname)
            except ValueError:
                pass
            return response.json({"status": "Recording Incomplete RETRY"}, status=201)

    try:
        _TIMERS_ADDED.remove(fname)
    except ValueError:
        pass

    log.debug(f"Checking for {fname}")
    if does_recording_exist(fname):
        async with recordings_lock:
            if channel_id not in _RECORDINGS:
                _RECORDINGS[channel_id] = {}
            _RECORDINGS[channel_id][timestamp] = {"filename": fname}

        global _t_recs
        _t_recs = asyncio.create_task(update_recordings(channel_id))

        log.info(f"Recording ARCHIVED: {log_suffix}")
        return response.json(
            {
                "status": "Recorded OK",
                "full_title": _epg["full_title"],
            },
            ensure_ascii=False,
        )
    else:
        log.error(f"Recording NOT ARCHIVED: {log_suffix}")
        return response.json(
            {
                "status": "Recording Failed",
                "full_title": _epg["full_title"],
            },
            ensure_ascii=False,
            status=201,
        )


@app.post("/prom_event/add")
async def handle_prom_event_add(request):
    try:
        cloud = request.json["cloud"] if "cloud" in request.json else False
        _event = get_program_id(
            request.json["channel_id"], request.json["url"] if "url" in request.json else None, cloud
        )
        _epg, _ = get_epg(request.json["channel_id"], _event["program_id"], cloud)
        _offset = "[%d/%d]" % (_event["offset"], _event["duration"])
        request.app.ctx.metrics["RQS_LATENCY"].labels(
            request.json["method"],
            request.json["endpoint"] + _epg["full_title"] + f" _ {_offset}",
            request.json["id"],
        ).observe(float(request.json["lat"]))
        log.info(
            '%s [%s] [%s] [%s] "%s" _ %s'
            % (
                request.json["msg"],
                _event["channel"],
                _event["start"],
                _event["program_id"],
                _epg["full_title"],
                _offset,
            )
        )
    except KeyError:
        return response.empty(404)
    return response.empty(200)


@app.post("/prom_event/remove")
async def handle_prom_event_remove(request):
    try:
        cloud = request.json["cloud"] if "cloud" in request.json else False
        _event = get_program_id(
            request.json["channel_id"], request.json["url"] if "url" in request.json else None, cloud
        )
        _epg, _ = get_epg(request.json["channel_id"], _event["program_id"], cloud)
        _offset = "[%d/%d]" % (_event["offset"], _event["duration"])
        if request.json["method"] == "live":
            found = False
            for _metric in request.app.ctx.metrics["RQS_LATENCY"]._metrics:
                if request.json["method"] in _metric and str(request.json["id"]) in _metric:
                    found = True
                    break
            if found:
                request.app.ctx.metrics["RQS_LATENCY"].remove(*_metric)
        else:
            request.app.ctx.metrics["RQS_LATENCY"].remove(
                request.json["method"],
                request.json["endpoint"] + _epg["full_title"] + f" _ {_offset}",
                request.json["id"],
            )
            _offset = "[%d/%d]" % (_event["offset"] + request.json["offset"], _event["duration"])
        log.info(
            '%s [%s] [%s] [%s] "%s" _ %s'
            % (
                request.json["msg"],
                _event["channel"],
                _event["start"],
                _event["program_id"],
                _epg["full_title"],
                _offset,
            )
        )
    except KeyError:
        return response.empty(404)
    return response.empty(200)


@app.get("/record/<channel_id:int>/<url>")
async def handle_record_program(request, channel_id, url):
    cloud = bool(request.args.get("cloud"))
    mp4 = bool(request.args.get("mp4"))
    vo = bool(request.args.get("vo"))
    channel, program_id, start, duration, offset = get_program_id(channel_id, url, cloud).values()
    if request.args.get("time"):
        record_time = int(request.args.get("time"))
    else:
        record_time = duration - offset

    msg = await record_program(channel_id, program_id, offset, record_time, cloud, mp4, vo)
    if msg:
        log.warning(msg)
        raise exceptions.ServiceUnavailable(msg)

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
    return await reload_epg()


@app.get("/timers_check")
async def handle_timers_check(request):
    global _t_timers

    if not RECORDINGS:
        return response.json({"status": "RECORDINGS not configured"}, 404)

    async with timers_lock:
        if _t_timers and not _t_timers.done():
            raise exceptions.ServiceUnavailable("Busy processing timers")

        _t_timers = asyncio.create_task(timers_check())

    return response.json({"status": "Timers check queued"}, 200)


async def network_saturated():
    global _NETWORK_SATURATED
    cur = last = 0
    iface_rx = f"/sys/class/net/{IPTV_IFACE}/statistics/rx_bytes"
    while True:
        async with aiofiles.open(iface_rx) as f:
            cur = int((await f.read())[:-1])
        if last:
            tp = int((cur - last) * 8 / 1000 / 3)
            _NETWORK_SATURATED = tp > IPTV_BW
        last = cur
        await asyncio.sleep(3)


async def reap_vod_child(p):
    while True:
        try:
            retcode = p.wait(timeout=0.001)
            log.debug(f"reap_vod_child: [{p.pid}]:{retcode}")
            return await recording_cleanup(p.pid, retcode)
        except subprocess.TimeoutExpired:
            await asyncio.sleep(1)


async def record_program(channel_id, program_id, offset=0, record_time=0, cloud=False, mp4=False, vo=False):
    if _NETWORK_SATURATED:
        return "Network Saturated"
    if await vod_ongoing(channel_id, program_id):
        return "Recording already ongoing"

    client_port = find_free_port(_IPTV)
    cmd = f"{VOD_EXEC} {channel_id} {program_id} -p {client_port}"
    if offset:
        cmd += f" -s {offset}"
    if record_time:
        cmd += f" -t {record_time}"
    if cloud:
        cmd += " --cloud"
    if mp4:
        cmd += " --mp4"
    if vo:
        cmd += " --vo"
    cmd += " -w"

    log.debug(f"Launching {cmd}")
    if not WIN32:
        global _CHILDREN
        p = subprocess.Popen(cmd.split())  # nosec B603
        async with children_lock:
            _CHILDREN[p.pid] = (cmd, (client_port, channel_id, program_id), app.add_task(reap_vod_child(p)))
    else:
        subprocess.Popen(cmd.split())  # nosec B603


async def recording_cleanup(pid, status):
    global _CHILDREN, _t_timers
    async with children_lock:
        if abs(status) == 15:
            client_port, channel_id, program_id = _CHILDREN[pid][1]
        del _CHILDREN[pid]

    if abs(status) == 15:
        log.debug(f"recording_cleanup: [{pid}]:{status} {channel_id} {program_id}")
        subprocess.run(["pkill", "-f", f"ffmpeg .+ -i udp://@{_IPTV}:{client_port}"])  # nosec B603, B607
        await handle_program_name(None, channel_id, program_id, missing=randint(1, 9999))  # nosec B311

    if not _NETWORK_SATURATED:
        async with timers_lock:
            if _t_timers and _t_timers.done():
                _t_timers = asyncio.create_task(timers_check(delay=3))

    log.debug(f"recording_cleanup: [{pid}]:{status} DONE")


async def reload_epg():
    global _CHANNELS, _CLOUD, _EPGDATA, tvgrab

    if (
        not os.path.exists(CHANNELS)
        and os.path.exists(config_data)
        and os.path.exists(epg_data)
        and os.path.exists(epg_metadata)
        and os.path.exists(GUIDE)
    ):
        log.warning("Missing channel list! Need to download it. Please be patient...")
        async with tvgrab_lock:
            tvgrab = await asyncio.create_subprocess_exec(
                "tv_grab_es_movistartv",
                "--m3u",
                CHANNELS,
            )
            await tvgrab.wait()
    elif (
        not os.path.exists(config_data)
        or not os.path.exists(epg_data)
        or not os.path.exists(epg_metadata)
        or not os.path.exists(GUIDE)
    ):
        log.warning("Missing channels data! Need to download it. Please be patient...")
        return await update_epg()
    elif int(os.path.getmtime(GUIDE)) < int(datetime.now().replace(minute=0, second=0).timestamp()):
        log.warning("Guide too old. Need to update it. Please be patient...")
        return await update_epg()

    async with epg_lock:
        try:
            async with aiofiles.open(epg_metadata, encoding="utf8") as f:
                metadata = ujson.loads(await f.read())["data"]
            async with aiofiles.open(config_data, encoding="utf8") as f:
                packages = ujson.loads(await f.read())["data"]["tvPackages"]
            channels = metadata["channels"]
            services = {}
            for package in packages.split("|") if packages != "ALL" else metadata["packages"]:
                services = {**services, **metadata["packages"][package]["services"]}
            int_channels = {}
            for channel in [chan for chan in channels if chan in services]:
                int_channels[int(channel)] = channels[channel]
                int_channels[int(channel)]["id"] = int(channels[channel]["id"])
                int_channels[int(channel)]["number"] = int(services[channel])
                if "replacement" in channels[channel]:
                    int_channels[int(channel)]["replacement"] = int(channels[channel]["replacement"])
            _CHANNELS = int_channels
            log.info(f"Loaded Channels metadata => {SANIC_URL}/MovistarTV.m3u")
        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f"Failed to load Channels metadata {repr(ex)}")
            if os.path.exists(epg_metadata):
                os.remove(epg_metadata)
            return await reload_epg()

        await update_cloud()

        try:
            async with aiofiles.open(epg_data, encoding="utf8") as f:
                epgdata = ujson.loads(await f.read())["data"]
            int_epgdata = {}
            for channel in epgdata:
                int_epgdata[int(channel)] = {}
                for timestamp in epgdata[channel]:
                    int_epgdata[int(channel)][int(timestamp)] = epgdata[channel][timestamp]
            _EPGDATA = int_epgdata
            log.info(f"Loaded EPG data => {SANIC_URL}/guide.xml.gz")
        except (FileNotFoundError, TypeError, ValueError) as ex:
            log.error(f"Failed to load EPG data {repr(ex)}")
            if os.path.exists(epg_data):
                os.remove(epg_data)
            return await reload_epg()

        log.info(f"Total Channels: {len(_EPGDATA)}")
        nr_epg = 0
        for channel in _EPGDATA:
            nr_epg += (
                len(set(_EPGDATA[channel]) - (set(_CLOUD[channel]) - set(_EPGDATA[channel])))
                if channel in _CLOUD
                else len(_EPGDATA[channel])
            )
        log.info(f"Total EPG entries: {nr_epg}")
        log.info("EPG Updated")

    return response.json({"status": "EPG Updated"}, 200)


async def timers_check(delay=0):
    await asyncio.sleep(delay)

    if not os.path.exists(timers) or _NETWORK_SATURATED:
        return

    _ffmpeg = await get_ffmpeg_procs()
    nr_procs = len(_ffmpeg)
    if RECORDING_THREADS and not nr_procs < RECORDING_THREADS:
        return

    global _TIMERS_ADDED
    log.info("Processing timers")
    try:
        async with aiofiles.open(timers, encoding="utf8") as f:
            try:
                _timers = tomli.loads(await f.read())
            except ValueError:
                _timers = ujson.loads(await f.read())
    except (TypeError, ValueError) as ex:
        log.error(f"handle_timers: {repr(ex)}")
        return

    deflang = (
        _timers["language"]["default"] if ("language" in _timers and "default" in _timers["language"]) else ""
    )
    for channel_id in _timers["match"]:
        channel_id = int(channel_id)
        if channel_id not in _EPGDATA:
            log.info(f"Channel [{channel_id}] not found in EPG")
            continue
        channel_name = _CHANNELS[channel_id]["name"]

        for timer_match in _timers["match"][str(channel_id)]:
            fixed_timer = 0
            if " ## " in timer_match:
                match_split = timer_match.split(" ## ")
                timer_match = match_split[0]
                for rest in match_split[1:3]:
                    if rest[0].isnumeric():
                        try:
                            hour, minute = [int(t) for t in rest.split(":")]
                            fixed_timer = int(
                                datetime.now().replace(hour=hour, minute=minute, second=0).timestamp()
                            )
                            log.debug(f'[{channel_name}] "{timer_match}" {hour:02}:{minute:02} {fixed_timer}')
                        except ValueError:
                            pass
                    else:
                        lang = rest
            else:
                lang = deflang
            vo = lang == "VO"

            timestamps = [ts for ts in reversed(_EPGDATA[channel_id]) if ts < _last_epg]
            if fixed_timer:
                # fixed timers are checked daily, so we want today's and all of last week
                fixed_timestamps = [fixed_timer] if fixed_timer < _last_epg else []
                fixed_timestamps += [fixed_timer - i * 24 * 3600 for i in range(1, 8)]
                log.debug(f'[{channel_name}] "{timer_match}" wants {fixed_timestamps}')

                found_ts = []
                for ts in timestamps:
                    for fixed_ts in fixed_timestamps:
                        if abs(ts - fixed_ts) <= 900:
                            found_ts.append(ts)
                            break
                timestamps = found_ts
                log.debug(f"[{channel_name}] has {timestamps}")

            for timestamp in timestamps:
                _, filename = get_recording_path(channel_id, timestamp)
                fname, name = filename[len(RECORDINGS) + 1 :], os.path.basename(filename)
                duration, pid, title = [
                    _EPGDATA[channel_id][timestamp][t] for t in ["duration", "pid", "full_title"]
                ]
                if timestamp + duration >= _last_epg:
                    continue
                if channel_id in _RECORDINGS and (name or title) in str(_RECORDINGS[channel_id]):
                    continue
                if re.match(timer_match, title) and fname not in _TIMERS_ADDED and name not in str(_ffmpeg):
                    cloud = channel_id in _CLOUD and timestamp in _CLOUD[channel_id]
                    if await record_program(channel_id, pid, 0, duration, cloud, MP4_OUTPUT, vo):
                        if _NETWORK_SATURATED:
                            return
                        else:
                            break
                    log.info(
                        f'Found MATCH: [{channel_name}] [{channel_id}] [{timestamp}] [{pid}] "{fname}"'
                        f'{" [VO]" if vo else ""}'
                    )
                    _TIMERS_ADDED.append(fname)
                    nr_procs += 1
                    await asyncio.sleep(3)
                    if _NETWORK_SATURATED or (RECORDING_THREADS and not nr_procs < RECORDING_THREADS):
                        log.info(f"Already recording {nr_procs} streams")
                        return


async def update_cloud():
    global _CLOUD, _EPGDATA, cloud_data, tv_cloud

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
        async with _SESSION_CLOUD.get(
            f"{MVTV_URL}?action=recordingList&mode=0&state=2&firstItem=0&numItems=999"
        ) as r:
            cloud_recordings = (await r.json())["resultData"]["result"]
    except (ClientConnectorError, ConnectionRefusedError, KeyError):
        cloud_recordings = None
    if not cloud_recordings:
        log.info("No cloud recordings found")
        return

    new_cloud = {}
    for _event in cloud_recordings:
        channel_id = _event["serviceUID"]
        _start = int(_event["beginTime"] / 1000)
        if channel_id not in new_cloud:
            new_cloud[channel_id] = {}
        if _start not in new_cloud[channel_id]:
            if channel_id in _EPGDATA and _start in _EPGDATA[channel_id]:
                new_cloud[channel_id][_start] = _EPGDATA[channel_id][_start]
            elif channel_id in _CLOUD and _start in _CLOUD[channel_id]:
                new_cloud[channel_id][_start] = _CLOUD[channel_id][_start]
            else:
                product_id = _event["productID"]
                try:
                    async with _SESSION_CLOUD.get(
                        f"{MVTV_URL}?action=epgInfov2&" f"productID={product_id}&channelID={channel_id}"
                    ) as r:
                        _data = (await r.json())["resultData"]
                        year = _data["productionDate"] if "productionDate" in _data else None
                    async with _SESSION_CLOUD.get(
                        f"{MVTV_URL}?action=getRecordingData&"
                        f"extInfoID={product_id}&channelID={channel_id}&mode=1"
                    ) as r:
                        _data = (await r.json())["resultData"]
                    meta_data = get_title_meta(
                        _data["name"], _data["seriesID"] if "seriesID" in _data else None
                    )
                    new_cloud[channel_id][_start] = {
                        "age_rating": _data["ageRatingID"],
                        "duration": _data["duration"],
                        "end": int(str(_data["endTime"])[:-3]),
                        "episode": meta_data["episode"]
                        if meta_data["episode"]
                        else _data["episode"]
                        if "episode" in _data
                        else None,
                        "full_title": meta_data["full_title"],
                        "genre": _data["theme"],
                        "is_serie": meta_data["is_serie"],
                        "pid": product_id,
                        "season": meta_data["season"]
                        if meta_data["season"]
                        else _data["season"]
                        if "season" in _data
                        else None,
                        "start": int(_start),
                        "year": year,
                        "serie": meta_data["serie"]
                        if meta_data["serie"]
                        else _data["seriesName"]
                        if "seriesName" in _data
                        else None,
                        "serie_id": _data["seriesID"] if "seriesID" in _data else None,
                    }
                    if meta_data["episode_title"]:
                        new_cloud[channel_id][_start]["episode_title"] = meta_data["episode_title"]
                except Exception as ex:
                    log.warning(f"{channel_id} {product_id} not located in Cloud {repr(ex)}")

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
        tv_cloud = await asyncio.create_subprocess_exec(
            "tv_grab_es_movistartv",
            "--cloud_m3u",
            CHANNELS_CLOUD,
            "--cloud_recordings",
            GUIDE_CLOUD,
        )
        await tv_cloud.wait()

    log.info(
        f"{'Updated' if updated else 'Loaded'} Cloud Recordings data"
        f"=> {SANIC_URL}/MovistarTVCloud.m3u & {SANIC_URL}/cloud.xml"
    )


async def update_epg():
    global _last_epg, _t_timers, tvgrab
    for i in range(1, 6):
        async with tvgrab_lock:
            tvgrab = await asyncio.create_subprocess_exec(
                "tv_grab_es_movistartv",
                "--m3u",
                CHANNELS,
                "--guide",
                GUIDE,
            )
            await tvgrab.wait()

        if tvgrab.returncode:
            if i < 5:
                log.error(f"Waiting 15s before trying to update EPG again [{i+1}/5]...")
                await asyncio.sleep(15)
        else:
            await reload_epg()
            if RECORDINGS:
                async with timers_lock:
                    if _t_timers and not _t_timers.done():
                        await asyncio.wait(_t_timers)
                    _last_epg = int(datetime.now().replace(minute=0, second=0).timestamp())
                    _t_timers = asyncio.create_task(timers_check())
            break


async def update_recordings(archive=None):
    def dump_files(files, channel=False, latest=False):
        m3u = ""
        for file in files:
            filename, ext = os.path.splitext(file)
            relname = filename[len(RECORDINGS) + 1 :]
            if os.path.exists(filename + ".jpg"):
                logo = relname + ".jpg"
            else:
                _logo = glob(f"{filename}*.jpg")
                if len(_logo) and os.path.isfile(_logo[0]):
                    logo = _logo[0][len(RECORDINGS) + 1 :]
                else:
                    logo = ""
            path, name = [t.replace(",", ":").replace(" - ", ": ") for t in os.path.split(relname)]
            m3u += '#EXTINF:-1 group-title="'
            m3u += '# Recientes"' if latest else f'{path}"' if path else '#"'
            m3u += f' tvg-logo="{SANIC_URL}/recording/?' if logo else ""
            m3u += (urllib.parse.quote(logo) + '"') if logo else ""
            m3u += f",{path} - " if (channel and path) else ","
            m3u += f"{name}\n{SANIC_URL}/recording/?"
            m3u += urllib.parse.quote(relname + ext) + "\n"
        return m3u

    async def find_videos(path):
        while True:
            try:
                files = [
                    file
                    for file in glob(f"{path}/**", recursive=True)
                    if os.path.splitext(file)[1] in VID_EXTS
                ]
                files.sort(key=os.path.getmtime)
                return files
            except FileNotFoundError:
                await asyncio.sleep(1)

    async with recordings_lock:
        if archive:
            async with aiofiles.open(recordings, "w", encoding="utf8") as f:
                await f.write(ujson.dumps(_RECORDINGS, ensure_ascii=False, indent=4, sort_keys=True))

        if RECORDINGS_PER_CHANNEL:
            if archive and not isinstance(archive, bool):
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

        _files = await find_videos(RECORDINGS)
        for dir in [RECORDINGS] + topdirs:
            log.debug(f'Looking for recordings in "{dir}"')
            files = (
                _files
                if dir == RECORDINGS
                else [file for file in _files if file.startswith(os.path.join(RECORDINGS, dir))]
            )
            m3u = f"#EXTM3U name=\"{'Recordings' if dir == RECORDINGS else dir}\" dlna_extras=mpeg_ps_pal\n"
            if dir == RECORDINGS:
                m3u += dump_files(reversed(files), latest=True)
                m3u += dump_files(sorted(files))
                m3u_file = RECORDINGS_M3U
            else:
                m3u += dump_files(files, channel=True)
                m3u_file = os.path.join(os.path.join(RECORDINGS, dir), f"{dir}.m3u")

            async with aiofiles.open(m3u_file, "w", encoding="utf8") as f:
                await f.write(m3u)
            if RECORDINGS_PER_CHANNEL and len(topdirs):
                log.info(f"Wrote m3u [{m3u_file[len(RECORDINGS) + 1 :]}]")

        log.info(f"Local Recordings Updated => {SANIC_URL}/Recordings.m3u")


if __name__ == "__main__":
    try:
        monitor(
            app,
            is_middleware=False,
            latency_buckets=[1.0],
            mmc_period_sec=None,
            multiprocess_mode="livesum",
        ).expose_endpoint()
        app.run(host="127.0.0.1", port=8889, access_log=False, auto_reload=False, debug=DEBUG, workers=1)
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        log.critical(f"{repr(ex)}")
        sys.exit(1)
