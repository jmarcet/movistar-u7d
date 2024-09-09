import asyncio
import ctypes
import gc
import json
import logging
import os
import re
import shutil
import sys
import time
import unicodedata
import urllib.parse
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL, PIPE
from collections import defaultdict, deque, namedtuple
from contextlib import closing, suppress
from datetime import date, datetime, timedelta
from glob import glob, iglob
from itertools import chain
from json import JSONDecodeError
from operator import itemgetter
from socket import AF_INET, SOCK_DGRAM, socket

import aiohttp
import asyncstdlib as a
import tomli
import ujson
import xmltodict
from aiofiles import open as async_open, os as aio_os
from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from psutil import AccessDenied, NoSuchProcess, Process, boot_time, process_iter
from sanic import Sanic
from sanic.mixins.startup import StartupMixin
from tomli import TOMLDecodeError
from xmltodict import ParsingInterrupted

from mu7d_cfg import (
    CONF,
    DATEFMT,
    DIV_ONE,
    DIV_TWO,
    DROP_KEYS,
    END_POINTS,
    END_POINTS_FILE,
    EXT,
    FMT,
    IPTV_DNS,
    NFO_EXT,
    UA,
    VERSION,
    VID_EXTS,
    VID_EXTS_KEEP,
    WIN32,
    XML_INT_KEYS,
    YEAR_SECONDS,
)

if WIN32:
    # Disable forced WindowsSelectorEventLoopPolicy, which is non compatible w/create_subprocess_exec()
    def _setup_loop(self) -> None: ...  # pylint: disable=unused-argument

    StartupMixin.setup_loop = _setup_loop

app = Sanic("movistar-u7d")
_g = app.ctx

Channel = namedtuple("Channel", "address, name, number, port")
Program = namedtuple("Program", "pid, duration, full_title, genre, serie")
ProgramVOD = namedtuple("ProgramVOD", "ch_name, pid, full_title, start, duration, offset")
PromEvent = namedtuple("PromEvent", "ch_id, endpoint, id, lat, method, msg, offset, url", defaults=(0, None))
Recording = namedtuple("Recording", "filename, duration")
Timer = namedtuple("Timer", "ch_id, ts, cloud, comskip, delay, keep, repeat, vo")

epg_lock = asyncio.Lock()
network_bw_lock = asyncio.Lock()
recordings_lock = asyncio.Lock()
timers_lock = asyncio.Lock()
tvgrab_lock = asyncio.Lock()
tvgrab_local_lock = asyncio.Lock()

anchored_regex = re.compile(r"^(.+) - \d{8}_\d{4}$")
channeldir_regex = re.compile(r"^(\d{3})\. (.+)$")
flussonic_regex = re.compile(r"\w*-?(\d{10})-?(\d+){0,1}\.?\w*")

log = logging.getLogger("U7D")

logging.getLogger("asyncio").setLevel(logging.FATAL)
logging.getLogger("filelock").setLevel(logging.FATAL)
logging.getLogger("sanic.error").setLevel(logging.FATAL)
logging.getLogger("sanic.root").disabled = True
logging.getLogger("sanic.server").disabled = True

logging.basicConfig(datefmt=DATEFMT, format=FMT, level=CONF.get("DEBUG", True) and logging.DEBUG or logging.INFO)


class IPTVNetworkError(Exception):
    """Connectivity with Movistar IPTV network error"""


class LocalNetworkError(Exception):
    """Local network error"""


async def add_prom_event(event, cloud=False, local=False, p_vod=None):
    await asyncio.sleep(0.05)
    try:
        await prom_event(event, "add", cloud, local, p_vod)
        await asyncio.sleep(YEAR_SECONDS)
    except CancelledError:
        await prom_event(event._replace(offset=int(time.time() - event.id)), "remove", cloud, local, p_vod)


async def alive():
    ua_u7d = f"movistar-u7d v{VERSION} [{sys.platform}] [{EXT}] [{_g._IPTV}]"
    async with aiohttp.ClientSession(headers={"User-Agent": ua_u7d}) as session:
        for _ in range(10):
            try:
                await session.get("https://openwrt.marcet.info/u7d/alive")
                break
            except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
                await asyncio.sleep(6)


async def create_covers_cache():
    log.info("Recreating covers cache")

    covers_path = os.path.join(_g.RECORDINGS_TMP, "covers")
    if await aio_os.path.exists(covers_path):
        shutil.rmtree(covers_path)
    await aio_os.makedirs(covers_path)

    for nfo in iglob(f"{_g.RECORDINGS}/[0-9][0-9][0-9]. */**/*{NFO_EXT}", recursive=True):
        cover = nfo.replace(NFO_EXT, ".jpg")
        fanart = os.path.join(
            os.path.dirname(cover), "metadata", os.path.basename(cover).replace(".jpg", "-fanart.jpg")
        )

        if await aio_os.path.exists(fanart):
            cover = fanart
        elif not await aio_os.path.exists(cover):
            continue

        cached_cover = cover.replace(_g.RECORDINGS, covers_path)
        await aio_os.makedirs(os.path.dirname(cached_cover), exist_ok=True)
        shutil.copy2(cover, cached_cover)

    log.info("Covers cache recreated")


async def does_recording_exist(filename):
    return await aio_os.path.exists(os.path.join(_g.RECORDINGS, filename + _g.VID_EXT))


def find_free_port(iface=""):
    with closing(socket(AF_INET, SOCK_DGRAM)) as sock:
        sock.bind((iface, 0))
        return sock.getsockname()[1]


def freemem():
    gc.collect()
    if not WIN32:
        ctypes.CDLL("libc.so.6").malloc_trim(0)


def get_channel_dir(channel_id):
    return "%03d. %s" % (_g._CHANNELS[channel_id].number, _g._CHANNELS[channel_id].name)


def get_channel_id(channel_name):
    res = [
        channel_id
        for channel_id in _g._CHANNELS
        if channel_name.lower() in _g._CHANNELS[channel_id].name.lower().replace(" ", "").replace(".", "")
    ]
    return res[0] if len(res) == 1 else None


async def get_end_point():
    end_points = END_POINTS
    ep_path = os.path.join(CONF["CACHE_DIR"], END_POINTS_FILE)
    if await aio_os.path.exists(ep_path):
        try:
            async with async_open(ep_path) as f:
                end_points = ujson.loads(await f.read())["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError):
            pass

    for end_point in end_points:
        async with aiohttp.ClientSession(headers={"User-Agent": UA}) as session:
            try:
                async with session.get(end_point):
                    return end_point + "/appserver/mvtv.do"
            except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
                pass
            finally:
                await session.close()

    log.critical("IPTV de Movistar no detectado")


def get_epg(channel_id, program_id, cloud=False):
    guide = _g._CLOUD if cloud else _g._EPGDATA
    if channel_id not in guide:
        log.error(f"Not Found: {channel_id=}")
        return (None, None)

    for timestamp in filter(lambda ts: guide[channel_id][ts].pid == program_id, guide[channel_id]):
        return timestamp, guide[channel_id][timestamp]

    log.error(f"Not Found: {channel_id=} {program_id=}")
    return (None, None)


def get_iptv_ip():
    with closing(socket(AF_INET, SOCK_DGRAM)) as sock:
        sock.settimeout(1)
        try:
            sock.connect((IPTV_DNS, 53))
            return sock.getsockname()[0]
        except OSError as ex:
            raise IPTVNetworkError("Imposible conectar con DNS de Movistar IPTV") from ex


async def get_local_info(channel, timestamp, path, _log=None, extended=False):
    nfo_file = path + NFO_EXT
    if not await aio_os.path.exists(nfo_file):
        return {}
    try:
        async with async_open(nfo_file, encoding="utf-8") as f:
            nfo = xmltodict.parse(await f.read(), postprocessor=pp_xml)["metadata"]
        if extended:
            _match = re.search(r"^(.+) (?:S\d+E\d+|Ep[isode.]+ \d+)", nfo.get("originalName", nfo["name"]))
            serie = _match.groups()[0] if _match else nfo.get("seriesName", "")
            nfo.update({"full_title": nfo.get("originalName", nfo["name"]), "serie": serie})
        return nfo
    except (FileNotFoundError, ParsingInterrupted, OSError, PermissionError, TypeError, ValueError) as ex:
        (_log or log).error(
            f'Cannot get extended local metadata: [{channel}] [{timestamp}] "{nfo_file=}" => {repr(ex)}'
        )
        return {}


def get_path(filename, bare=False):
    return os.path.join(_g.RECORDINGS, filename + (_g.VID_EXT if not bare else ""))


def get_program_name(filename):
    name = os.path.split(os.path.dirname(filename))[1]
    if not channeldir_regex.match(name):
        return name
    name = os.path.basename(filename)
    _match = anchored_regex.match(name)
    if _match:
        return _match.groups()[0]
    return name


def get_program_vod(channel_id, url=None, cloud=False, local=False):
    if channel_id not in _g._CHANNELS:
        return

    if not url:
        start = int(time.time())
    elif not url.isdigit():
        _match = flussonic_regex.match(url)
        if not _match:
            return
        start = int(_match.groups()[0])
    elif len(url) == 10:
        start = int(url)
    elif len(url) == 12:
        start = int(datetime.strptime(url, "%Y%m%d%H%M").timestamp())
    elif len(url) == 14:
        start = int(datetime.strptime(url, "%Y%m%d%H%M%S").timestamp())
    else:
        return

    channel = _g._CHANNELS[channel_id].name
    duration = last_timestamp = new_start = 0
    full_title = ""

    if not cloud and not local:
        if start not in _g._EPGDATA[channel_id]:
            for timestamp in _g._EPGDATA[channel_id]:
                if timestamp > start:
                    break
                last_timestamp = timestamp
            if not last_timestamp:
                return
            start, new_start = last_timestamp, start
        pid = _g._EPGDATA[channel_id][start].pid
        duration = _g._EPGDATA[channel_id][start].duration
        full_title = _g._EPGDATA[channel_id][start].full_title

    elif cloud and local:
        return

    else:
        guide = _g._CLOUD if cloud else _g._RECORDINGS if local else {}
        if channel_id not in guide:
            return
        if start in guide[channel_id]:
            duration = guide[channel_id][start].duration
        else:
            for timestamp in reversed(guide[channel_id]):
                duration = guide[channel_id][timestamp].duration
                if timestamp < start <= timestamp + duration:
                    start, new_start = timestamp, start
                    break
            if not new_start:
                return
        if cloud:
            full_title = _g._CLOUD[channel_id][start].full_title
            pid = _g._CLOUD[channel_id][start].pid
        else:
            full_title = _g._RECORDINGS[channel_id][start].filename
            pid = get_path(full_title)

            _match = anchored_regex.match(full_title)
            if _match:
                full_title = _match.groups()[0]
            full_title = os.path.basename(full_title).translate(str.maketrans("_;[]", "/:()"))

    return ProgramVOD(channel, pid, full_title, start, duration, offset=new_start - start if new_start else 0)


async def get_recording_files(filename, cache=False):
    absname = os.path.join(_g.RECORDINGS, filename.removesuffix(_g.VID_EXT))
    nfo = os.path.join(absname + NFO_EXT)

    path, basename = os.path.split(absname)
    metadata = os.path.join(path, "metadata")

    files = glob_safe(f"{absname}.*")
    files += [nfo] if await aio_os.path.exists(nfo) else []
    if await aio_os.path.exists(metadata):
        files += [*glob_safe(os.path.join(metadata, f"{basename}-*")), metadata]
    files += [path] if path != _g.RECORDINGS else []

    if cache and _g.RECORDINGS_TMP:
        files += glob_safe(
            os.path.join(
                path.replace(_g.RECORDINGS, os.path.join(_g.RECORDINGS_TMP, "covers")), f"**/{basename}*"
            ),
            recursive=True,
        )

    return [f for f in files if await aio_os.access(f, os.W_OK)]


def get_recording_name(channel_id, timestamp, cloud=False):
    guide = _g._CLOUD if cloud else _g._EPGDATA
    epg = guide[channel_id][timestamp]

    path = os.path.join(_g.RECORDINGS, get_channel_dir(channel_id))
    if epg.serie:
        path = os.path.join(path, get_safe_filename(epg.serie))
    else:
        path = os.path.join(path, get_safe_filename(epg.full_title))

    filename = os.path.join(path.rstrip(" _.,;"), get_safe_filename(epg.full_title))

    if not any((epg.genre[0] in ("1", "8"), epg.serie)):
        filename += f' - {datetime.fromtimestamp(timestamp).strftime("%Y%m%d_%H%M")}'

    return filename[len(_g.RECORDINGS) + 1 :]


def get_safe_filename(filename):
    filename = filename.translate(str.maketrans("/:()", "_;[]"))
    return "".join(c for c in filename if c.isalnum() or c in " ,.;_-[]@#$%€").rstrip()


def get_siblings(channel_id, filename):
    return tuple(
        ts
        for ts in sorted(_g._RECORDINGS[channel_id])
        if os.path.split(_g._RECORDINGS[channel_id][ts].filename)[0] == os.path.split(filename)[0]
    )


async def get_vod_info(session, endpoint, channel, cloud, program):
    params = {"action": "getRecordingData" if cloud else "getCatchUpUrl"}
    params.update({"extInfoID": program, "channelID": channel, "mode": 1})

    try:
        async with session.get(endpoint, params=params) as r:
            return (await r.json())["resultData"]
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError, KeyError, TypeError):
        pass


def glob_safe(string, recursive=False):
    return glob(f"{string.translate(str.maketrans('[]', '??'))}", recursive=recursive)


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
    else:
        log_suffix = f": [{proc.pid}]"
    log.warning(DIV_ONE, "Recording KILLED", log_suffix)


async def launch(cmd):
    cmd = (*sys.orig_argv[:-1], *cmd) if EXT == ".py" else cmd
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=os.path.dirname(__file__) if EXT == ".py" else None)
    try:
        return await proc.wait()
    except CancelledError:
        with suppress(CancelledError):
            proc.terminate()
            return await proc.wait()


async def load_epg():
    await reload_epg()

    if _g._SHUTDOWN:
        return app.stop()

    if not _g._last_epg:
        if not await a.all(a.map(aio_os.path.exists, (_g.CHANNELS, _g.GUIDE, _g.epg_data))):
            return app.stop()
        _g._last_epg = await aio_os.path.getmtime(_g.GUIDE)

    if _g.RECORDINGS:
        if not await aio_os.path.exists(_g.RECORDINGS):
            try:
                await aio_os.makedirs(_g.RECORDINGS)
            except PermissionError:
                log.error(f'Cannot access "{_g.RECORDINGS}" => RECORDINGS disabled')
                _g.RECORDINGS = None
        else:
            if _g.RECORDINGS_UPGRADE:
                await upgrade_recordings()
            elif _g.RECORDINGS_REINDEX:
                await reindex_recordings()
            else:
                await reload_recordings()
            if (
                _g.RECORDINGS_TMP
                and _g._RECORDINGS
                and not await aio_os.path.exists(os.path.join(_g.RECORDINGS_TMP, "covers"))
            ):
                await create_covers_cache()

        if not _g._t_timers:
            if await aio_os.path.exists(_g.TVG_BUSY):
                log.warning("Delaying timers_check until TVG is done...")
            elif _g._last_epg < datetime.now().replace(minute=0, second=0, microsecond=0).timestamp():
                log.warning("Delaying timers_check until the EPG is updated...")
            else:
                delay = int(max(5, boot_time() + 90 - time.time()))
                if delay > 10:
                    log.info(f"Waiting {delay}s to check recording timers since the system just booted...")
                app.add_task(timers_check(delay))

    app.add_task(update_epg_cron())


async def log_network_saturated(nr_procs=None, _wait=False):
    level = _g._NETWORK_SATURATION
    msg = ""
    if level:
        msg = f"Network {'Hard' if level == 2 else 'Soft' if level == 1 else ''} Saturated. "
    if not nr_procs:
        if _wait:  # when called by network_saturation(), we wait so the recordings count is always correct
            await asyncio.sleep(1)
        nr_procs = len(await ongoing_vods())
    if _g._SHUTDOWN:
        return
    msg += f"Recording {nr_procs} streams." if nr_procs else ""
    if not _g._last_bw_warning or (time.time() - _g._last_bw_warning[0] > 5 or _g._last_bw_warning[1] != msg):
        _g._last_bw_warning = (time.time(), msg)
        log.warning(msg)
    return msg


async def network_bw_control():
    async with network_bw_lock:
        await log_network_saturated()
        await kill_vod()
        await asyncio.sleep(2)


async def network_saturation():
    iface_rx = f"/sys/class/net/{_g.IPTV_IFACE}/statistics/rx_bytes"
    before = last = 0
    cutpoint = _g.IPTV_BW_SOFT + (_g.IPTV_BW_HARD - _g.IPTV_BW_SOFT) / 2

    while not _g._SHUTDOWN:
        if not await aio_os.path.exists(iface_rx):
            last = 0
            log.error(f"{_g.IPTV_IFACE} NOT ACCESSIBLE")
            while True:
                if await aio_os.path.exists(iface_rx):
                    break
                await asyncio.sleep(1)
            log.info(f"{_g.IPTV_IFACE} IS now ACCESSIBLE")

        async with async_open(iface_rx) as f:
            now, cur = time.time(), int((await f.read())[:-1])

        if last:
            tp = (cur - last) * 0.008 / (now - before)
            _g._NETWORK_SATURATED = tp > cutpoint
            _g._NETWORK_SATURATION = 2 if tp > _g.IPTV_BW_HARD else 1 if tp > _g.IPTV_BW_SOFT else 0

            if _g._NETWORK_SATURATION:
                if timers_lock.locked():
                    _g._t_timers.cancel()
                    app.add_task(log_network_saturated(_wait=True))
                if _g._NETWORK_SATURATION == 2 and not network_bw_lock.locked():
                    app.add_task(network_bw_control())

        before, last = now, cur
        await asyncio.sleep(1)


async def ongoing_vods(channel_id="", program_id="", filename="", all_=False, fast_=False):
    parent = int(os.getenv("U7D_PARENT", "0")) if not WIN32 else None
    family = Process(parent).children(recursive=True) if parent else process_iter()

    if fast_:  # For U7D we just want to know if there are recordings in place
        return "mu7d_vod REC " in str(family)

    if not WIN32:
        regex = "^mu7d_vod REC " if not all_ else "^mu7d_vod .*"
    else:
        regex = "^%smu7d_vod%s" % ((sys.executable.replace("\\", "\\\\") + " ") if EXT == ".py" else "", EXT)

    filename = filename.translate(str.maketrans("[]", ".."))

    regex += "(" if filename and program_id else ""
    regex += f" {channel_id:4} {program_id}" if program_id else ""
    regex += "|" if filename and program_id else ""
    regex += " .+%s" % filename.replace("\\", "\\\\") if filename else ""
    regex += ")" if filename and program_id else ""

    vods = filter(None, map(lambda proc: proc_grep(proc, regex), family))
    return tuple(vods) if not all_ else "|".join([" ".join(proc.cmdline()).strip() for proc in vods])


def parse_channels(data):
    return {int(k) if k.isdigit() else k: Channel(*c.values()) if k.isdigit() else c for k, c in data.items()}


def parse_epg(data):
    return {
        int(k) if k.isdigit() else k: Program(*p.values()) if k.isdigit() and len(k) == 10 else p
        for k, p in data.items()
    }


def parse_recordings(data):
    return {
        int(k) if k.isdigit() else k: Recording(*p.values()) if k.isdigit() and len(k) == 10 else p
        for k, p in data.items()
    }


def pp_xml(path, key, value):  # pylint: disable=unused-argument
    return key, int(value) if key in XML_INT_KEYS else value if value else ""


def proc_grep(proc, regex):
    try:
        return proc if re.match(regex, " ".join(proc.cmdline())) else None
    except (AccessDenied, NoSuchProcess, PermissionError):
        pass


async def prom_event(event, method, cloud=False, local=False, p_vod=None):
    if method not in ("add", "remove", "na"):
        return

    if not p_vod:
        p_vod = get_program_vod(event.ch_id, cloud, local)
    if not p_vod:
        return

    if method == "add":
        offset = "[%d/%d]" % (p_vod.offset, p_vod.duration)
        labels = (event.method, f"{event.endpoint} _ {p_vod.full_title} _ {offset}", event.id)
        _g.metrics["RQS_LATENCY"].labels(*labels).observe(float(event.lat))
        event_msg = event.msg

    elif method == "remove":
        for metric in filter(
            lambda _metric: event.method in _metric and str(event.id) in _metric,
            _g.metrics["RQS_LATENCY"]._metrics,
        ):
            _g.metrics["RQS_LATENCY"].remove(*metric)
            break

        start, end = p_vod.offset, p_vod.offset + event.offset
        if not event.lat or start == end:
            return

        offset = "[%d-%d/%d]" % (start, end, p_vod.duration)
        event_msg = event.msg.replace("Playing", "Stopped")
    else:
        offset = "[%d/%d]" % (p_vod.offset, p_vod.duration)
        event_msg = event.msg.replace("Playing", "NA     ")

    msg = "%-95s%9s:" % (event_msg, f"[{event.lat:.4f}s]" if event.lat else "")
    msg += " [%4d] [%08d] [%d]" % (event.ch_id, p_vod.pid if not local else 0, p_vod.start)
    msg += ' [%s] "%s" _ %s' % (p_vod.ch_name, p_vod.full_title, offset)

    log.info(msg) if method != "na" else log.error(msg)


async def prune_duplicates(channel_id, filename):
    def _clean(fname):
        return re.sub(r"[^a-z0-9]", "", os.path.basename(fname).lower())

    new_files = await get_recording_files(filename, cache=True)

    for ts in tuple(
        ts
        for ts in _g._RECORDINGS[channel_id]
        if _clean(_g._RECORDINGS[channel_id][ts].filename) in _clean(filename)
    ):
        duplicate = _g._RECORDINGS[channel_id][ts].filename
        old_files = tuple(f for f in await get_recording_files(duplicate, cache=True) if f not in new_files)

        await remove(*old_files)
        [log.warning(f'REMOVED DUPLICATED "{file}"') for file in old_files if not await aio_os.path.exists(file)]

        del _g._RECORDINGS[channel_id][ts]


async def prune_expired(channel_id, filename):
    program = get_program_name(filename)
    if program not in _g._KEEP[channel_id]:
        return

    siblings = get_siblings(channel_id, filename)
    if not siblings:
        return

    async def _drop(ts):
        filename = _g._RECORDINGS[channel_id][ts].filename
        old_files = await get_recording_files(filename, cache=True)
        await remove(*old_files)
        del _g._RECORDINGS[channel_id][ts]
        [log.warning(f'REMOVED "{file}"') for file in old_files if not await aio_os.path.exists(file)]

    if _g._KEEP[channel_id][program] < 0:
        max_days = abs(_g._KEEP[channel_id][program])
        newest = date.fromtimestamp(max(siblings[-1], await aio_os.path.getmtime(get_path(filename))))
        [await _drop(ts) for ts in siblings if (newest - date.fromtimestamp(ts)).days > max_days]

    elif len(siblings) >= _g._KEEP[channel_id][program]:
        [await _drop(ts) for ts in siblings[0 : len(siblings) - _g._KEEP[channel_id][program] + 1]]


async def reaper():
    if WIN32 or os.getpid() != 1:
        return

    while True:
        for child in (child for child in Process().children() if child.status() == "zombie"):
            try:
                child.wait()
            except ChildProcessError:
                break
        await asyncio.sleep(5)


async def record_program(ch_id, pid, offset=0, time=0, cloud=False, comskip=0, index=True, mkv=False, vo=False):
    timestamp = get_epg(ch_id, pid, cloud)[0]
    if not timestamp:
        return "Event not found"

    filename = get_recording_name(ch_id, timestamp, cloud)
    ongoing = await ongoing_vods(filename=filename, all_=True)
    if ongoing:
        log_suffix = f': [{ch_id:4}] [{pid}] [{timestamp}] "{filename}"'
        if f"{ch_id} {pid} -b {timestamp} " in ongoing:
            msg = DIV_ONE % ("Recording ONGOING", log_suffix)
            log.warning(msg)
            return re.sub(r"\s+:", ":", msg)
        if f"{ch_id} {pid} -b " not in ongoing and filename in ongoing:
            msg = DIV_ONE % ("Recording ONGOING: REPEATED EVENT", log_suffix)
            log.warning(msg)
            return re.sub(r"\s+:", ":", msg)

        r = re.search(f" {ch_id} {pid} -b " + r"(\d+) -p", ongoing)
        prev_ts = int(r.groups()[0])
        begin_time = f"beginTime=[{timestamp - prev_ts:+}s]"
        if timestamp < prev_ts:
            log.warning(DIV_TWO, "Event CHANGED => KILL", begin_time, log_suffix)
            ongoing = await ongoing_vods(ch_id, pid, filename)
            ongoing[0].terminate()
            await asyncio.sleep(5)
        elif timestamp > prev_ts:
            msg = DIV_TWO % ("Event DELAYED", begin_time, log_suffix)
            if filename not in _g._DELAYED:
                _g._DELAYED.add(filename)
                log.info(msg)
            return re.sub(r"\s+:", ":", msg)

    if _g._NETWORK_SATURATION:
        return await log_network_saturated()

    port = find_free_port(_g._IPTV)
    cmd = [f"mu7d_vod{EXT}", f"{ch_id:4}", str(pid), "-b", str(timestamp), "-p", f"{port:5}", "-w"]
    cmd += ["-o", filename]
    cmd += ["-s", str(offset)] if offset else []
    cmd += ["-t", str(time)] if time else []
    cmd += ["--cloud"] if cloud else []
    cmd += ["--comskip"] if comskip == 1 else ["--comskipcut"] if comskip == 2 else []
    cmd += ["--index"] if index else []
    cmd += ["--mkv"] if mkv else []
    cmd += ["--vo"] if vo else []

    log.debug('Launching: "%s"', " ".join(cmd))
    app.add_task(launch(cmd))


async def reindex_recordings():
    async def _write_nfo(nfo_file, nfo):
        xml = xmltodict.unparse({"metadata": dict(sorted(nfo.items()))}, pretty=True)
        async with async_open(nfo_file + ".tmp", "w", encoding="utf8") as f:
            await f.write(xml)
        await rename(nfo_file + ".tmp", nfo_file)

    async with recordings_lock:
        log.info("REINDEXING RECORDINGS")

        nfos_updated = 0
        _recordings = defaultdict(dict)

        for nfo_file in sorted(
            glob(f"{_g.RECORDINGS}/[0-9][0-9][0-9]. */**/*{NFO_EXT}", recursive=True), key=os.path.getmtime
        ):
            basename = nfo_file.removesuffix(NFO_EXT)
            if not await does_recording_exist(basename):
                log.error(f'No recording found: "{basename + _g.VID_EXT}" {nfo_file=}')
                continue

            try:
                async with async_open(nfo_file, encoding="utf-8") as f:
                    nfo = xmltodict.parse(await f.read(), postprocessor=pp_xml)["metadata"]
            except (FileNotFoundError, ParsingInterrupted, OSError, PermissionError, TypeError, ValueError) as x:
                log.error(f"Cannot read {nfo_file=} => {repr(x)}")
                continue

            nfo_needs_update = False
            filename = nfo["cover"][:-4]
            channel_id, duration, timestamp = nfo["serviceUID"], nfo["duration"], nfo["beginTime"]

            if channel_id not in _g._CHANNELS:
                stale = True
                dirname = filename.split(os.path.sep)[0]
                _match = re.match(r"^(\d{3})\. (.+)$", dirname)
                if _match:
                    channel_nr = int(_match.groups()[0])
                    _ids = tuple(filter(lambda ch: _g._CHANNELS[ch].number == channel_nr, _g._CHANNELS))
                    if _ids:
                        stale = False
                        log.warning(f'Replaced channel_id [{channel_id:4}] => [{_ids[0]:4}] in "{filename}"')
                        nfo["serviceUID"] = channel_id = _ids[0]
                        nfo_needs_update = True
                if stale:
                    log.warning(f'Wrong channel_id [{channel_id:4}] in nfo => Skipping "{filename}"')
                    continue

            recording_files = await get_recording_files(filename)
            covers_files = [
                x[len(_g.RECORDINGS) + 1 :]
                for x in recording_files
                if "metadata" in x and await aio_os.path.isfile(x)
            ]

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

            await utime(timestamp, *recording_files)
            _recordings[channel_id][timestamp] = Recording(filename, duration)

        if nfos_updated:
            log.info(f"Updated {nfos_updated} metadata files")
        else:
            log.info("No metadata files needed updates")

        if len(_recordings):
            _g._RECORDINGS = _recordings
            if _g.RECORDINGS_TMP:
                await create_covers_cache()

        log.info("RECORDINGS REINDEXED")

    await update_recordings()
    freemem()


async def reload_epg():
    if await a.all(a.map(aio_os.path.exists, (_g.CHANNELS, _g.channels_data))):
        async with epg_lock:
            try:
                async with async_open(_g.channels_data, encoding="utf8") as f:
                    _g._CHANNELS = json.loads(await f.read(), object_hook=parse_channels)["data"]
            except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError) as ex:
                log.error(f"Failed to load Channels metadata {repr(ex)}")
                await remove(_g.channels_data, _g.epg_data)
                return await reload_epg()

    elif await a.all(a.map(aio_os.path.exists, (_g.epg_data, _g.GUIDE, _g.GUIDE + ".gz"))):
        if tvgrab_lock.locked():
            return

        log.warning("Missing channel list! Need to download it. Please be patient...")

        cmd = (f"mu7d_tvg{EXT}", "--m3u", _g.CHANNELS)
        async with tvgrab_lock:
            await launch(cmd)

    if not await a.all(
        a.map(aio_os.path.exists, (_g.CHANNELS, _g.channels_data, _g.epg_data, _g.GUIDE, _g.GUIDE + ".gz"))
    ):
        if _g._CHANNELS and await a.all(a.map(aio_os.path.exists, (_g.CHANNELS, _g.channels_data))):
            log.info(f"Channels         Updated => {_g.U7D_URL}/MovistarTV.m3u")
            log.info(f"Total: {len(_g._CHANNELS):2} Channels")

        if tvgrab_lock.locked():
            return

        await remove(_g.TVG_BUSY)

        log.warning("Missing EPG data! Need to download it. Please be patient...")
        return await update_epg()

    async with epg_lock:
        try:
            async with async_open(_g.epg_data, encoding="utf8") as f:
                _g._EPGDATA = json.loads(await f.read(), object_hook=parse_epg)["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError) as ex:
            log.error(f"Failed to load EPG data {repr(ex)}")
            await remove(_g.epg_data)
            return await reload_epg()

        log.info(f"Channels  &  EPG Updated => {_g.U7D_URL}/MovistarTV.m3u      - {_g.U7D_URL}/guide.xml.gz")
        nr_epg = sum((len(_g._EPGDATA[channel]) for channel in _g._EPGDATA))
        log.info(f"Total: {len(_g._CHANNELS):2} Channels & {nr_epg:5} EPG entries")

    await update_cloud()
    freemem()


async def reload_recordings():
    if await upgrade_recording_channels():
        await reindex_recordings()
    else:
        async with recordings_lock:
            try:
                async with async_open(_g.recordings_data, encoding="utf8") as f:
                    _g._RECORDINGS = defaultdict(dict, json.loads(await f.read(), object_hook=parse_recordings))
            except (JSONDecodeError, TypeError, ValueError) as ex:
                log.error(f'Failed to parse "recordings.json" => {repr(ex)}')
                await remove(_g.CHANNELS_LOCAL, _g.GUIDE_LOCAL)
                await reindex_recordings()
            except (FileNotFoundError, OSError, PermissionError):
                await remove(_g.CHANNELS_LOCAL, _g.GUIDE_LOCAL)

        if not _g._RECORDINGS:
            return

        async with recordings_lock:
            for channel_id in _g._RECORDINGS:
                oldest_epg = min(_g._EPGDATA[channel_id].keys())
                for timestamp in [
                    ts
                    for ts in _g._RECORDINGS[channel_id]
                    if not await does_recording_exist(_g._RECORDINGS[channel_id][ts].filename)
                ]:
                    filename = _g._RECORDINGS[channel_id][timestamp].filename
                    if timestamp > oldest_epg:
                        log.warning(f'Archived Local Recording "{filename}" not found on disk')
                    elif channel_id in _g._CLOUD and timestamp in _g._CLOUD[channel_id]:
                        log.warning(f'Archived Cloud Recording "{filename}" not found on disk')

    await update_epg_local()
    freemem()


async def remove(*items):
    for item in items:
        try:
            if await aio_os.path.isfile(item):
                await aio_os.remove(item)
            elif await aio_os.path.isdir(item):
                if not await aio_os.listdir(item):
                    await aio_os.rmdir(item)
                elif os.path.split(item)[1] != "metadata":
                    _glob = sorted(glob(f"{item}/**/*", recursive=True), reverse=True)
                    if not any(filter(lambda file: any(file.endswith(ext) for ext in VID_EXTS_KEEP), _glob)):
                        [await aio_os.remove(x) async for x in a.filter(aio_os.path.isfile, _glob)]
                        [await aio_os.rmdir(x) async for x in a.filter(aio_os.path.isdir, _glob)]
                        await aio_os.rmdir(item)
        except (FileNotFoundError, OSError, PermissionError):
            pass


async def rename(src, dst):
    if WIN32 and await aio_os.path.exists(dst):
        await aio_os.remove(dst)
    await aio_os.rename(src, dst)


async def timers_check(delay=0):  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    def _clean(string):
        return unicodedata.normalize("NFKD", string).encode("ASCII", "ignore").decode("utf8").strip()

    def _filter_recorded(channel_id, timestamps, stored_filenames):
        return filter(
            lambda ts: channel_id not in _g._RECORDINGS
            or ts not in _g._RECORDINGS[channel_id]
            or get_recording_name(channel_id, ts) not in stored_filenames,
            timestamps,
        )

    await asyncio.sleep(delay)

    if timers_lock.locked() or _g._SHUTDOWN:
        return

    async with epg_lock, recordings_lock, timers_lock:
        _g._t_timers = asyncio.current_task()

        log.debug("Processing timers")

        if not await aio_os.path.exists(_g.timers_data):
            log.debug("timers_check: no timers.conf found")
            return

        nr_procs = len(await ongoing_vods())
        if _g._NETWORK_SATURATION or nr_procs >= _g.RECORDINGS_PROCESSES:
            await log_network_saturated(nr_procs)
            return

        try:
            async with async_open(_g.timers_data, encoding="utf8") as f:
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
            if channel_id not in _g._EPGDATA:
                log.warning(f"Channel [{channel_id}] in timers.conf not found in EPG")
                continue
            channel_name = _g._CHANNELS[channel_id].name

            log.debug("Checking EPG  : [%4d] [%s]", channel_id, channel_name)
            stored_filenames = (program.filename for program in _g._RECORDINGS[channel_id].values())

            for timer_match in _timers["match"][str_channel_id]:
                comskip = 2 if _timers.get("comskipcut") else 1 if _timers.get("comskip") else 0
                days = {}
                delay = keep = 0
                fresh = repeat = False
                fixed_timer = None
                genre = msg = ""
                lang = deflang

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

                        elif res.startswith("genre"):
                            genre = res[5:] if res[5:].isdigit() else ""

                        elif res.startswith("keep"):
                            keep = int(res[4:].rstrip("d")) * (-1 if res.endswith("d") else 1)

                        elif res == "nocomskip":
                            comskip = 0

                        elif res == "repeat":
                            repeat = True

                        else:
                            lang = res

                vo = lang == "VO"
                msg = f"[{comskip=}] [{delay=}] [{fresh=}] [{keep=}] [{repeat=}] [{days=}]{msg}"
                log.debug("Checking timer: [%4d] [%s] [%s] %s", channel_id, channel_name, timer_match, msg)

                if fresh:
                    tss = filter(
                        lambda ts: ts <= _g._last_epg + 3600
                        and ts + _g._EPGDATA[channel_id][ts].duration > int(time.time()),
                        _g._EPGDATA[channel_id],
                    )
                else:
                    tss = filter(lambda ts: ts <= _g._last_epg + 3600 - delay, _g._EPGDATA[channel_id])

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

                if genre:
                    tss = filter(lambda ts: _g._EPGDATA[channel_id][ts].genre.startswith(genre), tss)

                tss = tuple(tss)

                if keep:
                    for ts in filter(
                        lambda ts: re.search(
                            _clean(timer_match), _clean(_g._EPGDATA[channel_id][ts].full_title), re.IGNORECASE
                        ),
                        tss,
                    ):
                        kept[channel_id][get_program_name(get_recording_name(channel_id, ts))] = keep
                        break

                for timestamp in filter(
                    lambda ts: re.search(
                        _clean(timer_match), _clean(_g._EPGDATA[channel_id][ts].full_title), re.IGNORECASE
                    ),
                    _filter_recorded(channel_id, tss, stored_filenames),
                ):
                    _b, _f = "norml" if not fresh else "fresh", get_recording_name(channel_id, timestamp)
                    curr_timers[_b][_f] = Timer(channel_id, timestamp, False, comskip, delay, keep, repeat, vo)

        if sync_cloud:
            comskip = 2 if _timers.get("comskipcut") else 1 if _timers.get("comskip") else 0
            vo = _timers.get("sync_cloud_language", "") == "VO"

            for channel_id in _g._CLOUD:
                log.debug("Checking Cloud: [%4d] [%s]", channel_id, _g._CHANNELS[channel_id].name)
                stored_filenames = (program.filename for program in _g._RECORDINGS[channel_id].values())

                for timestamp in _filter_recorded(channel_id, _g._CLOUD[channel_id].keys(), stored_filenames):
                    _f = get_recording_name(channel_id, timestamp, cloud=True)
                    curr_timers["cloud"][_f] = Timer(channel_id, timestamp, True, comskip, 0, 0, False, vo)

        _g._KEEP = kept
        ongoing = await ongoing_vods(all_=True)  # we want to check against all ongoing vods, also in pp
        [_g._DELAYED.remove(filename) for filename in _g._DELAYED if filename not in ongoing]

        log.debug("_DELAYED=%s", str(_g._DELAYED))
        log.debug("_KEEP=%s", str(_g._KEEP))
        log.debug("Ongoing VODs: |%s|", ongoing)

        for filename, ti in chain(
            sorted(
                curr_timers["fresh"].items(), key=lambda x: x[1].ts + _g._EPGDATA[x[1].ch_id][x[1].ts].duration
            ),
            sorted(curr_timers["norml"].items(), key=lambda x: x[1].ts),
            sorted(curr_timers["cloud"].items(), key=lambda x: x[1].ts),
        ):
            guide = _g._EPGDATA if not ti.cloud else _g._CLOUD
            pid = guide[ti.ch_id][ti.ts].pid
            duration = guide[ti.ch_id][ti.ts].duration if not ti.cloud else 0
            log_suffix = f': [{ti.ch_id:4}] [{pid}] [{ti.ts}] "{filename}"'

            if 0 < duration < 240:
                log.info(DIV_TWO, "Skipping MATCH", f"Too short [{duration}s]", log_suffix)
                continue

            if f"{ti.ch_id} {pid} -b {ti.ts} " in ongoing:
                continue  # recording already ongoing

            if f"{ti.ch_id} {pid} -b " not in ongoing and filename in ongoing:
                continue  # repeated event

            if filename in _g._DELAYED:
                continue  # recording already ongoing but delayed

            _x = filter(
                lambda x: filename.startswith(_g._RECORDINGS[ti.ch_id][x].filename), _g._RECORDINGS[ti.ch_id]
            )
            recs = tuple(_x)
            # only repeat a recording when title changed
            if recs and not len(filename) > len(_g._RECORDINGS[ti.ch_id][recs[0]].filename):
                # or asked to and new event is more recent than the archived
                if not ti.repeat or ti.ts <= recs[0]:
                    continue

            if ti.keep:
                if ti.keep < 0:
                    if (datetime.fromtimestamp(ti.ts) - datetime.now()).days < ti.keep:
                        log.info('Older than     %d days: SKIPPING [%d] "%s"', abs(ti.keep), ti.ts, filename)
                        continue
                else:
                    program = get_program_name(filename)

                    current = tuple(filter(lambda x: program in x, (await ongoing_vods(all_=True)).split("|")))
                    if len(current) == ti.keep:
                        log.info('Recording %02d programs: SKIPPING [%d] "%s"', ti.keep, ti.ts, filename)
                        continue

                    siblings = get_siblings(ti.ch_id, filename)
                    if len(current) + len(siblings) == ti.keep and ti.ts < siblings[0]:
                        log.info('Recorded  %02d programs: SKIPPING [%d] "%s"', ti.keep, ti.ts, filename)
                        continue

            if ti.ts + ti.delay + 30 > round(datetime.now().timestamp()):
                next_timers.append((guide[ti.ch_id][ti.ts].full_title, ti.ts + ti.delay + 30))
                continue

            if await record_program(
                ti.ch_id, pid, 0, duration, ti.cloud, ti.comskip, True, _g.MKV_OUTPUT, ti.vo
            ):
                continue

            log.info(DIV_ONE, f"Found {'Cloud ' if ti.cloud else ''}EPG MATCH", log_suffix)

            nr_procs += 1
            if nr_procs >= _g.RECORDINGS_PROCESSES:
                await log_network_saturated(nr_procs)
                break

            await asyncio.sleep(2.5 if not WIN32 else 4)

        next_title, next_ts = sorted(next_timers, key=itemgetter(1))[0] if next_timers else ("", 0)

        if _g._t_timers_next and (not next_ts or _g._t_timers_next.get_name() != f"{next_ts}"):
            if not _g._t_timers_next.done():
                _g._t_timers_next.cancel()
            _g._t_timers_next = None

        if next_ts and not _g._t_timers_next:
            log.info(f'Adding timers_check() @ {datetime.fromtimestamp(next_ts)} "{next_title}"')
            _g._t_timers_next = app.add_task(timers_check(delay=next_ts - time.time()), name=f"{next_ts}")


async def update_cloud():
    async with epg_lock:
        cmd = (f"mu7d_tvg{EXT}", "--cloud_m3u", _g.CHANNELS_CLOUD, "--cloud_recordings", _g.GUIDE_CLOUD)
        async with tvgrab_lock:
            if await launch(cmd):
                return

        try:
            async with async_open(_g.cloud_data, encoding="utf8") as f:
                _g._CLOUD = json.loads(await f.read(), object_hook=parse_epg)["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError):
            return await update_cloud()

        log.info(f"Cloud Recordings Updated => {_g.U7D_URL}/MovistarTVCloud.m3u - {_g.U7D_URL}/cloud.xml")
        nr_epg = sum((len(_g._CLOUD[channel]) for channel in _g._CLOUD))
        log.info(f"Total: {len(_g._CLOUD):2} Channels & {nr_epg:5} EPG entries")


async def update_epg():
    cmd = (f"mu7d_tvg{EXT}", "--m3u", _g.CHANNELS, "--guide", _g.GUIDE)
    async with tvgrab_lock:
        if await launch(cmd):
            return

    await reload_epg()
    _g._last_epg = int(datetime.now().replace(minute=0, second=0, microsecond=0).timestamp())

    if _g.RECORDINGS:
        if timers_lock.locked():
            _g._t_timers.cancel()
        app.add_task(timers_check(delay=3))

        if _g._RECORDINGS and await upgrade_recording_channels():
            await reindex_recordings()


async def update_epg_cron():
    if _g._SHUTDOWN:
        return

    last_datetime = datetime.now().replace(minute=0, second=0, microsecond=0).timestamp()
    if await aio_os.path.exists(_g.TVG_BUSY):
        log.warning("TVG was interrumped. Launching again...")
        await update_epg()
    elif await aio_os.path.getmtime(_g.GUIDE) < last_datetime:
        log.warning("EPG too old. Updating it...")
        await update_epg()
    await asyncio.sleep(last_datetime + 3600 - 0.25 - time.time())
    while not _g._SHUTDOWN:
        await asyncio.gather(asyncio.sleep(3600), asyncio.wait_for(update_epg(), timeout=3300))


async def update_epg_local():
    async with recordings_lock:
        cmd = (f"mu7d_tvg{EXT}", "--local_m3u", _g.CHANNELS_LOCAL, "--local_recordings", _g.GUIDE_LOCAL)

        async with tvgrab_local_lock:
            if await launch(cmd):
                return

        log.info(f"Local Recordings Updated => {_g.U7D_URL}/MovistarTVLocal.m3u - {_g.U7D_URL}/local.xml")
        nr_epg = sum((len(_g._RECORDINGS[channel]) for channel in _g._RECORDINGS))
        log.info(f"Total: {len(_g._RECORDINGS):2} Channels & {nr_epg:5} EPG entries")


async def update_recordings(channel_id=None):
    async with recordings_lock:
        if not _g._RECORDINGS:
            return

        sorted_recordings = {
            ch: dict(sorted({k: v._asdict() for k, v in _g._RECORDINGS[ch].items()}.items()))
            for ch in (k for k in _g._CHANNELS if k in _g._RECORDINGS)
        }
        async with async_open(_g.recordings_data + ".tmp", "w", encoding="utf8") as f:
            await f.write(ujson.dumps(sorted_recordings, ensure_ascii=False, indent=4))
        await rename(_g.recordings_data + ".tmp", _g.recordings_data)

    await update_epg_local()

    if not _g.RECORDINGS_M3U:
        if channel_id:
            await utime(
                max(_g._RECORDINGS[channel_id].keys()), os.path.join(_g.RECORDINGS, get_channel_dir(channel_id))
            )
        else:
            for ch_id in _g._RECORDINGS:
                await utime(
                    max(_g._RECORDINGS[ch_id].keys()), os.path.join(_g.RECORDINGS, get_channel_dir(ch_id))
                )
        await utime(max(max(_g._RECORDINGS[channel].keys()) for channel in _g._RECORDINGS), _g.RECORDINGS)
        return

    translation = str.maketrans("_;[]", "/:()")

    async def _dump_files(files, channel=False, latest=False):
        m3u = deque()
        async for file in (f async for f in files if await aio_os.path.exists(f + _g.VID_EXT)):
            path, name = (urllib.parse.quote(x.translate(translation)) for x in os.path.split(file))
            _m3u = '#EXTINF:-1 group-title="'
            _m3u += '# Recientes"' if latest else f'{path}"' if path else '#"'
            _m3u += f' tvg-logo="{_g.U7D_URL}/recording/?{urllib.parse.quote(file + ".jpg")}"'
            _m3u += f",{path} - " if (channel and path) else ","
            _m3u += f"{name}\n"
            _m3u += f"{_g.U7D_URL}/recording/?{urllib.parse.quote(file + _g.VID_EXT)}"
            m3u.append(_m3u)
        return m3u

    def _get_files(channel=None):
        if channel:
            return (f for f in sorted(_g._RECORDINGS[channel][ts].filename for ts in _g._RECORDINGS[channel]))
        return (
            f
            for f in sorted(
                [p[1].filename for p in chain(*(_g._RECORDINGS[ch].items() for ch in _g._RECORDINGS))]
            )
        )

    def _get_files_reversed():
        return (
            p.filename
            for _, p in sorted(
                tuple(chain(*(_g._RECORDINGS[ch].items() for ch in _g._RECORDINGS))),
                key=itemgetter(0),
                reverse=True,
            )
        )

    channels = (channel_id,) if channel_id else tuple(ch for ch in _g._CHANNELS if ch in _g._RECORDINGS)
    channels_dirs = [(ch, get_channel_dir(ch)) for ch in channels]
    channels_dirs += [(0, _g.RECORDINGS)]

    for ch_id, _dir in channels_dirs:
        header = f"#EXTM3U name=\"{'Recordings' if _dir == _g.RECORDINGS else _dir}\" dlna_extras=mpeg_ps_pal\n"
        if ch_id:
            m3u_file = os.path.join(_g.RECORDINGS, _dir, f"{_dir}.m3u")
        else:
            m3u_file = os.path.join(_g.RECORDINGS, "Recordings.m3u")

        async with async_open(m3u_file + ".tmp", "w", encoding="utf8") as f:
            await f.write(header)
            if ch_id:
                await f.write("\n".join(await _dump_files(_get_files(ch_id), channel=True)))
            else:
                await f.write("\n".join(await _dump_files(_get_files_reversed(), latest=True)) + "\n")
                await f.write("\n".join(await _dump_files(_get_files())))
        await rename(m3u_file + ".tmp", m3u_file)

        if ch_id:
            await utime(max(_g._RECORDINGS[ch_id].keys()), *(m3u_file, os.path.join(_g.RECORDINGS, _dir)))
        else:
            await utime(
                max(max(_g._RECORDINGS[channel].keys()) for channel in _g._RECORDINGS),
                *(m3u_file, _g.RECORDINGS),
            )

        log.info(f"Wrote m3u [{m3u_file[len(_g.RECORDINGS) + 1 :]}]")

    log.info(f"Local Recordings' M3U Updated => {_g.U7D_URL}/Recordings.m3u")


async def upgrade_recording_channels():
    if _g._SHUTDOWN:
        return

    changed_nr, stale = deque(), deque()

    names = tuple(_g._CHANNELS[x].name for x in _g._CHANNELS)
    numbers = tuple(_g._CHANNELS[x].number for x in _g._CHANNELS)

    dirs = a.filter(
        lambda _dir: aio_os.path.isdir(os.path.join(_g.RECORDINGS, _dir)), await aio_os.listdir(_g.RECORDINGS)
    )
    pairs = (r.groups() async for r in a.map(channeldir_regex.match, dirs) if r)

    async for nr, name in pairs:
        nr = int(nr)
        if nr not in numbers and name in names:
            _f = filter(lambda x: _g._CHANNELS[x].name == name and _g._CHANNELS[x].number != nr, _g._CHANNELS)
            for channel_id in _f:
                changed_nr.append((channel_id, nr, _g._CHANNELS[channel_id].number, name))
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
        await aio_os.rename(
            os.path.join(_g.RECORDINGS, old_channel_name), os.path.join(_g.RECORDINGS, new_channel_name)
        )
        log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{new_channel_name}"')

    changed_pairs = deque()
    for channel_id, old_nr, new_nr, name in changed_nr:
        old_channel_name = f"{old_nr:03}. {name}"
        new_channel_name = f"{new_nr:03}. {name}"
        old_channel_path = os.path.join(_g.RECORDINGS, old_channel_name)
        new_channel_path = os.path.join(_g.RECORDINGS, new_channel_name)

        changed_pairs.append((old_channel_name, new_channel_name))

        for nfo_file in sorted(glob(f"{old_channel_path}/**/*{NFO_EXT}", recursive=True)):
            new_nfo_file = nfo_file.replace(old_channel_name, new_channel_name)
            if await aio_os.path.exists(new_nfo_file):
                log.warning(f'SKIPPING existing "{new_nfo_file[len(_g.RECORDINGS) + 1 :]}"')
                continue

            async with async_open(nfo_file, encoding="utf-8") as f:
                data = await f.read()

            data = data.replace(f">{old_nr:03}. ", f">{new_nr:03}. ").replace(f">{old_nr}<", f">{new_nr}<")

            async with async_open(nfo_file, "w", encoding="utf8") as f:
                await f.write(data)

            if not await aio_os.path.exists(new_channel_path):
                continue

            files = await get_recording_files(nfo_file.removesuffix(NFO_EXT))
            old_files = (f for f in files if os.path.splitext(f)[-1] in (*VID_EXTS, ".jpg", ".nfo", ".png"))
            new_files = (f.replace(old_channel_name, new_channel_name) for f in old_files)

            old_dir = os.path.dirname(files[0])
            new_dir = old_dir.replace(old_channel_name, new_channel_name)

            if not await aio_os.path.exists(new_dir):
                await aio_os.mkdir(new_dir)

            if "/metadata/" in str(files):
                meta_dir = os.path.join(new_dir, "metadata")
                if not await aio_os.path.exists(meta_dir):
                    await aio_os.mkdir(meta_dir)

            for old_file, new_file in zip(old_files, new_files):
                if await aio_os.path.exists(new_file):
                    log.warning(f'SKIPPING existing "{new_file[len(_g.RECORDINGS) + 1 :]}"')
                else:
                    await aio_os.rename(old_file, new_file)

        if await aio_os.path.exists(new_channel_path):
            changed_channel_name = f'OLD_{datetime.now().strftime("%Y%m%dT%H%M%S")}_{old_nr:03}_{name}'
            changed_channel_path = os.path.join(_g.RECORDINGS, changed_channel_name)

            log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{changed_channel_name}"')
            await aio_os.rename(old_channel_path, changed_channel_path)
        else:
            log.warning(f'RENAMING channel folder: "{old_channel_name}" => "{new_channel_name}"')
            await aio_os.rename(old_channel_path, new_channel_path)

    return True


async def upgrade_recordings():
    log.info(f"UPGRADING RECORDINGS METADATA: {_g.RECORDINGS_UPGRADE}")
    covers = items = names = wrong = 0

    for nfo_file in sorted(glob(f"{_g.RECORDINGS}/**/*{NFO_EXT}", recursive=True)):
        basename = nfo_file.split(NFO_EXT)[0]
        recording = basename + _g.VID_EXT

        mtime = int(await aio_os.path.getmtime(recording))

        try:
            async with async_open(nfo_file, encoding="utf-8") as f:
                xml = await f.read()
            xml = "\n".join(line for line in xml.splitlines() if "5S_signLanguage" not in line)
            nfo = xmltodict.parse(xml, postprocessor=pp_xml)["metadata"]
        except (FileNotFoundError, ParsingInterrupted, OSError, PermissionError, TypeError, ValueError) as ex:
            log.error(f"Metadata malformed: {nfo_file=} => {repr(ex)}")
            continue

        if abs(_g.RECORDINGS_UPGRADE) == 2:
            cmd = ("ffprobe", "-i", recording, "-show_entries", "format=duration")
            cmd += ("-v", "quiet", "-of", "json")
            proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE, stderr=DEVNULL)
            recording_data = ujson.loads((await proc.communicate())[0].decode())
            duration = round(float(recording_data.get("format", {}).get("duration", 0)))

            _start, _exp = (nfo[x] // 1000 if nfo[x] > 10**10 else nfo[x] for x in ("beginTime", "expDate"))
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

        if not nfo.get("cover", "") == basename[len(_g.RECORDINGS) + 1 :] + ".jpg":
            nfo["cover"] = basename[len(_g.RECORDINGS) + 1 :] + ".jpg"
            covers += 1
        if not await aio_os.path.exists(basename + ".jpg"):
            log.warning('Cover not found: "%s"', basename[len(_g.RECORDINGS) + 1 :] + ".jpg")

        drop_covers, new_covers = [], {}
        if nfo.get("covers"):
            for cover in nfo["covers"]:
                if not nfo["covers"][cover].startswith("metadata"):
                    continue
                covers += 1
                cover_path = os.path.join(os.path.dirname(basename), nfo["covers"][cover])
                if not await aio_os.path.exists(cover_path):
                    log.warning('Cover not found: "%s"', cover_path[len(_g.RECORDINGS) + 1 :])
                    drop_covers.append(cover)
                    continue
                new_covers[cover] = cover_path[len(_g.RECORDINGS) + 1 :]
        elif "covers" in nfo:
            nfo.pop("covers")

        if new_covers:
            nfo["covers"] = new_covers
        elif drop_covers:
            [nfo["covers"].pop(cover) for cover in drop_covers]

        if _g.RECORDINGS_UPGRADE > 0:
            xml = xmltodict.unparse({"metadata": dict(sorted(nfo.items()))}, pretty=True)
            async with async_open(nfo_file + ".tmp", "w", encoding="utf8") as f:
                await f.write(xml)
            await rename(nfo_file + ".tmp", nfo_file)
            await utime(mtime, nfo_file)

    if any((covers, items, names, wrong)):
        msg = ("Would update", "Would fix") if _g.RECORDINGS_UPGRADE < 0 else ("Updated", "Fixed")
        log.info(
            f"{msg[0]} #{covers} covers. {msg[1]} #{items} items, #{names} originalNames & #{wrong} timestamps"
        )
    else:
        log.info(f"No updates {'would be ' if _g.RECORDINGS_UPGRADE < 0 else ''}carried out")

    if _g.RECORDINGS_UPGRADE > 0:
        await update_recordings()


async def utime(timestamp, *items):
    [await a.sync(os.utime)(item, (-1, timestamp)) for item in items if await aio_os.access(item, os.W_OK)]
