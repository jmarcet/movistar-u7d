#!/usr/bin/env python3

import aiohttp
import asyncio
import logging as log
import os
import psutil
import re
import socket
import sys
import tomli

from aiohttp.client_exceptions import ClientOSError
from asyncio.exceptions import CancelledError
from contextlib import closing
from filelock import FileLock, Timeout
from xml.sax.saxutils import unescape  # nosec B406


_version = "4.9"

EXT = ".exe" if getattr(sys, "frozen", False) else ".py"
WIN32 = sys.platform == "win32"

EPG_URL = "http://127.0.0.1:8889"
IPTV_DNS = "172.26.23.3"
MIME_M3U = "audio/x-mpegurl"
MIME_TS = "video/MP2T;audio/mp3"
MIME_WEBM = "video/webm"
UA = "libcurl-agent/1.0 [IAL] WidgetManager Safari/538.1 CAP:803fd12a 1"
UA_U7D = f"movistar-u7d v{_version} [{sys.platform}] [{EXT}]"
URL_BASE = "http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg"
URL_COVER = f"{URL_BASE}/covers/programmeImages/portrait/290x429"
URL_LOGO = f"{URL_BASE}/channelLogo"
URL_MVTV = "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do"
VID_EXTS = (".avi", ".mkv", ".mp4", ".mpeg", ".mpg", ".ts")
YEAR_SECONDS = 365 * 24 * 60 * 60

title_select_regex = re.compile(r".+ T\d+ .+")
title_1_regex = re.compile(r"(.+(?!T\d)) +T(\d+)(?: *Ep?.? *(\d+))?[ -]*(.*)")
title_2_regex = re.compile(r"(.+(?!T\d))(?: +T(\d+))? *Ep?.? *(\d+)[ -]*(.*)")


def find_free_port(iface=""):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind((iface, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_iptv_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.connect((IPTV_DNS, 53))
        return s.getsockname()[0]


def get_lan_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.connect(("8.8.8.8", 53))
        return s.getsockname()[0]


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


def mu7d_config():
    confname = "mu7d.conf"
    fileconf = ""

    if not WIN32:
        if os.path.exists(os.path.join(os.getenv("HOME"), confname)):
            fileconf = os.path.join(os.getenv("HOME"), confname)
        elif os.path.exists(os.path.join("/etc", confname)):
            fileconf = os.path.join("/etc", confname)
    elif os.path.exists(os.path.join(os.getenv("USERPROFILE"), confname)):
        fileconf = os.path.join(os.getenv("USERPROFILE"), confname)

    try:
        with open(fileconf) as f:
            conf = tomli.loads(f.read())
    except FileNotFoundError:
        conf = {}

    if "HOME" not in conf:
        conf["HOME"] = os.getenv("HOME", os.getenv("USERPROFILE"))

    if "COMSKIP" not in conf:
        conf["COMSKIP"] = None
    else:
        conf["COMSKIP"] = min(conf["COMSKIP"], os.cpu_count())

    if "DEBUG" not in conf:
        conf["DEBUG"] = False

    if "EXTRA_CHANNELS" not in conf:
        conf["EXTRA_CHANNELS"] = []
    else:
        conf["EXTRA_CHANNELS"] = conf["EXTRA_CHANNELS"].split(" ")

    if "IPTV_IFACE" not in conf or WIN32:
        conf["IPTV_IFACE"] = conf["IPTV_BW_SOFT"] = conf["IPTV_BW_HARD"] = None
    else:
        if "IPTV_BW_HARD" not in conf or WIN32:
            conf["IPTV_BW_HARD"] = 0
        else:
            conf["IPTV_BW_HARD"] = min(100000, conf["IPTV_BW_HARD"])

        if "IPTV_BW_SOFT" not in conf or not conf["IPTV_BW_HARD"] or WIN32:
            conf["IPTV_BW_HARD"] = conf["IPTV_BW_SOFT"] = 0
        else:
            conf["IPTV_BW_SOFT"] = min(conf["IPTV_BW_HARD"] - 10000, conf["IPTV_BW_SOFT"])

    if "LAN_IP" not in conf:
        conf["LAN_IP"] = get_lan_ip()

    if "MP4_OUTPUT" not in conf:
        conf["MP4_OUTPUT"] = False

    if "NO_SUBS" not in conf:
        conf["NO_SUBS"] = False

    if "NO_VERBOSE_LOGS" not in conf:
        conf["NO_VERBOSE_LOGS"] = False

    if "RECORDINGS" not in conf:
        conf["RECORDINGS"] = conf["RECORDINGS_M3U"] = None
    else:
        conf["RECORDINGS"] = conf["RECORDINGS"].rstrip("/").rstrip("\\")
        conf["RECORDINGS_M3U"] = os.path.join(conf["RECORDINGS"], "Recordings.m3u")

    if "RECORDINGS_PER_CHANNEL" not in conf:
        conf["RECORDINGS_PER_CHANNEL"] = True

    if not WIN32 and conf["IPTV_BW_SOFT"]:
        conf["RECORDINGS_THREADS"] = 9999
    elif "RECORDINGS_THREADS" not in conf:
        conf["RECORDINGS_THREADS"] = 4

    if "TVG_THREADS" not in conf:
        conf["TVG_THREADS"] = os.cpu_count()
    conf["TVG_THREADS"] = min(8, conf["TVG_THREADS"])

    if "U7D_PORT" not in conf:
        conf["U7D_PORT"] = 8888

    if "U7D_THREADS" not in conf:
        conf["U7D_THREADS"] = 1

    conf["CHANNELS"] = os.path.join(conf["HOME"], "MovistarTV.m3u")
    conf["CHANNELS_CLOUD"] = os.path.join(conf["HOME"], "MovistarTVCloud.m3u")
    conf["GUIDE"] = os.path.join(conf["HOME"], "guide.xml")
    conf["GUIDE_CLOUD"] = os.path.join(conf["HOME"], "cloud.xml")
    conf["U7D_URL"] = f"http://{conf['LAN_IP']}:{conf['U7D_PORT']}"

    return conf


async def ongoing_vods(channel_id="", program_id="", filename="", _fast=False, _rec=True):
    parent = os.getenv("U7D_PARENT")
    family = psutil.Process(int(parent)).children(recursive=True) if parent else psutil.process_iter()

    if _fast:  # For U7D we just want to know if there are recordings in place
        return "ffmpeg" in str(family)

    regex = f"{'.*' if WIN32 else ''}movistar_vod"
    regex += "" if not WIN32 else ".exe" if getattr(sys, "frozen", False) else ".py"
    regex += "(" if program_id or filename else ""
    regex += f" {channel_id} {program_id}" if program_id else ""
    regex += f"|.+{os.path.basename(filename)}" if filename else ""
    regex += ")" if program_id or filename else ""

    vods = list(filter(None, [proc_grep(proc, regex) for proc in family]))
    if _rec:
        return [proc for proc in vods if "ffmpeg" in str(proc.children())]
    else:
        return "|".join([" ".join(proc.cmdline()).strip() for proc in vods])


def proc_grep(proc, regex):
    try:
        return proc if re.match(regex, " ".join(proc.cmdline())) else None
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.PermissionError):
        pass


async def u7d_main():
    async def launch_epg():
        proc = await asyncio.create_subprocess_exec(*epg_cmd)
        task = asyncio.create_task(reap_proc(proc), name="epg_t")
        return proc, task

    async def launch_u7d():
        proc = await asyncio.create_subprocess_exec(*u7d_cmd)
        task = asyncio.create_task(reap_proc(proc), name="u7d_t")
        return proc, task

    async def reap_proc(proc):
        await proc.wait()

    if WIN32:
        conf = mu7d_config()

    epg_p, epg_t = await launch_epg()
    u7d_p, u7d_t = await launch_u7d()

    while True:
        try:
            done, pending = await asyncio.wait(asyncio.all_tasks(), return_when=asyncio.FIRST_COMPLETED)
        except CancelledError:
            break

        if WIN32:
            if u7d_p.pid:
                async with aiohttp.ClientSession() as session:
                    try:
                        await session.get(f"http://{conf['LAN_IP']}:{conf['U7D_PORT']}/terminate")
                    except ClientOSError:
                        pass
                try:
                    psutil.Process(u7d_p.pid).wait(timeout=10)
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    pass
            [child.terminate() for child in psutil.Process().children()]
            break

        if epg_t in done:
            epg_p, epg_t = await launch_epg()
        elif u7d_t in done:
            u7d_p, u7d_t = await launch_u7d()


if __name__ == "__main__":
    try:
        if not WIN32:
            psutil.Process().nice(-10)
            psutil.Process().ionice(psutil.IOPRIO_CLASS_BE, 0)
        else:
            psutil.Process().nice(psutil.ABOVE_NORMAL_PRIORITY_CLASS)
    except psutil.AccessDenied:
        pass

    if not WIN32:
        import signal
        from setproctitle import setproctitle

        setproctitle("mu7d")

        def cleanup_handler(signum, frame):
            [task.cancel() for task in asyncio.all_tasks()]
            os.killpg(os.getpid(), signal.SIGTERM)
            while True:
                try:
                    os.waitpid(-1, 0)
                except ChildProcessError:
                    break

        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        [signal.signal(sig, cleanup_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    _conf = mu7d_config()

    log.basicConfig(
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s] [INI] [%(levelname)s] %(message)s",
        level=log.DEBUG if _conf["DEBUG"] else log.INFO,
    )

    banner = f"Movistar U7D v{_version}"
    log.info("=" * len(banner))
    log.info(banner)
    log.info("=" * len(banner))

    if not WIN32 and ("UID" in _conf or "GID" in _conf):
        log.info("Dropping privileges...")
        if "GID" in _conf:
            try:
                os.setgid(_conf["GID"])
            except PermissionError:
                log.warning(f"Could not drop privileges to UID {_conf['UID']}")
        if "UID" in _conf:
            try:
                os.setuid(_conf["UID"])
            except PermissionError:
                log.warning(f"Could not drop privileges to GID {_conf['GID']}")

    os.environ["PATH"] = "%s;%s" % (os.path.dirname(__file__), os.getenv("PATH"))
    os.environ["U7D_PARENT"] = str(os.getpid())

    prefix = [sys.executable] if EXT == ".py" else []

    epg_cmd = prefix + [f"movistar_epg{EXT}"]
    u7d_cmd = prefix + [f"movistar_u7d{EXT}"]

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".mu7d.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            asyncio.run(u7d_main())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
