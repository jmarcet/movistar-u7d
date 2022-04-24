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

from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError
from contextlib import closing
from filelock import FileLock, Timeout
from xml.sax.saxutils import unescape  # nosec B406

if hasattr(asyncio, "exceptions"):
    from asyncio.exceptions import CancelledError
else:
    from asyncio import CancelledError


_version = "4.9"

EXT = ".exe" if getattr(sys, "frozen", False) else ".py"
WIN32 = sys.platform == "win32"

EPG_URL = "http://127.0.0.1:8889"
IPTV_DNS = "172.26.23.3"
MIME_M3U = "audio/x-mpegurl"
MIME_TS = "video/MP2T;audio/mp3"
MIME_WEBM = "video/webm"
TERMINATE = os.path.join(os.getenv("TMP"), ".mu7d.terminate") if WIN32 else None
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


async def launch(cmd):
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=os.path.dirname(__file__))
    try:
        return await proc.wait()
    except CancelledError:
        log.debug(f"Cancelled: {cmd}")
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        finally:
            return await proc.wait()


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


async def ongoing_vods(channel_id="", program_id="", filename="", _all=False, _fast=False):
    parent = os.getenv("U7D_PARENT") if not WIN32 else None
    family = psutil.Process(int(parent)).children(recursive=True) if parent else psutil.process_iter()

    if _fast:  # For U7D we just want to know if there are recordings in place
        return "ffmpeg" in str(family)

    regex = "(comskip|ffmpeg|mkvmerge|movistar_vod).*" if filename and not program_id else "movistar_vod.*"
    regex += "(" if program_id and filename else ""
    regex += f" {channel_id} {program_id}" if program_id else ""
    regex += "|" if program_id and filename else ""
    regex += " .+%s" % filename.replace("\\", "\\\\") if filename else ""
    regex += ")" if program_id and filename else ""

    vods = list(filter(None, [proc_grep(proc, regex) for proc in family]))
    if not _all:
        if filename and not program_id:
            return vods
        else:
            return [proc for proc in vods if "ffmpeg" in str(proc.children())]
    else:
        return "|".join([" ".join(proc.cmdline()).strip() for proc in vods])


def proc_grep(proc, regex):
    try:
        return proc if re.search(regex, " ".join(proc.cmdline())) else None
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.PermissionError):
        pass


def reaper(signum, sigframe):
    for child in psutil.Process().children():
        try:
            if child.status() == "zombie":
                child.wait()
        except psutil.NoSuchProcess:
            pass


async def u7d_main():
    prefix = [sys.executable] if EXT == ".py" else []

    epg_cmd = prefix + [f"movistar_epg{EXT}"]
    u7d_cmd = prefix + [f"movistar_u7d{EXT}"]

    epg_t = asyncio.create_task(launch(epg_cmd))
    u7d_t = asyncio.create_task(launch(u7d_cmd))

    while True:
        try:
            done, pending = await asyncio.wait(asyncio.all_tasks(), return_when=asyncio.FIRST_COMPLETED)
        except CancelledError:
            if not WIN32:
                break

        if WIN32:
            if not u7d_t.done():
                async with aiohttp.ClientSession() as session:
                    try:
                        await session.get(f"http://{_conf['LAN_IP']}:{_conf['U7D_PORT']}/terminate")
                    except (ClientOSError, ServerDisconnectedError):
                        u7d_t.cancel()
                await u7d_t
            cleanup()
            break

        epg_t = asyncio.create_task(launch(epg_cmd)) if epg_t in done else epg_t
        u7d_t = asyncio.create_task(launch(u7d_cmd)) if u7d_t in done else u7d_t


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
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            while True:
                try:
                    os.waitpid(-1, 0)
                except Exception:
                    break

        signal.signal(signal.SIGCHLD, reaper)
        [signal.signal(sig, cleanup_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    else:

        def cleanup():
            with open(TERMINATE, "wb") as f:
                f.write(b"")
            log.debug("cleanup WIN32")
            vods = []
            for proc in psutil.process_iter():
                try:
                    name = " ".join(proc.cmdline())
                    if "movistar_vod" in name:
                        vods.append(proc)
                        children = proc.children()
                        if children:
                            children[0].terminate()
                    elif "movistar_" in name:
                        proc.terminate()
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    pass
            if vods:
                log.debug(f"Waiting for: {vods}")
                [vod.wait() for vod in vods]
            os.remove(TERMINATE)

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

    if WIN32:
        if os.path.exists(TERMINATE):
            os.remove(TERMINATE)

    elif "UID" in _conf or "GID" in _conf:
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

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".mu7d.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            asyncio.run(u7d_main())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
