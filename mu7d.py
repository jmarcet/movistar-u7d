#!/usr/bin/env python3

import aiofiles
import asyncio
import logging
import os
import psutil
import re
import socket
import sys
import tomli
import urllib.parse
import xmltodict

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from asyncio.exceptions import CancelledError
from contextlib import closing
from datetime import datetime
from filelock import FileLock, Timeout
from glob import glob
from html import unescape
from shutil import which
from time import sleep


_version = "6.0alpha"

log = logging.getLogger("INI")


EXT = ".exe" if getattr(sys, "frozen", False) else ".py"
WIN32 = sys.platform == "win32"

ATOM = 188
BUFF = 65536
CHUNK = 1316
DATEFMT = "%Y-%m-%d %H:%M:%S"
DIV_LOG = "%-64s%40s"
DIV_ONE = "%-104s%s"
DIV_TWO = DIV_LOG + "%s"
EPG_URL = "http://127.0.0.1:8889"
FMT = "[%(asctime)s] [%(name)s] [%(levelname)8s] %(message)s"
IPTV_DNS = "172.26.23.3"
MIME_M3U = "audio/x-mpegurl"
MIME_WEBM = "video/webm"
NFO_EXT = "-movistar.nfo"
UA = "libcurl-agent/1.0 [IAL] WidgetManager Safari/538.1 CAP:803fd12a 1"
UA_U7D = f"movistar-u7d v{_version} [{sys.platform}] [{EXT}]"
URL_BASE = "http://html5-static.svc.imagenio.telefonica.net/appclientv/nux/incoming/epg"
URL_COVER = f"{URL_BASE}/covers/programmeImages/portrait/290x429"
URL_LOGO = f"{URL_BASE}/channelLogo"
URL_MVTV = "http://portalnc.imagenio.telefonica.net:2001/appserver/mvtv.do"
VID_EXTS = (".avi", ".mkv", ".mp4", ".mpeg", ".mpg", ".ts")
VID_EXTS_KEEP = (*VID_EXTS, ".tmp", ".tmp2")
YEAR_SECONDS = 365 * 24 * 60 * 60

DROP_KEYS = ["5S_signLanguage", "hasDolby", "isHdr", "isPromotional", "isuserfavorite", "lockdata", "logos"]
DROP_KEYS += ["opvr", "playcount", "recordingAllowed", "resume", "startOver", "watched", "wishListEnabled"]

episode_regex = re.compile(r"^.*(?:Ep[isode.]+) (\d+)$")
title_select_regex = re.compile(r".+ T\d+ .+")
title_1_regex = re.compile(r"(.+(?!T\d)) +T(\d+)(?: *Ep[isode.]+ (\d+))?[ -]*(.*)")
title_2_regex = re.compile(r"(.+(?!T\d))(?: +T(\d+))? *(?:[Ee]p[isode.]+|[Cc]ap[iítulo.]*) ?(\d+)[ .-]*(.*)")


def add_logfile(logger, logfile, loglevel):
    try:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(logging.Formatter(fmt=FMT, datefmt=DATEFMT))
        fh.setLevel(loglevel)
        logger.addHandler(fh)
    except FileNotFoundError:
        log.error(f"Cannot write logs to '{logfile}'")
        return


def cleanup_handler(signum=None, frame=None):
    [task.cancel() for task in asyncio.all_tasks()]


def find_free_port(iface=""):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind((iface, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def get_iptv_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.settimeout(1)
        try:
            s.connect((IPTV_DNS, 53))
            return s.getsockname()[0]
        except OSError:
            raise ValueError("Imposible conectar con DNS de Movistar IPTV")


def get_lan_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.connect(("8.8.8.8", 53))
        return s.getsockname()[0]


async def get_local_info(channel, timestamp, path, extended=False):
    try:
        nfo_file = path + NFO_EXT
        if not os.path.exists(nfo_file):
            return
        async with aiofiles.open(nfo_file, encoding="utf-8") as f:
            nfo = xmltodict.parse(await f.read())["metadata"]
        nfo.update({k: int(v) for k, v in nfo.items() if k in ("beginTime", "duration", "endTime")})
        if extended:
            _match = re.search(r"^(.+) (?:S(\d+)E(\d+)|Ep[isode.]+ (\d+))", nfo.get("originalName", nfo["name"]))
            if _match:
                is_serie = True
                serie = _match.groups()[0]
                season = int(_match.groups()[1] or nfo.get("season", 0))
                episode = int(_match.groups()[2] or _match.groups()[3] or nfo.get("episode", 0))
            else:
                is_serie = bool(nfo.get("seriesID") or nfo.get("episodeName"))
                serie = nfo.get("seriesName", "")
                season = int(nfo.get("season", 0))
                episode = int(nfo.get("episode", 0))
            nfo.update(
                {
                    "start": nfo["beginTime"],
                    "end": nfo["endTime"],
                    "full_title": nfo.get("originalName", nfo["name"]),
                    "age_rating": int(nfo.get("ageRatingID")),
                    "description": nfo.get("description") or nfo.get("synopsis") or "",
                    "is_serie": is_serie,
                    "serie": serie,
                    "season": season,
                    "episode": episode,
                    "serie_id": int(nfo.get("seriesID", 0)),
                }
            )
        return nfo
    except Exception as ex:
        log.error(f'Cannot get extended local metadata: [{channel}] [{timestamp}] "{path=}" => {repr(ex)}')


def get_safe_filename(filename):
    filename = filename.translate(str.maketrans("/:()", "_;[]"))
    return "".join(c for c in filename if c.isalnum() or c in " ,.;_-[]@#$%€").rstrip()


def get_title_meta(title, serie_id, service_id, genre):
    try:
        _t = unescape(title).replace("\n", " ").replace("\r", " ").strip()
    except TypeError:
        _t = title.replace("\n", " ").replace("\r", " ").strip()
    full_title = re.sub(r"(\d+)/(\d+)", r"\1\2", _t)

    if title_select_regex.match(full_title):
        x = title_1_regex.search(full_title)
    else:
        x = title_2_regex.search(full_title)

    is_serie = False
    season = episode = 0
    serie = episode_title = ""

    if x and x.groups():
        _x = x.groups()
        serie = _x[0] if _x[0] else full_title.split(" - ")[0] if " - " in full_title else full_title
        season = int(_x[1]) if _x[1] else season
        episode = int(_x[2]) if _x[2] else episode
        episode_title = _x[3] if _x[3] else episode_title

        is_serie = bool(serie and episode)
        serie, episode_title = [x.strip(":-/ ") for x in (serie, episode_title)]
        if serie.lower() == episode_title.lower():
            episode_title = ""

        if all((serie, season, episode)):
            full_title = "%s S%02dE%02d" % (serie, season, episode)
            if episode_title:
                if episode_regex.match(episode_title):
                    episode_title = ""
                else:
                    full_title += f" - {episode_title}"

    elif serie_id:
        is_serie = True
        if " - " in full_title:
            serie, episode_title = full_title.split(" - ", 1)
        else:
            serie = full_title

    # Boing (578) & MEGA (2863); Movies (genre[0] == "1")
    if all((service_id in (578, 2863), ": " in full_title, not episode_title, genre[0] != "1")):
        _match = re.match(r"^([^/]+): ([^a-z].+)", full_title)
        if _match:
            is_serie = True
            serie, episode_title = [x.strip(":-/ ") for x in _match.groups()]
            if season and episode:
                episode_title = re.sub(r" S\d+E\d+$", "", episode_title)
                full_title = "%s S%02dE%02d - %s" % (serie, season, episode, episode_title)
            else:
                if not episode:
                    _match = episode_regex.match(episode_title)
                    if _match:
                        episode = int(_match.groups()[0])
                full_title = "%s - %s" % (serie, episode_title)

    for item in (episode_title, full_title, serie):
        item = re.sub(r"\s+", " ", item)

    return {
        "full_title": full_title,
        "serie": serie,
        "season": season,
        "episode": episode,
        "episode_title": episode_title,
        "is_serie": is_serie,
    }


async def get_vod_info(session, channel, cloud, program):
    params = {"action": "getRecordingData" if cloud else "getCatchUpUrl"}
    params.update({"extInfoID": program, "channelID": channel, "mode": 1})

    msg = f"Could not get uri for [{channel:4}] [{program}]:"
    try:
        async with session.get(URL_MVTV, params=params) as r:
            res = await r.json()
        if res.get("resultData"):
            return res["resultData"]
        log.error(f"{msg} {res}")
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError, TypeError) as ex:
        log.error(f"{msg} {repr(ex)}")


def glob_safe(string, recursive=False):
    return glob(f"{string.replace('[', '?').replace(']', '?')}", recursive=recursive)


async def launch(cmd):
    cmd = [sys.executable, *cmd] if EXT == ".py" else cmd
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=os.path.dirname(__file__) if EXT == ".py" else None)
    try:
        return await proc.wait()
    except CancelledError:
        log.debug('Cancelled: "%s"' % cmd)
        try:
            proc.terminate()
        except ProcessLookupError:
            pass

        return await proc.wait()


def mu7d_config():
    confname = "mu7d.conf"
    fileconf = os.path.join(os.path.dirname(__file__), confname)

    if not WIN32:
        if os.path.exists(os.path.join(os.getenv("HOME"), confname)):
            fileconf = os.path.join(os.getenv("HOME"), confname)
        elif os.path.exists(os.path.join("/etc", confname)):
            fileconf = os.path.join("/etc", confname)
    elif os.path.exists(os.path.join(os.getenv("USERPROFILE"), confname)):
        fileconf = os.path.join(os.getenv("USERPROFILE"), confname)

    try:
        with open(fileconf, encoding="utf8") as f:
            conf = tomli.loads(f.read().lstrip("\ufeff"))
    except FileNotFoundError:
        conf = {}

    if "HOME" not in conf:
        conf["HOME"] = os.getenv("HOME", os.getenv("USERPROFILE"))

    if "UID" not in conf:
        conf["UID"] = 65534

    if "GID" not in conf:
        conf["GID"] = 65534

    if "COMSKIP" not in conf or not which("comskip"):
        conf["COMSKIP"] = None
    else:
        if WIN32:
            conf["COMSKIP"] += " --quiet"
        conf["COMSKIP"] = conf["COMSKIP"].split()

    if "DEBUG" not in conf:
        conf["DEBUG"] = False

    if "DROP_CHANNELS" not in conf:
        conf["DROP_CHANNELS"] = []
    elif any((not isinstance(x, int) for x in conf["DROP_CHANNELS"])):
        conf["DROP_CHANNELS"] = []

    if "EXTRA_CHANNELS" not in conf:
        conf["EXTRA_CHANNELS"] = []
    elif any((not isinstance(x, int) for x in conf["EXTRA_CHANNELS"])):
        conf["EXTRA_CHANNELS"] = []

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

    if "LOG_TO_FILE" not in conf:
        conf["LOG_TO_FILE"] = os.path.join(conf["HOME"], "mu7d.log")

    if "MKV_OUTPUT" not in conf:
        conf["MKV_OUTPUT"] = False

    if "NO_SUBS" not in conf:
        conf["NO_SUBS"] = False

    if "NO_VERBOSE_LOGS" not in conf:
        conf["NO_VERBOSE_LOGS"] = False

    if "RECORDINGS" not in conf or not all((which("ffmpeg"), which("ffprobe"))):
        conf["RECORDINGS"] = conf["RECORDINGS_M3U"] = None
    else:
        conf["RECORDINGS"] = conf["RECORDINGS"].rstrip("/").rstrip("\\")

    if "RECORDINGS_M3U" not in conf:
        conf["RECORDINGS_M3U"] = True

    if "RECORDINGS_REINDEX" not in conf:
        conf["RECORDINGS_REINDEX"] = False

    if "RECORDINGS_TRANSCODE_INPUT" not in conf:
        conf["RECORDINGS_TRANSCODE_INPUT"] = []
    else:
        conf["RECORDINGS_TRANSCODE_INPUT"] = conf["RECORDINGS_TRANSCODE_INPUT"].split()

    if "RECORDINGS_TRANSCODE_OUTPUT" not in conf:
        conf["RECORDINGS_TRANSCODE_OUTPUT"] = "-c copy -c:a:0 libfdk_aac -c:a:1 libfdk_aac -b:a 128k"
        conf["RECORDINGS_TRANSCODE_OUTPUT"] += f" -packetsize {CHUNK} -ts_packetsize {CHUNK} -seek2any 1"
    conf["RECORDINGS_TRANSCODE_OUTPUT"] = conf["RECORDINGS_TRANSCODE_OUTPUT"].split()

    if "RECORDINGS_TMP" not in conf:
        conf["RECORDINGS_TMP"] = None
    else:
        conf["RECORDINGS_TMP"] = conf["RECORDINGS_TMP"].rstrip("/").rstrip("\\")

    if "RECORDINGS_UPGRADE" not in conf:
        conf["RECORDINGS_UPGRADE"] = False

    if not WIN32 and conf["IPTV_BW_SOFT"]:
        conf["RECORDINGS_THREADS"] = 9999
    elif "RECORDINGS_THREADS" not in conf:
        conf["RECORDINGS_THREADS"] = 4

    if "U7D_PORT" not in conf:
        conf["U7D_PORT"] = 8888

    if "U7D_THREADS" not in conf:
        conf["U7D_THREADS"] = 1

    conf["CHANNELS"] = os.path.join(conf["HOME"], "MovistarTV.m3u")
    conf["CHANNELS_CLOUD"] = os.path.join(conf["HOME"], "MovistarTVCloud.m3u")
    conf["CHANNELS_LOCAL"] = os.path.join(conf["HOME"], "MovistarTVLocal.m3u")
    conf["GUIDE"] = os.path.join(conf["HOME"], "guide.xml")
    conf["GUIDE_CLOUD"] = os.path.join(conf["HOME"], "cloud.xml")
    conf["GUIDE_LOCAL"] = os.path.join(conf["HOME"], "local.xml")
    conf["U7D_URL"] = f"http://{conf['LAN_IP']}:{conf['U7D_PORT']}"

    return conf


async def ongoing_vods(channel_id="", program_id="", filename="", _all=False, _fast=False):
    parent = os.getenv("U7D_PARENT") if not WIN32 else None
    family = psutil.Process(int(parent)).children(recursive=True) if parent else psutil.process_iter()

    if _fast:  # For U7D we just want to know if there are recordings in place
        return "movistar_vod RE" in str(family)

    if not WIN32:
        regex = "^movistar_vod REC" if not _all else "^movistar_vod .*"
    else:
        regex = "^%smovistar_vod%s" % ((sys.executable.replace("\\", "\\\\") + " ") if EXT == ".py" else "", EXT)
    regex += "(" if filename and program_id else ""
    regex += f" {channel_id:4} {program_id}" if program_id else ""
    regex += "|" if filename and program_id else ""
    regex += " .+%s" % filename.replace("\\", "\\\\") if filename else ""
    regex += ")" if filename and program_id else ""

    vods = list(filter(None, [proc_grep(proc, regex) for proc in family]))
    return vods if not _all else "|".join([" ".join(proc.cmdline()).strip() for proc in vods])


def proc_grep(proc, regex):
    try:
        return proc if re.match(regex, " ".join(proc.cmdline())) else None
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.PermissionError):
        pass


def reaper(signum, sigframe):
    for child in psutil.Process().children():
        try:
            if child.status() == "zombie":
                child.wait()
        except psutil.NoSuchProcess:
            pass


def remove(*items):
    for item in items:
        try:
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                if not os.listdir(item):
                    os.rmdir(item)
                elif os.path.split(item)[1] != "metadata":
                    _glob = sorted(glob(f"{item}/**/*", recursive=True), reverse=True)
                    if not any(filter(lambda file: any(file.endswith(ext) for ext in VID_EXTS_KEEP), _glob)):
                        [os.remove(x) for x in filter(os.path.isfile, _glob)]
                        [os.rmdir(x) for x in filter(os.path.isdir, _glob)]
                        os.rmdir(item)
        except (FileNotFoundError, OSError, PermissionError):
            pass


def utime(timestamp, *items):
    [os.utime(item, (-1, timestamp)) for item in items if os.access(item, os.W_OK)]


async def u7d_main():
    u7d_cmd = [f"movistar_u7d{EXT}"]
    epg_cmd = [f"movistar_epg{EXT}"]

    u7d_t = asyncio.create_task(launch(u7d_cmd))
    epg_t = asyncio.create_task(launch(epg_cmd))

    while True:
        try:
            done, _ = await asyncio.wait(asyncio.all_tasks(), return_when=asyncio.FIRST_COMPLETED)
        except CancelledError:
            if WIN32:
                await u7d_t
            break

        u7d_t = asyncio.create_task(launch(u7d_cmd)) if u7d_t in done else u7d_t
        epg_t = asyncio.create_task(launch(epg_cmd)) if epg_t in done else epg_t


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

        def cancel_handler(signum, frame):
            cleanup_handler()
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            while True:
                try:
                    os.waitpid(-1, 0)
                except Exception:
                    break

        signal.signal(signal.SIGCHLD, reaper)
        [signal.signal(sig, cancel_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    def _check_iptv():
        iptv_iface_ip = _get_iptv_iface_ip()
        while True:
            uptime = int(datetime.now().timestamp() - psutil.boot_time())
            try:
                iptv_ip = get_iptv_ip()
                if not _conf["IPTV_IFACE"] or WIN32 or iptv_iface_ip and iptv_iface_ip == iptv_ip:
                    log.info(f"IPTV address: {iptv_ip}")
                    break
                log.info("IPTV address: waiting for interface to be routed...")
            except ValueError as ex:
                if uptime < 180:
                    log.info(ex)
                else:
                    raise
            sleep(5)

    def _check_ports():
        epg_uri = urllib.parse.urlparse(EPG_URL)
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            if s.connect_ex((epg_uri.hostname, epg_uri.port)) == 0:
                raise ValueError(f"El puerto {epg_uri.netloc} está ocupado")

        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            if s.connect_ex((_conf["LAN_IP"], _conf["U7D_PORT"])) == 0:
                raise ValueError(f"El puerto {_conf['LAN_IP']}:{_conf['U7D_PORT']} está ocupado")

    def _get_iptv_iface_ip():
        iptv_iface = _conf["IPTV_IFACE"]
        if iptv_iface:
            import netifaces

            while True:
                uptime = int(datetime.now().timestamp() - psutil.boot_time())
                try:
                    iptv = netifaces.ifaddresses(iptv_iface)[2][0]["addr"]
                    log.info(f"IPTV interface: {iptv_iface}")
                    return iptv
                except (KeyError, ValueError):
                    if uptime < 180:
                        log.info("IPTV interface: waiting for it...")
                        sleep(5)
                    else:
                        raise ValueError(f"Unable to get address from interface {iptv_iface}...")

    _conf = mu7d_config()

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=_conf["DEBUG"] and logging.DEBUG or logging.INFO)
    if _conf["LOG_TO_FILE"]:
        add_logfile(log, _conf["LOG_TO_FILE"], _conf["DEBUG"] and logging.DEBUG or logging.INFO)

    banner = f"Movistar U7D v{_version}"
    log.info("=" * len(banner))
    log.info(banner)
    log.info("=" * len(banner))

    if not WIN32 and any(("UID" in _conf, "GID" in _conf)):
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
    os.environ["PYTHONOPTIMIZE"] = "0" if _conf["DEBUG"] else "2"
    os.environ["U7D_PARENT"] = str(os.getpid())

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".mu7d.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            _check_iptv()
            _check_ports()
            asyncio.run(u7d_main())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except ValueError as ex:
        log.critical(ex)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
