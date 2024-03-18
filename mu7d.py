#!/usr/bin/env python3

import aiohttp
import aiofiles
import asyncio
import logging
import os
import psutil
import re
import socket
import sys
import tomli
import ujson
import urllib.parse
import xmltodict

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from asyncio.exceptions import CancelledError
from contextlib import closing
from datetime import datetime
from filelock import FileLock, Timeout
from glob import glob
from html import unescape
from json import JSONDecodeError
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
URL_FANART = "http://recortes.svc.imagenio.telefonica.net/recorte/n/bigtv_fanart"
URL_LOGO = f"{URL_BASE}/channelLogo"
VID_EXTS = (".avi", ".mkv", ".mp4", ".mpeg", ".mpg", ".ts")
VID_EXTS_KEEP = (*VID_EXTS, ".tmp", ".tmp2")
YEAR_SECONDS = 365 * 24 * 60 * 60

DROP_KEYS = ("5S_signLanguage", "hasDolby", "isHdr", "isPromotional", "isuserfavorite", "lockdata", "logos")
DROP_KEYS += ("opvr", "playcount", "recordingAllowed", "resume", "startOver", "watched", "wishListEnabled")

XML_INT_KEYS = ("ageRatingID", "beginTime", "duration", "endTime", "expDate")
XML_INT_KEYS += ("productType", "rating", "serviceID", "serviceUID", "themeID")

END_POINTS = (
    "http://portalnc.imagenio.telefonica.net:2001",
    "http://asiptvnc.imagenio.telefonica.net:2070",
    "http://reg360.imagenio.telefonica.net:2070",
    "https://auraiptv.imagenio.telefonica.net",
    "https://oauth4p.imagenio.telefonica.net",
    "http://asfenc.imagenio.telefonica.net:2001",
)

END_POINTS_FILE = "mu7d.endpoints"

EPG_CHANNELS = {
    # 5066 # _ "La 1 HD"
    4455,  # _ "La 2 HD"
    2524,  # _ "Antena 3 HD"
    1825,  # _ "Cuatro HD"
    1826,  # _ "Tele 5 HD"
    2526,  # _ "laSexta HD"
    # 4913 # _ "TVG HD"
    # 4912 # _ "TVG 2 HD"
    1221,  # _ "Paramount Network"
    844,  # __ "TRECE"
    884,  # __ "Energy"
    # 747 # __ "FDF"
    4714,  # _ "Neox HD"
    3223,  # _ "Atreseries HD"
    4063,  # _ "GOL PLAY HD"
    2544,  # _ "Teledeporte HD"
    3063,  # _ "Real Madrid TV HD"
    657,  # __ "DMAX"
    3603,  # _ "DKISS"
    663,  # __ "Divinity"
    4716,  # _ "Nova HD"
    4715,  # _ "MEGA HD"
    3185,  # _ "BeMad HD"
    3443,  # _ "Ten"
    1616,  # _ "Disney Channel HD"
    578,  # __ "Boing"
    4467,  # _ "Clan TVE HD"
    5106,  # _ "Canal 24 H. HD"
    4990,  # _ "El Toro TV HD"
    # 2275 # _ "Canal Sur Andalucía"
    # 2273 # _ "TVG Europa"
    # 4299 # _ "Canal Extremadura SAT"
    # 2271 # _ "TV3CAT"
    # 2269 # _ "ETB Basque."
    # 2743 # _ "Aragón TV Int"
    # 5087 # _ "Telemadrid INT HD"
    5029,  # _ "8tv"
    3103,  # _ "Movistar Plus+"
}

anchored_regex = re.compile(r"^(.+ - \d{8}_)\d{4}$")
episode_regex = re.compile(r"^.*(?:Ep[isode.]+) (\d+)$")
title_select_regex = re.compile(r".+ T\d+ .+")
title_1_regex = re.compile(r"(.+(?!T\d)) +T(\d+)(?: *Ep[isode.]+ (\d+))?[ -]*(.*)")
title_2_regex = re.compile(r"(.+(?!T\d))(?: +T(\d+))? *(?:[Ee]p[isode.]+|[Cc]ap[iítulo.]*) ?(\d+)[ .-]*(.*)")


class IPTVNetworkError(Exception):
    """Connectivity with Movistar IPTV network error"""


class LocalNetworkError(Exception):
    """Local network error"""


def add_logfile(logger, logfile, loglevel):
    try:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(logging.Formatter(fmt=FMT, datefmt=DATEFMT))
        fh.setLevel(loglevel)
        logger.addHandler(fh)
    except (FileNotFoundError, OSError, PermissionError):
        log.error(f"Cannot write logs to '{logfile}'")
        return


def cleanup_handler(signum=None, frame=None):
    [task.cancel() for task in asyncio.all_tasks()]


def find_free_port(iface=""):
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.bind((iface, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


async def get_end_point(home, _log):
    end_points = END_POINTS
    ep_path = os.path.join(home, ".xmltv", "cache", END_POINTS_FILE)
    if os.path.exists(ep_path):
        try:
            async with aiofiles.open(ep_path) as f:
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

    _log.debug("Failed to verify end_point. Using default one.")
    return end_points[0] + "/appserver/mvtv.do"


def get_iptv_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.settimeout(1)
        try:
            s.connect((IPTV_DNS, 53))
            return s.getsockname()[0]
        except OSError:
            raise IPTVNetworkError("Imposible conectar con DNS de Movistar IPTV")


def get_lan_ip():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
        s.connect(("8.8.8.8", 53))
        return s.getsockname()[0]


async def get_local_info(channel, timestamp, path, _log, extended=False):
    try:
        nfo_file = path + NFO_EXT
        if not os.path.exists(nfo_file):
            return
        async with aiofiles.open(nfo_file, encoding="utf-8") as f:
            nfo = xmltodict.parse(await f.read(), postprocessor=pp_xml)["metadata"]
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
        _log.error(f'Cannot get extended local metadata: [{channel}] [{timestamp}] "{path=}" => {repr(ex)}')


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
        serie, episode_title = (x.strip(":-/ ") for x in (serie, episode_title))
        serie = re.sub(r"([^,.])[,.]$", r"\1", serie)
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
            serie, episode_title = (x.strip(":-/ ") for x in _match.groups())
            serie = re.sub(r"([^,.])[,.]$", r"\1", serie)
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


async def get_vod_info(session, endpoint, channel, cloud, program, _log):
    params = {"action": "getRecordingData" if cloud else "getCatchUpUrl"}
    params.update({"extInfoID": program, "channelID": channel, "mode": 1})

    try:
        async with session.get(endpoint, params=params) as r:
            res = await r.json()
        if res.get("resultData"):
            return res["resultData"]
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError, TypeError) as ex:
        res = ex
    _log.error(f"[{channel:4}] [{program}]: NOT AVAILABLE => {res}")


def glob_safe(string, recursive=False):
    return glob(f"{string.replace('[', '?').replace(']', '?')}", recursive=recursive)


def keys_to_int(dictionary):
    return {int(k) if k.isdigit() else k: v for k, v in dictionary.items()}


async def launch(cmd):
    cmd = (sys.executable, *cmd) if EXT == ".py" else cmd
    proc = await asyncio.create_subprocess_exec(*cmd, cwd=os.path.dirname(__file__) if EXT == ".py" else None)
    try:
        return await proc.wait()
    except CancelledError:
        log.debug('Cancelled: "%s"' % str(cmd))
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
    except (AttributeError, FileNotFoundError, OSError, PermissionError):
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

    if "EPG_CHANNELS" not in conf:
        conf["EPG_CHANNELS"] = EPG_CHANNELS
    else:
        conf["EPG_CHANNELS"] = set(conf["EPG_CHANNELS"])

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

    if "MKV_OUTPUT" not in conf or not isinstance(conf["MKV_OUTPUT"], bool):
        conf["MKV_OUTPUT"] = False

    if "NO_SUBS" not in conf or not isinstance(conf["NO_SUBS"], bool):
        conf["NO_SUBS"] = False

    if "OTT_HACK" not in conf or not isinstance(conf["OTT_HACK"], bool):
        conf["OTT_HACK"] = False

    if "RECORDINGS" not in conf or not all((which("ffmpeg"), which("ffprobe"))):
        conf["RECORDINGS"] = conf["RECORDINGS_M3U"] = None
    else:
        conf["RECORDINGS"] = conf["RECORDINGS"].rstrip("/").rstrip("\\")

    if "RECORDINGS_M3U" not in conf or not isinstance(conf["RECORDINGS_M3U"], bool):
        conf["RECORDINGS_M3U"] = False

    if "RECORDINGS_REINDEX" not in conf or not isinstance(conf["RECORDINGS_REINDEX"], bool):
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

    if "RECORDINGS_UPGRADE" not in conf or conf["RECORDINGS_UPGRADE"] not in (-2, -1, 0, 1, 2):
        conf["RECORDINGS_UPGRADE"] = 0

    if not WIN32 and conf["IPTV_BW_SOFT"]:
        conf["RECORDINGS_PROCESSES"] = 9999
    elif "RECORDINGS_PROCESSES" not in conf or not isinstance(conf["RECORDINGS_PROCESSES"], int):
        conf["RECORDINGS_PROCESSES"] = 4

    if "U7D_PORT" not in conf or not isinstance(conf["U7D_PORT"], int):
        conf["U7D_PORT"] = 8888

    if "U7D_PROCESSES" not in conf or not isinstance(conf["U7D_PROCESSES"], int):
        conf["U7D_PROCESSES"] = 1

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

    if filename:
        filename = filename.translate(str.maketrans("[]", ".."))
        _match = anchored_regex.match(filename)
        if _match:
            filename = _match.groups()[0]

    regex += "(" if filename and program_id else ""
    regex += f" {channel_id:4} {program_id}" if program_id else ""
    regex += "|" if filename and program_id else ""
    regex += " .+%s" % filename.replace("\\", "\\\\") if filename else ""
    regex += ")" if filename and program_id else ""

    vods = filter(None, map(lambda proc: proc_grep(proc, regex), family))
    return tuple(vods) if not _all else "|".join([" ".join(proc.cmdline()).strip() for proc in vods])


def pp_xml(path, key, value):
    return key, int(value) if key in XML_INT_KEYS else value


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
    u7d_cmd = (f"movistar_u7d{EXT}",)
    epg_cmd = (f"movistar_epg{EXT}",)

    u7d_t = asyncio.create_task(launch(u7d_cmd))
    epg_t = asyncio.create_task(launch(epg_cmd))

    while True:
        try:
            done, _ = await asyncio.wait(asyncio.all_tasks(), return_when=asyncio.FIRST_COMPLETED)
        except CancelledError:
            if not WIN32:
                break
            await u7d_t

        if WIN32:
            input("\nPulsa una tecla para salir...")
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
            except IPTVNetworkError as err:
                if uptime < 180:
                    log.info(err)
                else:
                    raise
            sleep(5)

    def _check_ports():
        epg_uri = urllib.parse.urlparse(EPG_URL)
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            if s.connect_ex((epg_uri.hostname, epg_uri.port)) == 0:
                raise LocalNetworkError(f"El puerto {epg_uri.netloc} está ocupado")

        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            if s.connect_ex((_conf["LAN_IP"], _conf["U7D_PORT"])) == 0:
                raise LocalNetworkError(f"El puerto {_conf['LAN_IP']}:{_conf['U7D_PORT']} está ocupado")

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
                        raise LocalNetworkError(f"Unable to get address from interface {iptv_iface}...")

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
            del _conf
            asyncio.run(u7d_main())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except (IPTVNetworkError, LocalNetworkError) as err:
        log.critical(err)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
