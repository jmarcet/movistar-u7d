#!/usr/bin/env python3

import logging
import os
import sys
from contextlib import closing
from shutil import which
from socket import AF_INET, SOCK_DGRAM, socket

import tomli
from tomli import TOMLDecodeError

VERSION = "7.0alpha"

EXT = ".exe" if getattr(sys, "frozen", False) else ".py"
LINUX = sys.platform == "linux"
WIN32 = sys.platform == "win32"

ATOM = 188
BUFF = 65536
CHUNK = 1316
DATEFMT = "%Y-%m-%d %H:%M:%S"
DIV_LOG = "%-64s%40s"
DIV_ONE = "%-104s%s"
DIV_TWO = DIV_LOG + "%s"
FMT = "[%(asctime)s] [%(name)s] [%(levelname)8s] %(message)s"
IPTV_DNS = "172.26.23.3"
MIME_GUIDE = "application/xml"
MIME_M3U = "audio/x-mpegurl"
MIME_WEBM = "video/webm"
NFO_EXT = "-movistar.nfo"
UA = "libcurl-agent/1.0 [IAL] WidgetManager Safari/538.1 CAP:803fd12a 1"
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
    # 5066,# "La 1 HD"
    4455,  # "La 2 HD"
    2524,  # "Antena 3 HD"
    1825,  # "Cuatro HD"
    1826,  # "Tele 5 HD"
    2526,  # "laSexta HD"
    # 4913,# "TVG HD"
    # 4912,# "TVG 2 HD"
    1221,  # "Paramount Network"
    844,  # _"TRECE"
    5275,  # "Energy HD"
    5276,  # "FDF HD"
    4714,  # "Neox HD"
    3223,  # "Atreseries HD"
    4063,  # "GOL PLAY HD"
    2544,  # "Teledeporte HD"
    3063,  # "Real Madrid TV HD"
    657,  # _"DMAX"
    3603,  # "DKISS"
    5277,  # "Divinity HD"
    4716,  # "Nova HD"
    4715,  # "MEGA HD"
    3185,  # "Be Mad HD"
    3443,  # "Ten"
    1616,  # "Disney Channel HD"
    5278,  # "Boing HD"
    4467,  # "Clan TVE HD"
    5106,  # "Canal 24 H. HD"
    4990,  # "El Toro TV HD"
    # 2275,# "Canal Sur Andalucía"
    # 2273,# "TVG Europa"
    # 4299,# "Canal Extremadura SAT"
    # 2271,# "TV3CAT"
    # 2269,# "ETB Basque"
    # 2743,# "Aragón TV Int"
    # 5087,# "Telemadrid INT HD"
    # 3103,# "Portada HD"
}


def add_logfile(logger, logfile, loglevel):
    try:
        fh = logging.FileHandler(logfile)
        fh.setFormatter(logging.Formatter(fmt=FMT, datefmt=DATEFMT))
        fh.setLevel(loglevel)
        logger.addHandler(fh)
    except (FileNotFoundError, OSError, PermissionError):
        return -1


def _get_lan_ip():
    with closing(socket(AF_INET, SOCK_DGRAM)) as sock:
        sock.connect(("8.8.8.8", 53))
        return sock.getsockname()[0]


def _mu7d_config():  # pylint: disable=too-many-branches
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
    except (AttributeError, OSError, PermissionError, TOMLDecodeError, TypeError, ValueError) as ex:
        return {"Exception": ex}

    if "HOME" not in conf or not os.path.exists(conf["HOME"]):
        conf["HOME"] = os.getenv("HOME", os.getenv("USERPROFILE"))

    if "UID" not in conf or not isinstance(conf["UID"], int):
        conf["UID"] = 65534

    if "GID" not in conf or not isinstance(conf["GID"], int):
        conf["GID"] = 65534

    if "COMSKIP" not in conf or not which("comskip") or not isinstance(conf["COMSKIP"], str):
        conf["COMSKIP"] = None
    else:
        if WIN32:
            conf["COMSKIP"] += " --quiet"
        conf["COMSKIP"] = conf["COMSKIP"].split()

    if "DEBUG" not in conf or not isinstance(conf["DEBUG"], bool):
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
        conf["LAN_IP"] = _get_lan_ip()

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

    if not WIN32 and conf["IPTV_BW_SOFT"]:
        conf["RECORDINGS_PROCESSES"] = 9999
    elif "RECORDINGS_PROCESSES" not in conf or not isinstance(conf["RECORDINGS_PROCESSES"], int):
        conf["RECORDINGS_PROCESSES"] = 4

    if "RECORDINGS_REINDEX" not in conf or not isinstance(conf["RECORDINGS_REINDEX"], bool):
        conf["RECORDINGS_REINDEX"] = False

    if "RECORDINGS_TMP" not in conf:
        conf["RECORDINGS_TMP"] = None
    else:
        conf["RECORDINGS_TMP"] = conf["RECORDINGS_TMP"].rstrip("/").rstrip("\\")

    if "RECORDINGS_TRANSCODE_INPUT" not in conf:
        conf["RECORDINGS_TRANSCODE_INPUT"] = []
    else:
        conf["RECORDINGS_TRANSCODE_INPUT"] = conf["RECORDINGS_TRANSCODE_INPUT"].split()

    if "RECORDINGS_TRANSCODE_OUTPUT" not in conf:
        conf["RECORDINGS_TRANSCODE_OUTPUT"] = "-c copy -c:a:0 libfdk_aac -c:a:1 libfdk_aac -b:a 128k"
        conf["RECORDINGS_TRANSCODE_OUTPUT"] += f" -packetsize {CHUNK} -ts_packetsize {CHUNK} -seek2any 1"
    conf["RECORDINGS_TRANSCODE_OUTPUT"] = conf["RECORDINGS_TRANSCODE_OUTPUT"].split()

    if "RECORDINGS_UPGRADE" not in conf or conf["RECORDINGS_UPGRADE"] not in (-2, -1, 0, 1, 2):
        conf["RECORDINGS_UPGRADE"] = 0

    if "U7D_PORT" not in conf or not isinstance(conf["U7D_PORT"], int):
        conf["U7D_PORT"] = 8888

    conf["CACHE_DIR"] = os.path.join(conf["HOME"], ".mu7d")
    conf["TMP_DIR"] = os.getenv("TMP", os.getenv("TMPDIR", "/tmp"))  # nosec B108

    conf["CHANNELS"] = os.path.join(conf["HOME"], "MovistarTV.m3u")
    conf["CHANNELS_CLOUD"] = os.path.join(conf["HOME"], "MovistarTVCloud.m3u")
    conf["CHANNELS_LOCAL"] = os.path.join(conf["HOME"], "MovistarTVLocal.m3u")
    conf["GUIDE"] = os.path.join(conf["HOME"], "guide.xml")
    conf["GUIDE_CLOUD"] = os.path.join(conf["HOME"], "cloud.xml")
    conf["GUIDE_LOCAL"] = os.path.join(conf["HOME"], "local.xml")
    conf["U7D_URL"] = f"http://{conf['LAN_IP']}:{conf['U7D_PORT']}"

    return conf


CONF = _mu7d_config()
