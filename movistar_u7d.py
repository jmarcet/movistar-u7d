#!/usr/bin/env python3

import aiohttp
import asyncio
import asyncio_dgram
import os
import signal
import socket
import subprocess
import sys
import timeit
import ujson
import urllib.parse

from collections import namedtuple
from contextlib import closing
from sanic import Sanic, exceptions, response
from sanic.compat import open_async, stat_async
from sanic.handlers import ContentRangeHandler
from sanic.log import error_logger, logger as log, LOGGING_CONFIG_DEFAULTS
from sanic.models.server_types import ConnInfo
from sanic.server import HttpProtocol
from sanic.touchup.meta import TouchUpMeta

from movistar_epg import get_ffmpeg_procs
from vod import VodData, VodLoop, VodSetup, find_free_port, COVER_URL, IMAGENIO_URL, UA, check_dns


if os.name != "nt":
    from setproctitle import setproctitle

    setproctitle("movistar_u7d")

if "LAN_IP" in os.environ:
    SANIC_HOST = os.getenv("LAN_IP")
else:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 53))
    SANIC_HOST = s.getsockname()[0]
    s.close

HOME = os.getenv("HOME", os.getenv("HOMEPATH"))
CHANNELS = os.path.join(HOME, "MovistarTV.m3u")
CHANNELS_CLOUD = os.path.join(HOME, "cloud.m3u")
CHANNELS_RECORDINGS = os.path.join(HOME, "recordings.m3u")
DEBUG = bool(int(os.getenv("DEBUG", 0)))
GUIDE = os.path.join(HOME, "guide.xml")
GUIDE_CLOUD = os.path.join(HOME, "cloud.xml")
IPTV_BW = int(os.getenv("IPTV_BW", "0"))
IPTV_BW = 85000 if IPTV_BW > 90000 else IPTV_BW
MIME_M3U = "audio/x-mpegurl"
MIME_TS = "video/MP2T;audio/mp3"
MIME_WEBM = "video/webm"
MP4_OUTPUT = bool(os.getenv("MP4_OUTPUT", False))
NETWORK_FSIGNAL = os.path.join(os.getenv("TMP", "/tmp"), ".u7d_bw")
RECORDINGS = os.getenv("RECORDINGS", None)
SANIC_EPG_URL = "http://127.0.0.1:8889"
SANIC_PORT = int(os.getenv("SANIC_PORT", "8888"))
SANIC_URL = f"http://{SANIC_HOST}:{SANIC_PORT}"
SANIC_THREADS = int(os.getenv("SANIC_THREADS", "4"))
VERBOSE_LOGS = bool(int(os.getenv("VERBOSE_LOGS", 1)))
VOD_EXEC = "vod.exe" if os.path.exists("vod.exe") else "vod.py"

YEAR_SECONDS = 365 * 24 * 60 * 60

LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS["formatters"]["generic"]["format"] = "%(asctime)s [U7D] [%(levelname)s] %(message)s"
LOG_SETTINGS["formatters"]["generic"]["datefmt"] = LOG_SETTINGS["formatters"]["access"][
    "datefmt"
] = "[%Y-%m-%d %H:%M:%S]"

_CHANNELS = {}
_IPTV = _SESSION = _SESSION_LOGOS = None
_NETWORK_SATURATED = False
_RESPONSES = []

VodArgs = namedtuple("Vod", ["channel", "broadcast", "iptv_ip", "client_ip", "client_port", "start", "cloud"])

app = Sanic("movistar_u7d", log_config=LOG_SETTINGS)
app.config.update(
    {
        "FALLBACK_ERROR_FORMAT": "json",
        "GRACEFUL_SHUTDOWN_TIMEOUT": 0,
        "REQUEST_TIMEOUT": 1,
        "RESPONSE_TIMEOUT": 1,
    }
)
app.ctx.vod_client = _t_tp = None


@app.listener("before_server_start")
async def before_server_start(app, loop):
    global _CHANNELS, _IPTV, _SESSION, _t_tp

    while True:
        _IPTV = check_dns()
        if _IPTV:
            break
        else:
            await asyncio.sleep(5)

    conn = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1)
    _SESSION = aiohttp.ClientSession(connector=conn, json_serialize=ujson.dumps)

    if not app.ctx.vod_client:
        conn_vod = aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS)
        app.ctx.vod_client = aiohttp.ClientSession(connector=conn_vod, headers={"User-Agent": UA})

    while True:
        try:
            async with _SESSION.get(f"{SANIC_EPG_URL}/channels/") as r:
                if r.status == 200:
                    _CHANNELS = await r.json()
                    break
                else:
                    log.error("Failed to get channel list from EPG service")
                    await asyncio.sleep(5)
        except (ConnectionRefusedError, aiohttp.client_exceptions.ClientConnectorError):
            log.debug("Waiting for EPG service...")
            await asyncio.sleep(5)

    if IPTV_BW and os.path.exists(NETWORK_FSIGNAL):
        async with await open_async(NETWORK_FSIGNAL) as f:
            iface_rx = await f.read()
        log.info("Setting dynamic limit of clients from " + iface_rx.split("/")[4] + " bw")
        _t_tp = app.add_task(throughput(iface_rx))


@app.listener("before_server_stop")
async def before_server_stop(app, loop):
    global _RESPONSES
    for req_ip, raw_url, resp in _RESPONSES:
        try:
            await resp.eof()
            log.info(f"[{req_ip}] " f"{SANIC_URL if VERBOSE_LOGS else ''}" f"{raw_url} -> Stopped 2")
        except AttributeError:
            pass


@app.listener("after_server_stop")
async def after_server_stop(app, loop):
    try:
        _t_tp.cancel()
        await asyncio.wait({_t_tp})
    except (AttributeError, ProcessLookupError):
        pass


@app.get("/canales.m3u")
@app.get("/channels.m3u")
@app.get("/MovistarTV.m3u")
async def handle_channels(request):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    if not os.path.exists(CHANNELS):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")
    return await response.file(CHANNELS, mime_type=MIME_M3U)


@app.get("/cloud.m3u")
@app.get("/nube.m3u")
async def handle_channels_cloud(request):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    if not os.path.exists(CHANNELS_CLOUD):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")
    return await response.file(CHANNELS_CLOUD, mime_type=MIME_M3U)


@app.get("/grabaciones.m3u")
@app.get("/recordings.m3u")
async def handle_channels_recordings(request):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    if not os.path.exists(CHANNELS_RECORDINGS):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")
    return await response.file(CHANNELS_RECORDINGS, mime_type=MIME_M3U)


@app.get("/guia.xml")
@app.get("/guide.xml")
async def handle_guide(request):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    if not os.path.exists(GUIDE):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")
    return await response.file(GUIDE)


@app.get("/cloud.xml")
@app.get("/nube.xml")
async def handle_guide_cloud(request):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    if not os.path.exists(GUIDE_CLOUD):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")
    return await response.file(GUIDE_CLOUD)


@app.route("/Covers/<path:int>/<cover>", methods=["GET", "HEAD"])
@app.route("/Logos/<logo>", methods=["GET", "HEAD"])
async def handle_logos(request, cover=None, logo=None, path=None):
    log.debug(f"[{request.ip}] {request.method} {request.url}")
    if logo and logo.split(".")[0].isdigit():
        orig_url = f"{IMAGENIO_URL}/channelLogo/{logo}"
    elif path and cover and cover.split(".")[0].isdigit():
        orig_url = f"{COVER_URL}/{path}/{cover}"
    else:
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type="image/jpeg", status=200)

    global _SESSION_LOGOS
    if not _SESSION_LOGOS:
        headers = {"User-Agent": "MICA-IP-STB"}
        _SESSION_LOGOS = aiohttp.ClientSession(headers=headers)

    async with _SESSION_LOGOS.get(orig_url) as r:
        if r.status == 200:
            logo_data = await r.read()
            headers = {}
            headers.setdefault("Content-Disposition", f'attachment; filename="{logo}"')
            return response.HTTPResponse(
                body=logo_data, content_type="image/jpeg", headers=headers, status=200
            )
        else:
            raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")


@app.route("/<channel_id:int>/live", methods=["GET", "HEAD"])
async def handle_channel(request, channel_id):
    _start = timeit.default_timer()
    _raw_url = request.raw_url.decode()
    _sanic_url = (SANIC_URL + _raw_url + " ") if VERBOSE_LOGS else ""
    if _NETWORK_SATURATED:
        procs = await get_ffmpeg_procs()
        if procs:
            os.kill(int(procs[-1].split()[0]), signal.SIGINT)
            log.warning(
                f'[{request.ip}] {_raw_url} -> Killed ffmpeg "' + procs[-1].split(RECORDINGS)[1] + '"'
            )
        else:
            log.warning(f"[{request.ip}] {_raw_url} -> Network Saturated")
            raise exceptions.ServiceUnavailable("Network Saturated")
    try:
        name, mc_grp, mc_port = [_CHANNELS[str(channel_id)][t] for t in ["name", "address", "port"]]
    except (AttributeError, KeyError):
        log.error(f"{_raw_url} not found")
        raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((mc_grp if os.name != "nt" else "", int(mc_port)))
    sock.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(mc_grp) + socket.inet_aton(_IPTV)
    )

    stream = await asyncio_dgram.from_socket(sock)
    try:
        _response = await request.respond(content_type=MIME_TS)
        await _response.send((await stream.recv())[0][28:])
    except asyncio.exceptions.TimeoutError:
        log.error(f"{_raw_url} not found")
        raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    _lat = timeit.default_timer() - _start
    asyncio.create_task(
        _SESSION.post(
            f"{SANIC_EPG_URL}/prom_event/add",
            json={
                "method": "live",
                "endpoint": f"{name} _ {request.ip} _ ",
                "channel_id": channel_id,
                "msg": f"[{request.ip}] -> Playing {_sanic_url}[{_lat:1.4}s]",
                "id": _start,
                "lat": _lat,
            },
        )
    )

    try:
        while True:
            await _response.send((await stream.recv())[0][28:])
    finally:
        try:
            await _response.eof()
        finally:
            stream.close()
            asyncio.create_task(
                _SESSION.post(
                    f"{SANIC_EPG_URL}/prom_event/remove",
                    json={
                        "method": "live",
                        "endpoint": f"{name} _ {request.ip} _ ",
                        "channel_id": channel_id,
                        "msg": f"[{request.ip}] -> Stopped {_sanic_url}[{_lat:1.4}s]",
                        "id": _start,
                    },
                )
            )


@app.route("/<channel_id:int>/<url>", methods=["GET", "HEAD"])
async def handle_flussonic(request, channel_id, url, cloud=False):
    _start = timeit.default_timer()
    _raw_url = request.raw_url.decode()
    _sanic_url = (SANIC_URL + _raw_url + " ") if VERBOSE_LOGS else ""
    procs = None
    if _NETWORK_SATURATED:
        procs = await get_ffmpeg_procs()
        if not procs:
            log.warning(f"[{request.ip}] {_raw_url} -> Network Saturated")
            raise exceptions.ServiceUnavailable("Network Saturated")
    try:
        async with _SESSION.get(
            f"{SANIC_EPG_URL}/program_id/{channel_id}/{url}" + ("?cloud=1" if cloud else "")
        ) as r:
            channel, program_id, start, duration, offset = (await r.json()).values()
    except (AttributeError, KeyError, ValueError):
        log.error(f"{_raw_url} not found")
        raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    client_port = find_free_port(_IPTV)

    record = request.args.get("record", 0)
    if procs:
        if record:
            log.warning(f"[{request.ip}] {_raw_url} -> Network Saturated")
            raise exceptions.ServiceUnavailable("Network Saturated")
        else:
            os.kill(int(procs[-1].split()[0]), signal.SIGINT)
            log.warning(
                f'[{request.ip}] {_raw_url} -> Killed ffmpeg "' + procs[-1].split(RECORDINGS)[1] + '"'
            )
    elif record:
        cmd = (
            f"{VOD_EXEC} {channel_id} {program_id} -s {offset}"
            f" -p {client_port} -i {request.ip} -a {_IPTV}"
        )
        record = int(record)
        record_time = record if record > 1 else duration - offset

        cmd += f" -t {record_time} -w"
        if cloud:
            cmd += " --cloud"
        if MP4_OUTPUT or request.args.get("mp4", False):
            cmd += " --mp4"
        if request.args.get("vo", False):
            cmd += " --vo"

        if os.name != "nt":
            signal.signal(signal.SIGCHLD, signal.SIG_IGN)

        log.debug(f"Launching {cmd}")
        subprocess.Popen(cmd.split())
        return response.json(
            {
                "status": "OK",
                "channel_id": channel_id,
                "program_id": program_id,
                "start": start,
                "offset": offset,
                "time": record_time,
            }
        )

    _args = VodArgs(channel_id, program_id, _IPTV, request.ip, client_port, offset, cloud)
    vod_data = await VodSetup(_args, app.ctx.vod_client)
    if not isinstance(vod_data, VodData):
        if vod_data:
            raise exceptions.ServiceUnavailable("Movistar IPTV catchup service DOWN")
        else:
            raise exceptions.NotFound(f"Requested URL {_raw_url} not found")
    vod = asyncio.create_task(VodLoop(_args, vod_data))
    _endpoint = _CHANNELS[str(channel_id)]["name"] + f" _ {request.ip} _ "
    with closing(await asyncio_dgram.bind((_IPTV, client_port))) as stream:
        try:
            _response = await request.respond(content_type=MIME_TS)
            await _response.send((await stream.recv())[0])
        except asyncio.exceptions.TimeoutError:
            log.error(f"NOT_AVAILABLE: [{channel}] [{channel_id}] [{start}]")
            raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

        _lat = timeit.default_timer() - _start
        _RESPONSES.append((request.ip, _raw_url, _response))
        asyncio.create_task(
            _SESSION.post(
                f"{SANIC_EPG_URL}/prom_event/add",
                json={
                    "method": "catchup",
                    "endpoint": _endpoint,
                    "channel_id": channel_id,
                    "url": url,
                    "msg": f"[{request.ip}] -> Playing {_sanic_url}[{_lat:1.4}s]",
                    "id": _start,
                    "cloud": cloud,
                    "lat": _lat,
                },
            )
        )

        try:
            while True:
                await _response.send((await stream.recv())[0])
        finally:
            try:
                await _response.eof()
            finally:
                _RESPONSES.remove((request.ip, _raw_url, _response))
                asyncio.create_task(
                    _SESSION.post(
                        f"{SANIC_EPG_URL}/prom_event/remove",
                        json={
                            "method": "catchup",
                            "endpoint": _endpoint,
                            "channel_id": channel_id,
                            "url": url,
                            "msg": f"[{request.ip}] -> Stopped {_sanic_url}[{_lat:1.4}s]",
                            "id": _start,
                            "cloud": cloud,
                            "offset": timeit.default_timer() - _start,
                        },
                    )
                )
                try:
                    vod.cancel()
                    await asyncio.wait({vod})
                except (ProcessLookupError, TypeError):
                    pass


@app.route("/cloud/<channel_id:int>/<url>", methods=["GET", "HEAD"])
async def handle_flussonic_cloud(request, channel_id, url):
    if url == "live":
        return await handle_channel(request, channel_id)
    return await handle_flussonic(request, channel_id, url, cloud=True)


@app.route("/recording/", methods=["GET", "HEAD"])
async def handle_recording(request):
    if not RECORDINGS:
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")

    _path = urllib.parse.unquote(request.raw_url.decode().split("/recording/")[1])
    file = os.path.join(RECORDINGS, _path[1:])
    if os.path.exists(file):
        if request.method == "HEAD":
            return response.HTTPResponse(status=200)

        ext = os.path.splitext(file)[1]
        if ext == ".jpg":
            return await response.file(file)

        elif ext in (".avi", ".mkv", ".mp4", ".mpeg", ".mpg", ".ts"):
            try:
                _range = ContentRangeHandler(request, await stat_async(file))
            except exceptions.HeaderNotFound:
                _range = None

            return await response.file_stream(file, mime_type=MIME_WEBM, _range=_range)

    raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")


@app.get("/favicon.ico")
async def handle_notfound(request):
    return response.empty(404)


@app.get("/metrics")
async def handle_prometheus(request):
    try:
        async with _SESSION.get(f"{SANIC_EPG_URL}/metrics") as r:
            return response.text((await r.read()).decode())
    except (aiohttp.client_exceptions.ClientConnectorError, AttributeError):
        raise exceptions.ServiceUnavailable("Not available")


async def throughput(iface_rx):
    global _NETWORK_SATURATED
    cur = last = 0
    while True:
        async with await open_async(iface_rx) as f:
            cur = int((await f.read())[:-1])
            if last:
                tp = int((cur - last) * 8 / 1000 / 3)
                _NETWORK_SATURATED = tp > IPTV_BW
            last = cur
            await asyncio.sleep(3)


class VodHttpProtocol(HttpProtocol, metaclass=TouchUpMeta):
    def connection_made(self, transport):
        """
        HTTP-protocol-specific new connection handler tuned for VOD.
        """
        try:
            transport.set_write_buffer_limits(low=188, high=1316)
            self.connections.add(self)
            self.transport = transport
            self._task = self.loop.create_task(self.connection_task())
            self.recv_buffer = bytearray()
            self.conn_info = ConnInfo(self.transport, unix=self._unix)
        except Exception:
            error_logger.exception("protocol.connect_made")


if __name__ == "__main__":
    try:
        app.run(
            host=SANIC_HOST,
            port=SANIC_PORT,
            protocol=VodHttpProtocol,
            access_log=False,
            auto_reload=False,
            debug=DEBUG,
            workers=SANIC_THREADS if os.name != "nt" else 1,
        )
    except (KeyboardInterrupt, TimeoutError):
        sys.exit(1)
    except Exception as ex:
        log.critical(f"{repr(ex)}")
        sys.exit(1)
