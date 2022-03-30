#!/usr/bin/env python3

import aiofiles
import aiohttp
import asyncio
import asyncio_dgram
import json
import os
import socket
import sys
import time
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError
from aiohttp.resolver import AsyncResolver
from asyncio.exceptions import CancelledError
from collections import namedtuple
from contextlib import closing
from filelock import FileLock, Timeout
from sanic import Sanic, exceptions, response
from sanic.compat import stat_async
from sanic.handlers import ContentRangeHandler
from sanic.log import error_logger, logger as log, LOGGING_CONFIG_DEFAULTS
from sanic.models.server_types import ConnInfo
from sanic.server import HttpProtocol
from sanic.touchup.meta import TouchUpMeta

from mu7d import MIME_M3U, MIME_TS, MIME_WEBM, EPG_URL, IPTV_DNS, UA, URL_COVER, URL_LOGO, WIN32, YEAR_SECONDS
from mu7d import find_free_port, get_iptv_ip, mu7d_config, ongoing_vods, _version
from movistar_vod import VodData, VodLoop, VodSetup


LOG_SETTINGS = LOGGING_CONFIG_DEFAULTS
LOG_SETTINGS["formatters"]["generic"]["format"] = "%(asctime)s [U7D] [%(levelname)s] %(message)s"
LOG_SETTINGS["formatters"]["generic"]["datefmt"] = "[%Y-%m-%d %H:%M:%S]"
LOG_SETTINGS["formatters"]["access"]["datefmt"] = "[%Y-%m-%d %H:%M:%S]"

app = Sanic("movistar_u7d")
app.config.update(
    {
        "FALLBACK_ERROR_FORMAT": "json",
        "GRACEFUL_SHUTDOWN_TIMEOUT": 0,
        "REQUEST_TIMEOUT": 1,
        "RESPONSE_TIMEOUT": 1,
    }
)
app.ctx.vod_client = None


@app.listener("before_server_start")
async def before_server_start(app, loop):
    global _CHANNELS, _IPTV, _SESSION, _SESSION_LOGOS

    if not WIN32:
        import signal

        def cleanup_handler(signum, frame):
            asyncio.get_event_loop().stop()

        [signal.signal(sig, cleanup_handler) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]

    _SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1),
        json_serialize=ujson.dumps,
    )
    _SESSION_LOGOS = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            keepalive_timeout=YEAR_SECONDS,
            resolver=AsyncResolver(nameservers=[IPTV_DNS]) if not WIN32 else None,
        ),
        headers={"User-Agent": UA},
        json_serialize=ujson.dumps,
    )

    while True:
        try:
            async with _SESSION.get(f"{EPG_URL}/channels/") as r:
                if r.status == 200:
                    _CHANNELS = json.loads(
                        await r.text(),
                        object_hook=lambda d: {int(k) if k.isdigit() else k: v for k, v in d.items()},
                    )
                    break
                else:
                    log.error("Failed to get channel list from EPG service")
                    await asyncio.sleep(5)
        except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
            log.debug("Waiting for EPG service...")
            await asyncio.sleep(1)

    if IPTV_BW_SOFT:
        app.add_task(network_saturated())
        log.info(f"BW: {IPTV_BW_SOFT}/{IPTV_BW_HARD} kbps / {IPTV_IFACE}")

    if not app.ctx.vod_client:
        app.ctx.vod_client = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                keepalive_timeout=YEAR_SECONDS,
                resolver=AsyncResolver(nameservers=[IPTV_DNS]) if not WIN32 else None,
            ),
            headers={"User-Agent": UA},
            json_serialize=ujson.dumps,
        )

    _IPTV = get_iptv_ip()

    if not WIN32:
        [signal.signal(sig, signal.SIG_DFL) for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)]


@app.listener("after_server_start")
async def after_server_start(app, loop):
    banner = f"Movistar U7D - U7D v{_version}"
    if U7D_THREADS > 1:
        log.info(f"*** {banner} ***")
    else:
        log.info("-" * len(banner))
        log.info(banner)
        log.info("-" * len(banner))


@app.listener("before_server_stop")
async def before_server_stop(app, loop):
    global _RESPONSES
    for req_ip, raw_url, resp in _RESPONSES:
        try:
            await resp.eof()
            log.info(f"[{req_ip}] " f"{U7D_URL if not NO_VERBOSE_LOGS else ''}" f"{raw_url} -> Stopped 2")
        except AttributeError:
            pass

    for session in (_SESSION, _SESSION_LOGOS, app.ctx.vod_client):
        try:
            await session.close()
        except CancelledError:
            pass


def get_channel_id(channel_name):
    return [
        chan
        for chan in _CHANNELS
        if channel_name.lower().replace("hd", "").replace("tv", "")
        == _CHANNELS[chan]["name"].lower().replace(" ", "").rstrip("hd").rstrip("tv").rstrip("+").rstrip(".")
    ][0]


@app.route("/<channel_id:int>/live", methods=["GET", "HEAD"])
@app.route("/<channel_id:int>/mpegts", methods=["GET", "HEAD"])
@app.route(r"/<channel_name:([A-Za-z1-9]+)\.ts$>", methods=["GET", "HEAD"])
async def handle_channel(request, channel_id=None, channel_name=None):
    _start = time.time()
    _raw_url = request.raw_url.decode()
    _u7d_url = (U7D_URL + _raw_url + " ") if not NO_VERBOSE_LOGS else ""
    if channel_name:
        try:
            channel_id = get_channel_id(channel_name)
        except IndexError:
            raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    try:
        name, mc_grp, mc_port = [_CHANNELS[channel_id][t] for t in ["name", "address", "port"]]
    except (AttributeError, KeyError):
        log.error(f"{_raw_url} not found")
        raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    if _NETWORK_SATURATED and not await ongoing_vods(_fast=True):
        log.warning(f"[{request.ip}] {_raw_url} -> Network Saturated")
        raise exceptions.ServiceUnavailable("Network Saturated")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((mc_grp if not WIN32 else "", int(mc_port)))
    sock.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(mc_grp) + socket.inet_aton(_IPTV)
    )

    stream = await asyncio_dgram.from_socket(sock)
    try:
        _response = await request.respond(content_type=MIME_TS)
        await _response.send((await stream.recv())[0][28:])
    except TimeoutError:
        log.error(f"{_raw_url} not found")
        raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    _lat = time.time() - _start
    app.add_task(
        _SESSION.post(
            f"{EPG_URL}/prom_event/add",
            json={
                "method": "live",
                "endpoint": f"{name} _ {request.ip} _ ",
                "channel_id": channel_id,
                "msg": f"[{request.ip}] -> Playing {_u7d_url}[{_lat:.4f}s]",
                "id": _start,
                "lat": _lat,
            },
        )
    )

    try:
        while True:
            await _response.send((await stream.recv())[0][28:])
    except AttributeError:
        pass
    finally:
        try:
            await _response.eof()
        except AttributeError:
            pass
        finally:
            stream.close()
            app.add_task(
                _SESSION.post(
                    f"{EPG_URL}/prom_event/remove",
                    json={
                        "method": "live",
                        "endpoint": f"{name} _ {request.ip} _ ",
                        "channel_id": channel_id,
                        "msg": f"[{request.ip}] -> Stopped {_u7d_url}[{_lat:.4f}s]",
                        "id": _start,
                    },
                )
            )


@app.route("/<channel_id:int>/<url>", methods=["GET", "HEAD"])
@app.route(r"/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"])
async def handle_flussonic(request, url, channel_id=None, channel_name=None, cloud=False):
    _start = time.time()
    _raw_url = request.raw_url.decode()
    _u7d_url = (U7D_URL + _raw_url + " ") if not NO_VERBOSE_LOGS else ""
    if channel_name:
        try:
            channel_id = get_channel_id(channel_name)
        except IndexError:
            raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    if not url:
        return response.empty(404)

    try:
        async with _SESSION.get(
            f"{EPG_URL}/program_id/{channel_id}/{url}" + ("?cloud=1" if cloud else "")
        ) as r:
            channel, program_id, start, duration, offset = (await r.json()).values()
    except (AttributeError, KeyError, ValueError):
        log.error(f"{_raw_url} not found")
        raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_TS, status=200)

    client_port = find_free_port(_IPTV)

    if _NETWORK_SATURATED and not await ongoing_vods(_fast=True):
        log.warning(f"[{request.ip}] {_raw_url} -> Network Saturated")
        raise exceptions.ServiceUnavailable("Network Saturated")

    _args = VodArgs(channel_id, program_id, _IPTV, request.ip, client_port, offset, cloud)
    vod_data = await VodSetup(_args, app.ctx.vod_client)
    if not isinstance(vod_data, VodData):
        if vod_data:
            raise exceptions.ServiceUnavailable("Movistar IPTV catchup service DOWN")
        else:
            raise exceptions.NotFound(f"Requested URL {_raw_url} not found")
    vod = app.add_task(VodLoop(_args, vod_data))
    _endpoint = _CHANNELS[channel_id]["name"] + f" _ {request.ip} _ "
    with closing(await asyncio_dgram.bind((_IPTV, client_port))) as stream:
        try:
            _response = await request.respond(content_type=MIME_TS)
            await _response.send((await stream.recv())[0])
        except TimeoutError:
            log.error(f"NOT_AVAILABLE: [{channel}] [{channel_id}] [{start}]")
            raise exceptions.NotFound(f"Requested URL {_raw_url} not found")

        _lat = time.time() - _start
        _RESPONSES.append((request.ip, _raw_url, _response))
        app.add_task(
            _SESSION.post(
                f"{EPG_URL}/prom_event/add",
                json={
                    "method": "catchup",
                    "endpoint": _endpoint,
                    "channel_id": channel_id,
                    "url": url,
                    "msg": f"[{request.ip}] -> Playing {_u7d_url}[{_lat:.4f}s]",
                    "id": _start,
                    "cloud": cloud,
                    "lat": _lat,
                },
            )
        )

        try:
            while True:
                await _response.send((await stream.recv())[0])
        except AttributeError:
            pass
        finally:
            try:
                await _response.eof()
            except AttributeError:
                pass
            finally:
                _RESPONSES.remove((request.ip, _raw_url, _response))
                app.add_task(
                    _SESSION.post(
                        f"{EPG_URL}/prom_event/remove",
                        json={
                            "method": "catchup",
                            "endpoint": _endpoint,
                            "channel_id": channel_id,
                            "url": url,
                            "msg": f"[{request.ip}] -> Stopped {_u7d_url}[{_lat:.4f}s]",
                            "id": _start,
                            "cloud": cloud,
                            "offset": time.time() - _start,
                        },
                    )
                )
                try:
                    vod.cancel()
                    await asyncio.wait({vod})
                except (ProcessLookupError, TypeError):
                    pass


@app.route("/cloud/<channel_id:int>/<url>", methods=["GET", "HEAD"])
@app.route(r"/cloud/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"])
async def handle_flussonic_cloud(request, url, channel_id=None, channel_name=None):
    if url == "live" or url == "mpegts":
        return await handle_channel(request, channel_id, channel_name)
    return await handle_flussonic(request, url, channel_id, channel_name, cloud=True)


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


@app.get("/guia.xml.gz")
@app.get("/guide.xml.gz")
async def handle_guide_gz(request):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    if not os.path.exists(GUIDE + ".gz"):
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")
    return await response.file(GUIDE + ".gz")


@app.route("/Covers/<path:int>/<cover>", methods=["GET", "HEAD"])
@app.route("/Logos/<logo>", methods=["GET", "HEAD"])
async def handle_logos(request, cover=None, logo=None, path=None):
    log.debug(f"[{request.ip}] {request.method} {request.url}")
    if logo and logo.split(".")[0].isdigit():
        orig_url = f"{URL_LOGO}/{logo}"
    elif path and cover and cover.split(".")[0].isdigit():
        orig_url = f"{URL_COVER}/{path}/{cover}"
    else:
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type="image/jpeg", status=200)

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


@app.get(r"/<m3u_file:([A-Za-z1-9]+)\.m3u$>")
async def handle_m3u_files(request, m3u_file):
    log.info(f"[{request.ip}] {request.method} {request.url}")
    m3u, m3u_matched = m3u_file.lower(), None
    msg = f'"{m3u_file}"'
    if m3u in ("movistartv", "canales", "channels"):
        m3u_matched = CHANNELS
    elif m3u in ("movistartvcloud", "cloud", "nube"):
        m3u_matched = CHANNELS_CLOUD
    elif m3u in ("grabaciones", "recordings"):
        m3u_matched = RECORDINGS_M3U
    elif RECORDINGS_PER_CHANNEL:
        try:
            channel_id = get_channel_id(m3u)
            channel_tag = "%03d. %s" % (_CHANNELS[channel_id]["number"], _CHANNELS[channel_id]["name"])
            m3u_matched = os.path.join(os.path.join(RECORDINGS, channel_tag), f"{channel_tag}.m3u")
            msg = f"[{channel_tag}]"
        except IndexError:
            pass

    if m3u_matched and os.path.exists(m3u_matched):
        log.info(f'[{request.ip}] Found {msg} m3u: "{m3u_matched}"')
        return await response.file(m3u_matched, mime_type=MIME_M3U)
    else:
        log.warning(f"[{request.ip}] {msg} m3u: Not Found")
        raise exceptions.NotFound(f"Requested URL {request.raw_url.decode()} not found")


@app.get("/favicon.ico")
async def handle_notfound(request):
    return response.empty(404)


@app.get("/metrics")
async def handle_prometheus(request):
    try:
        async with _SESSION.get(f"{EPG_URL}/metrics") as r:
            return response.text((await r.read()).decode())
    except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
        raise exceptions.ServiceUnavailable("Not available")


@app.get("/record/<channel_id:int>/<url>")
async def handle_record_program(request, channel_id, url):
    if not url:
        log.warning("Cannot record live channels! Use wget for that.")
        return response.empty(404)

    try:
        async with _SESSION.get(f"{EPG_URL}{request.raw_url.decode()}") as r:
            return response.json(await r.json())
    except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
        raise exceptions.ServiceUnavailable("Not available")


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


@app.get("/timers_check")
async def handle_timers_check(request):
    try:
        async with _SESSION.get(f"{EPG_URL}/timers_check") as r:
            return response.json(await r.json())
    except (ClientConnectorError, ConnectionRefusedError, ServerDisconnectedError):
        raise exceptions.ServiceUnavailable("Not available")


async def network_saturated():
    global _NETWORK_SATURATED
    iface_rx = f"/sys/class/net/{IPTV_IFACE}/statistics/rx_bytes"
    before = last = 0
    cutpoint = IPTV_BW_SOFT + (IPTV_BW_HARD - IPTV_BW_SOFT) / 2

    while True:
        async with aiofiles.open(iface_rx) as f:
            now, cur = time.time(), int((await f.read())[:-1])

        if last:
            tp = (cur - last) * 0.008 / (now - before)
            _NETWORK_SATURATED = tp > cutpoint

        before, last = now, cur
        await asyncio.sleep(1)


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
    if not WIN32:
        from setproctitle import setproctitle

        setproctitle("movistar_u7d")

    _IPTV = _NETWORK_SATURATED = _SESSION = _SESSION_LOGOS = None

    _CHANNELS = {}
    _CHILDREN = {}
    _RESPONSES = []

    _conf = mu7d_config()

    CHANNELS = _conf["CHANNELS"]
    CHANNELS_CLOUD = _conf["CHANNELS_CLOUD"]
    GUIDE = _conf["GUIDE"]
    GUIDE_CLOUD = _conf["GUIDE_CLOUD"]
    IPTV_BW_HARD = _conf["IPTV_BW_HARD"]
    IPTV_BW_SOFT = _conf["IPTV_BW_SOFT"]
    IPTV_IFACE = _conf["IPTV_IFACE"]
    NO_VERBOSE_LOGS = _conf["NO_VERBOSE_LOGS"]
    RECORDINGS = _conf["RECORDINGS"]
    RECORDINGS_M3U = _conf["RECORDINGS_M3U"]
    RECORDINGS_PER_CHANNEL = _conf["RECORDINGS_PER_CHANNEL"]
    U7D_THREADS = _conf["U7D_THREADS"]
    U7D_URL = _conf["U7D_URL"]

    VodArgs = namedtuple("Vod", ["channel", "program", "client_ip", "client_port", "start", "cloud"])

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_u7d.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            app.run(
                host=_conf["LAN_IP"],
                port=_conf["U7D_PORT"],
                protocol=VodHttpProtocol,
                access_log=False,
                auto_reload=False,
                debug=_conf["DEBUG"],
                workers=U7D_THREADS if not WIN32 else 1,
            )
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
