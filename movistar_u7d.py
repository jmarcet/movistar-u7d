#!/usr/bin/env python3

import aiofiles
import aiohttp
import asyncio
import asyncio_dgram
import json
import logging
import os
import socket
import sys
import time
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL, PIPE
from collections import namedtuple
from datetime import datetime
from filelock import FileLock, Timeout
from sanic import Sanic, response
from sanic.compat import open_async, stat_async
from sanic.exceptions import HeaderNotFound, NotFound, ServiceUnavailable
from sanic.handlers import ContentRangeHandler
from sanic.log import error_logger
from sanic.models.server_types import ConnInfo
from sanic.server.protocols.http_protocol import HttpProtocol
from warnings import filterwarnings

from mu7d import ATOM, BUFF, CHUNK, DATEFMT, EPG_URL, FMT, MIME_M3U, MIME_WEBM, UA
from mu7d import URL_COVER, URL_FANART, URL_LOGO, VID_EXTS, WIN32, YEAR_SECONDS, IPTVNetworkError
from mu7d import add_logfile, cleanup_handler, find_free_port, get_end_point, get_iptv_ip
from mu7d import get_vod_info, keys_to_int, mu7d_config, ongoing_vods, _version
from movistar_vod import Vod


app = Sanic("movistar_u7d")

log = logging.getLogger("U7D")


@app.listener("before_server_start")
async def before_server_start(app):
    global _CHANNELS, _IPTV, _SESSION, _SESSION_LOGOS

    app.config.FALLBACK_ERROR_FORMAT = "json"
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0
    app.config.REQUEST_TIMEOUT = 1
    # app.config.RESPONSE_TIMEOUT = 1

    if WIN32:
        import win32api  # pylint: disable=import-error
        import win32con  # pylint: disable=import-error

        global _loop

        _loop = asyncio.get_running_loop()

        def cancel_handler(event):
            log.debug("cancel_handler(event=%d)" % event)
            if _loop and event in (
                win32con.CTRL_BREAK_EVENT,
                win32con.CTRL_CLOSE_EVENT,
                win32con.CTRL_LOGOFF_EVENT,
                win32con.CTRL_SHUTDOWN_EVENT,
            ):
                _loop.stop()
                while _loop.is_running():
                    time.sleep(0.05)

        win32api.SetConsoleCtrlHandler(cancel_handler, 1)

    _SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS, limit_per_host=1),
        json_serialize=ujson.dumps,
    )
    _SESSION_LOGOS = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS),
        headers={"User-Agent": UA},
        json_serialize=ujson.dumps,
    )

    while True:
        try:
            async with _SESSION.get(f"{EPG_URL}/channels/") as r:
                if r.status == 200:
                    _CHANNELS = json.loads(await r.text(), object_hook=keys_to_int)
                    break
                await asyncio.sleep(5)
        except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
            log.debug("Waiting for EPG service...")
            await asyncio.sleep(1)

    if IPTV_BW_SOFT:
        app.add_task(network_saturated())

    _IPTV = get_iptv_ip()


@app.listener("after_server_start")
async def after_server_start(app):
    app.ctx.ep = await get_end_point(HOME, log)
    app.ctx.vod_client = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS),
        headers={"User-Agent": UA},
        json_serialize=ujson.dumps,
    )

    banner = f"Movistar U7D - U7D v{_version}"
    if U7D_PROCESSES > 1:
        log.info(f"*** {banner} ***")
    else:
        log.info("-" * len(banner))
        log.info(banner)
        log.info("-" * len(banner))

    if IPTV_BW_SOFT:
        log.info(f"BW: {IPTV_BW_SOFT}-{IPTV_BW_HARD} kbps / {IPTV_IFACE}")


@app.listener("before_server_stop")
async def before_server_stop(app):
    cleanup_handler()


def get_channel_id(channel_name):
    res = [
        chan
        for chan in _CHANNELS
        if channel_name.lower() in _CHANNELS[chan]["name"].lower().replace(" ", "").replace(".", "")
    ]
    return res[0] if len(res) == 1 else None


@app.route("/<channel_id:int>/live", methods=["GET", "HEAD"], name="channel_live")
@app.route("/<channel_id:int>/mpegts", methods=["GET", "HEAD"], name="channel_mpegts")
@app.route(r"/<channel_name:([A-Za-z1-9]+)\.ts$>", methods=["GET", "HEAD"], name="channel_name")
async def handle_channel(request, channel_id=None, channel_name=None):
    log.debug("%s %s %s" % tuple(map(str, (request, request.args, request.headers))))

    _start = time.time()

    if channel_name:
        channel_id = get_channel_id(channel_name)

    if channel_id not in _CHANNELS:
        raise NotFound(f"Requested URL {request.path} not found")

    ua = request.headers.get("user-agent", "")
    if " Chrome/" in ua:
        return await handle_flussonic(request, f"{int(datetime.now().timestamp())}.ts", channel_id)

    try:
        name, mc_grp, mc_port = (_CHANNELS[channel_id][t] for t in ("name", "address", "port"))
    except (AttributeError, KeyError):
        raise NotFound(f"Requested URL {request.path} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_WEBM, status=200)

    if _NETWORK_SATURATED and not await ongoing_vods(_fast=True):
        log.warning(f"[{request.ip}] {request.path} -> Network Saturated")
        raise ServiceUnavailable("Network Saturated")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((mc_grp if not WIN32 else "", int(mc_port)))
    sock.setsockopt(
        socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(mc_grp) + socket.inet_aton(_IPTV)
    )

    prom = None
    stream = await asyncio_dgram.from_socket(sock)
    _response = await request.respond(content_type=MIME_WEBM)

    try:
        # 1st packet on SDTV channels is bogus and breaks ffmpeg
        if ua.startswith("Jellyfin") and " HD" not in name:
            log.debug('UA="%s" detected, skipping first packet' % ua)
            await stream.recv()
        else:
            await _response.send((await stream.recv())[0][28:])

        prom = app.add_task(
            send_prom_event(
                {
                    "channel_id": channel_id,
                    "id": _start,
                    "lat": time.time() - _start,
                    "method": "live",
                    "endpoint": f"{name} _ {request.ip} _ ",
                    "msg": f"[{request.ip}] -> Playing {request.url}",
                }
            )
        )

        while True:
            await _response.send((await stream.recv())[0][28:])

    finally:
        stream.close()
        sock.close()
        if prom:
            prom.cancel()
            await prom
        await _response.eof()


@app.route("/<channel_id:int>/<url>", methods=["GET", "HEAD"], name="flussonic_id")
@app.route(r"/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"], name="flussonic_name")
async def handle_flussonic(request, url, channel_id=None, channel_name=None, cloud=False, local=False):
    log.debug("%s %s %s" % tuple(map(str, (request, request.args, request.headers))))

    _start = time.time()

    if not url:
        return response.empty(404)

    if channel_name:
        channel_id = get_channel_id(channel_name)

    if channel_id not in _CHANNELS:
        raise NotFound(f"Requested URL {request.path} not found")

    event = {
        "channel_id": channel_id,
        "cloud": cloud,
        "id": _start,
        "lat": 0,
        "url": url,
        "method": "catchup",
        "msg": f"[{request.ip}] -> Playing {request.url}",
        "endpoint": _CHANNELS[channel_id]["name"] + f" _ {request.ip} _ ",
    }

    try:
        params = {"cloud": int(cloud), "local": int(local)}
        async with _SESSION.get(f"{EPG_URL}/program_id/{channel_id}/{url}", params=params) as r:
            _, program_id, _, duration, offset = (await r.json()).values()
    except (AttributeError, KeyError, ValueError, ClientConnectionError, ClientOSError, ServerDisconnectedError):
        raise NotFound(f"Requested URL {request.path} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_WEBM, status=200)

    ua = request.headers.get("user-agent", "")

    if local:
        if os.path.exists(program_id):
            if " Chrome/" in ua or not program_id.endswith(".ts"):
                prom = app.add_task(send_prom_event(event))
                try:
                    return await transcode(request, event, filename=program_id, offset=offset)
                finally:
                    prom.cancel()
                    await prom

            _stat = await stat_async(program_id)
            bytepos = round(offset * _stat.st_size / duration)
            bytepos -= bytepos % CHUNK
            to_send = _stat.st_size - bytepos

            async with await open_async(program_id, mode="rb") as f:
                await f.seek(bytepos)
                _response = await request.respond(content_type=MIME_WEBM)
                prom = app.add_task(send_prom_event(event))

                try:
                    while to_send >= CHUNK:
                        content = await f.read(BUFF)
                        await _response.send(content)
                        to_send -= len(content)
                finally:
                    prom.cancel()
                    await prom
                    await _response.eof()

            return
        raise NotFound(f"Requested URL {request.path} not found")

    if _NETWORK_SATURATED and not await ongoing_vods(_fast=True):
        log.warning(f"[{request.ip}] {request.path} -> Network Saturated")
        raise ServiceUnavailable("Network Saturated")

    client_port = find_free_port(_IPTV)
    info = await get_vod_info(request.app.ctx.vod_client, request.app.ctx.ep, channel_id, cloud, program_id, log)
    if not info:
        raise NotFound(f"Requested URL {request.path} not found")

    args = VodArgs(channel_id, program_id, request.ip, client_port, offset, cloud)
    vod = app.add_task(Vod(args, request.app.ctx.vod_client, info))

    if " Chrome/" in ua:
        return await transcode(request, event, channel_id=channel_id, port=client_port, vod=vod)

    prom = None
    stream = await asyncio_dgram.bind((_IPTV, client_port))
    _response = await request.respond(content_type=MIME_WEBM)

    try:
        # 1st packet on SDTV channels is bogus and breaks ffmpeg
        if ua.startswith("Jellyfin") and " HD" not in _CHANNELS[channel_id]["name"]:
            log.debug('UA="%s" detected, skipping first packet' % ua)
            await stream.recv()
        else:
            await _response.send((await stream.recv())[0])

        prom = app.add_task(send_prom_event({**event, "lat": time.time() - _start}))

        while True:
            await _response.send((await stream.recv())[0])

    finally:
        stream.close()
        vod.cancel()
        if prom:
            prom.cancel()
            await prom
        await _response.eof()


@app.route("/cloud/<channel_id:int>/<url>", methods=["GET", "HEAD"], name="flussonic_cloud_id")
@app.route(r"/cloud/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"], name="flussonic_cloud_name")
@app.route("/local/<channel_id:int>/<url>", methods=["GET", "HEAD"], name="flussonic_local_id")
@app.route(r"/local/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"], name="flussonic_local_name")
async def handle_flussonic_extra(request, url, channel_id=None, channel_name=None):
    if url in ("live", "mpegts"):
        return await handle_channel(request, channel_id, channel_name)
    cloud = request.route.path.startswith("cloud")
    local = request.route.path.startswith("local")
    return await handle_flussonic(request, url, channel_id, channel_name, cloud=cloud, local=local)


@app.get("/cloud.xml", name="xml_cloud")
@app.get("/guia.xml", name="xml_guia")
@app.get("/guia.xml.gz", name="xmlz_guia")
@app.get("/guide.xml", name="xml_guide")
@app.get("/guide.xml.gz", name="xmlz_guide")
@app.get("/local.xml", name="xml_local")
@app.get("/nube.xml", name="xml_nube")
async def handle_guides(request):
    guide = {
        "cloud.xml": GUIDE_CLOUD,
        "guia.xml": GUIDE,
        "guia.xml.gz": GUIDE + ".gz",
        "guide.xml": GUIDE,
        "guide.xml.gz": GUIDE + ".gz",
        "local.xml": GUIDE_LOCAL,
        "nube.xml": GUIDE_CLOUD,
    }[request.route.path]
    pad = 10 if request.route.path.endswith(".xml") else 7

    if not os.path.exists(guide):
        raise NotFound(f"Requested URL {request.path} not found")

    log.info(f'[{request.ip}] {request.method} {request.url}{" " * pad} => "{guide}"')
    return await response.file(guide)


@app.route("/Covers/<path:int>/<cover>", methods=["GET", "HEAD"], name="images_covers")
@app.route("/Logos/<logo>", methods=["GET", "HEAD"], name="images_logos")
async def handle_images(request, cover=None, logo=None, path=None):
    log.debug("[%s] %s %s" % (request.ip, request.method, request.url))
    if path and cover:
        urls = (f'{URL_FANART}/{request.args.get("fanart")}',) if "?fanart=" in request.url else ()
        urls += (f"{URL_COVER}/{path}/{cover}",)
    elif logo:
        urls = (f"{URL_LOGO}/{logo}",)
    else:
        raise NotFound(f"Requested URL {request.path} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type="image/jpeg", status=200)

    for url in urls:
        try:
            async with _SESSION_LOGOS.get(url) as r:
                if r.status == 200:
                    logo_data = await r.read()
                    if logo_data:
                        return response.HTTPResponse(
                            body=logo_data,
                            content_type="image/jpeg" if not url.endswith(".png") else "image/png",
                            headers={"Content-Disposition": f'attachment; filename="{os.path.basename(url)}"'},
                        )
                    log.warning("Got empty image => %s" % str(r).splitlines()[0])
                else:
                    log.warning("Could not get image => %s" % str(r).splitlines()[0])
        except (ClientConnectionError, ClientOSError, ServerDisconnectedError) as ex:
            log.warning("Could not get image => %s" % str(ex))
    raise NotFound(f"Requested URL {request.path} not found")


@app.get(r"/<m3u_file:([A-Za-z1-9]+)\.m3u$>")
async def handle_m3u_files(request, m3u_file):
    m3u_matched = pad = ""

    m3u = m3u_file.lower()
    if m3u in ("movistartv", "canales", "channels", "playlist"):
        m3u_matched = CHANNELS
        pad = " " * 5
    elif m3u in ("movistartvcloud", "cloud", "nube"):
        m3u_matched = CHANNELS_CLOUD
    elif m3u in ("movistartvlocal", "local"):
        m3u_matched = CHANNELS_LOCAL
    elif m3u in ("grabaciones", "recordings"):
        m3u_matched = os.path.join(RECORDINGS, "Recordings.m3u")
        pad = " " * 5
    else:
        channel_id = get_channel_id(m3u)
        if channel_id in _CHANNELS:
            channel_tag = "%03d. %s" % (_CHANNELS[channel_id]["number"], _CHANNELS[channel_id]["name"])
            m3u_matched = os.path.join(os.path.join(RECORDINGS, channel_tag), f"{channel_tag}.m3u")

    if os.path.exists(m3u_matched):
        log.info(f'[{request.ip}] {request.method} {request.url}{pad} => "{m3u_matched}"')
        return await response.file(m3u_matched, mime_type=MIME_M3U)

    raise NotFound(f"Requested URL {request.path} not found")


@app.get("/favicon.ico")
async def handle_notfound(request):
    return response.empty(404)


@app.get("/metrics")
async def handle_prometheus(request):
    try:
        async with _SESSION.get(f"{EPG_URL}/metrics") as r:
            return response.text(await r.text())
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
        raise ServiceUnavailable("Not available")


@app.get("/timers_check", name="proxied_timers_check")
@app.get("/update_epg", name="proxied_update_epg")
async def handle_proxied(request):
    try:
        async with _SESSION.get(f"{EPG_URL}/{request.route.path}") as r:
            return response.json(await r.json())
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
        raise ServiceUnavailable("Not available")


@app.get("/record/<channel_id:int>/<url>", name="record_program_id")
@app.get(r"/record/<channel_name:([A-Za-z1-9]+)>/<url>", name="record_program_name")
async def handle_record_program(request, url, channel_id=None, channel_name=None):
    if not url:
        log.warning("Cannot record live channels! Use wget for that.")
        return response.empty(404)

    if channel_name:
        channel_id = get_channel_id(channel_name)

    if channel_id not in _CHANNELS:
        raise NotFound(f"Requested URL {request.path} not found")

    try:
        async with _SESSION.get(f"{EPG_URL}/record/{channel_id}/{url}", params=request.args) as r:
            return response.json(await r.json())
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
        raise ServiceUnavailable("Not available")


@app.route("/recording/", methods=["GET", "HEAD"])
async def handle_recording(request):
    if not RECORDINGS:
        raise NotFound(f"Requested URL {request.path} not found")

    _path = urllib.parse.unquote(request.url).split("/recording/")[1][1:]
    ext = os.path.splitext(_path)[1]

    if RECORDINGS_TMP and ext in (".jpg", ".png"):
        cached_cover = os.path.join(RECORDINGS_TMP, "covers", _path)
        if os.path.exists(cached_cover):
            if request.method == "HEAD":
                return response.HTTPResponse(status=200)

            return await response.file(cached_cover)
        log.warning(f'Cover not found in cache: "{_path}"')

    file = os.path.join(RECORDINGS, _path)
    if os.path.exists(file):
        if request.method == "HEAD":
            return response.HTTPResponse(status=200)

        if ext in (".jpg", ".png"):
            return await response.file(file)

        if ext in VID_EXTS:
            try:
                _range = ContentRangeHandler(request, await stat_async(file))
                return await response.file_stream(file, mime_type=MIME_WEBM, _range=_range)
            except HeaderNotFound:
                pass

    raise NotFound(f"Requested URL {request.path} not found")


async def network_saturated():
    global _NETWORK_SATURATED
    iface_rx = f"/sys/class/net/{IPTV_IFACE}/statistics/rx_bytes"
    before = last = 0
    cutpoint = IPTV_BW_SOFT + (IPTV_BW_HARD - IPTV_BW_SOFT) / 2

    while True:
        if not os.path.exists(iface_rx):
            last = 0
            log.error(f"{IPTV_IFACE} NOT ACCESSIBLE")
            while not os.path.exists(iface_rx):
                await asyncio.sleep(1)
            log.info(f"{IPTV_IFACE} IS now ACCESSIBLE")

        async with aiofiles.open(iface_rx) as f:
            now, cur = time.time(), int((await f.read())[:-1])

        if last:
            tp = (cur - last) * 0.008 / (now - before)
            _NETWORK_SATURATED = tp > cutpoint

        before, last = now, cur
        await asyncio.sleep(1)


async def send_prom_event(event):
    await asyncio.sleep(0.05)
    event["msg"] = "%-95s%9s:" % (event["msg"], f"[{event['lat']:.4f}s]" if event["lat"] else "")

    try:
        try:
            await _SESSION.post(f"{EPG_URL}/prom_event/add", json={**event})
            await asyncio.sleep(YEAR_SECONDS)
        except CancelledError:
            event["msg"] = event["msg"].replace("Playing", "Stopped")
            await _SESSION.post(
                f"{EPG_URL}/prom_event/remove", json={**event, "offset": time.time() - event["id"]}
            )
    except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
        pass


async def transcode(request, event, channel_id=0, filename="", offset=0, port=0, vod=None):
    log.debug(
        'transcode(): channel_id=%s port=%s vod=%s offset=%s filename="%s"'
        % tuple(map(str, (channel_id, port, vod, offset, filename)))
    )

    if request.args.get("vo") == "1":
        lang_channel = ("-map", "0:a", "-map", "-0:m:language:spa", "-map", "-0:m:language:esp")
    else:
        lang_channel = ("-map", "0:a", "-map", "-0:m:language:mul", "-map", "-0:m:language:vo")

    cmd = ["ffmpeg"]
    if filename:
        cmd += ["-ss", f"{offset}", "-i", filename]
    else:
        cmd += ["-skip_initial_bytes", f"{CHUNK}"] if " HD" not in _CHANNELS[channel_id]["name"] else []
        cmd += ["-i", f"udp://@{_IPTV}:{port}"]
    cmd += ["-map", "0:v", *lang_channel, "-c:v:0", "copy", "-c:a:0", "libfdk_aac", "-b:a", "128k"]
    cmd += ["-f", "matroska", "-v", "fatal", "-"]

    proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE)
    _response = await request.respond(content_type=MIME_WEBM)

    await _response.send(await proc.stdout.read(BUFF))
    prom = app.add_task(send_prom_event({**event, "lat": time.time() - event["id"]}))

    try:
        while True:
            content = await proc.stdout.read(BUFF)
            if len(content) < 1:
                break
            await _response.send(content)

    finally:
        proc.kill()
        if vod:
            vod.cancel()
        prom.cancel()
        await prom
        await proc.wait()
        await _response.eof()


class VodHttpProtocol(HttpProtocol):
    def connection_made(self, transport):
        """
        HTTP-protocol-specific new connection handler tuned for Movistar IPTV.
        """
        try:
            transport.set_write_buffer_limits(low=ATOM, high=BUFF)
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

    _IPTV = _NETWORK_SATURATED = _SESSION = _SESSION_LOGOS = _loop = None

    _CHANNELS = {}

    _conf = mu7d_config()

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)
    logging.getLogger("sanic.error").setLevel(logging.FATAL)
    logging.getLogger("sanic.root").disabled = True

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=_conf["DEBUG"] and logging.DEBUG or logging.INFO)
    if _conf["LOG_TO_FILE"]:
        add_logfile(log, _conf["LOG_TO_FILE"], _conf["DEBUG"] and logging.DEBUG or logging.INFO)

    # Ths happens when u7d is killed while on before_server_start
    filterwarnings(
        action="ignore",
        category=RuntimeWarning,
        message=r"coroutine '\w+.create_server' was never awaited",
        module="sys",
    )

    if not os.getenv("U7D_PARENT"):
        log.critical("Must be run with mu7d")
        sys.exit(1)

    CHANNELS = _conf["CHANNELS"]
    CHANNELS_CLOUD = _conf["CHANNELS_CLOUD"]
    CHANNELS_LOCAL = _conf["CHANNELS_LOCAL"]
    DEBUG = _conf["DEBUG"]
    GUIDE = _conf["GUIDE"]
    GUIDE_CLOUD = _conf["GUIDE_CLOUD"]
    GUIDE_LOCAL = _conf["GUIDE_LOCAL"]
    HOME = _conf["HOME"]
    IPTV_BW_HARD = _conf["IPTV_BW_HARD"]
    IPTV_BW_SOFT = _conf["IPTV_BW_SOFT"]
    IPTV_IFACE = _conf["IPTV_IFACE"]
    LAN_IP = _conf["LAN_IP"]
    RECORDINGS = _conf["RECORDINGS"]
    RECORDINGS_TMP = _conf["RECORDINGS_TMP"]
    U7D_PORT = _conf["U7D_PORT"]
    U7D_PROCESSES = _conf["U7D_PROCESSES"]

    VodArgs = namedtuple("Vod", ["channel", "program", "client_ip", "client_port", "start", "cloud"])

    del _conf
    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_u7d.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            app.run(
                host=LAN_IP,
                port=U7D_PORT,
                protocol=VodHttpProtocol,
                access_log=False,
                auto_reload=False,
                debug=DEBUG,
                workers=U7D_PROCESSES if not WIN32 else 1,
            )
    except (AttributeError, CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except IPTVNetworkError as err:
        log.critical(err)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
