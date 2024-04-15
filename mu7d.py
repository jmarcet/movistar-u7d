#!/usr/bin/env python3

import aiofiles
import aiohttp
import asyncio
import os
import sys
import time
import ujson
import urllib.parse

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from asyncio.exceptions import CancelledError
from asyncio.subprocess import DEVNULL, PIPE
from asyncio_dgram import bind as dgram_bind, from_socket as dgram_from_socket
from collections import defaultdict, namedtuple
from contextlib import closing
from datetime import datetime
from filelock import FileLock, Timeout
from psutil import AccessDenied, Process, boot_time
from sanic import response
from sanic_prometheus import monitor
from sanic.exceptions import Forbidden, HeaderNotFound, NotFound, ServiceUnavailable
from sanic.handlers import ContentRangeHandler
from sanic.log import error_logger
from sanic.models.server_types import ConnInfo
from sanic.server.protocols.http_protocol import HttpProtocol
from signal import SIGINT, SIGTERM, SIG_IGN, signal
from socket import IPPROTO_IP, IPPROTO_UDP, IP_ADD_MEMBERSHIP, SO_REUSEADDR, SOCK_DGRAM, SOL_SOCKET, inet_aton
from socket import AF_INET, SOCK_STREAM, socket
from time import sleep

from mu7d_cfg import ATOM, BUFF, CHUNK, CONF, DIV_ONE, LINUX, MIME_GUIDE, MIME_M3U
from mu7d_cfg import MIME_WEBM, UA, URL_COVER, URL_FANART, URL_LOGO, VID_EXTS, WIN32

from mu7d_lib import add_prom_event, alive, create_covers_cache, does_recording_exist, find_free_port
from mu7d_lib import get_channel_id, get_end_point, get_epg, get_iptv_ip, get_local_info, get_path
from mu7d_lib import get_program_vod, get_recording_name, get_vod_info, log_network_saturated, network_saturation
from mu7d_lib import ongoing_vods, prom_event, prune_duplicates, prune_expired, reaper, record_program
from mu7d_lib import recordings_lock, reload_epg, reload_recordings, reindex_recordings, timers_check, update_epg
from mu7d_lib import update_epg_cron, update_recordings, upgrade_recordings
from mu7d_lib import IPTVNetworkError, LocalNetworkError, PromEvent, Recording, app, log, _g, _version

from mu7d_vod import Vod


@app.listener("before_server_start")
async def before_server_start(app):
    app.config.FALLBACK_ERROR_FORMAT = "json"
    app.config.GRACEFUL_SHUTDOWN_TIMEOUT = 0
    app.config.REQUEST_TIMEOUT = 1


@app.listener("after_server_start")
async def after_server_start(app):
    if not WIN32:

        def cleanup_handler(signum, frame):
            [signal(sig, SIG_IGN) for sig in (SIGINT, SIGTERM)]
            app.stop()
            os.killpg(0, SIGTERM)
            time.sleep(0.5)
            while True:
                try:
                    os.waitpid(-1, 0)
                except ChildProcessError:
                    break

        [signal(sig, cleanup_handler) for sig in (SIGINT, SIGTERM)]

    else:
        import win32api  # pylint: disable=import-error
        import win32con  # pylint: disable=import-error

        _loop = asyncio.get_event_loop()

        def close_handler(event):
            log.debug("close_handler(event=%d)" % event)
            if _loop.is_running() and event in (
                win32con.CTRL_BREAK_EVENT,
                win32con.CTRL_C_EVENT,
                win32con.CTRL_CLOSE_EVENT,
                win32con.CTRL_LOGOFF_EVENT,
                win32con.CTRL_SHUTDOWN_EVENT,
            ):
                _loop.stop()
                while _loop.is_running():
                    time.sleep(0.05)
            log.debug("close_handler(event=%d) -> GOOD BYE" % event)
            return True

        win32api.SetConsoleCtrlHandler(close_handler, True)

    _g._NETWORK_SATURATED = False
    _g._NETWORK_SATURATION = 0

    _g._CHANNELS = {}
    _g._CLOUD = {}
    _g._EPGDATA = {}
    _g._KEEP = defaultdict(dict)
    _g._RECORDINGS = defaultdict(dict)

    _g._IPTV = get_iptv_ip()

    _g._SESSION = aiohttp.ClientSession(headers={"User-Agent": UA}, json_serialize=ujson.dumps)
    _g.vod_client = aiohttp.ClientSession(headers={"User-Agent": UA}, json_serialize=ujson.dumps)

    _g.ep = await get_end_point()
    if not _g.ep:
        return sys.exit(1)

    _g._last_bw_warning = None
    _g._last_epg = None
    _g._t_timers = None
    _g._t_timers_next = None

    app.add_task(alive())

    if _g.IPTV_BW_SOFT:
        app.add_task(network_saturation())
        _msg = " => Ignoring RECORDINGS_PROCESSES" if _g.RECORDINGS else ""
        log.info(f"BW: {_g.IPTV_BW_SOFT}-{_g.IPTV_BW_HARD} kbps / {_g.IPTV_IFACE}{_msg}")

    await reload_epg()

    if not _g._last_epg:
        if not os.path.exists(_g.GUIDE):
            return sys.exit(1)
        _g._last_epg = int(os.path.getmtime(_g.GUIDE))

    if _g.RECORDINGS:
        if not os.path.exists(_g.RECORDINGS):
            try:
                os.makedirs(_g.RECORDINGS)
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
                and not os.path.exists(os.path.join(_g.RECORDINGS_TMP, "covers"))
            ):
                await create_covers_cache()

        uptime = int(datetime.now().timestamp() - boot_time())
        if not _g._t_timers:
            if int(datetime.now().replace(minute=0, second=0).timestamp()) <= _g._last_epg:
                delay = max(5, 180 - uptime)
                if delay > 10:
                    log.info(f"Waiting {delay}s to check recording timers since the system just booted...")
                _g._t_timers = app.add_task(timers_check(delay))
            else:
                log.warning("Delaying timers_check until the EPG is updated...")

    app.add_task(reaper())
    app.add_task(update_epg_cron())


@app.listener("before_server_stop")
async def before_server_stop(app):
    [task.cancel() for task in asyncio.all_tasks()]


@app.put("/archive/<channel_id:int>/<program_id:int>")
async def handle_archive(request, channel_id, program_id):
    if not _g.RECORDINGS:
        raise NotFound(f"Requested URL {request.path} not found")

    cloud = request.args.get("cloud") == "1"

    timestamp = get_epg(channel_id, program_id, cloud)[0]
    if not timestamp:
        raise NotFound(f"Requested URL {request.path} not found")

    filename = get_recording_name(channel_id, timestamp, cloud)

    log_suffix = f': [{channel_id:4}] [{program_id}] [{timestamp}] "{filename}"'

    if not _g._t_timers or _g._t_timers.done():
        _g._t_timers = app.add_task(timers_check(delay=3))

    log.debug('Checking for "%s"' % filename)
    if not does_recording_exist(filename):
        msg = "Recording NOT ARCHIVED"
        log.error(DIV_ONE % (msg, log_suffix))
        return response.json({"status": msg}, ensure_ascii=False, status=204)

    async with recordings_lock:
        prune_duplicates(channel_id, filename)
        if channel_id in _g._KEEP:
            prune_expired(channel_id, filename)

        nfo = await get_local_info(channel_id, timestamp, get_path(filename, bare=True))
        if not nfo:
            raise NotFound(f"Requested URL {request.path} not found")
        # Metadata's beginTime rules since it is how the recording will be found
        _g._RECORDINGS[channel_id][nfo["beginTime"]] = Recording(filename, nfo["duration"])

        msg = "Recording ARCHIVED"
        log.info(DIV_ONE % (msg, log_suffix))

    app.add_task(update_recordings(channel_id))
    return response.json({"status": msg}, ensure_ascii=False)


@app.route("/<channel_id:int>/live", methods=["GET", "HEAD"], name="channel_live")
@app.route("/<channel_id:int>/mpegts", methods=["GET", "HEAD"], name="channel_mpegts")
@app.route(r"/<channel_name:([A-Za-z1-9]+)\.ts$>", methods=["GET", "HEAD"], name="channel_name")
async def handle_channel(request, channel_id=None, channel_name=None):
    log.debug("%s %s %s" % tuple(map(str, (request, request.args, request.headers))))

    _start = time.time()

    if channel_name:
        channel_id = get_channel_id(channel_name)

    if channel_id not in _g._CHANNELS:
        raise NotFound(f"Requested URL {request.path} not found")

    ua = request.headers.get("user-agent", "")
    if " Chrome/" in ua:
        return await handle_flussonic(request, f"{int(datetime.now().timestamp())}.ts", channel_id)

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_WEBM, status=200)

    if _g._NETWORK_SATURATED and not await ongoing_vods(_fast=True):
        log.warning(f"[{request.ip}] {request.path} -> Network Saturated")
        raise ServiceUnavailable("Network Saturated")

    _response, ch, prom = await request.respond(content_type=MIME_WEBM), _g._CHANNELS[channel_id], None
    try:
        with closing(socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) as sock:
            sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            sock.bind((ch.address if not WIN32 else "", ch.port))
            sock.setsockopt(IPPROTO_IP, IP_ADD_MEMBERSHIP, inet_aton(ch.address) + inet_aton(_g._IPTV))
            with closing(await dgram_from_socket(sock)) as stream:
                # 1st packet on SDTV channels is bogus and breaks ffmpeg
                if ua.startswith("Jellyfin") and " HD" not in ch.name:
                    log.debug('UA="%s" detected, skipping first packet' % ua)
                    await stream.recv()
                else:
                    await _response.send((await stream.recv())[0][28:])

                prom = app.add_task(
                    add_prom_event(
                        PromEvent(
                            ch_id=channel_id,
                            endpoint=f"{ch.name} _ {request.ip}",
                            id=_start,
                            lat=time.time() - _start,
                            method="live",
                            msg=f"[{request.ip}] -> Playing {request.url}",
                        )
                    )
                )

                while True:
                    await _response.send((await stream.recv())[0][28:])

    finally:
        if prom:
            prom.cancel()
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

    if channel_id not in _g._CHANNELS:
        raise NotFound(f"Requested URL {request.path} not found")

    p_vod = get_program_vod(channel_id, url, cloud, local)
    if not p_vod:
        raise NotFound(f"Requested URL {request.path} not found")

    if request.method == "HEAD":
        return response.HTTPResponse(content_type=MIME_WEBM, status=200)

    event = PromEvent(
        ch_id=channel_id,
        endpoint=f"{p_vod.ch_name} _ {request.ip}",
        id=_start,
        lat=0,
        method="catchup",
        msg=f"[{request.ip}] -> Playing {request.url}",
        url=url,
    )

    ua = request.headers.get("user-agent", "")

    if local:
        if os.path.exists(p_vod.pid):
            if " Chrome/" in ua or not p_vod.pid.endswith(".ts"):
                return await transcode(request, event, p_vod, filename=p_vod.pid, offset=p_vod.offset)

            _stat = await aiofiles.os.stat(p_vod.pid)
            bytepos = round(p_vod.offset * _stat.st_size / p_vod.duration)
            bytepos -= bytepos % CHUNK
            to_send = _stat.st_size - bytepos

            async with aiofiles.open(p_vod.pid, mode="rb") as f:
                await f.seek(bytepos)
                _response = await request.respond(content_type=MIME_WEBM)
                prom = app.add_task(add_prom_event(event, cloud, local, p_vod))

                try:
                    while to_send >= CHUNK:
                        content = await f.read(BUFF)
                        await _response.send(content)
                        to_send -= len(content)
                finally:
                    if prom:
                        prom.cancel()
                    await _response.eof()

            return
        raise NotFound(f"Requested URL {request.path} not found")

    if _g._NETWORK_SATURATED and not await ongoing_vods(_fast=True):
        log.warning(f"[{request.ip}] {request.path} -> Network Saturated")
        raise ServiceUnavailable("Network Saturated")

    client_port = find_free_port(_g._IPTV)
    info = await get_vod_info(_g.vod_client, _g.ep, channel_id, cloud, p_vod.pid)
    if not info:
        await prom_event(event, "na", cloud, p_vod=p_vod)
        raise NotFound(f"Requested URL {request.path} not found")

    args = VodArgs(channel_id, p_vod.pid, request.ip, client_port, p_vod.offset, cloud)
    vod = app.add_task(Vod(args, _g.vod_client, info))
    if not vod:
        raise NotFound(f"Requested URL {request.path} not found")

    if " Chrome/" in ua:
        return await transcode(request, event, p_vod, channel_id=channel_id, port=client_port, vod=vod)

    _response, prom = await request.respond(content_type=MIME_WEBM), None
    try:
        with closing(await dgram_bind((_g._IPTV, client_port))) as stream:
            # 1st packet on SDTV channels is bogus and breaks ffmpeg
            if ua.startswith("Jellyfin") and " HD" not in _g._CHANNELS[channel_id].name:
                log.debug('UA="%s" detected, skipping first packet' % ua)
                await stream.recv()
            else:
                await _response.send((await stream.recv())[0])

            prom = app.add_task(add_prom_event(event._replace(lat=time.time() - _start), cloud, local, p_vod))

            while True:
                await _response.send((await stream.recv())[0])

    finally:
        vod.cancel()
        if prom:
            prom.cancel()
        await _response.eof()


@app.route("/cloud/<channel_id:int>/<url>", methods=["GET", "HEAD"], name="flussonic_cloud_id")
@app.route(r"/cloud/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"], name="flussonic_cloud_name")
@app.route("/local/<channel_id:int>/<url>", methods=["GET", "HEAD"], name="flussonic_local_id")
@app.route(r"/local/<channel_name:([A-Za-z1-9]+)>/<url>", methods=["GET", "HEAD"], name="flussonic_local_name")
async def handle_flussonic_extra(request, url, channel_id=None, channel_name=None):
    if url in ("live", "mpegts"):
        return await handle_channel(request, channel_id, channel_name)
    cloud, local = (request.route.path.startswith(x) for x in ("cloud", "local"))
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
        "cloud.xml": _g.GUIDE_CLOUD,
        "guia.xml": _g.GUIDE,
        "guia.xml.gz": _g.GUIDE + ".gz",
        "guide.xml": _g.GUIDE,
        "guide.xml.gz": _g.GUIDE + ".gz",
        "local.xml": _g.GUIDE_LOCAL,
        "nube.xml": _g.GUIDE_CLOUD,
    }[request.route.path]
    pad = 10 if request.route.path.endswith(".xml") else 7

    if not os.path.exists(guide):
        raise NotFound(f"Requested URL {request.path} not found")

    log.info(f'[{request.ip}] {request.method} {request.url}{" " * pad} => "{guide}"')
    return await response.file(guide, validate_when_requested=False, mime_type=MIME_GUIDE)


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
        return response.HTTPResponse(
            content_type="image/jpeg" if not urls[0].endswith(".png") else "image/png", status=200
        )

    for url in urls:
        try:
            async with _g._SESSION.get(url) as r:
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
        m3u_matched = _g.CHANNELS
        pad = " " * 5
    elif m3u in ("movistartvcloud", "cloud", "nube"):
        m3u_matched = _g.CHANNELS_CLOUD
    elif m3u in ("movistartvlocal", "local"):
        m3u_matched = _g.CHANNELS_LOCAL
    elif m3u in ("grabaciones", "recordings"):
        m3u_matched = os.path.join(_g.RECORDINGS, "Recordings.m3u")
        pad = " " * 5
    else:
        channel_id = get_channel_id(m3u)
        if channel_id in _g._CHANNELS:
            channel_tag = "%03d. %s" % (_g._CHANNELS[channel_id].number, _g._CHANNELS[channel_id].name)
            m3u_matched = os.path.join(os.path.join(_g.RECORDINGS, channel_tag), f"{channel_tag}.m3u")

    if os.path.exists(m3u_matched):
        log.info(f'[{request.ip}] {request.method} {request.url}{pad} => "{m3u_matched}"')
        return await response.file(m3u_matched, validate_when_requested=False, mime_type=MIME_M3U)

    raise NotFound(f"Requested URL {request.path} not found")


@app.get("/favicon.ico", name="favicon")
async def handle_notfound(request):
    return response.empty(404)


@app.get("/program_title/<channel_id:int>/<program_id:int>", name="program_title")
async def handle_program_title(request, channel_id, program_id):
    cloud = request.args.get("cloud", "") == "1"
    epg = get_epg(channel_id, program_id, cloud)[1]
    if epg:
        return response.json({"full_title": epg.full_title})
    raise NotFound(f"Requested URL {request.path} not found")


@app.get("/record/<channel_id:int>/<url>", name="record_program_id")
@app.get(r"/record/<channel_name:([A-Za-z1-9]+)>/<url>", name="record_program_name")
async def handle_record_program(request, url, channel_id=None, channel_name=None):
    if not _g.RECORDINGS:
        raise NotFound(f"Requested URL {request.path} not found")

    if not url:
        log.warning("Cannot record live channels! Use wget for that.")
        raise NotFound(f"Requested URL {request.path} not found")

    if channel_name:
        channel_id = get_channel_id(channel_name)

    if channel_id not in _g._CHANNELS:
        raise NotFound(f"Requested URL {request.path} not found")

    cloud = request.args.get("cloud") == "1"
    comskip = int(request.args.get("comskip", "0"))
    index = request.args.get("index", "1") == "1"
    mkv = request.args.get("mkv") == "1"
    record_time = int(request.args.get("time", "0"))
    vo = request.args.get("vo") == "1"

    if comskip not in (0, 1, 2):
        raise NotFound(f"Requested URL {request.path} not found")

    p_vod = get_program_vod(channel_id, url, cloud)
    if not p_vod:
        raise NotFound(f"Requested URL {request.path} not found")

    if not record_time:
        record_time = p_vod.duration - p_vod.offset

    msg = await record_program(channel_id, p_vod.pid, p_vod.offset, record_time, cloud, comskip, index, mkv, vo)
    if msg:
        raise Forbidden(msg)

    return response.json(
        {
            "status": "OK",
            "channel": p_vod.ch_name,
            "channel_id": channel_id,
            "pid": p_vod.pid,
            "start": p_vod.start,
            "offset": p_vod.offset,
            "time": record_time,
            "cloud": cloud,
            "comskip": comskip,
            "index": index,
            "mkv": mkv,
            "vo": vo,
        }
    )


@app.route("/recording/", methods=["GET", "HEAD"], name="recording")
async def handle_recording(request):
    if not _g.RECORDINGS:
        raise NotFound(f"Requested URL {request.path} not found")

    _path = urllib.parse.unquote(request.url).split("/recording/")[1][1:]
    ext = os.path.splitext(_path)[1]
    mime_img = "image/jpeg" if ext == ".jpg" else "image/png" if ext == ".png" else None

    if _g.RECORDINGS_TMP and ext in (".jpg", ".png"):
        cached_cover = os.path.join(_g.RECORDINGS_TMP, "covers", _path)
        if os.path.exists(cached_cover):
            if request.method == "HEAD":
                return response.HTTPResponse(content_type=mime_img, status=200)

            return await response.file(cached_cover, validate_when_requested=False, mime_type=mime_img)
        log.warning(f'Cover not found in cache: "{_path}"')

    file = os.path.join(_g.RECORDINGS, _path)
    if os.path.exists(file):
        if request.method == "HEAD":
            return response.HTTPResponse(status=200)

        if ext in (".jpg", ".png"):
            return await response.file(file, validate_when_requested=False, mime_type=mime_img)

        if ext in VID_EXTS:
            try:
                _range = ContentRangeHandler(request, await aiofiles.os.stat(file))
                return await response.file_stream(file, mime_type=MIME_WEBM, _range=_range)
            except HeaderNotFound:
                pass

    raise NotFound(f"Requested URL {request.path} not found")


@app.get("/reindex_recordings", name="recordings_reindex")
@app.get("/reload_recordings", name="recordings_reload")
async def handle_recordings(request):
    if not _g.RECORDINGS:
        raise NotFound(f"Requested URL {request.path} not found")

    app.add_task(reindex_recordings() if request.route.path == "reindex_recordings" else reload_recordings())
    _m = "Reindex" if request.route.path == "reindex_recordings" else "Reload"
    return response.json({"status": f"Recordings {_m} Queued"}, 200)


@app.get("/reload_epg", name="epg_reload")
@app.get("/update_epg", name="epg_update")
async def handle_reload_epg(request):
    app.add_task(reload_epg() if request.route.path == "reload_epg" else update_epg())
    _m = "Reload" if request.route.path == "reload_epg" else "Update"
    return response.json({"status": f"EPG {_m} Queued"}, 200)


@app.get("/timers_check", name="timers_check")
async def handle_timers_check(request):
    if not _g.RECORDINGS:
        raise NotFound(f"Requested URL {request.path} not found")

    if _g._NETWORK_SATURATION:
        return response.json({"status": await log_network_saturated()}, 404)

    delay = int(request.args.get("delay", 0))

    if _g._t_timers and not _g._t_timers.done():
        if delay:
            _g._t_timers.cancel()
        else:
            raise Forbidden("Already processing timers")

    _g._t_timers = app.add_task(timers_check(delay=delay))

    return response.json({"status": "Timers check queued"}, 200)


async def transcode(request, event, p_vod, channel_id=0, filename="", offset=0, port=0, vod=None):
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
        cmd += ["-skip_initial_bytes", f"{CHUNK}"] if " HD" not in _g._CHANNELS[channel_id].name else []
        cmd += ["-i", f"udp://@{_g._IPTV}:{port}"]
    cmd += ["-map", "0:v", *lang_channel, "-c:v:0", "copy", "-c:a:0", "libfdk_aac", "-b:a", "128k"]
    cmd += ["-f", "matroska", "-v", "fatal", "-"]

    proc = await asyncio.create_subprocess_exec(*cmd, stdin=DEVNULL, stdout=PIPE)
    _response = await request.respond(content_type=MIME_WEBM)

    await _response.send(await proc.stdout.read(BUFF))
    prom = app.add_task(add_prom_event(event._replace(lat=time.time() - event.id), p_vod=p_vod))

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
        if prom:
            prom.cancel()
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

        setproctitle("mu7d")

    else:
        from psutil import ABOVE_NORMAL_PRIORITY_CLASS

    try:
        Process().nice(-10 if not WIN32 else ABOVE_NORMAL_PRIORITY_CLASS)
    except AccessDenied:
        pass

    def _check_iptv():
        iptv_iface_ip = _get_iptv_iface_ip()
        while True:
            uptime = int(datetime.now().timestamp() - boot_time())
            try:
                iptv_ip = get_iptv_ip()
                if not CONF["IPTV_IFACE"] or WIN32 or iptv_iface_ip and iptv_iface_ip == iptv_ip:
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
        with closing(socket(AF_INET, SOCK_STREAM)) as sock:
            if sock.connect_ex((CONF["LAN_IP"], CONF["U7D_PORT"])) == 0:
                raise LocalNetworkError(f"El puerto {CONF['LAN_IP']}:{CONF['U7D_PORT']} está ocupado")

    def _get_iptv_iface_ip():
        iptv_iface = CONF["IPTV_IFACE"]
        if iptv_iface:
            import netifaces

            while True:
                uptime = int(datetime.now().timestamp() - boot_time())
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

    if not CONF:
        log.critical("Imposible parsear fichero de configuración")
        sys.exit(1)

    banner = f"Movistar U7D v{_version}"
    log.info("=" * len(banner))
    log.info(banner)
    log.info("=" * len(banner))

    if LINUX and any(("UID" in CONF, "GID" in CONF)):
        log.info("Dropping privileges...")
        if "GID" in CONF:
            try:
                os.setgid(CONF["GID"])
            except PermissionError:
                log.warning(f"Could not drop privileges to UID {CONF['UID']}")
        if "UID" in CONF:
            try:
                os.setuid(CONF["UID"])
            except PermissionError:
                log.warning(f"Could not drop privileges to GID {CONF['GID']}")

    os.environ["PATH"] = "%s;%s" % (os.path.dirname(__file__), os.getenv("PATH"))
    os.environ["PYTHONOPTIMIZE"] = "0" if CONF["DEBUG"] else "2"
    os.environ["U7D_PARENT"] = str(os.getpid())

    _g.CHANNELS = CONF["CHANNELS"]
    _g.CHANNELS_CLOUD = CONF["CHANNELS_CLOUD"]
    _g.CHANNELS_LOCAL = CONF["CHANNELS_LOCAL"]
    _g.DEBUG = CONF["DEBUG"]
    _g.GUIDE = CONF["GUIDE"]
    _g.GUIDE_CLOUD = CONF["GUIDE_CLOUD"]
    _g.GUIDE_LOCAL = CONF["GUIDE_LOCAL"]
    _g.IPTV_BW_HARD = CONF["IPTV_BW_HARD"]
    _g.IPTV_BW_SOFT = CONF["IPTV_BW_SOFT"]
    _g.IPTV_IFACE = CONF["IPTV_IFACE"]
    _g.LAN_IP = CONF["LAN_IP"]
    _g.MKV_OUTPUT = CONF["MKV_OUTPUT"]
    _g.RECORDINGS = CONF["RECORDINGS"]
    _g.RECORDINGS_M3U = CONF["RECORDINGS_M3U"]
    _g.RECORDINGS_REINDEX = CONF["RECORDINGS_REINDEX"]
    _g.RECORDINGS_PROCESSES = CONF["RECORDINGS_PROCESSES"]
    _g.RECORDINGS_TMP = CONF["RECORDINGS_TMP"]
    _g.RECORDINGS_UPGRADE = CONF["RECORDINGS_UPGRADE"]
    _g.U7D_PORT = CONF["U7D_PORT"]
    _g.U7D_URL = CONF["U7D_URL"]
    _g.VID_EXT = ".mkv" if CONF["MKV_OUTPUT"] else ".ts"

    VodArgs = namedtuple("Vod", "channel, program, client_ip, client_port, start, cloud")

    monitor(
        app,
        is_middleware=False,
        latency_buckets=[1.0],
        mmc_period_sec=None,
        multiprocess_mode="livesum",
    ).expose_endpoint()

    lockfile = os.path.join(CONF["TMP_DIR"], ".mu7d.lock")
    try:
        with FileLock(lockfile, timeout=0):
            _check_iptv()
            _check_ports()
            del CONF
            app.run(
                host=_g.LAN_IP,
                port=_g.U7D_PORT,
                access_log=False,
                auto_reload=False,
                backlog=0,
                debug=_g.DEBUG,
                protocol=VodHttpProtocol,
                register_sys_signals=False,
                single_process=True,
                workers=1,
            )
    except (CancelledError, KeyboardInterrupt):
        sys.exit(0)
    except IPTVNetworkError as err:
        log.critical(err)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
