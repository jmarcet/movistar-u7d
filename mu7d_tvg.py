#!/usr/bin/env python3
#
# Based on tv_grab_es_movistartv by Willow:
# Source: https://github.com/MovistarTV/tv_grab_es_movistartv

import argparse
import asyncio
import codecs
import json
import logging
import os
import re
import struct
import sys
import time
from asyncio.exceptions import CancelledError
from collections import defaultdict, deque
from contextlib import closing
from datetime import date, datetime, timedelta
from glob import iglob
from html import escape, unescape
from itertools import combinations
from json import JSONDecodeError
from socket import (
    AF_INET,
    IP_ADD_MEMBERSHIP,
    IPPROTO_IP,
    IPPROTO_UDP,
    SO_REUSEADDR,
    SOCK_DGRAM,
    SOL_SOCKET,
    inet_aton,
    socket,
)

import aiohttp
import asyncstdlib as a
from aiofiles import open as async_open, os as aio_os
from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from async_files.gzip import open as async_gzip_open
from asyncio_dgram import from_socket as dgram_from_socket
from defusedxml.ElementTree import ParseError, fromstring
from filelock import FileLock, Timeout

from mu7d_cfg import CONF, DATEFMT, END_POINTS_FILE, FMT, UA, VERSION, WIN32, add_logfile
from mu7d_lib import IPTVNetworkError, get_end_point, get_iptv_ip, get_local_info, rename

log = logging.getLogger("TVG")


DEMARCATIONS = {
    15: "Andalucia",
    34: "Aragon",
    13: "Asturias",
    29: "Cantabria",
    1: "Catalunya",
    38: "Castilla la Mancha",
    4: "Castilla y Leon",
    6: "Comunidad Valenciana",
    32: "Extremadura",
    24: "Galicia",
    10: "Islas Baleares",
    37: "Islas Canarias",
    31: "La Rioja",
    19: "Madrid",
    12: "Murcia",
    35: "Navarra",
    36: "Pais Vasco",
}

THEME_MAP = {
    "Cine": "Movie",
    "Deportes": "Sports",
    "Documentales": "Documentary",
    "Infantil": "Children's",
    "Música": "Music",
    "Otros": "Other",
    "Programas": "Show",
    "Series": "Series",
}


class Cache:
    @staticmethod
    async def cache(full):
        await Cache.check_dirs()
        if full and datetime.now().hour < 1:
            await Cache.clean()

    @staticmethod
    async def check_dirs():
        progs_path = os.path.join(CACHE_DIR, "programs")
        if not await aio_os.path.exists(progs_path):
            await aio_os.makedirs(progs_path)

    @staticmethod
    async def clean():
        for file in iglob(os.path.join(CACHE_DIR, "programs", "*.json")):
            try:
                async with async_open(file, encoding="utf8") as f:
                    _data = json.loads(await f.read(), object_hook=Cache.keys_to_int)["data"]
                if _data["endTime"] // 1000 < _DEADLINE:
                    log.debug('Eliminando "%s" caducado', os.path.basename(file))
                    await aio_os.remove(file)
            except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError):
                pass

    @staticmethod
    def keys_to_int(data):
        return {int(k) if k.isdigit() else k: v for k, v in data.items()}

    @staticmethod
    async def load(cfile):
        try:
            async with async_open(os.path.join(CACHE_DIR, cfile), "r", encoding="utf8") as f:
                return json.loads(await f.read(), object_hook=Cache.keys_to_int)["data"]
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, TypeError, ValueError):
            return {}

    @staticmethod
    async def load_config():
        return await Cache.load("config.json")

    @staticmethod
    async def load_epg():
        return await Cache.load("epg.json")

    @staticmethod
    async def load_epg_extended_info(pid):
        return await Cache.load(os.path.join("programs", f"{pid}.json"))

    @staticmethod
    async def load_epg_local():
        try:
            async with async_open(os.path.join(HOME, "recordings.json"), "r", encoding="utf8") as f:
                return json.loads(await f.read(), object_hook=Cache.keys_to_int)
        except (FileNotFoundError, JSONDecodeError, OSError, PermissionError, ValueError):
            return

    @staticmethod
    async def load_epg_metadata():
        return await Cache.load("epg_metadata.json")

    @staticmethod
    async def load_service_provider_data():
        return await Cache.load("provider.json")

    @staticmethod
    async def save(cfile, data, sort_keys=False):
        _file = os.path.join(CACHE_DIR, cfile)
        async with async_open(_file + ".tmp", "w", encoding="utf8") as f:
            await f.write(json.dumps({"data": data}, ensure_ascii=False, indent=4, sort_keys=sort_keys))
        await rename(_file + ".tmp", _file)

    @staticmethod
    async def save_channels_data(xdata):
        channels, services = xdata["channels"], xdata["services"]

        clean_channels = {}
        for channel in (ch for ch in services if ch in EPG_CHANNELS & set(channels)):
            clean_channels[channel] = {
                "address": channels[channel]["address"],
                "name": channels[channel]["name"],
                "number": services[channel],
                "port": channels[channel]["port"],
            }

        await Cache.save("channels.json", clean_channels)

    @staticmethod
    async def save_config(data):
        await Cache.save("config.json", data)

    @staticmethod
    async def save_end_points(data):
        await Cache.save(END_POINTS_FILE, data)

    @staticmethod
    async def save_epg(data):
        await Cache.save("epg.json", data)

    @staticmethod
    async def save_epg_cloud(data):
        await Cache.save("cloud.json", data)

    @staticmethod
    async def save_epg_metadata(data):
        await Cache.save("epg_metadata.json", data)

    @staticmethod
    async def save_epg_extended_info(data):
        await Cache.save(os.path.join("programs", f'{data["productID"]}.json'), data, sort_keys=True)

    @staticmethod
    async def save_service_provider_data(data):
        await Cache.save("provider.json", data)


class MovistarTV:
    @staticmethod
    async def get_epg_extended_info(channel_id, ts, program):
        async def _fill_data(data):
            if data and any(
                program[t[0]] and program[t[0]] not in (data.get(t[1], ""), "Cine") for t in EPG_EXTINFO_PAIRS
            ):
                for src, dst in EPG_EXTINFO_PAIRS:
                    if program[src] and program[src] not in (data.get(dst, ""), "Cine"):
                        # log.debug('%s="%s" => %s="%s"', src, program.get(src, ""), dst, data.get(dst, ""))
                        data[dst] = program[src]
                await Cache.save_epg_extended_info(data)
                return True

        pid = program["pid"]
        data = await Cache.load_epg_extended_info(pid)
        await _fill_data(data)

        if not data or any(
            (
                data["beginTime"] // 1000 != ts,
                data["serviceUID"] != channel_id,
                not data.get("covers", {}).get("fanart"),
                not data.get("description", "").strip(),
                "duration" not in data,
                "labelGenre" not in data,
                "synopsis" not in data or data["synopsis"] == "Cine",
                all((data.get("theme", "") == "Cine", not data.get("originalTitle", "").strip())),
                data.get("theme", "") == "Series" and not any((data.get("season"), data.get("episode"))),
            )
        ):
            _data = await MovistarTV.get_service_data(f"epgInfov2&productID={pid}&channelID={channel_id}")
            if not _data:
                if data:
                    return data
                log.debug("Información extendida no encontrada: [%04d] [%d] [%d] ", channel_id, pid, ts)
                return
            if _data["beginTime"] // 1000 != ts and data and data["beginTime"] // 1000 != ts:
                log.debug(
                    "Event mismatch STILL BROKEN: [%4s] [%d] beginTime=[%+d]",
                    *(str(channel_id), pid, _data["beginTime"] // 1000 - ts),
                )
            if not await _fill_data(_data):
                await Cache.save_epg_extended_info(_data)
            return _data

        return data

    @staticmethod
    async def get_service_config(refresh):
        if not refresh or datetime.now().hour > 0:
            cfg = await Cache.load_config()
            if cfg:
                if refresh:
                    log.info(f'Demarcación: {DEMARCATIONS.get(cfg["demarcation"], cfg["demarcation"])}')
                    log.info(f'Paquete contratado: {cfg["tvPackages"]}')
                return cfg

        client, params, platform = await asyncio.gather(
            MovistarTV.get_service_data("getClientProfile"),
            MovistarTV.get_service_data("getConfigurationParams"),
            MovistarTV.get_service_data("getPlatformProfile"),
        )

        if not all((client, params, platform)):
            raise IPTVNetworkError("IPTV de Movistar no detectado")

        log.info(f'Demarcación: {DEMARCATIONS.get(client["demarcation"], client["demarcation"])}')
        log.info(f'Paquete contratado: {client["tvPackages"]}')

        dvb_entry_point = platform["dvbConfig"]["dvbipiEntryPoint"].split(":")
        genres = await MovistarTV.get_service_data(f'getEpgSubGenres&tvWholesaler={client["tvWholesaler"]}')
        uri = platform[next(filter(lambda f: re.search("base.*uri", f, re.IGNORECASE), platform.keys()))]
        conf = {
            "tvPackages": client["tvPackages"],
            "demarcation": client["demarcation"],
            "tvWholesaler": client["tvWholesaler"],
            "mcast_grp": dvb_entry_point[0],
            "mcast_port": int(dvb_entry_point[1]),
            "tvChannelLogoPath": "%s%s" % (uri, params["tvChannelLogoPath"]),
            "tvCoversPath": "%s%s%s290x429/" % (uri, params["tvCoversPath"], params["portraitSubPath"]),
            "tvCoversLandscapePath": "%s%s%s%s"
            % (uri, params["tvCoversPath"], params["landscapeSubPath"], params["bigSubpath"]),
            "genres": genres,
        }
        await Cache.save_config(conf)
        await MovistarTV.update_end_points(platform["endPoints"])
        return conf

    @staticmethod
    async def get_service_data(action):
        try:
            async with _SESSION.get(f"{_END_POINT}?action={action}") as response:
                if response.status == 200:
                    return json.loads(unescape(await response.text()))["resultData"]
        except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
            pass

    @staticmethod
    async def update_end_points(end_points):
        end_points = dict(sorted(end_points.items(), key=lambda x: int(re.sub(r"[A-Za-z]+", "", x[0])))).values()
        await Cache.save_end_points(tuple(dict.fromkeys(end_points)))


class MulticastIPTV:
    def __init__(self):
        self.__cached_epg = defaultdict(dict)
        self.__channels_keys = {}
        self.__epg = defaultdict(dict)
        self.__epg_lock = asyncio.Lock()
        self.__expired = 0
        self.__fixed = 0
        self.__new_gaps = defaultdict(list)
        self.__xml_data = {}

    def __expire_epg(self):
        epg = self.__cached_epg
        for channel in epg:
            _expired = tuple(filter(lambda ts: ts + epg[channel][ts]["duration"] < _DEADLINE, epg[channel]))
            tuple(map(epg[channel].pop, _expired))
            self.__expired += len(_expired)

    @staticmethod
    def __fill_cloud_event(data, meta, pid):  # timestamp, year):
        return {
            "pid": pid,
            "duration": data["duration"],
            "full_title": meta["full_title"],
            "genre": data["theme"],
            "serie": meta["serie"] or data.get("seriesName"),
        }

    def __fix_edges(self, channel, sorted_channel, new_epg=False):
        drop = defaultdict(list)
        fixed = gaps = 0
        if new_epg:
            name, epg = "New EPG   ", self.__epg
        else:
            name, epg = "Cached EPG", self.__cached_epg
        now = int(datetime.now().timestamp())
        chan = f"[{channel:4}] "

        for i in range(len(sorted_channel) - 1):
            ts = sorted_channel[i]
            _end = ts + epg[channel][ts]["duration"]
            _next = sorted_channel[i + 1]
            __ts, __end, __next = map(time.ctime, (ts, _end, _next))

            if _end < _next:
                msg = f"{chan}{name} GAP     ->  END:[{__end}] [{_end}] - TO:[{__next}] [{_next}]"
                if new_epg:
                    log.debug(msg)
                    self.__new_gaps[channel].append((_end, _next))
                elif ts < now:
                    if _next - ts < 1500:
                        epg[channel][ts]["duration"] = _next - ts
                        log.warning(msg.replace(" GAP    ", " EXTEND "))
                        fixed += 1
                    else:
                        log.warning(msg)
                        gaps += _next - _end

            elif _end > _next:
                if ts < now:
                    epg[channel][ts]["duration"] = _next - ts
                    msg = f"{chan}{name} SHORTEN ->  END:[{__end}] [{_end}] - TO:[{__next}] [{_next}]"
                    log.warning(msg)
                    fixed += 1
                else:
                    drop[channel].append(ts)
                    msg = f"{chan}{name} DROP    -> FROM:[{__ts}] [{ts}] - TO:[{__next}] [{_next}]"
                    log.debug(msg)

        for channel_id in drop:
            for ts in drop[channel_id]:
                del epg[channel_id][ts]

        return fixed, gaps

    def __fix_epg(self):
        fixed_diff = fixed_over = 0
        for channel in (ch for ch in self.__xml_data["services"] if ch in self.__epg):
            _new = sorted(self.__epg[channel])
            msg = f"[{channel:4}] New EPG            -> FROM:[{time.ctime(_new[0])}] [{_new[0]}] - "
            msg += f"TO:[{time.ctime(_new[-1])}] [{_new[-1]}]"

            log.debug(msg)

            for ts in _new:
                _duration = self.__epg[channel][ts]["duration"]
                _end = ts + _duration

                if _duration != _end - ts:
                    self.__epg[channel][ts]["duration"] = _end - ts
                    msg = f"New EPG    WRONG  -> FROM:[{time.ctime(ts)}] [{ts}] - "
                    msg += f"TO:[{time.ctime(ts + _duration)}] [{ts + _duration}] > "
                    msg += f"[{time.ctime(_end)}] [{_end}]"
                    log.warning(msg)
                    fixed_diff += 1

            fixed_over += self.__fix_edges(channel, _new, new_epg=True)[0]

        if fixed_diff or fixed_over:
            log.warning("El nuevo EPG contenía errores")
            if fixed_diff:
                _s = "programas no duraban" if fixed_diff > 1 else "programa no duraba"
                log.warning(f"{fixed_diff} {_s} el tiempo hasta al siguiente programa")
            if fixed_over:
                _s = "programas acababan" if fixed_over > 1 else "programa acababa"
                log.warning(f"{fixed_over} {_s} después de que el siguiente empezara")
        else:
            log.info("Nuevo EPG sin errores")

        self.__fixed = fixed_diff + fixed_over

        # log.debug("__fix_epg(): %s", str(self.__epg.keys()))

    async def __get_bin_epg(self):
        log.info("Descargando 8 días de EPG binario...")

        segments = self.__xml_data["segments"]
        await asyncio.gather(
            *(self.__get_epg_day(segments[key]["Address"], segments[key]["Port"], key) for key in segments)
        )

    @staticmethod
    def __get_channels(root):
        channel_list = {}
        for svc in (s for s in root[0][0].iterfind("{urn:dvb:ipisdns:2006}SingleService") if len(s[0]) > 0):
            channel_id = int(svc[1].attrib["ServiceName"])
            channel_list[channel_id] = {
                "id": channel_id,
                "address": svc[0][0].attrib["Address"],
                "port": int(svc[0][0].attrib["Port"]),
                "name": svc[2][0].text.encode("latin1").decode("utf8").strip(" .*"),
                "shortname": svc[2][1].text.encode("latin1").decode("utf8"),
                "genre": svc[2][3][0].text.encode("latin1").decode("utf8"),
                "logo_uri": svc[1].attrib.get("logoURI", "4146.jpg"),
            }
            if svc[2][4].tag == "{urn:dvb:ipisdns:2006}ReplacementService":
                channel_list[channel_id]["replacement"] = int(svc[2][4][0].attrib["ServiceName"])
        return channel_list

    async def __get_epg_data(self, mcast_grp, mcast_port):
        log.info("Actualizando metadatos de la EPG. Descargando canales, paquetes y segmentos...")

        while True:
            _msg = ""
            try:
                xml = await self.get_xml_files(mcast_grp, mcast_port)
                _msg += "Ficheros XML: ["
                _msg += "2_0 " if "2_0" in xml else "    "
                _msg += "5_0 " if "5_0" in xml else "    "
                _msg += "6_0" if "6_0" in xml else "   "
                _msg += "] / [2_0 5_0 6_0]"
                if not all((x in xml for x in ("2_0", "5_0", "6_0"))):
                    log.warning(f"{_msg} => Incompletos. Reintentando...")
                    continue

                self.__xml_data["channels"] = self.__get_channels(fromstring(xml["2_0"].replace("\n", " ")))
                self.__xml_data["packages"] = self.__get_packages(fromstring(xml["5_0"].replace("\n", " ")))
                self.__xml_data["segments"] = self.__get_segments(fromstring(xml["6_0"].replace("\n", " ")))
                log.info(f"{_msg} => Completos y correctos")
                break
            except (AttributeError, IndexError, ParseError, KeyError, TypeError, UnicodeError) as ex:
                log.debug("%s", repr(ex))
                log.warning(f"{_msg} => Incorrectos. Reintentando...")

        self.__xml_data["services"] = self.__get_services()
        stats = tuple(len(self.__xml_data[x]) for x in ("segments", "channels", "packages", "services"))
        log.info("Días de EPG: %i _ Canales: %i _ Paquetes: %i _ Servicios contratados: %i", *stats)

        await Cache.save_epg_metadata(self.__xml_data)
        del self.__xml_data["packages"]

    async def __get_epg_day(self, mcast_grp, mcast_port, source):
        while True:
            try:
                await self.__parse_bin_epg_day(await self.get_xml_files(mcast_grp, mcast_port))
            except Exception as ex:  # pylint: disable=broad-exception-caught
                log.debug("%s -> %s", source.split(".")[0], repr(ex))
                continue
            log.info("%s -> descargado", source.split(".")[0])
            break

    @staticmethod
    def __get_packages(root):
        package_list = {}
        for package in root[0].iterfind("{urn:dvb:ipisdns:2006}Package"):
            package_name = package[0].text
            package_list[package_name] = {
                "id": package.attrib["Id"],
                "name": package_name,
                "services": {
                    service[0].attrib["ServiceName"]: service[1].text
                    for service in package
                    if service.tag != "{urn:dvb:ipisdns:2006}PackageName"
                },
            }
        return package_list

    @staticmethod
    def __get_segments(root):
        segment_list = {}
        for segments in root[0][1][1].iterfind("{urn:dvb:ipisdns:2006}DVBBINSTP"):
            source = segments.attrib["Source"]
            segment_list[source] = {
                "Source": source,
                "Port": int(segments.attrib["Port"]),
                "Address": segments.attrib["Address"],
                "Segments": {segment.attrib["ID"]: segment.attrib["Version"] for segment in segments[0]},
            }
        return segment_list

    @staticmethod
    async def __get_service_provider_ip():
        if datetime.now().hour > 0:
            data = await Cache.load_service_provider_data()
            if data:
                return data

        _demarcation = DEMARCATIONS.get(_CONFIG["demarcation"], f'la demarcación {_CONFIG["demarcation"]}')
        log.info("Buscando el Proveedor de Servicios de %s...", _demarcation)

        xml = (await MulticastIPTV.get_xml_files(_CONFIG["mcast_grp"], _CONFIG["mcast_port"], init=True))["1_0"]
        result = re.findall(
            f'DEM_{_CONFIG["demarcation"]}' + r'\..*?Address="(.*?)".*?\s*Port="(.*?)".*?',
            xml,
            re.DOTALL,
        )[0]
        data = {"mcast_grp": result[0], "mcast_port": int(result[1])}
        await Cache.save_service_provider_data(data)

        log.info("Proveedor de Servicios de %s: %s", _demarcation, data["mcast_grp"])

        return data

    def __get_services(self):
        _packages = self.__xml_data["packages"]

        services = {}
        for package in _CONFIG["tvPackages"].split("|") if _CONFIG["tvPackages"] != "ALL" else _packages:
            services.update(_packages.get(package, {}).get("services", {}))

        # Move "Portada HD" to last position, channel number 999
        key = next(k for k, v in services.items() if v == "0")
        services.pop(key)
        services[key] = "999"

        return {int(k): int(v) for k, v in services.items()}

    def __merge_epg(self):
        for channel in tuple(self.__epg):
            if channel in self.__cached_epg:
                merged_epg = {}
                _new = sorted(self.__epg[channel])
                new_start, new_end = _new[0], _new[-1] + self.__epg[channel][_new[-1]]["duration"]
                for ts in sorted(self.__cached_epg[channel]):
                    if (
                        ts < new_start
                        or ts >= new_end
                        or any(filter(lambda gap: gap[0] <= ts < gap[1], self.__new_gaps[channel]))
                    ):
                        merged_epg[ts] = self.__cached_epg[channel][ts]
                self.__cached_epg[channel] = {**merged_epg, **self.__epg[channel]}
            else:
                self.__cached_epg[channel] = self.__epg[channel]
            del self.__epg[channel]

        gaps = 0
        for channel in (ch for ch in self.__xml_data["services"] if ch in self.__cached_epg):
            _fixed, _gaps = self.__fix_edges(channel, sorted(self.__cached_epg[channel]))
            self.__fixed += _fixed
            gaps += _gaps

        # log.debug("__merge_epg(): %s", str(self.__cached_epg.keys()))
        return gaps

    @staticmethod
    def __parse_bin_epg_body(data, service_id):
        programs = {}
        while data:
            start = struct.unpack(">I", data[4:8])[0]
            duration = struct.unpack(">H", data[8:10])[0]
            title_end = struct.unpack("B", data[31:32])[0] + 32
            genre = "{:02X}".format(struct.unpack("B", data[20:21])[0])
            full_title = MulticastIPTV.decode_string(data[32:title_end])
            serie_id = struct.unpack(">H", data[title_end + 5 : title_end + 7])[0]
            year = struct.unpack(">H", data[title_end + 9 : title_end + 11])[0]
            meta_data = MulticastIPTV.get_title_meta(full_title, serie_id, service_id, genre, year)
            # episode = struct.unpack("B", data[title_end + 8 : title_end + 9])[0]
            # season = struct.unpack("B", data[title_end + 11 : title_end + 12])[0]
            programs[start] = {
                "pid": struct.unpack(">I", data[:4])[0],
                "duration": duration,
                "full_title": meta_data["full_title"],
                "genre": genre,
                "serie": meta_data["serie"],
            }
            pr_title_end = struct.unpack("B", data[title_end + 12 : title_end + 13])[0] + title_end + 13
            cut = pr_title_end or title_end
            data = data[struct.unpack("B", data[cut + 3 : cut + 4])[0] + cut + 4 :]
        return programs

    async def __parse_bin_epg_day(self, bin_epg_day):
        for _id in (d for d in bin_epg_day if bin_epg_day[d]):
            head = self.__parse_bin_epg_header(bin_epg_day[_id])
            key = self.__channels_keys.get(head["service_id"])
            if not key:
                continue
            async with self.__epg_lock:
                self.__epg[key].update(self.__parse_bin_epg_body(head["data"], head["service_id"]))
        # log.debug("__parse_bin_epg(): %s", str(self.__epg.keys()))

    @staticmethod
    def __parse_bin_epg_header(data):
        data = data.encode("latin1")
        body = struct.unpack("B", data[6:7])[0] + 7
        # "service_url": data[7:body],
        # "service_version": struct.unpack("B", data[5:6])[0],
        # "size": struct.unpack(">H", data[1:3])[0],
        return {"data": data[body:-4], "service_id": struct.unpack(">H", data[3:5])[0]}

    def __sort_epg(self):
        epg = self.__cached_epg
        _services = self.__xml_data["services"]
        self.__cached_epg = {ch: dict(sorted(epg[ch].items())) for ch in (k for k in _services if k in epg)}

    @staticmethod
    def decode_string(string):
        return unescape(("".join(chr(char ^ 0x15) for char in string)).encode("latin1").decode("utf8"))

    async def get_epg(self):
        self.__cached_epg = await Cache.load_epg()

        skipped = 0
        _channels = self.__xml_data["channels"]
        _services = self.__xml_data["services"]
        for channel_id in (ch for ch in _services if ch in set(_channels) - EPG_CHANNELS):
            channel_name = _channels[channel_id]["name"].strip(" *")
            channel_number = _services[channel_id]
            log.info(f'Saltando canal: [{channel_id:4}] "{channel_number:03}. {channel_name}"')
            if channel_id in self.__cached_epg:
                del self.__cached_epg[channel_id]
            skipped += 1
        if skipped:
            log.info(f"Canales disponibles no indexados: {skipped:2}")
        no_chs = EPG_CHANNELS - set(_services)
        if no_chs:
            log.warning(
                f"Canal{'es' if len(no_chs) > 1 else ''} no existente{'s' if len(no_chs) > 1 else ''}: {no_chs}"
            )

        # Services not in channels are special access channels like DAZN, Disney, Netflix & Prime
        self.__channels_keys = {
            _channels[key].get("replacement", key): key
            for key in _services
            if key in EPG_CHANNELS & set(_channels)
        }

        while True:
            await self.__get_bin_epg()
            self.__fix_epg()
            gaps = self.__merge_epg()
            self.__expire_epg()
            self.__sort_epg()
            await Cache.save_epg(self.__cached_epg)
            if not gaps:
                del self.__epg, self.__xml_data["segments"]
                break
            log.warning(f"EPG con huecos de [{str(timedelta(seconds=gaps))}s]. Descargando de nuevo...")

        log.info(f"Eventos en Caché: Arreglados = {self.__fixed} _ Caducados = {self.__expired}")
        return self.__cached_epg

    async def get_epg_cloud(self):
        data = await MovistarTV.get_service_data("recordingList&mode=0&state=2&firstItem=0&numItems=999")
        if not data or not data.get("result"):
            log.info("No existen grabaciones en la Nube")
            return
        cloud_recordings = data["result"]

        _channels = self.__xml_data["channels"]

        for program in cloud_recordings:
            channel_id = program["serviceUID"]
            if channel_id not in EPG_CHANNELS:
                log.warning(f"[{channel_id:4}] tiene grabaciones en la Nube pero no está activado")
                continue

            pid = program["productID"]
            timestamp = program["beginTime"] // 1000

            data = await MovistarTV.get_service_data(f"epgInfov2&productID={pid}&channelID={channel_id}")

            data = await MovistarTV.get_service_data(f"getRecordingData&extInfoID={pid}&channelID={channel_id}")
            if not data:  # There can be events with no data sometimes
                continue

            service_id = _channels[channel_id].get("replacement", channel_id)
            meta = MulticastIPTV.get_title_meta(
                data["name"], data.get("seriesID"), service_id, data["theme"], data.get("year", 0)
            )

            self.__cached_epg[channel_id][timestamp] = self.__fill_cloud_event(data, meta, pid)

        self.__sort_epg()
        await Cache.save_epg_cloud(self.__cached_epg)
        return self.__cached_epg

    async def get_service_provider_data(self, refresh):
        if not refresh:
            data = await Cache.load_epg_metadata()
            if data:
                self.__xml_data = data
                return data

        connection = await self.__get_service_provider_ip()
        await self.__get_epg_data(connection["mcast_grp"], connection["mcast_port"])
        return self.__xml_data

    @staticmethod
    def get_title_meta(title, serie_id, service_id, genre, year):
        try:
            _t = unescape(title).replace("\n", " ").replace("\r", " ").strip()
        except TypeError:
            _t = title.replace("\n", " ").replace("\r", " ").strip()
        full_title = re.sub(r"(\d+)/(\d+)", r"\1\2", _t)

        if title_select_regex.match(full_title):
            x = title_1_regex.search(full_title)
        else:
            x = title_2_regex.search(full_title)

        season = episode = 0
        serie = episode_title = ""

        if x and x.groups():
            _x = x.groups()
            serie = _x[0] or full_title.split(" - ", 1)[0]
            season = int(_x[1] or season)
            episode = int(_x[2] or episode)
            episode_title = _x[3] or episode_title

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
            if " - " in full_title:
                serie, episode_title = full_title.split(" - ", 1)
            else:
                serie = full_title

        # Boing (578) & MEGA (2863); Movies (genre[0] == "1")
        if all((service_id in (578, 2863), ": " in full_title, not episode_title, genre[0] != "1")):
            _match = re.match(r"^([^/]+): ([^a-z].+)", full_title)
            if _match:
                serie, episode_title = (x.strip(":-/ ") for x in _match.groups())
                serie = re.sub(r"([^,.])[,.]$", r"\1", serie)
                if season and episode:
                    episode_title = re.sub(r" S\d+E\d+$", "", episode_title)
                    full_title = "%s S%02dE%02d - %s" % (serie, season, episode, episode_title)
                else:
                    full_title = "%s - %s" % (serie, episode_title)

        for item in (full_title, serie):
            item = re.sub(r"\s+", " ", item)

        if genre[0] == "1" and year:
            full_title += f" ({year})"

        return {"full_title": full_title, "serie": serie}

    @staticmethod
    async def get_xml_files(mc_grp, mc_port, init=False):
        _files = {}
        failed = 0
        last_file = ""

        with closing(socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) as sock:
            sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            sock.bind((mc_grp if not WIN32 else "", mc_port))
            sock.setsockopt(IPPROTO_IP, IP_ADD_MEMBERSHIP, inet_aton(mc_grp) + inet_aton(_IPTV))
            with closing(await dgram_from_socket(sock)) as stream:
                # Wait for an end chunk to start at the beginning
                while True:
                    try:
                        chunk = MulticastIPTV.parse_chunk((await asyncio.wait_for(stream.recv(), timeout=3))[0])
                        if chunk["end"]:
                            last_file = f'{chunk["filetype"]}_{chunk["fileid"]}'
                            break
                    except (AttributeError, KeyError, TypeError, UnicodeError):
                        pass
                    except (IPTVNetworkError, OSError) as ex:
                        if init:
                            msg = "Multicast IPTV de Movistar no detectado"
                            failed += 1
                            if failed == 3:
                                raise IPTVNetworkError(msg) from ex
                            log.error(msg)
                        else:
                            log.debug("Timeout descargando XML")
                # Loop until last_file
                while True:
                    xmldata = ""
                    chunk = {"end": False}
                    while not chunk["end"]:
                        chunk = MulticastIPTV.parse_chunk((await stream.recv())[0])
                        xmldata += chunk["data"]
                    current_file = f'{chunk["filetype"]}_{chunk["fileid"]}'
                    _files[current_file] = xmldata[:-4]  # Discard last 4bytes binary footer
                    if current_file == last_file:
                        break
        return _files

    @staticmethod
    def parse_chunk(data):
        return {
            "end": struct.unpack("B", data[:1])[0],
            "filetype": struct.unpack("B", data[4:5])[0],
            "fileid": struct.unpack(">H", data[5:7])[0] & 0x0FFF,
            "data": data[12:].decode("latin1"),
        }


class XmlTV:
    def __init__(self, data):
        self.__doc = deque(['<?xml version="1.0" encoding="UTF-8"?>'])
        self.__channels = data["channels"]
        self.__services = data["services"]
        self.__trans = str.maketrans("()!?", "❨❩ᴉ‽")

    # pylint: disable=too-many-branches,too-many-statements
    async def __add_programmes_tags(self, ch_id, programs, local, tz_offset):
        ext_infos = []
        async for batch in a.batched(programs.keys(), 8 * (os.cpu_count() or 1)):
            if not local:
                ext_infos += await asyncio.gather(
                    *(MovistarTV.get_epg_extended_info(ch_id, ts, programs[ts]) for ts in batch)
                )
            else:
                ext_infos += await asyncio.gather(
                    *(
                        get_local_info(ch_id, ts, os.path.join(RECORDINGS, programs[ts]["filename"]), log, True)
                        for ts in batch
                    )
                )

        if local:
            tss = tuple(programs)
            last_idx = len(tss) - 1

        for idx, ts in enumerate(programs):
            ext_info = ext_infos[idx]
            if not local:
                program = programs[ts]
            else:
                if not ext_info:
                    log.error(f"Metadatos no encontrados -> {programs[ts]}")
                    continue
                program = ext_info
                if idx < last_idx and ts + program["duration"] > tss[idx + 1]:
                    program["duration"] = tss[idx + 1] - ts  # Fix overlapping between recordings

            dst_start = time.localtime(ts).tm_isdst
            dst_stop = time.localtime(ts + program["duration"]).tm_isdst
            start = datetime.fromtimestamp(ts).strftime("%Y%m%d%H%M%S")
            stop = datetime.fromtimestamp(ts + program["duration"]).strftime("%Y%m%d%H%M%S")

            self.__append_elem(
                "programme",
                attr={
                    "start": f"{start} +0{tz_offset + dst_start}00",
                    "stop": f"{stop} +0{tz_offset + dst_stop}00",
                    "channel": f"{ch_id}.movistar.tv",
                },
                pad=8,
                child=True,
            )

            is_serie = bool(program["serie"])
            title = program["serie"] or program["full_title"]
            subtitle = desc = ""
            _match = None

            if is_serie:
                subtitle = program["full_title"].split(title, 1)[-1].strip("- ")
                if subtitle in title:
                    subtitle = ""
            elif any((not local and program["genre"][0] == "1", local and ext_info["theme"] == "Cine")):
                _match = re.match(r"^(.+) \(\d{4}\)$", title)
                if _match:
                    title = program["full_title"] = _match.groups()[0]  # Remove year from movies' full_title

            if ext_info:
                if ext_info["theme"] == "Cine" and title == "Cine" and ext_info["name"] != "Cine":
                    title = program["full_title"] = ext_info["name"]

                orig_title = ext_info.get("originalTitle", "")
                orig_title = "" if orig_title.lower().lstrip().startswith("episod") else orig_title

                if orig_title:
                    _clean_origt = self.__clean(orig_title)

                    if is_serie:
                        if " - " in subtitle:
                            se, _stitle = subtitle.split(" - ", 1)
                            if _clean_origt.startswith(self.__clean(_stitle)):
                                subtitle = f"{se} - {orig_title}"
                    elif ext_info["theme"] in ("Cine", "Documentales") and " (" in program["full_title"]:
                        _match = re.match(r"([^()]+) \((.+)\)", program["full_title"])
                        if _match and orig_title == _match.groups()[1]:
                            title = _match.groups()[0]  # Remove (original title) from full_title

                    if ext_info["theme"] == "Cine":
                        subtitle = f"«{orig_title}»"

                    elif ext_info["theme"] not in ("Deportes", "Música", "Programas"):
                        if _clean_origt != self.__clean(title):
                            if not subtitle or (_clean_origt not in self.__clean(subtitle)):
                                desc = f"«{orig_title}»"

                if local and subtitle and ext_info.get("episodeName"):
                    if self.__clean(ext_info["episodeName"]) in self.__clean(subtitle):
                        if not subtitle.endswith(ext_info["episodeName"]):
                            if len(ext_info["episodeName"]) >= len(subtitle):
                                _match = series_regex.match(subtitle)
                                if _match:
                                    subtitle = _match.groups()[0] + " - " + ext_info["episodeName"]
                                else:
                                    subtitle = ext_info["episodeName"]

                if ext_info.get("description", "").strip():
                    if desc:
                        desc += "\n\n"
                    desc += re.sub(r"\s*\n", r"\n\n", re.sub(r",\s*\n", ", ", ext_info["description"].strip()))

            if OTT_HACK:
                title = title.translate(self.__trans)
                if desc:
                    desc = desc.translate(self.__trans)

            self.__append_elem("title", title.rstrip(":."), "es", pad=12)
            if subtitle:
                self.__append_elem("sub-title", subtitle, "es", pad=12)
            if desc:
                self.__append_elem("desc", desc, "es", pad=12)

            if ext_info:
                if any((key in ext_info for key in ("directors", "mainActors", "producer"))):
                    self.__append_elem("credits", pad=12, child=True)
                    for key in ("directors", "mainActors", "producer"):
                        if key in ext_info:
                            field = ext_info[key]
                            if isinstance(field, str):
                                field = field.split(", ")
                            key = re.sub(r"(?:main)?([^s]+)s?$", r"\1", key.lower())
                            for credit in field:
                                self.__append_elem(key, credit.strip(), pad=16)
                    self.__append_elem("credits", pad=12, close=True)

                year = ext_info.get("productionDate")
                if year:
                    self.__append_elem("date", year, pad=12)

                if ext_info.get("labelGenre"):
                    self.__append_elem("category", ext_info["labelGenre"], "es", pad=12)
                if ext_info.get("genre") and ext_info["genre"] != ext_info.get("labelGenre", ""):
                    self.__append_elem("category", ext_info["genre"], "es", pad=12)
                if ext_info.get("theme"):
                    if ext_info["theme"] not in (ext_info.get("genre", ""), ext_info.get("labelGenre", "")):
                        self.__append_elem("category", ext_info["theme"], "es", pad=12)
                    if ext_info["theme"] in THEME_MAP:
                        self.__append_elem("category", THEME_MAP[ext_info["theme"]], "en", pad=12)

                src = f"{U7D_URL}/{'recording/?' if local else 'Covers/'}" + ext_info["cover"]
                if ext_info.get("covers", {}).get("fanart"):
                    if not local:
                        src = f"{src}?fanart=" + os.path.basename(ext_info["covers"]["fanart"])
                    else:
                        src = f"{U7D_URL}/recording/?" + ext_info["covers"]["fanart"]
                self.__append_elem("icon", attr={"src": src}, pad=12)

                self.__append_elem("rating", attr={"system": "pl"}, pad=12, child=True)
                self.__append_elem("value", AGE_RATING[int(ext_info["ageRatingID"])], pad=16)
                self.__append_elem("rating", pad=12, close=True)

            self.__append_elem("programme", pad=8, close=True)
            yield

    def __append_elem(self, name, text=None, lang=None, attr={}, pad=0, child=False, close=False):
        elem = (" " * pad) + "<" + ("/" if close else "") + name
        if lang:
            elem += f' lang="{lang}"'
        elif attr:
            for key, value in attr.items():
                elem += f' {key}="{value}"'
        if text:
            elem += f">{escape(text)}</{name}>"
        elif any((child, close)):
            elem += ">"
        else:
            elem += "/>"
        self.__doc.append(elem)

    @staticmethod
    def __clean(string):
        return re.sub(r"[^a-z0-9]", "", string.lower())

    @staticmethod
    def __write(file_path, content):
        with codecs.open(file_path, "w", "UTF-8") as f:
            f.write(content)

    async def generate_xml(self, parsed_epg, local=False):
        attr = {
            "date": datetime.now().strftime("%Y%m%d%H%M%S"),
            "source-info-name": "Movistar IPTV Spain",
            "source-info-url": "https://www.movistar.es/",
            "generator-info-name": "Movistar U7D's mu7d_tvg",
            "generator-info-url": "https://github.com/jmarcet/movistar-u7d/",
        }
        self.__append_elem("tv", attr=attr, pad=4, child=True)

        for channel_id in parsed_epg:
            self.__append_elem("channel", attr={"id": f"{channel_id}.movistar.tv"}, pad=8, child=True)
            self.__append_elem("display-name", self.__channels[channel_id]["name"], pad=12)
            self.__append_elem(
                "icon", attr={"src": f"{U7D_URL}/Logos/" + self.__channels[channel_id]["logo_uri"]}, pad=12
            )
            self.__append_elem("channel", pad=8, close=True)

        yield self.__doc
        self.__doc.clear()

        tz_offset = abs(time.timezone // 3600)
        for channel_id in tuple(parsed_epg):
            async for _ in self.__add_programmes_tags(channel_id, parsed_epg[channel_id], local, tz_offset):
                yield self.__doc
                self.__doc.clear()
            del parsed_epg[channel_id]
        del parsed_epg

        self.__append_elem("tv", pad=4, close=True)
        yield self.__doc
        self.__doc.clear()

    def write_m3u(self, file_path, cloud=None, local=None):
        m3u = '#EXTM3U name="'
        m3u += "Cloud " if cloud else "Local " if local else ""
        m3u += 'MovistarTV" catchup="flussonic-ts" catchup-days="'
        m3u += '9999" ' if (cloud or local) else '8" '
        m3u += f'max-conn="12" refresh="1200" url-tvg="{U7D_URL}/'
        m3u += "cloud.xml" if cloud else "local.xml" if local else "guide.xml.gz"
        m3u += '"\n'

        services = {k: v for k, v in self.__services.items() if k in (cloud or local or EPG_CHANNELS)}

        for channel_id in (ch for ch in services if ch in self.__channels):
            channel_name = self.__channels[channel_id]["name"]
            channel_tag = "Cloud" if cloud else "Local" if local else "U7D"
            channel_tag += " - TDT Movistar.es"
            channel_number = services[channel_id]
            channel_logo = f"{U7D_URL}/Logos/" + self.__channels[channel_id]["logo_uri"]
            m3u += f'#EXTINF:-1 ch-number="{channel_number}" audio-track="2" '
            m3u += f'tvg-id="{channel_id}.movistar.tv" '
            m3u += f'group-title="{channel_tag}" '
            m3u += f'tvg-logo="{channel_logo}"'
            m3u += f",{channel_name}\n"
            m3u += f"{U7D_URL}"
            m3u += "/cloud" if cloud else "/local" if local else ""
            m3u += f"/{channel_id}/mpegts\n"

        self.__write(file_path, m3u)


def create_args_parser():
    now = datetime.now()
    desc = "Descarga guía EPG de Movistar TV desde %s hasta %s" % (
        (now - timedelta(hours=2)).strftime("%d/%m/%Y"),
        (now + timedelta(days=6)).strftime("%d/%m/%Y"),
    )
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--m3u", help="Exporta lista de canales en formato m3u a este fichero.")
    parser.add_argument("--guide", help="Actualiza y exporta la guía xmltv a este fichero.")
    parser.add_argument(
        "--cloud_m3u", help="Exporta canales con Grabaciones en la Nube, en formato m3u, a este fichero."
    )
    parser.add_argument(
        "--cloud_recordings", help="Exporta guía xmltv de Grabaciones en la Nube a este fichero."
    )
    parser.add_argument(
        "--local_m3u", help="Exporta canales con Grabaciones en Local, en formato m3u, a este fichero."
    )
    parser.add_argument("--local_recordings", help="Exporta guía xmltv de Grabaciones en Local a este fichero.")
    return parser


async def tvg_main(args, refresh, time_start):
    global _CONFIG, _END_POINT, _DEADLINE, _MIPTV, _SESSION, _XMLTV

    _DEADLINE = int(datetime.combine(date.today() - timedelta(days=7), datetime.min.time()).timestamp())

    await Cache.cache(full=bool(args.m3u or args.guide))  # Inicializa la caché

    _SESSION = aiohttp.ClientSession(headers={"User-Agent": UA}, json_serialize=json.dumps)

    epg = {}

    try:
        _END_POINT = await get_end_point()
        if not _END_POINT:
            return
        # Descarga la configuración del servicio Web de MovistarTV
        _CONFIG = await MovistarTV.get_service_config(refresh)

        # Busca el Proveedor de Servicios y descarga los archivos XML: canales, paquetes y segmentos
        _MIPTV = MulticastIPTV()
        xdata = await _MIPTV.get_service_provider_data(refresh)

        # Crea el objeto XMLTV a partir de los datos descargados del Proveedor de Servicios
        _XMLTV = XmlTV(xdata)

        if args.m3u:
            await Cache.save_channels_data(xdata)
            _XMLTV.write_m3u(args.m3u)
            if not args.guide:
                return

        epg_nr_days = len(xdata["segments"])
        del xdata

        # Descarga los segmentos de cada EPG_X_BIN.imagenio.es y obtiene la guía decodificada
        if args.guide:
            epg = await _MIPTV.get_epg()
        elif any((args.cloud_m3u, args.cloud_recordings, args.local_m3u, args.local_recordings)):
            if any((args.cloud_m3u, args.cloud_recordings)):
                epg = await _MIPTV.get_epg_cloud()
            else:
                epg = await Cache.load_epg_local()
            if not epg:
                return
            if any((args.cloud_m3u, args.local_m3u)):
                _XMLTV.write_m3u(
                    args.cloud_m3u or args.local_m3u,
                    cloud=tuple(epg) if args.cloud_m3u else None,
                    local=tuple(epg) if args.local_m3u else None,
                )
            if not any((args.cloud_recordings, args.local_recordings)):
                return
        del _MIPTV
        epg_nr_channels = len(epg)

        # Genera el árbol XMLTV de los paquetes contratados
        if args.guide:
            async with (
                async_open(args.guide + ".tmp", "w", encoding="utf8") as f,
                async_gzip_open(args.guide + ".gz" + ".tmp", "wt", encoding="utf8") as f_z,
            ):
                async for dom in _XMLTV.generate_xml(epg, bool(args.local_m3u or args.local_recordings)):
                    block = "\n".join(dom) + "\n"
                    await asyncio.gather(f.write(block), f_z.write(block))
            await rename(args.guide + ".tmp", args.guide)
            await rename(args.guide + ".gz" + ".tmp", args.guide + ".gz")
        elif any((args.cloud_recordings, args.local_recordings)):
            xml_file = args.cloud_recordings or args.local_recordings
            async with async_open(xml_file + ".tmp", "w", encoding="utf8") as f:
                async for dom in _XMLTV.generate_xml(epg, bool(args.local_m3u or args.local_recordings)):
                    await f.write("\n".join(dom) + "\n")
            await rename(xml_file + ".tmp", xml_file)

        if not any((args.cloud_recordings, args.local_recordings)):
            _t = str(timedelta(seconds=round(time.time() - time_start)))
            log.info(f"EPG de {epg_nr_channels} canales y {epg_nr_days} días generada en {_t}s")
            log.info(f"Fecha de caducidad: [{time.ctime(_DEADLINE)}] [{_DEADLINE}]")

    finally:
        await _SESSION.close()


if __name__ == "__main__":
    if not WIN32:
        from setproctitle import setproctitle

        setproctitle("mu7d_tvg      # %s" % " ".join(sys.argv[1:]))

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=CONF["DEBUG"] and logging.DEBUG or logging.INFO)

    if CONF.get("Exception"):
        log.critical(f'Imposible parsear fichero de configuración => {repr(CONF["Exception"])}')
        sys.exit(1)

    if CONF["LOG_TO_FILE"]:
        add_logfile(log, CONF["LOG_TO_FILE"], CONF["DEBUG"] and logging.DEBUG or logging.INFO)

    args = create_args_parser().parse_args()

    if any((args.cloud_m3u, args.cloud_recordings, args.local_m3u, args.local_recordings)):
        refresh = False  # pylint: disable=invalid-name
    else:
        refresh = True  # pylint: disable=invalid-name
        banner = f"Movistar U7D - TVG v{VERSION}"
        log.info("-" * len(banner))
        log.info(banner)
        log.info("-" * len(banner))
        log.debug("%s", " ".join(sys.argv[1:]))

    if any(
        map(
            all,
            combinations(
                (
                    any((args.m3u, args.guide)),
                    any((args.cloud_m3u, args.cloud_recordings)),
                    any((args.local_m3u, args.local_recordings)),
                ),
                2,
            ),
        )
    ):
        log.critical("No es posible mezclar categorías")
        sys.exit(1)

    _CONFIG = _DEADLINE = _END_POINT = _IPTV = _MIPTV = _SESSION = _XMLTV = None

    CACHE_DIR = CONF["CACHE_DIR"]
    EPG_CHANNELS = CONF["EPG_CHANNELS"]
    HOME = CONF["HOME"]
    OTT_HACK = CONF["OTT_HACK"]
    RECORDINGS = CONF["RECORDINGS"]
    U7D_URL = CONF["U7D_URL"]

    AGE_RATING = ("0", "0", "0", "7", "12", "16", "17", "18")
    EPG_EXTINFO_PAIRS = (("full_title", "name"), ("serie", "seriesName"))

    episode_regex = re.compile(r"^.*(?:Ep[isode.]+) (\d+)$")
    series_regex = re.compile(r"^(S\d+E\d+|Ep[isode]*\.)(?:.*)")
    title_select_regex = re.compile(r".+ T\d+ .+")
    title_1_regex = re.compile(r"(.+(?!T\d)) +T(\d+)(?: *Ep[isode.]+ (\d+))?[ -]*(.*)")
    title_2_regex = re.compile(r"(.+(?!T\d))(?: +T(\d+))? *(?:[Ee]p[isode.]+|[Cc]ap[iítulo.]*) ?(\d+)[ .-]*(.*)")

    local = "-local" if any((args.local_m3u, args.local_recordings)) else ""  # pylint: disable=invalid-name
    lockfile = os.path.join(CONF["TMP_DIR"], f".mu7d_tvg{local}.lock")
    del CONF, local
    try:
        with FileLock(lockfile, timeout=0):
            _IPTV = get_iptv_ip()
            asyncio.run(tvg_main(args, refresh, time.time()))
    except (CancelledError, KeyboardInterrupt):
        sys.exit(0)
    except IPTVNetworkError as err:
        log.critical(err)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
