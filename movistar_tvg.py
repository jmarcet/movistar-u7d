#!/usr/bin/env python3
#
# Based on tv_grab_es_movistartv by Willow:
# Source: https://github.com/MovistarTV/tv_grab_es_movistartv

import aiohttp
import argparse
import asyncio
import codecs
import defusedxml.ElementTree as ElTr
import glob
import gzip
import logging
import os
import random
import re
import socket
import struct
import sys
import threading
import time
import ujson as json

from aiohttp.client_exceptions import ClientConnectionError, ClientOSError, ServerDisconnectedError
from asyncio.exceptions import CancelledError
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime, timedelta
from filelock import FileLock, Timeout
from html import unescape
from queue import Queue
from xml.dom import minidom  # nosec B408
from xml.etree.ElementTree import Element, ElementTree, SubElement  # nosec B405

from mu7d import DATEFMT, FMT, UA, UA_U7D, WIN32, YEAR_SECONDS
from mu7d import add_logfile, get_iptv_ip, get_local_info, get_title_meta, mu7d_config, _version


log = logging.getLogger("TVG")


DEMARCATIONS = {
    "15": "Andalucia",
    "34": "Aragon",
    "13": "Asturias",
    "29": "Cantabria",
    "1": "Catalunya",
    "38": "Castilla la Mancha",
    "4": "Castilla y Leon",
    "6": "Comunidad Valenciana",
    "32": "Extremadura",
    "24": "Galicia",
    "10": "Islas Baleares",
    "37": "Islas Canarias",
    "31": "La Rioja",
    "19": "Madrid",
    "12": "Murcia",
    "35": "Navarra",
    "36": "Pais Vasco",
}

END_POINTS = {
    "epNoCach1": "http://www-60.svc.imagenio.telefonica.net:2001",
    "epNoCach2": "http://nc2.svc.imagenio.telefonica.net:2001",
    "epNoCach4": "http://nc4.svc.imagenio.telefonica.net:2001",
    "epNoCach5": "http://nc5.svc.imagenio.telefonica.net:2001",
    "epNoCach6": "http://nc6.svc.imagenio.telefonica.net:2001",
}

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
    def __init__(self, full=True):
        if full:
            self.check_dirs()
            if datetime.now().hour < 1:
                self.clean()

    @staticmethod
    def check_dirs():
        progs_path = os.path.join(CACHE_DIR, "programs")
        if not os.path.exists(progs_path):
            os.makedirs(progs_path)

    @staticmethod
    def clean():
        for file in glob.glob(os.path.join(CACHE_DIR, "programs", "*.json")):
            try:
                with open(file, encoding="utf8") as f:
                    _data = json.load(f)["data"]
                _exp_date = int(_data["endTime"] / 1000)
                if _exp_date < _DEADLINE:
                    log.debug('Eliminando "%s" caducado' % os.path.basename(file))
                    os.remove(file)
            except (IOError, KeyError, ValueError):
                pass

    @staticmethod
    def load(cfile):
        try:
            with open(os.path.join(CACHE_DIR, cfile), "r", encoding="utf8") as f:
                return json.load(f)["data"]
        except (FileNotFoundError, IOError, KeyError, ValueError):
            pass

    @staticmethod
    def load_config():
        return Cache.load("config.json")

    @staticmethod
    def load_end_points():
        return Cache.load(END_POINTS_FILE) or END_POINTS

    @staticmethod
    async def load_epg():
        data = Cache.load("epg.json")
        now = datetime.now()
        remote_cache_ts = int(now.replace(hour=0, minute=0, second=0).timestamp())
        if now.hour == 0 and now.minute < 59:
            remote_cache_ts -= 3600 * 24
        if not data or int(os.path.getmtime(os.path.join(CACHE_DIR, "epg.json"))) < remote_cache_ts:
            async with aiohttp.ClientSession(headers={"User-Agent": UA_U7D}) as _SESSION:
                for i in range(5):
                    log.info("Intentando obtener Caché de EPG actualizada...")
                    try:
                        async with _SESSION.get("https://openwrt.marcet.info/epg.json") as r:
                            if r.status == 200:
                                res = await r.json()
                                log.info("Obtenida Caché de EPG actualizada")
                                data = res["data"]
                                break
                            if i < 4:
                                log.warning(f"No ha habido suerte. Reintentando en 15s... [{i + 2} / 5]")
                                await asyncio.sleep(15)
                    except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
                        if i < 4:
                            log.warning(f"No ha habido suerte. Reintentando en 15s... [{i + 2} / 5]")
                            await asyncio.sleep(15)
            if not data:
                log.warning(
                    "Caché de EPG no encontrada. "
                    "Tendrá que esperar unos días para poder acceder a todo el archivo de los útimos 7 días."
                )
        if data:
            epg = defaultdict(dict)
            for channel in data:
                epg[int(channel)] = {int(k): v for k, v in data[channel].items()}
            return epg

    @staticmethod
    def load_epg_cloud():
        data = Cache.load("cloud.json")
        cloud_epg = defaultdict(dict)
        for channel in data:
            cloud_epg[int(channel)] = {int(k): v for k, v in data[channel].items()}
        return cloud_epg

    @staticmethod
    def load_epg_extended_info(pid):
        return Cache.load(os.path.join("programs", f"{pid}.json"))

    @staticmethod
    def load_epg_local():
        try:
            with open(os.path.join(_conf["HOME"], "recordings.json"), "r", encoding="utf8") as f:
                data = json.load(f)
        except (IOError, KeyError, ValueError):
            return

        local_epg = defaultdict(dict)
        for channel in data:
            local_epg[int(channel)] = {int(k): v for k, v in data[channel].items()}

        # Recordings can overlap, so we need to shorten the duration when they do
        for channel in sorted(local_epg):
            sorted_channel = sorted(local_epg[channel])
            for i in range(len(sorted_channel) - 1):
                ts = sorted_channel[i]
                _next = sorted_channel[i + 1]
                _end = ts + local_epg[channel][ts]["duration"]

                if _end >= _next:
                    _end = _next - 1
                    local_epg[channel][ts]["duration"] = _end - ts

        return local_epg

    @staticmethod
    def load_epg_metadata(refresh):
        metadata = Cache.load("epg_metadata.json")
        if not metadata:
            return

        now = datetime.now()
        update_time = int(now.replace(hour=0, minute=0, second=0).timestamp())
        log.debug("Fecha de actualización de epg_metadata: [%s] [%d]" % (time.ctime(update_time), update_time))
        if refresh and int(os.path.getmtime(os.path.join(CACHE_DIR, "epg_metadata.json"))) < update_time:
            return

        channels_data = defaultdict(dict)
        for channel in metadata["channels"]:
            channels_data[int(channel)] = metadata["channels"][channel]
        metadata["channels"] = channels_data

        return metadata

    @staticmethod
    def load_service_provider_data():
        return Cache.load("provider.json")

    @staticmethod
    def save(cfile, data):
        data = json.loads(unescape(json.dumps(data)))
        with open(os.path.join(CACHE_DIR, cfile), "w", encoding="utf8") as f:
            json.dump({"data": data}, f, ensure_ascii=False, indent=4, sort_keys=True)

    @staticmethod
    def save_config(data):
        Cache.save("config.json", data)

    @staticmethod
    def save_end_points(data):
        log.info(f"Nuevos End Points: {sorted(data.keys())}")
        Cache.save(END_POINTS_FILE, data)

    @staticmethod
    def save_epg(data):
        Cache.save("epg.json", data)

    @staticmethod
    def save_epg_metadata(data):
        Cache.save("epg_metadata.json", data)

    @staticmethod
    def save_epg_extended_info(data):
        Cache.save(os.path.join("programs", f'{data["productID"]}.json'), data)

    @staticmethod
    def save_service_provider_data(data):
        Cache.save("provider.json", data)


class MovistarTV:
    @staticmethod
    def get_end_point():
        return random.choice(list(MovistarTV.get_end_points().values()))  # nosec B311

    @staticmethod
    def get_end_points():
        if _CONFIG and _CONFIG.get("end_points"):
            return _CONFIG["end_points"]
        return Cache.load_end_points()

    @staticmethod
    async def get_epg_extended_info(channel_id, program):
        def _fill_data(data):
            pairs = [("episode", "episode"), ("episode_title", "episodeName"), ("full_title", "name")]
            pairs += [("season", "season"), ("serie", "seriesName"), ("year", "productionDate")]
            if data and any((program.get(t[0]) and program.get(t[0], "") != data.get(t[1], "") for t in pairs)):
                for src, dst in pairs:
                    if program.get(src) and program.get(src, "") != data.get(dst, ""):
                        # log.debug('%s="%s" => %s="%s"' % (src, program.get(src, ""), dst, data.get(dst, "")))
                        data[dst] = program[src]
                Cache.save_epg_extended_info(data)
                return True

        pid, ts = program["pid"], program["start"]
        data = Cache.load_epg_extended_info(pid)
        _fill_data(data)

        if not data or data["beginTime"] // 1000 != ts:
            _data = await MovistarTV.get_service_data(f"epgInfov2&productID={pid}&channelID={channel_id}")
            if not _data:
                if data:
                    return data
                log.debug("Información extendida no encontrada: [%04d] [%d] [%d] " % (channel_id, pid, ts))
                return
            if _data["beginTime"] // 1000 != ts and data and data["beginTime"] // 1000 != ts:
                log.debug(
                    "Event mismatch STILL BROKEN: [%4s] [%d] beginTime=[%+d]"
                    % (str(channel_id), pid, _data["beginTime"] // 1000 - ts)
                )
            if not _fill_data(_data):
                Cache.save_epg_extended_info(_data)
            return _data

        return data

    @staticmethod
    async def get_genres(tv_wholesaler):
        log.info("Descargando mapa de géneros")
        return await MovistarTV.get_service_data(f"getEpgSubGenres&tvWholesaler={tv_wholesaler}")

    @staticmethod
    async def get_service_config():
        cfg = Cache.load_config()
        if cfg:
            if _VERBOSE:
                log.info(f'Demarcación: {DEMARCATIONS.get(str(cfg["demarcation"]), cfg["demarcation"])}')
                log.info(f'Paquete contratado: {cfg["tvPackages"]}')
            return cfg

        log.info("Descargando configuración del cliente, parámetros de configuración y perfil del servicio")
        client, params, platform = await asyncio.gather(
            MovistarTV.get_service_data("getClientProfile"),
            MovistarTV.get_service_data("getConfigurationParams"),
            MovistarTV.get_service_data("getPlatformProfile"),
        )

        if not all((client, params, platform)):
            raise ValueError("IPTV de Movistar no detectado")

        if _VERBOSE:
            log.info(f'Demarcación: {DEMARCATIONS.get(str(client["demarcation"]), client["demarcation"])}')
            log.info(f'Paquete contratado: {client["tvPackages"]}')

        dvb_entry_point = platform["dvbConfig"]["dvbipiEntryPoint"].split(":")
        uri = platform[list(filter(lambda f: re.search("base.*uri", f, re.IGNORECASE), platform.keys()))[0]]
        conf = {
            "tvPackages": client["tvPackages"],
            "demarcation": client["demarcation"],
            "tvWholesaler": client["tvWholesaler"],
            "end_points": MovistarTV.update_end_points(platform["endPoints"]),
            "mcast_grp": dvb_entry_point[0],
            "mcast_port": int(dvb_entry_point[1]),
            "tvChannelLogoPath": "%s%s" % (uri, params["tvChannelLogoPath"]),
            "tvCoversPath": "%s%s%s290x429/" % (uri, params["tvCoversPath"], params["portraitSubPath"]),
            "tvCoversLandscapePath": "%s%s%s%s"
            % (uri, params["tvCoversPath"], params["landscapeSubPath"], params["bigSubpath"]),
            "genres": await MovistarTV.get_genres(client["tvWholesaler"]),
        }
        Cache.save_config(conf)
        return conf

    @staticmethod
    async def get_service_data(action):
        ep = MovistarTV.get_end_point()
        try:
            async with _SESSION.get(f"{ep}/appserver/mvtv.do?action={action}") as response:
                if response.status == 200:
                    return (await response.json())["resultData"]
        except (ClientConnectionError, ClientOSError, ServerDisconnectedError):
            pass

    @staticmethod
    def update_end_points(data):
        Cache.save_end_points(data)
        return data


class MulticastEPGFetcher(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self, daemon=True)
        self.queue = queue

    def run(self):
        while True:
            mcast = self.queue.get()
            _MIPTV.get_epg_day(mcast["mcast_grp"], mcast["mcast_port"], mcast["source"])
            self.queue.task_done()


class MulticastIPTV:
    def __init__(self):
        self.__epg = None
        self.__ch_ids = None
        self.__sv_ids = None
        self.__xml_data = {}

    def __clean_epg_channels(self, epg):
        if not self.__ch_ids:
            self.__fill_channel_ids()

        clean_epg = {}
        for channel in EPG_CHANNELS & set(epg) & set(self.__ch_ids):
            clean_epg[channel] = epg[channel]

        return clean_epg

    @staticmethod
    def __expire_epg(epg):
        expired = 0
        for channel in epg:
            _expired = tuple(filter(lambda ts: epg[channel][ts]["end"] < _DEADLINE, epg[channel]))
            tuple(map(epg[channel].pop, _expired))
            expired += len(_expired)
        return expired

    def __fill_channel_ids(self):
        services = {}
        _channels = self.__xml_data["channels"]
        _packages = self.__xml_data["packages"]

        for package in _CONFIG["tvPackages"].split("|") if _CONFIG["tvPackages"] != "ALL" else _packages:
            services.update(_packages.get(package, {}).get("services", {}))

        enabled = set(_channels) & EPG_CHANNELS
        self.__ch_ids = [int(k) for k in services]
        self.__sv_ids = [_channels[int(k)].get("replacement", int(k)) for k in services if int(k) in enabled]

    @staticmethod
    def __fix_epg(epg):
        fixed_diff = fixed_over = 0
        new_gaps = defaultdict(list)
        for channel in sorted(epg):
            _new = sorted(epg[channel])
            msg = f"[{channel:4}] New EPG            -> FROM:[{time.ctime(_new[0])}] [{_new[0]}] - "
            msg += f"TO:[{time.ctime(_new[-1])}] [{_new[-1]}]"

            log.debug(msg)

            for ts in sorted(epg[channel]):
                _duration = epg[channel][ts]["duration"]
                _end = epg[channel][ts]["end"]

                if _duration != _end - ts:
                    epg[channel][ts]["duration"] = _end - ts
                    msg = f"New EPG    WRONG  -> FROM:[{time.ctime(ts)}] [{ts}] - "
                    msg += f"TO:[{time.ctime(ts + _duration)}] [{ts + _duration}] > "
                    msg += f"[{time.ctime(_end)}] [{_end}]"
                    log.warning(msg)
                    fixed_diff += 1

            fixed_over += MulticastIPTV.fix_edges(epg, channel, _new, new_gaps=new_gaps)[0]

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

        return fixed_diff + fixed_over, new_gaps

    def __get_bin_epg(self):
        self.__epg = [{} for _ in range(len(self.__xml_data["segments"]))]
        queue = Queue()
        threads = 8

        for _ in range(threads):
            process = MulticastEPGFetcher(queue)
            process.start()

        for key in sorted(self.__xml_data["segments"]):
            queue.put(
                {
                    "mcast_grp": self.__xml_data["segments"][key]["Address"],
                    "mcast_port": self.__xml_data["segments"][key]["Port"],
                    "source": key,
                }
            )

        queue.join()

    @staticmethod
    def __get_channels(xml_channels):
        channel_list = {}
        root = ElTr.fromstring(xml_channels.replace("\n", " "))
        services = root[0][0].findall("{urn:dvb:ipisdns:2006}SingleService")

        for i in services:
            try:
                channel_id = int(i[1].attrib["ServiceName"])
                channel_list[channel_id] = {
                    "id": channel_id,
                    "address": i[0][0].attrib["Address"],
                    "port": i[0][0].attrib["Port"],
                    "name": i[2][0].text.encode("latin1").decode("utf8"),
                    "shortname": i[2][1].text.encode("latin1").decode("utf8"),
                    "genre": i[2][3][0].text.encode("latin1").decode("utf8"),
                    "logo_uri": i[1].attrib["logoURI"] if "logoURI" in i[1].attrib else "MAY_1/imSer/4146.jpg",
                }
                if i[2][4].tag == "{urn:dvb:ipisdns:2006}ReplacementService":
                    channel_list[channel_id]["replacement"] = int(i[2][4][0].attrib["ServiceName"])
            except (AttributeError, KeyError, IndexError, TypeError, UnicodeError):
                pass

        if _VERBOSE:
            log.info("Canales: %i" % len(channel_list))

        return channel_list

    def __get_epg_data(self, mcast_grp, mcast_port):
        while True:
            xml = self.get_xml_files(mcast_grp, mcast_port)
            _msg = "Ficheros XML: ["
            _msg += "2_0 " if "2_0" in xml else "    "
            _msg += "5_0 " if "5_0" in xml else "    "
            _msg += "6_0" if "6_0" in xml else "   "
            _msg += "] / [2_0 5_0 6_0]"
            if all((x in xml for x in ("2_0", "5_0", "6_0"))):
                log.info(f"{_msg} => Completos")
                break
            log.warning(f"{_msg} => Incompletos. Reintentando...")

        if _VERBOSE:
            log.info("Descargando canales y paquetes")

        self.__xml_data["channels"] = self.__get_channels(xml["2_0"])
        self.__xml_data["packages"] = self.__get_packages(xml["5_0"])
        if _VERBOSE:
            log.info("Descargando índices")
        self.__xml_data["segments"] = self.__get_segments(xml["6_0"])

        Cache.save_epg_metadata(self.__xml_data)

    @staticmethod
    def __get_packages(xml):
        package_list = {}
        root = ElTr.fromstring(xml.replace("\n", " "))
        packages = root[0].findall("{urn:dvb:ipisdns:2006}Package")

        for package in packages:
            try:
                package_name = package[0].text
                package_list[package_name] = {
                    "id": package.attrib["Id"],
                    "name": package_name,
                    "services": {},
                }
                for service in package:
                    if service.tag != "{urn:dvb:ipisdns:2006}PackageName":
                        service_id = service[0].attrib["ServiceName"]
                        package_list[package_name]["services"][service_id] = service[1].text
            except (AttributeError, IndexError, KeyError, TypeError):
                pass

        if _VERBOSE:
            log.info(f"Paquetes: {len(package_list)}")

        return package_list

    @staticmethod
    def __get_segments(xml):
        segment_list = {}
        root = ElTr.fromstring(xml.replace("\n", " "))
        payloads = root[0][1][1].findall("{urn:dvb:ipisdns:2006}DVBBINSTP")

        for segments in payloads:
            try:
                source = segments.attrib["Source"]
                segment_list[source] = {
                    "Source": source,
                    "Port": segments.attrib["Port"],
                    "Address": segments.attrib["Address"],
                    "Segments": {},
                }
                for segment in segments[0]:
                    segment_id = segment.attrib["ID"]
                    segment_list[source]["Segments"][segment_id] = segment.attrib["Version"]
            except (AttributeError, IndexError, KeyError, TypeError):
                pass

        if _VERBOSE:
            log.info("Días de EPG: %i" % len(segment_list))

        return segment_list

    @staticmethod
    def __get_service_provider_ip():
        if _VERBOSE:
            _demarcation = MulticastIPTV.get_demarcation_name()
            log.info("Buscando el Proveedor de Servicios de %s" % _demarcation)

        data = Cache.load_service_provider_data()
        if not data:
            xml = MulticastIPTV.get_xml_files(_CONFIG["mcast_grp"], _CONFIG["mcast_port"])["1_0"]
            result = re.findall(
                f'DEM_{_CONFIG["demarcation"]}' + r'\..*?Address="(.*?)".*?\s*Port="(.*?)".*?',
                xml,
                re.DOTALL,
            )[0]
            data = {"mcast_grp": result[0], "mcast_port": result[1]}
            Cache.save_service_provider_data(data)

        if _VERBOSE:
            log.info("Proveedor de Servicios de %s: %s" % (_demarcation, data["mcast_grp"]))

        return data

    @staticmethod
    def __merge_epg(epg, expired, fixed, new_epg, new_gaps):
        for channel in sorted(new_epg):
            if channel in epg:
                cached_epg = {}
                _new = sorted(new_epg[channel])
                new_start, new_end = _new[0], new_epg[channel][_new[-1]]["end"]
                for ts in sorted(epg[channel]):
                    if (
                        ts < new_start
                        or ts >= new_end
                        or any(filter(lambda gap: gap[0] <= ts < gap[1], new_gaps[channel]))
                    ):
                        cached_epg[ts] = epg[channel][ts]
                epg[channel] = {**cached_epg, **new_epg[channel]}
            else:
                epg[channel] = new_epg[channel]

        gaps = 0
        for channel in sorted(epg):
            sorted_channel = sorted(epg[channel])
            _fixed, _gaps = MulticastIPTV.fix_edges(epg, channel, sorted_channel, new_epg=new_epg)
            fixed += _fixed
            gaps += _gaps

        gaps_msg = f" _ Huecos = [{str(timedelta(seconds=gaps))}s]" if gaps else ""
        msg = f"Eventos en Caché: Arreglados = {fixed} _ Caducados = {expired}{gaps_msg}"
        log.info(msg)

    def __parse_bin_epg(self):
        if not self.__sv_ids:
            self.__fill_channel_ids()

        parsed_epg = {}
        for epg_day in self.__epg:
            channel_epg = {}
            for _id in [d for d in epg_day if epg_day[d]]:
                head = self.__parse_bin_epg_header(epg_day[_id])
                if head["service_id"] not in self.__sv_ids:
                    continue
                channel_epg[head["service_id"]] = self.__parse_bin_epg_body(head["data"], head["service_id"])
            self.merge_dicts(parsed_epg, channel_epg)
        self.__epg = parsed_epg
        Cache.save_epg(self.__epg)

    @staticmethod
    def __parse_bin_epg_body(data, service_id):
        programs = {}
        epg_dt = data[:-4]
        while epg_dt:
            start = struct.unpack(">I", epg_dt[4:8])[0]
            duration = struct.unpack(">H", epg_dt[8:10])[0]
            title_end = struct.unpack("B", epg_dt[31:32])[0] + 32
            genre = "{:02X}".format(struct.unpack("B", epg_dt[20:21])[0])
            episode = struct.unpack("B", epg_dt[title_end + 8 : title_end + 9])[0]
            season = struct.unpack("B", epg_dt[title_end + 11 : title_end + 12])[0]
            full_title = MulticastIPTV.decode_string(epg_dt[32:title_end])
            serie_id = struct.unpack(">H", epg_dt[title_end + 5 : title_end + 7])[0]
            meta_data = get_title_meta(full_title, serie_id, service_id, genre)
            programs[start] = {
                "pid": struct.unpack(">I", epg_dt[:4])[0],
                "start": start,
                "duration": duration,
                "end": start + duration,
                "genre": genre,
                "age_rating": struct.unpack("B", epg_dt[24:25])[0],
                "full_title": meta_data["full_title"],
                "serie_id": serie_id,
                "episode": meta_data["episode"] or episode,
                "year": str(struct.unpack(">H", epg_dt[title_end + 9 : title_end + 11])[0]),
                "serie": meta_data["serie"],
                "season": meta_data["season"] or season,
                "is_serie": meta_data["is_serie"],
            }
            if meta_data["episode_title"]:
                programs[start]["episode_title"] = meta_data["episode_title"]
            pr_title_end = struct.unpack("B", epg_dt[title_end + 12 : title_end + 13])[0] + title_end + 13
            cut = pr_title_end or title_end
            epg_dt = epg_dt[struct.unpack("B", epg_dt[cut + 3 : cut + 4])[0] + cut + 4 :]
        return programs

    @staticmethod
    def __parse_bin_epg_header(data):
        data = data.encode("latin1")
        body = struct.unpack("B", data[6:7])[0] + 7
        return {
            "size": struct.unpack(">H", data[1:3])[0],
            "service_id": struct.unpack(">H", data[3:5])[0],
            "service_version": struct.unpack("B", data[5:6])[0],
            "service_url": data[7:body],
            "data": data[body:],
        }

    def __sanitize_epg(self):
        sane_epg = {}
        _channels = self.__xml_data["channels"]
        for channel_key in self.__epg:
            assigned = False
            for channel_id in [ch for ch in _channels if _channels[ch].get("replacement", "") == channel_key]:
                sane_epg[channel_id] = self.__epg[channel_key]
                assigned = True
                break
            if not assigned:
                sane_epg[channel_key] = self.__epg[channel_key]
        self.__epg = sane_epg

    @staticmethod
    def decode_string(string):
        return unescape(("".join(chr(char ^ 0x15) for char in string)).encode("latin1").decode("utf8"))

    @staticmethod
    def fix_edges(epg, channel, sorted_channel, new_epg=None, new_gaps=None):
        drop = defaultdict(list)
        fixed = gaps = 0
        is_new_epg = new_gaps is not None
        name = "New EPG   " if is_new_epg else "Cached EPG"
        now = int(datetime.now().timestamp())
        chan = f"[{channel:4}] "

        for i in range(len(sorted_channel) - 1):
            ts = sorted_channel[i]
            _end = epg[channel][ts]["end"]
            _next = epg[channel][sorted_channel[i + 1]]["start"]
            __ts, __end, __next = map(time.ctime, (ts, _end, _next))

            if _end < _next:
                msg = f"{chan}{name} GAP     ->  END:[{__end}] [{_end}] - TO:[{__next}] [{_next}]"
                if is_new_epg:
                    log.debug(msg)
                    new_gaps[channel].append((_end, _next))
                elif ts < now:
                    if _next - ts < 1500:
                        epg[channel][ts]["end"] = _next
                        epg[channel][ts]["duration"] = _next - ts
                        log.warning(msg.replace(" GAP    ", " EXTEND "))
                        fixed += 1
                    else:
                        log.warning(msg)
                        gaps += _next - _end

            elif _end > _next:
                if ts < now:
                    epg[channel][ts]["end"] = _next
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

    @staticmethod
    def get_demarcation_name():
        return DEMARCATIONS.get(str(_CONFIG["demarcation"]), f'la demarcación {_CONFIG["demarcation"]}')

    async def get_epg(self):
        cached_epg = self.__clean_epg_channels(await Cache.load_epg())
        self.__get_bin_epg()
        self.__parse_bin_epg()
        self.__sanitize_epg()
        new_epg = self.__clean_epg_channels(self.__epg)

        if datetime.now().hour < 1:
            self.__expire_epg(new_epg)

        fixed, new_gaps = self.__fix_epg(new_epg)

        if not cached_epg:
            Cache.save_epg(new_epg)
            return new_epg, False

        expired = self.__expire_epg(cached_epg)
        self.__merge_epg(cached_epg, expired, fixed, new_epg, new_gaps)

        Cache.save_epg(cached_epg)
        return cached_epg, True

    def get_epg_day(self, mcast_grp, mcast_port, source):
        log.info(f'Descargando XML {source.split(".")[0]} -> {mcast_grp}:{mcast_port}')
        self.__epg[int(source.split("_")[1]) - 1] = self.get_xml_files(mcast_grp, mcast_port)

    def get_service_provider_data(self, refresh):
        data = Cache.load_epg_metadata(refresh)
        if data:
            self.__xml_data = data
        else:
            log.info("Actualizando metadatos de la EPG")
            connection = self.__get_service_provider_ip()
            self.__get_epg_data(connection["mcast_grp"], connection["mcast_port"])
        return self.__xml_data

    @staticmethod
    def get_xml_files(mc_grp, mc_port):
        _files = {}
        failed = 0
        first_file = ""
        loop = True

        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.settimeout(3)
            s.bind((mc_grp if not WIN32 else "", int(mc_port)))
            s.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                socket.inet_aton(mc_grp) + socket.inet_aton(_IPTV),
            )
            # Wait for an end chunk to start by the beginning
            while True:
                try:
                    chunk = MulticastIPTV.parse_chunk(s.recv(1500))
                    if chunk["end"]:
                        first_file = f'{chunk["filetype"]}_{chunk["fileid"]}'
                        break
                except (AttributeError, KeyError, TimeoutError, TypeError, UnicodeError):
                    pass
                except OSError:
                    if threading.current_thread() == threading.main_thread():
                        msg = "Multicast IPTV de Movistar no detectado"
                    else:
                        msg = "Imposible descargar XML"
                    log.error(msg)
                    failed += 1
                    if failed == 3:
                        raise ValueError(msg)
            # Loop until firstfile
            while loop:
                try:
                    xmldata = ""
                    chunk = MulticastIPTV.parse_chunk(s.recv(1500))
                    # Discard headers
                    body = chunk["data"]
                    while not chunk["end"]:
                        xmldata += body
                        chunk = MulticastIPTV.parse_chunk(s.recv(1500))
                        body = chunk["data"]
                    # Discard last 4bytes binary footer?
                    xmldata += body[:-4]
                    _files[f'{chunk["filetype"]}_{chunk["fileid"]}'] = xmldata
                    if f'{chunk["filetype"]}_{chunk["fileid"]}' == first_file:
                        loop = False
                        log.info(f"{mc_grp}:{mc_port} -> XML descargado")
                except (AttributeError, KeyError, TimeoutError, TypeError, UnicodeError):
                    pass
        return _files

    @staticmethod
    def merge_dicts(dict1, dict2, path=[]):
        for key in dict2:
            if key in dict1:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    MulticastIPTV.merge_dicts(dict1[key], dict2[key], path + [str(key)])
                elif dict1[key] == dict2[key]:
                    pass
                else:
                    raise ValueError("Conflicto en %s" % ".".join(path + [str(key)]))
            else:
                dict1[key] = dict2[key]
        return dict1

    @staticmethod
    def parse_chunk(data):
        return {
            "end": struct.unpack("B", data[:1])[0],
            "size": struct.unpack(">HB", data[1:4])[0],
            "filetype": struct.unpack("B", data[4:5])[0],
            "fileid": struct.unpack(">H", data[5:7])[0] & 0x0FFF,
            "chunk_number": struct.unpack(">H", data[8:10])[0] >> 4,
            "chunk_total": struct.unpack("B", data[10:11])[0],
            "data": data[12:].decode("latin1"),
        }


class XmlTV:
    def __init__(self, data):
        self.__channels = data["channels"]
        self.__packages = data["packages"]

    @staticmethod
    async def __build_programme_tag(channel_id, ts, program, tz_offset):
        local = "filename" in program

        if not local:
            ext_info = await MovistarTV.get_epg_extended_info(channel_id, program)
        else:
            filename = os.path.join(_conf["RECORDINGS"], program["filename"])
            ext_info = await get_local_info(channel_id, ts, filename, extended=True)
            if not ext_info:
                return
            ext_info["end"] = ts + program["duration"]  # Use the corrected duration to avoid overlaps
            program = ext_info

        dst_start = time.localtime(program["start"]).tm_isdst
        dst_stop = time.localtime(program["end"]).tm_isdst
        start = datetime.fromtimestamp(program["start"]).strftime("%Y%m%d%H%M%S")
        stop = datetime.fromtimestamp(program["end"]).strftime("%Y%m%d%H%M%S")

        tag_programme = Element(
            "programme",
            {
                "start": f"{start} +0{tz_offset + dst_start}00",
                "stop": f"{stop} +0{tz_offset + dst_stop}00",
                "channel": f"{channel_id}.movistar.tv",
            },
        )

        tag_title = SubElement(tag_programme, "title", LANG["es"])
        tag_desc = tag_stitle = None

        _match = False
        is_serie, title = program["is_serie"], program["full_title"]

        if is_serie:
            title = program["serie"]
            subtitle = program["full_title"].split(title, 1)[-1].strip("- ")

            if subtitle and subtitle not in title:
                tag_stitle = SubElement(tag_programme, "sub-title", LANG["es"])
                tag_stitle.text = subtitle

        tag_title.text = title

        if local:
            _match = daily_regex.match(tag_title.text)
            if _match:
                tag_title.text = _match.groups()[0]

        if ext_info:

            def _clean(string):
                return re.sub(r"[^a-z0-9]", "", string.lower())

            desc = ""
            year = ext_info.get("productionDate")
            orig_title = ext_info.get("originalTitle", "")
            orig_title = "" if orig_title.lower().lstrip().startswith("episod") else orig_title

            if orig_title:
                _clean_origt = _clean(orig_title)

                if is_serie:
                    if tag_stitle is not None and " - " in tag_stitle.text:
                        se, _stitle = tag_stitle.text.split(" - ", 1)
                        if _clean_origt.startswith(_clean(_stitle)):
                            tag_stitle.text = f"{se} - {orig_title}"
                else:
                    if ext_info["theme"] in ("Cine", "Documentales") and " (" in program["full_title"]:
                        _match = re.match(r"([^()]+) \((.+)\)", program["full_title"])
                        if _match and orig_title == _match.groups()[1]:
                            tag_title.text = _match.groups()[0]

                if ext_info["theme"] == "Cine":
                    tag_stitle = SubElement(tag_programme, "sub-title", LANG["es"])
                    tag_stitle.text = f"«{orig_title}»"

                elif ext_info["theme"] not in ("Deportes", "Música", "Programas"):
                    if _clean_origt != _clean(tag_title.text):
                        if tag_stitle is None or (_clean_origt not in _clean(tag_stitle.text)):
                            desc = f"«{orig_title}»"

            if all((local, tag_stitle is not None, ext_info.get("episodeName"))):
                if _clean(ext_info["episodeName"]) in _clean(tag_stitle.text):
                    if not tag_stitle.text.endswith(ext_info["episodeName"]):
                        if len(ext_info["episodeName"]) >= len(tag_stitle.text):
                            _match = series_regex.match(tag_stitle.text)
                            if _match:
                                tag_stitle.text = _match.groups()[0] + " - " + ext_info["episodeName"]
                            else:
                                tag_stitle.text = ext_info["episodeName"]

            if ext_info.get("description", "").strip():
                _desc = re.sub(r"\s*\n", r"\n\n", re.sub(r",\s*\n", ", ", ext_info["description"].strip()))
                tag_desc = SubElement(tag_programme, "desc", LANG["es"])
                tag_desc.text = f"{desc}\n\n" if desc else ""
                tag_desc.text += _desc

            if any((key in ext_info for key in ("directors", "mainActors", "producer"))):
                tag_credits = SubElement(tag_programme, "credits")
                for key in ("directors", "mainActors", "producer"):
                    if key in ext_info:
                        field = ext_info[key]
                        if isinstance(field, str):
                            field = field.split(", ")
                        elif isinstance(field, dict):  # -movistar.nfo (lists in xml)
                            field = list(field.values())[0]
                        key = key.lower()
                        for credit in field:
                            tag_credit = SubElement(tag_credits, re.sub(r"(?:main)?([^s]+)s?$", r"\1", key))
                            tag_credit.text = credit.strip()

            if year:
                tag_date = SubElement(tag_programme, "date")
                tag_date.text = year

            if ext_info.get("labelGenre"):
                tag_genrelabel = SubElement(tag_programme, "category", LANG["es"])
                tag_genrelabel.text = ext_info["labelGenre"]
            if ext_info.get("genre") and ext_info["genre"] != ext_info.get("labelGenre", ""):
                tag_genre = SubElement(tag_programme, "category", LANG["es"])
                tag_genre.text = ext_info["genre"]
            if ext_info.get("theme"):
                if ext_info["theme"] not in (ext_info.get("genre", ""), ext_info.get("labelGenre", "")):
                    tag_theme = SubElement(tag_programme, "category", LANG["es"])
                    tag_theme.text = ext_info["theme"]
                if ext_info["theme"] in THEME_MAP:
                    tag_category = SubElement(tag_programme, "category", LANG["en"])
                    tag_category.text = THEME_MAP[ext_info["theme"]]

            if ext_info["cover"]:
                src = f"{U7D_URL}/{'recording/?' if local else 'Covers/'}" + ext_info["cover"]
                SubElement(tag_programme, "icon", {"src": src})

            if is_serie and program["episode"]:
                tag_epnum = SubElement(tag_programme, "episode-num", {"system": "xmltv_ns"})
                tag_epnum.text = "%d.%d." % (max(0, program["season"] - 1), max(0, program["episode"] - 1))

        tag_rating = SubElement(tag_programme, "rating", {"system": "pl"})
        tag_value = SubElement(tag_rating, "value")
        tag_value.text = AGE_RATING[program["age_rating"]]

        return tag_programme

    def __get_client_services(self):
        services = {}
        for package in _CONFIG["tvPackages"].split("|") if _CONFIG["tvPackages"] != "ALL" else self.__packages:
            if package in self.__packages:
                services.update(self.__packages[package]["services"])
        return {int(k): int(v) for k, v in services.items()}

    @staticmethod
    def __write(file_path, content):
        with codecs.open(file_path, "w", "UTF-8") as f:
            f.write(content)

    async def generate_xml(self, parsed_epg, verbose, local=False):
        root = Element(
            "tv",
            {
                "date": datetime.now().strftime("%Y%m%d%H%M%S"),
                "source-info-name": "Movistar IPTV Spain",
                "source-info-url": "https://www.movistar.es/",
                "generator-info-name": "Movistar U7D's movistar_tvg",
                "generator-info-url": "https://github.com/jmarcet/movistar-u7d/",
            },
        )
        tz_offset = int(abs(time.timezone / 3600))
        services = self.__get_client_services()
        for channel_id in sorted(services, key=lambda key: services[key]):
            if channel_id in self.__channels and parsed_epg.get(channel_id):
                tag_channel = Element("channel", {"id": f"{channel_id}.movistar.tv"})
                tag_dname = SubElement(tag_channel, "display-name")
                tag_dname.text = self.__channels[channel_id]["name"].strip(" *")
                logo = f"{U7D_URL}/Logos/" + self.__channels[channel_id]["logo_uri"]
                SubElement(tag_channel, "icon", {"src": logo})
                root.append(tag_channel)

        for channel_id in [
            cid
            for cid in sorted(services, key=lambda key: services[key])
            if any((cid in self.__channels, local)) and cid in parsed_epg
        ]:
            channel_name = self.__channels[channel_id]["name"].strip(" *")
            if verbose and _VERBOSE:
                log.info(f'XML: "{channel_name}"')

            _tasks = [
                self.__build_programme_tag(channel_id, program, parsed_epg[channel_id][program], tz_offset)
                for program in sorted(parsed_epg[channel_id])
            ]
            [root.append(program) for program in (await asyncio.gather(*_tasks)) if program is not None]
        return ElementTree(root)

    def write_m3u(self, file_path, cloud=None, local=None):
        m3u = '#EXTM3U name="'
        m3u += "Cloud " if cloud else "Local " if local else ""
        m3u += 'MovistarTV" catchup="flussonic-ts" catchup-days="'
        m3u += '9999" ' if (cloud or local) else '8" '
        m3u += f'max-conn="12" refresh="1200" url-tvg="{U7D_URL}/'
        m3u += "cloud.xml" if cloud else "local.xml" if local else "guide.xml.gz"
        m3u += '"\n'

        services = self.__get_client_services()
        if any((cloud, local)):
            _services = {}
            for channel_id, channel in services.items():
                if channel_id in (cloud or local):
                    _services[channel_id] = channel
            services = _services

        channels = sorted(services, key=lambda key: services[key])
        if not any((cloud, local)):
            channels = channels[1:] + channels[:1]

        _fresh = not os.path.exists(file_path)
        _skipped = 0
        for channel_id in channels:
            if channel_id in self.__channels:
                channel_name = self.__channels[channel_id]["name"].strip(" *")
                if channel_id not in EPG_CHANNELS and not local:
                    msg = f'Saltando canal: [{channel_id:4}] "{channel_name}"'
                    log.info(msg) if _fresh else log.debug(msg)
                    _skipped += 1
                    continue
                channel_tag = "Cloud" if cloud else "Local" if local else "U7D"
                channel_tag += " - TDT Movistar.es"
                channel_number = services[channel_id]
                channel_number = 999 if channel_number == 0 else channel_number
                channel_logo = f"{U7D_URL}/Logos/" + self.__channels[channel_id]["logo_uri"]
                m3u += f'#EXTINF:-1 ch-number="{channel_number}" audio-track="2" '
                m3u += f'tvg-id="{channel_id}.movistar.tv" '
                m3u += f'group-title="{channel_tag}" '
                m3u += f'tvg-logo="{channel_logo}"'
                m3u += f",{channel_name}\n"
                m3u += f"{U7D_URL}"
                m3u += "/cloud" if cloud else "/local" if local else ""
                m3u += f"/{channel_id}/mpegts\n"
        if _skipped:
            log.info(f"Canales disponibles no indexados: {_skipped:2}")

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


async def tvg_main():
    global _CONFIG, _DEADLINE, _MIPTV, _SESSION, _VERBOSE, _XMLTV

    _DEADLINE = int(datetime.combine(date.today() - timedelta(days=7), datetime.min.time()).timestamp())

    # Obtiene los argumentos de entrada
    args = create_args_parser().parse_args()

    if (args.cloud_m3u or args.cloud_recordings) and (args.local_m3u or args.local_recordings):
        return
    if any((args.cloud_m3u, args.cloud_recordings, args.local_m3u, args.local_recordings)):
        _VERBOSE = False
    else:
        _VERBOSE = True
        banner = f"Movistar U7D - TVG v{_version}"
        log.info("-" * len(banner))
        log.info(banner)
        log.info("-" * len(banner))
        log.debug("%s" % " ".join(sys.argv[1:]))

    cached = False
    Cache(full=bool(args.m3u or args.guide))  # Inicializa la caché

    _SESSION = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(keepalive_timeout=YEAR_SECONDS),
        headers={"User-Agent": UA},
        json_serialize=json.dumps,
    )

    try:
        # Descarga la configuración del servicio Web de MovistarTV
        _CONFIG = await MovistarTV.get_service_config()

        # Busca el Proveedor de Servicios y descarga los archivos XML: canales, paquetes y segmentos
        _MIPTV = MulticastIPTV()
        xdata = _MIPTV.get_service_provider_data(refresh=bool(args.m3u or args.guide))

        # Crea el objeto XMLTV a partir de los datos descargados del Proveedor de Servicios
        _XMLTV = XmlTV(xdata)

        if args.m3u:
            _XMLTV.write_m3u(args.m3u)
            if not args.guide:
                return

        # Descarga los segments de cada EPG_X_BIN.imagenio.es y devuelve la guía decodificada
        if args.guide:
            epg, cached = await _MIPTV.get_epg()

        elif args.cloud_m3u or args.cloud_recordings or args.local_m3u or args.local_recordings:
            if args.cloud_m3u or args.cloud_recordings:
                epg = Cache.load_epg_cloud()
                if not epg:
                    log.error("No existe caché de grabaciones en la Nube. Debe generarla con movistar_epg")
            else:
                epg = Cache.load_epg_local()
            if not epg:
                return
            if args.cloud_m3u or args.local_m3u:
                _XMLTV.write_m3u(
                    args.cloud_m3u or args.local_m3u,
                    cloud=list(epg) if args.cloud_m3u else None,
                    local=list(epg) if args.local_m3u else None,
                )
            if not args.cloud_recordings and not args.local_recordings:
                return

        # Genera el árbol XMLTV de los paquetes contratados
        epg_x = await _XMLTV.generate_xml(epg, not cached, args.local_m3u or args.local_recordings)
        dom = minidom.parseString(ElTr.tostring(epg_x.getroot()))  # nosec B318

        if args.guide:
            with open(args.guide, "w", encoding="utf8") as f:
                dom.writexml(f, indent="    ", addindent="    ", newl="\n", encoding="UTF-8")
            with gzip.open(args.guide + ".gz", "wt", encoding="utf8") as f:
                dom.writexml(f, indent="    ", addindent="    ", newl="\n", encoding="UTF-8")

        elif args.cloud_recordings or args.local_recordings:
            with open(args.cloud_recordings or args.local_recordings, "w", encoding="utf8") as f:
                dom.writexml(f, indent="    ", addindent="    ", newl="\n", encoding="UTF-8")

        if not any((args.cloud_recordings, args.local_recordings)):
            _t = str(timedelta(seconds=round(time.time() - _time_start)))
            log.info(f"EPG de {len(epg)} canales y {len(xdata['segments'])} días generada en {_t}s")
            log.info(f"Fecha de caducidad: [{time.ctime(_DEADLINE)}] [{_DEADLINE}]")

    finally:
        await _SESSION.close()


if __name__ == "__main__":
    if not WIN32:
        from setproctitle import setproctitle

        setproctitle("movistar_tvg %s" % " ".join(sys.argv[1:]))

    else:
        import psutil
        import win32api  # pylint: disable=import-error
        import win32con  # pylint: disable=import-error

        def cancel_handler(event):
            log.debug("cancel_handler(event=%d)" % event)
            if event in (
                win32con.CTRL_BREAK_EVENT,
                win32con.CTRL_C_EVENT,
                win32con.CTRL_CLOSE_EVENT,
                win32con.CTRL_LOGOFF_EVENT,
                win32con.CTRL_SHUTDOWN_EVENT,
            ):
                psutil.Process().terminate()

        win32api.SetConsoleCtrlHandler(cancel_handler, 1)

    _time_start = time.time()

    _conf = mu7d_config()

    DEBUG = _conf["DEBUG"]

    _CONFIG = _DEADLINE = _IPTV = _MIPTV = _SESSION = _VERBOSE = _XMLTV = None

    CACHE_DIR = os.path.join(_conf["HOME"], ".xmltv", "cache")
    EPG_CHANNELS -= set(_conf["DROP_CHANNELS"])
    EPG_CHANNELS |= set(_conf["EXTRA_CHANNELS"])
    U7D_URL = _conf["U7D_URL"]

    AGE_RATING = ["0", "0", "0", "7", "12", "16", "17", "18"]
    LANG = {"es": {"lang": "es"}, "en": {"lang": "en"}}

    END_POINTS_FILE = "movistar_tvg.endpoints"

    daily_regex = re.compile(r"^(.+?) - \d{8}(?:_\d{4})?$")
    series_regex = re.compile(r"^(S\d+E\d+|Ep[isode]*\.)(?:.*)")

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)

    logging.basicConfig(datefmt=DATEFMT, format=FMT, level=_conf["DEBUG"] and logging.DEBUG or logging.INFO)
    if _conf["LOG_TO_FILE"]:
        add_logfile(log, _conf["LOG_TO_FILE"], _conf["DEBUG"] and logging.DEBUG or logging.INFO)

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_tvg.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            _IPTV = get_iptv_ip()
            asyncio.run(tvg_main())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
    except ValueError as err:
        log.critical(err)
        sys.exit(1)
