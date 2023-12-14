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

from aiohttp.resolver import AsyncResolver
from collections import defaultdict
from contextlib import closing
from datetime import date, datetime, timedelta
from filelock import FileLock, Timeout
from html import unescape
from queue import Queue
from xml.dom import minidom  # nosec B408
from xml.etree.ElementTree import Element, ElementTree, SubElement  # nosec B405

from mu7d import IPTV_DNS, UA, UA_U7D, WIN32, YEAR_SECONDS
from mu7d import get_iptv_ip, get_local_info, get_title_meta, mu7d_config, _version


log = logging.getLogger("TVG")


epg_channels = [
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
]

demarcations = {
    "Andalucia": 15,
    "Aragon": 34,
    "Asturias": 13,
    "Cantabria": 29,
    "Catalunya": 1,
    "Castilla la Mancha": 38,
    "Castilla y Leon": 4,
    "Comunidad Valenciana": 6,
    "Extremadura": 32,
    "Galicia": 24,
    "Islas Baleares": 10,
    "Islas Canarias": 37,
    "La Rioja": 31,
    "Madrid": 19,
    "Murcia": 12,
    "Navarra": 35,
    "Pais Vasco": 36,
}

end_points = {
    "epNoCach1": "http://www-60.svc.imagenio.telefonica.net:2001",
    "epNoCach2": "http://nc2.svc.imagenio.telefonica.net:2001",
    "epNoCach4": "http://nc4.svc.imagenio.telefonica.net:2001",
    "epNoCach5": "http://nc5.svc.imagenio.telefonica.net:2001",
    "epNoCach6": "http://nc6.svc.imagenio.telefonica.net:2001",
}

genre_map = {
    "0": {
        "0": "Arts / Culture (without music)",
        "1": "Popular culture / Traditional arts",
        "2": "Popular culture / Traditional arts",
        "3": "Arts magazines / Culture magazines",
        "4": "Fine arts",
        "5": "Fashion",
        "6": "Broadcasting / Press",
        "7": "Performing arts",
        "8": "Performing arts",
        "9": "Arts magazines / Culture magazines",
        "A": "New media",
        "B": "Popular culture / Traditional arts",
        "C": "Film / Cinema",
        "D": "Arts magazines / Culture magazines",
        "E": "Performing arts",
        "F": "Experimental film / Video",
    },
    "1": {
        "0": "Movie / Drama",
        "1": "Adventure / Western / War",
        "2": "Romance",
        "3": "Soap / Melodrama / Folkloric",
        "4": "Serious / Classical / Religious / Historical movie / Drama",
        "5": "Science fiction / Fantasy / Horror",
        "6": "Detective / Thriller",
        "7": "Comedy",
        "8": "Serious / Classical / Religious / Historical movie / Drama",
        "9": "Movie / drama",
        "A": "Adventure / Western / War",
        "B": "Movie / drama",
        "C": "Adult movie / Drama",
        "D": "Science fiction / Fantasy / Horror",
        "E": "Adult movie / Drama",
        "F": "Science fiction / Fantasy / Horror",
    },
    "2": {
        "0": "Social / Political issues / Economics",
        "1": "Magazines / Reports / Documentary",
        "2": "Economics / Social advisory",
        "3": "Social / Political issues / Economics",
        "4": "Social / Political issues / Economics",
        "5": "Social / Political issues / Economics",
        "6": "Social / Political issues / Economics",
        "7": "Social / Political issues / Economics",
        "8": "Social / Political issues / Economics",
        "9": "Social / Political issues / Economics",
        "A": "Social / Political issues / Economics",
        "B": "Social / Political issues / Economics",
        "C": "Social / Political issues / Economics",
        "D": "Social / Political issues / Economics",
        "E": "Social / Political issues / Economics",
        "F": "Social / Political issues / Economics",
    },
    "4": {
        "0": "Sports",
        "1": "Motor sport",
        "2": "Team sports (excluding football)",
        "3": "Water sport",
        "4": "Team sports (excluding football)",
        "5": "Team sports (excluding football)",
        "6": "Martial sports",
        "7": "Football / Soccer",
        "8": "Water sport",
        "9": "Team sports (excluding football)",
        "A": "Athletics",
        "B": "Sports",
        "C": "Motor sport",
        "D": "Sports",
        "E": "Sports",
        "F": "Tennis / Squash",
    },
    "5": {
        "0": "Children's / Youth programs",
        "1": "Entertainment programs for 10 to 16",
        "2": "Pre-school children's programs",
        "3": "Entertainment programs for 6 to 14",
        "4": "Children's / Youth programs",
        "5": "Informational / Educational / School programs",
        "6": "Entertainment programs for 6 to 14",
        "7": "Children's / Youth programs",
        "8": "Children's / Youth programs",
        "9": "Children's / Youth programs",
        "A": "Children's / Youth programs",
        "B": "Children's / Youth programs",
        "C": "Children's / Youth programs",
        "D": "Children's / Youth programs",
        "E": "Children's / Youth programs",
        "F": "Children's / Youth programs",
    },
    "6": {
        "0": "Music / Ballet / Dance",
        "1": "Musical / Opera",
        "2": "Serious music / Classical music",
        "3": "Rock / Pop",
        "4": "Music / Ballet / Dance",
        "5": "Music / Ballet / Dance",
        "6": "Music / Ballet / Dance",
        "7": "Musical / Opera",
        "8": "Ballet",
        "9": "Jazz",
        "A": "Music / Ballet / Dance",
        "B": "Rock / Pop",
        "C": "Music / Ballet / Dance",
        "D": "Music / Ballet / Dance",
        "E": "Music / Ballet / Dance",
        "F": "Music / Ballet / Dance",
    },
    "7": {
        "0": "Show / Game show",
        "1": "Variety show",
        "2": "Variety show",
        "3": "Variety show",
        "4": "Talk show",
        "5": "Variety show",
        "6": "Variety show",
        "7": "Variety show",
        "8": "Variety show",
        "9": "Variety show",
        "A": "Variety show",
        "B": "Show / Game show",
        "C": "Talk show",
        "D": "Show / Game show",
        "E": "Show / Game show",
        "F": "Show / Game show",
    },
    "8": {
        "0": "Education / Science / Factual topics",
        "1": "Further education",
        "2": "Social / Spiritual sciences",
        "3": "Medicine / Physiology / Psychology",
        "4": "Social / Spiritual sciences",
        "5": "Technology / Natural sciences",
        "6": "Social / Spiritual sciences",
        "7": "Education / Science / Factual topics",
        "8": "Further education",
        "9": "Nature / Animals / Environment",
        "A": "Foreign countries / Expeditions",
        "B": "Further education",
        "C": "Social / Spiritual sciences",
        "D": "Further education",
        "E": "Education / Science / Factual topics",
        "F": "Education / Science / Factual topics",
    },
    "9": {
        "0": "Movie / Drama",
        "1": "Adult movie / Drama",
        "2": "Adult movie / Drama",
        "3": "Adult movie / Drama",
        "4": "Adult movie / Drama",
        "5": "Adult movie / Drama",
        "6": "Adult movie / Drama",
        "7": "Adult movie / Drama",
        "8": "Adult movie / Drama",
        "9": "Adult movie / Drama",
        "A": "Adult movie / Drama",
        "B": "Adult movie / Drama",
        "C": "Adult movie / Drama",
        "D": "Adult movie / Drama",
        "E": "Adult movie / Drama",
        "F": "Adult movie / Drama",
    },
}


class MulticastEPGFetcher(threading.Thread):
    def __init__(self, queue, exc_queue):
        threading.Thread.__init__(self, daemon=True)
        self.queue = queue
        self.exc_queue = exc_queue

    def run(self):
        while True:
            mcast = self.queue.get()
            try:
                iptv.get_day(mcast["mcast_grp"], mcast["mcast_port"], mcast["source"])
            except Exception as ex:
                self.exc_queue.put(ex)
            self.queue.task_done()


class Cache:
    def __init__(self):
        self.__programs = {}
        self.__end_points = None
        self.__check_dirs()
        self.__clean()

    @staticmethod
    def __check_dirs():
        cache_path = os.path.join(app_dir, "cache")
        progs_path = os.path.join(cache_path, "programs")
        if not os.path.exists(app_dir):
            os.mkdir(app_dir)
        if not os.path.exists(cache_path):
            log.info(f"Creando caché en {cache_path}")
            os.mkdir(cache_path)
        if not os.path.exists(progs_path):
            os.mkdir(progs_path)

    @staticmethod
    def __clean():
        for file in glob.glob(os.path.join(cache_dir, "programs", "*.json")):
            try:
                with open(file, encoding="utf8") as f:
                    _data = json.load(f)["data"]
                _exp_date = int(_data["endTime"] / 1000)
                if _exp_date < deadline:
                    os.remove(file)
            except (IOError, KeyError, ValueError):
                pass

    @staticmethod
    def __load(cfile):
        try:
            with open(os.path.join(cache_dir, cfile), "r", encoding="utf8") as f:
                return json.load(f)["data"]
        except (FileNotFoundError, IOError, KeyError, ValueError):
            pass

    @staticmethod
    def __save(cfile, data):
        data = json.loads(unescape(json.dumps(data)))
        with open(os.path.join(cache_dir, cfile), "w", encoding="utf8") as f:
            try:
                json.dump({"data": data}, f, ensure_ascii=False, indent=4, sort_keys=True)
            except AttributeError:
                json.dump({"data": data}, f, ensure_ascii=False, sort_keys=True)

    def load_cloud_epg(self):
        data = self.__load("cloud.json")
        cloud_epg = defaultdict(dict)
        for channel in data:
            cloud_epg[int(channel)] = {int(k): v for k, v in data[channel].items()}
        return cloud_epg

    def load_config(self):
        return self.__load("config.json")

    def load_cookie(self):
        return self.__load(cookie_file)

    def load_end_points(self):
        if not self.__end_points:
            self.__end_points = self.__load(end_points_file)
        return self.__end_points or end_points

    async def load_epg(self):
        data = self.__load("epg.json")
        now = datetime.now()
        remote_cache_ts = int(now.replace(hour=0, minute=0, second=0).timestamp())
        if now.hour == 0 and now.minute < 59:
            remote_cache_ts -= 3600 * 24
        if not data or int(os.path.getmtime(os.path.join(cache_dir, "epg.json"))) < remote_cache_ts:
            async with aiohttp.ClientSession(headers={"User-Agent": UA_U7D}) as session:
                for i in range(5):
                    log.info("Intentando obtener Caché de EPG actualizada...")
                    try:
                        async with session.get("https://openwrt.marcet.info/epg.json") as r:
                            if r.status == 200:
                                res = await r.json()
                                log.info("Obtenida Caché de EPG actualizada")
                                data = res["data"]
                                break
                            if i < 4:
                                log.warning(f"No ha habido suerte. Reintentando en 15s... [{i + 2} / 5]")
                                await asyncio.sleep(15)
                    except Exception as ex:
                        if i < 4:
                            log.warning(
                                f"No ha habido suerte. Reintentando en 15s... [{i + 2} / 5] => {repr(ex)}"
                            )
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

    def load_epg_metadata(self):
        metadata = self.__load("epg_metadata.json")
        if not metadata:
            return

        now = datetime.now()
        update_time = int(now.replace(hour=0, minute=0, second=0).timestamp())
        log.debug(f"Fecha de actualización de epg_metadata: [{time.ctime(update_time)}] [{update_time}]")
        if int(os.path.getmtime(os.path.join(app_dir, "cache", "epg_metadata.json"))) < update_time:
            log.info("Actualizando metadatos de la EPG")
            return

        channels_data = defaultdict(dict)
        for channel in metadata["channels"]:
            channels_data[int(channel)] = metadata["channels"][channel]
        metadata.update({"channels": channels_data})
        return metadata

    def load_epg_extended_info(self, pid):
        if pid in self.__programs:
            return self.__programs[pid]
        return self.__load(os.path.join("programs", f"{pid}.json"))

    def load_local_epg(self):
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

    def load_service_provider_data(self):
        return self.__load("provider.json")

    def save_config(self, data):
        self.__save("config.json", data)

    def save_cookie(self, data):
        self.__save(cookie_file, data)

    def save_end_points(self, data):
        log.info(f"Nuevos End Points: {sorted(data.keys())}")
        self.__end_points = data
        self.__save(end_points_file, data)

    def save_epg(self, data):
        self.__save("epg.json", data)

    def save_epg_extended_info(self, data):
        self.__programs[data["productID"]] = json.loads(unescape(json.dumps(data)))
        self.__save(os.path.join("programs", f'{data["productID"]}.json'), data)

    def save_epg_metadata(self, data):
        self.__save("epg_metadata.json", data)

    def save_service_provider_data(self, data):
        self.__save("provider.json", data)


class MovistarTV:
    def __init__(self):
        self.__cookie = cache.load_cookie()
        self.__end_points_down = []
        self.__web_service_down = False

    @staticmethod
    def __get_end_points():
        try:
            return config["end_points"]
        except TypeError:
            return cache.load_end_points()

    async def __get_genres(self, tv_wholesaler):
        log.info("Descargando mapa de géneros")
        return await self.__get_service_data(f"getEpgSubGenres&tvWholesaler={tv_wholesaler}")

    async def __get_service_data(self, action):
        if self.__web_service_down:
            return
        ep = self.get_end_point()
        if not ep:
            log.error("Servicio Web de Movistar TV caído: decargando guía básica")
            self.__web_service_down = True
            return
        try:
            headers = {}
            if self.__cookie:
                for ck in self.__cookie.split("; "):
                    if "=" in ck:
                        headers["Cookie"] = self.__cookie
            url = f"{ep}/appserver/mvtv.do?action={action}"
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    content = (await response.json())["resultData"]
                    new_cookie = response.headers.get("set-cookie")
                    if new_cookie and not self.__cookie:
                        self.__cookie = new_cookie
                        cache.save_cookie(self.__cookie)
                    elif new_cookie and new_cookie != self.__cookie:
                        cache.save_cookie(new_cookie)
                    return content
        except Exception as ex:
            log.warning(f"Timeout: {ep} => {repr(ex)}")

    @staticmethod
    def __update_end_points(data):
        cache.save_end_points(data)
        return data

    def get_end_point(self):
        eps = self.__get_end_points()
        for ep in sorted(eps):
            if eps[ep] not in self.__end_points_down:
                return eps[ep]

    async def get_epg_extended_info(self, channel_id, pid, ts):
        data = cache.load_epg_extended_info(pid)
        mismatch = False
        if not data or data["beginTime"] // 1000 != ts:
            if data:
                mismatch = True
            data = await self.__get_service_data(f"epgInfov2&productID={pid}&channelID={channel_id}&extra=1")
            if not data:
                log.debug("Información extendida no encontrada: [%04d] [%d] [%d] " % (channel_id, pid, ts))
                return
            if mismatch and data["beginTime"] // 1000 != ts:
                log.debug(
                    "Event mismatch STILL BROKEN: [%4s] [%d] beginTime=[%+d]"
                    % (str(channel_id), pid, data["beginTime"] // 1000 - ts)
                )
                return
            cache.save_epg_extended_info(data)
        return data

    def get_first_end_point(self):
        eps = self.__get_end_points()
        return sorted(eps)[0]

    def get_random_end_point(self):
        eps = self.__get_end_points()
        return eps[random.choice(eps.keys())]  # nosec B311

    async def get_service_config(self):
        cfg = cache.load_config()
        if cfg:
            if VERBOSE:
                log.info("tvPackages: %s" % cfg["tvPackages"])
                log.info("Demarcation: %s" % cfg["demarcation"])
            return cfg
        log.info("Descargando configuración del cliente, parámetros de configuración y perfil del servicio")
        client, params, platform = await asyncio.gather(
            self.__get_service_data("getClientProfile"),
            self.__get_service_data("getConfigurationParams"),
            self.__get_service_data("getPlatformProfile"),
        )
        if not client or not params or not platform:
            raise ValueError("IPTV de Movistar no detectado")
        dvb_entry_point = platform["dvbConfig"]["dvbipiEntryPoint"].split(":")
        uri = platform[list(filter(lambda f: re.search("base.*uri", f, re.IGNORECASE), platform.keys()))[0]]
        if VERBOSE:
            log.info("tvPackages: %s" % client["tvPackages"])
            log.info("Demarcation: %s" % client["demarcation"])
        conf = {
            "tvPackages": client["tvPackages"],
            "demarcation": client["demarcation"],
            "tvWholesaler": client["tvWholesaler"],
            "end_points": self.__update_end_points(platform["endPoints"]),
            "mcast_grp": dvb_entry_point[0],
            "mcast_port": int(dvb_entry_point[1]),
            "tvChannelLogoPath": "%s%s" % (uri, params["tvChannelLogoPath"]),
            "tvCoversPath": "%s%s%s290x429/" % (uri, params["tvCoversPath"], params["portraitSubPath"]),
            "tvCoversLandscapePath": "%s%s%s%s"
            % (uri, params["tvCoversPath"], params["landscapeSubPath"], params["bigSubpath"]),
            "genres": await self.__get_genres(client["tvWholesaler"]),
        }
        cache.save_config(conf)
        return conf


class MulticastIPTV:
    def __init__(self):
        self.__xml_data = {}
        self.__epg = None

    def __clean_epg_channels(self, epg):
        services = {}
        _packages = self.__xml_data["packages"]
        for package in config["tvPackages"].split("|") if config["tvPackages"] != "ALL" else _packages:
            if package in _packages:
                services.update(_packages[package]["services"])
        subscribed = [int(k) for k in services]

        clean_epg = {}
        for channel in set(epg) & set(epg_channels) & set(subscribed):
            clean_epg[channel] = epg[channel]
        return clean_epg

    def __decode_string(self, string):
        _t = ("".join(chr(char ^ 0x15) for char in string)).encode("latin1").decode("utf8")
        return unescape(_t)

    def __expire_epg(self, epg):
        expired = 0
        for channel in epg:
            _expired = tuple(filter(lambda ts: epg[channel][ts]["end"] < deadline, epg[channel]))
            tuple(map(epg[channel].pop, _expired))
            expired += len(_expired)
        return expired

    def __fix_edges(self, epg, channel, sorted_channel, new_epg=None, new_gaps=None):
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

    def __fix_epg(self, epg):
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

            fixed_over += self.__fix_edges(epg, channel, _new, new_gaps=new_gaps)[0]

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
        queue, exc_queue = Queue(), Queue()
        threads = tvg_threads
        self.__epg = [{} for r in range(len(self.__xml_data["segments"]))]
        log.info(f"Multithread: {threads} descargas simultáneas")
        for _ in range(threads):
            process = MulticastEPGFetcher(queue, exc_queue)
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
        if exc_queue.qsize():
            raise exc_queue.get()

    @staticmethod
    def __get_channels(xml_channels):
        root = ElTr.fromstring(xml_channels.replace("\n", " "))
        services = root[0][0].findall("{urn:dvb:ipisdns:2006}SingleService")
        channel_list = {}
        for i in services:
            channel_id = "unknown"
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
            except (KeyError, IndexError):
                pass
        if VERBOSE:
            log.info("Canales: %i" % len(channel_list))
        return channel_list

    @staticmethod
    def __get_demarcation_name():
        for demarcation in demarcations:
            if demarcations[demarcation] == config["demarcation"]:
                return demarcation
        return config["demarcation"]

    def __get_epg_data(self, mcast_grp, mcast_port):
        while True:
            xml = self.__get_xml_files(mcast_grp, mcast_port)
            _msg = "["
            _msg += "2_0 " if "2_0" in xml else "    "
            _msg += "5_0 " if "5_0" in xml else "    "
            _msg += "6_0" if "6_0" in xml else "   "
            _msg += "] / [2_0 5_0 6_0]"
            if "2_0" in xml and "5_0" in xml and "6_0" in xml:
                if VERBOSE:
                    log.info(f"Ficheros XML descargados: {_msg}")
                break
            log.warning(f"Ficheros XML incompletos: {_msg}")
            time.sleep(10)
        if VERBOSE:
            log.info("Descargando canales y paquetes")
        try:
            self.__xml_data["channels"] = self.__get_channels(xml["2_0"])
            self.__xml_data["packages"] = self.__get_packages(xml["5_0"])
            if VERBOSE:
                log.info("Descargando índices")
            self.__xml_data["segments"] = self.__get_segments(xml["6_0"])
        except Exception as ex:
            log.debug(f"{repr(ex)}")
            log.warning("Error descargando datos de la EPG. Reintentando...")
            return self.__get_epg_data(mcast_grp, mcast_port)
        cache.save_epg_metadata(self.__xml_data)

    @staticmethod
    def __get_packages(xml):
        root = ElTr.fromstring(xml.replace("\n", " "))
        packages = root[0].findall("{urn:dvb:ipisdns:2006}Package")
        package_list = {}
        for package in packages:
            package_name = "unknown"
            try:
                package_name = package[0].text
                package_list[package_name] = {
                    "id": package.attrib["Id"],
                    "name": package_name,
                    "services": {},
                }
                for service in package:
                    if not service.tag == "{urn:dvb:ipisdns:2006}PackageName":
                        service_id = service[0].attrib["ServiceName"]
                        package_list[package_name]["services"][service_id] = service[1].text
            except (AttributeError, IndexError, KeyError):
                log.debug(f"El paquete {package_name} no tiene la estructura correcta")
        if VERBOSE:
            log.info(f"Paquetes: {len(package_list)}")
        return package_list

    def __get_sane_epg(self, epg):
        sane_epg = {}
        for channel_key in epg:
            _channels = self.__xml_data["channels"]
            assigned = False
            for channel_id in _channels:
                if (
                    "replacement" in _channels[channel_id]
                    and _channels[channel_id]["replacement"] == channel_key
                ):
                    sane_epg[channel_id] = epg[channel_key]
                    assigned = True
                    break
            if not assigned:
                sane_epg[channel_key] = epg[channel_key]
        return sane_epg

    @staticmethod
    def __get_segments(xml):
        root = ElTr.fromstring(xml.replace("\n", " "))
        payloads = root[0][1][1].findall("{urn:dvb:ipisdns:2006}DVBBINSTP")
        segment_list = {}
        for segments in payloads:
            source = "unknown"
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
            except KeyError:
                log.debug(f"El segmento {source} no tiene la estructura correcta")
        if VERBOSE:
            log.info("Días de EPG: %i" % len(segment_list))
        return segment_list

    def __get_service_provider_ip(self):
        if VERBOSE:
            log.info("Buscando el Proveedor de Servicios de %s" % self.__get_demarcation_name())
        data = cache.load_service_provider_data()
        if not data:
            xml = self.__get_xml_files(config["mcast_grp"], config["mcast_port"])["1_0"]
            result = re.findall(
                "DEM_" + str(config["demarcation"]) + r'\..*?Address="(.*?)".*?\s*Port="(.*?)".*?',
                xml,
                re.DOTALL,
            )[0]
            data = {"mcast_grp": result[0], "mcast_port": result[1]}
            cache.save_service_provider_data(data)
        if VERBOSE:
            log.info("Proveedor de Servicios de %s: %s" % (self.__get_demarcation_name(), data["mcast_grp"]))
        return data

    def __get_xml_files(self, mc_grp, mc_port):
        loop = True
        max_files = 1000
        _files = {}
        failed = 0
        first_file = ""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.settimeout(3)
            s.bind((mc_grp if not WIN32 else "", int(mc_port)))
            s.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                socket.inet_aton(mc_grp) + socket.inet_aton(_iptv),
            )
            # Wait for an end chunk to start by the beginning
            while True:
                try:
                    chunk = self.__parse_chunk(s.recv(1500))
                    if chunk["end"]:
                        first_file = str(chunk["filetype"]) + "_" + str(chunk["fileid"])
                        break
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
                    chunk = self.__parse_chunk(s.recv(1500))
                    # Discard headers
                    body = chunk["data"]
                    while not chunk["end"]:
                        xmldata += body
                        chunk = self.__parse_chunk(s.recv(1500))
                        body = chunk["data"]
                    # Discard last 4bytes binary footer?
                    xmldata += body[:-4]
                    _files[str(chunk["filetype"]) + "_" + str(chunk["fileid"])] = xmldata
                    # log.debug("XML: %s_%s" % (chunk["filetype"], chunk["fileid"]))
                    max_files -= 1
                    if str(chunk["filetype"]) + "_" + str(chunk["fileid"]) == first_file or max_files == 0:
                        if VERBOSE:
                            log.info(f"{mc_grp}:{mc_port} -> XML descargado")
                        loop = False
                except Exception as ex:
                    log.debug(f"Error al descargar los archivos XML: {repr(ex)}")
        return _files

    def __merge_dicts(self, dict1, dict2, path=[]):
        for key in dict2:
            if key in dict1:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    self.__merge_dicts(dict1[key], dict2[key], path + [str(key)])
                elif dict1[key] == dict2[key]:
                    pass
                else:
                    raise ValueError("Conflicto en %s" % ".".join(path + [str(key)]))
            else:
                dict1[key] = dict2[key]
        return dict1

    def __merge_epg(self, epg, expired, fixed, new_epg, new_gaps):
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
            _fixed, _gaps = self.__fix_edges(epg, channel, sorted_channel, new_epg=new_epg)
            fixed += _fixed
            gaps += _gaps

        gaps_msg = f" _ Huecos = [{str(timedelta(seconds=gaps))}s]" if gaps else ""
        msg = f"Eventos en Caché: Arreglados = {fixed} _ Caducados = {expired}{gaps_msg}"
        log.info(msg)

    def __parse_bin_epg(self):
        merged_epg = {}
        for epg_day in self.__epg:
            programs = {}
            for ch_id in epg_day:
                if epg_day[ch_id] and "replacement" not in epg_day[ch_id]:
                    head = self.__parse_bin_epg_header(epg_day[ch_id])
                    programs[head["service_id"]] = self.__parse_bin_epg_body(head["data"])
            self.__merge_dicts(merged_epg, programs)
        log.info(f"Canales con EPG: {len(merged_epg)}")
        cache.save_epg(merged_epg)
        return merged_epg

    def __parse_bin_epg_body(self, data):
        epg_dt = data[:-4]
        programs = {}
        while epg_dt:
            start = struct.unpack(">I", epg_dt[4:8])[0]
            duration = struct.unpack(">H", epg_dt[8:10])[0]
            title_end = struct.unpack("B", epg_dt[31:32])[0] + 32
            episode = struct.unpack("B", epg_dt[title_end + 8 : title_end + 9])[0]
            season = struct.unpack("B", epg_dt[title_end + 11 : title_end + 12])[0]
            full_title = self.__decode_string(epg_dt[32:title_end])
            serie_id = struct.unpack(">H", epg_dt[title_end + 5 : title_end + 7])[0]
            meta_data = get_title_meta(full_title, serie_id)
            programs[start] = {
                "pid": struct.unpack(">I", epg_dt[:4])[0],
                "start": start,
                "duration": duration,
                "end": start + duration,
                "genre": "{:02X}".format(struct.unpack("B", epg_dt[20:21])[0]),
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

    @staticmethod
    def __parse_chunk(data):
        try:
            chunk = {
                "end": struct.unpack("B", data[:1])[0],
                "size": struct.unpack(">HB", data[1:4])[0],
                "filetype": struct.unpack("B", data[4:5])[0],
                "fileid": struct.unpack(">H", data[5:7])[0] & 0x0FFF,
                "chunk_number": struct.unpack(">H", data[8:10])[0] >> 4,
                "chunk_total": struct.unpack("B", data[10:11])[0],
                "data": data[12:].decode("latin1"),
            }
            return chunk
        except Exception as ex:
            raise ValueError(f"get_chunk: error al analizar los datos {repr(ex)}")

    def get_cloud_epg(self):
        return self.__clean_epg_channels(cache.load_cloud_epg())

    def get_day(self, mcast_grp, mcast_port, source):
        day = int(source.split("_")[1]) - 1
        log.info("Descargando XML " + source.split(".")[0] + f" -> {mcast_grp}:{mcast_port}")
        self.__epg[day] = self.__get_xml_files(mcast_grp, mcast_port)

    async def get_epg(self):
        cached_epg = self.__clean_epg_channels(await cache.load_epg())
        self.__get_bin_epg()
        try:
            new_epg = self.__clean_epg_channels(self.__get_sane_epg(self.__parse_bin_epg()))
            log.info(f"Conservando {len(new_epg)} canales en abierto")
        except Exception as ex:
            log.debug(f"{repr(ex)}")
            log.warning("Error descargando la EPG. Reintentando...")
            return await self.get_epg()

        if datetime.now().hour < 2:
            self.__expire_epg(new_epg)

        fixed, new_gaps = self.__fix_epg(new_epg)

        if not cached_epg:
            cache.save_epg(new_epg)
            return new_epg, False

        expired = self.__expire_epg(cached_epg)
        self.__merge_epg(cached_epg, expired, fixed, new_epg, new_gaps)

        cache.save_epg(cached_epg)
        return cached_epg, True

    def get_service_provider_data(self):
        data = cache.load_epg_metadata()
        if data:
            self.__xml_data = data
        else:
            connection = self.__get_service_provider_ip()
            self.__get_epg_data(connection["mcast_grp"], connection["mcast_port"])
        return self.__xml_data


class XMLTV:
    def __init__(self, data):
        self.__channels = data["channels"]
        self.__packages = data["packages"]

    async def __build_programme_tag(self, channel_id, ts, program, tz_offset):
        local = "filename" in program

        if not local:
            ext_info = await mtv.get_epg_extended_info(channel_id, program["pid"], program["start"])
        else:
            filename = os.path.join(_conf["RECORDINGS"], program["filename"])
            ext_info = await get_local_info(channel_id, ts, filename, extended=True)
            if not ext_info:
                log.error(f"Metadatos no encontrados: {program=}")
                return
            # Use the corrected duration to avoid overlaps
            ext_info["end"] = ts + program["duration"]
            program = ext_info

        dst_start = time.localtime(program["start"]).tm_isdst
        dst_stop = time.localtime(program["end"]).tm_isdst
        start = datetime.fromtimestamp(program["start"]).strftime("%Y%m%d%H%M%S")
        stop = datetime.fromtimestamp(program["end"]).strftime("%Y%m%d%H%M%S")
        year = program["year"] or ext_info.get("productionDate", "") if ext_info else ""

        tag_programme = Element(
            "programme",
            {
                "channel": f"{channel_id}.movistar.tv",
                "start": f"{start} +0{tz_offset + dst_start}00",
                "stop": f"{stop} +0{tz_offset + dst_stop}00",
            },
        )

        if year:
            tag_date = SubElement(tag_programme, "date")
            tag_date.text = year

        tag_title = SubElement(tag_programme, "title", lang["es"])

        _match = False
        if ext_info and ext_info["theme"] == "Cine" or program["genre"].startswith("1"):
            is_serie = False
        elif ext_info and "seriesName" in ext_info and "serie" in program and ": " in ext_info["seriesName"]:
            _match = series_regex.match(program["full_title"])
            is_serie = True
            if "episode_title" in program:
                del program["episode_title"]
            program["serie"] = ext_info["seriesName"]
        else:
            _match = series_regex.match(program["full_title"])
            is_serie = _match or program["is_serie"] or ": " in program["full_title"]
        if is_serie:
            title, subtitle = _match.groups() if _match else (program["full_title"], None)
            stitle = program.get("episode_title", "")
            stitle = "" if stitle.startswith("Episod") else stitle

            if not subtitle and not stitle:
                if ": " in program["full_title"]:
                    title, subtitle = program["full_title"].split(": ", 1)
                elif program["season"] is not None and program["episode"]:
                    subtitle = f'S{program["season"]:02}E{program["episode"]:02}'
                elif program["episode"]:
                    subtitle = f'Ep. {program["episode"]}'
            elif "episode" in program and str(program["episode"]) not in stitle:
                stitle = f'Ep. {program["episode"]} - {stitle}'

            tag_title.text = title or program["serie"] or program["full_title"]
            tag_stitle = SubElement(tag_programme, "sub-title", lang["es"])
            tag_stitle.text = subtitle or stitle
        else:
            tag_title.text = program["full_title"]

        if local:
            _match = daily_regex.match(tag_title.text)
            if _match:
                tag_title.text = _match.groups()[0]

        if ext_info:
            tag_desc = SubElement(tag_programme, "desc", lang["es"])
            tag_desc.text = ""
            orig_title = ext_info.get("originalTitle", "")
            orig_title = "" if "Episod" in orig_title or orig_title == "Cine" else orig_title
            _clean_title = program["full_title"].lower().replace("(", "").replace(")", "")

            if orig_title:
                if ext_info["theme"] in ("Cine", "Documentales") and not is_serie:
                    tag_stitle = SubElement(tag_programme, "sub-title", lang["es"])
                    tag_stitle.text = f"«{orig_title}»"

                    _match = cinema_regex.match(program["full_title"])
                    if _match and orig_title and orig_title in _match.groups():
                        tag_title.text = " ".join([x for x in _match.groups() if x and x != orig_title])

                elif orig_title.lower() not in _clean_title and _clean_title not in orig_title.lower():
                    tag_desc.text += f"«{orig_title}»"

            if any((key in ext_info for key in ("mainActors", "producer", "producers", "directors"))):
                tag_desc.text += (" " if tag_desc.text else "") + (f"Año: {year}. " if year else "")
                for key in ("mainActors", "producer", "producers", "directors"):
                    if key in ext_info:
                        field = ext_info[key]
                        if key != "producer":
                            if isinstance(field, list):
                                field = [item.strip() for item in field]
                            else:
                                field = [list(field.values())[0]] if isinstance(field, dict) else [field]
                                field = field[0] if isinstance(field[0], list) else field
                        else:
                            field = field.split(", ")
                        tag_desc.text += ", ".join(field) + ". "
                tag_desc.text += "\n"

            tag_desc.text += ("\n" if tag_desc.text else "") + ext_info["description"]

            if ext_info["cover"]:
                src = f"{u7d_url}/{'recording/?' if local else 'Covers/'}" + ext_info["cover"]
                SubElement(tag_programme, "icon", {"src": src})

        gens = self.__get_genre_and_subgenre(program["genre"]) if not local else program["gens"]
        keys = self.__get_key_and_subkey(program["genre"], config["genres"]) if not local else None
        tag_rating = SubElement(tag_programme, "rating", {"system": "pl"})
        tag_value = SubElement(tag_rating, "value")
        tag_value.text = age_rating[program["age_rating"]]
        tag_category = SubElement(tag_programme, "category", lang["en"])
        tag_category.text = gens["genre"]
        if gens["sub-genre"]:
            tag_subcategory = SubElement(tag_programme, "category", {"lang": "en"})
            tag_subcategory.text = gens["sub-genre"]
        if keys:
            tag_keyword = SubElement(tag_programme, "keyword")
            tag_keyword.text = keys["key"]
            if keys["sub-key"]:
                tag_subkeyword = SubElement(tag_programme, "keyword")
                tag_subkeyword.text = keys["sub-key"]

        return tag_programme

    def __generate_m3u(self, file_path, cloud=None, local=None):
        m3u = '#EXTM3U name="'
        m3u += "Cloud " if cloud else "Local " if local else ""
        m3u += 'MovistarTV" catchup="flussonic-ts" catchup-days="'
        m3u += '9999" ' if (cloud or local) else '8" '
        m3u += f'dlna_extras=mpeg_ps_pal max-conn="12" refresh="1200" url-tvg="{u7d_url}/'
        m3u += "cloud.xml" if cloud else "local.xml" if local else "guide.xml.gz"
        m3u += '"\n'
        services = self.__get_client_channels()
        if cloud or local:
            _services = {}
            for channel_id, channel in services.items():
                if channel_id in (cloud or local):
                    _services[channel_id] = channel
            services = _services
        channels = sorted(services, key=lambda key: services[key])
        if not cloud and not local:
            channels = channels[1:] + channels[:1]
        _fresh = not os.path.exists(file_path)
        for channel_id in channels:
            if channel_id in self.__channels:
                channel_name = self.__channels[channel_id]["name"]
                channel_tag = "Cloud" if cloud else "Local" if local else "U7D"
                channel_tag += " - TDT Movistar.es"
                channel_number = services[channel_id]
                if channel_number == 0:
                    channel_number = 999
                channel_logo = f"{u7d_url}/Logos/" + self.__channels[channel_id]["logo_uri"]
                if channel_id not in epg_channels and not local:
                    msg = f'M3U: Saltando canal encriptado "{channel_name}" {channel_id}'
                    log.info(msg) if _fresh else log.debug(msg)
                    continue
                m3u += f'#EXTINF:-1 ch-number="{channel_number}" audio-track="2" '
                m3u += f'tvg-id="{channel_id}.movistar.tv" '
                m3u += f'group-title="{channel_tag}" '
                m3u += f'tvg-logo="{channel_logo}"'
                m3u += f",{channel_name}\n"
                m3u += f"{u7d_url}"
                m3u += "/cloud" if cloud else "/local" if local else ""
                m3u += f"/{channel_id}/mpegts\n"
        return m3u

    def __get_client_channels(self):
        services = {}
        for package in config["tvPackages"].split("|") if config["tvPackages"] != "ALL" else self.__packages:
            if package in self.__packages:
                services.update(self.__packages[package]["services"])
        return {int(k): int(v) for k, v in services.items()}

    @staticmethod
    def __get_genre_and_subgenre(code):
        return {
            "genre": genre_map[code[0]]["0"],
            "sub-genre": None if code[1] == "0" else genre_map[code[0]][code[1]],
        }

    @staticmethod
    def __get_key_and_subkey(code, genres):
        if not genres:
            return
        genre = next(
            genre
            for genre in genres
            if genre["id"].upper() == (code[0] if code[0] == "0" else ("%s%s" % (code[0], "0")).upper())
        )
        subgenre = (
            None
            if code[1] == "0"
            else next(
                subgenre
                for subgenre in genre["subgenres"]
                if subgenre["id"].upper()
                == (code[1].upper() if code[0] == "0" else ("%s%s" % (code[0], code[1])).upper())
            )
        )
        return {"key": genre["name"], "sub-key": subgenre["name"] if subgenre else None}

    @staticmethod
    def __write_to_disk(file_path, content):
        with codecs.open(file_path, "w", "UTF-8") as f:
            f.write(content)

    async def generate_xml(self, parsed_epg, verbose, local=False):
        root = Element(
            "tv",
            {
                "date": datetime.now().strftime("%Y%m%d%H%M%S"),
                "generator_info_url": "http://wiki.xmltv.org/index.php/XMLTVFormat",
            },
        )
        tz_offset = int(abs(time.timezone / 3600))
        services = self.__get_client_channels()
        for channel_id in sorted(services, key=lambda key: services[key]):
            if channel_id in self.__channels:
                tag_channel = Element("channel", {"id": f"{channel_id}.movistar.tv"})
                tag_dname = SubElement(tag_channel, "display-name")
                tag_dname.text = self.__channels[channel_id]["name"]
                root.append(tag_channel)

        for channel_id in [
            cid
            for cid in sorted(services, key=lambda key: services[key])
            if (cid in self.__channels or local) and cid in parsed_epg
        ]:
            channel_name = self.__channels[channel_id]["name"]
            if channel_id not in epg_channels and not local:
                log.info(f'XML: Saltando canal encriptado "{channel_name}" {channel_id}')
                continue

            if verbose and VERBOSE:
                log.info(f'XML: "{channel_name}"')

            _tasks = [
                self.__build_programme_tag(channel_id, program, parsed_epg[channel_id][program], tz_offset)
                for program in sorted(parsed_epg[channel_id])
            ]
            [root.append(program) for program in (await asyncio.gather(*_tasks)) if program is not None]
        return ElementTree(root)

    def write_m3u(self, file_path, cloud=None, local=None):
        m3u = self.__generate_m3u(file_path, cloud, local)
        self.__write_to_disk(file_path, m3u)


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
    global VERBOSE, cache, config, deadline, epg, iptv, mtv, session, xmltv

    deadline = int(datetime.combine(date.today() - timedelta(days=7), datetime.min.time()).timestamp())

    # Obtiene los argumentos de entrada
    args = create_args_parser().parse_args()

    if (args.cloud_m3u or args.cloud_recordings) and (args.local_m3u or args.local_recordings):
        return
    if any((args.cloud_m3u, args.cloud_recordings, args.local_m3u, args.local_recordings)):
        VERBOSE = False
    else:
        VERBOSE = True
        banner = f"Movistar U7D - TVG v{_version}"
        log.info("-" * len(banner))
        log.info(banner)
        log.info("-" * len(banner))
        log.debug(" ".join(sys.argv[1:]))

    # Crea la caché
    cache = Cache()
    cached = False

    session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            keepalive_timeout=YEAR_SECONDS,
            resolver=AsyncResolver(nameservers=[IPTV_DNS]) if not WIN32 else None,
        ),
        headers={"User-Agent": UA},
        json_serialize=json.dumps,
    )

    try:
        # Descarga la configuración del servicio Web de MovistarTV
        mtv = MovistarTV()
        config = await mtv.get_service_config()

        # Busca el Proveedor de Servicios y descarga los archivos XML:
        # canales, paquetes y segments
        iptv = MulticastIPTV()
        xdata = iptv.get_service_provider_data()

        # Crea el objeto XMLTV a partir de los datos descargados del Proveedor de Servicios
        xmltv = XMLTV(xdata)

        if args.m3u:
            xmltv.write_m3u(args.m3u)
            if not args.guide:
                return

        # Descarga los segments de cada EPG_X_BIN.imagenio.es y devuelve la guía decodificada
        if args.guide:
            epg, cached = await iptv.get_epg()

        elif args.cloud_m3u or args.cloud_recordings or args.local_m3u or args.local_recordings:
            if args.cloud_m3u or args.cloud_recordings:
                epg = cache.load_cloud_epg()
                if not epg:
                    log.error("No existe caché de grabaciones en la Nube. Debe generarla con movistar_epg")
            else:
                epg = cache.load_local_epg()
            if not epg:
                return
            if args.cloud_m3u or args.local_m3u:
                xmltv.write_m3u(
                    args.cloud_m3u or args.local_m3u,
                    cloud=list(epg) if args.cloud_m3u else None,
                    local=list(epg) if args.local_m3u else None,
                )
            if not args.cloud_recordings and not args.local_recordings:
                return

        # Genera el árbol XMLTV de los paquetes contratados
        epg_x = await xmltv.generate_xml(epg, not cached, args.local_m3u or args.local_recordings)
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
            _t = str(timedelta(seconds=round(time.time() - time_start)))
            log.info(f"EPG de {len(epg)} canales y {len(xdata['segments'])} días descargada en {_t}s")
            log.info(f"Fecha de caducidad: [{time.ctime(deadline)}] [{deadline}]")

    finally:
        await session.close()


if __name__ == "__main__":
    if not WIN32:
        from setproctitle import setproctitle

        setproctitle("movistar_tvg %s" % " ".join(sys.argv[1:]))

    time_start = time.time()

    _conf = mu7d_config()

    DEBUG = _conf["DEBUG"]
    VERBOSE = cache = config = deadline = epg = iptv = mtv = session = xmltv = None

    app_dir = os.path.join(_conf["HOME"], ".xmltv")
    cache_dir = os.path.join(app_dir, "cache")
    epg_channels += _conf["EXTRA_CHANNELS"]
    lan_ip = _conf["LAN_IP"]
    tvg_threads = _conf["TVG_THREADS"]
    u7d_url = _conf["U7D_URL"]

    age_rating = ["0", "0", "0", "7", "12", "16", "17", "18"]
    lang = {"es": {"lang": "es"}, "en": {"lang": "en"}}
    max_credits = 4

    cookie_file = "movistar_tvg.cookie"
    end_points_file = "movistar_tvg.endpoints"

    cinema_regex = re.compile(r"^(.+?) \(([^)]+)\) ?(:?\(?.+\)?)?$")
    daily_regex = re.compile(r"^(.+?) - \d{8}(?:_\d{4})?$")
    series_regex = re.compile(r"^(.+) (S\d+E\d+(?: - .+)?)$")

    logging.basicConfig(
        datefmt="[%Y-%m-%d %H:%M:%S]",
        format="%(asctime)s [%(name)s] [%(levelname)8s] %(message)s",
        level=logging.DEBUG if _conf["DEBUG"] else logging.INFO,
    )

    logging.getLogger("asyncio").setLevel(logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.FATAL)

    lockfile = os.path.join(os.getenv("TMP", os.getenv("TMPDIR", "/tmp")), ".movistar_tvg.lock")  # nosec B108
    try:
        with FileLock(lockfile, timeout=0):
            _iptv = get_iptv_ip()
            asyncio.run(tvg_main())
    except KeyboardInterrupt:
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
    except ValueError as err:
        log.critical(err)
        sys.exit(1)
