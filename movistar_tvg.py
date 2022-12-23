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
from queue import Queue
from xml.dom import minidom  # nosec B408
from xml.etree.ElementTree import Element, ElementTree, SubElement  # nosec B405

from mu7d import IPTV_DNS, UA, UA_U7D, WIN32, YEAR_SECONDS
from mu7d import get_iptv_ip, get_title_meta, mu7d_config, _version


log = logging.getLogger("TVG")


epg_channels = [
    2543,
    4455,
    4919,
    2524,
    1825,
    1826,
    2526,
    884,
    4714,
    3223,
    844,
    1221,
    4063,
    2544,
    3063,
    657,
    663,
    4716,
    4715,
    3185,
    1616,
    578,
    4467,
    5104,
    743,
    582,
    3603,
    3443,
    5029,
    3103,
    4990,
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
        except (IOError, KeyError, ValueError):
            pass

    @staticmethod
    def __save(cfile, data):
        with open(os.path.join(cache_dir, cfile), "w", encoding="utf8") as f:
            try:
                json.dump({"data": data}, f, ensure_ascii=False, indent=4, sort_keys=True)
            except AttributeError:
                json.dump({"data": data}, f, ensure_ascii=False, sort_keys=True)

    def load_cloud_epg(self):
        data = self.__load("cloud.json")
        _int_data = defaultdict(dict)
        for channel in data:
            _int_data[int(channel)] = {int(k): v for k, v in data[channel].items()}
        return _int_data

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
        remote_cache_ts = int(now.replace(hour=0, minute=59, second=0).timestamp())
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
                                log.warning(f"No ha habido suerte. Reintentando en 15s... [{i+2}/5]")
                                await asyncio.sleep(15)
                    except Exception as ex:
                        if i < 4:
                            log.warning(
                                f"No ha habido suerte. Reintentando en 15s... [{i+2}/5] => {repr(ex)}"
                            )
                            await asyncio.sleep(15)
            if not data:
                log.warning(
                    "Caché de EPG no encontrada. "
                    "Tendrá que esperar unos días para poder acceder a todo el archivo de los útimos 7 días."
                )
        if data:
            _int_data = defaultdict(dict)
            for channel in data:
                _int_data[int(channel)] = {int(k): v for k, v in data[channel].items()}
            return _int_data

    def load_epg_extended_info(self, pid):
        if pid in self.__programs:
            return self.__programs[pid]
        else:
            return self.__load(os.path.join("programs", f"{pid}.json"))

    def load_service_provider_data(self):
        return self.__load("provider.json")

    def save_config(self, data):
        self.__save("config.json", data)

    def save_cookie(self, data):
        # log.debug(f"Set-Cookie: {data}")
        self.__save(cookie_file, data)

    def save_end_points(self, data):
        log.info(f"Nuevos End Points: {sorted(data.keys())}")
        self.__end_points = data
        self.__save(end_points_file, data)

    def save_epg(self, data):
        self.__save("epg.json", data)

    def save_epg_data(self, data):
        self.__save("epg_metadata.json", data)

    def save_epg_extended_info(self, data):
        self.__programs[data["productID"]] = data
        self.__save(os.path.join("programs", f'{data["productID"]}.json'), data)

    def save_service_provider_data(self, data):
        self.__save("provider.json", data)


class MovistarTV:
    def __init__(self):
        self.__cookie = cache.load_cookie()
        self.__end_points_down = []
        self.__web_service_down = False
        self.__session = None

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
            log.warning("Servicio Web de Movistar TV caído: decargando guía básica")
            self.__web_service_down = True
            return
        __attempts = 10
        while __attempts > 0:
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
                __attempts -= 1
                log.debug(f"Timeout: {ep}, reintentos: {__attempts} => {repr(ex)}")
                continue

    @staticmethod
    def __update_end_points(data):
        cache.save_end_points(data)
        return data

    def get_end_point(self):
        eps = self.__get_end_points()
        for ep in sorted(eps):
            if eps[ep] not in self.__end_points_down:
                return eps[ep]

    async def get_epg_extended_info(self, pid, channel_id):
        try:
            data = cache.load_epg_extended_info(pid)
        except Exception as ex:
            log.debug(f"{repr(ex)}")
        if not data:
            try:
                data = await self.__get_service_data(
                    f"epgInfov2&productID={pid}&channelID={channel_id}&extra=1"
                )
                cache.save_epg_extended_info(data)
            except Exception as ex:
                log.debug(f"Información extendida no encontrada: {pid} {repr(ex)}")
                return
        return data

    def get_first_end_point(self):
        eps = self.__get_end_points()
        for ep in sorted(eps):
            return eps[ep]

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

    def __decode_string(self, string):
        _t = ("".join(chr(char ^ 0x15) for char in string)).encode("latin1").decode("utf8")
        return _t.replace("&quot;", "«", 1).replace("&quot;", "»", 1)

    def __drop_encrypted(self, epg):
        clean_channels = {}
        for channel in set(epg) & set(epg_channels):
            clean_channels[channel] = epg[channel]
        return clean_channels

    def __expire_epg(self, epg):
        expired = 0
        for channel in epg:
            _expired = tuple(filter(lambda ts: epg[channel][ts]["end"] < deadline, epg[channel]))
            tuple(map(epg[channel].pop, _expired))
            expired += len(_expired)
        return expired

    def __find_gaps(self, epg, channel, sorted_channel, new_epg=None, new_gaps=None):
        drop = defaultdict(list)
        fixed = 0
        is_new_epg = new_gaps is not None
        name = "New EPG" if is_new_epg else "Cached EPG"
        now = int(datetime.now().timestamp())

        for i in range(len(sorted_channel) - 1):
            ts = sorted_channel[i]
            _end = epg[channel][ts]["end"]
            _next = epg[channel][sorted_channel[i + 1]]["start"]
            __ts, __end, __next = [time.ctime(x) for x in (ts, _end, _next)]

            if _end < _next:
                msg = f"[{channel}] {name} GAP [{ts}] FROM:[{__end}] [{_end}] - TO:[{__next}] [{_next}]"
                if is_new_epg:
                    log.debug(msg)
                    new_gaps[channel].append((_end, _next))
                elif ts < now:
                    log.error(msg)

            elif _end > _next:
                if (epg[channel][ts]["full_title"] == epg[channel][_next]["full_title"] or ts < now) and (
                    not is_new_epg or ts not in new_epg[channel]
                ):
                    drop[channel].append(ts)
                    msg = f"[{channel}] {name} DROP -> GAP FROM:[{__ts}] [{ts}] - TO:[{__next}] [{_next}]"
                else:
                    epg[channel][ts]["end"] = _next
                    epg[channel][ts]["duration"] = _next - ts
                    msg = f"[{channel}] {name} SHORTENED [{ts}] end=[{__end}] [{_end}] > [{__next}] [{_next}]"
                log.debug(msg) if ts > now else log.warning(msg)
                fixed += 1

        for channel in drop:
            for ts in drop[channel]:
                del epg[channel][ts]

        return fixed

    def __fix_epg(self, epg):
        fixed_diff = fixed_over = 0
        new_gaps = defaultdict(list)
        for channel in sorted(epg):
            _new = sorted(epg[channel])
            log.debug(f"[{channel}] New EPG FROM:[{time.ctime(_new[0])}] - TO:[{time.ctime(_new[-1])}]")

            for ts in sorted(epg[channel]):
                _duration = epg[channel][ts]["duration"]
                _end = epg[channel][ts]["end"]

                if _duration != _end - ts:
                    epg[channel][ts]["duration"] = _end - ts
                    log.warning(
                        f"[{channel}] New EPG WRONG DURATION [{ts}] duration={_duration} -> {_end - ts}"
                    )
                    fixed_diff += 1

            fixed_over += self.__find_gaps(epg, channel, _new, new_gaps=new_gaps)

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
        for n in range(threads):
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
                    "logo_uri": i[1].attrib["logoURI"]
                    if "logoURI" in i[1].attrib
                    else "MAY_1/imSer/4146.jpg",
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
            _msg = "[" + " ".join(sorted(xml)) + "] / [2_0 5_0 6_0]"
            if "2_0" in xml and "5_0" in xml and "6_0" in xml:
                if VERBOSE:
                    log.info(f"Ficheros XML descargados: {_msg}")
                break
            else:
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
        cache.save_epg_data(self.__xml_data)

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
            for channel_id in _channels:
                if (
                    "replacement" in _channels[channel_id]
                    and _channels[channel_id]["replacement"] == channel_key
                ):
                    sane_epg[channel_id] = epg[channel_key]
                    break
            if channel_id not in sane_epg:
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
                    while not (chunk["end"]):
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
                    log.error(f"Error al descargar los archivos XML: {repr(ex)}")
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
                        or any(filter(lambda gap: ts >= gap[0] and ts < gap[1], new_gaps[channel]))
                    ):
                        cached_epg[ts] = epg[channel][ts]
                epg[channel] = {**cached_epg, **new_epg[channel]}
            else:
                epg[channel] = new_epg[channel]

        for channel in sorted(epg):
            sorted_channel = sorted(epg[channel])
            fixed += self.__find_gaps(epg, channel, sorted_channel, new_epg=new_epg)

        log.info(f"Eventos: Arreglados = {fixed} - Caducados = {expired}")

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
        return self.__drop_encrypted(cache.load_cloud_epg())

    def get_day(self, mcast_grp, mcast_port, source):
        day = int(source.split("_")[1]) - 1
        log.info("Descargando XML " + source.split(".")[0] + f" -> {mcast_grp}:{mcast_port}")
        self.__epg[day] = self.__get_xml_files(mcast_grp, mcast_port)

    async def get_epg(self):
        cached_epg = self.__drop_encrypted(await cache.load_epg())
        self.__get_bin_epg()
        try:
            new_epg = self.__drop_encrypted(self.__get_sane_epg(self.__parse_bin_epg()))
            log.info(f"Conservando {len(new_epg)} canales en abierto")
        except Exception as ex:
            log.debug(f"{repr(ex)}")
            log.warning("Error descargando la EPG. Reintentando...")
            return await self.get_epg()

        if datetime.now().hour < 2:
            self.__expire_epg(new_epg)

        log.info("Comprobando si el EPG necesita arreglos...")
        fixed, new_gaps = self.__fix_epg(new_epg)

        if not cached_epg:
            cache.save_epg(new_epg)
            return new_epg, False

        log.debug(f"Fecha de caducidad: [{time.ctime(deadline)}] [{deadline}]")
        expired = self.__expire_epg(cached_epg)

        log.info("Comprobando si el EPG resultante necesita arreglos...")
        self.__merge_epg(cached_epg, expired, fixed, new_epg, new_gaps)

        cache.save_epg(cached_epg)
        return cached_epg, True

    def get_service_provider_data(self):
        if not self.__xml_data:
            connection = self.__get_service_provider_ip()
            self.__get_epg_data(connection["mcast_grp"], connection["mcast_port"])
        return self.__xml_data


class XMLTV:
    def __init__(self, data):
        self.__channels = data["channels"]
        self.__packages = data["packages"]

    async def __build_programme_tag(self, channel_id, program, tz_offset):
        dst_start = time.localtime(int(program["start"])).tm_isdst
        dst_stop = time.localtime(int(program["end"])).tm_isdst
        start = datetime.fromtimestamp(program["start"]).strftime("%Y%m%d%H%M%S")
        stop = datetime.fromtimestamp(program["end"]).strftime("%Y%m%d%H%M%S")
        tag_programme = Element(
            "programme",
            {
                "channel": f"{channel_id}.movistar.tv",
                "start": f"{start} +0{tz_offset + dst_start}00",
                "stop": f"{stop} +0{tz_offset + dst_stop}00",
            },
        )
        tag_title = SubElement(tag_programme, "title", lang["es"])
        tag_title.text = program["full_title"]
        tag_desc = SubElement(tag_programme, "desc", lang["es"])
        tag_desc.text = "Año: " + program["year"] + ". "
        ext_info = await mtv.get_epg_extended_info(program["pid"], channel_id)
        orig_title = ext_info.get("originalTitle") if ext_info else None
        if orig_title and orig_title not in program["full_title"] and not orig_title.startswith("Episod"):
            tag_desc.text += f"«{orig_title}» "
        gens = self.__get_genre_and_subgenre(program["genre"])
        keys = self.__get_key_and_subkey(program["genre"], config["genres"])
        # Series
        if program["is_serie"] or program["serie_id"] > 0:
            tsse = self.__get_series_data(program, ext_info)
            tag_title.text = tsse["title"]
            tag_stitle = SubElement(tag_programme, "sub-title", lang["es"])
            tag_stitle.text = tsse["sub-title"]
            tag_date = SubElement(tag_programme, "date")
            tag_date.text = program["year"]
            tag_episode_num = SubElement(tag_programme, "episode-num", {"system": "xmltv_ns"})
            tag_episode_num.text = (
                str(int(tsse["season"]) - 1 if tsse["season"] else "")
                + "."
                + str(int(tsse["episode"]) - 1 if tsse["episode"] else "")
            )
        # Películas y otros
        elif ext_info:
            if "productionDate" in ext_info:
                if "Movie" in gens["genre"]:
                    tag_stitle = SubElement(tag_programme, "sub-title", lang["es"])
                    tag_stitle.text = str(ext_info["productionDate"])
                tag_date = SubElement(tag_programme, "date")
                tag_date.text = str(ext_info["productionDate"])
                tag_desc.text = "Año: " + tag_date.text + ". "
        # Comunes a los tres
        if ext_info:
            if ("mainActors" or "directors") in ext_info:
                tag_credits = SubElement(tag_programme, "credits")
                if "directors" in ext_info:
                    tag_desc.text += ",".join(ext_info["directors"])
                    tag_desc.text += ". "
                    length = (
                        len(ext_info["directors"])
                        if len(ext_info["directors"]) <= max_credits
                        else max_credits
                    )
                    for director in ext_info["directors"][:length]:
                        tag_director = SubElement(tag_credits, "director")
                        tag_director.text = director.strip()
                if "mainActors" in ext_info:
                    tag_desc.text += ",".join(ext_info["mainActors"])
                    tag_desc.text += "."
                    length = (
                        len(ext_info["mainActors"])
                        if len(ext_info["mainActors"]) <= max_credits
                        else max_credits
                    )
                    for actor in ext_info["mainActors"][:length]:
                        tag_actor = SubElement(tag_credits, "actor")
                        tag_actor.text = actor.strip()
            SubElement(tag_programme, "icon", {"src": f"{u7d_url}/Covers/" + ext_info["cover"]})
            tag_desc.text += "\n\n" + ext_info["description"]
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

    def __generate_m3u(self, file_path, cloud=None):
        m3u = '#EXTM3U name="'
        m3u += "Cloud " if cloud else ""
        m3u += 'MovistarTV" catchup="flussonic-ts" catchup-days="'
        m3u += '9999" ' if cloud else '8" '
        m3u += f'dlna_extras=mpeg_ps_pal max-conn="12" refresh="1200" url-tvg="{u7d_url}/'
        m3u += "cloud.xml" if cloud else "guide.xml.gz"
        m3u += '"\n'
        services = self.__get_client_channels()
        if cloud:
            cloud_services = {}
            for id, channel in services.items():
                if id in cloud:
                    cloud_services[id] = channel
            services = cloud_services
        channels = sorted(services, key=lambda key: services[key])
        if not cloud:
            channels = channels[1:] + channels[:1]
        _fresh = not os.path.exists(file_path)
        for channel_id in channels:
            if channel_id in self.__channels:
                channel_name = self.__channels[channel_id]["name"]
                channel_tag = "Cloud" if cloud else "U7D"
                channel_tag += " - TDT Movistar.es"
                channel_number = services[channel_id]
                if channel_number == "0":
                    channel_number = "999"
                channel_logo = f"{u7d_url}/Logos/" + self.__channels[channel_id]["logo_uri"]
                if channel_id not in epg_channels:
                    msg = f'M3U: Saltando canal encriptado "{channel_name}" {channel_id}'
                    log.info(msg) if _fresh else log.debug(msg)
                    continue
                m3u += f'#EXTINF:-1 ch-number="{channel_number}" '
                m3u += f'tvg-id="{channel_id}.movistar.tv" '
                m3u += f'group-title="{channel_tag}" '
                m3u += f'tvg-logo="{channel_logo}"'
                m3u += f",{channel_name}\n"
                m3u += f"{u7d_url}"
                m3u += "/cloud" if cloud else ""
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
    def __get_series_data(program, ext_info):
        episode = program["episode"]
        season = program["season"]
        stitle = program.get("episode_title", "")
        return {
            "title": program["serie"] or program["full_title"],
            "sub-title": stitle if not stitle.startswith("Episod") else "",
            "season": season,
            "episode": episode,
        }

    @staticmethod
    def __write_to_disk(file_path, content):
        with codecs.open(file_path, "w", "UTF-8") as file_h:
            file_h.write(content)
            file_h.close()

    async def generate_xml(self, parsed_epg, verbose):
        if VERBOSE:
            log.info("Generando la guía XMLTV...")
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
            else:
                log.debug(f"El canal {channel_id} no tiene EPG")

        if VERBOSE:
            log.info("XML: Descargando info extendida")
        for channel_id in [
            cid
            for cid in sorted(services, key=lambda key: services[key])
            if cid in self.__channels and cid in parsed_epg
        ]:
            channel_name = self.__channels[channel_id]["name"]
            if channel_id not in epg_channels:
                log.debug(f'XML: Saltando canal encriptado "{channel_name}" {channel_id}')
                continue

            if verbose and VERBOSE:
                log.info(f'XML: "{channel_name}"')

            _tasks = [
                self.__build_programme_tag(channel_id, parsed_epg[channel_id][program], tz_offset)
                for program in sorted(parsed_epg[channel_id])
            ]
            [root.append(program) for program in (await asyncio.gather(*_tasks))]
        return ElementTree(root)

    def write_m3u(self, file_path, cloud=None):
        m3u = self.__generate_m3u(file_path, cloud)
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
    return parser


def export_channels(m3u_file, cloud=None):
    xmltv.write_m3u(m3u_file, cloud)
    log.info(f"Lista de canales " f'{"de Grabaciones en la Nube " if cloud else ""}' f"exportada: {m3u_file}")


async def tvg_main():
    global VERBOSE, cache, config, deadline, epg, iptv, mtv, session, xmltv

    deadline = int(datetime.combine(date.today() - timedelta(days=7), datetime.min.time()).timestamp())

    # Obtiene los argumentos de entrada
    args = create_args_parser().parse_args()

    if args.cloud_m3u or args.cloud_recordings:
        VERBOSE = False
        if args.cloud_m3u:
            log.info("Generando Lista de canales de Grabaciones en la Nube...")
        if args.cloud_recordings:
            log.info("Generando Guía XMLTV de Grabaciones en la Nube...")
    else:
        VERBOSE = True
        banner = f"Movistar U7D - TVG v{_version}"
        log.info("-" * len(banner))
        log.info(banner)
        log.info("-" * len(banner))
        log.debug(" ".join(sys.argv[1:]))

    # Crea la caché
    cache = Cache()

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
            export_channels(args.m3u)
            if not args.guide:
                return

        # Descarga los segments de cada EPG_X_BIN.imagenio.es y devuelve la guía decodificada
        if args.guide:
            epg, cached = await iptv.get_epg()
        elif args.cloud_m3u or args.cloud_recordings:
            epg = cache.load_cloud_epg()
            if not epg:
                log.error("No existe caché de grabaciones en la nube. Debe generarla con movistar_epg")
                return
            if args.cloud_m3u:
                export_channels(args.cloud_m3u, list(epg))
            if not args.cloud_recordings:
                return
            cached = False

        # Genera el árbol XMLTV de los paquetes contratados
        epg_x = await xmltv.generate_xml(epg, verbose=not cached)
        dom = minidom.parseString(ElTr.tostring(epg_x.getroot()))  # nosec B318

        if args.guide:
            with open(args.guide, "w", encoding="utf8") as f:
                dom.writexml(f, indent="    ", addindent="    ", newl="\n", encoding="UTF-8")
            with gzip.open(args.guide + ".gz", "wt", encoding="utf8") as f:
                dom.writexml(f, indent="    ", addindent="    ", newl="\n", encoding="UTF-8")
            msg = "EPG de %i canales y %i días descargada" % (len(epg), len(xdata["segments"]))
        elif args.cloud_recordings:
            with open(args.cloud_recordings, "w", encoding="utf8") as f:
                dom.writexml(f, indent="    ", addindent="    ", newl="\n", encoding="UTF-8")
            msg = "EPG de Grabaciones en la Nube descargada"

        total_time = str(timedelta(seconds=round(time.time() - time_start)))
        log.info(f"{msg} en {total_time}s")

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

    logging.basicConfig(
        datefmt="[%Y-%m-%d %H:%M:%S]",
        format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
        level=logging.DEBUG if _conf["DEBUG"] else logging.INFO,
    )

    logging.getLogger("asyncio").setLevel(logging.DEBUG if _conf["DEBUG"] else logging.FATAL)
    logging.getLogger("filelock").setLevel(logging.DEBUG if _conf["DEBUG"] else logging.FATAL)

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
    except ValueError as ex:
        log.critical(ex)
        sys.exit(1)
