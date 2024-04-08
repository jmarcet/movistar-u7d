#!/usr/bin/env python3

import asyncio
import logging
import os
import psutil
import sys

from asyncio.exceptions import CancelledError
from contextlib import closing
from datetime import datetime
from filelock import FileLock, Timeout
from socket import AF_INET, SOCK_STREAM, socket
from time import sleep

from movistar_cfg import CONF, DATEFMT, EXT, FMT, WIN32, add_logfile
from movistar_lib import IPTVNetworkError, LocalNetworkError
from movistar_lib import cleanup_handler, get_iptv_ip, launch, reaper, _version

log = logging.getLogger("INI")

logging.basicConfig(datefmt=DATEFMT, format=FMT, level=logging.INFO)
logging.getLogger("asyncio").setLevel(logging.FATAL)
logging.getLogger("filelock").setLevel(logging.FATAL)

if CONF.get("LOG_TO_FILE"):
    if add_logfile(log, CONF["LOG_TO_FILE"], CONF.get("DEBUG", True) and logging.DEBUG or logging.INFO):
        log.error(f'Cannot write logs to {CONF["LOG_TO_FILE"]}')


async def u7d_main():
    u7d_cmd = (f"movistar_u7d{EXT}",)
    u7d_t = asyncio.create_task(launch(u7d_cmd))

    while True:
        try:
            await u7d_t
        except CancelledError:
            if not WIN32:
                break

        if WIN32:
            input("\nPulsa una tecla para salir...")
            break

        u7d_t = asyncio.create_task(launch(u7d_cmd))


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

    if not CONF:
        log.critical("Imposible parsear fichero de configuración")
        sys.exit(1)

    banner = f"Movistar U7D v{_version}"
    log.info("=" * len(banner))
    log.info(banner)
    log.info("=" * len(banner))

    if not WIN32 and any(("UID" in CONF, "GID" in CONF)):
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

    lockfile = os.path.join(CONF["TMP_DIR"], ".mu7d.lock")
    try:
        with FileLock(lockfile, timeout=0):
            _check_iptv()
            _check_ports()
            del CONF
            asyncio.run(u7d_main())
    except (CancelledError, KeyboardInterrupt):
        sys.exit(1)
    except (IPTVNetworkError, LocalNetworkError) as err:
        log.critical(err)
        sys.exit(1)
    except Timeout:
        log.critical("Cannot be run more than once!")
        sys.exit(1)
