from cx_Freeze import Executable, setup
from version import _version

import sanic


build_exe_options = {
    "include_files": sanic.__path__ + ["movistar-u7d.ps1", "ffmpeg.exe", "mkvmerge.exe", "timers.conf"],
    "includes": ["anyio._backends._asyncio"],
    "excludes": ["test", "tkinter", "unittest"],
    "optimize": 2,
}

executables = [
    Executable("movistar_epg.py"),
    Executable("movistar_u7d.py"),
    Executable("tv_grab_es_movistartv"),
    Executable("vod.py"),
]

setup(
    name="movistar-u7d",
    version=_version,
    description="Movistar IPTV U7D to flussonic catchup proxy",
    options={"build_exe": build_exe_options},
    executables=executables,
)
