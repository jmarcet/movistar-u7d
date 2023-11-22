from cx_Freeze import Executable, setup  # pylint: disable=import-error

import sanic

from mu7d import _version


build_exe_options = {
    "include_files": sanic.__path__ + ["comskip.ini", "mu7d.conf", "timers.conf", "ffmpeg.exe", "ffprobe.exe"],
    "excludes": ["test", "tkinter", "unittest"],
    "optimize": 2,
}

executables = [
    Executable("movistar_epg.py"),
    Executable("movistar_tvg.py"),
    Executable("movistar_u7d.py"),
    Executable("movistar_vod.py"),
    Executable("mu7d.py"),
]

setup(
    name="movistar-u7d",
    version=_version,
    description="Movistar U7D",
    options={"build_exe": build_exe_options},
    executables=executables,
)
