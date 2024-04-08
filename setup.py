from cx_Freeze import Executable, setup  # pylint: disable=import-error
from glob import glob

import sanic

from movistar_lib import _version


ffmpeg = ["ffmpeg.exe", "ffprobe.exe"] + glob("*.dll")

build_exe_options = {
    "include_files": sanic.__path__ + ["comskip.ini", "mu7d.conf", "timers.conf"] + ffmpeg,
    "excludes": ["test", "tkinter", "unittest"],
    "optimize": 2,
}

executables = [
    Executable("movistar_u7d.py"),
    Executable("movistar_tvg.py"),
    Executable("movistar_vod.py"),
]

setup(
    name="movistar-u7d",
    version=_version,
    description="Movistar U7D",
    options={"build_exe": build_exe_options},
    executables=executables,
)
