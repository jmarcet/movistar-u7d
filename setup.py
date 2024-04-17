from glob import glob

import sanic
from cx_Freeze import Executable, setup  # pylint: disable=import-error

from mu7d_lib import VERSION

ffmpeg = ["ffmpeg.exe", "ffprobe.exe"] + glob("*.dll")

build_exe_options = {
    "include_files": sanic.__path__ + ["comskip.ini", "mu7d.conf", "timers.conf"] + ffmpeg,
    "excludes": ["test", "tkinter", "unittest"],
    "optimize": 2,
}

executables = [
    Executable("mu7d.py"),
    Executable("mu7d_tvg.py"),
    Executable("mu7d_vod.py"),
]

setup(
    name="movistar-u7d",
    version=VERSION,
    description="Movistar U7D",
    options={"build_exe": build_exe_options},
    executables=executables,
)
