import asyncio
import os
import sys

EXT = ".exe" if getattr(sys, "frozen", False) else ".py"


async def launch(cmd):
    return await (await asyncio.create_subprocess_exec(*cmd)).wait()


async def main():
    prefix = [sys.executable] if EXT == ".py" else []

    epg = asyncio.create_task(launch(prefix + [f"movistar_epg{EXT}"]))
    u7d = asyncio.create_task(launch(prefix + [f"movistar_u7d{EXT}"]))

    for coro in asyncio.as_completed({epg, u7d}):
        await coro
        break


if __name__ == "__main__":
    if EXT == ".exe":
        path = os.environ["PATH"] = os.path.dirname(sys.executable)
    else:
        path = os.environ["PATH"] = "%s;%s" % (os.path.dirname(__file__), os.getenv("PATH"))
    os.environ["PYTHONUNBUFFERED"] = "1"

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)
