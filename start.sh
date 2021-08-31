#!/bin/sh

[ -n "${U7D_UID}" ] && _SUDO="s6-setuidgid ${U7D_UID}"

( while (true); do nice -n -10 ionice -c 2 -n 0 ${_SUDO} /app/movistar_epg.py; sleep 1; done ) &
( while (true); do nice -n -15 ionice -c 1 -n 0 ${_SUDO} /app/movistar_u7d.py; sleep 1; done ) &

tail -f /dev/null

