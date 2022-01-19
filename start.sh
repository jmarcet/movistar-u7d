#!/bin/sh

[ -n "${U7D_UID}" ] && _SUDO="s6-setuidgid ${U7D_UID}"

export PATH="/app:$PATH"
hash -r

( while (true); do nc -z -v -w 1 172.26.23.3 53 2>/dev/null && break; sleep 1; done )
( while (true); do nice -n -10 ionice -c 2 -n 0 ${_SUDO} movistar_epg.py; sleep 1; done ) &
( while (true); do nc -z -v -w 1 127.0.0.1 8889 2>/dev/null && break; sleep 1; done )
( while (true); do nice -n -15 ionice -c 1 -n 0 ${_SUDO} movistar_u7d.py; sleep 1; done ) &

${_SUDO} tail -f /dev/null

