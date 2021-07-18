#!/bin/sh

if [ -n "${U7D_UID}" -o -n "${U7D_GID}" ]; then
	_SUDO="sudo -E"
	[ -n "${U7D_UID}" ] && _SUDO="${_SUDO} -u ${U7D_UID}"
	[ -n "${U7D_GID}" ] && _SUDO="${_SUDO} -g ${U7D_GID}"
fi

while ! test -e "${HOME:-/home}/MovistarTV.m3u"; do
    ${_SUDO} /app/tv_grab_es_movistartv --m3u "${HOME:-/home}/MovistarTV.m3u"
    sleep 15
done

while ! test -e "${HOME:-/home}/guide.xml"; do
    ${_SUDO} /app/tv_grab_es_movistartv \
        --tvheadend "${HOME:-/home}/MovistarTV.m3u" \
        --output "${HOME:-/home}/guide.xml"
    sleep 15
done

( while (true); do nice -n -10 ionice -c 2 -n 0 ${_SUDO} /app/movistar-epg.py; sleep 1; done ) &
( while (true); do nice -n -15 ionice -c 1 -n 0 ${_SUDO} /app/movistar-u7d.py; sleep 1; done ) &

tail -f /dev/null

