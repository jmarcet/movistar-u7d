#!/bin/sh

if [ -n "${U7D_UID}" -o -n "${U7D_GID}" ]; then
    _SUDO="sudo -E"
    [ -n "${U7D_UID}" ] && _SUDO="${_SUDO} -u ${U7D_UID}"
    [ -n "${U7D_GID}" ] && _SUDO="${_SUDO} -g ${U7D_GID}"
fi

_ping()
{
    while ! ping -q -c 1 -W 1 $1 >/dev/null; do echo "$2"; sleep 1; done
}

if [ -n "$IPTV_ADDRESS" ] || [ -n "$LAN_IP" ]; then
    [ -n "$IPTV_ADDRESS" ] && \
        _ping $IPTV_ADDRESS "Waiting for IPTV_ADDRESS=$IPTV_ADDRESS to be up..."
    [ -n "$LAN_IP" ] && \
        _ping $LAN_IP "Waiting for LAN_IP=$LAN_IP to be up..."
else
    _ping `hostname` "Waiting for `hostname` to be pingable..."
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

( while (true); do nice -n -10 ionice -c 2 -n 0 ${_SUDO} /app/movistar_epg.py; sleep 1; done ) &
( while (true); do nice -n -15 ionice -c 1 -n 0 ${_SUDO} /app/movistar_u7d.py; sleep 1; done ) &

tail -f /dev/null

