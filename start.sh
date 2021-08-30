#!/bin/sh

[ -n "${U7D_UID}" ] && _SUDO="s6-setuidgid ${U7D_UID}"

grep -q '172.26.23.3' /etc/resolv.conf || \
    sed "1 i nameserver 172.26.23.3" -i /etc/resolv.conf

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

( while (true); do nice -n -10 ionice -c 2 -n 0 ${_SUDO} /app/movistar_epg.py; sleep 1; done ) &
( while (true); do nice -n -15 ionice -c 1 -n 0 ${_SUDO} /app/movistar_u7d.py; sleep 1; done ) &

tail -f /dev/null

