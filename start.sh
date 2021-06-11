#!/bin/sh

if [ -n "${IPTV_GW}" -a -n "${U7D_GW}" ] && \
	[ -e /sys/class/net/eth0 -a -e /sys/class/net/eth1 ]; then
    [ -n "${MAC_IPTV_U7D}" ] && ifconfig eth0 hw ether ${MAC_IPTV_U7D}
    [ -n "${MAC_MOVISTAR_U7D}" ] && ifconfig eth1 hw ether ${MAC_MOVISTAR_U7D}
    ip route del 0.0.0.0/0 via ${IPTV_GW} dev eth0
    ip route add 172.0.0.0/11 via ${IPTV_GW} dev eth0
    ip route add 0.0.0.0/0 via ${U7D_GW} dev eth1
fi

echo "nameserver 127.0.0.1" > /etc/resolv.conf

/etc/init.d/dnsmasq start
tail -f /var/log/dns.log &

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

( while (true); do ${_SUDO} /app/movistar-epg.py; sleep 1; done ) &
( while (true); do ${_SUDO} /app/movistar-u7d.py; sleep 1; done ) &

tail -f /dev/null
