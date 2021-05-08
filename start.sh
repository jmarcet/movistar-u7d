#!/bin/sh

if ifconfig eth1 &>/dev/null; then
    [ -n "${MAC_IPTV_U7D}" ] && ifconfig eth0 hw ether ${MAC_IPTV_U7D}
    [ -n "${MAC_MOVISTAR_U7D}" ] && ifconfig eth1 hw ether ${MAC_MOVISTAR_U7D}
    ip route del 0.0.0.0/0 via ${IPTV_GW} dev eth0
    ip route add 172.0.0.0/11 via ${IPTV_GW} dev eth0
    ip route add 0.0.0.0/0 via ${U7D_GW} dev eth1
fi

echo "nameserver 172.26.23.3" > /etc/resolv.conf
echo "nameserver 127.0.0.1" >> /etc/resolv.conf
echo "172.26.22.23 www-60.svc.imagenio.telefonica.net" >> /etc/hosts
echo "172.26.83.49 html5-static.svc.imagenio.telefonica.net" >> /etc/hosts

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
