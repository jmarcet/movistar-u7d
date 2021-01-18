#!/bin/sh

if ifconfig eth1 &>/dev/null; then
    ifconfig eth0 hw ether ${MAC_IPTV_U7D}
    ifconfig eth1 hw ether ${MAC_MOVISTAR_U7D}
    ip route del 0.0.0.0/0 via ${IPTV_GW} dev eth0
    ip route add 172.0.0.0/11 via ${IPTV_GW} dev eth0
    ip route add 0.0.0.0/0 via ${U7D_GW} dev eth1
fi

echo "nameserver 172.26.23.3" > /etc/resolv.conf
echo "nameserver 127.0.0.1" >> /etc/resolv.conf
echo "172.26.22.23 www-60.svc.imagenio.telefonica.net" >> /etc/hosts

while (true); do 
    /app/movistar-u7d.py
    sleep 1
done
