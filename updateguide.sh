#!/bin/sh

# /usr/bin/docker ps | grep -q movistar_u7d || exit

# /usr/bin/docker exec movistar_u7d /app/tv_grab_es_movistartv --tvheadend /home/MovistarTV.m3u --output /home/guide.xml
# /usr/bin/docker exec movistar_u7d /usr/bin/wget -qO /dev/null http://127.0.0.1:8889/reload_epg

tv_grab_es_movistartv --tvheadend ${HOME:-/home}/MovistarTV.m3u --output ${HOME:-/home}/guide.xml
wget -qO /dev/null http://127.0.0.1:8889/reload_epg
