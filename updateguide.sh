#!/bin/sh

/usr/bin/docker ps | grep -q movistar_u7d || exit

/usr/bin/docker exec -it movistar_u7d /app/tv_grab_es_movistartv --tvheadend /home/MovistarTV.m3u --output /home/guide.xml
/usr/bin/docker exec -it movistar_u7d /usr/bin/wget -qO /dev/null http://127.0.0.1:8889/reload_epg
