#!/bin/sh

/usr/bin/docker ps | grep -q movistar_u7d || exit

/usr/bin/docker exec -it movistar_u7d /app/tv_grab_es_movistartv --tvheadend /home/MovistarTV.m3u --output /home/guide.xml

curl http://10.137.70.3:8888/reload_epg
