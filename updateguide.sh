#!/bin/sh

/usr/bin/docker ps | grep -q movistar_u7d || exit

rm -fr /opt/xmltv/.xmltv/cache
/usr/bin/docker exec -it movistar_u7d /app/tv_grab_es_movistartv --m3u /home/MovistarTV.m3u
/usr/bin/docker exec -it movistar_u7d /app/tv_grab_es_movistartv --tvheadend /home/MovistarTV.m3u --output /home/guide.xml
