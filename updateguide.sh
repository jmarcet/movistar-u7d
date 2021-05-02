#!/bin/sh

# /usr/bin/docker ps | grep -q movistar_u7d || exit

# /usr/bin/docker exec -u nobody movistar_u7d test -e /home/MovistarTV.m3u || \
# 	/usr/bin/docker exec -u nobody movistar_u7d /app/tv_grab_es_movistartv --m3u /home/MovistarTV.m3u
# /usr/bin/docker exec -u nobody movistar_u7d /app/tv_grab_es_movistartv --tvheadend /home/MovistarTV.m3u --output /home/guide.xml
# /usr/bin/docker exec -u nobody movistar_u7d /usr/bin/wget -qO /dev/null http://127.0.0.1:8889/reload_epg

test -e "${HOME:-/home}/MovistarTV.m3u" || tv_grab_es_movistartv --m3u "${HOME:-/home}/MovistarTV.m3u"
for i in $( seq 5 ); do
	tv_grab_es_movistartv \
		--tvheadend "${HOME:-/home}/MovistarTV.m3u" \
		--output "${HOME:-/home}/guide.xml" \
		&& break
	sleep 10
	echo "$(date): reintentando..."
done
wget -qO /dev/null http://127.0.0.1:8889/reload_epg
