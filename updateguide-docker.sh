#!/bin/sh

docker ps | grep -q movistar_u7d || exit

docker exec -u nobody movistar_u7d test -e /home/MovistarTV.m3u || \
	docker exec -u nobody movistar_u7d \
	/app/tv_grab_es_movistartv --m3u /home/MovistarTV.m3u
OK=
for i in $( seq 5 ); do
	docker exec -u nobody movistar_u7d \
		/app/tv_grab_es_movistartv \
		--tvheadend /home/MovistarTV.m3u \
		--output /home/guide.xml \
		&& OK=1 && break
	sleep 10
	echo "$(date): reintentando..."
done
[ -n "$OK" ] || exit 1
docker exec -u nobody movistar_u7d /usr/bin/wget -qO /dev/null http://127.0.0.1:8889/reload_epg
