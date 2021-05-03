#!/bin/sh

test -e "${HOME:-/home}/MovistarTV.m3u" || tv_grab_es_movistartv --m3u "${HOME:-/home}/MovistarTV.m3u"
OK=
for i in $( seq 5 ); do
	tv_grab_es_movistartv \
		--tvheadend "${HOME:-/home}/MovistarTV.m3u" \
		--output "${HOME:-/home}/guide.xml" \
		&& OK=1 && break
	sleep 10
	echo "$(date): reintentando..."
done
[ -n "$OK" ] || exit 1
wget -qO /dev/null http://127.0.0.1:8889/reload_epg
