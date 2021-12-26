#!/bin/sh

[ -n "${U7D_UID}" ] && _SUDO="s6-setuidgid ${U7D_UID}"
[ -e "${HOME}/.xmltv" ] || ${_SUDO} mkdir -p "${HOME}/.xmltv"

_tvgrab_log="${HOME}/.xmltv/tv_grab_es_movistartv.log"
${_SUDO} echo > "${_tvgrab_log}"

( while (true); do nc -z -v -w 1 172.26.23.3 53 2>/dev/null && break; sleep 1; done )
( while (true); do nice -n -10 ionice -c 2 -n 0 ${_SUDO} /app/movistar_epg.py; sleep 1; done ) &
( while (true); do nc -z -v -w 1 127.0.0.1 8889 2>/dev/null && break; sleep 1; done )
( while (true); do nice -n -15 ionice -c 1 -n 0 ${_SUDO} /app/movistar_u7d.py; sleep 1; done ) &

${_SUDO} tail -F "${_tvgrab_log}"

