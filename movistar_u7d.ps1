Start-Process python3 .\movistar_epg.py -NoNewWindow -RedirectStandardError $env:TMP\movistar_epg_err.log -RedirectStandardOutput $env:TMP\movistar_epg_out.log
sleep 5
Start-Process python3 .\movistar_u7d.py -NoNewWindow -RedirectStandardError $env:TMP\movistar_u7d_err.log -RedirectStandardOutput $env:TMP\movistar_u7d_out.log

tail -f $env:TMP\movistar_epg_err.log $env:TMP\movistar_epg_out.log $env:TMP\movistar_u7d_err.log $env:TMP\movistar_u7d_out.log $env:HOMEPATH\.xmltv\tv_grab_es_movistartv.log

