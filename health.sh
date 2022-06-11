#!/bin/sh

eval `grep 'UA =' mu7d.py | sed -e 's:^.*UA = :export UA=:'`
wget -U "${UA}" -qO- "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do?action=getCatchUpUrl&extInfoID=" | grep -q 'Missing TV program ID parameter'
