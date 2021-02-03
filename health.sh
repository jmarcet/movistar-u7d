#!/bin/sh

wget -U 'MICA-IP-STB' -qO- "http://www-60.svc.imagenio.telefonica.net:2001/appserver/mvtv.do?action=getCatchUpUrl&extInfoID=" | grep -q 'Missing TV program ID parameter'
