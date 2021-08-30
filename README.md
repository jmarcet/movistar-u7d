Movistar IPTV U7D - Flussonic catchup proxy
===========================================

[![TiviMate Movistar](https://img.youtube.com/vi/WVHkjAZ1hBA/0.jpg)](https://www.youtube.com/embed/WVHkjAZ1hBA)

Uso
---

Ofrece compatibilidad completa con:

- En Android TV: [TiviMate](https://play.google.com/store/apps/details?id=ar.tvplayer.tv).

- En Android: [OTT Navigator IPTV](https://play.google.com/store/apps/details?id=studio.scillarium.ottnavigator)

- Multiplataforma: [Kodi](https://kodi.tv/) con [IPTV Simple PVR](https://github.com/kodi-pvr/pvr.iptvsimple)

Una vez instalado el servicio, tendremos las siguientes URLs disponibles, donde 192.168.1.1 será la IP donde funcione este proxy:

 - Canales: `http://192.168.1.1:8888/MovistarTV.m3u` `http://192.168.1.1:8888/canales.m3u` o `http://192.168.1.1:8888/channels.m3u`

Recuerda que si no usas docker, necesitar generar la lista de canales manualmente:

```
tv_grab_es_movistartv --m3u "${HOME:-/home}/MovistarTV.m3u"
ln -s MovistarTV.m3u "${HOME:-/home}/canales.m3u"
ln -s MovistarTV.m3u "${HOME:-/home}/channels.m3u"
```


 - Guía de programación (EPG): `http://192.168.1.1:8888/guide.xml`

Con configurar esas dos cosas debería ser suficiente. Aseguráos de que el cliente que uséis guarda al menos 8 días de historial del EPG. En el caso del `Kodi IPTV Simple`, además de añadir la lista de canales y la guía (sin caché) en los ajustes del addon, deberemos activar el Timeshift y el Catchup. Éste último en modo Default con 8 días de catchup y la opción de Reproducir desde la EPG en modo Live TV. De esta manera conseguiremos una experiencia de uso prácticamente idéntica a la del `TiviMate`, manteniendo la interfaz de LiveTV con el Catchup, con reproducción continua, etc.

 - Los canales serán accesibles en URLs como: `http://192.168.1.1:8888/{canal}/live`

Donde `canal` es el id del canal según la EPG. Podemos verlos en la lista de canales.

 - Se podrá acceder a cualquier instante de los últimos 7 días en `http://192.168.137.1:8888/{canal}/{timestamp}.{extension}`

Opcionalmente, el timestamp puede ir precedido de una palabra y/o seguido de una duración en segundos: `http://192.168.137.1:8888/{canal}/video-{timestamp}-{duracion}.{extension}`. Esto ha cambiado para tener mayor compatibilidad con diferentes clientes.

El timestamp es en el que queremos iniciar la reproducción, la duración no se usa, puede ser **0**, y la extensión puede tener cualquier valor. `OTT Nagivator IPTV` y `TiviMate` usan **.m3u8** mientras VLC necesita que sea **.mpeg** por ejemplo, o no reproduce bien los streams.


Qué es y de dónde nace
----------------------

Este proyecto nació del descontento de acceder a los canales de TV de Movistar a través de su [app de Movistar](https://play.google.com/store/apps/details?id=es.plus.yomvi), que dicho de forma elegante, está muy por detrás de la competencia.

 - No tiene ningún tipo de integración real con el Android, ni siquiera para la entrada de datos. Intentad usar un mini teclado, veréis lo _cómodo_ que es. Te dan ganas de llamar al soporte y decirles la categoría de software que venden con su producto.

 - Tiene un click molesto cada vez que pulsas un botón en el mando, no se puede deshabilitar.

 - El vídeo es de menor calidad que el que llega por la VLAN de tv, en lugar de a 50HZ es a 25, y se nota.

 - Se congela todo el tiempo, se cuelga.

 - Debido al magnífico DRM, en el siguiente video, el video nunca se ve: [Movistar+_20210320.mp4](https://openwrt.marcet.info/u7d/Movistar%2B_20210320.mp4)

Durante años usé [udpxy](http://www.udpxy.com/) para acceder a los directos desde cualquier cliente IPTV, aunque estuviera conectado por wifi. Con la lista de canales y programación que podías obtener con el fantástico [tv_grab_es_movistartv](https://github.com/MovistarTV/tv_grab_es_movistartv) no hacía falta nada más que un webserver para servirlo todo. Faltaba el acceso a los últimos 7 días.

Para esto, lo mejor que hubo durante mucho tiempo fue un [addon cerrado](https://sourceforge.net/projects/movistartv/) para el magnífico [Kodi](https://kodi.tv/), que funcionaba relativamente bien. Te permite reproducir programas de la parilla de los últimos 7 días; también te permite hacer grabaciones en local o en la nube, que son accesibles después para reproducir.

Tenía unos cuantos _peros_:
 - Aunque puedes reproducir un programa, no puedes avanzar, ni pausar, ni rebobinar, sólo reproducir y detener.
 - Con frecuencia la reproducción fallaba y aunque a veces podías continuar en el mismo punto, a menudo tenías que volver a repdoducirlo desde el principio.
 - Lo peor de todo eran los microcortes durante la reproducción, imposible de eliminar por completo.

En suma, era usable para grabaciones locales y para directos, para otros usos decepcionaba bastante.

A continuación de ésto, descubrí el [framework de entrada de TV del Android](https://source.android.com/devices/tv/), que es lo que usan todos los grandes fabricantes cuando ofrecen televisores con Android. De no tener SmartTV, hay diferentes TVboxes que hacen *smart* cualquier televisor con HDMI. El Android aporta las bases necesarias para poder acceder a cualquier tipo de contenido. Hay una aplicación simple de ejemplo que viene con el [AOSP](https://source.android.com/), la versión libre del Android que todos los que venden aparatos con Android utilizan. Además, se pueden usar otros clientes (que a su vez utilizan dicho framework). Por lo que sé, Sony y Phillips tienen sus propios clientes, aunque no los he probado.

Un poco más adelante, descubrí el que creo que es, si no el mejor, uno de los mejores clientes para acceder a la TV. El [TiviMate](https://play.google.com/store/apps/details?id=ar.tvplayer.tv).

Pasó a ser mi modo favorito de acceder a los canales, nada se le acercaba. Es super fluido, te permite hacer no solo PiP de lo que estás viendo, sino que puedes estar visionando hasta 9 canales simultáneos como si fuera un sistema de cámaras de seguridad.

Tan contento con él estaba que tenía que poder usar el resto de funcionalidad. Daba acceso a servicios de catchup (últimos 7 días), y lo hacía de dos maneras diferentes. Después de hacer pruebas, monitorear qué conexiones realizaba cuando intentabas ver algo de los últimos 7 días, ...

Se me ocurrió que podía hacer algún tipo de proxy entre dicho TiviMate con catchup flussonic y la IPTV de Movistar (de las variantes de catchup que soporta es el que más extendido he encontrado y a la vez es el más sencillo e intuitivo de implementar).

El resultado es algo así (el funcionamiento real es fluido todo el tiempo, el video se llega a atascar en los momentos de mayor tráfico de datos):

 - [TiviMate_Movistar_20210320_U7D-1.mp4](https://openwrt.marcet.info/u7d/TiviMate_Movistar_20210320_U7D-1.mp4)

 - [TiviMate_Movistar_20210320_U7D-2.mp4](https://openwrt.marcet.info/u7d/TiviMate_Movistar_20210320_U7D-2.mp4)


Componentes
-----------

El resultado son dos microservicios escritos en python asíncrono, con [Sanic](https://github.com/sanic-org/sanic):

 - `movistar_u7d.py`: el proxy principal con el que se comunica el cliente final, como el TiviMate.

 - `movistar_epg.py`: otro miscroservicio en python asíncrono. Encargado de actualizar la EPG y de localizar el programa correspondiente al punto temporal que solicita el cliente. Mantiene el estado necesario para el servicio, permitiendo que el microservicio principal no tenga estado y pueda trabajar en múltiples hilos sin problemas.

 - `vod.py`: pequeño script que mantiene abierta la reproducción de los programas bajo demanda (los últimos 7 días), habrá uno en ejecución por cada programa que se esté visionando. Encargado también de realizar las grabaciones con ffmpeg.

 - `tv_grab_es_movistartv`: encargado de generar la lista de canales y la programación, así como de guardar una caché de los últimos 8 días de programas, de manera que necesita ser ejecutado de forma recurrente (cada 2h). Esta información es imprescindible para que todo el proceso funcione bien. Tanto TiviMate como cualquier repdoductor con catchup flussonic sólo se preocupan por canal y un timestamp, que define un momento preciso en el tiempo. El proxy es el encargado de encontrar qué programa de la EPG corresponde a ese canal en ese momento y negociar con Movistar la reproducción.

Para Systemd:

 - `movistar_u7d.service`: script para el microservicio principal

 - `movistar_epg.service`: script para el microservicio que mantiene el estado (la EPG)


Para docker:
 - `env-example`: fichero con variables de entorno por si queremos modificar alguno de los valores por defecto o queremos hacerlo funcionar en el propio router. Con docker-compsoe lo copiamos a `.env` y hacemos los cambios necesarios.


Observaciones
-------------

 - Sólo hace falta tener contratada la fibra de Movista con acceso a la TDT, no es necesario ningún paquete de televisión.

 - La funcionalidad que más trabajo me dio conseguir y que más agradezco a la hora de usarlo es la reproducción continua. ¿Que qué es eso? Pues dado que Movistar da acceso a la programación de los últimos 7 días a partir de un identificador de canal y un identificador de programa, ambos incluidos en la EPG, a la hora de reproducir cualquier momento de la última semana se establece una negociación que te da acceso a reproducir **ese** programa, no el siguiente. De esta manera, tanto en la app oficial como en el addon cerrado, reproduces un programa y al acabar (normalmente sobre 1 o 2 minutos después del final) se detiene.

Con este servicio, en lugar de cortarse, se produce una mínima pausa, durante la que se detiene el sonido y la imagen queda congelada. Es durante un mínimo instante de tiempo, un par de segundos, por lo que aunque te das cuenta de que ha habido un cambio de programa, no molesta lo más mínimo. Así puedes ver toda la programación de un canal en diferido, el tiempo que quieras dentro de la última semana, sin más que iniciar la reproducción en el instante deseado.


Instalación
-----------

Si tenemos una máquina conectada por cable al router, podemos ejecutarlo sin mayores complicaciones.

 - Tenemos la opción de utilizar docker y docker-compose. Dentro del container queda casi todo lo necesario:

```
docker-copose up -d && docker-copose logs -f
```

Si, por el contrario, preferimos instalarlo y usarlo directamente:

 - Primero instalamos las dependencias de Python 3.8+:

```
pip3 install -r requirements.txt
```

 - Copiamos `movistar_u7d.py`, `movistar_epg.py`, `tv_grab_es_movistartv` y `vod.py` a alguna ruta que tengamos en el PATH:

```
cp movistar_u7d.py movistar_epg.py tv_grab_es_movistartv vod.py /usr/local/bin/
```

 - Si queremos usar systemd, copiamos los `.service` a `/etc/systemd/system`, ajustando las variables de entorno que queramos. Habilitamos los servicios y los iniciamos:

```
cp *.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable movistar_epg
systemctl enable movistar_u7d
systemctl start movistar_epg
systemctl start movistar_u7d
```

 - Sin systemd, tendremos que lanzar directamente los dos `movistar_u7d.py` y `movistar_epg.py`.

 - En cualquier caso, si lo usas sin docker, es importante que el proxy tenga acceso a las DNS de Movistar, 172.26.23.3

 - La primera vez tendremos que esperar a que termine de descargar la guía, ya que sin EPG no funcionará nada.

 - Puede ser necesario establecer algunas variables de entorno. Lás principales son `LAN_IP` e `IPTV_ADDRESS`. `LAN_IP` es la ip donde queremos alojar el servicio, por defecto escuchará en todos los interfaces de red. `IPTV_ADDRESS` corresponde a la ip de la vlan2 de Movistar, la que tiene el tráfico iptv; en caso de hacerlo funcionar dentro del propio router, será imprescindible.


Instalación en el propio router con docker-compose
--------------------------------------------------

Si queremos usarlo dentro del propio router, con su propia subred y con acceso a la VLAN de televisión, se complica todo un poco. De interesaros esta opción, [aquí](https://openwrt.marcet.info/latest/targets/x86/64/) podeís encontrar builds de openwrt para x86-64 con todo lo necesario para desplegar lo siguiente. Los actualizo cada pocos días.


Cualquier duda o consulta no dudéis en abrir una [incidencia](https://github.com/jmarcet/movistar_u7d/issues) [aquí](https://github.com/jmarcet/movistar_u7d) en Github.


Posibles problemas
------------------

A veces se desincroniza la guía entre el cliente (TiviMate) y el proxy, mostrando `Error 404` en todo o en casi todo. La solución pasa por ir a los ajustes del cliente (TiviMate), borrar la EPG y cargarla de nuevo.


Agradecimientos
---------------

Sin ningún orden en especial:

- [_WiLloW_](https://github.com/MovistarTV): por su [tv_grab_es_movistartv](https://github.com/MovistarTV/tv_grab_es_movistartv), un trabajo increíble que desenmaraña todos los metadatos de MovistarTV.

- [XXLuigiMario](https://github.com/XXLuigiMario): por su [u7d.py](https://github.com/XXLuigiMario/MovistarU7D) que fue, en cierta manera, el punto de partida de todo, aunque le faltase media funcionalidad. Partiendo de él y analizando el tráfico del [addon cerrado](https://sourceforge.net/projects/movistartv/), con todos sus fallos, conseguí entender e implemnetar correctamente la negociación rtsp de la que depende toda reproducción de los últimos 7 días.

- [Sanic Framework](https://sanicframework.org/): hace comodísima la programación de microservicios web.


Futuro
------

Movistar sigue añadiendo DRM a los canales, no contento con los propios de su plataforma, continúa con la TDT. En mi comunidad autónoma ya no se ven los regionales, así que le quedará poco tiempo de uso a todo esto lamentablemente.

[Aquí](https://comunidad.movistar.es/t5/Soporte-M-D-Yomvi/Por-favor-no-encripteis-los-canales-de-TDT/m-p/4437418#M107537) podéis ver un intento vano de hacerlos entrar en razón.

