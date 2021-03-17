Movistar IPTV U7D - Flussonic Catchup proxy
===========================================

Qué es y de dónde nace
----------------------

Este proyecto nació del descontento de acceder a los canales de TV de Movistar a través de su [app de Movistar](https://play.google.com/store/apps/details?id=es.plus.yomvi), que dicho de forma elegante, está muy por detrás de la competencia.

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


Componentes
-----------

El resultado son dos microservicios escritos en python asíncrono, con [Sanic](https://github.com/sanic-org/sanic):

 - `movistar-u7d.py`: el proxy principal con el que se comunica el cliente final, como el TiviMate.

 - `movistar-epg.py`: otro miscroservicio en python asíncrono encargado de localizar el programa correspondiente al punto temporal que solicita el cliente. Mantiene el estado necesario para el servicio, permitiendo que el microservicio principal no tenga estado y pueda trabajar en múltiples hilos sin problemas.

 - `u7d.py`: pequeño script que mantiene abierta la reproducción de los programas de los últimos 7 días, habrá uno en ejecución por cada programa que se esté visionando, de consumo inapreciable.

 - `tv_grab_es_movistartv`: encargado de generar la lista de canales y la programación, así como de guardar una caché de los últimos 8 días de programas, de manera que necesita ser ejecutado de forma recurrente (cada 2h). Esta información es imprescindible para que todo el proceso funcione bien. Tanto TiviMate como cualquier repdoductor con catchup flussonic sólo se preocupan por canal y un timestamp, que define un momento preciso en el tiempo. El proxy es el encargado de encontrar qué programa de la EPG corresponde a ese canal en ese momento y negociar con Movistar la reproducción.

 - `env-example`: fichero fundamental. Contiene las variables de entorno con el que configurar las diferentes partes. Para usarlo con docker, basta con copiarlo a `.env` y hacer los cambios necesarios. La primera parte de estas variables sólo hacen falta para usarlo dentro del container, la segunda mitad en cambio contiene variables necesarias para ejecutarlo de un modo u otro.


Instalación
-----------

La forma más fácil de hacer funcionar todo es con Docker, docker-compose incluso mejor. Dentro del container queda todo lo necesario salvo el `udpxy`, que debe estar configurado para que pueda acceder a los canales de Movistar, y un `crontab`, es decir, algo que periódicamente llame al script `updateguide.sh` si usamos Docker, o `tv_grab_es_movistartv` directamente de alguna otra forma.

El resto de ficheros:

 - `updateguide.sh`: script de ejemplo para ejecutar desde el host, y de forma recurrente, si el servicio se ejecuta dentro del docker
 - `crontab`: crontab de ejemplo para ejecutar el script anterior, cada 2h, en el minuto 5. La frecuencia es así de alta para enterarse de los cambios de última hora que a veces sufre la programación.


En el caso de querer ejecutarlo todo directamente, pues:

```
pip3 install -r requirements.txt
```

Y a partir de ahi lanzarlo con algo como `start.sh`, que está pensado para el docker, pero sirve de ejemplo. Sin olvidarnos del contab anterior, la EPG siempre la necesitamos actualizada.

Cualquier duda o consulta no dudéis en abrir una [incidencia](https://github.com/jmarcet/movistar-u7d/issues) [aquí](https://github.com/jmarcet/movistar-u7d) en Github.


Uso
---

Sólo queda configurar el cliente. Para eso tenemos las siguientes URLs, donde 192.168.1.10 es una ip de ejemplo donde el microservicio `movistar-u7d.py` espera peticiones:

 - Canales: `http://192.168.1.10/channels.m3u` o `http://192.168.1.10/MovistarTV.m3u`

 - Guía de programación (EPG): `http://192.168.1.10/guide.xml`

Con configurar esas dos cosas debería ser suficiente. Aseguráos de que el TiviMate (o cliente IPTV con catchup Flussonic) guarda al menos 8 días de historial del EPG.


Futuro
------

Tenía pensado rescribirlo todo en Golang. Movistar, sin embargo, sigue añadiendo DRM a los canales, no contento con los propios de su plataforma, continúa con la TDT. En mi comunidad autónoma ya no se ven los regionales, así que le quedará poco tiempo de uso a todo esto lamentablemente.

[Aquí](https://comunidad.movistar.es/t5/Soporte-M-D-Yomvi/Por-favor-no-encripteis-los-canales-de-TDT/m-p/4437418#M107537) podéis ver un intento vano de hacerlos entrar en razón.

