Movistar IPTV U7D - Flussonic catchup proxy
===========================================

[![TiviMate Movistar](../../raw/data/TiviMate_Video_Overlay.jpg)](https://openwrt.marcet.info/u7d/TiviMate_Movistar_20210320_U7D-1.mp4)


Qué es
------

 - Software para acceder a la red IPTV de televisión digital que [Movistar](https://www.movistar.es/particulares/internet/) ofrece con sus instalaciones de fibra, tanto directos como el archivo de 7 días, con cualquier cliente IPTV con soporte de catchup flussonic, tal como ofrecen Android, Android TV, iOS y casi todas las SmartTV.

 - Así mismo da acceso a las grabaciones que tengamos en la nube de [Movistar](https://www.movistar.es/particulares/internet/).

 - Por último, ofrece un sistema de grabaciones locales automáticas, o temporizadores, muy útiles para archivar programas o series emitidos.

[Movistar](https://www.movistar.es/particulares/internet/) cuenta con la plataforma de IPTV más potente e interesante de todo el mercado, se adhiere en general a estándares, lo que significa que funciona de una forma abierta. Si bien hoy toda la oferta de TV de pago de [Movistar](https://www.movistar.es/particulares/internet/) se transmite encriptada, los canales de TDT (con alguna excepción puntual) siguen siendo en abierto.

Por tanto, con sólo tener contratada la fibra de [Movistar](https://www.movistar.es/particulares/internet/) con acceso a la TDT, sin necesidad de ningún paquete de televisión, junto con este software, se tiene acceso a toda la parrilla de la TDT, con un archivo de 7 días, de una forma completamente diferente, donde el usuario tiene el control absoluto del contenido que ve o no ve con una comodidad inusual.

La perfección se consigue con una [NVIDIA Shield](https://amzn.to/2ZChwTk) o algún SmartTV con Android TV de gama alta, como puede tener SONY, aunque en algo bastante más barato como el [Xiaomi Mi Box S](https://amzn.to/3nGpN0Z) también se puede usar bastante bien.


Uso
---

Ofrece compatibilidad completa con:

1. En Android TV: [TiviMate](https://play.google.com/store/apps/details?id=ar.tvplayer.tv).

2. En Android: [OTT Navigator IPTV](https://play.google.com/store/apps/details?id=studio.scillarium.ottnavigator)

3. Multiplataforma: [Kodi](https://kodi.tv/) con [IPTV Simple PVR](https://github.com/kodi-pvr/pvr.iptvsimple)

Una vez instalado, tendremos las siguientes URLs disponibles, donde 192.168.1.10 sería la IP donde está funcionando el proxy:

 1. Canales: `http://192.168.1.10:8888/MovistarTV.m3u` `http://192.168.1.10:8888/Canales.m3u` o `http://192.168.1.10:8888/Channels.m3u`

 2. Guía de programación (EPG): `http://192.168.1.10:8888/guide.xml.gz` `http://192.168.1.10:8888/guia.xml.gz` `http://192.168.1.10:8888/guide.xml` o `http://192.168.1.10:8888/guia.xml`

Con estas dos ya tendremos acceso a los directos y a los últimos 7 días en cualquier cliente IPTV con soporte de catchup flussonic. De hecho, con configurar la primera suele ser suficiente, ya que la guía la saca de la propia lista de canales, que incluye un enlace a la misma.

![catchup1](../../raw/data/TiviMate_20211010_201536.png)
![catchup2](../../raw/data/TiviMate_20211010_201255.png)

También es posible acceder a las grabaciones que tengamos en la nube de [Movistar](https://www.movistar.es/particulares/internet/). De tener alguna grabación en dicha nube, tendremos disponibles una nueva lista de canales y una nueva guía de programación que, análoga a la general, incluirá solamente información de aquellos programas que tengamos grabados. El resultado es una forma muy cómoda de acceder a estas grabaciones en la nube.

 3. Canales con grabaciones en la nube de [Movistar](https://www.movistar.es/particulares/internet/): `http://192.168.1.10:8888/MovistarTVCloud.m3u`, `http://192.168.1.10:8888/Cloud.m3u` o `http://192.168.1.10:8888/Nube.m3u`

 4. Guía de programación de las grabaciones en la nube (EPG): `http://192.168.1.10:8888/cloud.xml` o `http://192.168.1.10:8888/nube.xml`

![cloud1](../../raw/data/TiviMate_20211010_195920.png)
![cloud2](../../raw/data/TiviMate_20211010_195949.png)

Por último, dispone de una funcionalidad de grabaciones locales automáticas, o temporizadores, y acceso a las mismas a través de una nueva lista de canales `.m3u` de tipo `VOD`, que resulta especialmente útil para almacenar de forma local programas y series, y así crear colecciones. El resultado es increíblemente cómodo de usar.

 5. Lista VOD de Grabaciones Locales: `http://192.168.1.10:8888/Recordings.m3u` o `http://192.168.1.10:8888/Grabaciones.m3u`

Ésta última sólo se activará cuando la variable de entorno `RECORDINGS` esté definida.

![recordings](../../raw/data/TiviMate_20211010_200145.png)

![overview](../../raw/data/TiviMate_20211010_200214.png)
![settings](../../raw/data/TiviMate_20211010_200007.png)

 6. Si tenemos además activa la opción `RECORDINGS_PER_CHANNEL`, en los canales de los que tengamos grabaciones realizadas, se generará una lista `m3u` por cada uno. Ésta será ligeramente diferente de las anteriores, más pensada para consumir con reproductores como `vlc` o `mpv`. Las encontraremos en la carpeta correspondiente a cada canal y podremos acceder a ellas con el nombre del canal sin espacios, el `hd` no es necesario: `http://192.168.1.10:8888/neox.m3u` o `http://192.168.1.10:8888/la2.m3u`

En todos los casos, las URL de las listas `m3u` son insensibles a mayúsculas o minúsculas.

 7. Métricas de utilización para [Prometheus](https://prometheus.io/docs/introduction/overview/): `http://192.168.1.10:8888/metrics`

Éstas contienen información en tiempo real de los clientes activos, separados por directos y catchups, y ordenados por la latencia inicial que tuvo cada stream para llegar. Perfectas para conectar con [Grafana](https://grafana.com/grafana/) y así poder visualizarlas y también tener un archivo histórico de uso:

![grafana-dashboard.json](../../raw/data/grafana.png)


Instalación en Windows
----------------------

Nos descargamos la última versión de [aquí](../../releases), la descomprimimos donde nos resulte más cómodo y del interior de la carpeta `movistar-u7d`, ejecutamos `mu7d.exe`.

Enseguida se abrirá una ventana de un terminal, donde aparecerán mensajes de información sobre todo lo que está haciendo.

![movistar-u7d-inicio](../../raw/data/movistar-u7d-inicio.png)

Por defecto la(s) lista(s) de canales y la(s) guía(s) las generará en vuestra carpeta de usuario, que o bien podemos abrir directamente, o podemos acceder a través de las [URLs normales](#uso). Podemos usar [VLC](https://www.videolan.org/vlc/) o [mpv](https://mpv.io/installation/) por ejemplo, que aunque no soportan en sí el catchup, es decir, no hay forma de seleccionar de forma cómoda programas de la parrilla, sí pueden reproducir todo. Mucho más completo resulta `Kodi` con `IPTV Simple` para usar en el ordenador.

![movistar-u8d-escritorio](../../raw/data/movistar-u7d-escritorio.png)

Existen varias [opciones de configuración](#configuración).


Instalación en Linux/UNIX/OS X
------------------------------

Podemos usar cualquier tipo de dispositivo, desde un ordenador tradicional a algo más sencillo como una raspberry, con Linux/UNIX/OS X como OS y conectado directamente al router (o con `igmpproxy` en el router). Un poco más abajo podéis ver las variables de entorno que se pueden ajustar para configurar el proxy, como activar las grabaciones, el control de ancho de banda automático, etc.

 1. Tenemos la opción de utilizar docker y docker-compose.

```
docker-compose up -d && docker-compose logs -f
```

Dentro del `docker` queda todo lo necesario y se ejecutará muy rápidamente, sólo tiene que descargar la última versión disponible [aquí](https://gitlab.marcet.info/javier/movistar-u7d/container_registry/2), no necesita generar nada. Un vez arranque sí, tardará unos minutos en generar la EPG y las listas de canales.

 2. Si por el contrario preferimos instalarlo y usarlo directamente:

 - Necesitamos los paquetes `git`, `libffi-dev` y `python3-pip` instalados con el gestor de paquetes de nuestra distro. Necesitamos por lo menos `Python 3.7` A continuación instalamos las dependencias:

```
pip3 install -r requirements.txt
```

 - Nos aseguramos de que los permisos de ejecución sean correctos. Copiamos [mu7d.py](mu7d.py), [movistar_u7d.py](movistar_u7d.py), [movistar_epg.py](movistar_epg.py), [movistar_vod.py](movistar_vod.py) y [movistar_tvg.py](movistar_tvg.py) a donde nos parezca adecuado. De no estar en el `PATH` tendremos que ajustar la ruta en el [mu7d.service](mu7d.service)

```
chmod +x mu7d.py movistar_u7d.py movistar_epg.py movistar_vod.py movistar_tvg.py
cp mu7d.py movistar_u7d.py movistar_epg.py movistar_vod.py movistar_tvg.py /usr/local/bin/
```

 - Para hacer grabaciones también necesitamos tener instalados tanto `ffmpeg` como `mkvtoolnix`.

 3. Para Systemd:

 - Copiamos [mu7d.service](mu7d.service) a `/etc/systemd/system`, lo habilitamos y lo iniciamos:

```
cp mu7d.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable mu7d
systemctl start mu7d
```

Sin systemd, podemos ejecutar directamente [mu7d.py](mu7d.py)


Configuración
-------------

 - La primera vez tendremos que esperar a que se genere la lista de canales y a que se descargue la guía. Sin playlist no hay canales y sin EPG no hay catchup. Tardará un buen rato.

 - Podemos controlar varios aspectos ajustando el fichero de configuración [mu7d.conf](mu7d.conf) que deberemos copiar al directorio `/etc` o al `$HOME`, en UNIX, o a nuestra carpetal personal en Windows.

   1. La más importante es `LAN_IP`, que corresponde a la ip que tendrán los canales de tv. Por defecto usa la ip local principal.

   2. `DEBUG`: para ejecutar el proxy con mucha información extra. Útil para depurar problemas, ver los canales encriptados que recibimos pero no son indexados, etc.

   3. `EXTRA_CHANNELS`: canales adicionales a los que queremos acceder. Si la lista de canales no existe, bien porque ejecutamos el proxy por primera vez o porque la borramos antes de ejecutarlo, podremos ver la lista de canales encriptados que se saltan. Los podemos añadir, separados por espacios, a esta variable. Por ejemplo `4917` o `5066` para `La 1 HD` en Madrid y en Galicia respectivamente, ambos encriptados.

   4. `IPTV_BW_SOFT` & `IPTV_BW_HARD`: límites de ancho de banda máximo que le perimitimos usar al proxy. Útil si se instala en el propio router, donde puede conocer el ancho de banda usado por la vlan de IPTV, o en un aparato independiente. Los valores máximos son de 90000/100000 Kbps. Esto consigue que no se produzcan cortes por saturación de la vlan de IPTV y se puedan hacer grabaciones en paralelo aprovechando todo el ancho de banda.

   5. `MP4_OUTPUT`: por defecto las grabaciones se harán en `.mkv`, conservando todas las pistas originales, con el audio `mp2` transcodeado a `aac` para que se puedan reproducir directamente con un navegador. Los subtítulos pueden dar incompatibilidad con algunos clientes, por lo que con esta variables definida obtendremos en su lugar un archivo `.mp4` sin subtítulos y un `.sub` adicional con los mismos si existen.

   6. `NO_SUBS`: si no queremos grabar subtítulos, tanto si es de forma general como para subsanar un problema puntual con un canal/programa que no se deja grabar. Hay 3 canales que emiten siempre con subtítulos de teletexto y estos resultan problemáticos a la hora de grabarse por lo que son descartados pero desconozco si hay más emisiones puntuales que los usan, para eso esta opción.

   7. `RECORDINGS`: define la ruta para las grabaciones. Si se define, permite usar la funcionalidad de temporizadores y hace que se exporte la lista de grabaciones locales. Las grabaciones se realizarán aquí y la lista de canales `VOD` se generará con todo el contenido desde aquí indexado. Esto quiere decir que puede ser útil para exponer una carpeta aunque no utilicemos los temporizadores.

   8. `RECORDINGS_PER_CHANNEL`: realizar las grabaciones organizadas en subdirectorios por canal. También activará la generación de un `m3u` en cada directorio de canal, con las grabaciones ordenadas de más antiguas a más nuevas.

   9. `RECORDINGS_THREADS`: número máximo de grabaciones locales simultáneas, por defecto 4. Si se establece `IPTV_BW` las grabaciones máximas también serán ajustadas dinámicamente. Si probáis un temporizador con algo que se repita mucho toda la semana, algo como `Los Simpson`, con la variable `IPTV_BW` definida con valor 85000, veréis como se lanzan en paralelo un montón de procesos `vod.py` y `ffmpeg`, un par por cada grabación. Podrás comprobar luego como todas las grabaciones están perfectas.

   10. `TVG_THREADS`: número de descargas paralelas al generar la EPG. Por defecto usa tantos hilos como CPUs detecta en el sistema, con un máximo de 8.

   11. `U7D_THREADS`: número de procesos simultáneos para el microservicio principal, por defecto 4.

   12. Para el resto, que es inusual necesitar cambiarlas, mirad directamente el fichero [mu7d.conf](mu7d.conf).

 - Si queremos hacer grabaciones, además de activar la opción `RECORDINGS` en el fichero anterior, también tendremos que copiar [timers.conf](timers.conf), ajustado a nuestro gusto, a nuestra carpeta de usuario.


Configuración de clientes
-------------------------

 - Bastará con añadir la, o las, lista(s) de canales. En general las guías no hace falta configurarlas por separado ya que son detectadas automáticamente a partir de las listas de canales.

 - Cada lista de canales será un proveedor de IPTV diferente, así tendremos uno para el catchup normal y otro para las grabaciones en la nube.

 - Por separado, y en caso de usar la funcionalidad de grabaciones, tendremos una lista de canales que tendremos que configurar como tipo `VOD`, es decir, en lugar de tener URLs de canales, tiene URLs con enlaces a grabaciones locales, servidas directamente por este proxy por HTTP.

 - A la hora de configurar cualquier cliente (TiviMate, OTT, Kodi u otro) es recomendable que en los ajustes del mismo desactivéis cualquier tipo de buffer de red que tenga, lo dejéis a 0.

 - De igual manera, es importante que lo configuréis para que la guía se recargue con la mayor frecuencia posible, y que no use caché si la ofrece (como el Kodi). El backend actualiza a cada hora en punto la guía general, y cada 5 minutos las grabaciones en la nube. La lista de grabaciones locales se indexa al ejecutarse el proxy y se actualiza a medida que se realizan grabaciones.

 - Por último, aseguraos de que el cliente que uséis guarda al menos 8 días de historial del EPG y en el caso de las Grabaciones en la Nube lo óptimo sería que no caducasen nunca. Desconozco el tiempo exacto que [Movistar](https://www.movistar.es/particulares/internet/) permite tener dichas grabaciones almacenadas. Grabaciones que no son más que eventos de la EPG que no se borran de la parrilla, así es como funcionan, de manera que las grabaciones se comparten entre los clientes.

 - En el caso del `Kodi IPTV Simple`, además de añadir la lista de canales y la guía (sin caché) en los ajustes del addon, deberemos activar el Catchup en modo Flussonic con 8 días de catchup y la opción de Reproducir desde la EPG en modo Live TV. De esta manera conseguiremos una experiencia de uso prácticamente idéntica a la del `TiviMate`, manteniendo la interfaz de LiveTV con el Catchup, con reproducción continua, etc.

![Kodi-PVR-General](../../raw/data/Kodi-PVR-General.png)
![Kodi-PVR-Guia](../../raw/data/Kodi-PVR-Guia.png)
![Kodi-IPTV_Simple-General](../../raw/data/Kodi-IPTV_Simple-General.png)
![Kodi-IPTV_Simple-EPG](../../raw/data/Kodi-IPTV_Simple-EPG.png)
![Kodi-IPTV_Simple-Catchup](../../raw/data/Kodi-IPTV_Simple-Catchup.png)


Posibles problemas
------------------

- La última versión del `TiviMate` por desgracia tiene un bug que hace que en la [NVIDIA Shield](https://amzn.to/2ZChwTk) no funcione para nada, pese a ser el software ideal con el que acceder. En otros dispositivos, funciona perfectamente.

Podéis encontrar un apk con la versión anterior buscando por [tivimate 3.9 apk](https://duckduckgo.com/?q=tivimate+3.9+apk)

Espero que se solucione pronto.

- En ocasiones, mientras se ve un diferido reciente, la transmisión da un salto temporal de forma repentina. La razón es un cambio en la programación que el cliente desconoce, debido a que actualiza la guía con muy poca frecuencia. El proxy descarga la EPG cada hora en punto, lo que parece ser suficiente, o en lugar de un salto la transmisión se detendría. Por tanto, lo ideal sería actualizar la EPG cada 20-30 minutos aunque no siempre es posible. Muchos clientes sólo permiten actualizarla cada 4 o más horas, así que están expuestos a sufrir la molestia.

  - Solución rápida: ir a los ajustes, borrar y recargar la guía.

  - Solución idónea: buscar que al cliente le añadan la posibilidad de actulizar la guía con mayor frecuencia.

- Cualquier duda o consulta, o si os encontráis con un cliente con catchup que os parece que debería funcionar con este proxy pero no lo hace (pienso en clientes incluidos en SmartTVs modernos), no dudéis en abrir una incidencia, ya sea en [Github](https://github.com/jmarcet/movistar_u7d) o en [Gitlab](https://gitlab.marcet.info/javier/movistar-u7d).


Grabaciones automáticas: Temporizadores
---------------------------------------

Existe una funcionalidad de grabaciones automáticas a partir de unos temporizadores que podremos definir con frases insensibles a mayúsculas, acentos y eñes y clasificadas por canales.

Por desgracia esta funcionalidad no es posible usarla de forma cómoda con el mando a distancia.

Será necesario establecer la variable de entorno `RECORDINGS`, en Windows editando el fichero [movistar-u7d.ps1](movistar-u7d.ps1), con la ruta donde queramos realizar las grabaciones y tendremos que editar el fichero [timers.conf](timers.conf) que deberá existir en el `$HOME`, o nuestra carpeta personal en Windows, con el siguente formato, llamado [TOML](https://toml.io/en/), que resulta fácil para los humanos:

```
default_language = "VO"
sync_cloud = true
sync_cloud_language = "VO"

[match]
657 = [
    "Alienigenas",
    "Area 51",
    "Extraterrestres",
    "Nasa",
    "Ovnis",
    "^Desmontando el cosmos$",
    "^El universo segun Stephen Hawking$",
]
1825 = [
    "^Cuarto Milenio$ ## Esp",
    "^Horizonte$ ## Esp",
]
4455 = [
    "Einstein",
    "Hawking",
    "La Luna",
    "Tesla",
    "Universo",
    "Basura espacial ## Esp",
    "Los planetas",
    "Documentos TV",
    "Sobrehumanos",
    "Marte",
    "Pluton",
    "Saturno",
    "Venus",
]
4714 = [
    "^American Dad$ ## 12:15",
    "^American Dad$ ## Esp ## 15:00",
    "^Los Simpson S(24|25|26|27|28|29|30|31|32|33)E\\d\\d[ $] ## Esp",
]
```

La primera clave, `language` es opcional. La podemos incluir para indicar que por defecto queremos que todas las grabaciones queden con la pista de audio que se recibe como secundaria como principal.

`match` contiene una lista por canales de cadenas de búsqueda, que constituyen los temporizadores. El número identificador de cada canal lo podemos ver en la lista general de los mismos.

Lás búsquedas se hacen de forma recurrente y las grabaciones se hacen no de los directos, sino del catchup. Esto permite hacerse con todos los episodios de cualquier serie o programa que queramos, de toda la última semana, con sólo añadir su nombre en el canal correspondiente en un fichero como el de arriba.

Cada cadena de texto es en realidad una [expresión regular](https://es.wikipedia.org/wiki/Expresi%C3%B3n_regular), como podéis ver en alguno de los ejemplos con símbolos extraños, aunque insensibles a acentos, eñes y mayúsculas. Lo más importante es poder poner "^" para indicar que debe ser el principio del título, y "$" para indicar el final.

Podemos definir por temporizador qué pista de audio queremos como principal. Si el temporizador finaliza con `" ## Esp"` o `" ## VO"`, estaremos indicando la pista que queremos, si la primera o la segunda. En el ejemplo de arriba se define la `VO` como comportamiento por defecto, es decir se especifica que las grabaciones deben tener como pista de audio principal la que llega como secundaria y, por separado, se definen varios temporizadores en los que se especifica que las grabaciones se hagan con la pista de audio normal como principal `" ## Esp"`.

Tenemos también la posibilidad de especificar a qué hora queremos grabar un programa. Igual que podemos especificar la pista de audio, separado por `" ## "`, podemos poner la hora y minuto de emisión del programa que nos interesa: `" ## HH:MM`. Esto se puede combinar con lo anterior, como se ve en los ejemplos de arriba. Si queremos grabar un programa en varios horarios diferentes, simplemente repetimos el temporizador, como se ve arriba. La hora que especificamos tiene cierta flexibilidad, serán incluídos todos los programas emitidos dentro de un margen de 10 minutos.

El orden de los dos campos anteriores, pista de audio y horario, no importa. Si repetimos el mismo, como poner dos veces un idioma o dos veces una hora, la última opción ganará. Y si añadimos más opciones, separadas por `" ## "`, serán ignoradas por completo.

Como se mencionó antes, las grabaciones se realizan desde el catchup, empezando después de que hayan finalizado y el EPG se haya actualizado una vez. Esto es con la intención de que cualquier cambio en el horario esté reflejado ya en la guía y así tener más posibilidades de grabar entero el evento. Esto también implica que se pueden reintentar en caso de problemas, por lo que las grabaciones se comprueban antes de ser archivadas como correctas. En caso de cualquier error, se reintentan poco después. De esta manera, podemos confiar en que todo lo grabado quede perfecto, sin el más mínimo error.

El control de qué grabaciones se reintentan, o no, viene determinado por el fichero `recordings.json` que se generará automáticamente y se actualizará con cada grabación hecha. Si queremos que una grabación existente se sobreescriba, bastará con eliminarla de este `recordings.json` y reiniciar [movistar_epg.py](movistar_epg.py). Si el temporizador que la generó sigue existiendo en el fichero [timers.conf](timers.conf), la grabación se repetirá poco después, hasta que sea satisfactoria. En el caso de resultar incompleta tres veces seguidas pero con la misma duración total las tres veces, se archivará como correcta.

Por último, las grabaciones se realizan en carpetas con el nombre de la serie si lo son. En caso de programas periódicos de noticias, no clasificados como series, serán grabados en una carpeta con el nombre del programa y cada grabación llevará añadida la fecha, de manera que se puedan grabar noticias diarias. En todos los casos, las grabaciones serán expuestas en la `.m3u` de grabaciones locales: `recordings.m3u` o `grabaciones.m3u`.

Si se activa la opción `RECORDINGS_PER_CHANNEL` irán además dentro de una carpeta por cada canal.

Ante cualquier duda sobre qué temporizador está iniciando una grabación, activando la opción de `DEBUG` se podrá ver con detalle.


Cómo funciona
-------------

 - Los canales son accesibles en URLs como: `http://192.168.1.10:8888/{nombre_canal}.ts` o `http://192.168.1.10:8888/{numero_canal}/mpegts`

El nombre de canal debe ir sin espacios, podemos evitar tanto las palabras `HD` como `TV`, y es insensible a mayúsculas/minúsculas. Así podemos ver el directo de `Neox HD` accediendo a `http://192.168.1.10:8888/neox.ts`

El número de canal es el interno, tal como figuran en la lista de canales principal.

 - Se puede acceder a cualquier instante de los últimos 7 días en URLs como: `http://192.168.1.10:8888/{canal}/{YYYYMMDDHHMM}`, `http://192.168.1.10:8888/{canal}/{YYYYMMDDHHMMSS}` y `http://192.168.1.10:8888/{canal}/{timestamp}`

Aquí como canal podemos usar tanto el nombre como el número interno, con las mismas premisas para el nombre que en los directos.

 - Y a las grabaciones en la nube de [Movistar](https://www.movistar.es/particulares/internet/), con mucho más tiempo de almacenado: `http://192.168.1.10:8888/cloud/{canal}/{YYYYMMDDHHMM}`, `http://192.168.1.10:8888/cloud/{canal}/{YYYYMMDDHHMMSS}` y `http://192.168.1.10:8888/cloud/{canal}/{timestamp}`

 - Opcionalmente, el timestamp puede ir precedido de una palabra y/o seguido de una duración en segundos así como de una extensión: `http://192.168.1.10:8888/{canal}/{palabra}-{timestamp}-{duracion}.{extension}`. Esto es para tener la mayor compatibilidad posible con diferentes clientes.

 - Adicionalmente, la funcionalidad de realizar grabaciones es accesible en el endpoint: `/record/{canal}/{timestamp}?cloud=1&mp4=1&time=1200&vo=1` donde en lugar del timestamp también valen los formatos de fecha `{YYYYMMDDHHMM}` y `{YYYYMMDDHHMMSS}` y los parámetros, todos opcionales, indican:

   1. `cloud=1`: queremos grabar en local una grabación existente en nuestra nube de Movistar.
   2. `mp4=1`: queremos la grabación en un fichero `.mp4` sin subtítulos más un fichero `.sub` en caso de tenerlos.
   3. `time=segundos`: tiempo en segundos que deseamos grabar. El límite actual será el de la duración del programa de catchup en cuestión, es decir, si le indicamos que grabe más allá de lo que dura un programa, se detendrá tan pronto como [Movistar](https://www.movistar.es/particulares/internet/) corte la transmisión del mismo. Si bien en ocasiones las transmisiones se extienden, lo habitual es que duren un minuto por encima de la duración que figura en la EPG.
   4. `vo=1`: queremos la pista de audio secundaria, habitualmente con audio en versión original, como pista de audio principal.

Esto nos permite grabar con facilidad pequeños fragmentos de programas o programas enteros. No vale, sin embargo, para grabar directos ni varios programas seguidos. Para directos siempre tenemos la opción de guardar la dirección del canal directo con algo como `wget`.

Las grabaciones llevan las pistas de audio `mp2` transcodeadas a `aac` para que se puedan reproducir sin problemas con un navegador como el Chrome.

Todas ellas quedan almacenadas con el nombre del programa, carátula, etc, tal como sucede con los temporizadores.

 - Por último, las grabaciones son accesibles desde `http://192.168.1.10:8888/recording/?{fichero}`

Donde el fichero se buscará, con ruta incluida, en la carpeta indicada por `RECORDINGS`. Se devuelve por HTTP con `content-range`, por lo que debería ser reproducible con cualquier tipo de cliente.


Componentes
-----------

Este software consta de cuatro partes. Las dos primeras son dos microservicios escritos en python asíncrono, con [Sanic](https://github.com/sanic-org/sanic), el tercero es también asíncrono pero sin [Sanic](https://github.com/sanic-org/sanic), al menos cuando se ejecuta como script independiente, y el último se encarga de descargar la EPG y generar las listas de canales y guías de programación. El extra es un fichero de ejemplo para montarnos gráficas estadísticas con [Grafana](https://grafana.com/grafana/):

 - [movistar_u7d](movistar_u7d.py): el microservicio principal y fachada del proxy con el que se comunica el cliente final, como el TiviMate. Se ejecuta en varios procesos simultáneos, número determinado por la variable de entorno `U7D_THREADS` que por defecto es 4.

 - [movistar_epg](movistar_epg.py): el segundo microservicio escrito con [Sanic](https://github.com/sanic-org/sanic). Mantiene el estado necesario para que el anterior sea funcional puro y pueda ejecutarse en múltiples procesos sin problemas. Está encargado de generar y actualizar las listas de canales y de grabaciones y las guías de programación, así como de gestionar los temporizadores de las grabaciones locales. También se encarga de lás métricas [Prometheus](https://prometheus.io/docs/introduction/overview/), aunque sean luego servidas por el anterior.

 - [movistar_vod](movistar_vod.py): contiene el código responsable de negociar con [Movistar](https://www.movistar.es/particulares/internet/) el acceso a los programas de catchup. Así mismo, se ejecuta como script independiente para realizar las grabaciones con [ffmpeg](https://ffmpeg.org/).

 - [movistar_tvg](movistar_tvg.py): encargado de generar las listas de canales y las guías programación, así como de guardar una caché de los últimos 7 días de la EPG, de manera que se ejecuta de forma recurrente (cada hora en punto para la guía general, cuando es necesario para el resto de los casos).

Esta caché que genera el `movistar_tvg` es imprescindible para que todo el proceso funcione bien. Los clientes con catchup flussonic, como el TiviMate, sólo se preocupan por el canal y un timestamp, que define un momento preciso en el tiempo. El proxy es el encargado de encontrar qué programa de la EPG corresponde a ese canal en ese momento y negociar con [Movistar](https://www.movistar.es/particulares/internet/) la reproducción, por lo que esta caché, que sirve de índice, resulta esencial para que todo pueda funcionar.

 - [grafana-dashboard.json](grafana-dashboard.json): dashboard para [Grafana](https://grafana.com/grafana/) con dos paneles de catchups y directos. Para usar conectado con las métricas [Prometheus](https://prometheus.io/docs/introduction/overview/). Lo podéis ver en el último pantallazo un poco más arriba.


De dónde nace
-------------

Este proyecto nació del descontento de acceder a los canales de TV de [Movistar](https://www.movistar.es/particulares/internet/) a través de su [app de Movistar](https://play.google.com/store/apps/details?id=es.plus.yomvi), que dicho de forma elegante, está muy por detrás de la competencia.

 - No tiene ningún tipo de integración real con el Android, ni siquiera para la entrada de datos. Intentad usar un mini teclado, veréis lo _cómodo_ que es. Te dan ganas de llamar al soporte y decirles la categoría de software que venden con su producto.

 - Tiene un click molesto cada vez que pulsas un botón en el mando, no se puede deshabilitar.

 - El vídeo es de menor calidad que el que llega por la VLAN de TV, en lugar de a 50HZ es a 25, y se nota.

 - Se congela todo el tiempo, se cuelga.

 - Debido al magnífico DRM, en el siguiente video, el video nunca se ve: [Movistar+_20210320.mp4](https://openwrt.marcet.info/u7d/Movistar%2B_20210320.mp4)

Durante años usé [udpxy](http://www.udpxy.com/) para acceder a los directos desde cualquier cliente IPTV, aunque estuviera conectado por wifi. Con la lista de canales y programación que podías obtener con el fantástico [tv_grab_es_movistartv](https://github.com/MovistarTV/tv_grab_es_movistartv) no hacía falta nada más que un servidor web para servirlo todo. Faltaba el acceso a los últimos 7 días.

Para esto, lo mejor que hubo durante mucho tiempo es un [addon cerrado](https://sourceforge.net/projects/movistartv/) para el magnífico [Kodi](https://kodi.tv/), que funciona relativamente bien (con Kodi 18, no lo hay para Kodi 19). Te permite reproducir programas de la parrilla de los últimos 7 días; también te permite hacer grabaciones en local o en la nube, que son accesibles después para reproducir.

Tenía unos cuantos _peros_:
 - Aunque puedes reproducir un programa, no puedes avanzar, ni pausar, ni rebobinar, sólo reproducir y detener.
 - Con frecuencia la reproducción fallaba y aunque a veces podías continuar en el mismo punto, a menudo tenías que volver a reproducirlo desde el principio.
 - Lo peor de todo eran los micro cortes durante la reproducción, imposible de eliminar por completo.

En suma, era usable para grabaciones locales y para directos, para otros usos decepcionaba bastante.

A continuación de ésto, descubrí el [framework de entrada de TV del Android](https://source.android.com/devices/tv/), que es lo que usan todos los grandes fabricantes cuando ofrecen televisores con Android TV. De no tener SmartTV, hay diferentes TVboxes que hacen *smart* cualquier televisor con HDMI. Personalmente uso la [NVIDIA Shield](https://amzn.to/2ZChwTk) y el [Xiaomi Mi Box S](https://amzn.to/3nGpN0Z). El Android aporta las bases necesarias para poder acceder a cualquier tipo de contenido. Hay una aplicación simple de ejemplo que viene con el [AOSP](https://source.android.com/), la versión libre del Android que todos los que venden aparatos con Android utilizan. Además, se pueden usar otros clientes (que a su vez utilizan dicho framework). Por lo que sé, Sony y Phillips tienen sus propios clientes, aunque no los he probado.

Un poco más adelante, descubrí el que creo que es, si no el mejor, uno de los mejores clientes para acceder a la TV. El [TiviMate](https://play.google.com/store/apps/details?id=ar.tvplayer.tv).

Pasó a ser mi modo favorito de acceder a los canales, nada se le acercaba. Es super fluido, te permite hacer no solo PiP de lo que estás viendo, sino que puedes estar visionando hasta 9 canales simultáneos como si fuera un sistema de cámaras de seguridad.

Tan contento con él estaba que tenía que poder usar el resto de funcionalidad. Daba acceso a servicios de catchup (últimos 7 días), y lo hacía de dos maneras diferentes. Después de hacer pruebas, monitorear qué conexiones realizaba cuando intentabas ver algo de los últimos 7 días, ...

Se me ocurrió que podía hacer algún tipo de proxy entre dicho TiviMate con catchup flussonic (de las variantes de catchup que soporta es el que más extendido he encontrado y a la vez es el más sencillo e intuitivo de implementar) y la IPTV de Movistar.

A día de doy alterno constantemente entre clientes, cada uno tiene sus pros y sus contras, ninguno es perfecto. Lo ideal de este proxy es que te abre un mundo de posibilidades para acceder a la IPTV, de forma estable y sin cortes.


Agradecimientos
---------------

Sin ningún orden en especial:

- [_WiLloW_](https://github.com/MovistarTV): por su [tv_grab_es_movistartv](https://github.com/MovistarTV/tv_grab_es_movistartv), un trabajo increíble que desenmaraña todos los metadatos de MovistarTV.

- [XXLuigiMario](https://github.com/XXLuigiMario): por su [u7d.py](https://github.com/XXLuigiMario/MovistarU7D) que fue, en cierta manera, el punto de partida de todo, aunque le faltase media funcionalidad. Partiendo de él y analizando el tráfico del [addon cerrado](https://sourceforge.net/projects/movistartv/), con todos sus fallos, conseguí entender e implementar correctamente la negociación rtsp de la que depende toda reproducción de los últimos 7 días.

- [Sanic Framework](https://sanicframework.org/): hace comodísima la programación de microservicios web.

- [Movistar](https://www.movistar.es/particulares/internet/): por ofrecer fantásticas opciones de conexión por fibra con TV con todos sus canales como streams IPTV estándar y de hacer accesible también su servicio de catchup. Disponéis de una red arquitecturada con una calidad increíble. Lástima que sólo ofrezcáis acceder a través de una app y un decodificador bastante lamentables, sin duda no a la altura del resto de vuestra infraestructura y oferta.


Futuro
------

De momento [Movistar](https://www.movistar.es/particulares/internet/) se ha detenido en seguir encriptando sus emisiones en TDT. Hay algún canal que no se puede ver, pero la mayoría sí. Esperemos que no empeore.

[Aquí](https://comunidad.movistar.es/t5/Soporte-M-D-Yomvi/Por-favor-no-encripteis-los-canales-de-TDT/m-p/4437418#M107537) podéis ver un intento (vano?) de hacerlos entrar en razón. Desde que lo escribí no han encriptado ningún canal adicional.


Donar con PayPal
----------------

Si te gusta mucho usar este software y te apetece invitarme a tomar algo:

[![Donar con Paypal](https://www.paypalobjects.com/es_ES/ES/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/donate?business=83ZPHF38QMQSE&no_recurring=0&currency_code=EUR)
