## Nueva versión v6.0

- Actualizado a `Python 3.12`, con mucho mejor rendimiento.

- `ffmpeg` actualizado a la versión 6

- Las imágenes de docker ahora usan `debian-slim` y [jellyfin-ffmpeg](https://github.com/jellyfin/jellyfin-ffmpeg), con mucho mejor rendimiento que con `Alpine` y con aceleración de la codificación/decodificación de video tanto en `ffmpeg` como en `comskip`, con soporte de Intel QSV, AMD, Nvidia, Raspberry, ...
  - Las imágenes de docker están disponibles para `amd64, `arm64` y `arm`, con nuevas etiquetas `stable` y `unstable` y nuevas versiones `slim`, `stable-slim` y `unstable-slim`, sin `ffmpeg`/`ffprobe`/`comskip` y mucho más pequeñas.
  - Para Windows el `comskip` se debe [descargar](https://www.kaashoek.com/files/) por separado y copiarlo o bien junto a `mu7d` o bien en una ruta que esté en el `PATH`, es decir, que con escribir `comskip` en un `CMD`/`PowerShell` lo encuentre y ejecute.

- Sistema de mantenimiento de la caché de la EPG reescrito. Ya no debería haber huecos ni errores.

- Actualizaciones de la EPG mucho más rápidas.

- Nueva playlist y guía de grabaciones locales, `MovistarTVLocal.m3u` & `local.xml`, accesible así igual que las grabaciones en la Nube de Movistar. `OTT Navigator` es la mejor alternativa para acceder a las mismas.

- Logs muy mejorados, con nueva opción para grabarlos en un fichero.

- Los canales se identifican no sólo por número sino también por nombre, con sólo incluir la parte necesaria del nombre que lo haga inconfundible con los demás canales.

- Reproducción de canales SD desde Jellyfin arreglada.

- Transcode transparente (del audio) cuando el cliente es `Chrome`, así se pueden reproducir directos o catchups directamente desde el navegador.

- El formato por defecto de las grabaciones es ahora `.ts`, con `.mkv` como opción. Ya no hay opción de `.mp4`.

- Ante cambios de numeración de canales por parte de Movistar, si existen grabaciones del mismo se renombran.

- Los temporizadores de grabaciones ahora se lanzan 30 segundos tras iniciarse la emisión.

- Cuando una grabación tiene un retraso de emisión mientras está en marcha, en lugar de ser cancelada se elimina el tiempo retrasado del principio. En caso de adelantarse sí se cancela y se repite.

- Mensajes de diagnóstico en caso de error mejorados:
  - IPTV de Movistar no accesible.
  - Multicast del IPTV de Movistar no accesible.

- Las grabaciones se hacen ahora en python puro, con `ffmpeg`, `ffprobe` y `comskip` reservado al postprocesado. Resultan así mucho más eficientes en recursos y también permiten hacer transcoding configurable en `mu7d.conf`.

- Si se establete `RECORDINGS_TMP`, las portadas de las grabaciones se cachean dentro de la carpeta `covers`, con la idea de no tener que despertar el HDD donde se almacenan las grabaciones con sólo moverse por la programación disponible.

- Opciones de configuración nuevas o modificadas:
  - `COMSKIP`: estableciéndola se activa el `comskip` y permite controlar los argumentos usados para lanzarlo.
  - `DROP_CHANNELS`: nueva opción para bloquear canales indeseados de la EPG y listas de canales.
  - `LOG_TO_FILE`: para escribir los logs en un fichero.
  - ~~`MP4_OUTPUT`~~: opción eliminada. Por defecto las grabaciones se realizan en formato `.ts`, el único en el que los subtítulos funcionan correctamente con cualquier reproductor.
  - `MKV_OUTPUT`: para hacer las grabaciones en formato `.mkv`. No es deseable de querer usar la nueva `MovistarTVLocal.m3u`/`local.xml`.
  - `RECORDINGS_M3U`: activada por defecto, para controlar si generar listas `m3u` de grabaciones. Con `Jellyfin` es recomendable desactivarla, o se quejará de que no puede localizar las grabaciones.
  - ~~`RECORDINGS_PER_CHANNEL`~~: opción eliminada. Las grabaciones siempre se hacen organizadas en una carpeta por canal.
  - `RECORDINGS_REINDEX`: regenerar índice de grabaciones, `recordings.json`, a partir de los metadatos guardados con las grabaciones, `-movistar.nfo`. Pensado para ser usado de forma controlada y a continuación ser desactivado.
  - `RECORDINGS_TMP`: para realizar las grabaciones y el postprocesado en almacenamiento rápido (SDD, NVME), copiándose al final a `RECORDINGS` que puede ser almacenamiento lento (HDD).
  - `RECORDINGS_TRANSCODE_INPUT` & `RECORDINGS_TRANSCODE_OUTPUT`: permiten controlar el transcoding de (entrada y salida de) las grabaciones.
  - `RECORDINGS_UPGRADE`: actualiza los ficheros de metadatos de las grabaciones `-movistar.nfo`. Es necesario activarlo una vez para indexar correctamente grabaciones hechas con versiones anteriores a la v6.
  - ~~`TVG_THREADS`~~: opción eliminada. La EPG se descargará siempre con 8 hilos de ejecución.

- Nuevas opciones globales en `timers.conf`:
  - `comskip`: detectar anuncios y grabar capítulos en fichero `.ffmeta`.
  - `comskipcut`: detectar y eliminar anuncios de las grabaciones.

- Nuevas opciones por temporizador en `timers.conf`:
  - `ComSkip`: detectar anuncios y guardar fichero `.ffmeta` con los capítulos. Script para `mpv` en `mpv/ffmeta.lua`.
  - `ComSkipCut`: detectar anuncios y eliminarlos.
  - `NoComSkip`: no hacer detección de anuncios cuando está activo de forma global.
  - `Keep#`: conserva las # últimas grabaciones.
  - `Keep#d`: conserva las grabaciones de los últimos # días.
  - `Repeat`: repetir grabación aunque ya esté almacenada, eliminando la anterior.
  - `@XXXXX--`: permite controlar los días de la semana en los que el temporizador se activa.

- Muchas otras pequeñas mejoras.


## Nueva versión v5.2

- Arreglado problema por cambio absurdo en los datos de la EPG de Movistar.

- Mejorado todo el sistema de detección de la IPTV de Movistar y de la información que se muestra al usuario en caso de problemas.

- Añadido canal "Nick Ukraine".

- Otras pequeñas mejoras.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v5.1...V5.2


## Nueva versión v5.1

- Arreglado problema de reproducción con el Kodi debido a un intento incorrecto de optimización.

- Arreglado y mejorado el sistema de actualización de las fechas de modificación de las grabaciones: al iniciarse se actualizan las listas m3u y los directorios de los canales y al acabar cada grabación se actualizan las carpetas afectadas, incluidas la del programa grabado y la de metadatos; siempre con la fecha de la emisión más reciente que contengan. De esta manera resulta muy fácil identificar la antigüedad de las grabaciones.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v5.0...V5.1


## Nueva versión v5.0

Tras una extensa limpieza y optimización cuando no reescritura por completo de todo el proxy, y tras pruebas prolongadas tanto en UNIX como en Windows, por fin está disponible la versión **5.0**. Las novedades son numerosas:

- Nueva forma de ejecutar y configurar el proxy, a través de `mu7d.exe` y `mu7d.conf`. Para ejecutar el proxy simplemente hacemos doble click en `mu7d.exe` y para configurarlo copiamos `mu7d.conf` a nuestra carpeta personal y hacemos los cambios que veamos oportunos. Los componentes han sido todos renombrados.

- Ahora al cerrar la ventana que se abre al ejecutar el proxy se cerrarán por completo todos los componentes del mismo, cancelando de forma limpia los clientes existentes que puedan existir (antes el Kodi se quedaba colgado) como también realizando una limpieza completa de aquellas grabaciones que pudiera haber en marcha.

- Arreglado problema de compatibilidad con Windows anteriores al 11.

- Informa directamente de problemas de conectividad con la IPTV de Movistar así como de conflicto con otros programas que puedan estar usando los mismos puertos.

- Actualiza la EPG al iniciar si es muy antigua, pero permite su uso inmediato.

- Nueva variable EXTRA_CHANNELS con la que poder configurar canales encriptados a los que queremos tener acceso, bien sea por el audio que no es encriptado o por cualquier otro motivo. La primera vez que ejecutemos el proxy, o si lo hacemos después de borrar la lista de canales `MovistarTV.m3u`, podremos ver la lista de canales que recibimos en nuestra suscripción pero son descartados por estar encriptados.

- Simplificada de forma importante la negociación del acceso a los programas de los últimos 7 días así como de las grabaciones en la nube. Debería ser algo más rápido, el código es infinitamente más simple, pero podría darse el caso de programas que se cortan poco después de empezar. No los he encontrado desde hace tiempo.

- Nueva funcionalidad para sincronizar grabaciones en la nube a local. Con tener `sync_cloud = true` en el `timers.conf` y `RECORDINGS` con una ruta donde hacer las grabaciones en `mu7d.conf`, ambos en nuestra carpeta personal, se sincronizarán todas las grabaciones que tengamos en la nube como grabaciones locales.

- Las grabaciones en Windows tienen ahora las mismas características que en UNIX, haciendo una cola en el postprocesado y funcionando con mínima prioridad, para no saturar el sistema.

- Las grabaciones quedan con la fecha y hora de la emisión original. Igualmente, al iniciar el proxy, se sincronizarán las fechas de modificación de todos los ficheros asociados a ellas, que en casos como el de integración con Jellyfin resulta muy útil.

- Los temporizadores configurables en `timers.conf` son ahora insensibles a acentos, eñes o diéresis.

- No se repiten grabaciones del mismo programa emitido en la misma semana de forma reiterada. Esto sucede con relativa frecuencia.

- Cuando se graba una reemisión de un programa con el título extendido, como sucede con Cuarto Milenio que primero se estrena sin título específico y posteriormente se reemite con título propio, ahora la copia antigua será borrada automáticamente.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v4.8.2...V5.0


## Nueva versión v4.8.2

- Arreglado problema para clientes suscritos a la oferta completa de Movistar.

- Incluido el canal `4919`, `La 2 HD` en Madrid.

- Canal `5066`, `La 1 HD`, eliminado de la lista de canales sin encriptar.

- Añadida nueva variable de entorno `EXTRA_CHANNELS` con la que podemos añadir canales que no están en la lista de canales en abierto. Por ejemplo, `La 1 HD`, que ha pasado a estar encriptada recientemente, podremos tenerla añadiendo `4917` o `5066` si estamos en Madrid o en Galicia respectivamente. No estoy seguro del resto de zonas.

Ejecutando el proxy con `DEBUG=1` veremos todos los canales que no son incluidos por no estar en la lista de canales en abierto. Puede ser que simplemente en tu comunidad alguno cambie de número y se reciba sin encriptar. Si me avisáis de este último caso, los añado a la lista de canales sin encriptar.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v4.8...v4.8.2


## Nueva versión v4.8

- Primera versión para Windows que funciona fuera de mi ordenador. Había un problema con Sanic, que necesita acceso a sus fuentes para funcionar. Ahora se incluyen en el archivo.

- La URL para cada canal hasta ahora necesitaba el número interno del canal, por lo que era necesario consultarlo en la lista de canales. Ahora podemos usar el nombre del canal, sin importar mayúsculas o minúsculas ni las palabras `HD` o `TV`.

- La guía se actualizará nada más iniciar no sólo la primera vez que se arranca el programa sino también si no ha recibido la última actualización, que se realizan a cada hora en punto.

- Los temporizadores ahora pueden incluir la hora de emisión que estamos interesados en grabar, útil para programas que se repiten a otras horas. Grabará aquella cualquier emisión que coincida con lo hora especificada con un margen de 15 minutos.

- Algunos canales se transmiten con subtítulos de tipo teletexto y no se podían grabar: `Boing`, `DKISS` y `Energy`. Ahora se graban sin subtítulos.

- Nueva variable de configuración `NOSUBS` para forzar grabaciones sin subtítulos, por si no se queiren o se tiene problemas con subtítulos de teletexto en algún otro programa o canal.

- Nueva variable de configuración `RECORDINGS_PER_CHANNEL` para que las grabaciones se organicen en una carpeta por canal.

- Cuando la variable anterior esté activa se generará una lista de grabaciones por canal dentro del directorio del canal, ordenados por fecha de emisión.

- La lista `m3u` de grabaciones ahora se almacena en el directorio de grabaciones.

- Los ficheros `mkv` de las grabaciones ahora incluyen empotrada la imagen de portada. Así programas como VLC muestran ahora portada por cada grabación.

- Muchos otros detalles internos.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v4.6.2...v4.8


## Nueva versión v4.6.2

Nueva versión con novedades importantes:

- Totalmente compatible y funcional con Kodi con el addon iptv simple. Arreglado el problema que había de compatibilidad y también simplificada la lista de canales, que ya no necesita ninguno de los añadidos que tenía específicamente para el Kodi.

- "La 1 HD" ha cambiado de número interno, vuelve a estar en la parrilla de canales.

- Completamente reestructurada la corrección de errores de la EPG, tanto de la descargada de forma progresiva como de la caché que se guarda de los últimos 7 días.

- Sistema de grabaciones completamente renovado.

- El formato del índice de grabaciones `recordings.json` ha cambiado ligeramente. Ahora junto con el título se guarda el timestamp, es decir, la fecha exacta de emisión, mucho más útil que el identificador interno de programa que usa Movistar que no deja de ser algo completamente efímero.

- Las grabaciones llevan ahora como título, en los metadatos, el nombre que llevan en disco. Esto hace mucho más sencillo gestionar las grabaciones con sistemas externos como [Jellyfin](https://jellyfin.org/).

- Muchos otros detalles internos.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v4.3...v4.6.2


## Nueva versión v4.3

- Gran reestructuración interna.

- Como gran novedad, ahora intenta obtener una copia actualizada de la caché de la EPG la primera vez que se usa. De esta manera podremos acceder a los últimos 7 días completos desde el principio y estará listo para usar de forma mucho más rápida.

- Arreglado problema con grabaciones incompletas archivadas como correctas.

- Eliminados todos los `DeprecationWarning`.

- Los logs ahora son algo más informativos y homogéneos.

- Otros pequeños problemas corregidos.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v4.2...v4.3


## Nueva versión v4.2

- Arreglado problema de codificación en la generación de la guía y el bug por él introducido en la última versión.

- Arreglado problema para acceder a grabaciones en la nube de Movistar.

- Arreglado problema al usarlo por primera vez.

- Script de lanzamiento reescrito, más simple y efectivo.

- Logs mejorados, mucho más informativos y homogéneos.

- Mejoras internas generales.

**Full Changelog**: https://gitlab.marcet.info/javier/movistar-u7d/compare/v4.0.9...v4.2


## Primera versión para Windows plenamente funcional, grabaciones también

- Finalmente tenemos release para Windows plenamente funcional. Gracias a esto el código es mucho más robusto y más limpio.

- Las grabaciones por defecto se guardarán en nuestra carpeta personal, dentro de `Videos\movistar-u7d`.

- La ruta, junto con el resto de ajustes los podemos configurar editando el fichero [movistar-u7d.ps1](../master/movistar-u7d.ps1).

- Para usar los temporizadores sólo tendremos que copiar el fichero [timers.conf](../master/timers.conf) que viene en el `.zip` a nuestra carpeta personal, ajustándolo a nuestro gusto según las [instrucciones](../../../#grabaciones-automáticas-temporizadores).


## Primera versión para Windows, corregida

- Añadidos ejectuables `ffmpeg`, `mkvmerge` y `tail`, con lo que ya no debería fallar en un Windows de serie, aunque las grabaciones aún no son funcionales.

