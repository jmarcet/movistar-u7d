$env:Path = "."
$env:PYTHONUNBUFFERED = "1"

# $env:DEBUG = "1"                                # mostrar logs de depuración
# $env:HOME = "C:\\Users\\usuario"                # ruta donde queremos que se generen las listas m3u, la guía y el directorio oculto .xmltv
# $env:LAN_IP = "192.168.1.15"                    # dirección IP, de las disponibles en el sistema, en la que queremos que el proxy funcione
# $env:EPG_THREADS = "4"                          # número de hilos para descargar la epg
# $env:NOSUBS = "1"                               # no grabar subtítulos, puede ser útil tanto si no los queremos como con emisiones con subtítulos de teletexto
# $env:MP4_OUTPUT = "1"                           # hacer grabaciones a fichero .mp4 y .sub por separado
# $env:RECORDING_THREADS = "4"                    # número de grabaciones simultáneas
# $env:RECORDINGS = "C:\\Users\\usuario\\Videos"  # directorio para grabaciones, si se establece se activará la comprobación periódica de timers.conf
# $env:RECORDINGS_PER_CHANNEL=1                   # realizar grabaciones en subdirectorios por canal
# $env:SANIC_PORT = "8888"                        # puerto en el que el proxy será accesible
# $env:VERBOSE_LOGS = "0"                         # no mostrar la url a la que se accede en los logs

$env:RECORDINGS = "$env:HOMEPATH\Videos\movistar-u7d"

start .\movistar_epg.exe -NoNewWindow
start .\movistar_u7d.exe -NoNewWindow -Wait

