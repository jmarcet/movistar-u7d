FROM python:3.9-slim

RUN apt-get update && apt-get upgrade -y && \
	apt-get install -y \
	dnsmasq ffmpeg git htop iproute2 iputils-ping less mkvtoolnix netcat net-tools \
	procps sudo vainfo vim wget

RUN apt-get -y clean && apt-get -y autoremove

RUN sed -e 's:^#IGNORE_RESOLVCONF:IGNORE_RESOLVCONF:' -i /etc/default/dnsmasq
COPY imagenio.conf /etc/dnsmasq.d/

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD /app/start.sh
