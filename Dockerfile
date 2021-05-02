FROM python:3.9-slim

RUN apt-get update && apt-get upgrade -y && \
	apt-get install -y \
	ffmpeg git htop iproute2 iputils-ping less mkvtoolnix netcat net-tools procps sudo vainfo vim wget

RUN apt-get -y clean && apt-get -y autoremove

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD /app/start.sh
