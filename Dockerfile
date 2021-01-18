FROM alpine:3.12

RUN apk update \
    && apk add bash build-base ffmpeg netcat-openbsd python2 python3 python3-dev py3-pip py3-psutil socat xmltv
    #&& apk add libva-intel-driver \
    #&& apk add libva-utils --repository=http://dl-cdn.alpinelinux.org/alpine/edge/testing

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app
COPY . .

RUN pip install -r requirements.txt

CMD /app/start.sh
