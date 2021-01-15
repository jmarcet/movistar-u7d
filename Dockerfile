FROM alpine:edge

RUN apk update \
    && apk add bash build-base ffmpeg netcat-openbsd python3 python3-dev py3-pip

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app
COPY . .

RUN pip install -r requirements.txt

CMD /app/movistar-u7d.py
