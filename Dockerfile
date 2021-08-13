FROM python:3.9-alpine

RUN apk update && apk add build-base && apk add bash ffmpeg htop mkvtoolnix s6 vim

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt && apk del build-base

COPY . .

CMD /app/start.sh
