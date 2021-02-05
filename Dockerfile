FROM python:3.9-alpine

RUN apk update \
    && apk add bash build-base git htop netcat-openbsd tcpdump vim xmltv

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD /app/start.sh
