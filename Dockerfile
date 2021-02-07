FROM python:3.9-alpine

RUN apk update \
    && apk add bash build-base git htop less netcat-openbsd socat tcpdump vim xmltv

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN git clone https://github.com/sanic-org/sanic /tmp/sanic && cd /tmp/sanic && \
    python setup.py bdist_wheel && pip install dist/* && cd /app && rm -fr /tmp/sanic
RUN pip install -r requirements.txt

COPY . .

CMD /app/start.sh
