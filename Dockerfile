FROM python:3.9-alpine

RUN apk update
RUN apk add build-base
RUN apk add bash ffmpeg git htop mkvtoolnix s6 vim

ARG TARGETARCH

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk del build-base

COPY . .

RUN cp -a /usr/local/lib/python3.9/site-packages/sanic/server.py \
    /usr/local/lib/python3.9/site-packages/sanic/server.py-orig

RUN sed \
    -e 's/transport.set_write_buffer_limits.\+$/transport.set_write_buffer_limits(low=188, high=1316)/' \
    -i /usr/local/lib/python3.9/site-packages/sanic/server.py

RUN if [ "$TARGETARCH" = "amd64" ] ; then apk add wrk && pip install ipython; fi

CMD /app/start.sh
