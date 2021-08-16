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

RUN for file in http server; do cp -a /usr/local/lib/python3.9/site-packages/sanic/${file}.py \
    /usr/local/lib/python3.9/site-packages/sanic/${file}.py-orig; done

RUN sed \
    -e '/except CancelledError:/,/await self.error_response(e)/d' \
    -e 's/raise ServerError.\+$/return/g' \
    -i /usr/local/lib/python3.9/site-packages/sanic/http.py

RUN sed \
    -e 's/transport.set_write_buffer_limits.\+$/transport.set_write_buffer_limits(low=1316, high=7896)/' \
    -i /usr/local/lib/python3.9/site-packages/sanic/server.py

RUN if [ "$TARGETARCH" = "amd64" ] ; then apk add ipython wrk; fi

CMD /app/start.sh
