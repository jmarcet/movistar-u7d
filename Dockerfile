FROM python:3.9-slim

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y curl ffmpeg git htop iputils-ping less mkvtoolnix netcat \
    net-tools procps s6 vim wget

ARG TARGETARCH

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN if [ "$TARGETARCH" = "arm" ]; then apt-get install -y build-essential; fi
RUN pip install -r requirements.txt

COPY . .

RUN for file in http server; do \
    cp -a /usr/local/lib/python3.9/site-packages/sanic/${file}.py \
        /usr/local/lib/python3.9/site-packages/sanic/${file}.py-orig; done

RUN sed \
    -e '/except CancelledError:/,/await self.error_response(e)/d' \
    -e 's/raise ServerError.\+$/return/g' \
    -i /usr/local/lib/python3.9/site-packages/sanic/http.py

RUN sed \
    -e 's/transport.set_write_buffer_limits.\+$/transport.set_write_buffer_limits(low=1316, high=7896)/' \
    -i /usr/local/lib/python3.9/site-packages/sanic/server.py

RUN apt-get install -y atomicparsley ipython libluajit-5.1-2
RUN if [ "$TARGETARCH" != "arm" ]; then curl \
    https://snapshot.debian.org/archive/debian/20200601T084419Z/pool/main/w/wrk/wrk_4.1.0-3_${TARGETARCH}.deb \
    --output /tmp/wrk.deb && dpkg -i /tmp/wrk.deb && rm -f /tmp/wrk.deb; fi
RUN if [ "$TARGETARCH" = "arm" ]; then apt-get remove -y build-essential; fi
RUN apt-get autoclean -y && apt-get autoremove -y

CMD /app/start.sh
