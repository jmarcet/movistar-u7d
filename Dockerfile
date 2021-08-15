FROM python:3.9-alpine

RUN apk update
RUN apk add build-base
RUN apk add bash ffmpeg htop mkvtoolnix s6 vim

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk del build-base

COPY . .

RUN sed \
    -e '/except CancelledError:/,/await self.error_response(e)/d' \
    -e 's/raise ServerError.\+$/return/g' \
    -i /usr/local/lib/python3.9/site-packages/sanic/http.py

CMD /app/start.sh
