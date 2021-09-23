FROM python:3.9-alpine

RUN apk update
RUN apk add build-base linux-headers
RUN apk add bash ffmpeg git htop mkvtoolnix s6 vim

ARG TARGETARCH

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk del build-base linux-headers

COPY . .

RUN if [ "$TARGETARCH" = "amd64" ] ; then apk add wrk && pip install ipython; fi

CMD /app/start.sh

