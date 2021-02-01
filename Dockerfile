FROM alpine:3.12

RUN apk update \
    && apk add bash build-base htop netcat-openbsd python3 python3-dev py3-pip py3-psutil py3-wheel xmltv

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD /app/start.sh
