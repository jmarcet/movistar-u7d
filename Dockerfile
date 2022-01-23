FROM python:3.10-alpine

ARG TARGETARCH

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN sed -e 's:alpine\/[.0-9v]\+\/:alpine/edge/:g' -i /etc/apk/repositories
RUN apk update && apk upgrade --available --prune --purge
RUN apk add build-base libffi-dev linux-headers
RUN apk add bash ffmpeg git htop mkvtoolnix s6 vim

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk del --purge build-base libffi-dev linux-headers

COPY . .

RUN if [ "$TARGETARCH" = "amd64" -o "$TARGETARCH" = "arm64" ]; then apk add wrk && pip install ipython; fi

CMD /app/start.sh

