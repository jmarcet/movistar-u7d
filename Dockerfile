FROM python:3.10-alpine

ARG TARGETARCH

ENV HOME="/home"
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

RUN sed -e 's:alpine\/[.0-9v]\+\/:alpine/edge/:g' -i /etc/apk/repositories
RUN apk update && apk upgrade --available --prune --purge
RUN apk add build-base libffi-dev linux-headers
RUN apk add bash curl ffmpeg git htop mkvtoolnix s6 vim

RUN mkdir /app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN \
    if [ "$TARGETARCH" = "amd64" ]; then \
        apk add sqlite-libs wrk; \
        pip install bandit black flake8 ipython pycodestyle; \
        flake8 *.py tv_grab_es_movistartv && pycodestyle *.py tv_grab_es_movistartv && bandit -r *.py tv_grab_es_movistartv; \
    fi
RUN apk del --purge build-base libffi-dev linux-headers

CMD /app/start.sh

