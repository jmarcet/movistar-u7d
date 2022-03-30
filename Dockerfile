############## build stage ##############
FROM python:3.10-alpine as buildstage

ARG TARGETARCH

ARG ARGTABLE_VER="2.13"
ARG ARGTABLE_VER1="2-13"

COPY patches/ /tmp/patches/

RUN sed -e 's:alpine\/[.0-9v]\+\/:alpine/edge/:g' -i /etc/apk/repositories
RUN apk update && apk upgrade --available --prune --purge
RUN apk add autoconf automake build-base curl ffmpeg4-dev git libtool
RUN \
    mkdir -p /tmp/argtable && \
    curl -o /tmp/argtable-src.tar.gz -L \
    "https://sourceforge.net/projects/argtable/files/argtable/argtable-${ARGTABLE_VER}/argtable${ARGTABLE_VER1}.tar.gz" && \
    tar xf /tmp/argtable-src.tar.gz -C /tmp/argtable --strip-components=1 && \
    cp /tmp/patches/config.* /tmp/argtable/ && \
    cd /tmp/argtable && \
    ./configure --prefix=/usr && \
    make -j 2 && make check && \
    make DESTDIR=/tmp/argtable-build install && \
    cp -pr /tmp/argtable-build/usr/* /usr/

RUN \
    git clone https://github.com/erikkaashoek/Comskip.git /tmp/comskip && \
    cd /tmp/comskip && \
    patch -p1 < /tmp/patches/comskip.patch && \
    ./autogen.sh && \
    ./configure --bindir=/usr/bin --sysconfdir=/config/comskip && \
    make -j 2 && \
    make DESTDIR=/tmp/comskip-build install

############## runtime stage ##############
FROM python:3.10-alpine

ARG TARGETARCH

ENV HOME=/home
ENV PYTHONPATH=/app
ENV TMP=/tmp

RUN sed -e 's:alpine\/[.0-9v]\+\/:alpine/edge/:g' -i /etc/apk/repositories
RUN apk update && apk upgrade --available --prune --purge
RUN apk add bash curl ffmpeg4-libs ffmpeg git htop mkvtoolnix s6 vim

RUN \
    if [ "$TARGETARCH" = "amd64" ]; then \
        apk add libva libva-intel-driver sqlite-libs wrk; \
        pip install bandit black flake8 ipython pycodestyle; \
    fi

COPY --from=buildstage /tmp/argtable-build/usr/lib/ /usr/lib/
COPY --from=buildstage /tmp/comskip-build/usr/ /usr/

WORKDIR /app

RUN apk add build-base libffi-dev linux-headers
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN apk del --purge build-base libffi-dev linux-headers

COPY . .

RUN \
    if [ "$TARGETARCH" = "amd64" ]; then \
        flake8 && pycodestyle && bandit *.py; \
    fi

CMD /app/mu7d.py
