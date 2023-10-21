FROM python:3.11-slim

ARG TARGETARCH="amd64"

# https://askubuntu.com/questions/972516/debian-frontend-environment-variable
ARG DEBIAN_FRONTEND="noninteractive"
# http://stackoverflow.com/questions/48162574/ddg#49462622
ARG APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn
# https://github.com/NVIDIA/nvidia-docker/wiki/Installation-(Native-GPU-Support)
ENV NVIDIA_DRIVER_CAPABILITIES="compute,video,utility"

# Install dependencies:
# mesa-va-drivers: needed for AMD VAAPI. Mesa >= 20.1 is required for HEVC transcoding.
# curl: healthcheck
RUN apt-get update \
 && apt-get install --no-install-recommends --no-install-suggests -y ca-certificates gnupg wget curl \
 && wget -O - https://repo.jellyfin.org/jellyfin_team.gpg.key | apt-key add - \
 && echo "deb [arch=$( dpkg --print-architecture )] https://repo.jellyfin.org/$( awk -F'=' '/^ID=/{ print $NF }' /etc/os-release ) $( awk -F'=' '/^VERSION_CODENAME=/{ print $NF }' /etc/os-release ) main" | tee /etc/apt/sources.list.d/jellyfin.list \
 && apt-get update \
 && apt-get install --no-install-recommends --no-install-suggests -y \
   mesa-va-drivers \
   jellyfin-ffmpeg5 \
   openssl \
   locales \
 && sed -i \
   -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' \
   -e 's/# es_ES.UTF-8 UTF-8/es_ES.UTF-8 UTF-8/' \
   /etc/locale.gen && locale-gen

RUN apt-get install --no-install-recommends --no-install-suggests -y \
   autoconf \
   automake \
   build-essential \
   ca-certificates \
   git \
   libargtable2-dev \
   libtool \
   pkg-config

WORKDIR /tmp

ARG JELLYFIN_FFMPEG_BRANCH=jellyfin-5.1
ARG COMSKIP_BRANCH=master

ARG ffmpeg_CFLAGS="-I/usr/lib/jellyfin-ffmpeg/include"
ARG ffmpeg_LIBS="-L/usr/lib/jellyfin-ffmpeg/lib -lavcodec -lavformat -lavutil -lswscale"

COPY patches/*.patch .

RUN git clone -b ${JELLYFIN_FFMPEG_BRANCH} https://github.com/jellyfin/jellyfin-ffmpeg \
 && cd jellyfin-ffmpeg \
 && ./configure --prefix=/usr/lib/jellyfin-ffmpeg --disable-x86asm \
 && make install-headers \
 && cd .. \
 && rm -fr jellyfin-ffmpeg \
 && git clone -b ${COMSKIP_BRANCH} https://github.com/erikkaashoek/Comskip \
 && cd Comskip \
 && patch -p1 < ../comskip.patch \
 && patch -p1 < ../qsv.patch \
 && ./autogen.sh \
 && ./configure --prefix=/usr \
 && make \
 && make install \
 && cd .. \
 && rm -fr Comskip \
 && rm -fr /usr/lib/jellyfin-ffmpeg/include \
 && rm -f /tmp/*.patch \
 && apt-get remove -y \
   autoconf \
   automake \
   build-essential \
   gnupg \
   libargtable2-dev \
   libtool \
   pkg-config \
   wget

WORKDIR /app

ENV HOME=/home
ENV PYTHONPATH=/app
ENV TMP=/tmp

ENV LC_ALL es_ES.UTF-8
ENV LANG es_ES.UTF-8
ENV LANGUAGE es_ES:UTF-8

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

RUN apt-get install --no-install-recommends --no-install-suggests -y \
   bash \
   htop \
   less \
   libargtable2.0 \
   mkvtoolnix \
   net-tools \
   netcat-openbsd \
   procps

COPY . .

RUN \
  if [ "$TARGETARCH" = "amd64" ]; then \
    apt-get install --no-install-recommends --no-install-suggests -y wrk \
    && pip install bandit black httpie ipython pycodestyle pylint ruff \
    && pycodestyle -v *.py && pylint --rcfile pylint.toml -v *.py \
    && ruff check --config ruff.toml -v *.py \
    && bandit -v *.py && black -l 113 -t py311 --diff -v *.py; \
  fi

RUN apt-get clean autoclean -y \
 && apt-get autoremove -y \
 && dpkg -P autoconf dirmngr dpkg-dev git git-man gpg-agent pkg-config wget \
 && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/lib/jellyfin-ffmpeg/ff* /usr/bin/ \
 && ln -s /usr/lib/jellyfin-ffmpeg/lib/* /usr/lib/

CMD ["/app/mu7d.py"]
