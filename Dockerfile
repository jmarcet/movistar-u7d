FROM python:3.14-slim-trixie AS base

ARG BUILD_TYPE
ARG TARGETARCH

ARG COMSKIP_BRANCH=master
ARG JELLYFIN_FFMPEG_BRANCH=jellyfin-7.1

# https://askubuntu.com/questions/972516/debian-frontend-environment-variable
ENV DEBIAN_FRONTEND="noninteractive"

ENV PIP_CACHE_DIR=/root/.cache/pip
ENV RUFF_CACHE_DIR=/root/.cache/ruff
ENV UV_CACHE_DIR=/root/.cache/uv

RUN --mount=type=cache,sharing=locked,target=/var/cache/apt \
    apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests -y \
       build-essential \
       git \
       htop \
       less \
       locales \
       net-tools \
       netcat-openbsd \
       procps \
       wget \
    && sed -i -e 's/# es_ES.UTF-8 UTF-8/es_ES.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen

ENV ffmpeg_CFLAGS="-I/usr/lib/jellyfin-ffmpeg/include"
ENV ffmpeg_LIBS="-L/usr/lib/jellyfin-ffmpeg/lib -lavcodec -lavformat -lavutil -lswscale"

COPY patches/comskip.patch patches/0001-Compute-the-added_recording-windows-from-the-measure.patch .

RUN --mount=type=cache,sharing=locked,target=/var/cache/apt \
    if [ "$BUILD_TYPE" = "full" ]; then \
        APT_SRC=/etc/apt/sources.list.d/jellyfin.sources \
        && echo "Types: deb" > ${APT_SRC} \
        && echo "URIs: https://repo.jellyfin.org/debian" >> ${APT_SRC} \
        && echo "Suites: $( awk -F'=' '/^VERSION_CODENAME=/{ print $NF }' /etc/os-release )" >> ${APT_SRC} \
        && echo "Components: main" >> ${APT_SRC} \
        && echo "Architectures: $( dpkg --print-architecture )" >> ${APT_SRC} \
        && echo "Signed-By: /etc/apt/keyrings/jellyfin.gpg" >> ${APT_SRC} \
        && apt-get install --no-install-recommends --no-install-suggests -y gnupg \
        && wget -O- https://repo.jellyfin.org/jellyfin_team.gpg.key | gpg --dearmor --yes --output /etc/apt/keyrings/jellyfin.gpg \
        && apt-get update \
        && apt-get install --no-install-recommends --no-install-suggests -y \
           jellyfin-ffmpeg7 \
        && apt-get purge -y gnupg \
        && apt-get install --no-install-recommends --no-install-suggests -y \
           autoconf \
           automake \
           libargtable2-dev \
           libtool \
           pkg-config; \
    fi

RUN if [ "$BUILD_TYPE" = "full" ]; then \
        git clone -b ${JELLYFIN_FFMPEG_BRANCH} https://github.com/jellyfin/jellyfin-ffmpeg \
        && cd jellyfin-ffmpeg \
        && ./configure --prefix=/usr/lib/jellyfin-ffmpeg --disable-x86asm \
        && make -j$(nproc) install-headers \
        && cd .. \
        && rm -fr jellyfin-ffmpeg \
        && git clone -b ${COMSKIP_BRANCH} https://github.com/erikkaashoek/Comskip \
        && cd Comskip \
        && patch -p1 < ../comskip.patch \
        && patch -p1 < ../0001-Compute-the-added_recording-windows-from-the-measure.patch \
        && ./autogen.sh \
        && ./configure \
        && make -j$(nproc) \
        && make -j$(nproc) install \
        && cd .. \
        && rm -f comskip.patch 0001-Compute-the-added_recording-windows-from-the-measure.patch \
        && rm -fr Comskip \
        && rm -fr /usr/lib/jellyfin-ffmpeg/include \
        && apt-get purge -y \
           autoconf \
           automake \
           libargtable2-dev \
           libtool \
           pkg-config \
        && apt-get install --no-install-recommends --no-install-suggests -y libargtable2.0 \
        && if [ "$TARGETARCH" = "amd64" ]; then \
               apt-get install --no-install-recommends --no-install-suggests -y \
                  intel-gpu-tools \
                  msr-tools; \
           fi \
        && ln -s /usr/lib/jellyfin-ffmpeg/ff* /usr/local/bin/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libavcodec.so.* /usr/local/lib/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libavformat.so.* /usr/local/lib/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libavutil.so.* /usr/local/lib/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libswscale.so.* /usr/local/lib/ \
        && ldconfig; \
    fi

WORKDIR /app

COPY requirements.txt .

RUN --mount=type=cache,target=/root/.cache \
    pip install --disable-pip-version-check --root-user-action ignore --use-pep517 uv \
    && uv pip install --system -r requirements.txt

COPY . .

RUN --mount=type=cache,target=/root/.cache \
   if [ "$TARGETARCH" = "amd64" ] && [ "${BUILD_TYPE}" != "full" ]; then \
       uv pip install --system bandit pycodestyle pylint ruff 2>&1 | tee /tmp/lint-install.txt \
       && make test \
       && uv pip uninstall --system $( awk '/==/ { print $2 }' /tmp/lint-install.txt ); \
   fi

RUN apt-get purge -y binutils-common build-essential dpkg-dev git git-man gpg-agent libcurl3-gnutls liberror-perl libnghttp2-14 \
                     libperl5.40 librtmp1 libsasl2-2 libsasl2-modules-db libssh2-1 patch perl perl-modules-5.40 pkgconf \
    && apt-get clean autoclean -y \
    && apt-get autoremove -y

RUN --mount=type=cache,target=/root/.cache \
    if [ "$TARGETARCH" != "amd64" ] || [ "$BUILD_TYPE" != "full" ]; then uv pip uninstall --system uv; fi

RUN rm -fr \
        Dockerfile Makefile patches pyproject.toml requirements*.txt setup.py tox.ini \
        /tmp/* /usr/local/.lock /var/cache/* /var/lib/apt/lists/* *.conf

RUN chown nobody:nogroup /home && chmod g+s /home

######################
# Squash final image #
######################
FROM scratch

COPY --from=base / /

# https://github.com/NVIDIA/nvidia-docker/wiki/Installation-(Native-GPU-Support)
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility,video"

ENV LC_ALL=es_ES.UTF-8
ENV LANG=es_ES.UTF-8
ENV LANGUAGE=es_ES:UTF-8

ENV HOME=/home
ENV PATH=/home/.local/bin:/usr/local/bin:/usr/sbin:/usr/bin
ENV PYTHONPATH=/app
ENV TMP=/tmp

WORKDIR /home

CMD ["/app/mu7d.py"]
