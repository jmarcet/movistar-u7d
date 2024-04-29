FROM python:3.12-slim

ARG BUILD_TYPE
ARG TARGETARCH

# https://askubuntu.com/questions/972516/debian-frontend-environment-variable
ARG DEBIAN_FRONTEND="noninteractive"

RUN --mount=type=cache,sharing=locked,target=/var/cache/apt \
    apt-get update \
    && apt-get install --no-install-recommends --no-install-suggests -y \
       htop \
       less \
       locales \
       net-tools \
       netcat-openbsd \
       procps \
       wget \
    && sed -i -e 's/# es_ES.UTF-8 UTF-8/es_ES.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen

ENV LC_ALL es_ES.UTF-8
ENV LANG es_ES.UTF-8
ENV LANGUAGE es_ES:UTF-8

ENV HOME=/home
ENV PATH=/home/.local/bin:/usr/local/bin:/usr/sbin:/usr/bin
ENV PYTHONPATH=/app
ENV TMP=/tmp

WORKDIR /tmp

RUN --mount=type=cache,sharing=locked,target=/var/cache/apt \
    apt-get install --no-install-recommends --no-install-suggests -y build-essential git

COPY requirements.txt .

RUN --mount=type=cache,target=${HOME}/.cache \
    pip install --disable-pip-version-check --root-user-action ignore --use-pep517 uv \
    && uv pip install --system -r requirements.txt

# http://stackoverflow.com/questions/48162574/ddg#49462622
ARG APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn

# https://github.com/NVIDIA/nvidia-docker/wiki/Installation-(Native-GPU-Support)
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility,video"

ARG COMSKIP_BRANCH=master
ARG JELLYFIN_FFMPEG_BRANCH=jellyfin

ARG ffmpeg_CFLAGS="-I/usr/lib/jellyfin-ffmpeg/include"
ARG ffmpeg_LIBS="-L/usr/lib/jellyfin-ffmpeg/lib -lavcodec -lavformat -lavutil -lswscale"

COPY patches/comskip.patch .

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
           jellyfin-ffmpeg6 \
        && apt-get purge -y gnupg \
        && apt-get install --no-install-recommends --no-install-suggests -y \
           autoconf \
           automake \
           libargtable2-dev \
           libtool \
           pkg-config \
        && git clone -b ${JELLYFIN_FFMPEG_BRANCH} https://github.com/jellyfin/jellyfin-ffmpeg \
        && cd jellyfin-ffmpeg \
        && ./configure --prefix=/usr/lib/jellyfin-ffmpeg --disable-x86asm \
        && make -j$(nproc) install-headers \
        && cd .. \
        && rm -fr jellyfin-ffmpeg \
        && git clone -b ${COMSKIP_BRANCH} https://github.com/erikkaashoek/Comskip \
        && cd Comskip \
        && patch -p1 < ../comskip.patch \
        && ./autogen.sh \
        && ./configure \
        && make -j$(nproc) \
        && make -j$(nproc) install \
        && cd .. \
        && rm -fr Comskip \
        && rm -fr /usr/lib/jellyfin-ffmpeg/include \
        && apt-get purge -y \
           autoconf \
           automake \
           libargtable2-dev \
           libtool \
           pkg-config \
        && apt-get install --no-install-recommends --no-install-suggests -y libargtable2.0 \
        && ln -s /usr/lib/jellyfin-ffmpeg/ff* /usr/local/bin/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libavcodec.so.* /usr/local/lib/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libavformat.so.* /usr/local/lib/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libavutil.so.* /usr/local/lib/ \
        && ln -s /usr/lib/jellyfin-ffmpeg/lib/libswscale.so.* /usr/local/lib/; \
    fi

RUN rm -f comskip.patch

RUN apt-get purge -y binutils-common build-essential dpkg-dev git git-man gpg-agent libcurl3-gnutls \
    liberror-perl libgdbm-compat4 libldap-2.5-0 libnghttp2-14 libperl5.36 librtmp1 libsasl2-2 \
    libsasl2-modules-db libssh2-1 patch perl perl-modules-5.36 pkgconf \
    && apt-get clean autoclean -y \
    && apt-get autoremove -y \
    && rm -fr /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=${HOME}/.cache \
    if [ "${BUILD_TYPE}" = "full" ] && [ "$TARGETARCH" = "amd64" ]; then \
        uv pip install --system bandit pycodestyle pylint ruff 2>&1 | tee /tmp/lint-install.txt \
        && bandit -v *.py \
        && pycodestyle -v *.py \
        && pylint --rcfile pyproject.toml -v *.py \
        && ruff check --config pyproject.toml --no-cache -v *.py \
        && ruff check --config pyproject.toml --diff --no-cache --no-fix-only -v *.py \
        && ruff format --config pyproject.toml --diff --no-cache -v *.py \
        && uv pip uninstall --system $( awk '/==/ { print $2 }' /tmp/lint-install.txt ); \
    fi

RUN --mount=type=cache,target=${HOME}/.cache \
    pip uninstall --disable-pip-version-check --root-user-action ignore -y uv wheel

RUN rm -fr Dockerfile patches pyproject.toml requirements*.txt tox.ini /tmp/*.txt /usr/local/.lock *.conf

RUN chown nobody:nogroup /home && chmod g+s /home

WORKDIR /home

CMD ["/app/mu7d.py"]
