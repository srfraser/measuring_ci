FROM ubuntu:18.04
MAINTAINER Simon Fraser <sfraser@mozilla.com>

RUN apt-get update -q && \
    apt-get install -yyq --no-install-recommends \
        autoconf \
        build-essential \
        clang \
        cmake \
        g++-7 \
        git \
        libdpkg-perl \
        libsnappy-dev \
        python3 \
        python3-cffi \
        python3-dev \
        python3-virtualenv \
        rsync \
        virtualenv \
        zip \
    && apt-get clean

RUN mkdir /work
COPY create_lambda_func.sh /create_lambda_func.sh

CMD ["/create_lambda_func.sh"]
