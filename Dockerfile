FROM ubuntu:bionic

RUN apt-get update \
  && apt-get --fix-missing install -y \
    libstdc++-7-dev\
    g++  \
    make \
    cmake \
    clang-format \
    clang \
    python \
    python-dev \
    vim \
    exuberant-ctags \
    git \
    wget \
    libssl-dev \
    libboost-program-options-dev \
    libboost-system-dev \
    libboost-date-time-dev \
    libboost-filesystem-dev \
    libboost-iostreams-dev \
    libsoci-dev \
    libpq-dev \
    libquickfix-dev \
    libtbb-dev \
    liblog4cxx-dev
