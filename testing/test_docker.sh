TEST="python3 -m venv /monetdb/build/venv && . /monetdb/build/venv/bin/activate && pip install --upgrade pip && pip install pymonetdb && cmake -Bbuild -S . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/monetdb/build/install -DASSERT=OFF -DSTRICT=ON && cmake --build build && cmake --install build && cmake --build build --target test && PATH=/monetdb/build/install/bin:$PATH /monetdb/build/install/bin/Mtest.py"

rm -rf build/*
podman run -i --rm --volume $(pwd):/monetdb:z --workdir /monetdb alpine:latest <<<"apk add --no-cache bison cmake pkgconf python3  openssl-dev bzip2-dev libbz2 lz4-dev lz4-libs pcre2-dev readline-dev xz-dev xz-libs zlib-dev build-base gcc py3-pip py3-cryptography && $TEST"
rm -rf build/*
podman run -i --platform linux/arm64 --rm --volume $(pwd):/monetdb:z --workdir /monetdb alpine:latest <<<"apk add --no-cache bison cmake pkgconf python3  openssl-dev bzip2-dev libbz2 lz4-dev lz4-libs pcre2-dev readline-dev xz-dev xz-libs zlib-dev build-base gcc py3-pip py3-cryptography && $TEST"
rm -rf build/*
podman run -i --platform linux/s390x --rm --volume $(pwd):/monetdb:z --workdir /monetdb alpine:latest <<<"apk add --no-cache bison cmake pkgconf python3  openssl-dev bzip2-dev libbz2 lz4-dev lz4-libs pcre2-dev readline-dev xz-dev xz-libs zlib-dev build-base gcc py3-pip py3-cryptography && $TEST"
rm -rf build/*
podman run -i --rm --volume $(pwd):/monetdb:z --workdir /monetdb amazonlinux:2 <<<"yum -y install bison cmake3 python3 openssl-devel bzip2-devel bzip2-libs lz4-devel lz4-libs pcre2-devel readline-devel xz-devel xz-libs zlib-devel gcc pip python3-cryptography make && ${TEST//cmake/cmake3}"
rm -rf build/*
podman run -i --rm --volume $(pwd):/monetdb:z --workdir /monetdb amazonlinux:latest <<<"dnf -y install bison cmake python3 openssl-devel bzip2-devel bzip2-libs lz4-devel lz4-libs pcre2-devel readline-devel xz-devel xz-libs zlib-devel gcc pip python3-cryptography && $TEST"
rm -rf build/*
podman run -i --rm --volume $(pwd):/monetdb:z --workdir /monetdb centos <<<"sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && dnf -y install bison cmake python3 openssl-devel bzip2-devel bzip2-libs lz4-devel lz4-libs pcre2-devel readline-devel xz-devel xz-libs zlib-devel gcc python3-pip python3-cryptography libarchive && $TEST"
rm -rf build/*
podman run -i --rm --volume $(pwd):/monetdb:z --workdir /monetdb fedora <<<"dnf -y install bison cmake python3 openssl-devel bzip2-devel bzip2-libs lz4-devel lz4-libs pcre2-devel readline-devel xz-devel xz-libs zlib-devel gcc pip python3-cryptography && $TEST"
