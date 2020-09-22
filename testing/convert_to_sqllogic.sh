#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
set -e 

usage() {
echo "Usage $0 --src-dir <source dir> --dst-dir <destination directory>"
echo "Converts old test sql scripts to sqllogic scripts."
echo "Options:"
echo " -s|--src-dir <source dir>          directory with old sql tests."
echo " -t|--dst-dir <destination dir>     destination dotrectory for *.test sqllogic tests."
echo " -d|--dry-run                    dry run"
echo
}

src=
dst=
dry_run=

for arg in "$@"
do
    case $arg in
        -s|--src-dir)
            src=$(pwd)/$2
            dst=$src
            shift
            shift
            ;;
        -t|--dst-dir)
            dst=$2
            shift
            shift
            ;;
        -d|--dry-run)
            dry_run="true"
            shift
            ;;
    esac
done

if [[ ! -d "${src}" ]];then
    echo "ERROR: source directory required!";
    usage;
    exit 1;
fi

[[ ! -d "${dst}" ]] || mkdir -p ${dst}

dryrun() {
    local f=$1;
    cat $f | mktest.py --database "test";
}

work() {
    local f=$1;
    local dst=$2;
    cat $f | mktest.py --database "test" > $dst;
}

for f in $(ls ${src}/*.sql);do
    echo ">>> converting $f ..."
    if [[ "${dry_run}" = true ]];then
        dryrun $f;
    else
        bn=$(basename $f .sql)
        work $f $dst/$bn.test
    fi
done;

if [[ -e ${src}/All ]];then
    [[ ${src} -ef ${dst} ]] || cp ${src}/All $dst;
fi
if [[ -e ${src}/SingleServer ]];then
    [[ ${src} -ef ${dst} ]] || cp ${src}/SingleServer $dst;
fi
echo "Done"
