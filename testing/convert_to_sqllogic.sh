#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
set -e 

usage() {
echo "USAGE $(basename $0) [OPTION ...] [FILE...]"
echo "DESCRIPTION: Converts old sql scripts to sqllogic scripts."
echo "OPTIONS:"
echo " -s|--src-dir <source dir>          directory with old sql tests."
echo " -t|--dst-dir <destination dir>     destination dotrectory for *.test sqllogic tests."
echo " -o|--overwrite <bool>              overwrites existing .test" 
echo " -d|--dry-run                       dry run"
echo
}

src=
dst=
dry_run=
overwrite=

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
        -o|--overwrite)
            overwrite="true"
            shift
            ;;
    esac
done

files=()
if [[ -d "${src}" ]];then
    for f in $(ls $src);do
        files+=" $src/$f";
    done
fi

for f in $@;do
    if [[ -f $f ]];then
        files+=" $f";
    fi
done

if [[ -z $files ]];then 
    usage;
    exit 1;
fi

if [[ -z "${dst}" ]];then
    echo "ERROR: need --dest-dir";
    usage;
    exit 1;
fi
[[ -d "${dst}" ]] || mkdir -p ${dst};

dryrun() {
    local f=$1;
    cat $f | mktest.py --database "test";
}

work() {
    local f=$1;
    local dst=$2;
    if [[ "${dry_run}" = true ]];then
        dryrun $f;
    else
        if [[ -e $dst ]];then
            if [[ "$overwrite" = "true" ]];then
                echo ">>> overwriting $dst ...";
                cat $f | mktest.py --database "test" > $dst;
            else
                echo "$dst already exists!"
            fi    
        else
            echo ">>> converting $f ...";
            cat $f | mktest.py --database "test" > $dst;
        fi
    fi
}

for f in $files;do
    ext=$(echo "${f#*.}");
    if [[ $ext == "sql.in" ]];then
        bn=$(basename $f .sql.in);
        work $f $dst/$bn.test.in;
        continue
    fi
    if [[ $ext == "sql" ]];then
        bn=$(basename $f .sql);
        work $f $dst/$bn.test;
    fi
done;

if [[ -e ${src}/All ]];then
    [[ ${src} -ef ${dst} ]] || cp ${src}/All $dst;
fi

if [[ -e ${src}/SingleServer ]];then
    [[ ${src} -ef ${dst} ]] || cp ${src}/SingleServer $dst;
fi
echo "Done"
