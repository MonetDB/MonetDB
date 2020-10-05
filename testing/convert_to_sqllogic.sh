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
srvpid=

mapi_port=$((30000 + RANDOM%10))
db=sqllogictest
dbpath="/tmp/sqllogictest"

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

start_mserver5() {
    mserver5 --debug=10 --set gdk_nr_threads=0 --set mapi_listenaddr=all \
        --set mapi_port=$mapi_port --forcemito --dbpath=$dbpath > /dev/null 2>&1 &
    srvpid=$!
    local i
    for ((i = 0; i < 100; i++)); do
    if [[ -f ${dbpath}/.started ]]; then
        echo "mserver5 started port=$mapi_port pid=$srvpid"
        break
    fi
    sleep 1
    done
    if ((i == 100)); then
        kill -KILL ${srvpid}
        exit 1
    fi
}

stop_mserver5() {
    echo "killing mserver5 ...";
    kill -TERM  $srvpid;
    wait $srvpid
}

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

if [[ -p /dev/stdin ]];then
    while read line;do
        if [[ -f $line ]];then
            files+=" $line";
        fi
    done
fi

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
                cat $f | mktest.py --database $db --port $mapi_port > $dst;
            else
                echo "$dst already exists!"
            fi    
        else
            echo ">>> converting $f ...";
            cat $f | mktest.py --database $db --port $mapi_port > $dst;
        fi
    fi
}

if [[ -d $dbpath ]];then
    rm -rf $dbpath;
    mkdir -p $dbpath;
fi

start_mserver5
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
stop_mserver5
rm -rf $dbpath

if [[ -e ${src}/All ]];then
    [[ ${src} -ef ${dst} ]] || cp ${src}/All $dst;
fi

if [[ -e ${src}/SingleServer ]];then
    [[ ${src} -ef ${dst} ]] || cp ${src}/SingleServer $dst;
fi
echo "Done"
