DEBUG=0

TAB_NAMES="names"
TAB_DOC="doc"
TAB_XMLDOC="xmldoc"

DB2=`which db2`

function fail {
    if [ ${#} -ne 1 ]; then
        error "Wrong use of fail"
    fi

    if [ ${1} -eq 0 ]; then
        out "Success"
        return 0;
    else
        out "Failed"
        return 0;
    fi
}

function error {
    if [ ${#} -ne 1 ]; then
        echo "Wrong use of error"
        exit 1;
    fi
    echo -e "Error: ${1}"
    exit 1
}

function warning
{
    if [ ${#} -ne 1 ]; then
        error "Wrong use of debug"
    fi
    echo -e "Warning: ${1}"
}

function out {
    if [ ${#} -eq 1 ]; then
        echo -e "${1}"
        return 0;
    fi
    if [ ${#} -eq 2 ]; then
        echo "${1}" "${2}"
    else
       error "wrong use of out"
    fi
}

function debug {
    if [ ${#} -ne 1 ]; then
        error "Wrong use of debug"
    fi
    test ${DEBUG} -eq 1 && echo -e "Debug: ${1}"
}

function LOG1 {
    prompt="\n# `date +%H:%M:%S` >  "; { echo -en "$prompt$prompt" ; echo -n "$*" ; echo -en "$prompt\n\n" ; } >&2;
}

function getAbsolutePath {
    if [ -z ${1} ]; then
       error "Wrong use of ${0}";
    fi

    PATH_=${1%/*}
    FILE_=${1##*/}

    echo `cd ${PATH_} 2> /dev/null && pwd || echo ${PATH_}`/${FILE_}
}

test -z $DB2  && error "DB2 not available";
