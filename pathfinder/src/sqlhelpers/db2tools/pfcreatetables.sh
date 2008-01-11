#!/bin/sh

THIS=${0}
THISDIR=${0%/*}
THISFILE=${0##*/}
DB2=`which db2`

DEBUG=0

TAB_NAMES="names"
TAB_URIS="uris"
TAB_DOC="doc"
TAB_XMLDOC="xmldoc"

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

test -z $DB2  && error "DB2 not available";
################################################
# FUNCTIONS

function create_names {
    #check parameter list
    if [ ${#} -ne 2 ]; then
       error "Usage: ${0} <schema> <tabname>"
    fi

    SCHEMA=${1}
    TABNAME=${2}
    
    # set schema to ours 
    ${DB2} "set current schema ${SCHEMA}"

    # create the names-table 
    ${DB2} -t<<EOT
CREATE TABLE ${TABNAME}(nameid INT NOT NULL PRIMARY KEY,
                   name   VARCHAR(32) NOT NULL);
EOT
    EFLAG=${?}
    return ${EFLAG}
}

function create_uris {
    #check parameter list
    if [ ${#} -ne 2]; then
       error "Usage: ${0} <schema> <tabname>"
    fi

    SCHEMA=${1}
    TABNAME=${2}

    # set schema to ours
    ${DB2} "set current schema ${SCHEMA}"

    # create the uris-table
    ${DB2} -t<<EOT
CREATE TABLE ${TABNAME}(uriid INT NOT NULL PRIMARY KEY,
                   uri    VARCHAR(100) NOT NULL);
EOT
    EFLAG=${?}
    return ${EFLAG}
}

function create_doc {
    #check parameter list
    if [ ${#} -ne 4 ]; then
        error "Usage: ${0} <schema> <tabname> <namesname> <urisname>"
    fi

    SCHEMA=${1}
    TABNAME=${2}
    NAMESNAME=${3}
    URISNAME=${4}

    ${DB2} "set current schema ${SCHEMA}"
    
    ${DB2} -t<<EOT
CREATE TABLE ${TABNAME}(pre INT NOT NULL PRIMARY KEY,
                 size INT NOT NULL,
                 level SMALLINT NOT NULL,
                 kind SMALLINT,
                 nameid INT,
                 value VARCHAR(100),
                 guide INT NOT NULL,
                 uriid  INT,
                 pre_plus_size INT GENERATED ALWAYS AS (pre + size),
                 FOREIGN KEY (nameid) REFERENCES ${NAMESNAME}(nameid),
                 FOREIGN KEY (uriid)  REFERENCES ${URISNAME}(uriid));
EOT
    EFLAG=${?}
    return ${EFLAG}
}

function create_xmldoc {
    #check parameter list
    if [ ${#} -ne 5 ]; then
        error "Usage: ${0} <schema> <tabname> <docname> <namesname> <urisname>"
    fi

    SCHEMA=${1}
    TABNAME=${2}
    DOCNAME=${3}
    NAMESNAME=${4}
    URISNAME=${5}

    ${DB2} "set current schema ${SCHEMA}"

    ${DB2} -t<<EOT
CREATE VIEW ${TABNAME}(pre, size, level, kind, guide,
                       value, name, uri, nameid, uriid) AS
(SELECT pre, size, level, kind, guide,
        value, name, uri, doc.nameid, doc.uriid
   FROM ${DOCNAME} AS doc
        LEFT OUTER JOIN
        ${NAMESNAME} AS names
          ON doc.nameid = names.nameid
        LEFT OUTER JOIN
        ${URISNAME} AS uris
          ON doc.uriid = uris.uriid);
EOT
    EFLAG=${?}
    return ${EFLAG}
}

function drop_table {
    #check parameter list
    if [ ${#} -ne 1 ]; then
        error "Usage: ${0} <table>"
    fi

    TABLE=${1}

    ${DB2} "drop table ${TABLE}" 
}

function drop_names {
    #check parameter list
    if [ ${#} -ne 1 ]; then
        error "Usage: ${0} <schema>"
    fi

    SCHEMA=${1}

    ${DB2} "set current schema ${SCHEMA}"
    drop_table "${TAB_NAMES}" 
    EFLAG=${?}
    return ${EFLAG}
}

function drop_uris {
    #check parameter list
    if [ ${#} -ne 1 ]; then
        error "Usage: ${0} <schema>"
    fi

    SCHEMA=${1}

    ${DB2} "set current schema ${SCHEMA}"
    drop_table "${TAB_URIS}" 
    EFLAG=${?}
    return ${EFLAG}
}

function drop_doc {
    #check parameter list
    if [ ${#} -ne 1 ]; then
        error "Usage: ${0} <schema>"
    fi

    SCHEMA=${1}

    ${DB2} "set current schema ${SCHEMA}"
    drop_table "${TAB_DOC}"
    EFLAG=${?}
    return ${EFLAG}
}

function drop_xmldoc {
    #check parameter list
    if [ ${#} -ne 1 ]; then
        error "Usage: ${0} <schema>"
    fi

    SCHEMA=${1}

    ${DB2} "set current schema ${SCHEMA}"
    ${DB2} "drop view xmldoc"
    EFLAG=${?}
    return ${EFLAG}
}

function print_help () {
    out    "Pathfinder XQuery"
    out    "(c) Database Group, Technische Universitaet Muenchen"
    out    "" 
    out    "Usage: $THISFILE (create|drop) <db> <schema>" 
    out    ""
    out    "Summary"
    out    "Create tables for Pathfinder SQL-Backend."
    out    ""
    out    "Parameters"
    out -e "(create|drop):\teither create or drop the tables"
    out -e "<db>:\t\tName of the Database the tables should be placed/removed"
    out -e "<schema>:\tName of the Schema the tables should be placed/removed"
}

################################################
# MAIN

if [ $# -ne 3 ]; then
    print_help;
    exit 0;
fi     

OPTION=${1}
DATABASE=${2}
SCHEMA=${3}

${DB2} "connect to ${DATABASE}"
EFLAG=$?
out -ne "Connecting to database ${DATABASE} ... "; fail $EFLAG; 
test $EFLAG -ne 0 && exit 1;

case ${OPTION} in
   create) create_names ${SCHEMA} ${TAB_NAMES};
           EFLAG=${?};
           out -ne "Creating ${SCHEMA}.${TAB_NAMES} ... "; fail ${EFLAG};

           create_uris ${SCHEMA} ${TAB_URIS};
           EFLAG=${?};
           out -ne "Creating ${SCHEMA}.${TAB_URIS} ... "; fail ${EFLAG};

           create_doc ${SCHEMA} ${TAB_DOC} ${TAB_NAMES} ${TAB_URIS};
           EFLAG=${?};
           out -ne "Creating ${SCHEMA}.${TAB_DOC} ... "; fail ${EFLAG};

           create_xmldoc ${SCHEMA} ${TAB_XMLDOC} ${TAB_DOC} ${TAB_NAMES} ${TAB_URIS};
           EFLAG=${?};
           out -ne "Creating ${SCHEMA}.${TAB_XMLDOC} ... "; fail ${EFLAG};
           ;;

   drop)   drop_xmldoc ${SCHEMA}
           EFLAG=${?}
           out -ne "Dropping ${SCHEMA}.${TAB_XMLDOC} ... "; fail ${EFLAG};

           drop_doc ${SCHEMA}
           EFLAG=${?}
           out -ne "Dropping ${SCHEMA}.${TAB_DOC} ... "; fail ${EFLAG};

           drop_names ${SCHEMA}
           EFLAG=${?}
           out -ne "Dropping ${SCHEMA}.${TAB_NAMES} ... "; fail ${EFLAG};

           drop_uris ${SCHEMA}
           EFLAG=${?}
           out -ne "Dropping ${SCHEMA}.${TAB_URIS} ... "; fail ${EFLAG};
           ;;
   *)      print_help; 
           exit 1;
           ;;
esac
${DB2} "terminate"
