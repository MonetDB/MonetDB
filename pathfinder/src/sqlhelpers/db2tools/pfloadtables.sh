#!/bin/bash

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

function load_table {
    #check parameter list
    if [ ${#} -ne 3 ]; then
       error "Usage: ${0} <file> <schema> <table>"
    fi

    SCHEMA=${2}
    FILE=${1}
    TABLE=${3}
    
    # set schema to ours 
    ${DB2} "set current schema ${SCHEMA}"

    # create the names-table 
    ${DB2} -t<<EOT
LOAD FROM ${FILE} OF DEL
    MODIFIED BY DELPRIORITYCHAR 
    REPLACE INTO ${TABLE} 
    NONRECOVERABLE
    INDEXING MODE rebuild;
EOT

    ${DB2} "SET INTEGRITY FOR ${TABLE} IMMEDIATE CHECKED FORCE GENERATED" 
    EFLAG=${?}
    return ${EFLAG}
}

function print_help () {
    out    "Pathfinder XQuery"
    out    "(c) Database Group, Technische Universitaet Muenchen"
    out    "" 
    out    "Usage: ${THISFILE} <db> <schema> <enc_file> <name_file>"
    out    ""
    out    ""
    out    "Summary"
    out    "Load the encoded Documents from the shredder to the"
    out    "the database." 
    out    "You maybe want to execute 'pfcreatetables' first." 
    out    ""
    out    "Parameters"
    out -e "<db>:\t\tName of the Database the content should be placed/removed."
    out -e "<schema>:\tName of the Schema the content should be placed/removed."
    out -e "<enc_file>:\tEncoded XML-Document"
    out -e "<name_file>:\tEncoded Name-File "
    out -e "<uri_file>:\tEncoded Namespace-File"
}

################################################
# MAIN

if [ $# -ne 5 ]; then
    print_help;
    exit 0;
fi     

DATABASE=${1}
SCHEMA=${2}
LFILE=${3}
NFILE=${4}
UFILE=${5}

${DB2} "connect to ${DATABASE}"
EFLAG=${?}
out -ne "Connecting to database ${DATABASE} ... "; fail $EFLAG; 

test ${EFLAG} -ne 0 && exit 1;

load_table "${NFILE}" "${SCHEMA}" "${TAB_NAMES}";
EFLAG=${?};
out -ne "Loading ${SCHEMA}.${TAB_NAMES} ... "; fail ${EFLAG};

load_table "${UFILE}" "${SCHEMA}" "${TAB_URIS}";
EFLAG=${?};
out -ne "Loading ${SCHEMA}.${TAB_URS} ... "; fail ${EFLAG};

load_table ${LFILE} "${SCHEMA}" "${TAB_DOC}";
EFLAG=${?};
out -ne "Loading ${SCHEMA}.${TAB_DOC} ... "; fail ${EFLAG};

${DB2} "terminate"
