#!/bin/sh

Mlog -x "$SQL_CLIENT ${TSTSRCDIR}/JdbcClient_create_tables.sql"
Mlog -x "$SQL_CLIENT ${TSTSRCDIR}/JdbcClient_inserts_selects.sql"
Mlog -x "$SQL_DUMP"
