#!/bin/sh

Mlog -x "$SQL_CLIENT $TSTSRCBASE/src/jdbc/tests/JdbcClient_create_tables.sql"
Mlog -x "$SQL_CLIENT $TSTSRCBASE/src/jdbc/tests/JdbcClient_inserts_selects.sql"
Mlog -x "$SQL_DUMP"
