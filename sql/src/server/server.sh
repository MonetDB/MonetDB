#!/bin/bash

#$MONET_PREFIX/share/Monet/mapi.mil \

Mserver --config ~/.monet.conf.`uname -s` --dbname sql \
	$SQL_PREFIX/share/sql/catalog.mil $SQL_PREFIX/share/sql/sqlserver.mil $*
