#!/bin/sh

#$MONET_PREFIX/share/Monet/mapi.mil \

Mserver --debug=34 --config ~/.monet.conf --dbname sql \
	$SQL_PREFIX/share/sql/catalog.mil $SQL_PREFIX/share/sql/sqlserver.mil 
