#!/bin/sh

Mserver -db sql \
	$SQL_PREFIX/share/sql/catalog.mil $MONET_PREFIX/share/Monet/mapi.mil \
	$SQL_PREFIX/share/sql/sqlserver.mil 
