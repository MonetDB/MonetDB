monetdb create dbmaster
monetdb set master=true dbmaster
monetdb release dbmaster
mclient -lsql -d dbmaster -s "create table tmp(i integer);"

monetdb create dbslave
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave
monetdb release dbslave
mclient -lsql -d dbslave -s "create table tmp(i integer);"

mclient -lsql -d dbslave -s 'CREATE PROCEDURE startSync() EXTERNAL NAME slave."sync";'
mclient -lsql -d dbslave -s 'CREATE PROCEDURE startSync(tag integer) EXTERNAL NAME slave."sync";'
mclient -lsql -d dbslave -s 'CREATE PROCEDURE stopSync() EXTERNAL NAME slave."stop";'
mclient -lsql -d dbslave -s 'CREATE FUNCTION isSynchronizing() RETURNS boolean EXTERNAL NAME slave."issynchronizing";'

mclient -lsql -d dbslave -s 'call startSync();'
mclient -lsql -d dbslave -s 'select isSynchronizing();'
mclient -lsql -d dbslave -s 'call stopSync();'
mclient -lsql -d dbslave -s 'select isSynchronizing();'

mclient -lsql -d dbslave -s 'CREATE PROCEDURE resetSlaves(uri string) EXTERNAL NAME master."invalidate";'
mclient -lsql -d dbslave -s 'CREATE FUNCTION master() RETURNS string EXTERNAL NAME master."getURI";'
mclient -lsql -d dbslave -s 'CREATE FUNCTION cutOffTag() RETURNS string EXTERNAL NAME master."getCutOffTag";'
mclient -lsql -d dbslave -s 'select cutOffTag();'
mclient -lsql -d dbslave -s 'select master();'

monetdb stop dbmaster dbslave
monetdb destroy -f dbmaster dbslave

