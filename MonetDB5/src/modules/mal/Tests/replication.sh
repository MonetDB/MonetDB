# this script requires merovingian to run

# the minimal sequence to get a single record from master to slave

monetdb create dbmaster dbslave
monetdb set master=true dbmaster
monetdb release dbmaster
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave
monetdb release dbslave

# make two identical databases
mclient -lsql -d dbmaster -s "create table tmp(i integer);"
mclient -lsql -d dbslave -s "create table tmp(i integer);"
mclient -lsql -d dbslave -s 'create function slavelog ()
    returns table ("commit" timestamp, "valid" timestamp, "error" string)
    external name sql.dump_slave;'

mclient -lsql -d dbmaster -s "insert into tmp values(1);"
mclient -lsql -d dbmaster -s "select * from tmp;"

mclient -lsql -d dbslave -s "select * from tmp;"
mclient -lsql -d dbslave -s "select * from slavelog();"

#remove all stuff
monetdb stop -a
monetdb destroy -f dbmaster dbslave
