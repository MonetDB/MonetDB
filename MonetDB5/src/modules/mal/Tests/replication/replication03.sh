# this script requires merovingian 

# test for invalid transactions. avoid leaks and garbagge
merovingian
mero=$!

monetdb create dbmaster dbslave
monetdb set master=true dbmaster
monetdb release dbmaster

mclient -lsql -d dbmaster -s "create table tmp(i integer);"
mclient -lsql -d dbmaster -s "insert into tmp values(123);"
mclient -lsql -d dbmaster -s "insert into tmp values(234);"
mclient -lsql -d dbmaster -s "insert into tmp values(345);"
mclient -lsql -d dbmaster -s "select * from tmp;"

sleep 7
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave
monetdb release dbslave
mclient -lsql -d dbslave -s "create table tmp(i integer);"
mclient -lsql -d dbslave -s "select * from tmp;"
# register utility function
mclient -lsql -d dbslave -s 'create function slavelog ()
    returns table ("tid" integer, "commit" timestamp, "valid" timestamp, "error" string)
    external name sql.dump_slave;'
mclient -lsql -d dbslave -s "select * from slavelog();"

#remove all stuff
monetdb stop dbmaster dbslave
monetdb destroy -f dbmaster dbslave

#remove local merovingian instance
kill $mero
