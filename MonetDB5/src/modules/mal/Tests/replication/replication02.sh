# this script requires merovingian 

# See if a slave can deal with invalidation actions of the master
merovingian
mero=$!

monetdb create dbmaster dbslave
monetdb set master=true dbmaster
monetdb release dbmaster
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave
monetdb release dbslave

# make two identical databases
mclient -lsql -d dbmaster -s "create table tmp(i integer);"
mclient -lsql -d dbslave -s "create table tmp(i integer);"

# register utility function
mclient -lsql -d dbslave -s 'create function slavelog ()
    returns table ("tid" integer, "commit" timestamp, "valid" timestamp, "error" string)
    external name sql.dump_slave;'

mclient -lsql -d dbmaster -s "insert into tmp values(127);"
mclient -lsql -d dbmaster -s "select * from tmp;"

#log records are flushed with a small delay
sleep 7
mclient -lsql -d dbslave -s "select * from tmp;"
mclient -lsql -d dbslave -s "select * from slavelog();"

# the master stops the slaves
mclient -lmal -d dbmaster -s "master.freeze();"

# extend the log record file
mclient -lsql -d dbmaster -s "insert into tmp values(274);"
mclient -lsql -d dbmaster -s "insert into tmp values(745);"
sleep 7
# slave should not see the changes
echo "only tuple 127 should be shown"
mclient -lsql -d dbslave -s "select * from tmp;"

monetdb stop dbslave
# the master restarts feeding the slaves
# slave should not see the changes after a restart either
mclient -lsql -d dbslave -s "select * from tmp;"

mclient -lmal -d dbmaster -s "c:= master.getCutOffTag(); io.print(c);"
mclient -lmal -d dbslave -s "c:= slave.synchronizing(); io.print(c);"
#remove all stuff
monetdb stop dbmaster dbslave
monetdb destroy -f dbmaster dbslave

#remove local merovingian instance
kill $mero
