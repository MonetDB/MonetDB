# this script requires merovingian 

# See if a slave can deal with actions of the master
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

mclient -lsql -d dbmaster -s "insert into tmp values(126);"
mclient -lsql -d dbmaster -s "select * from tmp;"

#log records are flushed with a small delay
sleep 7
mclient -lsql -d dbslave -s "select * from tmp;"
mclient -lsql -d dbslave -s "select * from slavelog();"

# the master stops the slaves
mclient -lmal -d dbmaster -s "master.stop();"

# extend the log record file
mclient -lsql -d dbmaster -s "insert into tmp values(264);"
mclient -lsql -d dbmaster -s "insert into tmp values(645);"
sleep 7
# slave should not see the changes
echo "only tuple 126 should be shown"
mclient -lsql -d dbslave -s "select * from tmp;"

# the master restarts feeding the slaves
mclient -lmal -d dbmaster -s "master.start();"
sleep 7
# slave should now see the changes
mclient -lsql -d dbslave -s "select * from tmp;"

#remove all stuff
monetdb stop dbmaster dbslave
monetdb destroy -f dbmaster dbslave

#remove local merovingian instance
kill $mero
