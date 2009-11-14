# this script requires merovingian 

# cut of the master after the first insert
# see if the second slave can pick up the new state
merovingian
mero=$!

monetdb create dbmaster dbslave 
monetdb set master=true dbmaster
monetdb release dbmaster
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave 
monetdb release dbslave 

# make identical databases
mclient -lsql -d dbmaster -s "create table tmp(i integer);"
mclient -lsql -d dbslave -s "create table tmp(i integer);"

mclient -lsql -d dbmaster -s "insert into tmp values(127);"

sleep 7
# the master invalidates the snapshot
mclient -lmal -d dbmaster -s "master.invalidate();"

# add more records to its state
mclient -lsql -d dbmaster -s "insert into tmp values(274);"
mclient -lsql -d dbmaster -s "insert into tmp values(745);"

# create a new slave, suspend master to avoid early forward pushing of logs
monetdb lock dbmaster  
monetdb create dbslave2
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave2
monetdb release dbslave2
mclient -lsql -d dbslave2 -s "create table tmp(i integer);"
#prepare for re-synchronization
monetdb stop dbslave2
monetdb release dbmaster
monetdb start dbslave2

sleep 7
echo " master has all records"
mclient -lsql -d dbmaster -s "select * from tmp;"
echo " slave should only have received 127"
mclient -lsql -d dbslave -s "select * from tmp;"
echo " slave should only have received 274, 745"
mclient -lsql -d dbslave2 -s "select * from tmp;"

#remove all stuff
monetdb stop dbmaster dbslave dbslave2
monetdb destroy -f dbmaster dbslave dbslave2

#remove local merovingian instance
kill $mero
