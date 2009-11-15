# this script requires merovingian 

# test resilience against synchronization violation
merovingian
mero=$!

monetdb create dbmaster dbslave 
monetdb set master=true dbmaster
monetdb release dbmaster
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave 
monetdb release dbslave 

# make identical databases
mclient -lsql -d dbmaster -s "create table tmp(i integer primary key);"
mclient -lsql -d dbslave -s "create table tmp(i integer primary key);"

mclient -lsql -d dbmaster -s "insert into tmp values(127);"

sleep 7
# the slave deviates from the snapshot 
mclient -lsql -d dbslave -s "insert into tmp values(127);"
mclient -lsql -d dbslave -s "insert into tmp values(128);"

echo " slave should have 127, 128"
mclient -lsql -d dbslave -s "select * from tmp;"

# master follows
mclient -lsql -d dbmaster -s "insert into tmp values(128);"
mclient -lsql -d dbmaster -s "insert into tmp values(129);"

sleep 7
# slave has broken the dependency and is frozen
echo " master should have 127, 128,129"
mclient -lsql -d dbmaster -s "select * from tmp;"
echo " slave should have 127, 128"
mclient -lsql -d dbslave -s "select * from tmp;"

#remove all stuff
monetdb stop dbmaster dbslave 
monetdb destroy -f dbmaster dbslave 

#remove local merovingian instance
kill $mero
