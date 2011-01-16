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
# the master freeze the snapshot at dbslave
mclient -lmal -d dbmaster -s "master.freeze();"

# add more records to its state
mclient -lsql -d dbmaster -s "insert into tmp values(274);"
mclient -lsql -d dbmaster -s "insert into tmp values(745);"

# from here dbslave is frozen, does not accept these updates
sleep 7
echo " slave should only have 127"
mclient -lsql -d dbslave -s "select * from tmp;"

# create a new slave
monetdb create dbslave2
monetdb set slave=`mclient -lmal -d dbmaster -s"master.getURI();"` dbslave2
monetdb release dbslave2

# the master log stream has been stopped by freeze()
sleep 7
mclient -lsql -d dbslave2 -s 'CREATE FUNCTION synchronizing() RETURNS boolean EXTERNAL NAME slave."synchronizing";'
mclient -lsql -d dbslave2 -s "select synchronizing();"

# create the table and initiate the synchronisation
mclient -lsql -d dbslave2 -s "create table tmp(i integer);"
mclient -lsql -d dbslave2 -s 'CREATE PROCEDURE startSync() EXTERNAL NAME slave."sync";'
mclient -lsql -d dbslave2 -s "call startSync();"

#wait for the stuff
sleep 7
echo " master has all records"
mclient -lsql -d dbmaster -s "select * from tmp;"
echo " slave should only have received 274, 745"
mclient -lsql -d dbslave2 -s "select * from tmp;"

#remove all stuff
monetdb stop dbmaster dbslave dbslave2
monetdb destroy -f dbmaster dbslave dbslave2

#remove local merovingian instance
kill $mero
