#include <stdio.h>
#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

main(int argc, char *argv){
	Mapi	dbh;

	dbh= mapi_connect("localhost:50001","guest",0,"sql");
	if(mapi_error(dbh)) die(dbh);

	/* mapi_trace(dbh,1);*/
	mapi_cache_limit(dbh,2);
	if( mapi_query(dbh,"create table emp(name varchar,age int)")) die(dbh);

	if( mapi_query(dbh,"insert into emp values(\"John\", 23)") ) die(dbh);

	if( mapi_query(dbh,"insert into emp values(\"Mary\", 22)") ) die(dbh);

	if( mapi_query(dbh,"select * from emp") ) die(dbh);

	while( mapi_fetch_row(dbh)){
		char *nme = mapi_fetch_field(dbh,0);
		char *age = mapi_fetch_field(dbh,1);
		printf("%s is %s\n", nme, age);
	}
	/* mapi_stat(dbh);
	printf("mapi_ping %d\n",mapi_ping(dbh)); */
	mapi_disconnect(dbh);
}
