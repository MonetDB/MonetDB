#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

int main(int argc, char **argv){
	Mapi	dbh;
	int    port,rows, l;
	char   buf[40];

	if( argc != 2){
		printf("usage:%s <port>\n",argv[0]);
			exit(-1);
	}
	port = atol( argv[1]);

	snprintf(buf,40,"localhost:%d",port);
	dbh= mapi_connect(buf,"guest",0,"sql");
	if(mapi_error(dbh)) die(dbh);

	mapi_cache_limit(dbh,2);
	mapi_trace_log(dbh,"/tmp/mapilog");
	/* mapi_trace(dbh,1);*/
	if( mapi_query(dbh,"create table emp(name varchar,age int)")) die(dbh);

	if( mapi_query(dbh,"insert into emp values(\"John\", 23)") ) die(dbh);

	if( mapi_query(dbh,"insert into emp values(\"Mary\", 22)") ) die(dbh);

	if( mapi_query(dbh,"select * from emp") ) die(dbh);

	l= mapi_fetch_all(dbh);
	printf("rows received %d\n",l);
	while( mapi_fetch_row(dbh)){
		char *nme = mapi_fetch_field(dbh,0);
		char *age = mapi_fetch_field(dbh,1);
		printf("%s is %s\n", nme, age);
	}
	/* mapi_stat(dbh);
	printf("mapi_ping %d\n",mapi_ping(dbh)); */
	mapi_disconnect(dbh);

	return 0;
}
