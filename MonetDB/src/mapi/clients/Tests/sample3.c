#include <stdio.h>
#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

int main(int argc, char **argv){
	Mapi	dbh;
	int    port,rows, i,j;
	char   buf[40];

	if( argc != 2){
		printf("usage:%s <port>\n",argv[0]);
			exit(-1);
	}
	port = atol( argv[1]);

	snprintf(buf,40,"localhost:%d",port);
	dbh= mapi_connect(buf,"guest",0,"sql");
	if(mapi_error(dbh)) die(dbh);

	/* mapi_trace(dbh,1);*/
	if( mapi_query(dbh,"create table emp(name varchar,age int)")) die(dbh);

	if( mapi_query(dbh,"insert into emp values(\"John\", 23)") ) die(dbh);

	if( mapi_query(dbh,"insert into emp values(\"Mary\", 22)") ) die(dbh);

	if( mapi_query(dbh,"select * from emp") ) die(dbh);

	/* Retrieve all tuples in the client cache first */
	rows= mapi_fetch_all_rows(dbh);
	printf("rows received %d with %d fields\n",rows, mapi_get_field_count(dbh));

	/* Interpret the cache as a two-dimensional array */
	for(i=0;i<rows;i++){
		if( mapi_seek_row(dbh,i)) break;
		for(j=0;j<mapi_get_field_count(dbh);j++){
			printf("%s=%s ", 
				mapi_get_name(dbh,j),
				mapi_fetch_field(dbh,j));
		}
		printf("\n");
	}
	if(mapi_error(dbh)) mapi_explain(dbh,stdout);
	mapi_disconnect(dbh);

	return 0;
}
