#include <stdio.h>
#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

int main(int argc, char **argv){
	Mapi	dbh;
	int    rows, i,j;

	if( argc != 3){
		printf("usage:%s <host>:<port> <language>\n",argv[0]);
			exit(-1);
	}

	dbh= mapi_connect(argv[1],"guest",0,argv[2]);
	if(mapi_error(dbh)) die(dbh);

	/* mapi_trace(dbh,1);*/
	if(strcmp(argv[2],"sql")==0){
		if( mapi_query(dbh,"create table emp(name varchar,age int)")) 
			die(dbh);
		if( mapi_query(dbh,"insert into emp values(\"John\", 23)") ) 
			die(dbh);
		if( mapi_query(dbh,"insert into emp values(\"Mary\", 22)") ) 
			die(dbh);
		if( mapi_query(dbh,"select * from emp") ) die(dbh);
	} else {
		if( mapi_query(dbh,"emp:= new(str,int);") != MOK) die(dbh);
		if( mapi_query(dbh,"emp.insert(\"John\",23);") != MOK) die(dbh);
		if( mapi_query(dbh,"emp.insert(\"Mary\",23);") != MOK) die(dbh);
		if( mapi_query(dbh,"print(emp);") != MOK) die(dbh);
	}

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
