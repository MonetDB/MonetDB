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
	if( mapi_query(dbh,"create table emp(name varchar,age int)")) die(dbh);

	/* a parameter binding test */
	char *nme= 0;
       	int   age= 0;
	char *parm[]={"peter","25",0};
	if( mapi_query_array(dbh,"insert into emp values(\"?\", ?)",parm) )
		die(dbh);

	if( mapi_query(dbh,"select * from emp") ) die(dbh);
	if( mapi_bind(dbh,0,&nme) ) mapi_explain(dbh,stdout);
	if( mapi_bind_var(dbh,1,SQL_INT,&age) ) mapi_explain(dbh,stdout);
	while( mapi_fetch_row(dbh)){
		printf("%s is %d\n", nme, age);
	}
	mapi_disconnect(dbh);
}
