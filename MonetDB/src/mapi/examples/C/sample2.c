#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

int main(int argc, char **argv)
{
	/* a parameter binding test */
	char *nme= 0;
       	int   age= 0;
	char *parm[]={"peter","25",0};
	Mapi	dbh;

	if( argc != 3){
		printf("usage:%s <host>:<port> <language>\n",argv[0]);
			exit(-1);
	}

	dbh= mapi_connect(argv[1],"guest",0,argv[2]);
	if(mapi_error(dbh)) die(dbh);

	/* mapi_trace(dbh,1);*/
	if( strcmp(argv[2],"sql")==0){
		if( mapi_query(dbh,"create table emp(name varchar,age int)")) 
			die(dbh);
		if( mapi_query_array(dbh,"insert into emp values(\"?\", ?)",parm) )
			die(dbh);
		if( mapi_query(dbh,"select * from emp") ) die(dbh);
	} else {
		if( mapi_query(dbh,"emp:=new(str,int);")) die(dbh);
		if( mapi_query_array(dbh,"emp.insert(\"?\", ?)",parm) )
			die(dbh);
		if( mapi_query(dbh,"print(emp);") ) die(dbh);
	}

	if( mapi_bind(dbh,0,&nme) ) mapi_explain(dbh,stdout);
	if( mapi_bind_var(dbh,1,SQL_INT,&age) ) mapi_explain(dbh,stdout);
	while( mapi_fetch_row(dbh)){
		printf("%s is %d\n", nme, age);
	}
	mapi_disconnect(dbh);

	return 0;
}
