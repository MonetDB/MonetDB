#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>
#include <string.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

int main(int argc, char **argv){
	Mapi	dbh;
	int	sqltest=0;
	if( argc != 4){
		printf("usage:%s <host> <port> <language>\n",argv[0]);
		exit(-1);
	}

	printf("Start %s test on %s\n",(sqltest?"sql":"mil"),argv[1]);
	dbh= mapi_connect(argv[1],atoi(argv[2]),"guest",0,argv[3]);
	if(mapi_error(dbh)) die(dbh);

	mapi_trace(dbh,1);
	if( strcmp(argv[3],"sql")==0 ){
		if( mapi_query(dbh,"create table emp(name varchar,age int)")) 
			die(dbh);
		if( mapi_query(dbh,"insert into emp values(\"John\", 23)") ) 
			die(dbh);
		if( mapi_query(dbh,"insert into emp values(\"Mary\", 22)") ) 
			die(dbh);
		if( mapi_query(dbh,"select * from emp") ) die(dbh);
	} else {
		if(mapi_query(dbh,"emp:= new(str,int);") != MOK) die(dbh);
		if(mapi_query(dbh,"emp.insert(\"John\",23);") != MOK) die(dbh);
		if(mapi_query(dbh,"emp.insert(\"Mary\",23);") != MOK) die(dbh);
		if(mapi_query(dbh,"print(emp);") != MOK) die(dbh);
	}

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
