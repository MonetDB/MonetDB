#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>
#include <string.h>

#define die(X) (mapi_explain(X, stdout), exit(-1))

int main(int argc, char **argv)
{
	Mapi dbh;
	MapiHdl hdl;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "guest", 0, argv[3]);
	if (mapi_error(dbh))
		die(dbh);

	/* mapi_trace(dbh, 1); */
	mapi_cache_limit(dbh, 2);
	if (strcmp(argv[3], "sql") == 0) {
		if ((hdl = mapi_query(dbh, "create table emp(name varchar, age int)")) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "insert into emp values(\"John\", 23)")) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "insert into emp values(\"Mary\", 22)")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "select * from emp")) == NULL)
			die(dbh);
	} else {
		if ((hdl = mapi_query(dbh, "emp:= new(str,int);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "emp.insert(\"John\",23);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "emp.insert(\"Mary\",22);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "print(emp);")) == NULL)
			die(dbh);
	}

	while (mapi_fetch_row(hdl)) {
		char *nme = mapi_fetch_field(hdl, 0);
		char *age = mapi_fetch_field(hdl, 1);
		printf("%s is %s\n", nme, age);
	}
	/* mapi_stat(dbh);
	printf("mapi_ping %d\n",mapi_ping(dbh)); */
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);

	return 0;
}
