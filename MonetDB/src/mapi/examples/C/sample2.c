#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>
#include <string.h>

#define die(X) (mapi_explain(X, stdout), exit(-1))

int main(int argc, char **argv)
{
	/* a parameter binding test */
	char *nme= 0;
       	int age= 0;
	char *parm[]={"peter", "25", 0};
	Mapi dbh;
	MapiHdl hdl;

	if (argc != 4) {
		printf("usage:%s <host> <port> <language>\n", argv[0]);
		exit(-1);
	}

	dbh = mapi_connect(argv[1], atoi(argv[2]), "guest", 0, argv[3]);
	if (mapi_error(dbh))
		die(dbh);

	/* mapi_trace(dbh,1);*/
	if (strcmp(argv[3], "sql") == 0) {
		if ((hdl = mapi_query(dbh, "create table emp(name varchar, age int)")) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query_array(dbh, "insert into emp values(\"?\", ?)", parm)) == NULL) 
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "select * from emp")) == NULL)
			die(dbh);
	} else {
		if ((hdl = mapi_query(dbh, "emp:= new(str,int);")) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query_array(dbh, "emp.insert(\"?\",?);", parm)) == NULL)
			die(dbh);
		mapi_close_handle(hdl);
		if ((hdl = mapi_query(dbh, "print(emp);")) == NULL)
			die(dbh);
	}

	if (mapi_bind(hdl, 0, &nme))
		mapi_explain_query(hdl, stdout);
	if (mapi_bind_var(hdl, 1, MAPI_INT, &age))
		mapi_explain_query(hdl, stdout);
	while (mapi_fetch_row(hdl)) {
		printf("%s is %d\n", nme, age);
	}
	mapi_close_handle(hdl);
	mapi_disconnect(dbh);

	return 0;
}
