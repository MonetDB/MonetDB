#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>
#include <stdio.h>

#define die(X) (mapi_explain(X, stdout), exit(-1))

int main(int argc, char **argv)
{
	Mapi dbh;
	MapiHdl hdl;
	int i, port;
	char buf[40];
	/* char *line; */

	if (argc != 2) {
		printf("usage:smack00 <port>\n");
		exit(-1);
	}
	port = atol(argv[1]);
	dbh = mapi_connect("localhost", port, "guest", 0, 0);
	if (mapi_error(dbh))
		die(dbh);

	for (i = 0; i < 20000; i++) {
		snprintf(buf, 40, "print(%d);", i);
		if ((hdl = mapi_query(dbh,buf)) == NULL)
			die(dbh);
		while (/*line=*/ mapi_fetch_line(hdl)) {
			/*printf("%s \n", line);*/
		}
		mapi_close_handle(hdl);
	}
	mapi_disconnect(dbh);

	return 0;
}
