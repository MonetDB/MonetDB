#include <stdio.h>
#include <monet_utils.h>
#include <stream.h>
#include <Mapi.h>

#define die(X) {mapi_explain(X,stdout); exit(-1); }

int main(int argc, char **argv){
	Mapi	dbh;
	int i;
	char buf1[40],buf[40], *line;
	int port;

	if( argc != 2){
		printf("usage: smack01 <port>\n");
		exit(-1);
	}
	port= atol(argv[1]);
	snprintf(buf1,40,"localhost:%d",port);


	for(i=0; i< 1000; i++){
		/* printf("setup connection %d\n", i);*/
		dbh= mapi_connect(buf1,"guest",0,0);
		if(mapi_error(dbh)) die(dbh);
		snprintf(buf,40,"print(%d);",i);
		if( mapi_query(dbh,buf)) die(dbh);
		while((line= mapi_fetch_line(dbh))){
			printf("%s \n", line );
		}
		mapi_disconnect(dbh);
		/* printf("close connection %d\n",i);*/
	}

	return 0;
}
