

#include "sqlexecute.h"
#include <comm.h>
#include <sys/stat.h>

extern catalog *default_catalog_create( );
extern catalog *catalog_create_stream( stream *s, context *lc );

void usage( char *prog ){
	fprintf(stderr, "sql_client [ --debug | -d ] [ --host hostname ] [ --port portnr]\n" );
	exit(-1);
}

static char *readresult( stream *rs, char *buf ){ 
	char *start = buf;
	while(rs->read(rs, start, 1, 1)){
		if (*start == '\n' ){
			break;
		}
		start ++;
	}
	return start;
}

void execute( context *lc, stream *rs, char *cmd ){
	statement *res =  sqlexecute( lc, cmd );
	char buf[BUFSIZ+1], *start = buf;

	if (res){
	    int nr = 1;
	    statement_dump( res, &nr, lc );

	    buf[0] = 1;
	    lc->out->write( lc->out, buf, 1, 1 );
	    lc->out->flush( lc->out );
	}
	if (res && res->type == st_output){
		int nRow, nRows = 0;

		start = readresult( rs, buf);
		start = '\0';
		nRows = atoi(buf);

	        for( nRow = 0; nRow < nRows; nRow++){
			start = readresult( rs, buf );
			*(start+1) = '\0';
			printf(buf);
		}
	}
	if (res) statement_destroy(res);
}

void clientAccept( context *lc, stream *rs ){
	int	is_chrsp	= 0;
	char *prompt = "> ";
	char *line = NULL;
	struct stat st;

	fstat(fileno(stdin),&st);
	if (S_ISCHR(st.st_mode))
	   is_chrsp = 1;

	while(!feof(stdin)){
		if (line) {
			free(line);
		}
#ifdef HAVE_LIBREADLINE
		if (is_chrsp){
	        	if ((line = (char *) readline(prompt)) == NULL) {
	               		return;
			}
			add_history(line);				\
		} else 
#endif
		{
		   	char *buf =(char *)malloc(BUFSIZ);
	        	if ((line =(char *)fgets(buf,BUFSIZ,stdin))==NULL) {
			   	free(buf);
	                	return;
			}
		}
		execute(lc, rs, line);
	}
}

int
main(int ac, char **av)
{
	char *prog = *av, *host = "localhost";
	int debug = 0, error = 0, fd = 0, port = 45123;
	stream *ws = NULL, *rs = NULL;
	context lc;

	while(ac > 1 && !error){
		av++;
		if (av[0][0] == '-' && av[0][1] != '-'){
		    switch(av[0][1]){
		    case 'd':
			debug = 1;
			break;
		    default:
			error = 1;
		    } 
		} else if (av[0][0] == '-' && av[0][1] == '-'){
		    switch(av[0][2]){
		    case 'd':
			if (strcmp(av[0], "--debug") == 0)
				debug = 1;
			break;
		    case 'p':
			if (strcmp(av[0], "--port") == 0){
				av++; ac--;
				port = atoi(av[0]);
			}
			break;
		    case 'h':
			if (strcmp(av[0], "--host") == 0){
				av++; ac--;
				host = strdup(av[0]);
			}
			break;
		    default:
			error = 1;
		    } 
		} else {
			fprintf(stderr, "Error: unknown argument %s\n", av[0] );
			usage(prog);
		}
		ac--;
	}

	if (debug) fprintf(stderr, "debug on %d\n", debug );

	fd = client( host, port);
	rs = socket_rastream( fd, "sql client read");
	ws = socket_wastream( fd, "sql client write");
	if (rs->errnr || ws->errnr){
		fprintf(stderr, "sockets not opened correctly\n");
		exit(1);
	}
	sql_init_context( &lc, ws, debug, default_catalog_create() );
	catalog_create_stream( rs, &lc );
	clientAccept( &lc, rs );
	if (rs){
	       	rs->close(rs);
	       	rs->destroy(rs);
	}
	sql_exit_context( &lc );
	return 0;
} /* main */

