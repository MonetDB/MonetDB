

#include "mem.h"
#include "sqlexecute.h"
#include <comm.h>
#include <sys/stat.h>
#include <catalog.h>

#ifdef HAVE_GETOPT_H 
#include <getopt.h>
#endif

stream *ws = NULL, *rs = NULL;

extern catalog *catalog_create_stream( stream *s, context *lc );

void usage( char *prog ){
	fprintf(stderr, "sql_client\n");
	fprintf(stderr, "\toptions:\n");
	fprintf(stderr, "\t\t -d          | --debug=[level]\n"); 
	fprintf(stderr, "\t\t -h hostname | --host=hostname  /* host to connect to */\n");
	fprintf(stderr, "\t\t -p portnr   | --port=portnr    /* port to connect to */\n");
	fprintf(stderr, "\t\t -s schema   | --schema=schema  /* schema to use */\n");
	fprintf(stderr, "\t\t -u user     | --user=user      /* user id */\n" );
	fprintf(stderr, "\t\t -o level    | --optimize=level /* optimzation level */\n" );
	exit(-1);
}

#define CHUNK (64*1024)

static char *readblock( stream *s ){
	int len = 0;
	int size = CHUNK + 1;
	char *buf = NEW_ARRAY(char, size ), *start = buf;

	while ((len = s->read(s, start, 1, CHUNK)) == CHUNK){
		size += CHUNK;
		buf = RENEW_ARRAY(char, buf, size); 
		start = buf + size - CHUNK - 1;
		*start = '\0';
	}
	start += len;
	*start = '\0';
	return buf;
}

void execute( context *lc, stream *rs, char *cmd ){
	statement *res =  sqlexecute( lc, cmd );

	if (res){
	    int nr = 1;
	    statement_dump( res, &nr, lc );

	    lc->out->flush( lc->out );
	}
	if (res && res->type == st_output){
		int nRows = 0;
		char *buf = readblock( rs ), *n = buf;
		
		nRows = strtol(n,&n,10);
		n++;

		printf("%s", n);
		if (nRows > 1)
			printf("%d Rows affected\n", nRows );
		else if (nRows == 1)
			printf("1 Row affected\n" );
		else 
			printf("no Rows affected\n" );
		_DELETE(buf);
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
		if (strcmp(line, "quit;\n") == 0){
			printf("quiting\n");
			exit(1);
		}
		execute(lc, rs, line);
	}
}

void clientAccept_new( context *lc, stream *rs ){
	int err = 0;
	stream *in = file_rastream( stdin, "<stdin>" );
	statement *s = NULL;

	while(lc->cur != EOF ){
		s = sqlnext(lc, in, &err);
		if (err) break;
		if (s){
	    		int nr = 1;
	    		statement_dump( s, &nr, lc );

	    		lc->out->flush( lc->out );
		}
		if (!(lc->debug&64) && s && s->type == st_output){
			int nRows = 0;
			char *buf = readblock( rs ), *n = buf;
			
			nRows = strtol(n,&n,10);
			n++;
	
			printf("%s", n);
			if (nRows > 1)
				printf("%d Rows affected\n", nRows );
			else if (nRows == 1)
				printf("1 Row affected\n" );
			else 
				printf("no Rows affected\n" );
			_DELETE(buf);
		}
		if (s) statement_destroy(s);
	}
}


/*
When using alloca(3) in a shared library, Intel's "C++ Compiler for 32-bit
applications, Version 5.0.1 Beta Build 010528D0" seems to require another
reference to alloca(3) in the file that contains the main(), otherwise it
complains about an "undefined reference to `\_alloca\_probe' when linking
the shared library to the executable.
Hence, we define this FAKE\_ALLOCA\_CALL and call it in the respective
main()s just before the final return.
*/
#if ( defined(__INTEL_COMPILER) && (!defined(STATIC)) )
# include <alloca.h>
# define FAKE_ALLOCA_CALL (void)alloca(0);
#else
# define FAKE_ALLOCA_CALL ;
#endif

int
main(int ac, char **av)
{
	char *schema = NULL;
	char *user = NULL;
	char *prog = *av, *host = "localhost";
	int debug = 0, fd = 0, port = 45123;
	int opt = SQL_FAST_INSERT;
	context lc;

	static struct option long_options[] =
             {
               {"debug", 2, 0, 'd'},
               {"host", 1, 0, 'h'},
               {"port", 1, 0, 'p'},
               {"schema", 1, 0, 's'},
               {"user", 1, 0, 'u'},
               {"optimize", 1, 0, 'o'},
               {0, 0, 0, 0}
             };

	while(1){
		int option_index = 0;

		int c = getopt_long( ac, av, "dh:p:s:u:o:", 
				long_options, &option_index);

		if (c == -1)
			break;

		switch (c){
		case 0:
			/* all long options are mapped on their short version */
			printf("option %s", long_options[option_index].name);
			if (optarg)
				printf( " with arg %s", optarg );
			printf("\n");
			usage(prog);
			break;
		case 'd':
			debug=2;
			if (optarg) debug=strtol(optarg,NULL,10);
			break;
		case 'h':
			host=_strdup(optarg);
			break;
		case 'p':
			port=strtol(optarg,NULL,10);
			break;
		case 's':
			schema=_strdup(optarg);
			break;
		case 'u':
			user=_strdup(optarg);
			break;
		case 'o':
			opt=strtol(optarg,NULL,10);
			break;
		case '?':
			usage(prog);
		default:
			printf( "?? getopt returned character code 0%o ??\n",c);
			usage(prog);
		}
	}
	if (optind < ac){
		printf("some arguments are not parsed by getopt\n");
		while(optind < ac)
			printf("%s ", av[optind++]);
		printf("\n");
		usage(prog);
	}

	fd = client( host, port);
	rs = block_stream(socket_rstream( fd, "sql client read"));
	ws = block_stream(socket_wstream( fd, "sql client write"));
	if (rs->errnr || ws->errnr){
		fprintf(stderr, "sockets not opened correctly\n");
		exit(1);
	}
	sql_init_context( &lc, ws, debug, default_catalog_create() );
	lc.optimize = opt;
	catalog_create_stream( rs, &lc );
	if (!schema) schema = _strdup("default-schema");
	if (!user) user = _strdup("default-user");
	lc.cat->cc_getschema( lc.cat, schema, user );
	lc.cur = ' ';
	if (debug&64){
		ws = lc.out;
		lc.out = file_wastream(stdout, "<stdout>" );
	}
	clientAccept_new( &lc, rs );
	if (rs){
	       	rs->close(rs);
	       	rs->destroy(rs);
	}
	if (debug&64){
	       	lc.out->close(lc.out);
	       	lc.out->destroy(lc.out);
		lc.out = ws;
	}
	sql_exit_context( &lc );
	FAKE_ALLOCA_CALL; /* buggy Intel C/C++ compiler for Linux ... */
	return 0;
} /* main */

