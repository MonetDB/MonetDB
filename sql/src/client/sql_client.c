
#include <monet_options.h>
#include <string.h>
#include "mem.h"
#include "sqlexecute.h"
#include <comm.h>
#include <sys/stat.h>
#include <catalog.h>
#include <query.h>
#include <simple_prompt.h>

stream *ws = NULL, *rs = NULL;

/*
 * Debug levels
 * 	0 	no debugging
 * 	1 	continue in case of errors 
 * 	2	output additional time statements
 * 	4	not used
 * 	8	output code to stderr
 * 	16	output parsed SQL
 * 	32	execute but no output write to the client
 * 	64 	output code only, no excution on the server.
 *     128 	export code in xml.
 */ 
extern catalog *catalog_create_stream( stream *s, context *lc );

void usage( char *prog ){
	fprintf(stderr, "sql_client\n");
	fprintf(stderr, "\toptions:\n");
	fprintf(stderr, "\t\t -d          | --debug=[level]\n"); 
	fprintf(stderr, "\t\t -h hostname | --host=hostname  /* host to connect to */\n");
	fprintf(stderr, "\t\t -p portnr   | --port=portnr    /* port to connect to */\n");
	fprintf(stderr, "\t\t -u user     | --user=user      /* user id */\n" );
	fprintf(stderr, "\t\t -P passwd   | --passwd=passwd  /* password */\n");
	exit(-1);
}

void receive( stream *rs ){
	int flag = 0;
	if (stream_readInt(rs, &flag) && flag != COMM_DONE){
		char buf[BLOCK+1], *n = buf;
		int last = 0;
		int type;
		int status;
		int nRows;

		stream_readInt(rs, &type);
		stream_readInt(rs, &status);
		if (status < 0){ /* output error */
			int nr = bs_read_next(rs,buf,&last);
			fprintf( stdout, "SQL ERROR %d: ", status );
			fwrite( n, nr, 1, stdout );
			while(!last){
				int nr = bs_read_next(rs,buf,&last);
				fwrite( buf, nr, 1, stdout );
			}
		}
		nRows = status;
		if (type == QTABLE && nRows > 0){
			int nr = bs_read_next(rs,buf,&last);
	
			fwrite( buf, nr, 1, stdout );
			while(!last){
				int nr = bs_read_next(rs,buf,&last);
				fwrite( buf, nr, 1, stdout );
			}
		}
		if (type == QTABLE){ /*|| type == QUPDATE){ */
			if (nRows > 1)
				printf("%d Rows affected\n", nRows );
			else if (nRows == 1)
				printf("1 Row affected\n" );
			else 
				printf("no Rows affected\n" );
		}
	} else if (flag != COMM_DONE){
		printf("flag %d\n", flag);
	}
}

static void forward_data(stream *in, context *sql)
{
	char buf[BUFSIZ];
	char *s = buf, *e = buf + BUFSIZ;
	char ch;
	int c = EOF;
	int done = 0;
	int newline = 0;

	while(!done){
		s = buf;
		while(s<e){
			if (in->read(in, &ch, 1, 1) == 1) {
				c = (int) ch;
			} else {
				sql->cur = EOF;
				done = 1;
				break;
			}
			*s++ = c;
			if (c == '\n'){
				if (newline){
					done = 1;
					break;
				} else {
					newline = 1;
				}
			} else {
				newline = 0;
			}
		}
		sql->out->write(sql->out, buf, 1, s-buf);
	}
}


int clientAccept( context *lc, stream *rs ){
	int err = 0;
	int i;
	stream *in = file_rastream( stdin, "<stdin>" );
	char buf[BUFSIZ];

	while(lc->cur != EOF ){
		stmt *s = sqlnext(lc, in, &err);
		if (!s && err){
			printf("%s\n", lc->errstr );
			printf("in -> %s\n", lc->sql );
			lc->errstr[0] = '\0';
			if (!(lc->debug & 1))
		       		break;
			else 
				err = 0;
		}
		if (s){
	    		int nr = 1;
			if (lc->debug&128){
	    			stmt2xml( s, &nr, lc );
				stmt_reset( s );
				nr= 1;
			} 
	    		stmt_dump( s, &nr, lc );
	    		lc->out->flush( lc->out );

			if (s && s->op1.stval->type == st_copyfrom &&
				!stmt_copyfrom_files(s)){
				forward_data(in, lc);
	    			lc->out->flush( lc->out );
			}
		}
		if (!(lc->debug&64) && s){
			receive(rs);
		}
		if (s) stmt_destroy(s);
	}
	in->destroy(in);

	if (!err){
		i = snprintf(buf, BUFSIZ, "s0 := mvc_commit(myc, 0, \"\");\n" );
		i += snprintf(buf+i, BUFSIZ-i, "result(Output, mvc_type(myc), mvc_status(myc));\n" );
		ws->write( ws, buf, i, 1 );
		ws->flush( ws );
		receive(rs);
	}

	/* client waves goodbye */
	buf[0] = EOT; 
	ws->write( ws, buf, 1, 1 );
	ws->flush( ws );

	return err;
}

int
main(int ac, char **av)
{
	char buf[BUFSIZ];
	char *prog = *av, *config = NULL, *passwd = NULL, *user = NULL;
	char *login = NULL, *db = NULL, *schema = NULL;
	int i = 0, debug = 0, fd = 0, port = 0, setlen = 0, res = 0;
	opt 	*set = NULL;
	context lc;

	static struct option long_options[] =
             {
               {"debug", 2, 0, 'd'},
	       {"config", 1, 0, 'c'},
               {"host", 1, 0, 'h'},
               {"port", 1, 0, 'p'},
               {"passwd", 1, 0, 'P'},
               {"user", 1, 0, 'u'},
               {0, 0, 0, 0}
             };

	if (!(setlen = mo_builtin_settings(&set)) )
                usage(prog);

	while(1){
		int option_index = 0;

		int c = getopt_long( ac, av, "d::c:h:p:P:u:", 
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
		case 'c':
			config = strdup(optarg);
			break;
		case 'd':
			if (optarg){ 
				setlen = mo_add_option( &set, setlen, 
					opt_cmdline, "sql_debug", optarg );
			} else {
				setlen = mo_add_option( &set, setlen, 
					opt_cmdline, "sql_debug", "2" );
			}
			break;
		case 'h':
			setlen = mo_add_option( &set, setlen, 
					opt_cmdline, "host", optarg );
			break;
		case 'p':
			setlen = mo_add_option( &set, setlen, 
					opt_cmdline, "sql_port", optarg );
			break;
		case 'P':
			passwd=strdup(optarg);
			break;
		case 'u':
			user=strdup(optarg);
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

	if (config){
		setlen = mo_config_file(&set, setlen, config );
		free(config);
	} else {
		if (!(setlen = mo_system_config(&set, setlen)) )
			usage(prog);
	}

	stream_init();
	debug = strtol(mo_find_option(set, setlen, "sql_debug"), NULL, 10);
	port = strtol(mo_find_option(set, setlen, "sql_port"), NULL, 10);
	fd = client( mo_find_option(set, setlen, "host"), port);
	rs = block_stream(socket_rstream( fd, "sql client read"));
	ws = block_stream(socket_wstream( fd, "sql client write"));
	if (rs->errnr || ws->errnr){
		fprintf(stderr, "sockets not opened correctly\n");
		exit(1);
	}

	if (!user)
		user = simple_prompt("User: ", BUFSIZ, 1 );
	if (!passwd)
		passwd = simple_prompt("Password: ", BUFSIZ, 0 );

	i = snprintf(buf, BUFSIZ, "api(milsql,%d);\n", debug );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );
	/* read login */
	login = readblock( rs );

	if (login) free(login);

	i = snprintf(buf, BUFSIZ, "login(%s,%s);\n", user, passwd );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );
	/* read database, schema */
	db = readblock( rs );
	if (db){
		char *s = strrchr(db, ',');
		if (s){ 
			*s = '\0';
			schema = s+1;
			s = strrchr(schema, '\n');
			if (s){ 
				*s = '\0';
			}
		}
	}
	if (strlen(schema) > 0){
		fprintf(stdout, "SQL  connected to database %s using schema %s\n", db, schema ); 

		if (debug&64) fprintf(stdout, buf );

		memset(&lc, 0, sizeof(lc));
		sql_init_context( &lc, ws, debug, default_catalog_create() );
		catalog_create_stream( rs, &lc );

		lc.cat->cc_getschemas( lc.cat, schema, user );
		lc.cur = ' ';
		if (debug&64){
			ws = lc.out;
			lc.out = file_wastream(stdout, "<stdout>" );
		}
		res = clientAccept( &lc, rs );

		if (debug&64){
	       		lc.out->close(lc.out);
	       		lc.out->destroy(lc.out);
			lc.out = ws;
		}
		sql_exit_context( &lc );
	} else {
		if (ws){
	       		ws->close(ws);
	       		ws->destroy(ws);
		}
	}
	if (db) free(db); /* db + schema */
	if (user) free(user);
	if (passwd) free(passwd);

	if (rs){
	       	rs->close(rs);
	       	rs->destroy(rs);
	}
	mo_free_options(set,setlen);
	return -res;
} /* main */
