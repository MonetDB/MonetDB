

#include "mem.h"
#include <monet_options.h>
#include <comm.h>
#include <sys/stat.h>
#include <query.h>
#include <simple_prompt.h>

#ifdef HAVE_LIBREADLINE
#include <readline/readline.h>
#include <readline/history.h>
#endif

stream *ws = NULL, *rs = NULL;

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


static void forward_data(stream *out)
	/* read from stdin */
{
	char buf[BUFSIZ];
	int done = 0;

	while(!done){
		char *s = fgets(buf, BUFSIZ, stdin);
		if (!s) {
			done = 1;
			break;
		} else {
			int len = strlen(s);

		       	if (len == 1){
				done = 1;
				break;
			}
			out->write(out, buf, 1, len);
			fwrite(buf, 1, len, stdout);
		}
	}
	out->flush(out);
}


void receive( stream *rs, stream *out ){
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
			fprintf( stdout, "\n");
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
		if (type == QTABLE || type == QUPDATE){
			if (nRows > 1)
				printf("SQL  %d Rows affected\n", nRows );
			else if (nRows == 1)
				printf("SQL  1 Row affected\n" );
			else 
				printf("SQL  no Rows affected\n" );
		}
		if (type == QDATA){
			printf("forward data \n");
			forward_data(out);
			receive(rs, out);
		}
	} else if (flag != COMM_DONE){
		printf("flag %d\n", flag);
	}
}

int parse_line( const char *line )
{
	int cnt = 0;
	int ins = 0;
	int esc = 0;

	while (isspace(*line)) line++;

	if (*line && *line == '-' && line[1] == '-')
		return 0;
	for(;*line != 0; line++){
		if (esc){
			while (isdigit(*line)) 
				line++;
			esc = 0;
		} else if (ins &&  *line == '\''){
			ins = 0;
		} else if (*line == '\\'){
			esc = 1;
		} else if (*line == '\''){
			ins = 1;
		} else if (*line == ';'){
			cnt++;
		}
	}
	return cnt;
}

void clientAccept( stream *ws, stream *rs, char *prompt ){
	int  i,	is_chrsp	= 0;
	char *line = NULL;
	struct stat st;
	char buf[BUFSIZ];

	fstat(fileno(stdin),&st);
	if (S_ISCHR(st.st_mode))
	   is_chrsp = 1;

	while(!feof(stdin)){
		int cmdcnt = 0;
		if (line) {
			free(line);
		}
#ifdef HAVE_LIBREADLINE
		if (is_chrsp){
	        	if ((line = (char *) readline(prompt)) == NULL) {
	               		break;
			}
			add_history(line);
		} else 
#endif
		{
		   	char *buf =(char *)malloc(BUFSIZ);
	        	if ((line =(char *)fgets(buf,BUFSIZ,stdin))==NULL) {
			   	free(buf);
	                	break;
			}
		}
		cmdcnt = parse_line(line);
		ws->write( ws, line, strlen(line), 1 );
		ws->write( ws, "\n", 1, 1 );
		if (cmdcnt)
			ws->flush( ws );

		for (; cmdcnt > 0; cmdcnt--)
			receive(rs, ws);
	}
	
	i = snprintf(buf, BUFSIZ, "COMMIT;\n" );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );
	receive(rs, ws);

	/* client waves goodbye */
	buf[0] = EOT; 
	ws->write( ws, buf, 1, 1 );
	ws->flush( ws );
}

int
main(int ac, char **av)
{
	char *prompt = NULL;
	char buf[BUFSIZ];
	char *prog = *av, *config = NULL, *passwd = NULL, *user = NULL;
	char *login = NULL, *schema = NULL;
	int i = 0, debug = 0, fd = 0, port = 0, setlen = 0;
	opt 	*set = NULL;

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

		int c = getopt_long( ac, av, "c:d::h:p:P:u:", 
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
	if (fd < 0)
		return fd;

	prompt = mo_find_option(set, setlen, "sql_prompt");
	if (!prompt) prompt = strdup("Msql> ");

	rs = block_stream(socket_rstream( fd, "client read"));
	ws = block_stream(socket_wstream( fd, "client write"));
	if (rs->errnr || ws->errnr){
		fprintf(stderr, "sockets not opened correctly\n");
		exit(1);
	}
	/*
	 * New start client sequence
	 *
	 * 1) socket connect
	 * 2) send 'api(char api,int debug)' api("sql",0);
	 * 3) receive request for login 
	 * 4) send user/passwd
	 */
	if (!user)
		user = simple_prompt("User: ", BUFSIZ, 1 );
	if (!passwd)
		passwd = simple_prompt("Password: ", BUFSIZ, 0 );

	snprintf(buf, BUFSIZ, "api(sql,%d);\n", debug ); 
	ws->write( ws, buf, strlen(buf), 1 );
	ws->flush( ws );
	/* read login */
	login = readblock( rs );

	if (login) free(login);

	i = snprintf(buf, BUFSIZ, "login(%s,%s);\n", user, passwd );
	ws->write( ws, buf, i, 1 );
	ws->flush( ws );
	/* read schema */
	schema = readblock( rs );
	if (schema){
		char *s = strrchr(schema, '\n');
		if (s) *s = '\0';
	}

	if (strlen(schema) > 0){
		fprintf(stdout, "SQL  connected to 'monet database' using schema %s\n", schema ); 
		clientAccept( ws, rs, prompt );
	}

	if (schema) free(schema);
	if (user) free(user);
	if (passwd) free(passwd);
	if (prompt) free(prompt);

	if (rs){
	       	rs->close(rs);
	       	rs->destroy(rs);
	}
	ws->close(ws);
	ws->destroy(ws);

	mo_free_options(set,setlen);
	return 0;
} /* main */

