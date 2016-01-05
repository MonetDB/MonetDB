/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @- Online help
 * The textual interface @sc{mclient} supports a limited
 * form of online help commands.
 * The argument is a (partial) operator call,
 * which is looked up in the symbol table.
 * If the pattern includes a '(' it also
 * displays the signature for each match.
 * The @sc{argument types} and @sc{address} attributes
 * are also shown if the call contains the closing bracket ')'.
 * @example
 * >?bat.is
 * bat.isSynced
 * bat.isCached
 * bat.isPersistent
 * bat.isTransient
 * bat.isSortedReverse
 * bat.isSorted
 * bat.isaSet
 * bat.isaKey
 * >?bat.isSorted(
 * command bat.isSorted(b:bat[:any_1,:any_2]):bit
 * >?bat.isSorted()
 * command bat.isSorted(b:bat[:any_1,:any_2]):bit address BKCisSorted;
 * Returns whether a BAT is ordered on head or not.
 * @end example
 *
 * The module and function names can be replaced by the
 * wildcard character '*'. General regulat pattern matching is not supported.
 * @example
 * >?*.print()
 * command color.print(c:color):void
 * pattern array.print(a:bat[:any_1,:any_2],b:bat[:any_1,:int]...):void
 * pattern io.print(b1:bat[:any_1,:any]...):int
 * pattern io.print(order:int,b:bat[:any_1,:any],b2:bat[:any_1,:any]...):int
 * pattern io.print(val:any_1):int
 * pattern io.print(val:any_1,lst:any...):int
 * pattern io.print(val:bat[:any_1,:any_2]):int
 * @end example
 *
 * The result of the help command can also be obtained in a BAT,
 * using the commands @sc{manual.help}.
 * Keyword based lookup is supported by the operation @sc{manual.search};
 * Additional routines are available in the @sc{inspect}
 * module to build reflexive code.
 *
 * For console input the @sc{readline} library linked with
 * the system provides a history mechanism and also name completion.
 * Add readline functionality to the MAL console.
 * This means that the user has history access and some other
 * features to assemble a command before it is being interpreted.
 */
#include "monetdb_config.h"
#include "mal.h"
#undef PATHLENGTH
#include "mal_client.h"
#include "mal_scenario.h"
#include "mal_readline.h"

/* #define _MAL_READLINE_DEBUG  */

#ifdef HAVE_LIBREADLINE

#include <readline/readline.h>
#include <readline/history.h>
#include "mal_debugger.h"

void init_readline(void);
void deinit_readline(void);
rl_completion_func_t *suspend_completion(void);
void continue_completion(rl_completion_func_t * func);

#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif


static const char *mal_commands[] = {
	"address",
	"atom",
	"barrier",
	"catch",
	"command",
	"comment",
	"exit",
	"end",
	"function",
	"factory",
	"leave",
	"pattern",
	"module",
	"raise",
	"redo",
	0
};
const char *mdb_commands[] = {
	"atoms",
	"break",
	"breakpoints",
	"call",
	"continue",
	"delete",
	"down",
	"exit",
	"finish",
	"help",
	"info",
	"io",
	"list",
	"List",
	"next",
	"module",
	"modules",
	"optimizer",
	"print",
	"quit",
	"run",
	"step",
	"trace",
	"timer",
	"up",
	"var",
	"where",
	0
};
static int malcommandlimit = 15;

static void mal_help_display(char **msg, int a, int b){
	int i;
	(void) msg; (void) a; (void) b;
	for(i=0;i<a; i++)
		mnstr_printf(GDKout,"%s\n",msg[i]);
}
/*
 * @-
 */
#ifndef HAVE_STRNCASECMP
#define strncasecmp strncmp
#endif

static char *
mal_command_generator(const char *text, int state)
{
	static int index, len, last;
	static char **msg =0;
	const char *name;
	int i;

#ifdef _MAL_READLINE_DEBUG
	printf("expand:%d [" SZFMT "] %s \n",state,strlen(text),text);
#endif
	if (!state) {
		index = 0;
		last = 0;
		len = (int) strlen(text);
		if( msg){
			for(i=0; msg[i]; i++)
				GDKfree(msg[i]);
			GDKfree(msg);
		}
		msg = 0;
	} else
	if( last >0){
		mal_unquote(msg[--last]);
		return strdup(msg[last]);
	}
	if( mdbSession() ){
		while ( (name = mdb_commands[index++]) ){
				if (strncasecmp(name,text,len) == 0)
					return strdup(name);
		}
		return NULL;
	}
	while (index < malcommandlimit && (name = mal_commands[index++])) {
			if (strncasecmp(name,text,len) == 0)
				return strdup(name);
	}
	if( msg == 0 && *text){
		char cmd[BUFSIZ], *c;
		c= strstr(text,":=");
		if(c) text=c+2;
		while(isspace((int)*text) ) text++;
		c= strchr(text,'.');
		if( c== NULL)
			snprintf(cmd,BUFSIZ,"%s.*(",text);
		else
			snprintf(cmd,BUFSIZ,"%s(",text);
		msg= getHelp(mal_clients->nspace,(str)cmd,1);
		for(last=0; msg[last]; last++)
			;
	}
	if(msg && last && msg[--last]){
		mal_unquote(msg[last]);
		return strdup(msg[last]);
	}
	return NULL;
}

int rl_complete(int ignore, int key){
	char *msg[1000];
	int i,top=0;

	putchar('\n');
	while( (msg[top]= mal_command_generator(rl_line_buffer,top)) )
		if( ++top== 1000) break;
	for(i=0;i<top; i++)
		printf("%s\n",msg[i]);

	printf("%s%s",rl_prompt,rl_line_buffer);
	for( top--;top>=0; top--)
		free(msg[top]);
	(void) ignore;
	(void) key;
	return 0;
}

static char **
mal_completion(const char *text, int start, int end)
{
	(void) start;
	(void) end;

	/* FIXME: Nice, context-sensitive completion strategy should go here */
	return rl_completion_matches(text, mal_command_generator);
}

rl_completion_func_t *
suspend_completion(void)
{
	rl_completion_func_t *func = rl_attempted_completion_function;

	rl_attempted_completion_function = NULL;
	return func;
}

void
continue_completion(rl_completion_func_t * func)
{
	rl_attempted_completion_function = func;
}

void
init_readline(void)
{
	str history = mal_clients->history; /* only console has history */

	/* Allow conditional parsing of the ~/.inputrc file. */
	rl_readline_name = "MonetDB";

	/* Tell the completer that we want to try our own completion before std completion (filename) kicks in. */
	rl_attempted_completion_function = mal_completion;
	rl_completion_display_matches_hook= mal_help_display;
	rl_completion_append_character=0;
	read_history(history);
}

void
deinit_readline(void)
{
	str history = mal_clients->history;
	if (history) {
		write_history(history);
	}
}
#endif /* HAVE_LIBREADLINE */
/*
 * @-
 */
#ifndef S_ISCHR
#define S_ISCHR(m)  (((m) & S_IFMT) == S_IFCHR)
#endif

#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

static void
showCommands(void)
{
	printf("?\t - show this message\n");
#ifdef HAVE_LIBREADLINE
	printf("!\t - show the history\n");
#endif
	printf("<file\t - read input from file\n");
	printf(">file\t - save response in file\n");
	printf(">\t - response to terminal\n");
	printf("cd\t - change directory\n");
	printf("\\q\t- terminate session\n");
}

#ifdef HAVE_LIBREADLINE
static int initReadline = 0;
#endif

char *
getConsoleInput(Client c, const char *prompt, int linemode, int exit_on_error)
{
	char *line = NULL;
	char *buf = NULL;
	size_t length;
	(void) exit_on_error;
	(void) linemode;

	do {
#ifdef HAVE_LIBREADLINE
		if (prompt) {

			if (buf)
				free(buf);
			buf = readline(prompt);
			/* add a newline to the end since that makes
			   further processing easier */
			if (buf) {
				add_history(buf);
				length = strlen(buf);
				buf = realloc(buf, length + 2);
				if( buf == NULL){
					GDKerror("getConsoleInput: " MAL_MALLOC_FAIL);
					return NULL;
				}
				buf[length++] = '\n';
				buf[length] = 0;
			}
			line = buf;
		} else
#endif
		{
#ifndef HAVE_LIBREADLINE
			if (prompt) {
				fputs(prompt, stdout);
				fflush(stdout);
			}
#endif
			if (buf == NULL) {
				buf= malloc(BUFSIZ);
				if( buf == NULL){
					GDKerror("getConsoleInput: " MAL_MALLOC_FAIL);
					return NULL;
				}
			}
			line = fgets(buf, BUFSIZ, stdin);
		}

		if (line == NULL) {
			/* end of file */
			if (buf)
				free(buf);
			return NULL;
		} else
			length = strlen(line);

		if (length > 0 ) {
			/* test for special commands */
			while (length > 0 &&
			       (*line & ~0x7F) == 0 &&
			       isspace((int) *line)) {
				line++;
				length--;
			}
			/* in the switch, use continue if the line was
			   processed, use break to send to parser */
			switch (*line) {
			case '\0':
				/* empty line */
				break;
			case '\\':
				switch (line[1]) {
				case 'q':
					free(buf);
					return NULL;
				default:
					break;
				}
				line= NULL;
				break;
			case '<':
				/* read commands from file */
				if (line[length - 1] == '\n')
					line[--length] = 0;
				if (line[length - 1] == '\r')
					line[--length] = 0;
				/* doFile(mid, line + 1, 0);*/
				line= NULL;
				continue;
			case '>':
				/* redirect output to file */
				line++;
				length--;
				if (line[length - 1] == '\n')
					line[--length] = 0;
				if (line[length - 1] == '\r')
					line[--length] = 0;

				if (c->fdout && c->fdout != GDKout && c->fdout != GDKerr){
					close_stream(c->fdout);
					c->fdout= 0;
				}
				if (length == 0 || strcmp(line, "stdout") == 0)
					c->fdout = GDKout;
				else if (strcmp(line, "stderr") == 0)
					c->fdout = GDKerr;
				else if ((c->fdout = open_wastream(line)) == NULL) {
					c->fdout = GDKout;
					mnstr_printf(GDKerr, "Cannot open %s\n", line);
				}
				line = NULL;
				continue;
#ifdef HAVE_LIBREADLINE
			case '!':
				{ char *nl;
				  int i;
					if(line[1]=='\n') {
						for(i=0; i< history_length; i++){
							nl= history_get(i)? history_get(i)->line:0;
							if( nl)
							mnstr_printf(c->fdout, "%d %s\n", i, nl);
						}
						line = NULL;
					} else
					if( history_expand(line,&nl) ==1  ) {
						mnstr_printf(c->fdout,"#%s",nl);
						line= nl;
					} else line= NULL;
				}
				continue;
#endif
			case '?':
				if( line[1] && line[1]!='\n'){
					showHelp( c->nspace,line+1, c->fdout);
				} else
					showCommands();
				line= NULL;
				continue;
			}
			/* make sure we return a pointer that can (and should) be freed by the caller */
			if (line)
				line = buf;
		}
	} while (line == NULL);
	return line;
}

int
readConsole(Client cntxt)
{
	/* execute from stdin */
	struct stat statb;
	char *buf;

	if (cntxt->promptlength == 0 ||
	   !(fstat(fileno(stdin), &statb) == 0 && S_ISCHR(statb.st_mode))  )
		return -1;

	/* read lines and move string to client buffer. */
#ifdef HAVE_LIBREADLINE
	if( initReadline ==0){
		init_readline();
		using_history();
		stifle_history(1000);
		initReadline =1 ;
	}
#endif
	buf= getConsoleInput(cntxt, cntxt->prompt, 0, 1);
	if( buf) {
		size_t len= strlen(buf);
		if( len >= cntxt->fdin->size) {
			/* extremly dirty inplace buffer overwriting */
			cntxt->fdin->buf= realloc(cntxt->fdin->buf, len+1);
			if( cntxt->fdin->buf == NULL) {
				GDKerror("readConsole: " MAL_MALLOC_FAIL);
				free(buf);
				goto bailout;
			}
			cntxt->fdin->size = len;
		}
		strcpy(cntxt->fdin->buf, buf);
		cntxt->fdin->pos = 0;
		cntxt->fdin->len = len;
		free(buf);
		return 1;
	}
  bailout:
	cntxt->fdin->eof = 1;
#ifdef HAVE_LIBREADLINE
	if( initReadline ){
		deinit_readline();
		initReadline= 0;
	}
#endif
	return -1;
}
