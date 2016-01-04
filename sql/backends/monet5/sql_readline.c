/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @- Word completion
 * Word completion recognizes the start of a line to issue a
 * list of valid SQL command options. Any other identifier
 * is considered a table name or column name.
 * The word completion does not recognize possible functions
 * and types yet.
 * Add readline functionality to the MAL console.
 * This means that the user has history access and some other
 * features to assemble a command before it is being interpreted.
 * @f sql_readline
 */
#include "monetdb_config.h"
#include "mal.h"
#undef PATHLENGTH
#include "mal_readline.h"
#include "mal_client.h"
#include "mal_scenario.h"
#include "sql_readline.h"

/* #define _SQL_READLINE_DEBUG  */

#ifdef HAVE_LIBREADLINE

#include <readline/readline.h>
#include <readline/history.h>
#include "mal_debugger.h"

void init_sql_readline(void);
void deinit_sql_readline(void);
rl_completion_func_t *suspend_completion(void);
void continue_completion(rl_completion_func_t * func);

#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif


static const char *sql_commands[] = {
	"COMMIT",
	"COPY",
	"CREATE TABLE",
	"CREATE VIEW",
	"DECLARE",
	"DROP TABLE",
	"DROP VIEW",
	"FROM",
	"BEGIN TRANSACTION",
	"ROLLBACK",
	"SELECT",
	"SET AUTO_COMMIT",
	"WHERE",
	0
};

static int sqlcommandlimit = 13;

#ifdef HAVE_STRNCASECMP
#define STR_EQUAL(a, b, l)	(strncasecmp(a, b, l) == 0)
#else
#define STR_EQUAL(a, b, l)	(strncmp(a, b, l) == 0)
#endif

static char *
sql_command_generator(const char *text, int state)
{
	static int index, len;
	const char *name;

#ifdef _SQL_READLINE_DEBUG
	printf("expand:%d [%d] %s \n", state, (int) strlen(text), text);
#endif
	if (!state) {
		index = 0;
		len = strlen(text);
	}
	if (mdbSession()) {
		while ((name = sql_commands[index++])) {
			if (STR_EQUAL(name, text, len))
				return strdup(name);
		}
		return NULL;
	}
	while (index < sqlcommandlimit && (name = sql_commands[index++])) {
		if (STR_EQUAL(name, text, len))
			return strdup(name);
	}
	return NULL;
}

static char **
sql_completion(const char *text, int start, int end)
{
	(void) start;
	(void) end;

	/* FIXME: Nice, context-sensitive completion strategy should go here */
	return rl_completion_matches(text, sql_command_generator);
}

void
init_sql_readline(void)
{
	str history = MCgetClient(CONSOLE)->history;	/* only for console */

	/* Allow conditional parsing of the ~/.inputrc file. */
	rl_readline_name = "MonetDB";
	/* Tell the completer that we want to try our own completion before std completion (filename) kicks in. */
	rl_attempted_completion_function = sql_completion;
	read_history(history);
}

void
deinit_sql_readline(void)
{
	str history = MCgetClient(CONSOLE)->history;	/* only for console */
	if (history) {
		write_history(history);
	}
}

/*
 * @-
 */
#ifndef S_ISCHR
#define S_ISCHR(m)  (((m) & S_IFMT) == S_IFCHR)
#endif

#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

static int initsqlReadline;

int
SQLreadConsole(Client cntxt)
{
	/* execute from stdin */
	struct stat statb;
	char *buf;

	if (cntxt->promptlength == 0)
		return -1;
	if (!(fstat(fileno(stdin), &statb) == 0 && S_ISCHR(statb.st_mode)))
		return -1;

	/* read lines and move string to client buffer. */
	if (initsqlReadline == 0) {
		init_sql_readline();
		using_history();
		stifle_history(1000);
		initsqlReadline = 1;
	}
	buf = getConsoleInput(cntxt, cntxt->prompt, 0, 1);
	if (buf) {
		size_t len = strlen(buf);
		if (len >= cntxt->fdin->size) {
			/* extremly dirty inplace buffer overwriting */
			assert(cntxt->fdin->buf);
			cntxt->fdin->buf = realloc(cntxt->fdin->buf, len + 1);
			if (cntxt->fdin->buf == NULL) {
				cntxt->fdin->len = 0;
				cntxt->fdin->size = 0;
				free(buf);
				return -1;
			}
			cntxt->fdin->len = len;
			cntxt->fdin->size = len;
		}
		strcpy(cntxt->fdin->buf, buf);
		cntxt->fdin->pos = 0;
		free(buf);
		return 1;
	} else {
		cntxt->fdin->eof = 1;
		if (initsqlReadline) {
			deinit_sql_readline();
			initsqlReadline = 0;
		}
	}
	return -1;
}

#else

int
SQLreadConsole(Client cntxt)
{
	(void) cntxt;
	return -1;
}
#endif /* HAVE_LIBREADLINE */
