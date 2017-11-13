/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * Readline specific stuff
 */
#include "monetdb_config.h"
#include <monet_options.h>

#ifdef HAVE_LIBREADLINE

#include <readline/readline.h>
#include <readline/history.h>
#include "ReadlineTools.h"

#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif

#ifndef NATIVE_WIN32
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#endif

#include <errno.h>

#define PATHLENGTH	256	/* maximum file pathname length. */

static const char *sql_commands[] = {
	"SELECT",
	"INSERT",
	"UPDATE",
	"SET",
	"DELETE",
	"COMMIT",
	"ROLLBACK",
	"DROP TABLE",
	"CREATE",
	"ALTER",
	"RELEASE SAVEPOINT",
	"START TRANSACTION",
	0,
};

static Mapi _mid;
static char _history_file[PATHLENGTH];
static int _save_history = 0;
static char *language;

static char *
sql_tablename_generator(const char *text, int state)
{

	static int seekpos, len, rowcount;
	static MapiHdl table_hdl;

	if (!state) {
		char *query;

		seekpos = 0;
		len = strlen(text);
		if ((query = malloc(len + 150)) == NULL)
			return NULL;
		snprintf(query, len + 150, "SELECT t.\"name\", s.\"name\" FROM \"sys\".\"tables\" t, \"sys\".\"schemas\" s where t.system = FALSE AND t.schema_id = s.id AND t.\"name\" like '%s%%'", text);
		table_hdl = mapi_query(_mid, query);
		free(query);
		if (table_hdl == NULL || mapi_error(_mid)) {
			if (table_hdl) {
				mapi_explain_query(table_hdl, stderr);
				mapi_close_handle(table_hdl);
			} else
				mapi_explain(_mid, stderr);
			return NULL;
		}
		mapi_fetch_all_rows(table_hdl);
		rowcount = mapi_get_row_count(table_hdl);
	}

	while (seekpos < rowcount) {
		if (mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET) != MOK ||
		    mapi_fetch_row(table_hdl) <= 0)
			continue;
		return strdup(mapi_fetch_field(table_hdl, 0));
	}

	return NULL;
}

/* SQL commands (at start of line) */
static char *
sql_command_generator(const char *text, int state)
{

	static int idx, len;
	const char *name;

	if (!state) {
		idx = 0;
		len = strlen(text);
	}


	while ((name = sql_commands[idx++])) {
#ifdef HAVE_STRNCASECMP
		if (strncasecmp(name, text, len) == 0)
#else
		if (strncmp(name, text, len) == 0)
#endif
			return strdup(name);
	}

	return NULL;
}


static char **
sql_completion(const char *text, int start, int end)
{
	char **matches;

	matches = (char **) NULL;

	(void) end;

	/* FIXME: Nice, context-sensitive completion strategy should go here */
	if (strcmp(language, "sql") == 0) {
		if (start == 0) {
			matches = rl_completion_matches(text, sql_command_generator);
		} else {
			matches = rl_completion_matches(text, sql_tablename_generator);
		}
	}
	if (strcmp(language, "mal") == 0) {
		matches = rl_completion_matches(text, sql_tablename_generator);
	}

	return (matches);
}

/* The MAL completion help */

static char *mal_commands[] = {
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

#ifdef illegal_ESC_binding
/* see also init_readline() below */
static int
mal_help(int cnt, int key)
{
	char *name, *c, *buf;
	int seekpos = 0, rowcount;
	MapiHdl table_hdl;

	(void) cnt;
	(void) key;

	c = rl_line_buffer + strlen(rl_line_buffer) - 1;
	while (c > rl_line_buffer && isspace((unsigned char) *c))
		c--;
	while (c > rl_line_buffer && !isspace((unsigned char) *c))
		c--;
	if ((buf = malloc(strlen(c) + 20)) == NULL)
		return 0;
	snprintf(buf, strlen(c) + 20, "manual.help(\"%s\");", c);
	table_hdl = mapi_query(_mid, buf);
	free(buf);
	if (table_hdl == NULL || mapi_error(_mid)) {
		if (table_hdl) {
			mapi_explain_query(table_hdl, stderr);
			mapi_close_handle(table_hdl);
		} else
			mapi_explain(_mid, stderr);
		return 0;
	}
	mapi_fetch_all_rows(table_hdl);
	rowcount = mapi_get_row_count(table_hdl);

	printf("\n");
	while (seekpos < rowcount) {
		if (mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET) != MOK ||
		    mapi_fetch_row(table_hdl) <= 0)
			continue;
		name = mapi_fetch_field(table_hdl, 0);
		if (name)
			printf("%s\n", name);
	}
	return key;
}
#endif

static char *
mal_command_generator(const char *text, int state)
{

	static int idx;
	static int seekpos, len, rowcount;
	static MapiHdl table_hdl;
	char *name, *buf;

	/* we pick our own portion of the linebuffer */
	text = rl_line_buffer + strlen(rl_line_buffer) - 1;
	while (text > rl_line_buffer && !isspace((unsigned char) *text))
		text--;
	if (!state) {
		idx = 0;
		len = strlen(text);
	}

/*	printf("expand test:%s\n",text);
	printf("currentline:%s\n",rl_line_buffer); */

	while (mal_commands[idx] && (name = mal_commands[idx++])) {
#ifdef HAVE_STRNCASECMP
		if (strncasecmp(name, text, len) == 0)
#else
		if (strncmp(name, text, len) == 0)
#endif
			return strdup(name);
	}
	/* try the server to answer */
	if (!state) {
		char *c;
		c = strstr(text, ":=");
		if (c)
			text = c + 2;
		while (isspace((unsigned char) *text))
			text++;
		if ((buf = malloc(strlen(text) + 32)) == NULL)
			return NULL;
		if (strchr(text, '.') == NULL)
			snprintf(buf, strlen(text) + 32,
				 "manual.completion(\"%s.*(\");", text);
		else
			snprintf(buf, strlen(text) + 32,
				 "manual.completion(\"%s(\");", text);
		seekpos = 0;
		table_hdl = mapi_query(_mid, buf);
		free(buf);
		if (table_hdl == NULL || mapi_error(_mid)) {
			if (table_hdl) {
				mapi_explain_query(table_hdl, stderr);
				mapi_close_handle(table_hdl);
			} else
				mapi_explain(_mid, stderr);
			return NULL;
		}
		mapi_fetch_all_rows(table_hdl);
		rowcount = mapi_get_row_count(table_hdl);
	}

	while (seekpos < rowcount) {
		if (mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET) != MOK ||
		    mapi_fetch_row(table_hdl) <= 0)
			continue;
		name = mapi_fetch_field(table_hdl, 0);
		if (name)
			return strdup(name);
	}

	return NULL;
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
init_readline(Mapi mid, char *lang, int save_history)
{
	language = lang;
	_mid = mid;
	/* Allow conditional parsing of the ~/.inputrc file. */
	rl_readline_name = "MapiClient";
	/* Tell the completer that we want to try our own completion
	 * before std completion (filename) kicks in. */
	if (strcmp(language, "sql") == 0) {
		rl_attempted_completion_function = sql_completion;
	} else if (strcmp(language, "mal") == 0) {
		/* recognize the help function, should react to <FCN2> */
#ifdef illegal_ESC_binding
		rl_bind_key('\033', mal_help);
#endif
		rl_attempted_completion_function = mal_completion;
	}

	if (save_history) {
#ifndef NATIVE_WIN32
		if (getenv("HOME") != NULL) {
			snprintf(_history_file, PATHLENGTH,
				 "%s/.mapiclient_history_%s",
				 getenv("HOME"), language);
			_save_history = 1;
		}
#else
		snprintf(_history_file, PATHLENGTH,
			 "%s%c_mapiclient_history_%s",
			 mo_find_option(NULL, 0, "prefix"), DIR_SEP, language);
		_save_history = 1;
#endif
		if (_save_history) {
			FILE *f;
			switch (read_history(_history_file)) {
			case 0:
				/* success */
				break;
			case ENOENT:
				/* history file didn't exist, so try to create
				 * it and then try again */
				if ((f = fopen(_history_file, "w")) == NULL) {
					/* failed to create, don't
					 * bother saving */
					_save_history = 0;
				} else {
					(void) fclose(f);
					if (read_history(_history_file) != 0) {
						/* still no luck, don't
						 * bother saving */
						_save_history = 0;
					}
				}
				break;
			default:
				/* unrecognized failure, don't bother saving */
				_save_history = 0;
				break;
			}
		}
		if (!_save_history)
			fprintf(stderr, "Warning: not saving history\n");
	}
}

void
deinit_readline(void)
{
	/* nothing to do since we use append_history() */
}

void
save_line(const char *s)
{
	add_history(s);
	if (_save_history)
		append_history(1, _history_file);
}


#endif /* HAVE_LIBREADLINE */
