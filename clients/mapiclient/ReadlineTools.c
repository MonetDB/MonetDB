/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
	char *name;

	if (!state) {
		seekpos = 0;
		len = strlen(text);
		if ((table_hdl = mapi_query(_mid, "SELECT t.\"name\", s.\"name\" FROM \"sys\".\"tables\" t, \"sys\".\"schemas\" s where t.schema_id = s.id")) == NULL || mapi_error(_mid)) {
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
		mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET);
		mapi_fetch_row(table_hdl);
		name = mapi_fetch_field(table_hdl, 0);
		if (strncmp(name, text, len) == 0) {
			char *s, *schema = mapi_fetch_field(table_hdl, 1);
			int l1 = strlen(name), l2 = strlen(schema);

			s = malloc(l1 + l2 + 2);
			s[0] = 0;
			strcat(s, schema); 
			strcat(s, ".");
			strcat(s, name);
			return s;
		}
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

static char *
mil_batname_generator(const char *text, int state)
{

	static int seekpos, len, rowcount;
	static MapiHdl table_hdl;
	char *name;

	if (!state) {
		seekpos = 0;
		len = strlen(text);
		if ((table_hdl = mapi_query(_mid, "ls();")) == NULL || mapi_error(_mid)) {
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
		mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET);
		mapi_fetch_row(table_hdl);
		name = mapi_fetch_field(table_hdl, 0);
		if (strncmp(name, text, len) == 0)
			return strdup(name);
	}

	return NULL;
}

static char **
mil_completion(const char *text, int start, int end)
{
	(void) start;
	(void) end;

	/* FIXME: Nice, context-sensitive completion strategy should go here */
	return rl_completion_matches(text, mil_batname_generator);
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
	char *name, *c, buf[BUFSIZ];
	int seekpos = 0, rowcount;
	MapiHdl table_hdl;

	(void) cnt;
	(void) key;

	c = rl_line_buffer + strlen(rl_line_buffer) - 1;
	while (c > rl_line_buffer && isspace(*c))
		c--;
	while (c > rl_line_buffer && !isspace(*c))
		c--;
	snprintf(buf, BUFSIZ, "manual.help(\"%s\");", c);
	if ((table_hdl = mapi_query(_mid, buf)) == NULL || mapi_error(_mid)) {
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
		mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET);
		mapi_fetch_row(table_hdl);
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
	char *name, buf[BUFSIZ];

	/* we pick our own portion of the linebuffer */
	text = rl_line_buffer + strlen(rl_line_buffer) - 1;
	while (text > rl_line_buffer && !isspace((int) *text))
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
		char cmd[BUFSIZ], *c;
		c = strstr(text, ":=");
		if (c)
			text = c + 2;
		while (isspace((int) *text))
			text++;
		c = strchr(text, '.');
		if (c == NULL)
			snprintf(cmd, BUFSIZ, "%s.*(", text);
		else
			snprintf(cmd, BUFSIZ, "%s(", text);
		seekpos = 0;
		len = strlen(cmd);
		snprintf(buf, BUFSIZ, "manual.completion(\"%s\");", cmd);
		if ((table_hdl = mapi_query(_mid, buf)) == NULL || mapi_error(_mid)) {
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
		mapi_seek_row(table_hdl, seekpos++, MAPI_SEEK_SET);
		mapi_fetch_row(table_hdl);
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
	/* Tell the completer that we want to try our own completion before std completion (filename) kicks in. */
	if (strcmp(language, "sql") == 0) {
		rl_attempted_completion_function = sql_completion;
	} else if (strcmp(language, "mil") == 0) {
		rl_attempted_completion_function = mil_completion;
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
		if (_save_history)
			read_history(_history_file);
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
