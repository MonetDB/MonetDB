/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Readline specific stuff
 */
#include "monetdb_config.h"

#ifdef HAVE_LIBREADLINE
#include <fcntl.h>
#include <unistd.h>

#include <readline/readline.h>
#include <readline/history.h>
#include "ReadlineTools.h"
#define LIBMUTILS 1
#include "mutils.h"

#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif

#ifndef NATIVE_WIN32
/* for umask */
#include <sys/types.h>
#include <sys/stat.h>
#endif

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
static char _history_file[FILENAME_MAX];
static bool _save_history = false;
static const char *language;

static char *
sql_tablename_generator(const char *text, int state)
{

	static int64_t seekpos, rowcount;
	static size_t len;
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
	static size_t idx, len;
	const char *name;

	if (!state) {
		idx = 0;
		len = strlen(text);
	}

	while ((name = sql_commands[idx++])) {
		if (strncasecmp(name, text, len) == 0)
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

#ifdef illegal_ESC_binding
/* see also init_readline() below */
static int
mal_help(int cnt, int key)
{
	char *name, *c, *buf;
	int64_t seekpos = 0, rowcount;
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
	static int64_t seekpos, rowcount;
	static size_t len;
	static MapiHdl table_hdl;
	const char *name;
	char *buf;

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
		if (strncasecmp(name, text, len) == 0)
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

static void
readline_show_error(const char *msg) {
	rl_save_prompt();
	rl_message(msg);
	rl_restore_prompt();
	rl_clear_message();
}

#ifndef BUFFER_SIZE
#define BUFFER_SIZE 1024
#endif

static int
invoke_editor(int cnt, int key) {
	char editor_command[BUFFER_SIZE];
	char *read_buff = NULL;
	char *editor = NULL;
	FILE *fp = NULL;
	long content_len;
	size_t read_bytes, idx;

	(void) cnt;
	(void) key;

#ifdef NATIVE_WIN32
	char *mytemp;
	char template[] = "mclient_temp_XXXXXX";
	if ((mytemp = _mktemp(template)) == NULL) {
		readline_show_error("invoke_editor: Cannot create temp file\n");
		goto bailout;
	}
	if ((fp = MT_fopen(mytemp, "r+")) == NULL) {
		// Notify the user that we cannot create temp file
		readline_show_error("invoke_editor: Cannot create temp file\n");
		goto bailout;
	}
#else
	int mytemp;
	char template[] = "/tmp/mclient_temp_XXXXXX";
	mode_t msk = umask(077);
	mytemp = mkstemp(template);
	(void) umask(msk);
	if (mytemp == -1) {
		readline_show_error("invoke_editor: Cannot create temp file\n");
		goto bailout;
	}
	if ((fp = fdopen(mytemp, "r+")) == NULL) {
		// Notify the user that we cannot create temp file
		readline_show_error("invoke_editor: Cannot create temp file\n");
		goto bailout;
	}
#endif

	fwrite(rl_line_buffer, sizeof(char), rl_end, fp);
	fflush(fp);

	editor = getenv("VISUAL");
	if (editor == NULL) {
		editor = getenv("EDITOR");
		if (editor == NULL) {
			readline_show_error("invoke_editor: EDITOR/VISUAL env variable not set\n");
			goto bailout;
		}
	}

	snprintf(editor_command, BUFFER_SIZE, "%s %s", editor, template);
	if (system(editor_command) != 0) {
		readline_show_error("invoke_editor: Starting editor failed\n");
		goto bailout;
	}

	fseek(fp, 0L, SEEK_END);
	content_len = ftell(fp);
	rewind(fp);

	if (content_len > 0) {
		read_buff = (char *)malloc(content_len + 1);
		if (read_buff == NULL) {
			readline_show_error("invoke_editor: Cannot allocate memory\n");
			goto bailout;
		}

		read_bytes = fread(read_buff, sizeof(char), (size_t) content_len, fp);
		if (read_bytes != (size_t) content_len) {
			readline_show_error("invoke_editor: Did not read from file correctly\n");
			goto bailout;
		}

		read_buff[read_bytes] = 0;

		/* Remove trailing whitespace */
		idx = read_bytes - 1;
		while(isspace(*(read_buff + idx))) {
			read_buff[idx] = 0;
			idx--;
		}

		rl_replace_line(read_buff, 0);
		rl_point = (int)(idx + 1);  // place the point one character after the end of the string

		free(read_buff);
	} else {
		rl_replace_line("", 0);
		rl_point = 0;
	}

	fclose(fp);
	MT_remove(template);

	return 0;

bailout:
	if (fp)
		fclose(fp);
	free(read_buff);
	MT_remove(template);
	return 1;
}

void
init_readline(Mapi mid, const char *lang, bool save_history)
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

	rl_add_funmap_entry("invoke-editor", invoke_editor);
	rl_bind_keyseq("\\M-e", invoke_editor);

	if (save_history) {
		int len;
		if (getenv("HOME") != NULL) {
			len = snprintf(_history_file, FILENAME_MAX,
				 "%s/.mapiclient_history_%s",
				 getenv("HOME"), language);
			if (len == -1 || len >= FILENAME_MAX)
				fprintf(stderr, "Warning: history filename path is too large\n");
			else
				_save_history = true;
		}
		if (_save_history) {
			FILE *f;
			switch (read_history(_history_file)) {
			case 0:
				/* success */
				break;
			case ENOENT:
				/* history file didn't exist, so try to create
				 * it and then try again */
				if ((f = MT_fopen(_history_file, "w")) == NULL) {
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
