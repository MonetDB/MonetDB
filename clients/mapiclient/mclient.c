/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

/* The Mapi Client Interface
 * A textual interface to the Monet server using the Mapi library,
 * providing command-line access for its users. It is the preferred
 * interface for non-DBAs.
 * See mclient.1 for usage information.
 */

#include "monetdb_config.h"
#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif
#include "mapi.h"
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <errno.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif
#ifdef HAVE_LIBREADLINE
#include <readline/readline.h>
#include <readline/history.h>
#include "ReadlineTools.h"
#endif
#include "stream.h"
#include "msqldump.h"
#include "mprompt.h"
#ifdef HAVE_LOCALE_H
#include <locale.h>
#endif
#ifdef HAVE_ICONV
#ifdef HAVE_ICONV_H
#include <iconv.h>
#endif
#ifdef HAVE_NL_LANGINFO
#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif
#else
#ifdef NATIVE_WIN32
#include <Windows.h>
#endif
#endif
#endif

#ifndef S_ISCHR
#define S_ISCHR(m)	(((m) & S_IFMT) == S_IFCHR)
#endif
#ifndef S_ISREG
#define S_ISREG(m)	(((m) & S_IFMT) == S_IFREG)
#endif

enum modes {
	MAL,
	SQL
};

static enum modes mode = SQL;
static stream *toConsole;
static stream *toConsole_raw;	/* toConsole without iconv conversion */
static stream *stdout_stream;
static stream *stderr_stream;
static FILE *fromConsole = NULL;
static char *language = "sql";
static char *logfile = NULL;
static char promptbuf[16];
static int echoquery = 0;
#ifdef HAVE_ICONV
static char *encoding;
static iconv_t cd_in;
#endif
static int errseen = 0;

#define setPrompt() sprintf(promptbuf, "%.*s>", (int) sizeof(promptbuf) - 2, language)
#define debugMode() (strncmp(promptbuf, "mdb", 3) == 0)

/* the internal formatters */
enum formatters {
	NOformatter,
	RAWformatter,
	TABLEformatter,
	CSVformatter,
	TABformatter,
	XMLformatter,
	TESTformatter
};
static enum formatters formatter = NOformatter;
char *output = NULL;		/* output format as string */

#define DEFWIDTH 80

/* use a 64 bit integer for the timer */
typedef lng timertype;
#if 0
static char *mark, *mark2;
#endif

static timertype t0, t1;	/* used for timing */

#define UTF8BOM		"\xEF\xBB\xBF"	/* UTF-8 encoding of Unicode BOM */
#define UTF8BOMLENGTH	3	/* length of above */

/* Pagination and simple ASCII-based rendering is provided for SQL
 * sessions. The result set size is limited by the cache size of the
 * Mapi Library. It is sufficiently large to accommodate most result
 * to be browsed manually.
 *
 * The pagewidth determines the maximum space allocated for a single
 * row. If the total space required is larger, then a heuristic
 * routine is called to distribute the available space. Attribute
 * values may then span multiple lines. Setting the pagewidth to 0
 * turns off row size control. */

#ifdef HAVE_POPEN
static char *pager = 0;		/* use external pager */
#include <signal.h>		/* to block SIGPIPE */
#endif
static int rowsperpage = 0;	/* for SQL pagination */
static int pagewidth = -1;	/* -1: take whatever is necessary, >0: limit */
static int pagewidthset = 0;	/* whether the user set the width explicitly */
static int interactive_stdin = 0;
static int croppedfields = 0;  /* whatever got cropped/truncated */
static char firstcrop = 1;     /* first time we see cropping/truncation */

enum modifiers {
	NOmodifier,
	DEBUGmodifier
};
static enum modifiers specials = NOmodifier;
/* set when we see DEBUG (only if mode == SQL).  Also retain these
 * modes until after you have received the answer. */

/* keep these aligned, the MINCOLSIZE should ensure you can always
 * write the NULLSTRING */
#define MINCOLSIZE 4
static char default_nullstring[] = "null";
static char *nullstring = default_nullstring;
/* this is the minimum size (that still makes some sense) for writing
 * variable length columns */
#define MINVARCOLSIZE 10

/* stolen piece */
#ifdef HAVE_FTIME
#include <sys/timeb.h>
#endif

#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# ifdef HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif
#ifdef HAVE_STROPTS_H
#include <stropts.h>		/* ioctl */
#endif
#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif
#ifdef HAVE_TERMIOS_H
#include <termios.h>		/* TIOCGWINSZ/TIOCSWINSZ */
#endif

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define fileno _fileno
#endif

static timertype
gettime(void)
{
	/* Return the time in milliseconds since an epoch.  The epoch
	   is roughly the time this program started. */
#ifdef HAVE_QUERYPERFORMANCECOUNTER
	static LARGE_INTEGER freq, start;	/* automatically initialized to 0 */
	LARGE_INTEGER ctr;

	if (start.QuadPart == 0 &&
	    (!QueryPerformanceFrequency(&freq) ||
	     !QueryPerformanceCounter(&start)))
		start.QuadPart = -1;
	if (start.QuadPart > 0) {
		QueryPerformanceCounter(&ctr);
		return (timertype) (((ctr.QuadPart - start.QuadPart) * 1000000) / freq.QuadPart);
	}
#endif
#ifdef HAVE_GETTIMEOFDAY
	{
		static struct timeval tpbase;	/* automatically initialized to 0 */
		struct timeval tp;

		if (tpbase.tv_sec == 0)
			gettimeofday(&tpbase, NULL);
		gettimeofday(&tp, NULL);
		tp.tv_sec -= tpbase.tv_sec;
		return (timertype) tp.tv_sec * 1000000 + (timertype) tp.tv_usec;
	}
#else
#ifdef HAVE_FTIME
	{
		static struct timeb tbbase;	/* automatically initialized to 0 */
		struct timeb tb;

		if (tbbase.time == 0)
			ftime(&tbbase);
		ftime(&tb);
		tb.time -= tbbase.time;
		return (timertype) tb.time * 1000000 + (timertype) tb.millitm * 1000;
	}
#endif
#endif
}

static void
timerStart(void)
{
	t0 = gettime();
}

static void
timerPause(void)
{
	t1 = gettime();
	if (t0 == 0)
		t0 = t1;
}

static void
timerResume(void)
{
	if (t1 == 0)
		t1 = gettime();
	assert(t1 >= t0);
	t0 = gettime() - (t1 - t0);
}

static void
timerEnd(void)
{
	mnstr_flush(toConsole);
	t1 = gettime();
	assert(t1 >= t0);
#if 0
	if (mark && specials == NOmodifier) {
		fprintf(stderr, "%s %7ld.%03ld msec %s\n", mark, (long) ((t1 - t0) / 1000), (long) ((t1 - t0) % 1000), mark2 ? mark2 : "");
		fflush(stderr);
	}
#endif
}

static timertype th = 0;
static void
timerHumanStop()
{
	th = gettime();
}

static char htimbuf[32];
static char *
timerHuman()
{
	timertype t = th - t0;

	assert(th >= t0);

	if (t / 1000 < 950) {
		snprintf(htimbuf, 32, "%ld.%03ldms", (long) (t / 1000), (long) (t % 1000));
		return(htimbuf);
	}
	t /= 1000;
	if (t / 1000 < 60) {
		snprintf(htimbuf, 32, "%ld.%lds", (long) (t / 1000),
				(long) ((t % 1000) / 100));
		return(htimbuf);
	}
	t /= 1000;
	snprintf(htimbuf, 32, "%ldm %lds", (long) (t / 60), (long) (t % 60));
	return(htimbuf);
}

/* The Mapi library eats away the comment lines, which we need to
 * detect end of debugging. We overload the routine to our liking. */

static char *
fetch_line(MapiHdl hdl)
{
	char *reply;

	if ((reply = mapi_fetch_line(hdl)) == NULL)
		return NULL;
	if (strncmp(reply, "mdb>#", 5) == 0) {
		if (strncmp(reply, "mdb>#EOD", 8) == 0)
			setPrompt();
		else
			sprintf(promptbuf, "mdb>");
	}
	return reply;
}

static int
fetch_row(MapiHdl hdl)
{
	char *reply;

	do {
		if ((reply = fetch_line(hdl)) == NULL)
			return 0;
	} while (*reply != '[' && *reply != '=');
	return mapi_split_line(hdl);
}

static void
SQLsetSpecial(const char *command)
{
	if (mode == SQL && command && specials == NOmodifier) {
		/* catch the specials for better rendering */
		while (*command == ' ' || *command == '\t')
			command++;
		if (strncmp(command, "debug", 5) == 0)
			specials = DEBUGmodifier;
		else
			specials = NOmodifier;
	}
}

/* return the display length of a UTF-8 string
   if e is not NULL, return length up to e */
static size_t
utf8strlen(const char *s, const char *e)
{
	size_t len = 0;

	if (s == NULL)
		return 0;
	while (*s && (e == NULL || s < e)) {
		/* only count first byte of a sequence */
		if ((*s & 0xC0) != 0x80)
			len++;
		s++;
	}
	return len;
}

/* skip the specified number of UTF-8 characters, but stop at a newline */
static char *
utf8skip(char *s, size_t i)
{
	while (*s && i > 0) {
		if ((*s & 0xC0) == 0xC0) {
			s++;
			while ((*s & 0xC0) == 0x80)
				s++;
		} else if (*s == '\n')
			return s;
		else
			s++;
		i--;
	}
	return s;
}

static int
SQLrow(int *len, int *numeric, char **rest, int fields, int trim, char wm)
{
	int i, more, first = 1;
	char *t;
	int rows = 0;		/* return number of output lines printed */
	size_t ulen;
	int *cutafter = alloca(sizeof(int) * fields);

	/* trim the text if needed */
	if (trim == 1) {
		for (i = 0; i < fields; i++) {
			if ((t = rest[i]) != NULL &&
			    utf8strlen(t, NULL) > (size_t) len[i])
			{
				/* eat leading whitespace */
				while (*t != 0 && isascii((int) *t) && isspace((int) *t))
					t++;
				rest[i] = t;
			}
		}
	}

	for (i = 0; i < fields; i++)
		cutafter[i] = -1;

	do {
		more = 0;
		for (i = 0; i < fields; i++) {
			if (rest[i] == NULL || *rest[i] == 0) {
				mnstr_printf(toConsole, "%c %*s ",
						first ? '|' : i > 0 && cutafter[i - 1] == 0 ? '>' : ':',
						len[i], "");
			} else {
				ulen = utf8strlen(rest[i], NULL);

				if (first && trim == 2) {
					/* calculate the height of this field according to
					 * the golden ratio, with a correction for a
					 * terminal screen (1.62 * 2 -> 3 : 9.72~10) */
					if (ulen > (size_t) len[i])
						cutafter[i] = 3 * len[i] / 10;
				}

				/* on each cycle we get closer to the limit */
				if (cutafter[i] >= 0)
					cutafter[i]--;

				/* break the string into pieces and
				 * left-adjust them in the column */
				t = strchr(rest[i], '\n');
				if (ulen > (size_t) len[i] || t) {
					char *s;

					t = utf8skip(rest[i], len[i]);
					if (trim == 1) {
						while (t > rest[i] && !(isascii((int) *t) && isspace((int) *t)))
							while ((*--t & 0xC0) == 0x80)
								;
						if (t == rest[i] && !(isascii((int) *t) && isspace((int) *t)))
							t = utf8skip(rest[i], len[i]);
					}
					mnstr_printf(toConsole, "%c",
						     first ? '|' : i > 0 && cutafter[i - 1] == 0 ? '>' : ':');
					if (numeric[i])
						mnstr_printf(toConsole, "%*s",
							     (int) (len[i] - (ulen - utf8strlen(t, NULL))),
							     "");
					s = t;
					if (trim == 1)
						while (isascii((int) *s) &&
						       isspace((int) *s))
							s++;
					if (trim == 2 && *s == '\n')
						s++;
					if (*s && cutafter[i] == 0) {
						t = utf8skip(rest[i], len[i] - 2);
						s = t;
						if (trim == 1)
							while (isascii((int) *s) &&
							       isspace((int) *s))
								s++;
						if (trim == 2 && *s == '\n')
							s++;
						mnstr_printf(toConsole, " %.*s...%*s",
							     (int) (t - rest[i]),
							     rest[i],
							     len[i] - 2 - (int) utf8strlen(rest[i], t),
							     "");
						croppedfields++;
					} else {
						mnstr_printf(toConsole, " %.*s ",
								  (int) (t - rest[i]),
								  rest[i]);
						if (!numeric[i])
							mnstr_printf(toConsole, "%*s",
								     (int) (len[i] - (ulen - utf8strlen(t, NULL))),
								     "");
					}
					rest[i] = *s ? s : 0;
					if (rest[i] == NULL) {
						/* avoid > as border marker if everything
						 * actually just fits */
						cutafter[i] = -1;
					}
					if (cutafter[i] == 0)
						rest[i] = NULL;
					if (rest[i])
						more = 1;
				} else {
					mnstr_printf(toConsole, "%c",
							first ? '|' : i > 0 && cutafter[i - 1] == 0 ? '>' : ':');
					if (numeric[i])
						mnstr_printf(toConsole, "%*s",
							      (int) (len[i] - ulen),
							      "");
					mnstr_printf(toConsole, " %s ",
						      rest[i]);
					if (!numeric[i])
						mnstr_printf(toConsole, "%*s",
							      (int) (len[i] - ulen),
							      "");
					rest[i] = 0;
					/* avoid > as border marker if everything
					 * actually just fits */
					if (cutafter[i] == 0)
						cutafter[i] = -1;
				}
			}
		}
		mnstr_printf(toConsole, "%c%s\n",
				first ? '|' : i > 0 && cutafter[i - 1] == 0 ? '>' : ':',
				wm ? ">" : "");
		first = 0;
		rows++;
	} while (more);
	return rows;
}

static void
XMLprdata(const char *val)
{
	if (val == NULL)
		return;
	while (*val) {
		if (*val == '&')
			mnstr_printf(toConsole_raw, "&amp;");
		else if (*val == '<')
			mnstr_printf(toConsole_raw, "&lt;");
		else if (*val == '>')
			mnstr_printf(toConsole_raw, "&gt;");
		else if (*val == '"')
			mnstr_printf(toConsole_raw, "&quot;");
		else if (*val == '\'')
			mnstr_printf(toConsole_raw, "&apos;");
		else if ((*val & 0xFF) < 0x20)	/* control character */
			mnstr_printf(toConsole_raw, "&#%d;", *val & 0xFF);
		else if ((*val & 0x80) != 0 /* && encoding != NULL */ ) {
			int n, m;
			int c = *val & 0x7F;

			for (n = 0, m = 0x40; c & m; n++, m >>= 1)
				c &= ~m;
			while (--n >= 0)
				c = (c << 6) | (*++val & 0x3F);
			mnstr_printf(toConsole_raw, "&#x%x;", c);
		} else
			mnstr_write(toConsole_raw, val, 1, 1);
		val++;
	}
}

static void
XMLprattr(const char *name, const char *val)
{
	mnstr_printf(toConsole_raw, " %s=\"", name);
	XMLprdata(val);
	mnstr_write(toConsole_raw, "\"", 1, 1);
}

static void
XMLrenderer(MapiHdl hdl)
{
	int i, fields;
	char *name;

	/* we must use toConsole_raw since the XML file is encoded in UTF-8 */
	mnstr_flush(toConsole);
	mnstr_printf(toConsole_raw, "<?xml version='1.0' encoding='UTF-8'?>\n");
	mnstr_printf(toConsole_raw,
		      "<!DOCTYPE table [\n"
		      " <!ELEMENT table (row)*>\n" /* a table consists of zero or more rows */
		      " <!ELEMENT row (column)+>\n"	/* a row consists of one or more columns */
		      " <!ELEMENT column (#PCDATA)>\n"
		      " <!ATTLIST table name CDATA #IMPLIED>\n"	/* a table may have a name */
		      " <!ATTLIST column name CDATA #IMPLIED\n"	/* a column may have a name */
		      "                  isnull (true|false) 'false'>]>\n");
	mnstr_printf(toConsole_raw, "<table");
	name = mapi_get_table(hdl, 0);
	if (name != NULL && *name != 0)
		XMLprattr("name", name);
	mnstr_printf(toConsole_raw, ">\n");
	while (!mnstr_errnr(toConsole) && (fields = fetch_row(hdl)) != 0) {
		mnstr_printf(toConsole_raw, "<row>");
		for (i = 0; i < fields; i++) {
			char *data = mapi_fetch_field(hdl, i);

			mnstr_printf(toConsole_raw, "<column");
			name = mapi_get_name(hdl, i);
			if (name != NULL && *name != 0)
				XMLprattr("name", name);
			if (data == NULL) {
				XMLprattr("isnull", "true");
				mnstr_write(toConsole_raw, "/", 1, 1);
			}
			mnstr_write(toConsole_raw, ">", 1, 1);
			if (data) {
				XMLprdata(data);
				mnstr_printf(toConsole_raw, "</column>");
			}
		}
		mnstr_printf(toConsole_raw, "</row>\n");
	}
	mnstr_printf(toConsole_raw, "</table>\n");
	mnstr_flush(toConsole_raw);
}

static void
CSVrenderer(MapiHdl hdl)
{
	int fields;
	char *s;
	char *sep = formatter == CSVformatter ? "," : "\t";
	int i;

	while (!mnstr_errnr(toConsole) && (fields = fetch_row(hdl)) != 0) {
		for (i = 0; i < fields; i++) {
			s = mapi_fetch_field(hdl, i);
			if (s == NULL)
				s = nullstring == default_nullstring ? "" : nullstring;
			if (strchr(s, *sep) != NULL || strchr(s, '\n') != NULL || strchr(s, '"') != NULL) {
				mnstr_printf(toConsole, "%s\"",
					      i == 0 ? "" : sep);
				while (*s) {
					switch (*s) {
					case '\n':
						mnstr_write(toConsole, "\\n", 1, 2);
						break;
					case '\t':
						mnstr_write(toConsole, "\\t", 1, 2);
						break;
					case '\r':
						mnstr_write(toConsole, "\\r", 1, 2);
						break;
					case '\\':
						mnstr_write(toConsole, "\\\\", 1, 2);
						break;
					case '"':
						mnstr_write(toConsole, "\\\"", 1, 2);
						break;
					default:
						if (*s == *sep)
							mnstr_write(toConsole, "\\", 1, 1);
						mnstr_write(toConsole, s, 1, 1);
						break;
					}
					s++;
				}
				mnstr_write(toConsole, "\"", 1, 1);
			} else
				mnstr_printf(toConsole, "%s%s",
					      i == 0 ? "" : sep, s ? s : "");
		}
		mnstr_printf(toConsole, "\n");
	}
}

static void
SQLseparator(int *len, int fields, char sep)
{
	int i, j;

	mnstr_printf(toConsole, "+");
	for (i = 0; i < fields; i++) {
		mnstr_printf(toConsole, "%c", sep);
		for (j = 0; j < (len[i] < 0 ? -len[i] : len[i]); j++)
			mnstr_printf(toConsole, "%c", sep);
		mnstr_printf(toConsole, "%c+", sep);
	}
	mnstr_printf(toConsole, "\n");
}

static void
SQLqueryEcho(MapiHdl hdl)
{
	if (echoquery) {
		char *qry;

		qry = mapi_get_query(hdl);
		if (qry) {
			if (formatter != TABLEformatter) {
				char *p = qry;
				char *q = p;
				while ((q = strchr(q, '\n')) != NULL) {
					*q++ = '\0';
					mnstr_printf(toConsole, "#%s\n", p);
					p = q;
				}
				if (*p) {
					/* query does not end in \n */
					mnstr_printf(toConsole, "#%s\n", p);
				}
			} else {
				size_t qrylen = strlen(qry);

				mnstr_printf(toConsole, "%s", qry);
				if (qrylen > 0 && qry[qrylen - 1] != '\n') {
					/* query does not end in \n */
					mnstr_printf(toConsole, "\n");
				}
			}
			free(qry);
		}
	}
}

/* state machine to recognize integers, floating point numbers, OIDs */
static char *
classify(const char *s, size_t l)
{
	/* state is the current state of the state machine:
	   0 - initial state, no input seen
	   1 - initial sign
	   2 - valid integer (with optionally a sign)
	   3 - valid integer, followed by a decimal point
	   4 - fixed point number of the form [sign] digits period digits
	   5 - exponent marker after integer or fixed point number
	   6 - sign after exponent marker
	   7 - valid floating point number with exponent
	   8 - integer followed by single 'L'
	   9 - integer followed by 'LL' (lng)
	   10 - fixed or floating point number followed by single 'L'
	   11 - fixed or floating point number followed by 'LL' (dbl)
	   12 - integer followed by '@'
	   13 - valid OID (integer followed by '@0')
	 */
	int state = 0;

	if ((l == 4 && strcmp(s, "true") == 0) ||
	    (l == 5 && strcmp(s, "false") == 0))
		return "bit";
	while (l != 0) {
		if (*s == 0 || !isascii(*s))
			return "str";
		if (isdigit((int) *s)) {
			switch (state) {
			case 0:
			case 1:
				state = 2;	/* digit after optional sign */
				break;
			case 3:
				state = 4;	/* digit after decimal point */
				break;
			case 5:
			case 6:
				state = 7;	/* digit after exponent marker and optional sign */
				break;
			case 12:
				if (*s == '0')	/* only @0 allowed */
					state = 13;
				else
					return "str";
				break;
			}
		} else if (*s == '.') {
			if (state == 2)
				state = 3;	/* decimal point */
			else
				return "str";
		} else if (*s == 'e' || *s == 'E') {
			if (state == 2 || state == 4)
				state = 5;	/* exponent marker */
			else
				return "str";
		} else if (*s == '+' || *s == '-') {
			if (state == 0)
				state = 1;	/* sign at start */
			else if (state == 5)
				state = 6;	/* sign after exponent marker */
			else
				return "str";
		} else if (*s == '@') {
			if (state == 2)
				state = 12;	/* OID marker */
			else
				return "str";
		} else if (*s == 'L') {
			switch (state) {
			case 2:
				state = 8;	/* int + 'L' */
				break;
			case 8:
				state = 9;	/* int + 'LL' */
				break;
			case 4:
			case 7:
				state = 10;	/* dbl + 'L' */
				break;
			case 10:
				state = 11;	/* dbl + 'LL' */
				break;
			default:
				return "str";
			}
		}
		s++;
		l--;
	}
	switch (state) {
	case 13:
		return "oid";
	case 2:
		return "int";
	case 4:
	case 7:
	case 11:
		return "dbl";
	case 9:
		return "lng";
	default:
		return "str";
	}
}

static void
TESTrenderer(MapiHdl hdl)
{
	int fields;
	char *reply;
	char *s;
	size_t l;
	char *tp;
	char *sep;
	int i;

	SQLqueryEcho(hdl);
	while (!mnstr_errnr(toConsole) && (reply = fetch_line(hdl)) != 0) {
		if (*reply != '[') {
			if (*reply == '=')
				reply++;
			mnstr_printf(toConsole, "%s\n", reply);
			continue;
		}
		fields = mapi_split_line(hdl);
		sep = "[ ";
		for (i = 0; i < fields; i++) {
			s = mapi_fetch_field(hdl, i);
			l = mapi_fetch_field_len(hdl, i);
			tp = mapi_get_type(hdl, i);
			if (strcmp(tp, "unknown") == 0)
				tp = classify(s, l);
			mnstr_printf(toConsole, "%s", sep);
			sep = ",\t";
			if (s == NULL)
				mnstr_printf(toConsole, mode == SQL ? "NULL" : "nil");
			else if (strcmp(tp, "varchar") == 0 ||
				 strcmp(tp, "char") == 0 ||
				 strcmp(tp, "clob") == 0 ||
				 strcmp(tp, "str") == 0 ||
				 /* NULL byte in string? */
				 strlen(s) < l ||
				 /* start or end with white space? */
				 (isascii(*s) && isspace((int) *s)) ||
				 (isascii(s[l - 1]) && isspace((int) s[l - 1])) ||
				 /* a bunch of geom types */
				 strcmp(tp, "curve") == 0 ||
				 strcmp(tp, "geometry") == 0 ||
				 strcmp(tp, "linestring") == 0 ||
				 strcmp(tp, "mbr") == 0 ||
				 strcmp(tp, "multilinestring") == 0 ||
				 strcmp(tp, "point") == 0 ||
				 strcmp(tp, "polygon") == 0 ||
				 strcmp(tp, "surface") == 0) {
				mnstr_printf(toConsole, "\"");
				while (l != 0) {
					switch (*s) {
					case '\n':
						mnstr_write(toConsole, "\\n", 1, 2);
						break;
					case '\t':
						mnstr_write(toConsole, "\\t", 1, 2);
						break;
					case '\r':
						mnstr_write(toConsole, "\\r", 1, 2);
						break;
					case '\\':
						mnstr_write(toConsole, "\\\\", 1, 2);
						break;
					case '"':
						mnstr_write(toConsole, "\\\"", 1, 2);
						break;
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
					case '8':
					case '9':
						if (strcmp(tp, "curve") == 0 ||
						    strcmp(tp, "geometry") == 0 ||
						    strcmp(tp, "linestring") == 0 ||
						    strcmp(tp, "mbr") == 0 ||
						    strcmp(tp, "multilinestring") == 0 ||
						    strcmp(tp, "point") == 0 ||
						    strcmp(tp, "polygon") == 0 ||
						    strcmp(tp, "surface") == 0) {
							char *e;
							double d;
							d = strtod(s, &e);
							if (s != e) {
								mnstr_printf(toConsole, "%.10g", d);
								l -= e - s;
								s = e;
								continue;
							}
						}
						/* fall through */
					default:
						if ((unsigned char) *s < ' ')
							mnstr_printf(toConsole,
								      "\\%03o",
								      (int) (unsigned char) *s);
						else
							mnstr_write(toConsole, s, 1, 1);
						break;
					}
					s++;
					l--;
				}
				mnstr_write(toConsole, "\"", 1, 1);
			} else if (strcmp(tp, "double") == 0 ||
				   strcmp(tp, "dbl") == 0 ||
				   strcmp(tp, "real") == 0)
				mnstr_printf(toConsole, "%.10g", atof(s));
			else
				mnstr_printf(toConsole, "%s", s);
		}
		mnstr_printf(toConsole, "\t]\n");
	}
}

static void
RAWrenderer(MapiHdl hdl)
{
	char *line;

	SQLqueryEcho(hdl);
	while ((line = fetch_line(hdl)) != 0) {
		if (*line == '=')
			line++;
		mnstr_printf(toConsole, "%s\n", line);
	}
}

static void
SQLheader(MapiHdl hdl, int *len, int fields, char more)
{
	SQLqueryEcho(hdl);
	SQLseparator(len, fields, '-');
	if (mapi_get_name(hdl, 0)) {
		int i;
		char **names = (char **) alloca(fields * sizeof(char *));
		int *numeric = (int *) alloca(fields * sizeof(int));

		for (i = 0; i < fields; i++) {
			names[i] = mapi_get_name(hdl, i);
			numeric[i] = 0;
		}
		SQLrow(len, numeric, names, fields, 1, more);
		SQLseparator(len, fields, '=');
	}
}

static void
SQLdebugRendering(MapiHdl hdl)
{
	char *reply;
	int cnt = 0;

	sprintf(promptbuf, "mdb>");
	while ((reply = fetch_line(hdl))) {
		cnt++;
		mnstr_printf(toConsole, "%s\n", reply);
		if (strncmp(reply, "mdb>#EOD", 8) == 0) {
			cnt = 0;
			break;
		}
	}
	if (cnt == 0) {
		setPrompt();
		specials = NOmodifier;
	}
}

static void
SQLpagemove(int *len, int fields, int *ps, int *silent)
{
	int c;

	SQLseparator(len, fields, '-');
	mnstr_printf(toConsole, "next page? (continue,quit,next)");
	mnstr_flush(toConsole);
	c = getc(fromConsole);
	if (c == 'c')
		*ps = 0;
	if (c == 'q')
		*silent = 1;
	while (c != EOF && c != '\n')
		c = getc(fromConsole);
	if (*silent == 0)
		SQLseparator(len, fields, '-');
}

static void
SQLrenderer(MapiHdl hdl, char singleinstr)
{
	int i, total, lentotal, vartotal, minvartotal;
	int fields, rfields, printfields = 0, max = 1, graphwaste = 0;
	int *len = NULL, *hdr = NULL, *numeric = NULL;
	char **rest = NULL;
	char buf[50];
	int ps = rowsperpage, silent = 0;
	mapi_int64 rows = 0;

#if 0
	if (mark2)
		free(mark2);
	mark2 = NULL;
#endif

	croppedfields = 0;
	fields = mapi_get_field_count(hdl);
	rows = mapi_get_row_count(hdl);

	len = (int *) malloc(sizeof(int) * fields);
	hdr = (int *) malloc(sizeof(int) * fields);
	rest = (char **) malloc(sizeof(char *) * fields);
	numeric = (int *) malloc(sizeof(int) * fields);

	memset(len, 0, sizeof(int) * fields);
	memset(hdr, 0, sizeof(int) * fields);
	memset(rest, 0, sizeof(char *) * fields);
	memset(numeric, 0, sizeof(int) * fields);

	total = 0;
	lentotal = 0;
	vartotal = 0;
	minvartotal = 0;
	for (i = 0; i < fields; i++) {
		char *s;

		len[i] = mapi_get_len(hdl, i);
		if (len[i] == 0) {
			/* no table width known, use maximum, rely on squeezing
			 * lateron to fix it to whatever is available */
			len[i] = pagewidth <= 0 ? DEFWIDTH : pagewidth;
		}
		if (len[i] < MINCOLSIZE)
			len[i] = MINCOLSIZE;
		s = mapi_get_name(hdl, i);
		if (s != NULL) {
			size_t l = strlen(s);
			assert(l <= INT_MAX);
			hdr[i] = (int) l;
		} else {
			hdr[i] = 0;
		}
		/* if no rows, just try to draw headers nicely */
		if (rows == 0)
			len[i] = hdr[i];
		s = mapi_get_type(hdl, i);
		numeric[i] = s != NULL &&
			(strcmp(s, "int") == 0 ||
			 strcmp(s, "tinyint") == 0 ||
			 strcmp(s, "bigint") == 0 ||
			 strcmp(s, "wrd") == 0 ||
			 strcmp(s, "smallint") == 0 ||
			 strcmp(s, "double") == 0 ||
			 strcmp(s, "float") == 0 ||
			 strcmp(s, "decimal") == 0);

		if (rows == 0) {
			vartotal += len[i];
			minvartotal += MINCOLSIZE;
		} else if (!numeric[i]) {
			vartotal += len[i];
			minvartotal += len[i] > MINVARCOLSIZE ? MINVARCOLSIZE : len[i];
		}
		total += len[i];

		/* do a very pessimistic calculation to determine if more
		 * columns would actually fit on the screen */
		if (pagewidth > 0 &&
				((((printfields + 1) * 3) - 1) + 2) + /* graphwaste */
				(total - vartotal) + minvartotal > pagewidth)
		{
			/* this last column was too much */
			total -= len[i];
			if (!numeric[i])
				vartotal -= len[i];
			break;
		}

		lentotal += (hdr[i] > len[i] ? hdr[i] : len[i]);
		printfields++;
	}

	/* what we waste on space on the display is the column separators '
	 * | ', but the edges lack the edgespace of course */
	graphwaste = ((printfields * 3) - 1) + 2;
	/* make sure we can indicate we dropped columns */
	if (fields != printfields)
		graphwaste++;

	/* punish the column headers first until you cannot squeeze any
	 * further */
	while (pagewidth > 0 && graphwaste + lentotal > pagewidth) {
		/* pick the column where the header is longest compared to its
		 * content */
		max = -1;
		for (i = 0; i < printfields; i++) {
			if (hdr[i] > len[i]) {
				if (max == -1 ||
						hdr[max] - len[max] < hdr[i] - len[i])
					max = i;
			}
		}
		if (max == -1)
			break;
		hdr[max]--;
		lentotal--;
	}

	/* correct the lengths if the headers are wider than the content,
	 * since the headers are maximally squeezed to the content above, if
	 * a header is larger than its content, it means there was space
	 * enough.  If not, the content will be squeezed below. */
	for (i = 0; i < printfields; i++)
		if (len[i] < hdr[i])
			len[i] = hdr[i];

	/* worst case: lentotal = total, which means it still doesn't fit,
	 * values will be squeezed next */
	while (pagewidth > 0 && graphwaste + total > pagewidth) {
		max = -1;
		for (i = 0; i < printfields; i++) {
			if (!numeric[i] && (max == -1 || len[i] > len[max]))
				max = i;
		}

		/* no varsized fields that we can squeeze */
		if (max == -1)
			break;
		/* penalty for largest field */
		len[max]--;
		total--;
		/* no more squeezing possible */
		if (len[max] == 1)
			break;
	}

	SQLheader(hdl, len, printfields, fields != printfields);

	while ((rfields = fetch_row(hdl)) != 0) {
		if (mnstr_errnr(toConsole))
			continue;
		if (rfields != fields) {
			mnstr_printf(stderr_stream,
					"invalid tuple received from server, "
					"got %d columns, expected %d, ignoring\n", rfields, fields);
			continue;
		}
		if (silent)
			continue;
		for (i = 0; i < printfields; i++) {
			rest[i] = mapi_fetch_field(hdl, i);
			if (rest[i] == NULL)
				rest[i] = nullstring;
			else {
				char *p = rest[i];

				while ((p = strchr(p, '\r')) != 0) {
					switch (p[1]) {
					case '\0':
						/* end of string: remove CR */
						*p = 0;
						break;
					case '\n':
						/* followed by LF: remove CR */
						/* note: copy including NUL */
						memmove(p, p + 1, strlen(p));
						break;
					default:
						/* replace with ' ' */
						*p = ' ';
						break;
					}
				}
			}
		}

		if (ps > 0 && rows >= ps && fromConsole != NULL) {
			SQLpagemove(len, printfields, &ps, &silent);
			rows = 0;
			if (silent)
				continue;
		}

		rows += SQLrow(len, numeric, rest, printfields, 2, 0);
	}
	if (fields)
		SQLseparator(len, printfields, '-');
	rows = mapi_get_row_count(hdl);
	snprintf(buf, sizeof(buf), LLFMT " rows", rows);
#if 0
	mark2 = strdup(buf);	/* for the timer output */
#endif
	printf(LLFMT " tuple%s%s%s%s", rows, rows != 1 ? "s" : "",
			singleinstr ? " (" : "",
			singleinstr ? timerHuman() : "",
			singleinstr ? ")" : "");

	if (fields != printfields || croppedfields > 0)
		printf(" !");
	if (fields != printfields) {
		rows = fields - printfields;
		printf(LLFMT " column%s dropped", rows, rows != 1 ? "s" : "");
	}
	if (fields != printfields && croppedfields > 0)
		printf(", ");
	if (croppedfields > 0)
		printf("%d field%s truncated",
				croppedfields, croppedfields != 1 ? "s" : "");
	if (fields != printfields || croppedfields > 0) {
		printf("!");
		if (firstcrop == 1) {
			firstcrop = 0;
			printf("\nnote: to disable dropping columns and/or truncating fields use \\w-1");
		}
	}
	printf("\n");

	free(len);
	free(hdr);
	free(rest);
	free(numeric);
}

static void
setFormatter(char *s)
{
#ifdef WIN32
	if (formatter == TESTformatter)
		_set_output_format(0);
#endif
	if (strcmp(s, "sql") == 0)
		formatter = TABLEformatter;
	else if (strcmp(s, "csv") == 0)
		formatter = CSVformatter;
	else if (strcmp(s, "tab") == 0)
		formatter = TABformatter;
	else if (strcmp(s, "raw") == 0)
		formatter = RAWformatter;
	else if (strcmp(s, "xml") == 0)
		formatter = XMLformatter;
	else if (strcmp(s, "test") == 0) {
#ifdef WIN32
		_set_output_format(_TWO_DIGIT_EXPONENT);
#endif
		formatter = TESTformatter;
	} else
		mnstr_printf(toConsole, "unsupported formatter\n");
}

static void
setWidth(void)
{
	if (!pagewidthset) {
#ifdef TIOCGWINSZ
		struct winsize ws;

		if (ioctl(fileno(stdout), TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0)
			pagewidth = ws.ws_col;
		else
#endif
#ifdef WIN32
			pagewidth = 79;	 /* 80 columns minus 1 for the edge */
#else
			pagewidth = -1;
#endif
	}
}

#ifdef HAVE_POPEN
static void
start_pager(stream **saveFD, stream **saveFD_raw)
{
	*saveFD = NULL;
	*saveFD_raw = NULL;

	if (pager) {
		FILE *p;
		struct sigaction act;

		/* ignore SIGPIPE so that we get an error instead of signal */
		act.sa_handler = SIG_IGN;
		sigemptyset(&act.sa_mask);
		act.sa_flags = 0;
		sigaction(SIGPIPE, &act, NULL);

		p = popen(pager, "w");
		if (p == NULL)
			fprintf(stderr, "Starting '%s' failed\n", pager);
		else {
			*saveFD = toConsole;
			*saveFD_raw = toConsole_raw;
			/* put | in name to indicate that file should be closed with pclose */
			toConsole = file_wastream(p, "|pager");
			toConsole_raw = toConsole;
#ifdef HAVE_ICONV
			if (encoding != NULL)
				toConsole = iconv_wstream(toConsole, encoding, "pager");
#endif
		}
	}
}

static void
end_pager(stream *saveFD, stream *saveFD_raw)
{
	if (saveFD) {
		mnstr_close(toConsole);
		mnstr_destroy(toConsole);
		toConsole = saveFD;
		toConsole_raw = saveFD_raw;
	}
}
#endif

static int
format_result(Mapi mid, MapiHdl hdl, char singleinstr)
{
	MapiMsg rc = MERROR;
	mapi_int64 aff, lid;
	char *reply;
#ifdef HAVE_POPEN
	stream *saveFD, *saveFD_raw;

	start_pager(&saveFD, &saveFD_raw);
#endif

	setWidth();

	do {
		/* handle errors first */
		if ((reply = mapi_result_error(hdl)) != NULL) {
			if (formatter == TABLEformatter) {
				mnstr_printf(toConsole, "%s", reply);
			} else {
				mapi_explain_result(hdl, stderr);
			}
			errseen = 1;
			/* don't need to print something like '0
			 * tuples' if we got an error */
			continue;
		}

		switch (mapi_get_querytype(hdl)) {
		case Q_BLOCK:
		case Q_PARSE:
			/* should never see these */
			continue;
		case Q_UPDATE:
			SQLqueryEcho(hdl);
			if (formatter == RAWformatter ||
			    formatter == TESTformatter)
				mnstr_printf(toConsole, "[ " LLFMT "\t]\n", mapi_rows_affected(hdl));
			else {
				timerHumanStop();
				aff = mapi_rows_affected(hdl);
				lid = mapi_get_last_id(hdl);
				if (lid != -1) {
					mnstr_printf(toConsole,
							LLFMT " affected row%s, "
							"last generated key: " LLFMT "%s%s%s\n",
							aff,
							(aff == 1 ? "" : "s"),
							lid,
							singleinstr ? " (" : "",
							singleinstr ? timerHuman() : "",
							singleinstr ? ")" : "");
				} else {
					mnstr_printf(toConsole,
							LLFMT " affected row%s%s%s%s\n",
							aff,
							(aff == 1 ? "" : "s"),
							singleinstr ? " (" : "",
							singleinstr ? timerHuman() : "",
							singleinstr ? ")" : "");
				}
			}
			continue;
		case Q_SCHEMA:
			SQLqueryEcho(hdl);
			if (formatter == TABLEformatter)
				mnstr_printf(toConsole,
					      "operation successful\n");
			continue;
		case Q_TRANS:
			SQLqueryEcho(hdl);
			if (formatter == TABLEformatter)
				mnstr_printf(toConsole,
					      "auto commit mode: %s\n",
					      mapi_get_autocommit(mid) ? "on" : "off");
			continue;
		case Q_PREPARE:
			SQLqueryEcho(hdl);
			if (formatter == TABLEformatter)
				mnstr_printf(toConsole,
					      "execute prepared statement "
					      "using: EXEC %d(...)\n",
					      mapi_get_tableid(hdl));
		case Q_TABLE:
			timerHumanStop();
			break;
		default:
			if (formatter == TABLEformatter && specials != DEBUGmodifier) {
				int i;
				mnstr_printf(stderr_stream,
						"invalid/unknown response from server, "
						"ignoring output\n");
				for (i = 0; i < 5 && (reply = fetch_line(hdl)) != 0; i++)
					mnstr_printf(stderr_stream, "? %s\n", reply);
				if (i == 5 && fetch_line(hdl) != 0) {
					mnstr_printf(stderr_stream, "(remaining output omitted, "
							"use \\fraw to examine in detail)\n");
					/* skip over the unknown/invalid stuff, otherwise
					 * mapi_next_result call will assert in close_result
					 * because the logic there doesn't expect random unread
					 * garbage somehow */
					while (fetch_line(hdl) != 0)
						;
				}
				continue;
			}
		}

		/* note: specials != NOmodifier implies mode == SQL */
		if (specials != NOmodifier && debugMode()) {
			SQLdebugRendering(hdl);
			continue;
		}
		if (debugMode())
			RAWrenderer(hdl);
		else {
			switch (formatter) {
			case XMLformatter:
				XMLrenderer(hdl);
				break;
			case CSVformatter:
			case TABformatter:
				CSVrenderer(hdl);
				break;
			case TESTformatter:
				TESTrenderer(hdl);
				break;
			case TABLEformatter:
				switch (specials) {
				case DEBUGmodifier:
					SQLdebugRendering(hdl);
					break;
				default:
					SQLrenderer(hdl, singleinstr);
					break;
				}
				break;
			default:
				RAWrenderer(hdl);
				break;
			}
		}
	} while (!mnstr_errnr(toConsole) &&
		 (rc = mapi_needmore(hdl)) == MOK &&
		 (rc = mapi_next_result(hdl)) == 1);
	if (mnstr_errnr(toConsole)) {
		mnstr_clearerr(toConsole);
		fprintf(stderr, "write error\n");
		errseen = 1;
	}
#ifdef HAVE_POPEN
	end_pager(saveFD, saveFD_raw);
#endif

	return rc;
}

static int
doRequest(Mapi mid, const char *buf, int interactive)
{
	MapiHdl hdl;

	if (mode == SQL)
		SQLsetSpecial(buf);

	if ((hdl = mapi_query(mid, buf)) == NULL) {
		mapi_explain(mid, stderr);
		errseen = 1;
		return 1;
	}

	format_result(mid, hdl, 0);

	if (mapi_get_active(mid) == NULL || !interactive)
		mapi_close_handle(hdl);
	return 0;
}

#define CHECK_RESULT(mid, hdl, buf, break_or_continue)			\
		switch (mapi_error(mid)) {				\
		case MOK:						\
			/* everything A OK */				\
			break;						\
		case MERROR:						\
			/* some error, but try to continue */		\
			if (hdl) {					\
				mapi_explain_query(hdl, stderr);	\
				mapi_close_handle(hdl);			\
				hdl = NULL;				\
			} else						\
				mapi_explain(mid, stderr);		\
			errseen = 1;					\
			break_or_continue;				\
		case MTIMEOUT:						\
			/* lost contact with the server */		\
			if (hdl) {					\
				mapi_explain_query(hdl, stderr);	\
				mapi_close_handle(hdl);			\
				hdl = NULL;				\
			} else						\
				mapi_explain(mid, stderr);		\
			errseen = 1;					\
			timerEnd();					\
			free(buf);					\
			return 1;					\
		}

static int
doFile(Mapi mid, const char *file)
{
	FILE *fp;
	char *buf = NULL;
	size_t length;
	MapiHdl hdl = NULL;
	MapiMsg rc = MOK;
	int bufsize = 0;
	int first = 1;		/* first line processing */
	size_t skip;

	if (file == NULL || strcmp(file, "-") == 0)
		fp = stdin;
	else if ((fp = fopen(file, "r")) == NULL) {
		fprintf(stderr, "%s: cannot open\n", file);
		return 1;
	}

	bufsize = BLOCK - 1;
	buf = malloc(bufsize + 1);
	if (!buf) {
		fprintf(stderr, "cannot allocate memory for send buffer\n");
		if (file != NULL)
			fclose(fp);
		return 1;
	}

	timerStart();
	do {
		timerPause();
		if ((length = fread(buf, 1, bufsize, fp)) == 0) {
			/* end of file */
			if (file != NULL) {
				fclose(fp);
				file = NULL;
			}
			if (hdl == NULL)
				break;	/* nothing more to do */
		} else {
			buf[length] = 0;
			if (strchr(buf, '\0') < buf + length) {
				fprintf(stderr, "NULL byte in input\n");
				errseen = 1;
				break;
			}
		}
		timerResume();

		if (hdl == NULL) {
			hdl = mapi_query_prep(mid);
			CHECK_RESULT(mid, hdl, buf, continue);
		}

		if (first &&
		    length >= UTF8BOMLENGTH &&
		    strncmp(buf, UTF8BOM, UTF8BOMLENGTH) == 0)
			skip = UTF8BOMLENGTH;	/* skip Byte Order Mark (BOM) */
		else
			skip = 0;
		first = 0;
		if (length > skip) {
			assert(hdl != NULL);

			mapi_query_part(hdl, buf + skip, length - skip);
			CHECK_RESULT(mid, hdl, buf + skip, continue);

			/*  make sure there is a newline in the buffer */
			if (strchr(buf + skip, '\n') == NULL)
				continue;
		}

		assert(hdl != NULL);
		/* If the server wants more but we're at the end of
		   file (length == 0), notify the server that we
		   don't have anything more.  If the server still
		   wants more (shouldn't happen according to the
		   protocol) we break out of the loop (via the
		   continue).  The assertion at the end will then go
		   off. */
		if (mapi_query_done(hdl) == MMORE &&
				(length > 0 || mapi_query_done(hdl) == MMORE))
			continue;	/* get more data */

		CHECK_RESULT(mid, hdl, buf + skip, continue);

		rc = format_result(mid, hdl, 0);

		if (rc == MMORE && (length > 0 || mapi_query_done(hdl) != MOK))
			continue;	/* get more data */

		CHECK_RESULT(mid, hdl, buf + skip, continue);

		mapi_close_handle(hdl);
		hdl = NULL;

	} while (length > 0);
	/* reached on end of file */
	if (hdl)
		mapi_close_handle(hdl);
	timerEnd();

	free(buf);
	if (file != NULL)
		fclose(fp);
	mnstr_flush(toConsole);
	return errseen;
}

/* The options available for controlling input and rendering depends
 * on the language mode. */

static void
showCommands(void)
{
	/* shared control options */
	mnstr_printf(toConsole, "\\?      - show this message\n");
	if (mode == MAL)
		mnstr_printf(toConsole, "?pat    - MAL function help. pat=[modnme[.fcnnme][(][)]] wildcard *\n");
	mnstr_printf(toConsole, "\\<file  - read input from file\n");
	mnstr_printf(toConsole, "\\>file  - save response in file, or stdout if no file is given\n");
	mnstr_printf(toConsole, "\\|cmd   - pipe result to process, or stop when no command is given\n");
#ifdef HAVE_LIBREADLINE
	mnstr_printf(toConsole, "\\h      - show the readline history\n");
#endif
	mnstr_printf(toConsole, "\\t      - toggle timer\n");
	if (mode == SQL) {
		mnstr_printf(toConsole, "\\D table- dumps the table, or the complete database if none given.\n");
		mnstr_printf(toConsole, "\\d[Stvsfn]+ [obj] - list database objects, or describe if obj given\n");
		mnstr_printf(toConsole, "\\A      - enable auto commit\n");
		mnstr_printf(toConsole, "\\a      - disable auto commit\n");
	}
	mnstr_printf(toConsole, "\\e      - echo the query in sql formatting mode\n");
	mnstr_printf(toConsole, "\\f      - format using a built-in renderer {csv,tab,raw,sql,xml}\n");
	mnstr_printf(toConsole, "\\w#     - set maximal page width (-1=unlimited, 0=terminal width, >0=limit to num)\n");
	mnstr_printf(toConsole, "\\r#     - set maximum rows per page (-1=raw)\n");
	mnstr_printf(toConsole, "\\L file - save client/server interaction\n");
	mnstr_printf(toConsole, "\\X      - trace mclient code\n");
	mnstr_printf(toConsole, "\\q      - terminate session\n");
}

#define MD_TABLE    1
#define MD_VIEW     2
#define MD_SEQ      4
#define MD_FUNC     8
#define MD_SCHEMA  16

enum hmyesno { UNKNOWN, YES, NO };

static int
doFileByLines(Mapi mid, FILE *fp, const char *prompt, const char useinserts)
{
	char *line = NULL;
	char *oldbuf = NULL, *buf = NULL;
	size_t length;
	MapiHdl hdl = mapi_get_active(mid);
	MapiMsg rc = MOK;
	int lineno = 1;
	enum hmyesno hassysfuncs = UNKNOWN;

#ifdef HAVE_LIBREADLINE
	if (prompt == NULL)
#endif
		oldbuf = buf = malloc(BUFSIZ);

	do {
		if (prompt) {
			/* clear errors when interactive */
			errseen = 0;
		}
		mnstr_flush(toConsole);
		timerPause();
#ifdef HAVE_LIBREADLINE
		if (prompt) {
			rl_completion_func_t *func = NULL;

			if (buf)
				free(buf);
			if (hdl)
				func = suspend_completion();
			buf = readline(hdl ? "more>" : prompt);
			if (hdl)
				continue_completion(func);
			/* add a newline to the end since that makes
			   further processing easier */
			/* don't store shortcut command in the history */
			if (buf) {
				length = strlen(buf);
				if (length > 1)
					save_line(buf);
				buf = realloc(buf, length + 2);
				buf[length++] = '\n';
				buf[length] = 0;
			}
			line = buf;
		} else
#endif
		{
			int c = 0;

#ifndef HAVE_LIBREADLINE
			if (prompt) {
				fputs(hdl ? "more>" : prompt, stdout);
				fflush(stdout);
			}
#endif
			if (buf != oldbuf)
				free(buf);
			buf = oldbuf;
			line = buf;
			while (line < buf + BUFSIZ - 1 &&
			       (c = getc(fp)) != EOF) {
				if (c == 0) {
					fprintf(stderr, "NULL byte in input on line %d of input\n", lineno);
					/* read away rest of line */
					while ((c = getc(fp)) != EOF &&
					       c != '\n')
						;
					errseen = 1;
					c = 0x1234; /* indicate error */
					if (hdl) {
						mapi_close_handle(hdl);
						hdl = NULL;
					}
					break;
				}
				*line++ = c;
				if (c == '\n')
					break;
			}
			if (c == 0x1234)
				continue;
			if (line == buf)
				line = NULL;
			else {
				*line = 0;
				line = buf;
			}
		}
#ifdef HAVE_ICONV
		if (line != NULL && encoding != NULL && cd_in != (iconv_t) -1) {
			ICONV_CONST char *from = line;
			size_t fromlen = strlen(from);
			size_t tolen = 4 * fromlen + 1;
			char *to = malloc(tolen);

			line = to;
			iconv(cd_in, &from, &fromlen, &to, &tolen);
			*to = 0;
			if (!oldbuf)
				free(buf);
			buf = line;
		} else
#endif
			if (line != NULL &&
#ifdef HAVE_ICONV
			    encoding == NULL &&
#endif
			    lineno == 1 &&
			    strncmp(line, UTF8BOM, UTF8BOMLENGTH) == 0)
			line += UTF8BOMLENGTH;	/* skip Byte Order Mark (BOM) */
		lineno++;
		if (line == NULL) {
			/* end of file */
			if (hdl == NULL) {
				if (line != NULL)
					continue;
				/* nothing more to do */
				return errseen;
			}

			/* hdl != NULL, we should finish the current query */
			line = NULL;
			length = 0;
		} else
			length = strlen(line);
		/* the fp == stdin test is so that we don't treat '\\'
		   special for files that were passed on the command
		   line with -e in effect (see near the bottom of
		   main()) */
		if (hdl == NULL && length > 0 && line[length - 1] == '\n' && fp == stdin) {
			/* test for special commands */
			if (mode != MAL)
				while (length > 0 &&
				       (*line & ~0x7F) == 0 &&
				       isascii((int) *line) &&
				       isspace((int) *line)) {
					line++;
					length--;
				}
			/* in the switch, use continue if the line was
			   processed, use break to send to server */
			switch (*line) {
			case '\n':
			case '\0':
				break;
			case '\\':
				switch (line[1]) {
				case 'q':
					free(buf);
					return errseen;
#if 0
				case 't':
					mark = mark ? NULL : "Timer";
					if (mark2)
						free(mark2);
					mark2 = strdup(line + 2);
					continue;
#endif
				case 'X':
					/* toggle interaction trace */
					mapi_trace(mid, !mapi_get_trace(mid));
					continue;
				case 'A':
					if (mode != SQL)
						break;
					mapi_setAutocommit(mid, 1);
					continue;
				case 'a':
					if (mode != SQL)
						break;
					mapi_setAutocommit(mid, 0);
					continue;
				case 'w':
					pagewidth = atoi(line + 2);
					pagewidthset = pagewidth != 0;
					continue;
				case 'r':
					rowsperpage = atoi(line + 2);
					continue;
				case 'd': {
					char hasWildcard = 0;
					char hasSchema = 0;
					char wantsSystem = 0; 
					unsigned int x = 0;
					char *p;
					if (mode != SQL)
						break;
					while (isascii((int) line[length - 1]) &&
					       isspace((int) line[length - 1]))
						line[--length] = 0;
					for (line += 2; *line && !(isascii((int) *line) && isspace((int) *line)); line++)
					{
						switch (*line) {
							case 't':
								x |= MD_TABLE;
							break;
							case 'v':
								x |= MD_VIEW;
							break;
							case 's':
								x |= MD_SEQ;
							break;
							case 'f':
								x |= MD_FUNC;
							break;
							case 'n':
								x |= MD_SCHEMA;
							break;
							case 'S':
								wantsSystem = 1;
							break;
							default:
								fprintf(stderr, "unknown sub-command for \\d: %c\n", *line);
								length = 0;
								line[1] = '\0';
							break;
						}
					}
					if (length == 0)
						continue;
					if (x == 0) /* default to tables and views */
						x = MD_TABLE | MD_VIEW;
					for ( ; *line && isascii((int) *line) && isspace((int) *line); line++)
						;

					/* is the object quoted? we only support fully
					 * quoted objects, not partial ones */
					if (line[0] == '"' || line[0] == '\'') {
						for (p = line; *p; p++)
							;
						if (--p == line) {
							fprintf(stderr, "unmatched %c\n", line[0]);
							continue;
						} else if ((*p == '"' || *p == '\'') && *p != line[0]) {
							fprintf(stderr, "unexpected %c, expecting %c\n",
									*p, line[0]);
							continue;
						} else if (*p != line[0]) {
							fprintf(stderr, "unexpected end of string while "
									"looking for matching %c\n", line[0]);
							continue;
						}
						/* remove the quotes */
						line++;
						*p = '\0';
					} else {
						/* not quoted: lowercase it, and search for
						 * wildcards * and ?, replace them with SQL
						 * variants */
						for (p = line; *p; p++) {
							*p = tolower((int) *p);
							if (*p == '*') {
								*p = '%';
								hasWildcard = 1;
							} else if (*p == '?') {
								*p = '_';
								hasWildcard = 1;
							} else if (*p == '.') {
								hasSchema = 1;
							}
						}
					}
					if (*line && !hasWildcard) {
#ifdef HAVE_POPEN
						stream *saveFD, *saveFD_raw;

						start_pager(&saveFD, &saveFD_raw);
#endif
						if (x & MD_TABLE || x & MD_VIEW)
							describe_table(mid, NULL, line, toConsole, 1);
						if (x & MD_SEQ)
							describe_sequence(mid, NULL, line, toConsole);
						if (x & MD_FUNC)
							dump_functions(mid, toConsole, NULL, line);
						if (x & MD_SCHEMA)
							describe_schema(mid, line, toConsole);
#ifdef HAVE_POPEN
						end_pager(saveFD, saveFD_raw);
#endif
					} else {
						/* get all object names in current schema */
						char *type, *name, *schema;
						char q[4096];
						char nameq[128];
						char funcq[512];

						if (hassysfuncs == UNKNOWN)
							hassysfuncs = has_systemfunctions(mid) ? YES : NO;

						if (!*line) {
							line = "%";
							hasSchema = 0;
						}
						if (hasSchema) {
							snprintf(nameq, sizeof(nameq), 
									"AND \"s\".\"name\" || '.' || \"o\".\"name\" LIKE '%s'",
									line);
						} else {
							snprintf(nameq, sizeof(nameq),
									"AND \"s\".\"name\" = \"current_schema\" "
									"AND \"o\".\"name\" LIKE '%s'",
									line);
						}
						if (hassysfuncs == YES) {
							snprintf(funcq, sizeof(funcq),
								"SELECT \"o\".\"name\", "
								       "(CASE WHEN \"sf\".\"function_id\" IS NOT NULL "
									     "THEN 'SYSTEM ' "
										 "ELSE '' END "
									   "|| 'FUNCTION') AS \"type\", "
									   "CASE WHEN \"sf\".\"function_id\" IS NULL "
										 "THEN false "
										 "ELSE true END AS \"system\", "
									   "\"s\".\"name\" AS \"sname\", "
									   "%d AS \"ntype\" "
								"FROM \"sys\".\"functions\" \"o\" "
								      "LEFT JOIN \"sys\".\"systemfunctions\" \"sf\" "
									    "ON \"o\".\"id\" = \"sf\".\"function_id\", "
									  "\"sys\".\"schemas\" \"s\" "
								"WHERE \"o\".\"schema_id\" = \"s\".\"id\" "
								  "%s ",
								MD_FUNC,
								nameq);
						} else {
							snprintf(funcq, sizeof(funcq),
								"SELECT \"o\".\"name\", "
								       "(CASE WHEN \"o\".\"id\" <= 2000 "
									     "THEN 'SYSTEM ' "
										 "ELSE '' END "
									   "|| 'FUNCTION') AS \"type\", "
									   "CASE WHEN \"o\".\"id\" > 2000 "
										 "THEN false "
										 "ELSE true END AS \"system\", "
									   "\"s\".\"name\" AS \"sname\", "
									   "%d AS \"ntype\" "
								"FROM \"sys\".\"functions\" \"o\", "
									  "\"sys\".\"schemas\" \"s\" "
								"WHERE \"o\".\"schema_id\" = \"s\".\"id\" "
								  "%s ",
								MD_FUNC,
								nameq);
						}
						snprintf(q, sizeof(q),
								"SELECT \"name\", "
								       "CAST(\"type\" AS VARCHAR(30)) AS \"type\", "
								       "\"system\", \"sname\", "
									   "\"ntype\" "
								"FROM ("
								"SELECT \"o\".\"name\", "
								       "(CASE \"o\".\"system\" "
									     "WHEN true THEN 'SYSTEM ' "
										 "ELSE '' "
										 "END || "
							           "CASE \"o\".\"type\" "
									     "WHEN 0 THEN 'TABLE' "
										 "WHEN 1 THEN 'VIEW' "
										 "ELSE '' "
									   "END) AS \"type\", "
								       "\"o\".\"system\", "
									   "\"s\".\"name\" AS \"sname\", "
									   "CASE \"o\".\"type\" "
									     "WHEN 0 THEN %d "
										 "WHEN 1 THEN %d "
										 "ELSE 0 "
									   "END AS \"ntype\" "
								"FROM \"sys\".\"_tables\" \"o\", "
								     "\"sys\".\"schemas\" \"s\" "
								"WHERE \"o\".\"schema_id\" = \"s\".\"id\" "
								  "%s "
								  "AND \"o\".\"type\" IN (0, 1) "
								"UNION "
								"SELECT \"o\".\"name\", "
								       "'SEQUENCE' AS \"type\", "
									   "false AS \"system\", "
									   "\"s\".\"name\" AS \"sname\", "
									   "%d AS \"ntype\" "
								"FROM \"sys\".\"sequences\" \"o\", "
								     "\"sys\".\"schemas\" \"s\" "
								"WHERE \"o\".\"schema_id\" = \"s\".\"id\" "
								  "%s "
								"UNION "
								"%s "
								"UNION "
								"SELECT NULL AS \"name\", "
								       "(CASE WHEN \"o\".\"name\" LIKE 'sys' "
									    "THEN 'SYSTEM ' "
										"ELSE '' END "
										"|| 'SCHEMA') AS \"type\", "
									   "CASE WHEN \"o\".\"name\" LIKE 'sys' "
									   "THEN true "
									   "ELSE false END AS \"system\", "
									   "\"o\".\"name\" AS \"sname\", "
									   "%d AS \"ntype\" "
								"FROM \"sys\".\"schemas\" \"o\" "
								"WHERE \"o\".\"name\" LIKE '%s' "
								") AS \"all\" "
								"WHERE \"ntype\" & %u > 0 "
								  "%s "
								"ORDER BY \"system\", \"name\"",
								MD_TABLE, MD_VIEW,
								nameq,
								MD_SEQ,
								nameq,
								funcq,
								MD_SCHEMA,
								line,
								x,
								(wantsSystem ?
								  "" :
								  "AND \"system\" = false"));
						hdl = mapi_query(mid, q);
						CHECK_RESULT(mid, hdl, buf, continue);
						while (fetch_row(hdl) == 5) {
							name = mapi_fetch_field(hdl, 0);
							type = mapi_fetch_field(hdl, 1);
							schema = mapi_fetch_field(hdl, 3);
							mnstr_printf(toConsole,
									  "%-*s  %s%s%s\n",
									  mapi_get_len(hdl, 1),
									  type, schema,
									  name != NULL ? "." : "",
									  name != NULL ? name : "");
						}
						mapi_close_handle(hdl);
						hdl = NULL;
					}
					continue;
				}
				case 'D':{
#ifdef HAVE_POPEN
					stream *saveFD, *saveFD_raw;
#endif

					if (mode != SQL)
						break;
					while (isascii((int) line[length - 1]) &&
					       isspace((int) line[length - 1]))
						line[--length] = 0;
					if (line[2] && !(isascii((int) line[2]) && isspace(line[2]))) {
						fprintf(stderr, "space required after \\D\n");
						continue;
					}
					for (line += 2; *line && isascii((int) *line) && isspace((int) *line); line++)
						;
#ifdef HAVE_POPEN
					start_pager(&saveFD, &saveFD_raw);
#endif
					if (*line) {
						mnstr_printf(toConsole, "START TRANSACTION;\n");
						dump_table(mid, NULL, line, toConsole, 0, 1, useinserts);
						mnstr_printf(toConsole, "COMMIT;\n");
					} else
						dump_database(mid, toConsole, 0, useinserts);
#ifdef HAVE_POPEN
					end_pager(saveFD, saveFD_raw);
#endif
					continue;
				}
				case '<':
					/* read commands from file */
					while (isascii((int) line[length - 1]) && isspace((int) line[length - 1]))
						line[--length] = 0;
					for (line += 2; *line && isascii((int) *line) && isspace((int) *line); line++)
						;
					doFile(mid, line);
					continue;
				case '>':
					/* redirect output to file */
					while (isascii((int) line[length - 1]) && isspace((int) line[length - 1]))
						line[--length] = 0;
					for (line += 2; *line && isascii((int) *line) && isspace((int) *line); line++)
						;
					if (toConsole != stdout_stream && toConsole != stderr_stream) {
						mnstr_close(toConsole);
						mnstr_destroy(toConsole);
					}
					if (*line == 0 || strcmp(line, "stdout") == 0)
						toConsole = stdout_stream;
					else if (strcmp(line, "stderr") == 0)
						toConsole = stderr_stream;
					else if ((toConsole = open_wastream(line)) == NULL ||
						 mnstr_errnr(toConsole)) {
						if (toConsole != NULL) {
							mnstr_close(toConsole);
							mnstr_destroy(toConsole);
						}
						toConsole = stdout_stream;
						fprintf(stderr, "Cannot open %s\n", line);
					}
					continue;
				case 'L':
					free(logfile);
					logfile = NULL;
					while (isascii((int) line[length - 1]) && isspace((int) line[length - 1]))
						line[--length] = 0;
					for (line += 2; *line && isascii((int) *line) && isspace((int) *line); line++)
						;
					if (*line == 0) {
						/* turn of logging */
						mapi_log(mid, NULL);
					} else {
						logfile = strdup(line);
						mapi_log(mid, logfile);
					}
					continue;
				case '?':
					showCommands();
					continue;
#ifdef HAVE_POPEN
				case '|':
					if (pager)
						free(pager);
					pager = NULL;
					setWidth();	/* reset to system default */

					while (isascii((int) line[length - 1]) && isspace((int) line[length - 1]))
						line[--length] = 0;
					for (line += 2; *line && isascii((int) *line) && isspace((int) *line); line++)
						;
					if (*line == 0)
						continue;
					pager = strdup(line);
					continue;
#endif
#ifdef HAVE_LIBREADLINE
				case 'h':
				{
					int h;
					char *nl;

					for (h = 0; h < history_length; h++) {
						nl = history_get(h) ? history_get(h)->line : 0;
						if (nl)
							mnstr_printf(toConsole, "%d %s\n", h, nl);
					}
					continue;
				}
/* for later
				case '!':
				{
					char *nl;

					nl = strchr(line, '\n');
					if (nl)
						*nl = 0;
					if (history_expand(line + 2, &nl)) {
						mnstr_printf(toConsole, "%s\n", nl);
					}
					mnstr_printf(toConsole, "Expansion needs work\n");
					continue;
				}
*/
#endif
				case 'e':
					echoquery = 1;
					continue;
				case 'f':
					while (isascii((int) line[length - 1]) && isspace((int) line[length - 1]))
						line[--length] = 0;
					for (line += 2; *line && isascii((int) *line) && isspace((int) *line); line++)
						;
					if (*line == 0) {
						mnstr_printf(toConsole, "Current formatter: ");
						switch (formatter) {
						case RAWformatter:
							mnstr_printf(toConsole, "raw\n");
							break;
						case TABLEformatter:
							mnstr_printf(toConsole, "sql\n");
							break;
						case CSVformatter:
							mnstr_printf(toConsole, "csv\n");
							break;
						case TABformatter:
							mnstr_printf(toConsole, "tab\n");
							break;
						case TESTformatter:
							mnstr_printf(toConsole, "test\n");
							break;
						case XMLformatter:
							mnstr_printf(toConsole, "xml\n");
							break;
						default:
							mnstr_printf(toConsole, "none\n");
							break;
						}
					} else
						setFormatter(line);
					continue;
				default:
					showCommands();
					continue;
				}
			}
		}

		if (hdl == NULL) {
			timerStart();
			hdl = mapi_query_prep(mid);
			CHECK_RESULT(mid, hdl, buf, continue);
		} else
			timerResume();

		assert(hdl != NULL);

		if (length > 0) {
			SQLsetSpecial(line);
			mapi_query_part(hdl, line, length);
			CHECK_RESULT(mid, hdl, buf, continue);
		}

		/* If the server wants more but we're at the
		   end of file (line == NULL), notify the
		   server that we don't have anything more.
		   If the server still wants more (shouldn't
		   happen according to the protocol) we break
		   out of the loop (via the continue).  The
		   assertion at the end will then go off. */
		if (mapi_query_done(hdl) == MMORE) {
			if (line != NULL) {
				continue;	/* get more data */
			} else if (mapi_query_done(hdl) == MMORE) {
				hdl = NULL;
				continue;	/* done */
			}
		}
		CHECK_RESULT(mid, hdl, buf, continue);

		rc = format_result(mid, hdl, 1);

		if (rc == MMORE && (line != NULL || mapi_query_done(hdl) != MOK))
			continue;	/* get more data */

		CHECK_RESULT(mid, hdl, buf, continue);

		timerEnd();
		mapi_close_handle(hdl);
		hdl = NULL;
	} while (line != NULL);
	/* reached on end of file */
	assert(hdl == NULL);
	return errseen;
}

static void
usage(const char *prog, int xit)
{
	fprintf(stderr, "Usage: %s [ options ]\n", prog);
	fprintf(stderr, "\nOptions are:\n");
#ifdef HAVE_SYS_UN_H
	fprintf(stderr, " -h hostname | --host=hostname    host or UNIX domain socket to connect to\n");
#else
	fprintf(stderr, " -h hostname | --host=hostname    host to connect to\n");
#endif
	fprintf(stderr, " -p portnr   | --port=portnr      port to connect to\n");
	fprintf(stderr, " -u user     | --user=user        user id\n");
	fprintf(stderr, " -d database | --database=database  database to connect to\n");

	fprintf(stderr, " -e          | --echo             echo the query\n");
#ifdef HAVE_ICONV
	fprintf(stderr, " -E charset  | --encoding=charset specify encoding (character set) of the terminal\n");
#endif
	fprintf(stderr, " -f kind     | --format=kind      specify output format {csv,tab,raw,sql,xml}\n");
	fprintf(stderr, " -H          | --history          load/save cmdline history (default off)\n");
	fprintf(stderr, " -i          | --interactive      read stdin after command line args\n");
	fprintf(stderr, " -l language | --language=lang    {sql,mal}\n");
	fprintf(stderr, " -L logfile  | --log=logfile      save client/server interaction\n");
	fprintf(stderr, " -s stmt     | --statement=stmt   run single statement\n");
	fprintf(stderr, " -X          | --Xdebug           trace mapi network interaction\n");
#ifdef HAVE_POPEN
	fprintf(stderr, " -| cmd      | --pager=cmd        for pagination\n");
#endif
	fprintf(stderr, " -?          | --help             show this usage message\n");

	fprintf(stderr, "\nSQL specific opions \n");
	fprintf(stderr, " -n nullstr  | --null=nullstr     change NULL representation for sql, csv and tab output modes\n");
	fprintf(stderr, " -r nr       | --rows=nr          for pagination\n");
	fprintf(stderr, " -w nr       | --width=nr         for pagination\n");
	fprintf(stderr, " -D          | --dump             create an SQL dump\n");
	fprintf(stderr, " -N          | --inserts          use INSERT INTO statements when dumping\n");
	exit(xit);
}

/* hardwired defaults, only used if monet environment cannot be found */
#define defaultPort 50000

int
main(int argc, char **argv)
{
	int port = 0;
	char *user = NULL;
	char *passwd = NULL;
	char *host = NULL;
	char *command = NULL;
	char *dbname = NULL;
	int trace = 0;
	int dump = 0;
	int useinserts = 0;
	int c = 0;
	Mapi mid;
	int save_history = 0;
	int interactive = 0;
	int has_fileargs = 0;
	int option_index = 0;
	struct stat statb;
	stream *config = NULL;
	char user_set_as_flag = 0;
	static struct option long_options[] = {
		{"database", 1, 0, 'd'},
		{"dump", 0, 0, 'D'},
		{"inserts", 0, 0, 'N'},
		{"echo", 0, 0, 'e'},
#ifdef HAVE_ICONV
		{"encoding", 1, 0, 'E'},
#endif
		{"format", 1, 0, 'f'},
		{"help", 0, 0, '?'},
		{"history", 0, 0, 'H'},
		{"host", 1, 0, 'h'},
		{"interactive", 0, 0, 'i'},
		{"language", 1, 0, 'l'},
		{"log", 1, 0, 'L'},
		{"null", 1, 0, 'n'},
#ifdef HAVE_POPEN
		{"pager", 1, 0, '|'},
#endif
		{"port", 1, 0, 'p'},
		{"rows", 1, 0, 'r'},
		{"statement", 1, 0, 's'},
#if 0
		{"time", 0, 0, 't'},
#endif
		{"user", 1, 0, 'u'},
		{"version", 0, 0, 'v'},
		{"width", 1, 0, 'w'},
		{"Xdebug", 0, 0, 'X'},
		{0, 0, 0, 0}
	};

#ifndef WIN32
	/* don't set locale on Windows: setting the locale like this
	 * causes the output to be converted (we could set it to
	 * ".OCP" if we knew for sure that we were running in a cmd
	 * window) */
#ifdef HAVE_SETLOCALE
	setlocale(LC_ALL, "");
#endif
#endif
	toConsole = stdout_stream = file_wastream(stdout, "stdout");
	toConsole_raw = toConsole;
	stderr_stream = file_wastream(stderr, "stderr");

	/* execute from stdin? */
	if (fstat(fileno(stdin), &statb) == 0 && S_ISCHR(statb.st_mode))
		interactive_stdin = 1;

#if 0
	mark = NULL;
	mark2 = NULL;
#endif

	/* parse config file first, command line options override */
	if (getenv("DOTMONETDBFILE") == NULL) {
		if (stat(".monetdb", &statb) == 0) {
			config = open_rastream(".monetdb");
		} else if (getenv("HOME") != NULL) {
			char buf[1024];
			snprintf(buf, sizeof(buf), "%s/.monetdb", getenv("HOME"));
			if (stat(buf, &statb) == 0) {
				config = open_rastream(buf);
			}
		}
	} else {
		char *cfile = getenv("DOTMONETDBFILE");
		if (strcmp(cfile, "") != 0) {
			if (stat(cfile, &statb) == 0) {
				config = open_rastream(cfile);
			} else {
				mnstr_printf(stderr_stream,
					      "failed to open file '%s': %s\n",
					      cfile, strerror(errno));
			}
		}
	}

	if (config != NULL) {
		char buf[1024];
		char *q;
		ssize_t len;
		int line = 0;
		while ((len = mnstr_readline(config, buf, sizeof(buf) - 1)) > 0) {
			line++;
			buf[len - 1] = '\0';	/* drop newline */
			if (buf[0] == '#' || buf[0] == '\0')
				continue;
			if ((q = strchr(buf, '=')) == NULL) {
				mnstr_printf(stderr_stream, "%s:%d: syntax error: %s\n", mnstr_name(config), line, buf);
				continue;
			}
			*q++ = '\0';
			/* this basically sucks big time, as I can't easily set
			 * a default, hence I only do things I think are useful
			 * for now, needs a better solution */
			if (strcmp(buf, "user") == 0) {
				user = strdup(q);	/* leak */
				q = NULL;
			} else if (strcmp(buf, "password") == 0 || strcmp(buf, "passwd") == 0) {
				passwd = strdup(q);	/* leak */
				q = NULL;
			} else if (strcmp(buf, "language") == 0) {
				language = strdup(q);	/* leak */
				if (strstr(language, "sql") == language) {
					mode = SQL;
					q = NULL;
				} else if (strcmp(language, "mal") == 0) {
					mode = MAL;
					q = NULL;
				} else {
					/* make sure we don't set garbage */
					mnstr_printf(stderr_stream,
						      "%s:%d: unsupported "
						      "language: %s\n",
						      mnstr_name(config),
						      line, q);
					free(language);
					language = NULL;
					q = NULL;
				}
			} else if (strcmp(buf, "save_history") == 0) {
				if (strcmp(q, "true") == 0 ||
				    strcmp(q, "on") == 0)
				{
					save_history = 1;
					q = NULL;
				} else if (strcmp(q, "false") == 0 ||
					   strcmp(q, "off") == 0)
				{
					save_history = 0;
					q = NULL;
				}
			} else if (strcmp(buf, "format") == 0) {
				output = strdup(q);
			} else if (strcmp(buf, "width") == 0) {
				pagewidth = atoi(q);
				pagewidthset = pagewidth != 0;
			}
			if (q != NULL)
				mnstr_printf(stderr_stream,
					      "%s:%d: unknown property: %s\n",
					      mnstr_name(config), line, buf);
		}
		mnstr_destroy(config);
	}

	while ((c = getopt_long(argc, argv, "DNd:e"
#ifdef HAVE_ICONV
				"E:"
#endif
				"f:h:iL:l:n:"
#ifdef HAVE_POPEN
				"|:"
#endif
#if 0
				"t"
#endif
				"w:r:p:s:Xu:vH?",
				long_options, &option_index)) != -1) {
		switch (c) {
		case 0:
#ifdef HAVE_POPEN
			if (strcmp(long_options[option_index].name, "pager") == 0) {
				pager = optarg;
				(void) pager;	/* will be further used later */
			}
#endif
			break;
		case 'e':
			echoquery = 1;
			break;
#ifdef HAVE_ICONV
		case 'E':
			encoding = optarg;
			break;
#endif
		case 'L':
			logfile = strdup(optarg);
			break;
		case 'l':
			/* accept unambiguous prefix of language */
			if (strcmp(optarg, "sql") == 0 ||
			    strcmp(optarg, "sq") == 0 || strcmp(optarg, "s") == 0 ||
				strstr(optarg, "sql") == optarg) {
				language = optarg;
				mode = SQL;
			} else if (strcmp(optarg, "mal") == 0 ||
				   strcmp(optarg, "ma") == 0) {
				language = "mal";
				mode = MAL;
			} else if (strcmp(optarg, "msql") == 0) {
				language = "msql";
				mode = MAL;
			} else {
				fprintf(stderr, "language option needs to be sql or mal\n");
				exit(-1);
			}
			break;
		case 'n':
			nullstring = optarg;
			break;
		case 'u':
			user = optarg;
			user_set_as_flag = 1;
			break;
		case 'f':
			if (output != NULL)
				free(output);
			output = strdup(optarg);	/* output format */
			break;
		case 'i':
			interactive = 1;
			break;
		case 'h':
			host = optarg;
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'D':
			dump = 1;
			break;
		case 'N':
			useinserts = 1;
			break;
		case 'd':
			dbname = optarg;
			break;
		case 's':
			command = optarg;
			break;
		case 'w':
			pagewidth = atoi(optarg);
			pagewidthset = pagewidth != 0;
			break;
		case 'r':
			rowsperpage = atoi(optarg);
			break;
#ifdef HAVE_POPEN
		case '|':
			pager = optarg;
			break;
#endif
#if 0
		case 't':
			mark = "Timer";
			break;
#endif
		case 'X':
			trace = MAPI_TRACE;
			break;
		case 'H':
			save_history = 1;
			break;
		case 'v':
			mnstr_printf(toConsole,
					"mclient, the MonetDB interactive terminal (%s)\n",
					MONETDB_RELEASE);
#ifdef HAVE_LIBREADLINE
			mnstr_printf(toConsole,
					"support for command-line editing compiled-in\n");
#endif
			return(0);
		case '?':
			/* a bit of a hack: look at the option that the
			   current `c' is based on and see if we recognize
			   it: if -? or --help, exit with 0, else with -1 */
			usage(argv[0], strcmp(argv[optind - 1], "-?") == 0 || strcmp(argv[optind - 1], "--help") == 0 ? 0 : -1);
			break;
		default:
			usage(argv[0], -1);
		}
	}
#ifdef HAVE_ICONV
#ifdef HAVE_NL_LANGINFO
	if (encoding == NULL)
		encoding = nl_langinfo(CODESET);
#endif
	if (encoding != NULL && strcasecmp(encoding, "utf-8") == 0)
		encoding = NULL;
	if (encoding != NULL) {
		toConsole = iconv_wstream(toConsole, encoding, "stdout");
		if (toConsole == NULL || mnstr_errnr(toConsole)) {
			fprintf(stderr, "warning: cannot convert local character set %s to UTF-8\n", encoding);
			if (toConsole != NULL) {
				mnstr_close(toConsole);
				mnstr_destroy(toConsole);
			}
			toConsole = toConsole_raw;
		}
		stdout_stream = toConsole;
		if ((cd_in = iconv_open("utf-8", encoding)) == (iconv_t) -1)
			fprintf(stderr, "warning: cannot convert UTF-8 to local character set %s\n", encoding);
	}
#endif /* HAVE_ICONV */

	/* when config file would provide defaults */
	if (user_set_as_flag)
		passwd = NULL;

	if (user == NULL)
		user = simple_prompt("user", BUFSIZ, 1, prompt_getlogin());
	if (passwd == NULL)
		passwd = simple_prompt("password", BUFSIZ, 0, NULL);

	mid = mapi_connect(host, port, user, passwd, language, dbname);

	if (mid == NULL) {
		fprintf(stderr, "failed to allocate Mapi structure\n");
		exit(2);
	}

	if (mapi_error(mid)) {
		if (trace)
			mapi_explain(mid, stderr);
		else
			fprintf(stderr, "%s\n", mapi_error_str(mid));
		exit(2);
	}
	mapi_cache_limit(mid, -1);
	if (dump) {
		if (mode == SQL) {
			exit(dump_database(mid, toConsole, 0, useinserts));
		} else {
			fprintf(stderr, "Dump only supported for SQL\n");
			exit(1);
		}
	}

	if (logfile)
		mapi_log(mid, logfile);

	mapi_trace(mid, trace);
	if (output) {
		setFormatter(output);
	} else {
		if (mode == SQL) {
			setFormatter("sql");
		} else {
			setFormatter("raw");
		}
	}

	c = 0;
	has_fileargs = optind != argc;

	/* give the user a welcome message with some general info */
	if ((interactive || (!has_fileargs && command == NULL)) && interactive_stdin) {
		char *lang;

		if (mode == SQL)
			lang = "/SQL";
		else
			lang = "";

		mnstr_printf(toConsole,
			      "Welcome to mclient, the MonetDB%s "
			      "interactive terminal (%s)\n",
			      lang, MONETDB_RELEASE);

		if (mode == SQL)
			dump_version(mid, toConsole, "Database:");

		mnstr_printf(toConsole, "Type \\q to quit, \\? for a list of available commands\n");
		if (mode == SQL)
			mnstr_printf(toConsole, "auto commit mode: on\n");
	}

	if (command != NULL) {
#ifdef HAVE_ICONV
		if (encoding != NULL && cd_in != (iconv_t) -1) {
			ICONV_CONST char *from = command;
			size_t fromlen = strlen(from);
			size_t tolen = 4 * fromlen + 1;
			char *to = malloc(tolen);

			command = to;
			iconv(cd_in, &from, &fromlen, &to, &tolen);
			*to = 0;
		}
#endif
		/* execute from command-line, need interactive to know whether
		 * to keep the mapi handle open */
		timerStart();
		c = doRequest(mid, command, (interactive || (!has_fileargs && command == NULL)));
		timerEnd();
	}

	if (optind < argc) {
		/* execute from file(s) */
		while (optind < argc) {
			if (echoquery &&
			    strcmp(argv[optind], "-") != 0 &&
			    stat(argv[optind], &statb) == 0 &&
			    S_ISREG(statb.st_mode) &&
			    statb.st_size < 1024*1024) {
				/* a bit of a hack: process file
				   line-by-line if using -e (--echo)
				   and the input file isn't "too
				   large" so that the queries that are
				   echoed have something to do with
				   the output that follows (otherwise
				   we just echo the start of the file
				   for each query) */
				FILE *fp;
				if ((fp = fopen(argv[optind], "r")) == NULL) {
					fprintf(stderr, "%s: cannot open\n", argv[optind]);
					c |= 1;
				} else {
					/* note that since fp != stdin,
					   we don't treat \ special */
					c |= doFileByLines(mid, fp, NULL, useinserts);
					fclose(fp);
				}
			} else
				c |= doFile(mid, argv[optind]);
			optind++;
		}
	}

	if (interactive || (!has_fileargs && interactive_stdin && command == NULL)) {
		char *prompt = NULL;

		if (interactive_stdin) {
#ifdef HAVE_LIBREADLINE
			init_readline(mid, language, save_history);
#else
			(void) save_history;	/* pacify compiler */
#endif
			/* reading from terminal, prepare prompt */
			setPrompt();
			prompt = promptbuf;
			fromConsole = stdin;
		}
		/* use default rendering if not overruled at commandline */
		c = doFileByLines(mid, stdin, prompt, useinserts);

#ifdef HAVE_LIBREADLINE
		if (interactive_stdin) {
			deinit_readline();
		}
#endif
	} else if (!has_fileargs && command == NULL) {
		c = doFile(mid, NULL);
	}
	mapi_destroy(mid);
	mnstr_destroy(stdout_stream);
	mnstr_destroy(stderr_stream);
	return c;
}
