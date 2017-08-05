/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#ifndef _MSC_VER
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif
#endif
#include "mprompt.h"

#ifdef _MSC_VER
#define fileno _fileno
#include <conio.h>
#endif

#ifndef HAVE_GETLOGIN
/* we assume this must be windows */
static char *defaultlogin = "win32";
#endif

char *
prompt_getlogin(void)
{
#ifdef HAVE_GETLOGIN
# ifdef __sun__
	/* from Solaris' getlogin manpage:
	 *  The correct procedure for determining the login name is to call
	 *  cuserid(3C), or to call getlogin() and if  it fails to call
	 *  getpwuid(3C). */
	return cuserid(NULL);
# else
	return getlogin();
# endif
#else
	return defaultlogin;
#endif
}

#ifdef _MSC_VER
char *
simple_prompt(const char *prompt, int maxlen, int echo, const char *def)
{
	size_t length = 0;
	char *destination = NULL;

	destination = (char *) malloc(maxlen + 2);
	if (!destination)
		return NULL;

	if (prompt) {
		_cputs(prompt);
		if (def) {
			_cputs("(");
			_cputs(def);
			_cputs(")");
		}
		_cputs(":");
	}
	if (echo) {
		_cgets_s(destination, maxlen, &length);
		while (length > 0 &&
		       (destination[length - 1] == '\n' ||
			destination[length - 1] == '\r'))
			destination[--length] = 0;
	} else {
		int c;

		while ((c = _getch()) != '\r' && c != '\n') {
			if (length < (size_t) maxlen)
				destination[length++] = c;
		}
		destination[length] = 0;
		_cputs("\r\n");
	}
	if (length == 0 && def)
		strcpy(destination, def);
	return destination;
}
#else
char *
simple_prompt(const char *prompt, int maxlen, int echo, const char *def)
{
	size_t length = 0;
	char *destination = NULL;
	FILE *termin = NULL, *termout = NULL;

#ifdef HAVE_TERMIOS_H
	struct termios t_orig, t;
#else
	(void) echo;
#endif

	destination = (char *) malloc(maxlen + 2);
	if (!destination)
		return NULL;

	termin = fopen("/dev/tty", "r");
	termout = fopen("/dev/tty", "w");

	if (termin == NULL || termout == NULL) {
		if (termin)
			fclose(termin);
		if (termout)
			fclose(termout);
		termin = stdin;
		termout = stderr;
	}

#ifdef HAVE_TERMIOS_H
	if (!echo) {
		tcgetattr(fileno(termin), &t);
		t_orig = t;
		t.c_lflag &= ~ECHO;
		tcsetattr(fileno(termin), TCSAFLUSH, &t);
	}
#endif
	if (prompt) {
		if (def)
			fprintf(termout, "%s(%s):", prompt, def);
		else
			fprintf(termout, "%s:", prompt);
		fflush(termout);
	}
	if (fgets(destination, maxlen, termin) == NULL)
		destination[0] = '\0';

	length = strlen(destination);
	if (length > 0 && destination[length - 1] != '\n') {
		char buf[128];
		size_t buflen;

		do {
			if (fgets(buf, sizeof(buf), termin) == NULL)
				break;
			buflen = strlen(buf);
		} while (buflen > 0 && buf[buflen - 1] != '\n');
	}

	if (length > 0 && destination[length - 1] == '\n')
		destination[length - 1] = '\0';
#ifdef HAVE_TERMIOS_H
	if (!echo) {
		tcsetattr(fileno(termin), TCSAFLUSH, &t_orig);
		fputs("\n", termout);
		fflush(termout);
	}
#endif
	if (termin != stdin)
		fclose(termin);
	if (termout != stdout)
		fclose(termout);
	if (destination[0] == 0 && def)
		strcpy(destination, def);
	return destination;
}
#endif /* _MSC_VER */
