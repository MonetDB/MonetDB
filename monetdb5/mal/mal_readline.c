/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#ifndef HAVE_EMBEDDED
#include "mal.h"
#include "mal_client.h"
#include "mal_scenario.h"
#include "mal_readline.h"
#include "mal_debugger.h"

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
	printf("\\q\t- terminate session\n");
}

char *
getConsoleInput(Client c, const char *prompt, int linemode, int exit_on_error)
{
	char *line = NULL;
	char *buf = NULL;
	size_t length;
	(void) exit_on_error;
	(void) linemode;
	(void) c;

	do {
		if (prompt) {
			fputs(prompt, stdout);
			fflush(stdout);
		}
		if (buf == NULL) {
			buf= malloc(BUFSIZ);
			if( buf == NULL){
				GDKerror("getConsoleInput: " MAL_MALLOC_FAIL);
				return NULL;
			}
		}
		line = fgets(buf, BUFSIZ, stdin);

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
			       isspace((unsigned char) *line)) {
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
			case '?':
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
	buf= getConsoleInput(cntxt, cntxt->prompt, 0, 1);
	if( buf) {
		size_t len= strlen(buf);
		if( len >= cntxt->fdin->size) {
			char *nbuf;
			/* extremely dirty inplace buffer overwriting */
			nbuf= realloc(cntxt->fdin->buf, len+1);
			if( nbuf == NULL) {
				GDKerror("readConsole: " MAL_MALLOC_FAIL);
				free(buf);
				goto bailout;
			}
			cntxt->fdin->buf = nbuf;
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
	return -1;
}
#endif
