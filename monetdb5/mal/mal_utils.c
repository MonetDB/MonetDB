/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c) M. Kersten
 * Passing strings between front-end and kernel often require marshalling.
 */
#include "monetdb_config.h"
#include "mal_utils.h"

void
mal_unquote(char *msg)
{
	char *p = msg, *s;

	s = p;
	while (*p) {
		if (*p == '\\') {
			p++;
			switch (*p) {
			case 'n':
				*s = '\n';
				break;
			case 't':
				*s = '\t';
				break;
			case 'r':
				*s = '\r';
				break;
			case 'f':
				*s = '\f';
				break;
			case '0':
			case '1':
			case '2':
			case '3':
				/* this could be the start of
				   an octal sequence, check it
				   out */
				if (p[1] && p[2] && p[1] >= '0' && p[1] <= '7' && p[2] >= '0' && p[2] <= '7') {
					*s = (char)(((p[0] - '0') << 6) | ((p[1] - '0') << 3) | (p[2] - '0'));
					p += 2;
					break;
				}
				/* fall through */
			default:
				*s = *p;
				break;
			}
			p++;
		} else {
			*s = *p++;
		}
		s++;
	}
	*s = 0;			/* close string */
}

char *
mal_quote(const char *msg, size_t size)
{
	char *s = GDKmalloc(size * 2 + 1);	/* we absolutely don't need more than this (until we start producing octal escapes */
	char *t = s;

	if ( s == NULL)
		return NULL;
	while (size > 0) {
		size--;
		switch (*msg) {
		case '"':
			*t++ = '\\';
			*t++ = '"';
			break;
		case '\n':
			*t++ = '\\';
			*t++ = 'n';
			break;
		case '\t':
			*t++ = '\\';
			*t++ = 't';
			break;
		case '\\':
			*t++ = '\\';
			*t++ = '\\';
			break;
		default:
			*t++ = *msg;
			break;
		}
		msg++;
		/* also deal with binaries */
	}
	*t = 0;
	return s;
}
