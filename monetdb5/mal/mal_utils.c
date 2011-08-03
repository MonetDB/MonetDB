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
 * @a M. Kersten
 * @v 0.0
 * @+ Some basic utiliteses
 * Passing strings between front-end and kernel often
 * require marshalling.
 */
/*
 * @-
 * At any point we should be able to construct an ascii representation of
 * the type descriptor. Including the variable references.
 * Unquoting of a string is done in place. It returns the start
 * of the unquoted part section.
 */
#include "monetdb_config.h"
#include "mal_utils.h"

#define PLACEHOLDER '?'
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
	char *s = GDKmalloc(strlen(msg) * 2 + 1);	/* we absolutely don't need more than this (until we start producing octal escapes */
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

void formatVolume(str buf, int len, lng vol){
	if( vol <1024)
		snprintf(buf,len,LLFMT,vol);
	else
	if( vol <1024*1024)
		snprintf(buf,len,LLFMT "K",vol/1024);
	else
	if( vol <1024* 1024*1024)
		snprintf(buf,len, LLFMT "M",vol/1024/1024);
	else
		snprintf(buf,len, "%6.1fG",vol/1024.0/1024/1024);
}

