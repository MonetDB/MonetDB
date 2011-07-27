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


#include "monetdb_config.h"
#include <sql_mem.h>
#include <gdk.h>
#include "sql_string.h"

/* 
 * some string functions.
 */

/* implace cast to lower case string */
char *
mkLower(char *s)
{
	char *r = s;

	while (*s) {
		*s = (char) tolower(*s);
		s++;
	}
	return r;
}

static char *
mkUpper(char *s)
{
	char *r = s;

	while (*s) {
		*s = (char) toupper(*s);
		s++;
	}
	return r;
}

char *
toLower(const char *s)
{
	char *r = _strdup(s);

	return mkLower(r);
}

char *
toUpper(const char *s)
{
	char *r = _strdup(s);

	return mkUpper(r);
}

/* concat s1,s2 into a new result string */
char *
strconcat(const char *s1, const char *s2)
{
	size_t i, j, l1 = strlen(s1);
	size_t l2 = strlen(s2) + 1;
	char *new_s = NEW_ARRAY(char, l1 + l2);

	for (i = 0; i < l1; i++) {
		new_s[i] = s1[i];
	}
	for (j = 0; j < l2; j++, i++) {
		new_s[i] = s2[j];
	}
	return new_s;
}

char *
strip_extra_zeros(char *s)
{
	char *res = s;

	for (; *s && isspace((int) (unsigned char) *s); s++)
		;
	for (; *s && *s == '0'; s++)
		;
	res = s;
	/* find end, and strip extra 0's */
	for (; *s; s++) ;	
	s--;
	for (; *s && *s == '0'; s--)
		;
	s++;
	*s = 0;
	return res;
}

char *
sql2str(char *s)
{
	int escaped = 0;
	char *cur, *p = s;

	for (cur = s; *cur; cur++) {
		if (escaped) {
			if (*cur == 'n') {
				*p++ = '\n';
			} else if (*cur == 't') {
				*p++ = '\t';
			} else if ((cur[0] >= '0' && cur[0] <= '7') && (cur[1] >= '0' && cur[1] <= '7') && (cur[2] >= '0' && cur[2] <= '7')) {
				*p++ = (cur[2] & 7) | ((cur[1] & 7) << 3) | ((cur[0] & 7) << 6);
				cur += 2;
			} else {
				*p++ = *cur;
			}
			escaped = FALSE;
		} else if (*cur == '\\') {
			escaped = TRUE;
		} else {
			*p++ = *cur;
		}
	}
	*p = '\0';
	return s;
}

char *
sql_strdup(char *s)
{
	size_t l = strlen(s);
	char *r = NEW_ARRAY(char, l);

	memcpy(r, s + 1, l - 2);
	r[l - 2] = 0;
	return r;
}

char *
sql_escape_str(char *s)
{
	size_t l = strlen(s);
	char *res, *r = NEW_ARRAY(char, (l * 2) + 1);

	res = r;
	while (*s) {
		if (*s == '\'' || *s == '\\') {
			*r++ = '\\';
		}
		*r++ = *s++;
	}
	*r = '\0';
	return res;
}

char *
sql_escape_ident(char *s)
{
	size_t l = strlen(s);
	char *res, *r = NEW_ARRAY(char, (l * 2) + 1);

	res = r;
	while (*s) {
		if (*s == '"' || *s == '\\') {
			*r++ = '\\';
		}
		*r++ = *s++;
	}
	*r = '\0';
	return res;
}

char *sql_message( const char *format, ... )
{
	char buf[BUFSIZ];
	va_list	ap;

	va_start (ap,format);
	(void) vsnprintf( buf, BUFSIZ, format, ap); 
	va_end (ap);
	return _strdup(buf);
}

char *sa_message( sql_allocator *sa, const char *format, ... )
{
	char buf[BUFSIZ];
	va_list	ap;

	va_start (ap,format);
	(void) vsnprintf( buf, BUFSIZ, format, ap); 
	va_end (ap);
	return sa_strdup(sa, buf);
}

