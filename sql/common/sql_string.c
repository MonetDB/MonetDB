/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "gdk.h"
#include "sql_string.h"
#include "mal_exception.h"

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
	char *r = _STRDUP(s);

	return r ? mkLower(r) : NULL;
}

char *
toUpper(const char *s)
{
	char *r = _STRDUP(s);

	return r ? mkUpper(r) : NULL;
}

/* concat s1,s2 into a new result string */
char *
strconcat(const char *s1, const char *s2)
{
	size_t i, j, l1 = strlen(s1);
	size_t l2 = strlen(s2) + 1;
	char *new_s = NEW_ARRAY(char, l1 + l2);

	if (new_s) {
		for (i = 0; i < l1; i++) {
			new_s[i] = s1[i];
		}
		for (j = 0; j < l2; j++, i++) {
			new_s[i] = s2[j];
		}
	}
	return new_s;
}

char *
strip_extra_zeros(char *s)
{
	char *res = s;

	for (; *s && isspace((unsigned char) *s); s++)
		;
	res = s;
	/* find end, and strip extra 0's */
	for (; *s; s++) ;
	s--;
	for (; *s && *s == '0' && s[-1] == '0'; s--)
		;
	s++;
	*s = 0;
	return res;
}

char *
sql_strdup(char *s)
{
	size_t l = strlen(s);
	char *r = NEW_ARRAY(char, l);

	if (r) {
		memcpy(r, s + 1, l - 2);
		r[l - 2] = 0;
	}
	return r;
}

char *
sql_escape_str(sql_allocator *sa, char *s)
{
	size_t l = strlen(s);
	char *res, *r = SA_NEW_ARRAY(sa, char, (l * 2) + 1);

	res = r;
	if (res) {
		while (*s) {
			if (*s == '\'' || *s == '\\') {
				*r++ = '\\';
			}
			*r++ = *s++;
		}
		*r = '\0';
	}
	return res;
}

const char *
sql_escape_ident(sql_allocator *sa, const char *s)
{
	size_t l = strlen(s);
	char *res, *r = SA_NEW_ARRAY(sa, char, (l * 2) + 1);

	res = r;
	if (res) {
		while (*s) {
			if (*s == '"' || *s == '\\') {
				*r++ = '\\';
			}
			*r++ = *s++;
		}
		*r = '\0';
	}
	return res;
}

char *sql_message( const char *format, ... )
{
	char buf[BUFSIZ];
	va_list	ap;

	va_start (ap,format);
	(void) vsnprintf( buf, BUFSIZ, format, ap);
	va_end (ap);
	return GDKstrdup(buf);
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

