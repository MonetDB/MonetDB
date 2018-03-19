/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_STRING_H_
#define _SQL_STRING_H_

#define D__SQL	16

#define _(String) (String)
#define N_(String) (String)

extern char *mkLower(char *v);
extern char *toLower(const char *v);
extern char *toUpper(const char *v);
extern char *strconcat(const char *s1, const char *s2);
extern char *strip_extra_zeros(char *v);
extern char *sql2str(char *s);
extern char *sql_strdup(char *s);
extern char *sql_escape_str(char *s);
extern const char *sql_escape_ident(const char *s);
extern char *sql_message(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
extern char *sa_message(sql_allocator *sa, _In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)));

#endif /*_SQL_STRING_H_*/

