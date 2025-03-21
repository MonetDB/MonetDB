/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
extern char *sql_strdup(char *s);
extern const char *sql_escape_ident(allocator *sa, const char *s);
extern char *sql_message(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
sql_export char *sa_message(allocator *sa, _In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)));

#endif /*_SQL_STRING_H_*/
