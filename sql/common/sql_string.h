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
extern char *sql_escape_ident(char *s);
extern char *sql_message(_In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 1, 2)));
extern char *sa_message(sql_allocator *sa, _In_z_ _Printf_format_string_ const char *format, ...)
	__attribute__((__format__(__printf__, 2, 3)));

#endif /*_SQL_STRING_H_*/

