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

#ifndef SQL_KEYWORD_H
#define SQL_KEYWORD_H

/* we need to define these here as the parser header file is generated to late.
 * The numbers get remapped in the scanner. 
 */
#define KW_ALIAS 4000
#define KW_TYPE  4001

typedef struct keyword {
	char *keyword;
	int len;
	int token;
	struct keyword *next;
} keyword;

extern void keywords_insert(char *k, int token);
extern keyword *find_keyword(char *text);
extern int keyword_exists(char *text);

extern void keyword_init(void);
extern void keyword_exit(void);

#endif /* SQL_KEYWORD_H */
