/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

extern int keywords_insert(char *k, int token);
extern keyword *find_keyword(char *text);
extern int keyword_exists(char *text);

extern void keyword_init(void);
extern void keyword_exit(void);

#endif /* SQL_KEYWORD_H */
