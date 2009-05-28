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
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _SEEN_UTILS_H
#define _SEEN_UTILS_H 1

#include "sql_config.h"
#include <stdio.h>  /* FILE* */
#include <sys/types.h>   /* time_t */

typedef struct _confkeyval {
	char *key;
	char *val;
} confkeyval;

char *replacePrefix(char *s, char *prefix);
void readConfFile(confkeyval *list, FILE *cnf);
void secondsToString(char *buf, time_t t, int longness);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
