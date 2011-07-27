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

#ifndef HELPERS_H
#define HELPERS_H

#include <stdio.h>

#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))

void ErrXit(char *text1, char *text2, int num);
FILE *Rfopen(char *name);
FILE *Wfopen(char *name);
FILE *Afopen(char *name);
#define isalpha_(c) (isascii(c) && (isalpha(c) || c == '_'))
#define isspace_(c) (isascii(c) && isspace(c))
#define isdigit_(c) (isascii(c) && isdigit(c))
char *filename(char *path);
char *tmpdir(void);

#endif /* HELPERS_H */
