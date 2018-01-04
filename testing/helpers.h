/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef HELPERS_H
#define HELPERS_H

#include <stdio.h>

#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))

__declspec(noreturn) void ErrXit(char *text1, char *text2, int num)
	__attribute__((__noreturn__));
FILE *Rfopen(char *name);
FILE *Wfopen(char *name);
FILE *Afopen(char *name);
#define isalpha_(c) (isascii(c) && (isalpha(c) || c == '_'))
#define isspace_(c) (isascii(c) && isspace(c))
#define isdigit_(c) (isascii(c) && isdigit(c))
char *filename(char *path);
char *tmpdir(void);

#endif /* HELPERS_H */
