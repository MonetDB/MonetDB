/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _SEEN_UTILS_H
#define _SEEN_UTILS_H 1

#include <stdio.h>  /* FILE* */
#include <sys/types.h>   /* time_t */

enum valtype {
	INVALID = 0,
	INT,
	BOOLEAN,
	STR,
	MURI,
	OTHER
};

typedef struct _confkeyval {
	char *key;
	char *val;
	int ival;
	enum valtype type;
} confkeyval;

void readConfFile(confkeyval *list, FILE *cnf);
void readConfFileFull(confkeyval *list, FILE *cnf);
void freeConfFile(confkeyval *list);
int defaultProperty(const char *key);
confkeyval *findConfKey(confkeyval *list, const char *key);
char *getConfVal(confkeyval *list, const char *key);
int getConfNum(confkeyval *list, const char *key);
char *setConfVal(confkeyval *ckv, const char *val);
char *setConfValForKey(confkeyval *list, const char *key, const char *val);
void secondsToString(char *buf, time_t t, int longness);
void abbreviateString(char *ret, const char *in, size_t width);
void generateSalt(char *buf, unsigned int len);
char *generatePassphraseFile(const char *path);
void sleep_ms(size_t ms);

#endif

/* vim:set ts=4 sw=4 noexpandtab: */
