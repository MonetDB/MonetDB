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

#ifndef _SEEN_UTILS_H
#define _SEEN_UTILS_H 1

#include <time.h>   /* time_t */

#define PROPLENGTH 64 /* Max number of properties */

enum valtype {
	INVALID = 0,
	INT,
	SINT,
	BOOLEAN,
	STR,
	MURI,
	LADDR,
	LOGLEVEL,
	MODS,						/* list of mserver5 modules */
	OTHER
};

typedef enum loglevel_t {
	ERROR = 1,
	WARNING,
	INFORMATION,
	DEBUG
} loglevel;

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
char* deletedir(const char *dir);


struct snapshot {
	char *dbname;
	time_t time;
	off_t size;
	char *name;
	char *path;
};

struct snapshot *push_snapshot(struct snapshot **snapshots, int *nsnapshots);
void copy_snapshot(struct snapshot *dest, struct snapshot *src);
void free_snapshots(struct snapshot *snapshots, int nsnapshots);


#endif
