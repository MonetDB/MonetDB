/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_string.h"
#include "sql_keyword.h"

#define HASH_SIZE 32768
#define HASH_MASK (HASH_SIZE-1)

static int keywords_init_done = 0;
static keyword *keywords[HASH_SIZE];

static int
keyword_key(char *k, int *l)
{
	char *s = k;
	unsigned int h = 1;

	while (*k) {
		h <<= 5;
		h += (*k - 'a');
		k++;
	}
	*l = (int) (k - s);
	h <<= 4;
	h += *l;
	return (int) ((h & 0x80000000) ? ~h + 1 : h);
}

int
keywords_insert(char *k, int token)
{
	keyword *kw = MNEW(keyword);
	if(kw) {
		int len = 0;
		int bucket = keyword_key(k = toLower(k), &len) & HASH_MASK;
#ifndef NDEBUG
		/* no duplicate keywords */
		keyword *kw2;
		for (kw2 = keywords[bucket]; kw2; kw2 = kw2->next)
			assert(strcmp(kw2->keyword, k) != 0);
#endif

		kw->keyword = k;
		kw->len = len;
		kw->token = token;
		kw->next = keywords[bucket];
		keywords[bucket] = kw;
		return 0;
	} else {
		return -1;
	}
}

keyword *
find_keyword(char *text)
{
	int len = 0;
	int bucket = keyword_key(mkLower(text), &len) & HASH_MASK;
	keyword *k = keywords[bucket];

	while (k) {
		if (len == k->len && strcmp(k->keyword, text) == 0)
			return k;

		k = k->next;
	}
	return NULL;
}

int
keyword_exists(char *text)
{
	if (find_keyword(text)) {
		return 1;
	}
	return 0;
}

void
keyword_init(void)
{
	int i;

	if (keywords_init_done)
		return;
	keywords_init_done = 1;

	for (i = 0; i < HASH_SIZE; i++)
		keywords[i] = NULL;
}

void
keyword_exit(void)
{
	int i;

	if (keywords_init_done == 0)
		return;
	keywords_init_done = 0;

	for (i = 0; i < HASH_SIZE; i++) {
		keyword *k = keywords[i];

		while (k) {
			keyword *l = k;

			k = k->next;
			_DELETE(l->keyword);

			_DELETE(l);
		}
	}
}
