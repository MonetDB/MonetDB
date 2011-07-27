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
	int h = 1;

	while (*k) {
		h <<= 5;
		h += (*k - 'a');
		k++;
	}
	*l = (int) (k - s);
	h <<= 4;
	h += *l;
	return (h < 0) ? -h : h;
}

void
keywords_insert(char *k, int token)
{
	keyword *kw = NEW(keyword);
	int len = 0;
	int bucket = keyword_key(k = toLower(k), &len) & HASH_MASK;

	kw->keyword = k;
	kw->len = len;
	kw->token = token;
	kw->next = keywords[bucket];
	keywords[bucket] = kw;
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
