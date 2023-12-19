/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "pp_mem.h"

void
ma_destroy(mallocator* ma)
{
	for (size_t i = 0; i<ma->nr; i++) {
		GDKfree(ma->blks[i]);
	}
	GDKfree(ma->blks);
	GDKfree(ma);
}

mallocator *
ma_create(void)
{
	mallocator *ma = (mallocator*)GDKmalloc(sizeof(mallocator));
	if (ma == NULL)
		return NULL;
	ma->size = 256;
	ma->nr = 1;
	ma->blks = (char**)GDKmalloc(sizeof(char*) * ma->size);
	if (ma->blks == NULL) {
		GDKfree(ma);
		return NULL;
	}
	ma->blks[0] = (char*)GDKmalloc(SA_BLOCK);
	ma->usedmem = SA_BLOCK;
	if (ma->blks[0] == NULL) {
		GDKfree(ma->blks);
		GDKfree(ma);
		return NULL;
	}
	ma->used = 0;
	return ma;
}

void *
ma_alloc( mallocator *ma, size_t sz )
{
	char *r;
	if (sz > (SA_BLOCK-ma->used)) {
		r = GDKmalloc(sz > SA_BLOCK ? sz : SA_BLOCK);
		if (r == NULL)
			return NULL;
		if (ma->nr >= ma->size) {
			char **tmp;
			ma->size *=2;
			tmp = (char**)GDKrealloc(ma->blks, sizeof(char*) * ma->size);
			if (tmp == NULL) {
				ma->size /= 2; /* undo */
				GDKfree(r);
				return NULL;
			}
			ma->blks = tmp;
		}
		if (sz > SA_BLOCK) {
			ma->blks[ma->nr] = ma->blks[ma->nr-1];
			ma->blks[ma->nr-1] = r;
			ma->nr ++;
			ma->usedmem += sz;
		} else {
			ma->blks[ma->nr] = r;
			ma->nr ++;
			ma->used = sz;
			ma->usedmem += SA_BLOCK;
		}
	} else {
		r = ma->blks[ma->nr-1] + ma->used;
		ma->used += sz;
	}
	return r;
}

char *
ma_strdup( mallocator *ma, const char *s )
{
	int l = strlen(s);
	char *r = ma_alloc(ma, l+1);

	if (r)
		memcpy(r, s, l+1);
	return r;
}

char *
ma_copy( mallocator *ma, const void *s, int l )
{
	char *r = ma_alloc(ma, l);

	if (r)
		memcpy(r, s, l);
	return r;
}
