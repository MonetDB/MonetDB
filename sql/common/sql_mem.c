/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"

/* Stefan: 
 * "Fake-include" to make msc.py create the proper dependencies;
 * otherwise, query.h doesn't get extracted from query.mx on Windows.
 * TODO: fix msc.py instead...
#include "query.h"
*/

#include <sql_mem.h>

sql_ref *
sql_ref_init(sql_ref *r)
{
	r->refcnt = 1;
	return r;
}

int
sql_ref_inc(sql_ref *r)
{
	assert(r->refcnt > 0);
	return (++r->refcnt);
}

int
sql_ref_dec(sql_ref *r)
{
	assert(r->refcnt > 0);
	return (--r->refcnt);
}


#define SA_BLOCK (64*1024)

sql_allocator *sa_create(void)
{
	sql_allocator *sa = MNEW(sql_allocator);
	if (sa == NULL) {
		return NULL;
	}
	sa->size = 64;
	sa->nr = 1;
	sa->blks = NEW_ARRAY(char*,sa->size);
	if (sa->blks == NULL) {
		_DELETE(sa);
		return NULL;
	}
	sa->blks[0] = NEW_ARRAY(char,SA_BLOCK);
	sa->usedmem = SA_BLOCK;
	if (sa->blks[0] == NULL) {
		_DELETE(sa->blks);
		_DELETE(sa);
		return NULL;
	}
	sa->used = 0;
	return sa;
}

sql_allocator *sa_reset( sql_allocator *sa )
{
	size_t i ;

	for (i = 1; i<sa->nr; i++) {
		_DELETE(sa->blks[i]);
	}
	sa->nr = 1;
	sa->used = 0;
	sa->usedmem = SA_BLOCK;
	return sa;
}

char *sa_realloc( sql_allocator *sa, void *p, size_t sz, size_t oldsz )
{
	char *r = sa_alloc(sa, sz);

	memcpy(r, (char*)p, oldsz);
	return r;
}

#define round16(sz) ((sz+15)&~15)
char *sa_alloc( sql_allocator *sa, size_t sz )
{
	char *r;
	sz = round16(sz);
	if (sz > SA_BLOCK) {
		char *t;
		// FIXME unchecked_malloc GDKmalloc can return NULL
		r = GDKmalloc(sz);
		if (sa->nr >= sa->size) {
			sa->size *=2;
			sa->blks = RENEW_ARRAY(char*,sa->blks,sa->size);
		}
		t = sa->blks[sa->nr-1];
		sa->blks[sa->nr-1] = r;
		sa->blks[sa->nr] = t;
		sa->nr ++;
		sa->usedmem += sz;
		return r;
	}
	if (sz > (SA_BLOCK-sa->used)) {
		// FIXME unchecked_malloc GDKmalloc can return NULL
		r = GDKmalloc(SA_BLOCK);
		if (sa->nr >= sa->size) {
			sa->size *=2;
			sa->blks = RENEW_ARRAY(char*,sa->blks,sa->size);
		}
		sa->blks[sa->nr] = r;
		sa->nr ++;
		sa->used = sz;
		sa->usedmem += SA_BLOCK;
		return r;
	}
	r = sa->blks[sa->nr-1] + sa->used;
	sa->used += sz;
	return r;
}

char *sa_zalloc( sql_allocator *sa, size_t sz )
{
	char *r = sa_alloc(sa, sz);

	if (r)
		memset(r, 0, sz);
	return r;
}	

void sa_destroy( sql_allocator *sa ) 
{
	size_t i ;

	for (i = 0; i<sa->nr; i++) {
		GDKfree(sa->blks[i]);
	}
	GDKfree(sa->blks);
	GDKfree(sa);
}

char *sa_strndup( sql_allocator *sa, const char *s, size_t l) 
{ 
	char *r = sa_alloc(sa, l+1); 

	if (r) {
		memcpy(r, s, l); 
		r[l] = 0; 
	}
	return r; 
}

char *sa_strdup( sql_allocator *sa, const char *s ) 
{ 
	return sa_strndup( sa, s, strlen(s));
}

char *sa_strconcat( sql_allocator *sa, const char *s1, const char *s2 )
{
	size_t l1 = strlen(s1);
	size_t l2 = strlen(s2);
	char *r = sa_alloc(sa, l1+l2+1);

	if (l1) 
		memcpy(r, s1, l1);
	if (l2)
		memcpy(r+l1, s2, l2);
	r[l1+l2] = 0;
	return r;
}

size_t sa_size( sql_allocator *sa )
{
	return sa->usedmem;
}

void
c_delete( const void *p ) 
{
	void *xp = (void*)p;

	GDKfree(xp);
}
