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
	sql_allocator *sa = NEW(sql_allocator);
	
	sa->size = 64;
	sa->nr = 1;
	sa->blks = NEW_ARRAY(char*,sa->size);
	sa->blks[0] = NEW_ARRAY(char,SA_BLOCK);
	if (!sa->blks[0]) {
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
		r = GDKmalloc(sz);
		if (sa->nr >= sa->size) {
			sa->size *=2;
			sa->blks = RENEW_ARRAY(char*,sa->blks,sa->size);
		}
		t = sa->blks[sa->nr-1];
		sa->blks[sa->nr-1] = r;
		sa->blks[sa->nr] = t;
		sa->nr ++;
		return r;
	}
	if (sz > (SA_BLOCK-sa->used)) {
		r = GDKmalloc(SA_BLOCK);
		if (sa->nr >= sa->size) {
			sa->size *=2;
			sa->blks = RENEW_ARRAY(char*,sa->blks,sa->size);
		}
		sa->blks[sa->nr] = r;
		sa->nr ++;
		sa->used = sz;
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
