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
#include "bat_table.h"
#include "bat_utils.h"

static BAT *
delta_full_bat_( sql_column *c, sql_delta *bat, int temp, BAT *d, BAT *s)
{
	/* return full normalized column bat

		if (s) {
			b := b.semijoin(s);
			i := i.semijoin(s);
			u := u.semijoin(s);
		}
		b := b.kunion(i);
		b := b.kdiff(u);
		b := b.kunion(u);
		b := b.kdiff(reverse(d));
	*/
	BAT *r, *b, *u, *i = temp_descriptor(bat->ibid);
	r = i; 
	if (temp) {
		if (s) {
			r = BATsemijoin(i,s);
			bat_destroy(i);
		}
		return r;
	}
	b = temp_descriptor(bat->bid);
	u = temp_descriptor(bat->ubid);
	if (s) {
		BAT *t;

		t = BATsemijoin(b,s); bat_destroy(b); b = t;
		t = BATsemijoin(i,s); bat_destroy(i); i = t;
		t = BATsemijoin(u,s); bat_destroy(u); u = t;
	}
	assert(b->ttype == i->ttype);
	if (BATcount(i)) {
		r = BATkunion(b,i); bat_destroy(b); b = r;
	}
	bat_destroy(i); 
	if (BATcount(u)) {
		r = BATkdiff(b,u); bat_destroy(b); b = r;
		assert(b->ttype == u->ttype);
		r = BATkunion(b,u); bat_destroy(b); b = r;
	}
	bat_destroy(u); 
	if (d && BATcount(d)) {
		r = BATkdiff(b,BATmirror(d)); bat_destroy(b); b = r;
	}
	if (!bat->cached && !c->base.wtime && !s) 
		bat->cached = temp_descriptor(b->batCacheid);
	return b;
}

BAT *
delta_full_bat( sql_column *c, sql_delta *bat, int temp, BAT *d, BAT *s)
{
	if (bat->cached && s) 
		return BATsemijoin(bat->cached, s);
	if (bat->cached) 
		return temp_descriptor(bat->cached->batCacheid);
	return delta_full_bat_( c, bat, temp, d, s);
}

static BAT *
full_column(sql_column *c, BAT *d, BAT *s )
{
	return delta_full_bat(c, c->data, isTemp(c), d, s);
}

static oid
column_find_row(sql_trans *tr, sql_column *c, void *value, ...)
{
	va_list va;
	BUN q;
	BAT *b = NULL, *s = NULL, *r = NULL, *d = NULL;
	oid rid = oid_nil;
	sql_column *nc;
	void *nv;
	sql_dbat *bat = c->t->data;

	if (bat->dbid) 
		d = store_funcs.bind_del(tr, c->t, RDONLY);
	va_start(va, value);
	while ((nc = va_arg(va, sql_column *)) != NULL) {
		nv = va_arg(va, void *);

		b = full_column(c, d, s);
		if (s)
			bat_destroy(s);
		s = BATselect(b, value, value);
		bat_destroy(b);
		c = nc;
		value = nv;
	}
	va_end(va);
	b = full_column(c, d, s);
	if (s)
		bat_destroy(s);
	if (d)
		bat_destroy(d);

	r = BATmirror(b);
	q = BUNfnd(r, value);
	if (q != BUN_NONE) {
		BATiter ri = bat_iterator(r);
		rid = *(oid *) BUNtail(ri, q);
	}
	bat_destroy(b);
	return rid;
}

static void *
column_find_value(sql_trans *tr, sql_column *c, oid rid)
{
	BUN q;
	BAT *b, *d = NULL;
	void *res = NULL;
	sql_dbat *bat = c->t->data;

	if (bat->dbid) 
		d = store_funcs.bind_del(tr, c->t, RDONLY);
	b = full_column(c, d, NULL);
	if (d)
		bat_destroy(d);

	q = BUNfnd(b, (ptr) &rid);
	if (q != BUN_NONE) {
		BATiter bi = bat_iterator(b);
		void *r;
		int sz;

		res = BUNtail(bi, q);
		sz = ATOMlen(b->ttype, res);
		r = GDKzalloc(sz);
		memcpy(r,res,sz);
		res = r;
	}
	bat_destroy(b);
	return res;
}

static int
column_update_value(sql_trans *tr, sql_column *c, oid rid, void *value)
{
	assert(rid != oid_nil);

	store_funcs.update_col(tr, c, value, c->type.type->localtype, rid);
	return 0;
}

static int
table_insert(sql_trans *tr, sql_table *t, ...)
{
	va_list va;
	node *n = cs_first_node(&t->columns);
	void *val = NULL;
	int cnt = 0;

	va_start(va, t);
	for (val = va_arg(va, void *); n && val; n = n->next, val = va_arg(va, void *))
	{
		sql_column *c = n->data;
		store_funcs.append_col(tr, c, val, c->type.type->localtype);
		cnt++;
	}
	va_end(va);
	if (n) {
		fprintf(stderr, "called table_insert(%s) with wrong number of args (%d,%d)\n", t->base.name, list_length(t->columns.set), cnt);
		assert(0);
		return -1;
	}
	return 0;
}

static int
table_delete(sql_trans *tr, sql_table *t, oid rid)
{
	assert(rid != oid_nil);

	store_funcs.delete_tab(tr, t, &rid, TYPE_oid);
	return 0;
}


#if 0
static int
table_dump(sql_trans *tr, sql_table *t)
{
	node *n = cs_first_node(&t->columns);
	int i, l = cs_size(&t->columns);
	BAT **b = (BAT**)GDKzalloc(sizeof(BAT*) * l);
	
	(void)tr;
	for (i = 0; n; n = n->next, i++) {
		sql_column *c = n->data;
		sql_delta *bat = c->data;

		b[i] = temp_descriptor(bat->bid);
	}
	BATmultiprintf(GDKstdout, l +1, b, TRUE, 0, 1);
	for (i = 0; i < l; i++)
		bat_destroy(b[i]);
	GDKfree(b);
	return 0;
}

static int
table_check(sql_trans *tr, sql_table *t)
{
	node *n = cs_first_node(&t->columns);
	BUN cnt = BUN_NONE;

	(void)tr;
	for (; n; n = n->next) {
		sql_column *c = n->data;
		sql_delta *bat = c->data;
		BAT *b = temp_descriptor(bat->bid);

		if (cnt == BUN_NONE) {
			cnt = BATcount(b);
		} else if (cnt != BATcount(b)) {
			assert(0);
			return (int)(cnt - BATcount(b));
		}
		bat_destroy(b);
	}
	return 0;
}
#endif

/* returns table rids, for the given select ranges */
static rids *
rids_select( sql_trans *tr, sql_column *key, void *key_value_low, void *key_value_high, ...)
{
	va_list va;
	BAT *b = NULL, *s = NULL, *d = NULL;
	sql_column *nc;
	void *nvl, *nvh;
	rids *rs = ZNEW(rids);
	sql_dbat *bat = key->t->data;

	/* special case, key_value_low and high NULL, ie return all */
	if (bat->dbid) 
		d = store_funcs.bind_del(tr, key->t, RDONLY);
	if (key_value_low || key_value_high) {
		va_start(va, key_value_high);
		while ((nc = va_arg(va, sql_column *)) != NULL) {
			nvl = va_arg(va, void *);
			nvh = va_arg(va, void *);
	
			b = full_column(key, d, s);
			if (s)
				bat_destroy(s);
			if (!key_value_low)
				key_value_low = ATOMnilptr(b->ttype);
			if (!key_value_high)
				key_value_high = ATOMnilptr(b->ttype);
			s = BATselect(b, key_value_low, key_value_high);
			bat_destroy(b);
			key = nc;
			key_value_low = nvl;
			key_value_high = nvh;
		}
		va_end(va);
	}
	b = full_column(key, d, s);
	if (s)
		bat_destroy(s);
	if (d)
		bat_destroy(d);
	if (key_value_low || key_value_high) {
		if (!key_value_low)
			key_value_low = ATOMnilptr(b->ttype);
		if (!key_value_high)
			key_value_high = ATOMnilptr(b->ttype);
		rs->data = BATselect(b, key_value_low, key_value_high);
		bat_destroy(b);
	} else {
		rs->data = b;
	}
	rs->cur = 0;
	return rs;
}

/* order rids by orderby_column values */
static rids *
rids_orderby(sql_trans *tr, rids *r, sql_column *orderby_col)
{
	BAT *b, *d = NULL;
	sql_dbat *bat = orderby_col->t->data;

	if (bat->dbid) 
		d = store_funcs.bind_del(tr, orderby_col->t, RDONLY);
	b = full_column(orderby_col, d, r->data);
	if (d)
		bat_destroy(d);
	bat_destroy(r->data);
	b = BATmirror(b);
	r->data = BATmirror(BATsort(b));
	bat_destroy(b);
	return r;
}


/* return table rids from result of rids_select, return (oid_nil) when done */
static oid 
rids_next(rids *r)
{
	if (r->cur < BATcount((BAT  *) r->data)) {
		BATiter bi = bat_iterator((BAT  *) r->data);
		return *(oid*)BUNhead(bi, r->cur++);
	}
	return oid_nil;
}

static rids *
rids_join(sql_trans *tr, rids *l, sql_column *lc, rids *r, sql_column *rc)
{
	BAT *lcb, *rcb, *d = NULL;
	sql_dbat *lbat, *rbat;
	
	lbat = lc->t->data;
	if (lbat->dbid) 
		d = store_funcs.bind_del(tr, lc->t, RDONLY);
	lcb = full_column(lc, d, r->data);
	if (d)
		bat_destroy(d);
	rbat = rc->t->data;
	if (rbat->dbid) 
		d = store_funcs.bind_del(tr, rc->t, RDONLY);
	rcb = full_column(rc, d, r->data);
	if (d)
		bat_destroy(d);
	bat_destroy(l->data);
	l->data = BATjoin(lcb, BATmirror(rcb), BATcount(lcb));
	bat_destroy(lcb);
	bat_destroy(rcb);
	return l;
}

/* clean up the resources taken by the result of rids_select */
static void 
rids_destroy(rids *r)
{
	bat_destroy(r->data);
	_DELETE(r);
}

int 
bat_table_init( table_functions *tf )
{
	tf->column_find_row = column_find_row;
	tf->column_find_value = column_find_value;
	tf->column_update_value = column_update_value;
	tf->table_insert = table_insert;
	tf->table_delete = table_delete;
	
	tf->rids_select = rids_select;
	tf->rids_orderby = rids_orderby;
	tf->rids_join = rids_join;
	tf->rids_next = rids_next;
	tf->rids_destroy = rids_destroy;
	return LOG_OK;
}
