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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "bat_table.h"
#include "bat_utils.h"
#include "bat_storage.h"

static BAT *
_delta_cands(sql_trans *tr, sql_table *t)
{
	sql_column *c = t->columns.set->h->data;
	/* create void,void bat with length and oid's set */
	BAT *tids = BATnew(TYPE_void, TYPE_void, 0, TRANSIENT);
	size_t nr = store_funcs.count_col(tr, c, 1);

	tids->H->seq = 0;
	tids->T->seq = 0;
	BATsetcount(tids, (BUN) nr);
	tids->H->revsorted = 0;
	tids->T->revsorted = 0;

	tids->T->key = 1;
	tids->T->dense = 1;
	tids->H->key = 1;
	tids->H->dense = 1;

	if (store_funcs.count_del(tr, t)) {
		BAT *d = store_funcs.bind_del(tr, t, RD_INS);
		BAT *diff = BATkdiff(tids, BATmirror(d));

		bat_destroy(tids);
		tids = BATmirror(BATmark(diff, 0));
		bat_destroy(diff);
		bat_destroy(d);
	}
	return tids;
}

static BAT *
delta_cands(sql_trans *tr, sql_table *t)
{
	sql_dbat *d;
	BAT *tids;

	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, tr->stime);
	}
	d = t->data;
	if (d->cached && !tr->parent) 
		return temp_descriptor(d->cached->batCacheid);
	tids = _delta_cands(tr, t);
	if (!d->cached && !tr->parent) /* only cache during catalog loading */
		d->cached = temp_descriptor(tids->batCacheid);
	return tids;
}

static BAT *
delta_full_bat_( sql_trans *tr, sql_column *c, sql_delta *bat, int temp)
{
	/* return full normalized column bat
	 * 	b := b.copy()
		b := b.append(i);
		b := b.replace(u);
	*/
	BAT *r, *b, *u, *i = temp_descriptor(bat->ibid);
	int needcopy = 1;

	r = i; 
	if (temp) 
		return r;
	b = temp_descriptor(bat->bid);
	u = temp_descriptor(bat->ubid);
	if (!b) {
		b = i;
	} else {
		if (BATcount(i)) {
			r = BATcopy(b, b->htype, b->ttype, 1, TRANSIENT); 
			bat_destroy(b); 
			b = r;
			BATappend(b, i, TRUE); 
			needcopy = 0;
		}
		bat_destroy(i); 
	}
	if (BATcount(u)) {
		if (needcopy) {
			r = BATcopy(b, b->htype, b->ttype, 1, TRANSIENT); 
			bat_destroy(b); 
			b = r;
		}
		BATreplace(b, u, TRUE);
	}
	bat_destroy(u); 
	(void)c;
	if (!bat->cached && !tr->parent) 
		bat->cached = temp_descriptor(b->batCacheid);
	return b;
}

static BAT *
delta_full_bat( sql_trans *tr, sql_column *c, sql_delta *bat, int temp)
{
	if (bat->cached && !tr->parent) 
		return temp_descriptor(bat->cached->batCacheid);
	return delta_full_bat_( tr, c, bat, temp);
}

static BAT *
full_column(sql_trans *tr, sql_column *c)
{
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->stime);
	}
	return delta_full_bat(tr, c, c->data, isTemp(c));
}

static oid column_find_row(sql_trans *tr, sql_column *c, const void *value, ...);
static oid
column_find_row(sql_trans *tr, sql_column *c, const void *value, ...)
{
	va_list va;
	BAT *b = NULL, *s = NULL, *r = NULL;
	oid rid = oid_nil;

	s = delta_cands(tr, c->t);
	va_start(va, value);
	b = full_column(tr, c);
	r = BATsubselect(b, s, value, NULL, 1, 0, 0);
	bat_destroy(s);
	s = r;
	bat_destroy(b);
	while ((c = va_arg(va, sql_column *)) != NULL) {
		value = va_arg(va, void *);

		b = full_column(tr, c);
		r = BATsubselect(b, s, value, NULL, 1, 0, 0);
		bat_destroy(s);
		s = r;
		bat_destroy(b);
	}
	va_end(va);
	if (BATcount(s) == 1) {
		BATiter ri = bat_iterator(s);
		rid = *(oid *) BUNtail(ri, 0);
	}
	bat_destroy(s);
	return rid;
}

static void *
column_find_value(sql_trans *tr, sql_column *c, oid rid)
{
	BUN q;
	BAT *b;
	void *res = NULL;

	b = full_column(tr, c);
	q = BUNfnd(BATmirror(b), (ptr) &rid);
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

	store_funcs.update_col(tr, c, &rid, value, c->type.type->localtype);
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
	BAT *b = NULL, *r = NULL, *s = NULL;
	rids *rs = ZNEW(rids);
	void *kvl = key_value_low, *kvh = key_value_high;
	int hi = 0;

	s = delta_cands(tr, key->t);
	b = full_column(tr, key);
	if (!kvl)
		kvl = ATOMnilptr(b->ttype);
	if (!kvh && key_value_low != ATOMnilptr(b->ttype))
		kvh = ATOMnilptr(b->ttype);
	hi = (kvl == kvh);
	r = BATsubselect(b, s, kvl, kvh, 1, hi, 0);
	bat_destroy(s);
	s = r;
	bat_destroy(b);
	if (key_value_low || key_value_high) {
		va_start(va, key_value_high);
		while ((key = va_arg(va, sql_column *)) != NULL) {
			kvl = va_arg(va, void *);
			kvh = va_arg(va, void *);
	
			b = full_column(tr, key);
			if (!kvl)
				kvl = ATOMnilptr(b->ttype);
			if (!kvh)
				kvh = ATOMnilptr(b->ttype);
			hi = (kvl == kvh);
			r = BATsubselect(b, s, kvl, kvh, 1, hi, 0);
			bat_destroy(s);
			s = r;
			bat_destroy(b);
		}
		va_end(va);
	}
	rs->data = s;
	rs->cur = 0;
	return rs;
}

/* order rids by orderby_column values */
static rids *
rids_orderby(sql_trans *tr, rids *r, sql_column *orderby_col)
{
	BAT *b, *s, *o;

	b = full_column(tr, orderby_col);
	s = BATproject(r->data, b);
	bat_destroy(b);
	BATsubsort(NULL, &o, NULL, s, NULL, NULL, 0, 0);
	bat_destroy(s);
	s = BATproject(o, r->data);
	bat_destroy(r->data);
	bat_destroy(o);
	r->data = s;
	return r;
}


/* return table rids from result of rids_select, return (oid_nil) when done */
static oid 
rids_next(rids *r)
{
	if (r->cur < BATcount((BAT *) r->data)) {
		BATiter bi = bat_iterator((BAT *) r->data);
		return *(oid*)BUNtail(bi, r->cur++);
	}
	return oid_nil;
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
	tf->rids_next = rids_next;
	tf->rids_destroy = rids_destroy;
	return LOG_OK;
}
