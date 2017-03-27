/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
	BAT *tids = COLnew(0, TYPE_void, 0, TRANSIENT);
	size_t nr = store_funcs.count_col(tr, c, 1);

	if (!tids)
		return NULL;
	tids->tseqbase = 0;
	BATsetcount(tids, (BUN) nr);
	tids->trevsorted = 0;

	tids->tkey = 1;
	tids->tdense = 1;

	if (store_funcs.count_del(tr, t)) {
		BAT *d, *diff = NULL;

		if ((d = store_funcs.bind_del(tr, t, RD_INS)) != NULL) {
			diff = BATdiff(tids, d, NULL, NULL, 0, BUN_NONE);
			bat_destroy(d);
		}
		bat_destroy(tids);
		tids = diff;
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
	if (!store_initialized && d->cached) 
		return temp_descriptor(d->cached->batCacheid);
	tids = _delta_cands(tr, t);
	if (!store_initialized && !d->cached) /* only cache during catalog loading */
		d->cached = temp_descriptor(tids->batCacheid);
	return tids;
}

static BAT *
delta_full_bat_( sql_column *c, sql_delta *bat, int temp)
{
	/* return full normalized column bat
	 * 	b := b.copy()
		b := b.append(i);
		b := b.replace(u);
	*/
	BAT *r, *b, *ui, *uv, *i = temp_descriptor(bat->ibid);
	int needcopy = 1;

	if (!i)
		return NULL;
	r = i; 
	if (temp) 
		return r;
	b = temp_descriptor(bat->bid);
	if (!b) {
		b = i;
	} else {
		if (BATcount(i)) {
			r = COLcopy(b, b->ttype, 1, TRANSIENT); 
			bat_destroy(b); 
			b = r;
			BATappend(b, i, NULL, TRUE);
			needcopy = 0;
		}
		bat_destroy(i); 
	}
	if (bat->uibid && bat->ucnt) {
		ui = temp_descriptor(bat->uibid);
		uv = temp_descriptor(bat->uvbid);
		if (ui && BATcount(ui)) {
			if (needcopy) {
				r = COLcopy(b, b->ttype, 1, TRANSIENT); 
				bat_destroy(b); 
				b = r;
			}
			void_replace_bat(b, ui, uv, TRUE);
		}
		bat_destroy(ui); 
		bat_destroy(uv); 
	}
	(void)c;
	if (!store_initialized && !bat->cached) 
		bat->cached = b;
	return b;
}

static BAT *
delta_full_bat( sql_column *c, sql_delta *bat, int temp)
{
	if (!store_initialized && bat->cached) 
		return bat->cached;
	return delta_full_bat_( c, bat, temp);
}

static BAT *
full_column(sql_trans *tr, sql_column *c)
{
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->stime);
	}
	return delta_full_bat(c, c->data, isTemp(c));
}

static void
full_destroy(sql_column *c, BAT *b)
{
	sql_delta *d = c->data;
	assert(d);
	if (d->cached != b)
		bat_destroy(b);
}

static oid
column_find_row(sql_trans *tr, sql_column *c, const void *value, ...)
{
	va_list va;
	BAT *b = NULL, *s = NULL, *r = NULL;
	oid rid = oid_nil;
	sql_column *n = NULL;

	va_start(va, value);
	s = delta_cands(tr, c->t);
	if (!s)
		return oid_nil;
	b = full_column(tr, c);
	if (!b)
		return oid_nil;
	r = BATselect(b, s, value, NULL, 1, 0, 0);
	if (!r)
		return oid_nil;
	bat_destroy(s);
	s = r;
	full_destroy(c, b);
	while ((n = va_arg(va, sql_column *)) != NULL) {
		value = va_arg(va, void *);
		c = n;

		b = full_column(tr, c);
		if (!b)
			return oid_nil;
		r = BATselect(b, s, value, NULL, 1, 0, 0);
		if (!r)
			return oid_nil;
		bat_destroy(s);
		s = r;
		full_destroy(c, b);
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
	BUN q = BUN_NONE;
	BAT *b;
	void *res = NULL;

	b = full_column(tr, c);
	if (b) {
		if (rid < b->hseqbase || rid >= b->hseqbase + BATcount(b))
			q = BUN_NONE;
		else
			q = rid - b->hseqbase;
	}
	if (q != BUN_NONE) {
		BATiter bi = bat_iterator(b);
		void *r;
		int sz;

		res = BUNtail(bi, q);
		sz = ATOMlen(b->ttype, res);
		// FIXME unchecked_malloc GDKmalloc can return NULL
		r = GDKmalloc(sz);
		memcpy(r,res,sz);
		res = r;
	}
	full_destroy(c, b);
	return res;
}

static int
column_update_value(sql_trans *tr, sql_column *c, oid rid, void *value)
{
	assert(rid != oid_nil);

	store_funcs.update_col(tr, c, &rid, value, c->type.type->localtype);
	return LOG_OK;
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
		return LOG_ERR;
	}
	return LOG_OK;
}

static int
table_delete(sql_trans *tr, sql_table *t, oid rid)
{
	assert(rid != oid_nil);

	store_funcs.delete_tab(tr, t, &rid, TYPE_oid);
	return LOG_OK;
}



/* returns table rids, for the given select ranges */
static rids *
rids_select( sql_trans *tr, sql_column *key, const void *key_value_low, const void *key_value_high, ...)
{
	va_list va;
	BAT *b = NULL, *r = NULL, *s = NULL;
	rids *rs = ZNEW(rids);
	const void *kvl = key_value_low, *kvh = key_value_high;
	/* if pointers are equal, make it an inclusive select */
	int hi = key_value_low == key_value_high;

	s = delta_cands(tr, key->t);
	b = full_column(tr, key);
	if (!kvl)
		kvl = ATOMnilptr(b->ttype);
	if (!kvh && kvl != ATOMnilptr(b->ttype))
		kvh = ATOMnilptr(b->ttype);
	if (key_value_low) {
		BAThash(b, 0);
		r = BATselect(b, s, kvl, kvh, 1, hi, 0);
		bat_destroy(s);
		s = r;
	}
	full_destroy(key, b);
	if (key_value_low || key_value_high) {
		va_start(va, key_value_high);
		while ((key = va_arg(va, sql_column *)) != NULL) {
			kvl = va_arg(va, void *);
			kvh = va_arg(va, void *);
	
			b = full_column(tr, key);
			if (!kvl)
				kvl = ATOMnilptr(b->ttype);
			if (!kvh && kvl != ATOMnilptr(b->ttype))
				kvh = ATOMnilptr(b->ttype);
			assert(kvh);
			r = BATselect(b, s, kvl, kvh, 1, hi, 0);
			bat_destroy(s);
			s = r;
			full_destroy(key, b);
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
	full_destroy(orderby_col, b);
	BATsort(NULL, &o, NULL, s, NULL, NULL, 0, 0);
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

static int
rids_empty(rids *r )
{
	BAT *b = r->data;
	return BATcount(b) <= 0;
}

static rids *
rids_join(sql_trans *tr, rids *l, sql_column *lc, rids *r, sql_column *rc)
{
	BAT *lcb, *rcb, *s = NULL, *d = NULL;
	
	lcb = full_column(tr, lc);
	rcb = full_column(tr, rc);
	BATjoin(&s, &d, lcb, rcb, l->data, r->data, FALSE, BATcount(lcb));
	bat_destroy(l->data);
	bat_destroy(d);
	l->data = s;
	full_destroy(lc, lcb);
	full_destroy(rc, rcb);
	return l;
}

static subrids *
subrids_create(sql_trans *tr, rids *t1, sql_column *rc, sql_column *lc, sql_column *obc)
{
	/* join t1.rc with lc order by obc */
	subrids *r = ZNEW(subrids);
	BAT *lcb, *rcb, *s, *obb, *d = NULL, *o, *g, *ids, *rids = NULL;
	
	lcb = full_column(tr, lc);
	rcb = full_column(tr, rc);

	s = delta_cands(tr, lc->t);
	BATjoin(&rids, &d, lcb, rcb, s, t1->data, FALSE, BATcount(lcb));
	bat_destroy(d);
	bat_destroy(s);
	full_destroy(rc, rcb);

	s = BATproject(rids, lcb);
	full_destroy(lc, lcb);
	lcb = s;

	obb = full_column(tr, obc);
	s = BATproject(rids, obb);
	full_destroy(obc, obb);
	obb = s;

	/* need id, obc */
	ids = o = g = NULL;
	BATsort(&ids, &o, &g, lcb, NULL, NULL, 0, 0);
	bat_destroy(lcb);

	s = NULL;
	BATsort(NULL, &s, NULL, obb, o, g, 0, 0);
	bat_destroy(obb);
	bat_destroy(o);
	bat_destroy(g);

	o = BATproject(s, rids);
	bat_destroy(rids);
	bat_destroy(s);
	rids = o;

	assert(ids->ttype == TYPE_int && ATOMtype(rids->ttype) == TYPE_oid);
	r->id = 0;
	r->pos = 0;
	r->ids = ids;
	r->rids = rids;
	return r;
}

static oid
subrids_next(subrids *r)
{
	if (r->pos < BATcount((BAT *) r->ids)) {
		BATiter ii = bat_iterator((BAT *) r->ids);
		BATiter ri = bat_iterator((BAT *) r->rids);
		int id = *(int*)BUNtail(ii, r->pos);
		if (id == r->id)
			return *(oid*)BUNtail(ri, r->pos++);
	}
	return oid_nil;
}

static sqlid
subrids_nextid(subrids *r)
{
	if (r->pos < BATcount((BAT *) r->ids)) {
		BATiter ii = bat_iterator((BAT *) r->ids);
		r->id = *(int*)BUNtail(ii, r->pos);
		return r->id;
	}
	return -1;
}

static void
subrids_destroy(subrids *r )
{
	if (r->ids)
		bat_destroy(r->ids);
	if (r->rids)
		bat_destroy(r->rids);
	_DELETE(r);
}

/* get the non - join results */
static rids *
rids_diff(sql_trans *tr, rids *l, sql_column *lc, subrids *r, sql_column *rc )
{
	BAT *lcb = full_column(tr, lc), *s, *d, *rids, *diff;
	BAT *rcb = full_column(tr, rc);

	s = BATproject(r->rids, rcb);
	full_destroy(rc, rcb);
	rcb = s;

	s = BATproject(l->data, lcb);

	diff = BATdiff(s, rcb, NULL, NULL, 0, BUN_NONE);

	BATjoin(&rids, &d, lcb, s, NULL, diff, FALSE, BATcount(s));
	bat_destroy(diff);
	bat_destroy(d);
	full_destroy(lc, lcb);
	bat_destroy(s);

	bat_destroy(l->data);
	l->data = rids;
	return l;
}

static int
table_vacuum(sql_trans *tr, sql_table *t)
{
	BAT *tids = delta_cands(tr, t);
	BAT **cols;
	node *n;
	// FIXME unchecked_malloc NEW_ARRAY can return NULL
	cols = NEW_ARRAY(BAT*, cs_size(&t->columns));
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		BAT *v = store_funcs.bind_col(tr, c, RDONLY);

		cols[c->colnr] = BATproject(tids, v);
		BBPunfix(v->batCacheid);
	}
	sql_trans_clear_table(tr, t);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		store_funcs.append_col(tr, c, cols[c->colnr], TYPE_bat);
		BBPunfix(cols[c->colnr]->batCacheid);
	}
	_DELETE(cols);
	return LOG_OK;
}

void
bat_table_init( table_functions *tf )
{
	tf->column_find_row = column_find_row;
	tf->column_find_value = column_find_value;

	tf->column_update_value = column_update_value;
	tf->table_insert = table_insert;
	tf->table_delete = table_delete;
	tf->table_vacuum = table_vacuum;
	
	tf->rids_select = rids_select;
	tf->rids_orderby = rids_orderby;
	tf->rids_join = rids_join;
	tf->rids_next = rids_next;
	tf->rids_destroy = rids_destroy;
	tf->rids_empty = rids_empty;

	tf->subrids_create = subrids_create;
	tf->subrids_next = subrids_next;
	tf->subrids_nextid = subrids_nextid;
	tf->subrids_destroy = subrids_destroy;
	tf->rids_diff = rids_diff;
}
