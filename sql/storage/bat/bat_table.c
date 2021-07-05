/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_table.h"
#include "bat_utils.h"
#include "bat_storage.h"

static BAT *
delta_cands(sql_trans *tr, sql_table *t)
{
	sqlstore *store = tr->store;
	BAT *cands = store->storage_api.bind_cands(tr, t, 1, 0);

	if (cands && (cands->ttype == TYPE_msk || mask_cand(cands))) {
		BAT *ncands = BATunmask(cands);
		BBPreclaim(cands);
		cands = ncands;
	}
	return cands;
}

static BAT *
full_column(sql_trans *tr, sql_column *c)
{
	/* return full normalized column bat
	 * 	b := b.copy()
		b := b.replace(u);
	*/
	sqlstore *store = tr->store;
	BAT *b = store->storage_api.bind_col(tr, c, RDONLY);
	BAT *ui = store->storage_api.bind_col(tr, c, RD_UPD_ID);

	if (!b || !ui) {
		bat_destroy(b);
		bat_destroy(ui);
		return NULL;
	}
	if (BATcount(ui)) {
		BAT *uv = store->storage_api.bind_col(tr, c, RD_UPD_VAL), *r;

		if (!uv) {
			bat_destroy(b);
			bat_destroy(ui);
			return NULL;
		}

		r = COLcopy(b, b->ttype, true, TRANSIENT);
		bat_destroy(b);
		b = r;
		if (!b || BATreplace(b, ui, uv, true) != GDK_SUCCEED) {
			bat_destroy(b);
			bat_destroy(ui);
			bat_destroy(uv);
			return NULL;
		}
		bat_destroy(uv);
	}
	bat_destroy(ui);
	return b;
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
		goto return_nil;
	b = full_column(tr, c);
	if (!b) {
		bat_destroy(s);
		goto return_nil;
	}
	r = BATselect(b, s, value, NULL, true, false, false);
	bat_destroy(s);
	bat_destroy(b);
	if (!r)
		goto return_nil;
	s = r;
	while ((n = va_arg(va, sql_column *)) != NULL) {
		value = va_arg(va, void *);
		c = n;

		b = full_column(tr, c);
		if (!b) {
			bat_destroy(s);
			goto return_nil;
		}
		r = BATselect(b, s, value, NULL, true, false, false);
		bat_destroy(s);
		bat_destroy(b);
		if (!r)
			goto return_nil;
		s = r;
	}
	va_end(va);
	if (BATcount(s) == 1) {
		rid = BUNtoid(s, 0);
	}
	bat_destroy(s);
	return rid;
  return_nil:
	va_end(va);
	return oid_nil;
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
		const void *r;
		size_t sz;

		r = BUNtail(bi, q);
		sz = ATOMlen(b->ttype, r);
		res = GDKmalloc(sz);
		if (res)
			memcpy(res, r, sz);
		bat_iterator_end(&bi);
	}
	bat_destroy(b);
	return res;
}

#define column_find_tpe(TPE) \
static TPE \
column_find_##TPE(sql_trans *tr, sql_column *c, oid rid) \
{ \
	BUN q = BUN_NONE; \
	BAT *b; \
	TPE res = -1; \
 \
	b = full_column(tr, c); \
	if (b) { \
		if (rid < b->hseqbase || rid >= b->hseqbase + BATcount(b)) \
			q = BUN_NONE; \
		else \
			q = rid - b->hseqbase; \
	} \
	if (q != BUN_NONE) { \
		BATiter bi = bat_iterator(b); \
		res = *(TPE*)BUNtail(bi, q); \
		bat_iterator_end(&bi); \
	} \
	bat_destroy(b); \
	return res; \
}

column_find_tpe(sqlid)
column_find_tpe(bte)
column_find_tpe(int)
column_find_tpe(lng)

static str
column_find_string_start(sql_trans *tr, sql_column *c, oid rid, ptr *cbat)
{
	BUN q = BUN_NONE;
	BAT **b = (BAT**) cbat;
	str res = NULL;

	*b = full_column(tr, c);
	if (*b) {
		if (rid < (*b)->hseqbase || rid >= (*b)->hseqbase + BATcount(*b))
			q = BUN_NONE;
		else
			q = rid - (*b)->hseqbase;
	}
	if (q != BUN_NONE) {
		BATiter bi = bat_iterator(*b);
		res = (str) BUNtvar(bi, q);
		bat_iterator_end(&bi);
	}
	return res;
}

static void
column_find_string_end(ptr cbat)
{
	BAT *b = (BAT*) cbat;
	bat_destroy(b);
}

static int
column_update_value(sql_trans *tr, sql_column *c, oid rid, void *value)
{
	sqlstore *store = tr->store;
	assert(!is_oid_nil(rid));

	return store->storage_api.update_col(tr, c, &rid, value, c->type.type->localtype);
}

static int
table_insert(sql_trans *tr, sql_table *t, ...)
{
	sqlstore *store = tr->store;
	va_list va;
	node *n = ol_first_node(t->columns);
	void *val = NULL;
	int cnt = 0;
	int ok = LOG_OK;
	BUN offset = 0;

	va_start(va, t);
	if (store->storage_api.claim_tab(tr, t, 1, &offset, NULL) != LOG_OK)
		return LOG_ERR;
	for (; n; n = n->next) {
		sql_column *c = n->data;
		val = va_arg(va, void *);
		if (!val)
			break;
		ok = store->storage_api.append_col(tr, c, offset, NULL, val, 1, c->type.type->localtype);
		if (ok != LOG_OK) {
			va_end(va);
			return ok;
		}
		cnt++;
	}
	va_end(va);
	if (n) {
		// This part of the code should never get reached
		TRC_ERROR(SQL_STORE, "Called table_insert(%s) with wrong number of args (%d,%d)\n", t->base.name, ol_length(t->columns), cnt);
		assert(0);
		return LOG_ERR;
	}
	return LOG_OK;
}

static int
table_delete(sql_trans *tr, sql_table *t, oid rid)
{
	sqlstore *store = tr->store;
	assert(!is_oid_nil(rid));

	return store->storage_api.delete_tab(tr, t, &rid, TYPE_oid);
}

static res_table *
table_orderby(sql_trans *tr, sql_table *t, sql_column *jl, sql_column *jr, sql_column *jl2, sql_column *jr2, sql_column *o, ...)
{
	/* jl/jr are columns on which we first join */
	/* if also jl2,jr2, we need too do another join, where both tables differ from 't' */

	va_list va;
	BAT *b = NULL, *r = NULL, *cl, *cr = NULL, *cr2 = NULL, *id = NULL, *grp = NULL;
	/* if pointers are equal, make it an inclusive select */

	cl = delta_cands(tr, t);
	if (cl == NULL)
		return NULL;

	if (jl && jr) {
		BAT *lcb, *rcb, *r = NULL, *l = NULL;
		gdk_return ret;

		cr = delta_cands(tr, jr->t);
		if (cr == NULL) {
			bat_destroy(cl);
			return NULL;
		}

		lcb = full_column(tr, jl);
		rcb = full_column(tr, jr);
		ret = BATjoin(&l, &r, lcb, rcb, cl, cr, false, BATcount(lcb));
		bat_destroy(cl);
		bat_destroy(cr);
		bat_destroy(lcb);
		bat_destroy(rcb);
		if (ret != GDK_SUCCEED)
			return NULL;
		cl = l;
		cr = r;
	}
	/* we assume 1->n joins, therefor first join between jl2/jr2 */
	if (jl2 && jr2) {
		assert(jr->t == jl2->t);
		BAT *lcb, *rcb, *r = NULL, *l = NULL;
		gdk_return ret;

		cr2 = delta_cands(tr, jr2->t);
		if (cr2 == NULL) {
			bat_destroy(cl);
			bat_destroy(cr);
			return NULL;
		}

		lcb = full_column(tr, jl2);
		rcb = full_column(tr, jr2);
		l = BATproject(cr, lcb); /* project because cr is join result */
		bat_destroy(lcb);
		lcb = l;
		ret = BATjoin(&l, &r, lcb, rcb, NULL, cr2, false, BATcount(lcb));
		bat_destroy(cr2);
		bat_destroy(lcb);
		bat_destroy(rcb);
		if (ret != GDK_SUCCEED)
			return NULL;
		lcb = BATproject(l, cl);
		rcb = BATproject(l, cr);
		bat_destroy(l);
		if (!lcb || !rcb) {
			bat_destroy(cl);
			bat_destroy(cr);
			bat_destroy(lcb);
			bat_destroy(rcb);
			bat_destroy(r);
			return NULL;
		}
		cl = lcb;
		cr = rcb;
		cr2 = r;
	}

	va_start(va, o);
	do {
		BAT *nid = NULL, *ngrp = NULL;
		sql_column *next = va_arg(va, sql_column *);

		b = full_column(tr, o);
		if (b)
			r = BATproject( (o->t==t) ? cl : (cr2 && o->t==jr2->t) ? cr2 : cr, b);
		bat_destroy(b);
		if (!b || !r) {
			bat_destroy(cl);
			bat_destroy(cr);
			bat_destroy(cr2);
			va_end(va);
			return NULL;
		}
		/* (sub)order b */
		if (BATsort(NULL, &nid, next?&ngrp:NULL, r, id, grp, false, false, false) != GDK_SUCCEED) {
			bat_destroy(r);
			bat_destroy(id);
			bat_destroy(grp);
			bat_destroy(cl);
			bat_destroy(cr);
			bat_destroy(cr2);
			va_end(va);
			return NULL;
		}
		bat_destroy(r);
		bat_destroy(id);
		bat_destroy(grp);
		id = nid;
		grp = ngrp;
		o = next;
	} while (o);
	bat_destroy(grp);
	va_end(va);

	r = BATproject(id, cl);
	bat_destroy(id);
	bat_destroy(cl);
	bat_destroy(cr);
	bat_destroy(cr2);
	cl = r;
	/* project all in the new order */
	res_table *rt = res_table_create(tr, 1/*result_id*/, 1/*query_id*/, ol_length(t->columns), Q_TABLE, NULL, NULL);
	rt->nr_rows = BATcount(cl);
	for (node *n = ol_first_node(t->columns); n; n = n->next) {
		o = n->data;
		b = full_column(tr, o);
		if (b)
			r = BATproject(cl, b);
		bat_destroy(b);
		if (!b || !r) {
			bat_destroy(cl);
			bat_destroy(b);
			res_table_destroy(rt);
			return NULL;
		}
		(void)res_col_create(tr, rt, t->base.name, o->base.name, o->type.type->base.name, o->type.type->digits, o->type.type->scale, TYPE_bat, r, true);
	}
	bat_destroy(cl);
	return rt;
}

static void *
table_fetch_value(res_table *rt, sql_column *c)
{
	/* this function is only ever called during startup, and therefore
	 * there are no other threads that may be modifying the BAT under
	 * our hands, so returning a pointer into the heap is fine */
	BAT *b = (BAT*)rt->cols[c->colnr].p;
	BATiter bi = bat_iterator_nolock(b);
	assert(b->ttype && b->ttype != TYPE_msk);
	if (b->tvarsized)
		return BUNtvar(bi, rt->cur_row);
	return Tloc(b, rt->cur_row);
	//return (void*)BUNtail(bi, rt->cur_row);
}

static void
table_result_destroy(res_table *rt)
{
	if (rt)
		res_table_destroy(rt);
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
	bool hi = key_value_low == key_value_high;

	if(!rs)
		return NULL;
	s = delta_cands(tr, key->t);
	if (s == NULL) {
		GDKfree(rs);
		return NULL;
	}
	b = full_column(tr, key);
	if (b == NULL) {
		bat_destroy(s);
		GDKfree(rs);
		return NULL;
	}
	if (!kvl)
		kvl = ATOMnilptr(b->ttype);
	if (!kvh && kvl != ATOMnilptr(b->ttype))
		kvh = ATOMnilptr(b->ttype);
	if (key_value_low) {
		BAThash(b);
		r = BATselect(b, s, kvl, kvh, true, hi, false);
		bat_destroy(s);
		s = r;
	}
	bat_destroy(b);
	if (s == NULL) {
		GDKfree(rs);
		return NULL;
	}
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
			r = BATselect(b, s, kvl, kvh, true, hi, false);
			bat_destroy(s);
			s = r;
			bat_destroy(b);
			if (s == NULL) {
				GDKfree(rs);
				va_end(va);
				return NULL;
			}
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
	if (BATsort(NULL, &o, NULL, s, NULL, NULL, false, false, false) != GDK_SUCCEED) {
		bat_destroy(s);
		return NULL;
	}
	bat_destroy(s);
	s = BATproject(o, r->data);
	bat_destroy(o);
	if (s == NULL)
		return NULL;
	bat_destroy(r->data);
	r->data = s;
	return r;
}


/* return table rids from result of rids_select, return (oid_nil) when done */
static oid
rids_next(rids *r)
{
	if (r->cur < BATcount((BAT *) r->data)) {
		BAT *t = r->data;

		if (t && (t->ttype == TYPE_msk || mask_cand(t))) {
			r->data = BATunmask(t);
			if (!r->data) {
				r->data = t;
				return oid_nil;
			}
			bat_destroy(t);
		}
		return BUNtoid((BAT *) r->data, r->cur++);
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
	gdk_return ret;

	lcb = full_column(tr, lc);
	rcb = full_column(tr, rc);
	ret = BATjoin(&s, &d, lcb, rcb, l->data, r->data, false, BATcount(lcb));
	bat_destroy(l->data);
	bat_destroy(d);
	if (ret != GDK_SUCCEED) {
		l->data = NULL;
	} else {
		l->data = s;
	}
	bat_destroy(lcb);
	bat_destroy(rcb);
	return l;
}

static subrids *
subrids_create(sql_trans *tr, rids *t1, sql_column *rc, sql_column *lc, sql_column *obc)
{
	/* join t1.rc with lc order by obc */
	subrids *r;
	BAT *lcb, *rcb, *s, *obb, *d = NULL, *o, *g, *ids, *rids = NULL;
	gdk_return ret;

	lcb = full_column(tr, lc);
	rcb = full_column(tr, rc);
	s = delta_cands(tr, lc->t);
	if (lcb == NULL || rcb == NULL || s == NULL) {
		if (lcb)
			bat_destroy(lcb);
		if (rcb)
			bat_destroy(rcb);
		bat_destroy(s);
		return NULL;
	}

	ret = BATjoin(&rids, &d, lcb, rcb, s, t1->data, false, BATcount(lcb));
	bat_destroy(s);
	bat_destroy(rcb);
	if (ret != GDK_SUCCEED) {
		bat_destroy(lcb);
		return NULL;
	}
	bat_destroy(d);

	s = BATproject(rids, lcb);
	bat_destroy(lcb);
	if (s == NULL) {
		bat_destroy(rids);
		return NULL;
	}
	lcb = s;

	if ((obb = full_column(tr, obc)) == NULL) {
		bat_destroy(lcb);
		bat_destroy(rids);
		return NULL;
	}
	s = BATproject(rids, obb);
	bat_destroy(obb);
	if (s == NULL) {
		bat_destroy(lcb);
		bat_destroy(rids);
		return NULL;
	}
	obb = s;

	/* need id, obc */
	ids = o = g = NULL;
	ret = BATsort(&ids, &o, &g, lcb, NULL, NULL, false, false, false);
	bat_destroy(lcb);
	if (ret != GDK_SUCCEED) {
		bat_destroy(obb);
		bat_destroy(rids);
		return NULL;
	}

	s = NULL;
	ret = BATsort(NULL, &s, NULL, obb, o, g, false, false, false);
	bat_destroy(obb);
	bat_destroy(o);
	bat_destroy(g);
	if (ret != GDK_SUCCEED) {
		bat_destroy(ids);
		bat_destroy(rids);
		return NULL;
	}

	o = BATproject(s, rids);
	bat_destroy(rids);
	bat_destroy(s);
	if (o == NULL) {
		bat_destroy(ids);
		return NULL;
	}
	rids = o;

	assert(ids->ttype == TYPE_int && ATOMtype(rids->ttype) == TYPE_oid);
	r = ZNEW(subrids);
	if (r == NULL) {
		bat_destroy(ids);
		bat_destroy(rids);
		return NULL;
	}
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
		sqlid id = *(sqlid*)BUNtloc(ii, r->pos);
		bat_iterator_end(&ii);
		if (id == r->id)
			return BUNtoid((BAT *) r->rids, r->pos++);
	}
	return oid_nil;
}

static sqlid
subrids_nextid(subrids *r)
{
	if (r->pos < BATcount((BAT *) r->ids)) {
		BATiter ii = bat_iterator((BAT *) r->ids);
		r->id = *(sqlid*)BUNtloc(ii, r->pos);
		bat_iterator_end(&ii);
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
	gdk_return ret;

	if (lcb == NULL || rcb == NULL) {
		if (lcb)
			bat_destroy(lcb);
		if (rcb)
			bat_destroy(rcb);
		return NULL;
	}
	s = BATproject(r->rids, rcb);
	bat_destroy(rcb);
	if (s == NULL) {
		bat_destroy(lcb);
		return NULL;
	}
	rcb = s;

	s = BATproject(l->data, lcb);
	if (s == NULL) {
		bat_destroy(lcb);
		bat_destroy(rcb);
		return NULL;
	}

	diff = BATdiff(s, rcb, NULL, NULL, false, false, BUN_NONE);
	bat_destroy(rcb);
	if (diff == NULL) {
		bat_destroy(lcb);
		bat_destroy(s);
		return NULL;
	}

	ret = BATjoin(&rids, &d, lcb, s, NULL, diff, false, BATcount(s));
	bat_destroy(diff);
	bat_destroy(lcb);
	bat_destroy(s);
	if (ret != GDK_SUCCEED)
		return NULL;

	bat_destroy(d);
	bat_destroy(l->data);
	l->data = rids;
	return l;
}

void
bat_table_init( table_functions *tf )
{
	tf->column_find_row = column_find_row;
	tf->column_find_value = column_find_value;
	tf->column_find_sqlid = column_find_sqlid;
	tf->column_find_bte = column_find_bte;
	tf->column_find_int = column_find_int;
	tf->column_find_lng = column_find_lng;
	tf->column_find_string_start = column_find_string_start; /* this function returns a pointer to the heap, use it with care! */
	tf->column_find_string_end = column_find_string_end; /* don't forget to call this function to unfix the bat descriptor! */
	tf->column_update_value = column_update_value;
	tf->table_insert = table_insert;
	tf->table_delete = table_delete;
	tf->table_orderby = table_orderby;
	tf->table_fetch_value = table_fetch_value;
	tf->table_result_destroy = table_result_destroy;

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
