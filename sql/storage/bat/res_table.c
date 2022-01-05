/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "res_table.h"
#include "bat_utils.h"
#include "sql_types.h"

static void
bat_incref(bat bid)
{
	BBPretain(bid);
}

static void
bat_decref(bat bid)
{
	BBPrelease(bid);
}

res_table *
res_table_create(sql_trans *tr, int res_id, oid query_id, int nr_cols, mapi_query_t type, res_table *next, void *O)
{
	BAT *order = (BAT*)O;
	res_table *t = MNEW(res_table);
	res_col *tcols = ZNEW_ARRAY(res_col, nr_cols);

	(void) tr;
	if (!t || !tcols) {
		_DELETE(t);
		_DELETE(tcols);
		return NULL;
	}

	*t = (res_table) {
		.id = res_id,
		.query_id = query_id,
		.query_type = type,
		.cols = tcols,
		.nr_cols = nr_cols,
		.next = next,
	};

	if (order) {
		t->order = order->batCacheid;
		bat_incref(t->order);
		t->nr_rows = BATcount(order);
	}
	return t;
}

res_col *
res_col_create(sql_trans *tr, res_table *t, const char *tn, const char *name, const char *typename, int digits, int scale, char mtype, void *val, bool cached)
{
	res_col *c = t->cols + t->cur_col;
	BAT *b;

	if (!sql_find_subtype(&c->type, typename, digits, scale))
		sql_init_subtype(&c->type, sql_trans_bind_type(tr, NULL, typename), digits, scale);
	c->tn = _STRDUP(tn);
	c->name = _STRDUP(name);
	if (c->tn == NULL || c->name == NULL) {
		_DELETE(c->tn);
		_DELETE(c->name);
		return NULL;
	}
	c->b = 0;
	c->p = NULL;
	c->mtype = mtype;
	if (mtype == TYPE_bat) {
		b = (BAT*)val;
	} else { // wrap scalar values in BATs for result consistency
		b = COLnew(0, mtype, 1, TRANSIENT);
		if (b == NULL) {
			_DELETE(c->tn);
			_DELETE(c->name);
			return NULL;
		}
		if (BUNappend(b, val, false) != GDK_SUCCEED) {
			BBPreclaim(b);
			_DELETE(c->tn);
			_DELETE(c->name);
			return NULL;
		}
		/* we need to set the order bat otherwise mvc_export_result won't work with single-row result sets containing BATs */
		if (!t->order) {
			oid zero = 0;
			BAT *o = BATconstant(0, TYPE_oid, &zero, 1, TRANSIENT);
			if (o == NULL) {
				BBPreclaim(b);
				_DELETE(c->tn);
				_DELETE(c->name);
				return NULL;
			}
			t->order = o->batCacheid;
			t->nr_rows = 1;
			BBPkeepref(t->order);
		}
		cached = true; /* simply keep memory pointer for this small bat */
	}
	c->b = b->batCacheid;
	c->cached = cached;
	if (cached)
		c->p = (void*)b;
	else
		bat_incref(c->b);
	t->cur_col++;
	assert(t->cur_col <= t->nr_cols);
	return c;
}

static void
res_col_destroy(res_col *c)
{
	if (c->b && !c->cached) {
		bat_decref(c->b);
	} else if (c->b) {
		bat_destroy((BAT*)c->p);
	} else {
		_DELETE(c->p);
	}
	_DELETE(c->name);
	_DELETE(c->tn);
}

void
res_table_destroy(res_table *t)
{
	int i;

	for (i = 0; i < t->nr_cols; i++) {
		res_col *c = t->cols + i;

		if (c)
			res_col_destroy(c);
	}
	if (t->order)
		bat_decref(t->order);
	_DELETE(t->cols);
	_DELETE(t);
}

res_table *
res_tables_remove(res_table *results, res_table *t)
{
	res_table *r = results;

	if (r == t) {
		results = t->next;
	} else {
		for (; r; r = r->next) {
			if (r->next == t) {
				r->next = t->next;
				break;
			}
		}
	}
	res_table_destroy(t);
	return results;
}

void
res_tables_destroy(res_table *tab)
{
	if (tab) {
		res_table *r = tab, *t;

		for (t = r; t; t = r) {
			r = t->next;
			res_table_destroy(t);
		}
	}
}

res_table *
res_tables_find(res_table *results, int res_id)
{
	res_table *r = results;

	for (; r; r = r->next) {
		if (r->id == res_id)
			return r;
	}
	return NULL;
}
