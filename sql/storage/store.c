/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "sql_types.h"
#include "sql_storage.h"
#include "store_dependency.h"
#include "store_sequence.h"
#include "mutils.h"

#include "bat/bat_utils.h"
#include "bat/bat_storage.h"
#include "bat/bat_table.h"
#include "bat/bat_logger.h"

/* version 05.23.05 of catalog */
#define CATALOG_VERSION 52305	/* first after Mar2025 */

ulng
store_function_counter(sqlstore *store)
{
	ulng ts = ATOMIC_INC(&store->function_counter);
	return ts;
}

static ulng
store_timestamp(sqlstore *store)
{
	ulng ts = ATOMIC_INC(&store->timestamp);
	return ts;
}

ulng
store_get_timestamp(sqlstore *store)
{
	ulng ts = ATOMIC_GET(&store->timestamp);
	return ts;
}

static ulng
store_transaction_id(sqlstore *store)
{
	ulng tid = ATOMIC_INC(&store->transaction);
	return tid;
}

ulng
store_oldest(sqlstore *store, sql_trans *tr)
{
	if (tr && tr->ts == (ulng) ATOMIC_GET(&store->oldest)) {
		sql_trans *otr = store->active->h->data;
		if (otr == tr && store->active->h->next) {
			otr = store->active->h->next->data;
			return otr->ts;
		}
	}
	return (ulng) ATOMIC_GET(&store->oldest);
}

static ulng
store_oldest_pending(sqlstore *store)
{
	assert(store->oldest_pending != TRANSACTION_ID_BASE);
	return store->oldest_pending;
}

static inline bool
instore(sqlid id)
{
	if (id >= 2000 && id <= 2167)
		return true;
	return false;
}

static void
id_destroy(sqlstore *store, void *p)
{
	(void)store;
	GDKfree(p);
}

static void
dep_destroy(sqlstore *store, sql_dependency_change *dep)
{
	(void)store;
	GDKfree(dep);
}

static void
type_destroy(sqlstore *store, sql_type *t)
{
	(void)store;
	assert(t->base.refcnt > 0);
	if (ATOMIC_DEC(&t->base.refcnt) > 0)
		return;
	_DELETE(t->impl);
	_DELETE(t->base.name);
	_DELETE(t);
}

void
arg_destroy(sql_store store, sql_arg *a)
{
	(void)store;
	_DELETE(a->name);
	_DELETE(a);
}

static void
func_destroy(sqlstore *store, sql_func *f)
{
	assert(f->base.refcnt > 0);
	if (ATOMIC_DEC(&f->base.refcnt) > 0)
		return;
	if (f->lang == FUNC_LANG_SQL && f->instantiated) {
		/* clean backend code */
		backend_freecode(sql_shared_module_name, 0, f->imp);
	}
	if (f->res)
		list_destroy2(f->res, store);
	list_destroy2(f->ops, store);
	_DELETE(f->imp);
	_DELETE(f->mod);
	_DELETE(f->query);
	_DELETE(f->base.name);
	_DELETE(f);
}

static void
seq_destroy(sqlstore *store, sql_sequence *s)
{
	(void)store;
	assert(s->base.refcnt > 0);
	if (ATOMIC_DEC(&s->base.refcnt) > 0)
		return;
	_DELETE(s->base.name);
	_DELETE(s);
}

static void
kc_destroy(sqlstore *store, sql_kc *kc)
{
	(void)store;
	_DELETE(kc);
}

static void
key_destroy(sqlstore *store, sql_key *k)
{
	assert(k->base.refcnt > 0);
	if (ATOMIC_DEC(&k->base.refcnt) > 0)
		return;
	list_destroy2(k->columns, store);
	_DELETE(k->check);
	k->columns = NULL;
	_DELETE(k->base.name);
	_DELETE(k);
}

void
idx_destroy(sqlstore *store, sql_idx * i)
{
	assert(i->base.refcnt > 0);
	if (ATOMIC_DEC(&i->base.refcnt) > 0)
		return;
	list_destroy2(i->columns, store);
	i->columns = NULL;

	if (ATOMIC_PTR_GET(&i->data))
		store->storage_api.destroy_idx(store, i);
	_DELETE(i->base.name);
	_DELETE(i);
}

static void
trigger_destroy(sqlstore *store, sql_trigger *t)
{
	assert(t->base.refcnt > 0);
	if (ATOMIC_DEC(&t->base.refcnt) > 0)
		return;
	/* remove trigger from schema */
	if (t->columns) {
		list_destroy2(t->columns, store);
		t->columns = NULL;
	}
	_DELETE(t->old_name);
	_DELETE(t->new_name);
	_DELETE(t->condition);
	_DELETE(t->statement);
	_DELETE(t->base.name);
	_DELETE(t);
}

void
column_destroy(sqlstore *store, sql_column *c)
{
	assert(c->base.refcnt > 0);
	if (ATOMIC_DEC(&c->base.refcnt) > 0)
		return;
	if (ATOMIC_PTR_GET(&c->data))
		store->storage_api.destroy_col(store, c);
	_DELETE(c->min);
	_DELETE(c->max);
	_DELETE(c->def);
	_DELETE(c->storage_type);
	_DELETE(c->base.name);
	_DELETE(c);
}

static void
int_destroy(sqlstore *store, int *v)
{
	(void)store;
	_DELETE(v);
}

void
table_destroy(sqlstore *store, sql_table *t)
{
	assert(t->base.refcnt > 0);
	if (ATOMIC_DEC(&t->base.refcnt) > 0)
		return;
	if (isTable(t))
		store->storage_api.destroy_del(store, t);
	/* cleanup its parts */
	list_destroy2(t->members, store);
	ol_destroy(t->idxs, store);
	ol_destroy(t->keys, store);
	ol_destroy(t->triggers, store);
	ol_destroy(t->columns, store);
	if (isPartitionedByExpressionTable(t)) {
		if (t->part.pexp->cols)
			list_destroy2(t->part.pexp->cols, store);
		_DELETE(t->part.pexp->exp);
		_DELETE(t->part.pexp);
	}
	_DELETE(t->query);
	_DELETE(t->base.name);
	_DELETE(t);
}

void
part_value_destroy(sql_store store, sql_part_value *pv)
{
	(void)store;
	_DELETE(pv->value);
	_DELETE(pv);
}

static void
part_destroy(sqlstore *store, sql_part *p)
{
	assert(p->base.refcnt > 0);
	if (ATOMIC_DEC(&p->base.refcnt) > 0)
		return;
	if (p->part.range.maxvalue) {
		_DELETE(p->part.range.minvalue);
		_DELETE(p->part.range.maxvalue);
	} else if (p->part.values)
		list_destroy2(p->part.values, store);
	_DELETE(p->base.name);
	_DELETE(p);
}

static void
schema_destroy(sqlstore *store, sql_schema *s)
{
	assert(s->base.refcnt > 0);
	if (ATOMIC_DEC(&s->base.refcnt) > 0)
		return;
	/* cleanup its parts */
	os_destroy(s->parts, store);
	os_destroy(s->triggers, store);
	os_destroy(s->idxs, store);
	os_destroy(s->keys, store);
	os_destroy(s->seqs, store);
	os_destroy(s->tables, store);
	os_destroy(s->funcs, store);
	os_destroy(s->types, store);
	_DELETE(s->base.name);
	_DELETE(s);
}

static void
predicate_destroy(sqlstore *store, pl *p)
{
	(void) store;
	if (p->r) {
		VALclear(&p->r->data);
		_DELETE(p->r);
	}
	if (p->f) {
		VALclear(&p->f->data);
		_DELETE(p->f);
	}
	_DELETE(p);
}

int
sql_trans_add_predicate(sql_trans* tr, sql_column *c, unsigned int cmp, atom *r, atom *f, bool anti, bool semantics)
{
	pl *p = ZNEW(pl);

	if (!p)
		return LOG_ERR;
	p->c = c;
	p->cmp = cmp;
	p->anti = anti;
	p->semantics = semantics;
	p->r = r;
	p->f = f;
	MT_lock_set(&tr->lock);
	if (!tr->predicates && !(tr->predicates = list_create((fdestroy) &predicate_destroy))) {
		predicate_destroy(tr->store, p);
		MT_lock_unset(&tr->lock);
		return LOG_ERR;
	}
	if (!list_append(tr->predicates, p)) {
		predicate_destroy(tr->store, p);
		MT_lock_unset(&tr->lock);
		return LOG_ERR;
	}
	MT_lock_unset(&tr->lock);
	return LOG_OK;
}

int
sql_trans_add_dependency(sql_trans* tr, sqlid id, sql_dependency_change_type tp)
{
	sql_dependency_change *dep = MNEW(sql_dependency_change);

	if (!dep)
		return LOG_ERR;
	*dep = (sql_dependency_change) {
		.objid = id,
		.type = tp
	};
	if (!tr->dependencies && !(tr->dependencies = list_create((fdestroy) &dep_destroy))) {
		_DELETE(dep);
		return LOG_ERR;
	}
	if (!list_append(tr->dependencies, dep)) {
		_DELETE(dep);
		return LOG_ERR;
	}
	return LOG_OK;
}

int
sql_trans_add_dependency_change(sql_trans *tr, sqlid id, sql_dependency_change_type tp)
{
	sql_dependency_change *dep = MNEW(sql_dependency_change);

	if (!dep)
		return LOG_ERR;
	*dep = (sql_dependency_change) {
		.objid = id,
		.type = tp
	};
	if (!tr->depchanges && !(tr->depchanges = list_create((fdestroy) &dep_destroy))) {
		_DELETE(dep);
		return LOG_ERR;
	}
	if (!list_append(tr->depchanges, dep)) {
		_DELETE(dep);
		return LOG_ERR;
	}
	return LOG_OK;
}

static void
load_keycolumn(sql_trans *tr, sql_key *k, res_table *rt_keycols/*, oid rid*/)
{
	sql_kc *kc = ZNEW(sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;

	str v = (char*)store->table_api.table_fetch_value(rt_keycols, find_sql_column(objects, "name"));
	kc->c = find_sql_column(k->t, v);
	list_append(k->columns, kc);
	assert(kc->c);
}

static sql_key *
load_key(sql_trans *tr, sql_table *t, res_table *rt_keys, res_table *rt_keycols/*, oid rid*/)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *keys = find_sql_table(tr, syss, "keys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;

	key_type ktype = (key_type) *(int*) store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "type"));
	sql_key *nk = (ktype != fkey)?(sql_key*)ZNEW(sql_ukey):(sql_key*)ZNEW(sql_fkey);
	sqlid kid = *(sqlid*) store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "id"));
	str v = (char*) store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "name"));

	base_init(NULL, &nk->base, kid, 0, v);
	nk->type = ktype;
	nk->columns = list_create((fdestroy) &kc_destroy);
	nk->t = t;

	switch (ktype) {
	case ckey: {
		str ch = (char*)store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "check"));
		if (!strNil(ch))
			nk->check =_STRDUP(ch);
	}	break;
	case ukey:
	case unndkey:
	case pkey: {
		sql_ukey *uk = (sql_ukey *) nk;

		if (ktype == pkey)
			t->pkey = uk;
		break;
	}
	case fkey: {
		sql_fkey *fk = (sql_fkey *) nk;
		int action = *(int*)store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "action"));
		fk->on_delete = action & 255;
		fk->on_update = (action>>8) & 255;

		fk->rkey = *(sqlid*)store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "rkey"));
		break;
	}
	}

	for ( ; rt_keycols->cur_row < rt_keycols->nr_rows; rt_keycols->cur_row++) {
		sqlid nid = *(sqlid*)store->table_api.table_fetch_value(rt_keycols, find_sql_column(objects, "id"));
		if (nid != nk->base.id)
			break;
		load_keycolumn(tr, nk, rt_keycols);
	}

	/* find idx with same name */
	node *n = ol_find_name(t->idxs, nk->base.name);
	if (n) {
		nk->idx = (sql_idx*)n->data;
		nk->idx->key = nk;
	}
	return nk;
}

static void
load_idxcolumn(sql_trans *tr, sql_idx * i, res_table *rt_idxcols/*, oid rid*/)
{
	sql_kc *kc = ZNEW(sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;
	str v;

	v = (char*)store->table_api.table_fetch_value(rt_idxcols, find_sql_column(objects, "name"));
	kc->c = find_sql_column(i->t, v);
	assert(kc->c);
	list_append(i->columns, kc);
}

static sql_idx *
load_idx(sql_trans *tr, sql_table *t, res_table *rt_idx, res_table *rt_idxcols/*, oid rid*/)
{
	sql_idx *ni = ZNEW(sql_idx);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *idxs = find_sql_table(tr, syss, "idxs");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;

	sqlid iid = *(sqlid*)store->table_api.table_fetch_value(rt_idx, find_sql_column(idxs, "id"));
	str v = (char*)store->table_api.table_fetch_value(rt_idx, find_sql_column(idxs, "name"));
	base_init(NULL, &ni->base, iid, 0, v);
	ni->type = (idx_type) *(int*) store->table_api.table_fetch_value(rt_idx, find_sql_column(idxs, "type"));
	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->key = NULL;
	ATOMIC_PTR_INIT(&ni->data, NULL);

	if (isTable(ni->t) && idx_has_column(ni->type))
		store->storage_api.create_idx(tr, ni);

	for ( ; rt_idxcols->cur_row < rt_idxcols->nr_rows; rt_idxcols->cur_row++) {
		sqlid nid = *(sqlid*)store->table_api.table_fetch_value(rt_idxcols, find_sql_column(objects, "id"));
		if (nid != ni->base.id)
			break;
		load_idxcolumn(tr, ni, rt_idxcols);
	}
	return ni;
}

static void
load_triggercolumn(sql_trans *tr, sql_trigger * i, res_table *rt_triggercols)
{
	sql_kc *kc = ZNEW(sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;

	str v = (char*)store->table_api.table_fetch_value(rt_triggercols, find_sql_column(objects, "name"));
	kc->c = find_sql_column(i->t, v);
	list_append(i->columns, kc);
	assert(kc->c);
}

static sql_trigger *
load_trigger(sql_trans *tr, sql_table *t, res_table *rt_triggers, res_table *rt_triggercols)
{
	sql_trigger *nt = ZNEW(sql_trigger);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *triggers = find_sql_table(tr, syss, "triggers");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;

	sqlid tid = *(sqlid*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "id"));
	str v = (char*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "name"));
	base_init(NULL, &nt->base, tid, 0, v);

	nt->time = *(sht*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "time"));
	nt->orientation = *(sht*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "orientation"));
	nt->event = *(sht*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "event"));

	v = (char*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "old_name"));
	if (!strNil(v))
		nt->old_name =_STRDUP(v);
	v = (char*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "new_name"));
	if (!strNil(v))
		nt->new_name =_STRDUP(v);
	v = (char*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "condition"));
	if (!strNil(v))
		nt->condition =_STRDUP(v);
	v = (char*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "statement"));
	if (!strNil(v))
		nt->statement =_STRDUP(v);

	nt->t = t;
	if (t)
		nt->columns = list_create((fdestroy) &kc_destroy);

	if (rt_triggercols) {
		for ( ; rt_triggercols->cur_row < rt_triggercols->nr_rows; rt_triggercols->cur_row++) {
			sqlid nid = *(sqlid*)store->table_api.table_fetch_value(rt_triggercols, find_sql_column(objects, "id"));
			if (nid != nt->base.id)
				break;
			load_triggercolumn(tr, nt, rt_triggercols);
		}
	}
	return nt;
}

static sql_column *
load_column(sql_trans *tr, sql_table *t, res_table *rt_cols)
{
	sql_column *c = ZNEW(sql_column);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *columns = find_sql_table(tr, syss, "_columns");
	sqlstore *store = tr->store;
	str v, def, tpe, st;
	int sz, d;

	sqlid cid = *(sqlid*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "id"));
	v = (char*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "name"));
	base_init(NULL, &c->base, cid, 0, v);

	sz = *(int*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "type_digits"));
	d = *(int*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "type_scale"));
	tpe = (char*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "type"));
	if (tpe && strcmp(tpe, "clob") == 0)
		tpe = "varchar";
	if (!sql_find_subtype(&c->type, tpe, sz, d)) {
		sql_type *lt = sql_trans_bind_type(tr, t->s, tpe);
		if (lt == NULL) {
			TRC_ERROR(SQL_STORE, "SQL type '%s' is missing\n", tpe);
			return NULL;
		}
		sql_init_subtype(&c->type, lt, sz, d);
	}
	c->def = NULL;
	def = (char*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "default"));
	if (!strNil(def))
		c->def =_STRDUP(def);
	c->null = *(bit*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "null"));
	c->colnr = *(int*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "number"));
	c->unique = 0;
	c->storage_type = NULL;
	st = (char*)store->table_api.table_fetch_value(rt_cols, find_sql_column(columns, "storage"));
	if (!strNil(st))
		c->storage_type =_STRDUP(st);
	ATOMIC_PTR_INIT(&c->data, NULL);
	c->t = t;
	if (isTable(c->t))
		store->storage_api.create_col(tr, c);
	TRC_DEBUG(SQL_STORE, "Load column: %s\n", c->base.name);
	if (!c->null)
		store->storage_api.col_not_null(tr, c, !c->null);
	return c;
}

static int
col_set_range(sql_trans *tr, sql_part *pt, bool add_range)
{
		sql_table *t = find_sql_table_id(tr, pt->t->s /* schema of both member and merge tbale */, pt->member);
		sql_column *c = t? find_sql_column(t, pt->t->part.pcol->base.name):NULL;

		if (c) {
			sqlstore *store = tr->store;
			return store->storage_api.col_set_range(tr, c, pt, add_range);
		}
		return 0;
}

static int
load_range_partition(sql_trans *tr, sql_schema *syss, sql_part *pt)
{
	oid rid;
	rids *rs;
	sqlstore *store = tr->store;
	sql_table *ranges = find_sql_table(tr, syss, "range_partitions");
	assert(ranges);

	rs = store->table_api.rids_select(tr, find_sql_column(ranges, "table_id"), &pt->member, &pt->member, NULL);
	if (rs == NULL)
		return -1;
	if ((rid = store->table_api.rids_next(rs)) != oid_nil) {
		ptr cbat;
		const char *v;

		pt->with_nills = (bit) store->table_api.column_find_bte(tr, find_sql_column(ranges, "with_nulls"), rid);
		v = store->table_api.column_find_string_start(tr, find_sql_column(ranges, "minimum"), rid, &cbat);
		pt->part.range.minvalue =_STRDUP(v);
		pt->part.range.minlength = strLen(v);
		store->table_api.column_find_string_end(cbat);
		v = store->table_api.column_find_string_start(tr, find_sql_column(ranges, "maximum"), rid, &cbat);
		pt->part.range.maxvalue =_STRDUP(v);
		pt->part.range.maxlength = strLen(v);
		store->table_api.column_find_string_end(cbat);
	}
	store->table_api.rids_destroy(rs);
	return 0;
}

static int
load_value_partition(sql_trans *tr, sql_schema *syss, sql_part *pt)
{
	sqlstore *store = tr->store;
	list *vals = NULL;
	oid rid;
	sql_table *values = find_sql_table(tr, syss, "value_partitions");
	assert(values);

	rids *rs = store->table_api.rids_select(tr, find_sql_column(values, "table_id"), &pt->member, &pt->member, NULL);
	if (rs == NULL)
		return -1;

	if (!rs)
		return -1;
	vals = list_create((fdestroy) &part_value_destroy);
	if (!vals) {
		store->table_api.rids_destroy(rs);
		return -1;
	}

	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		ptr cbat;

		const char *v = store->table_api.column_find_string_start(tr, find_sql_column(values, "value"), rid, &cbat);
		if (strNil(v)) { /* check for null value */
			pt->with_nills = true;
		} else {
			sql_part_value *nextv = ZNEW(sql_part_value);
			nextv->value =_STRDUP(v);
			nextv->length = strLen(v);
			list_append(vals, nextv);
		}
		store->table_api.column_find_string_end(cbat);
	}
	store->table_api.rids_destroy(rs);
	pt->part.values = vals;
	return 0;
}

static sql_part*
load_part(sql_trans *tr, sql_table *mt, oid rid)
{
	sql_part *pt = ZNEW(sql_part);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlid id;
	sqlstore *store = tr->store;
	const char *v;
	ptr cbat;

	assert(isMergeTable(mt) || isReplicaTable(mt));
	id = store->table_api.column_find_sqlid(tr, find_sql_column(objects, "id"), rid);
	if (is_int_nil(id)) { /* upgrade case, the id it's not initialized */
		id = store_next_oid(store);
		store->table_api.column_update_value(tr, find_sql_column(objects, "id"), rid, &id);
	}
	v = store->table_api.column_find_string_start(tr, find_sql_column(objects, "name"), rid, &cbat);
	base_init(NULL, &pt->base, id, 0, v);
	store->table_api.column_find_string_end(cbat);
	pt->t = mt;
	pt->member = store->table_api.column_find_sqlid(tr, find_sql_column(objects, "sub"), rid);
	list_append(mt->members, pt);
	return pt;
}

void
sql_trans_update_tables(sql_trans* tr, sql_schema *s)
{
	(void)tr;
	(void)s;
}

sql_base *
dup_base(sql_base *b)
{
	ATOMIC_INC(&b->refcnt);
	return b;
}

static sql_table *
load_table(sql_trans *tr, sql_schema *s, res_table *rt_tables, res_table *rt_parts,
		res_table *rt_cols, res_table *rt_idx, res_table *rt_idxcols,
		res_table *rt_keys, res_table *rt_keycols,
		res_table *rt_triggers, res_table *rt_triggercols,
		sqlid tid)
{
	sqlstore *store = tr->store;
	sql_table *t = ZNEW(sql_table);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *tables = find_sql_table(tr, syss, "_tables");
	sql_table *cols = find_sql_table(tr, syss, "_columns");
	sql_table *idxs = find_sql_table(tr, syss, "idxs");
	sql_table *keys = find_sql_table(tr, syss, "keys");
	sql_table *triggers = find_sql_table(tr, syss, "triggers");
	sqlid pcolid = int_nil;
	str v, exp = NULL;

	sqlid ntid = *(sqlid*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "id"));
	assert(tid == ntid);
	v = (char*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "name"));
	base_init(NULL, &t->base, tid, 0, v);
	t->query = NULL;
	v = (char*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "query"));
	if (!strNil(v))
		t->query =_STRDUP(v);
	t->type = *(sht*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "type"));
	t->system = *(bit*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "system"));
	t->commit_action = (ca_t)*(sht*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "commit_action"));
	t->persistence = SQL_PERSIST;
	if (t->commit_action)
		t->persistence = SQL_GLOBAL_TEMP;
	if (isRemote(t))
		t->persistence = SQL_REMOTE;
	t->access = *(sht*)store->table_api.table_fetch_value(rt_tables, find_sql_column(tables, "access"));

	t->pkey = NULL;
	t->s = s;
	t->sz = COLSIZE;

	t->columns = ol_new(NULL, (destroy_fptr) &column_destroy, store);
	t->idxs = ol_new(NULL, (destroy_fptr) &idx_destroy, store);
	t->keys = ol_new(NULL, (destroy_fptr) &key_destroy, store);
	t->triggers = ol_new(NULL, (destroy_fptr) &trigger_destroy, store);
	if (isMergeTable(t) || isReplicaTable(t))
		t->members = list_create((fdestroy) &part_destroy);
	ATOMIC_PTR_INIT(&t->data, NULL);

	if (isTable(t)) {
		if (store->storage_api.create_del(tr, t) != LOG_OK) {
			TRC_ERROR(SQL_STORE, "Load table '%s' is missing 'deletes'", t->base.name);
			return NULL;
		}
	}

	TRC_DEBUG(SQL_STORE, "Load table: %s\n", t->base.name);

	sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
	assert(partitions);

	if (rt_parts->cur_row < rt_parts->nr_rows) {
		ntid = *(sqlid*)store->table_api.table_fetch_value(rt_parts, find_sql_column(partitions, "table_id"));
		if (ntid == tid) {
			t->properties |= *(bte*)store->table_api.table_fetch_value(rt_parts, find_sql_column(partitions, "type"));

			if (isPartitionedByColumnTable(t)) {
				pcolid = *(sqlid*)store->table_api.table_fetch_value(rt_parts, find_sql_column(partitions, "column_id"));
			} else {
				v = (char*)store->table_api.table_fetch_value(rt_parts, find_sql_column(partitions, "expression"));
				assert(!strNil(v));
				exp =_STRDUP(v);
			}
			rt_parts->cur_row++;
		}
	}

	assert((!isRangePartitionTable(t) && !isListPartitionTable(t)) || (!exp && !is_int_nil(pcolid)) || (exp && is_int_nil(pcolid)));
	if (isPartitionedByExpressionTable(t)) {
		t->part.pexp = ZNEW(sql_expression);
		t->part.pexp->exp = exp;
		t->part.pexp->type = *sql_fetch_localtype(TYPE_void); /* initialized at initialize_sql_parts */
		t->part.pexp->cols = list_create((fdestroy) &int_destroy);
	}
	for ( ; rt_cols->cur_row < rt_cols->nr_rows; rt_cols->cur_row++) {
		ntid = *(sqlid*)store->table_api.table_fetch_value(rt_cols, find_sql_column(cols, "table_id"));
		while (instore(ntid)) {
			rt_cols->cur_row++;
			ntid = *(sqlid*)store->table_api.table_fetch_value(rt_cols, find_sql_column(cols, "table_id"));
		}

		if (ntid != t->base.id)
			break;
		sql_column* next = load_column(tr, t, rt_cols);
		if (next == NULL) {
			table_destroy(store, t);
			return NULL;
		}
		if (ol_add(t->columns, &next->base)) {
			table_destroy(store, t);
			return NULL;
		}
		if (pcolid == next->base.id)
			t->part.pcol = next;
	}

	if (!isKindOfTable(t))
		return t;

	/* load idx's first as the may be needed by the keys */
	for ( ; rt_idx->cur_row < rt_idx->nr_rows; rt_idx->cur_row++) {
		ntid = *(sqlid*)store->table_api.table_fetch_value(rt_idx, find_sql_column(idxs, "table_id"));

		if (ntid != t->base.id)
			break;
		sql_idx *i = load_idx(tr, t, rt_idx, rt_idxcols);

		if (!i || ol_add(t->idxs, &i->base) ||
		    os_add(s->idxs, tr, i->base.name, dup_base(&i->base))) {
			table_destroy(store, t);
			return NULL;
		}
	}

	for ( ; rt_keys->cur_row < rt_keys->nr_rows; rt_keys->cur_row++) {
		ntid = *(sqlid*)store->table_api.table_fetch_value(rt_keys, find_sql_column(keys, "table_id"));

		if (ntid != t->base.id)
			break;
		sql_key *k = load_key(tr, t, rt_keys, rt_keycols);

		if (!k || ol_add(t->keys, &k->base) ||
		    os_add(s->keys, tr, k->base.name, dup_base(&k->base)) ||
			(isGlobal(t) && os_add(tr->cat->objects, tr, k->base.name, dup_base(&k->base)))) {
			table_destroy(store, t);
			return NULL;
		}
	}

	/* after loading keys and idxs, update properties derived from indexes that require keys */
	if (ol_length(t->idxs))
		for (node *n = ol_first_node(t->idxs); n; n = n->next)
			create_sql_idx_done(tr, n->data);

	for ( ; rt_triggers->cur_row < rt_triggers->nr_rows; rt_triggers->cur_row++) {
		ntid = *(sqlid*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(triggers, "table_id"));

		if (ntid < t->base.id && instore(ntid)) /* skip triggers on system tables ugh */
			continue;
		if (ntid != t->base.id)
			break;
		sql_trigger *k = load_trigger(tr, t, rt_triggers, rt_triggercols);

		if (!k || ol_add(t->triggers, &k->base) ||
		    os_add(s->triggers, tr, k->base.name, dup_base(&k->base))) {
			table_destroy(store, t);
			return NULL;
		}
	}
	return t;
}

static sql_type *
load_type(sql_trans *tr, sql_schema *s, oid rid)
{
	sqlstore *store = tr->store;
	sql_type *t = ZNEW(sql_type);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *types = find_sql_table(tr, syss, "types");
	sqlid tid;
	const char *v;
	ptr cbat;

	tid = store->table_api.column_find_sqlid(tr, find_sql_column(types, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(types, "sqlname"), rid, &cbat);
	base_init(NULL, &t->base, tid, 0, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(types, "systemname"), rid, &cbat);
	t->impl =_STRDUP(v);
	store->table_api.column_find_string_end(cbat);
	t->digits = store->table_api.column_find_int(tr, find_sql_column(types, "digits"), rid);
	t->scale = store->table_api.column_find_int(tr, find_sql_column(types, "scale"), rid);
	t->radix = store->table_api.column_find_int(tr, find_sql_column(types, "radix"), rid);
	t->eclass = (sql_class)store->table_api.column_find_int(tr, find_sql_column(types, "eclass"), rid);
	t->localtype = ATOMindex(t->impl);
	t->s = s;
	return t;
}

static sql_arg *
load_arg(sql_trans *tr, sql_func *f, oid rid)
{
	sqlstore *store = tr->store;
	sql_arg *a = ZNEW(sql_arg);
	unsigned int digits, scale;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *args = find_sql_table(tr, syss, "args");
	const char *v, *tpe;
	ptr cbat;

	v = store->table_api.column_find_string_start(tr, find_sql_column(args, "name"), rid, &cbat);
	a->name =_STRDUP(v);
	store->table_api.column_find_string_end(cbat);
	a->inout = store->table_api.column_find_bte(tr, find_sql_column(args, "inout"), rid);
	digits = store->table_api.column_find_int(tr, find_sql_column(args, "type_digits"), rid);
	scale = store->table_api.column_find_int(tr, find_sql_column(args, "type_scale"), rid);

	tpe = store->table_api.column_find_string_start(tr, find_sql_column(args, "type"), rid, &cbat);
	if (tpe && strcmp(tpe, "clob") == 0)
		tpe = "varchar";
	if (!sql_find_subtype(&a->type, tpe, digits, scale)) {
		sql_type *lt = sql_trans_bind_type(tr, f->s, tpe);
		if (lt == NULL) {
			TRC_ERROR(SQL_STORE, "SQL type '%s' is missing\n", tpe);
			store->table_api.column_find_string_end(cbat);
			return NULL;
		}
		sql_init_subtype(&a->type, lt, digits, scale);
	}
	store->table_api.column_find_string_end(cbat);
	return a;
}

static sql_func *
load_func(sql_trans *tr, sql_schema *s, sqlid fid, subrids *rs)
{
	sqlstore *store = tr->store;
	sql_func *t = ZNEW(sql_func);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *funcs = find_sql_table(tr, syss, "functions");
	oid rid;
	bool update_env;	/* hacky way to update env function */
	const char *v;
	ptr cbat;

	rid = store->table_api.column_find_row(tr, find_sql_column(funcs, "id"), &fid, NULL);
	v = store->table_api.column_find_string_start(tr, find_sql_column(funcs, "name"), rid, &cbat);
	update_env = strcmp(v, "env") == 0;
	base_init(NULL, &t->base, fid, 0, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(funcs, "func"), rid, &cbat);
	update_env = update_env && strstr(v, "EXTERNAL NAME sql.sql_environment") != NULL;
	if (update_env) {
		/* see creation of env in sql_create_env()
		 * also see upgrade code in sql_upgrades.c */
		v = "CREATE FUNCTION env() RETURNS TABLE( name varchar(1024), value varchar(2048)) EXTERNAL NAME inspect.\"getEnvironment\";";
	}
	t->imp =_STRDUP(v);
	store->table_api.column_find_string_end(cbat);
	if (update_env) {
		v = "inspect";
	} else {
		v = store->table_api.column_find_string_start(tr, find_sql_column(funcs, "mod"), rid, &cbat);
	}
	if (strcmp(v, "pyapi") == 0 ||	 /* pyapi module no longer used */
		strcmp(v, "pyapi3map") == 0) /* pyapi3map module no longer used */
		t->mod =_STRDUP("pypapi3");
	else
		t->mod =_STRDUP(v);
	if (!update_env)
		store->table_api.column_find_string_end(cbat);
	t->lang = (sql_flang) store->table_api.column_find_int(tr, find_sql_column(funcs, "language"), rid);
	t->instantiated = t->lang != FUNC_LANG_SQL && t->lang != FUNC_LANG_MAL;
	t->type = (sql_ftype) store->table_api.column_find_int(tr, find_sql_column(funcs, "type"), rid);
	t->side_effect = (bool) store->table_api.column_find_bte(tr, find_sql_column(funcs, "side_effect"), rid);
	t->varres = (bool) store->table_api.column_find_bte(tr, find_sql_column(funcs, "varres"), rid);
	t->vararg = (bool) store->table_api.column_find_bte(tr, find_sql_column(funcs, "vararg"), rid);
	t->system = (bool) store->table_api.column_find_bte(tr, find_sql_column(funcs, "system"), rid);
	t->semantics = (bool) store->table_api.column_find_bte(tr, find_sql_column(funcs, "semantics"), rid);
	bte order_spec = (bte) store->table_api.column_find_bte(tr, find_sql_column(funcs, "order_specification"), rid);
	t->res = NULL;
	t->s = s;
	t->fix_scale = SCALE_EQ;
	t->sa = NULL;
	if (t->lang != FUNC_LANG_INT) {
		t->query = t->imp;
		t->imp = NULL;
	}
	/* convert old PYTHON2 and PYTHON2_MAP to PYTHON and PYTHON_MAP
	 * see also function sql_update_jun2020() in sql_upgrades.c */
	if ((int) t->lang == 7 || (int) t->lang == 8)		/* MAP_PY old FUNC_LANG_PY2 */
		t->lang = FUNC_LANG_PY;
	else if ((int) t->lang == 9 || (int) t->lang == 11)	/* old FUNC_LANG_MAP_PY2 or MAP_PY3 */
		t->lang = FUNC_LANG_PY;
	if (LANG_EXT(t->lang)) { /* instantiate functions other than sql and mal */
		switch(t->type) {
		case F_AGGR:
			t->imp =_STRDUP("eval_aggr");
			break;
		case F_LOADER:
			t->imp =_STRDUP("eval_loader");
			break;
		default: /* for every other function type at the moment */
			t->imp =_STRDUP("eval");
		}
	}
	if (order_spec == 2)
		t->order_required = true;
	if (order_spec == 1)
		t->opt_order = true;

	TRC_DEBUG(SQL_STORE, "Load function: %s\n", t->base.name);

	t->ops = list_create((fdestroy) &arg_destroy);
	if (rs) {
		for (rid = store->table_api.subrids_next(rs); !is_oid_nil(rid); rid = store->table_api.subrids_next(rs)) {
			sql_arg *a = load_arg(tr, t, rid);

			if (a == NULL)
				return NULL;
			if (a->inout == ARG_OUT) {
				if (!t->res)
					t->res = list_create((fdestroy) &arg_destroy);
				list_append(t->res, a);
			} else {
				list_append(t->ops, a);
			}
		}
	}
	return t;
}

static sql_sequence *
load_seq(sql_trans *tr, sql_schema * s, oid rid)
{
	sqlstore *store = tr->store;
	sql_sequence *seq = ZNEW(sql_sequence);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(tr, syss, "sequences");
	sqlid sid;
	const char *v;
	ptr cbat;

	sid = store->table_api.column_find_sqlid(tr, find_sql_column(seqs, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(seqs, "name"), rid, &cbat);
	base_init(NULL, &seq->base, sid, 0, v);
	store->table_api.column_find_string_end(cbat);
	seq->start = store->table_api.column_find_lng(tr, find_sql_column(seqs, "start"), rid);
	seq->minvalue = store->table_api.column_find_lng(tr, find_sql_column(seqs, "minvalue"), rid);
	seq->maxvalue = store->table_api.column_find_lng(tr, find_sql_column(seqs, "maxvalue"), rid);
	seq->increment = store->table_api.column_find_lng(tr, find_sql_column(seqs, "increment"), rid);
	seq->cacheinc = store->table_api.column_find_lng(tr, find_sql_column(seqs, "cacheinc"), rid);
	seq->cycle = (bit) store->table_api.column_find_bte(tr, find_sql_column(seqs, "cycle"), rid);
	seq->s = s;
	return seq;
}

static void
sql_trans_update_schema(sql_trans *tr, oid rid)
{
	sqlstore *store = tr->store;
	sql_schema *s = NULL, *syss = find_sql_schema(tr, "sys");
	sql_table *ss = find_sql_table(tr, syss, "schemas");
	sqlid sid;
	str v;

	sid = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "id"), rid);
	s = find_sql_schema_id(tr, sid);

	if (s==NULL)
		return ;

	TRC_DEBUG(SQL_STORE, "Update schema: %s %d\n", s->base.name, s->base.id);

	v = store->table_api.column_find_value(tr, find_sql_column(ss, "name"), rid);
	if (!v)
		return; /* TODO do better than this */
	_DELETE(s->base.name);
	base_init(NULL, &s->base, sid, 0, v);
	_DELETE(v);
	s->auth_id = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "authorization"), rid);
	s->system = (bit) store->table_api.column_find_bte(tr, find_sql_column(ss, "system"), rid);
	s->owner = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "owner"), rid);
}

static sql_schema *
load_schema(sql_trans *tr, res_table *rt_schemas, res_table *rt_tables, res_table *rt_parts,
		    res_table *rt_cols, res_table *rt_idx, res_table *rt_idxcols,
		    res_table *rt_keys, res_table *rt_keycols,
		    res_table *rt_triggers, res_table *rt_triggercols)
{
	sqlstore *store = tr->store;
	sql_schema *s = NULL, *syss = find_sql_schema(tr, "sys");
	sql_table *ss = find_sql_table(tr, syss, "schemas");
	sql_table *types = find_sql_table(tr, syss, "types");
	sql_table *tables = find_sql_table(tr, syss, "_tables");
	sql_table *funcs = find_sql_table(tr, syss, "functions");
	sql_table *seqs = find_sql_table(tr, syss, "sequences");
	sql_column *type_schema, *type_id, *table_schema, *table_id;
	sql_column *func_schema, *func_id, *seq_schema, *seq_id;
	rids *rs;

	sqlid sid = *(sqlid*)store->table_api.table_fetch_value(rt_schemas, find_sql_column(ss, "id"));
	if (instore(sid)) {
		s = find_sql_schema_id(tr, sid);

		if (s==NULL) {
			char *name = (char*)store->table_api.table_fetch_value(rt_schemas, find_sql_column(ss, "name"));
			s = find_sql_schema(tr, name);
			if (s == NULL) {
				GDKerror("SQL schema missing or incompatible, rebuild from archive");
				return NULL;
			}
		}
		s->base.id = sid;
	} else {
		s = ZNEW(sql_schema);
		if (s == NULL)
			return NULL;
		char *name = (char*)store->table_api.table_fetch_value(rt_schemas, find_sql_column(ss, "name"));
		base_init(NULL, &s->base, sid, 0, name);
		s->auth_id = *(sqlid*)store->table_api.table_fetch_value(rt_schemas, find_sql_column(ss, "authorization"));
		s->system = *(bte*)store->table_api.table_fetch_value(rt_schemas, find_sql_column(ss, "system"));
		s->owner = *(sqlid*)store->table_api.table_fetch_value(rt_schemas, find_sql_column(ss, "owner"));

		s->tables = os_new(NULL, (destroy_fptr) &table_destroy, false, true, true, false, store);
		s->types = os_new(NULL, (destroy_fptr) &type_destroy, false, true, true, false, store);
		s->funcs = os_new(NULL, (destroy_fptr) &func_destroy, false, false, false, false, store);
		s->seqs = os_new(NULL, (destroy_fptr) &seq_destroy, false, true, true, false, store);
		s->keys = os_new(NULL, (destroy_fptr) &key_destroy, false, true, true, false, store);
		s->idxs = os_new(NULL, (destroy_fptr) &idx_destroy, false, true, true, false, store);
		s->triggers = os_new(NULL, (destroy_fptr) &trigger_destroy, false, true, true, false, store);
		s->parts = os_new(NULL, (destroy_fptr) &part_destroy, false, false, true, false, store);
	}

	TRC_DEBUG(SQL_STORE, "Load schema: %s %d\n", s->base.name, s->base.id);

	sqlid tmpid = FUNC_OIDS;

	/* first load simple types */
	type_schema = find_sql_column(types, "schema_id");
	type_id = find_sql_column(types, "id");
	rs = store->table_api.rids_select(tr, type_schema, &s->base.id, &s->base.id, type_id, &tmpid, NULL, NULL);
	if (!rs) {
		schema_destroy(store, s);
		return NULL;
	}
	for (oid rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_type *t = load_type(tr, s, rid);
		if (os_add(s->types, tr, t->base.name, &t->base)) {
			schema_destroy(store, s);
			store->table_api.rids_destroy(rs);
			return NULL;
		}
	}
	store->table_api.rids_destroy(rs);

	/* second tables */
	table_schema = find_sql_column(tables, "schema_id");
	table_id = find_sql_column(tables, "id");
	for ( ; rt_tables->cur_row < rt_tables->nr_rows; rt_tables->cur_row++) {
		sqlid nsid = *(sqlid*)store->table_api.table_fetch_value(rt_tables, table_schema);
		sqlid tid = *(sqlid*)store->table_api.table_fetch_value(rt_tables, table_id);
		if (nsid > s->base.id)
			break;
		if (nsid < s->base.id)
			assert(0);
		if (!instore(tid)) {
			sql_table *t = load_table(tr, s, rt_tables, rt_parts,
					rt_cols, rt_idx, rt_idxcols, rt_keys, rt_keycols, rt_triggers, rt_triggercols, tid);
			if (t == NULL && store->debug&8) /* try to continue without this table */
				continue;
			if (t == NULL) {
				schema_destroy(store, s);
				return NULL;
			}
			if (os_add(s->tables, tr, t->base.name, &t->base)) {
				schema_destroy(store, s);
				return NULL;
			}
		}
	}

	/* next functions which could use these types */
	func_schema = find_sql_column(funcs, "schema_id");
	func_id = find_sql_column(funcs, "id");
	rs = store->table_api.rids_select(tr, func_schema, &s->base.id, &s->base.id, func_id, &tmpid, NULL, NULL);
	if (rs == NULL) {
		schema_destroy(store, s);
		return NULL;
	}
	if (!store->table_api.rids_empty(rs)) {
		sql_table *args = find_sql_table(tr, syss, "args");
		sql_column *arg_func_id = find_sql_column(args, "func_id");
		sql_column *arg_number = find_sql_column(args, "number");
		subrids *nrs = store->table_api.subrids_create(tr, rs, func_id, arg_func_id, arg_number);
		if (!nrs) {
			store->table_api.rids_destroy(rs);
			schema_destroy(store, s);
			return NULL;
		}
		sqlid fid;
		sql_func *f;

		for (fid = store->table_api.subrids_nextid(nrs); fid >= 0; fid = store->table_api.subrids_nextid(nrs)) {
			f = load_func(tr, s, fid, nrs);
			if (f == NULL) {
				store->table_api.subrids_destroy(nrs);
				store->table_api.rids_destroy(rs);
				schema_destroy(store, s);
				return NULL;
			}
			if (os_add(s->funcs, tr, f->base.name, &f->base)) {
				schema_destroy(store, s);
				return NULL;
			}
		}
		/* Handle all procedures without arguments (no args) */
		rs = store->table_api.rids_diff(tr, rs, func_id, nrs, arg_func_id);
		if (!rs) {
			store->table_api.subrids_destroy(nrs);
			schema_destroy(store, s);
			return NULL;
		}
		for (oid rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
			fid = store->table_api.column_find_sqlid(tr, func_id, rid);
			f = load_func(tr, s, fid, NULL);
			if (f == NULL) {
				store->table_api.subrids_destroy(nrs);
				store->table_api.rids_destroy(rs);
				schema_destroy(store, s);
				return NULL;
			}
			if (os_add(s->funcs, tr, f->base.name, &f->base)) {
				store->table_api.subrids_destroy(nrs);
				store->table_api.rids_destroy(rs);
				schema_destroy(store, s);
				return NULL;
			}
		}
		store->table_api.subrids_destroy(nrs);
	}
	store->table_api.rids_destroy(rs);

	/* last sequence numbers */
	seq_schema = find_sql_column(seqs, "schema_id");
	seq_id = find_sql_column(seqs, "id");
	rs = store->table_api.rids_select(tr, seq_schema, &s->base.id, &s->base.id, seq_id, &tmpid, NULL, NULL);
	if (rs == NULL) {
		schema_destroy(store, s);
		return NULL;
	}
	for (oid rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_sequence *seq = load_seq(tr, s, rid);
		if (os_add(s->seqs, tr, seq->base.name, &seq->base)) {
			schema_destroy(store, s);
			store->table_api.rids_destroy(rs);
			return NULL;
		}
	}
	store->table_api.rids_destroy(rs);

	struct os_iter oi;
	os_iterator(&oi, s->tables, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_table *t = (sql_table*)b;
		if (isMergeTable(t) || isReplicaTable(t)) {
			sql_table *objects = find_sql_table(tr, syss, "objects");
			sql_column *mt_nr = find_sql_column(objects, "nr");
			sql_column *mt_sub = find_sql_column(objects, "sub");
			rids *rs = store->table_api.rids_select(tr, mt_nr, &t->base.id, &t->base.id, NULL);
			if (rs == NULL) {
				schema_destroy(store, s);
				return NULL;
			}
			rs = store->table_api.rids_orderby(tr, rs, mt_sub);
			if (rs == NULL) {
				schema_destroy(store, s);
				return NULL;
			}
			for (oid rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
				sql_part *pt = load_part(tr, t, rid);
				if (isRangePartitionTable(t)) {
					if (load_range_partition(tr, syss, pt) < 0) {
						schema_destroy(store, s);
						return NULL;
					}
				} else if (isListPartitionTable(t)) {
					load_value_partition(tr, syss, pt);
				}
				if (os_add(s->parts, tr, pt->base.name, dup_base(&pt->base))) {
					schema_destroy(store, s);
					store->table_api.rids_destroy(rs);
					return NULL;
				}
			}
			store->table_api.rids_destroy(rs);
		}
	}
	return s;
}

int
sql_trans_update_schemas(sql_trans* tr)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(tr, syss, "schemas");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	rids *schemas = store->table_api.rids_select(tr, sysschema_ids, NULL, NULL);
	oid rid;

	if (schemas == NULL)
		return -1;
	TRC_DEBUG(SQL_STORE, "Update schemas\n");

	for (rid = store->table_api.rids_next(schemas); !is_oid_nil(rid); rid = store->table_api.rids_next(schemas)) {
		sql_trans_update_schema(tr, rid);
	}
	store->table_api.rids_destroy(schemas);
	return 0;
}

static bool
load_trans(sql_trans* tr)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(tr, syss, "schemas");
	sql_table *systables = find_sql_table(tr, syss, "_tables");
	sql_table *sysparts = find_sql_table(tr, syss, "table_partitions");
	sql_table *syscols = find_sql_table(tr, syss, "_columns");
	sql_table *sysidx = find_sql_table(tr, syss, "idxs");
	sql_table *syskeys = find_sql_table(tr, syss, "keys");
	sql_table *systriggers = find_sql_table(tr, syss, "triggers");
	sql_table *sysobjects = find_sql_table(tr, syss, "objects");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	bool ok = true;

	TRC_DEBUG(SQL_STORE, "Load transaction\n");
	assert(sysschema);
	assert(systables);
	assert(sysparts);
	assert(syscols);
	assert(sysidx);
	assert(syskeys);
	assert(systriggers);
	assert(sysobjects);
	assert(sysschema_ids);

	res_table *rt_schemas = store->table_api.table_orderby(tr, sysschema, NULL, NULL, NULL, NULL, sysschema_ids, NULL);
	res_table *rt_tables = store->table_api.table_orderby(tr, systables, NULL, NULL, NULL, NULL,
			find_sql_column(systables, "schema_id"),
			find_sql_column(systables, "id"), NULL);
	res_table *rt_parts = store->table_api.table_orderby(tr, sysparts,
			find_sql_column(sysparts, "table_id"),
			find_sql_column(systables, "id"),
			NULL, NULL,
			find_sql_column(systables, "schema_id"), /* order also on schema_id */
			find_sql_column(sysparts, "table_id"), NULL);
	res_table *rt_cols = store->table_api.table_orderby(tr, syscols,
			find_sql_column(syscols, "table_id"),
			find_sql_column(systables, "id"),
			NULL, NULL,
			find_sql_column(systables, "schema_id"), /* order also on schema_id */
			find_sql_column(syscols, "table_id"),
			find_sql_column(syscols, "number"), NULL);

	res_table *rt_idx = store->table_api.table_orderby(tr, sysidx,
			find_sql_column(sysidx, "table_id"),
			find_sql_column(systables, "id"),
			NULL, NULL,
			find_sql_column(systables, "schema_id"), /* order also on schema_id */
			find_sql_column(sysidx, "table_id"),
			find_sql_column(sysidx, "id"), NULL);
	res_table *rt_idxcols = store->table_api.table_orderby(tr, sysobjects,
			find_sql_column(sysobjects, "id"),
			find_sql_column(sysidx, "id"),
			find_sql_column(sysidx, "table_id"),
			find_sql_column(systables, "id"),
			find_sql_column(systables, "schema_id"), /* order also on schema_id, table_id */
			find_sql_column(sysidx, "table_id"),
			find_sql_column(sysobjects, "id"),
			find_sql_column(sysobjects, "nr"), NULL);
	res_table *rt_keys = store->table_api.table_orderby(tr, syskeys,
			find_sql_column(syskeys, "table_id"),
			find_sql_column(systables, "id"),
			NULL, NULL,
			find_sql_column(systables, "schema_id"), /* order also on schema_id */
			find_sql_column(syskeys, "table_id"),
			find_sql_column(syskeys, "id"), NULL);
	res_table *rt_keycols = store->table_api.table_orderby(tr, sysobjects,
			find_sql_column(sysobjects, "id"),
			find_sql_column(syskeys, "id"),
			find_sql_column(syskeys, "table_id"),
			find_sql_column(systables, "id"),
			find_sql_column(systables, "schema_id"), /* order also on schema_id, table_id */
			find_sql_column(syskeys, "table_id"),
			find_sql_column(sysobjects, "id"),
			find_sql_column(sysobjects, "nr"), NULL);
	res_table *rt_tabletriggers = store->table_api.table_orderby(tr, systriggers,
			find_sql_column(systriggers, "table_id"),
			find_sql_column(systables, "id"),
			NULL, NULL,
			find_sql_column(systables, "schema_id"), /* order also on schema_id */
			find_sql_column(systriggers, "table_id"),
			find_sql_column(systriggers, "id"), NULL);
	res_table *rt_triggercols = store->table_api.table_orderby(tr, sysobjects,
			find_sql_column(sysobjects, "id"),
			find_sql_column(systriggers, "id"),
			find_sql_column(systriggers, "table_id"),
			find_sql_column(systables, "id"),
			find_sql_column(systables, "schema_id"), /* order also on schema_id, table_id */
			find_sql_column(systriggers, "table_id"),
			find_sql_column(sysobjects, "id"),
			find_sql_column(sysobjects, "nr"), NULL);
	res_table *rt_triggers = store->table_api.table_orderby(tr, systriggers,
			NULL, NULL, NULL, NULL,
			find_sql_column(systriggers, "id"),
			find_sql_column(systriggers, "table_id"), NULL);
	for ( ; rt_schemas->cur_row < rt_schemas->nr_rows; rt_schemas->cur_row++) {
		sql_schema *ns = load_schema(tr, rt_schemas, rt_tables, rt_parts,
				rt_cols, rt_idx, rt_idxcols, rt_keys, rt_keycols, rt_tabletriggers, rt_triggercols);
		if (ns == NULL) {
			ok = false;
			goto finish;
		}
		if (!instore(ns->base.id)) {
			if (os_add(tr->cat->schemas, tr, ns->base.name, &ns->base)) {
				ok = false;
				goto finish;
			}
			if (isTempSchema(ns))
				tr->tmp = ns;
		}
	}
	if (rt_triggers) {
		for ( ; rt_triggers->cur_row < rt_triggers->nr_rows; rt_triggers->cur_row++) {
			sqlid ntid = *(sqlid*)store->table_api.table_fetch_value(rt_triggers, find_sql_column(systriggers, "table_id"));
			if (ntid != int_nil)
				continue;
			sql_trigger *k = load_trigger(tr, NULL, rt_triggers, NULL);

			if (!k || os_add(syss->triggers, tr, k->base.name, &k->base)) {
				ok = false;
				goto finish;
			}
		}
	}

finish:
	store->table_api.table_result_destroy(rt_schemas);
	store->table_api.table_result_destroy(rt_tables);
	store->table_api.table_result_destroy(rt_parts);
	store->table_api.table_result_destroy(rt_cols);
	store->table_api.table_result_destroy(rt_idx);
	store->table_api.table_result_destroy(rt_idxcols);
	store->table_api.table_result_destroy(rt_keys);
	store->table_api.table_result_destroy(rt_keycols);
	store->table_api.table_result_destroy(rt_tabletriggers);
	store->table_api.table_result_destroy(rt_triggercols);
	store->table_api.table_result_destroy(rt_triggers);
	return ok;
}

static sqlid
next_oid(sqlstore *store)
{
	sqlid id = 0;
	id = (sqlid) ATOMIC_ADD(&store->obj_id, 1);
	assert(id < 2000000000);
	return id;
}

sqlid
store_next_oid(sqlstore *store)
{
	return next_oid(store);
}

static int
insert_schemas(sql_trans *tr)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(tr, syss, "schemas");
	sql_table *systable = find_sql_table(tr, syss, "_tables");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
	node *o;

	struct os_iter si;
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for (sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *s = (sql_schema*)b;
		char *strnil = (char*)ATOMnilptr(TYPE_str);

		if (isDeclaredSchema(s))
			continue;
		if ((res = store->table_api.table_insert(tr, sysschema, &s->base.id, &s->base.name, &s->auth_id, &s->owner, &s->system)))
			return res;
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table*)b;
			sht ca = t->commit_action;

			if ((res = store->table_api.table_insert(tr, systable, &t->base.id, &t->base.name, &s->base.id, &strnil, &t->type, &t->system, &ca, &t->access)))
				return res;
			for (o = t->columns->l->h; o; o = o->next) {
				sql_column *c = o->data;

				if ((res = store->table_api.table_insert(tr, syscolumn, &c->base.id, &c->base.name, &c->type.type->base.name, &c->type.digits, &c->type.scale,
										&t->base.id, (c->def) ? &c->def : &strnil, &c->null, &c->colnr, (c->storage_type)? &c->storage_type : &strnil)))
					return res;
			}
		}
	}
	return res;
}

static int
insert_types(sql_trans *tr, sql_table *systype)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;
	for (node *n = types->h; n; n = n->next) {
		sql_type *t = n->data;
		int radix = t->radix, eclass = (int) t->eclass;
		sqlid next_schema = t->s ? t->s->base.id : 0;

		if ((res = store->table_api.table_insert(tr, systype, &t->base.id, &t->impl, &t->base.name, &t->digits, &t->scale, &radix, &eclass, &next_schema)))
			return res;
	}
	return res;
}

static int
insert_args(sql_trans *tr, sql_table *sysarg, list *args, sqlid funcid, const char *arg_def, int *number)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;
	for (node *n = args->h; n; n = n->next) {
		sql_arg *a = n->data;
		sqlid id = next_oid(tr->store);
		int next_number = (*number)++;
		char buf[32], *next_name;

		if (a->name) {
			next_name = a->name;
		} else {
			snprintf(buf, sizeof(buf), arg_def, next_number);
			next_name = buf;
		}
		if ((res = store->table_api.table_insert(tr, sysarg, &id, &funcid, &next_name, &a->type.type->base.name, &a->type.digits, &a->type.scale, &a->inout, &next_number)))
			return res;
	}
	return res;
}

static int
insert_functions(sql_trans *tr, sql_table *sysfunc, list *funcs_list, sql_table *sysarg)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;
	for (node *n = funcs_list->h; n; n = n->next) {
		sql_func *f = n->data;
		int number = 0, ftype = (int) f->type, flang = (int) FUNC_LANG_INT;
		sqlid next_schema = f->s ? f->s->base.id : 0;
		bit se = f->side_effect, vares = f->varres, varg = f->vararg, system = f->system, sem = f->semantics;
		bte order = f->order_required?2:f->opt_order?1:0;

		if (f->private) /* don't serialize private functions because they cannot be seen by users */
			continue;
		if ((res = store->table_api.table_insert(tr, sysfunc, &f->base.id, &f->base.name, &f->imp, &f->mod, &flang, &ftype, &se, &vares, &varg, &next_schema, &system, &sem, &order)))
			return res;
		if (f->res && (res = insert_args(tr, sysarg, f->res, f->base.id, "res_%d", &number)))
			return res;
		if (f->ops && (res = insert_args(tr, sysarg, f->ops, f->base.id, "arg_%d", &number)))
			return res;
	}
	return res;
}

static int
table_next_column_nr(sql_table *t)
{
	int nr = ol_length(t->columns);
	if (nr) {
		node *n = ol_last_node(t->columns);
		if (n) {
			sql_column *c = n->data;

			nr = c->colnr+1;
		}
	}
	return nr;
}

static sql_column *
bootstrap_create_column(sql_trans *tr, sql_table *t, const char *name, sqlid id, const char *sqltype, unsigned int digits)
{
	sqlstore *store = tr->store;
	sql_column *col = ZNEW(sql_column);

	if ((sqlid) ATOMIC_GET(&store->obj_id) <= id)
		ATOMIC_SET(&store->obj_id, id + 1);
	TRC_DEBUG(SQL_STORE, "Create column: %s\n", name);

	base_init(NULL, &col->base, id, t->base.new, name);
	assert(col->base.id > 0);
	sql_find_subtype(&col->type, sqltype, digits, 0);
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;
	if (ol_add(t->columns, &col->base))
		return NULL;

	ATOMIC_PTR_INIT(&col->data, NULL);
	if (isTable(col->t))
		store->storage_api.create_col(tr, col);
	return col;
}

static sql_table *
create_sql_table_with_id(allocator *sa, sqlid id, const char *name, sht type, bit system, int persistence, int commit_action, bte properties)
{
	sql_table *t = SA_ZNEW(sa, sql_table);

	assert((persistence==SQL_PERSIST ||
		persistence==SQL_DECLARED_TABLE ||
		commit_action || type) && commit_action>=0);
	assert(id);
	base_init(sa, &t->base, id, true, name);
	t->type = type;
	t->system = system;
	t->persistence = (temp_t)persistence;
	t->commit_action = (ca_t)commit_action;
	t->query = NULL;
	t->access = 0;
	t->columns = ol_new(sa, (destroy_fptr) &column_destroy, NULL);
	t->idxs = ol_new(sa, (destroy_fptr) &idx_destroy, NULL);
	t->keys = ol_new(sa, (destroy_fptr) &key_destroy, NULL);
	t->triggers = ol_new(sa, (destroy_fptr) &trigger_destroy, NULL);
	if (isMergeTable(t) || isReplicaTable(t))
		t->members = list_new(sa, (fdestroy) &part_destroy);
	t->pkey = NULL;
	t->sz = COLSIZE;
	t->s = NULL;
	t->properties = properties;
	memset(&t->part, 0, sizeof(t->part));
	ATOMIC_PTR_INIT(&t->data, NULL);
	return t;
}

sql_table *
create_sql_table(sqlstore *store, allocator *sa, const char *name, sht type, bit system, int persistence, int commit_action, bte properties)
{
	return create_sql_table_with_id(sa, next_oid(store), name, type, system, persistence, commit_action, properties);
}

static void
dup_sql_type(sql_trans *tr, sql_schema *s, sql_subtype *oc, sql_subtype *nc)
{
	nc->digits = oc->digits;
	nc->scale = oc->scale;
	nc->type = oc->type;
	if (s && nc->type->s) { /* user type */
		sql_type *lt = NULL;

		if (s->base.id == nc->type->s->base.id) {
			/* Current user type belongs to current schema. So search there for current user type. */
			lt = find_sql_type(tr, s, nc->type->base.name);
		} else {
			/* Current user type belongs to another schema in the current transaction. Search there for current user type. */
			lt = sql_trans_bind_type(tr, NULL, nc->type->base.name);
		}
		if (lt == NULL)
			GDKfatal("SQL type %s missing", nc->type->base.name);
		sql_init_subtype(nc, lt, nc->digits, nc->scale);
	}
}

static sql_column *
dup_sql_column(allocator *sa, sql_table *t, sql_column *c)
{
	sql_column *col = SA_ZNEW(sa, sql_column);

	base_init(sa, &col->base, c->base.id, t->persistence==SQL_DECLARED_TABLE?false:c->base.new, c->base.name);
	col->type = c->type; /* Both types belong to the same transaction, so no dup_sql_type call is needed */
	col->def = NULL;
	if (c->def)
		col->def = SA_STRDUP(sa, c->def);
	col->null = c->null;
	col->colnr = c->colnr;
	col->t = t;
	col->unique = c->unique;
	col->storage_type = NULL;
	if (c->storage_type)
		col->storage_type = SA_STRDUP(sa, c->storage_type);
	if (ol_add(t->columns, &col->base))
		return NULL;
	return col;
}

static sql_part *
dup_sql_part(allocator *sa, sql_table *mt, sql_part *op)
{
	sql_part *p = SA_ZNEW(sa, sql_part);

	base_init(sa, &p->base, op->base.id, mt->persistence==SQL_DECLARED_TABLE?false:op->base.new, op->base.name);
	p->with_nills = op->with_nills;

	if (isRangePartitionTable(mt)) {
		p->part.range.minvalue = SA_NEW_ARRAY(sa, char, op->part.range.minlength);
		p->part.range.maxvalue = SA_NEW_ARRAY(sa, char, op->part.range.maxlength);
		memcpy(p->part.range.minvalue, op->part.range.minvalue, op->part.range.minlength);
		memcpy(p->part.range.maxvalue, op->part.range.maxvalue, op->part.range.maxlength);
		p->part.range.minlength = op->part.range.minlength;
		p->part.range.maxlength = op->part.range.maxlength;
	} else if (isListPartitionTable(mt)) {
		p->part.values = list_new(sa, (fdestroy) &part_value_destroy);
		for (node *n = op->part.values->h ; n ; n = n->next) {
			sql_part_value *prev = (sql_part_value*) n->data, *nextv = SA_ZNEW(sa, sql_part_value);
			nextv->value = SA_NEW_ARRAY(sa, char, prev->length);
			memcpy(nextv->value, prev->value, prev->length);
			nextv->length = prev->length;
			list_append(p->part.values, nextv);
		}
	}
	list_append(mt->members, p);
	p->t = mt;
	p->member = op->member;
	assert(p->member);
	return p;
}

sql_table *
dup_sql_table(allocator *sa, sql_table *t)
{
	node *n;
	sql_table *nt = create_sql_table_with_id(sa, t->base.id, t->base.name, t->type, t->system, SQL_DECLARED_TABLE, t->commit_action, t->properties);

	nt->base.new = t->base.new;

	nt->access = t->access;
	nt->query = (t->query) ? SA_STRDUP(sa, t->query) : NULL;
	nt->s = t->s;

	if (isPartitionedByExpressionTable(nt)) {
		nt->part.pexp = SA_ZNEW(sa, sql_expression);
		nt->part.pexp->exp = SA_STRDUP(sa, t->part.pexp->exp);
		nt->part.pexp->type = t->part.pexp->type; /* No dup_sql_type call needed */
		nt->part.pexp->cols = SA_LIST(sa, (fdestroy) &int_destroy);
		for (n = t->part.pexp->cols->h; n; n = n->next) {
			int *nid = SA_NEW(sa, int);
			*nid = *(int *) n->data;
			list_append(nt->part.pexp->cols, nid);
		}
	}

	for (n = t->columns->l->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_column *dup = dup_sql_column(sa, nt, c);
		if (isPartitionedByColumnTable(nt) && c->base.id == t->part.pcol->base.id)
			nt->part.pcol = dup;
	}

	if (t->members)
		for (n = t->members->h; n; n = n->next)
			dup_sql_part(sa, nt, n->data);
	return nt;
}

static sql_table *
bootstrap_create_table(sql_trans *tr, sql_schema *s, const char *name, sqlid id)
{
	sqlstore *store = tr->store;
	int istmp = isTempSchema(s);
	int persistence = istmp?SQL_GLOBAL_TEMP:SQL_PERSIST;
	sht commit_action = istmp?CA_PRESERVE:CA_COMMIT;
	sql_table *t;

	if ((sqlid) ATOMIC_GET(&store->obj_id) <= id)
		ATOMIC_SET(&store->obj_id, id + 1);
	t = create_sql_table_with_id(NULL, id, name, tt_table, 1, persistence, commit_action, 0);
	t->bootstrap = 1;

	TRC_DEBUG(SQL_STORE, "Create table: %s\n", name);

	t->base.new = s->base.new;
	t->query = NULL;
	t->s = s;
	if (isTable(t) && store->storage_api.create_del(tr, t) != LOG_OK) {
		table_destroy(store, t);
		return NULL;
	}
	if (os_add(s->tables, tr, name, &t->base)) {
		table_destroy(store, t);
		return NULL;
	}
	return t;
}

static sql_schema *
bootstrap_create_schema(sql_trans *tr, const char *name, sqlid id, sqlid auth_id, int owner)
{
	sqlstore *store = tr->store;
	sql_schema *s = ZNEW(sql_schema);

	if ((sqlid) ATOMIC_GET(&store->obj_id) <= id)
		ATOMIC_SET(&store->obj_id, id + 1);
	TRC_DEBUG(SQL_STORE, "Create schema: %s %d %d\n", name, auth_id, owner);

	if (strcmp(name, dt_schema) == 0) {
		base_init(NULL, &s->base, (sqlid) FUNC_OIDS - 1, true, name);
	} else {
		base_init(NULL, &s->base, id, true, name);
	}
	s->base.new = store->first;
	s->auth_id = auth_id;
	s->owner = owner;
	s->system = TRUE;
	s->tables = os_new(NULL, (destroy_fptr) &table_destroy, false, true, true, false, store);
	s->types = os_new(NULL, (destroy_fptr) &type_destroy, false, true, true, false, store);
	s->funcs = os_new(NULL, (destroy_fptr) &func_destroy, false, false, false, false, store);
	s->seqs = os_new(NULL, (destroy_fptr) &seq_destroy, false, true, true, false, store);
	s->keys = os_new(NULL, (destroy_fptr) &key_destroy, false, true, true, false, store);
	s->idxs = os_new(NULL, (destroy_fptr) &idx_destroy, false, true, true, false, store);
	s->triggers = os_new(NULL, (destroy_fptr) &trigger_destroy, false, true, true, false, store);
	s->parts = os_new(NULL, (destroy_fptr) &part_destroy, false, false, true, false, store);
	if (os_add(tr->cat->schemas, tr, s->base.name, &s->base)) {
		return NULL;
	}
	if (isTempSchema(s))
		tr->tmp = s;

	s->store = tr->store;
	return s;
}

/* TODO clean this */
static inline int
dep_hash(sql_dependency_change *dep)
{
	return (int) BATatoms[TYPE_int].atomHash(&dep->objid);
}

static void
dep_hash_clear(sql_hash *h)
{
	if (h == NULL || h->sa || h->entries == 0)
		return;
	for (int i = 0; i < h->size; i++) {
		sql_hash_e *e = h->buckets[i];

		while (e) {
			sql_hash_e *next = e->chain;

			_DELETE(e->value);
			_DELETE(e);
			e = next;
		}
		h->buckets[i] = NULL;
	}
	h->entries = 0;
}

static void
dep_hash_destroy(sql_hash *h)
{
	if (h == NULL || h->sa)
		return;
	for (int i = 0; i < h->size; i++) {
		sql_hash_e *e = h->buckets[i];

		while (e) {
			sql_hash_e *next = e->chain;

			_DELETE(e->value);
			_DELETE(e);
			e = next;
		}
	}
	_DELETE(h->buckets);
	_DELETE(h);
}

static sqlstore *
store_load(sqlstore *store, allocator *pa)
{
	sql_trans *tr;
	sql_table *t, *types, *functions, *arguments;
	sql_schema *s;
	lng lng_store_oid;

	assert(pa);
	store->sa = pa;

	store->first = store->logger_api.log_isnew(store);

	if (store->first && store->readonly) {
		/* cannot initialize database in readonly mode */
		TRC_CRITICAL(SQL_STORE, "Cannot initialize store in readonly mode\n");
		return NULL;
	}

	types_init(store->sa); /* initialize global lists of types and functions, TODO: needs to move */

	/* we store some spare oids */
	ATOMIC_SET(&store->obj_id, FUNC_OIDS);

	tr = sql_trans_create(store, NULL, NULL);
	if (!tr) {
		TRC_CRITICAL(SQL_STORE, "Failed to start a transaction while loading the storage\n");
		return NULL;
	}
	tr->store = store;
	tr->active = 1;

	/* for now use malloc and free */
	store->active = list_create(NULL);
	store->dependencies = hash_new(NULL, 32, (fkeyvalue)&dep_hash);
	store->depchanges = hash_new(NULL, 32, (fkeyvalue)&dep_hash);
	store->sequences = hash_new(NULL, 32, (fkeyvalue)&seq_hash);
	store->seqchanges = list_create(NULL);
	if (!store->active || !store->dependencies || !store->depchanges || !store->sequences || !store->seqchanges) {
		goto critical;
	}

	s = bootstrap_create_schema(tr, "sys", 2000, ROLE_SYSADMIN, USER_MONETDB);
	if (s == NULL) {
		goto critical;
	}
	if (!store->first)
		s->base.new = 0;

	if ((t = bootstrap_create_table(tr, s, "schemas", 2001)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2002, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2003, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "authorization", 2004, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "owner", 2005, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "system", 2006, "boolean", 1) == NULL ||

		(types = t = bootstrap_create_table(tr, s, "types", 2007)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2008, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "systemname", 2009, "varchar", 256) == NULL ||
		bootstrap_create_column(tr, t, "sqlname", 2010, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "digits", 2011, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "scale", 2012, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "radix", 2013, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "eclass", 2014, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "schema_id", 2015, "int", 31) == NULL ||

		(functions = t = bootstrap_create_table(tr, s, "functions", 2016)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2017, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2018, "varchar", 256) == NULL ||
		bootstrap_create_column(tr, t, "func", 2019, "varchar", 8196) == NULL ||
		bootstrap_create_column(tr, t, "mod", 2020, "varchar", 8196) == NULL ||

		/* language asm=0, sql=1, R=2, C=3, J=4 */
		bootstrap_create_column(tr, t, "language", 2021, "int", 31) == NULL ||

		/* func, proc, aggr or filter */
		bootstrap_create_column(tr, t, "type", 2022, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "side_effect", 2023, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "varres", 2024, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "vararg", 2025, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "schema_id", 2026, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "system", 2027, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "semantics", 2162, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "order_specification", 2167, "tinyint", 7) == NULL ||

		(arguments = t = bootstrap_create_table(tr, s, "args", 2028)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2029, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "func_id", 2030, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2031, "varchar", 256) == NULL ||
		bootstrap_create_column(tr, t, "type", 2032, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "type_digits", 2033, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type_scale", 2034, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "inout", 2035, "tinyint", 7) == NULL ||
		bootstrap_create_column(tr, t, "number", 2036, "int", 31) == NULL ||

		(t = bootstrap_create_table(tr, s, "sequences", 2037)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2038, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "schema_id", 2039, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2040, "varchar", 256) == NULL ||
		bootstrap_create_column(tr, t, "start", 2041, "bigint", 63) == NULL ||
		bootstrap_create_column(tr, t, "minvalue", 2042, "bigint", 63) == NULL ||
		bootstrap_create_column(tr, t, "maxvalue", 2043, "bigint", 63) == NULL ||
		bootstrap_create_column(tr, t, "increment", 2044, "bigint", 63) == NULL ||
		bootstrap_create_column(tr, t, "cacheinc", 2045, "bigint", 63) == NULL ||
		bootstrap_create_column(tr, t, "cycle", 2046, "boolean", 1) == NULL ||

		(t = bootstrap_create_table(tr, s, "table_partitions", 2047)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2048, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2049, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "column_id", 2050, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "expression", 2051, "varchar", STORAGE_MAX_VALUE_LENGTH) == NULL ||
		bootstrap_create_column(tr, t, "type", 2052, "tinyint", 7) == NULL ||

		(t = bootstrap_create_table(tr, s, "range_partitions", 2053)) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2054, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "partition_id", 2055, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "minimum", 2056, "varchar", STORAGE_MAX_VALUE_LENGTH) == NULL ||
		bootstrap_create_column(tr, t, "maximum", 2057, "varchar", STORAGE_MAX_VALUE_LENGTH) == NULL ||
		bootstrap_create_column(tr, t, "with_nulls", 2058, "boolean", 1) == NULL ||

		(t = bootstrap_create_table(tr, s, "value_partitions", 2059)) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2060, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "partition_id", 2061, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "value", 2062, "varchar", STORAGE_MAX_VALUE_LENGTH) == NULL ||

		(t = bootstrap_create_table(tr, s, "dependencies", 2063)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2064, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "depend_id", 2065, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "depend_type", 2066, "smallint", 15) == NULL ||


		(t = bootstrap_create_table(tr, s, "_tables", 2067)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2068, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2069, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "schema_id", 2070, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "query", 2071, "varchar", 1 << 20) == NULL||
		bootstrap_create_column(tr, t, "type", 2072, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "system", 2073, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "commit_action", 2074, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "access", 2075, "smallint", 15) == NULL ||

		(t = bootstrap_create_table(tr, s, "_columns", 2076)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2077, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2078, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "type", 2079, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "type_digits", 2080, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type_scale", 2081, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2082, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "default", 2083, "varchar", STORAGE_MAX_VALUE_LENGTH) == NULL ||
		bootstrap_create_column(tr, t, "null", 2084, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "number", 2085, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "storage", 2086, "varchar", 2048) == NULL ||

		(t = bootstrap_create_table(tr, s, "keys", 2087)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2088, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2089, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type", 2090, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2091, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "rkey", 2092, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "action", 2093, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "check", 2165, "varchar", 2048) == NULL ||

		(t = bootstrap_create_table(tr, s, "idxs", 2094)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2095, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2096, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type", 2097, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2098, "varchar", 1024) == NULL ||

		(t = bootstrap_create_table(tr, s, "triggers", 2099)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2100, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2101, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2102, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "time", 2103, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "orientation", 2104, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "event", 2105, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "old_name", 2106, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "new_name", 2107, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "condition", 2108, "varchar", 2048) == NULL ||
		bootstrap_create_column(tr, t, "statement", 2109, "varchar", 2048) == NULL ||

		(t = bootstrap_create_table(tr, s, "objects", 2110)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2111, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2112, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "nr", 2113, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "sub", 2163, "int", 31) == NULL) {
		goto critical;
	}

	if ((s = bootstrap_create_schema(tr, "tmp", 2114, ROLE_SYSADMIN, USER_MONETDB)) == NULL) {
		goto critical;
	}
	store->tmp = s;

	if ((t = bootstrap_create_table(tr, s, "_tables", 2115)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2116, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2117, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "schema_id", 2118, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "query", 2119, "varchar", 1 << 20) == NULL ||
		bootstrap_create_column(tr, t, "type", 2120, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "system", 2121, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "commit_action", 2122, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "access", 2123, "smallint", 15) == NULL ||

		(t = bootstrap_create_table(tr, s, "_columns", 2124)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2125, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2126, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "type", 2127, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "type_digits", 2128, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type_scale", 2129, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2130, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "default", 2131, "varchar", STORAGE_MAX_VALUE_LENGTH) == NULL ||
		bootstrap_create_column(tr, t, "null", 2132, "boolean", 1) == NULL ||
		bootstrap_create_column(tr, t, "number", 2133, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "storage", 2134, "varchar", 2048) == NULL ||

		(t = bootstrap_create_table(tr, s, "keys", 2135)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2136, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2137, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type", 2138, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2139, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "rkey", 2140, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "action", 2141, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "check", 2166, "varchar", 2048) == NULL ||

		(t = bootstrap_create_table(tr, s, "idxs", 2142)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2143, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2144, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "type", 2145, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2146, "varchar", 1024) == NULL ||

		(t = bootstrap_create_table(tr, s, "triggers", 2147)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2148, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2149, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "table_id", 2150, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "time", 2151, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "orientation", 2152, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "event", 2153, "smallint", 15) == NULL ||
		bootstrap_create_column(tr, t, "old_name", 2154, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "new_name", 2155, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "condition", 2156, "varchar", 2048) == NULL ||
		bootstrap_create_column(tr, t, "statement", 2157, "varchar", 2048) == NULL ||

		(t = bootstrap_create_table(tr, s, "objects", 2158)) == NULL ||
		bootstrap_create_column(tr, t, "id", 2159, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "name", 2160, "varchar", 1024) == NULL ||
		bootstrap_create_column(tr, t, "nr", 2161, "int", 31) == NULL ||
		bootstrap_create_column(tr, t, "sub", 2164, "int", 31) == NULL) {
		goto critical;
	}

	if (bootstrap_create_schema(tr, dt_schema, -1, ROLE_SYSADMIN, USER_MONETDB) == NULL) {
		goto critical;
	}

	if (store->first) {
		if (insert_types(tr, types) || insert_functions(tr, functions, funcs, arguments) || insert_schemas(tr)) {
			TRC_CRITICAL(SQL_STORE, "Cannot load catalog\n");
			sql_trans_destroy(tr);
			return NULL;
		}
	} else {
		tr->active = 0;
	}

	if (sql_trans_commit(tr) != SQL_OK) {
		TRC_CRITICAL(SQL_STORE, "Cannot commit initial transaction\n");
		sql_trans_destroy(tr);
		return NULL;
	}
	tr->ts = store_timestamp(store);

	store->logger_api.get_sequence(store, OBJ_SID, &lng_store_oid);
	store->prev_oid = (sqlid)lng_store_oid;
	if ((sqlid) ATOMIC_GET(&store->obj_id) < store->prev_oid)
		ATOMIC_SET(&store->obj_id, store->prev_oid);

	/* load remaining schemas, tables, columns etc */
	tr->active = 1;
	if (!store->first && !load_trans(tr)) {
		TRC_CRITICAL(SQL_STORE, "Cannot load catalog tables\n");
		sql_trans_destroy(tr);
		return NULL;
	}
	if (sql_trans_commit(tr) != SQL_OK) {
		TRC_CRITICAL(SQL_STORE, "Cannot commit loaded objects transaction\n");
		sql_trans_destroy(tr);
		return NULL;
	}
	tr->active = 0;
	sql_trans_destroy(tr);
	store->initialized = 1;
	return store;

  critical:
	TRC_CRITICAL(SQL_STORE, "Allocation failure while initializing store\n");
	sql_trans_destroy(tr);
	return NULL;
}

sqlstore *
store_init(int debug, store_type store_tpe, int readonly, int singleuser)
{
	allocator *pa;
	sqlstore *store = MNEW(sqlstore);

	if (debug&2)
		GDKtracer_set_layer_level("sql_all", "debug");

	if (!store) {
		TRC_CRITICAL(SQL_STORE, "Allocation failure while initializing store\n");
		return NULL;
	}

	if (!(pa = sa_create(NULL))) {
		TRC_CRITICAL(SQL_STORE, "Allocation failure while initializing store\n");
		_DELETE(store);
		return NULL;
	}

	*store = (sqlstore) {
		.readonly = readonly,
		.singleuser = singleuser,
		.debug = debug,
		.transaction = ATOMIC_VAR_INIT(TRANSACTION_ID_BASE),
		.nr_active = ATOMIC_VAR_INIT(0),
		.timestamp = ATOMIC_VAR_INIT(0),
		.lastactive = ATOMIC_VAR_INIT(0),
		.function_counter = ATOMIC_VAR_INIT(0),
		.oldest = ATOMIC_VAR_INIT(0),
		.obj_id = ATOMIC_VAR_INIT(0),
		.sa = pa,
	};

	(void)store_timestamp(store); /* increment once */
	MT_lock_init(&store->lock, "sqlstore_lock");
	MT_lock_init(&store->commit, "sqlstore_commit");
	MT_lock_init(&store->flush, "sqlstore_flush");
	for(int i = 0; i<NR_TABLE_LOCKS; i++)
		MT_lock_init(&store->table_locks[i], "sqlstore_table");
	for(int i = 0; i<NR_COLUMN_LOCKS; i++)
		MT_lock_init(&store->column_locks[i], "sqlstore_column");

	MT_lock_set(&store->flush);
	MT_lock_set(&store->lock);

	/* initialize empty bats */
	switch (store_tpe) {
	case store_bat:
	case store_mem:
		if (bat_utils_init() == -1) {
			TRC_CRITICAL(SQL_STORE, "Allocation failure while initializing store\n");
			MT_lock_unset(&store->lock);
			MT_lock_unset(&store->flush);
			store_exit(store);
			return NULL;
		}
		bat_storage_init(&store->storage_api);
		bat_table_init(&store->table_api);
		bat_logger_init(&store->logger_api);
		break;
	default:
		break;
	}
	store->active_type = store_tpe;
	int v = 1;
	if (!store->logger_api.create ||
	    store->logger_api.create(store, debug, "sql_logs", CATALOG_VERSION*v) != LOG_OK) {
		MT_lock_unset(&store->lock);
		MT_lock_unset(&store->flush);
		store_exit(store);
		return NULL;
	}

	/* create the initial store structure or re-load previous data */
	MT_lock_unset(&store->lock);
	MT_lock_unset(&store->flush);
	if (!store_load(store, pa)) {
		/* zap current change list */
		store->changes = NULL;
		store_exit(store);
		return NULL;
	}
	return store;
}

void
store_exit(sqlstore *store)
{
	allocator *sa = store->sa;
	MT_lock_set(&store->commit);
	MT_lock_set(&store->flush);
	MT_lock_set(&store->lock);

	TRC_DEBUG(SQL_STORE, "Store locked\n");

	if (store->cat) {
		while (ATOMIC_GET(&store->nr_active) > 0) {
			const int sleeptime = 100;
			MT_lock_unset(&store->flush);
			MT_lock_unset(&store->lock);
			MT_lock_unset(&store->commit);
			MT_sleep_ms(sleeptime);
			MT_lock_set(&store->commit);
			MT_lock_set(&store->lock);
			MT_lock_set(&store->flush);
		}
		if (!list_empty(store->changes)) {
			ulng oldest = store_timestamp(store)+1;
			for(node *n=store->changes->h; n; n = n->next) {
				sql_change *c = n->data;

				if (c->cleanup && !c->cleanup(store, c, oldest)) {
					/* try again with newer oldest, should cleanup any pending issues */
					if (!c->cleanup(store, c, oldest+1))
						TRC_DEBUG(SQL_STORE, "not deleted\n");
					else
						_DELETE(c);
				} else
					_DELETE(c);
			}
		}
		MT_lock_unset(&store->commit);
		list_destroy(store->changes);
		os_destroy(store->cat->objects, store);
		os_destroy(store->cat->schemas, store);
		_DELETE(store->cat);
	} else {
		MT_lock_unset(&store->commit);
	}
	store->logger_api.destroy(store);

	list_destroy(store->active);
	dep_hash_destroy(store->dependencies);
	dep_hash_destroy(store->depchanges);
	list_destroy(store->seqchanges);
	seq_hash_destroy(store->sequences);

	TRC_DEBUG(SQL_STORE, "Store unlocked\n");
	MT_lock_unset(&store->flush);
	MT_lock_unset(&store->lock);
	sa_destroy(sa);
	MT_lock_destroy(&store->lock);
	MT_lock_destroy(&store->commit);
	MT_lock_destroy(&store->flush);
	for(int i = 0; i<NR_TABLE_LOCKS; i++)
		MT_lock_destroy(&store->table_locks[i]);
	for(int i = 0; i<NR_COLUMN_LOCKS; i++)
		MT_lock_destroy(&store->column_locks[i]);
	_DELETE(store);
}

/* call locked! */
static int
store_apply_deltas(sqlstore *store)
{
	int res = LOG_OK;

	store_lock(store);
	ulng oldest = store_oldest_pending(store);
	store_unlock(store);
	TRC_DEBUG(SQL_STORE, "Store apply deltas (" ULLFMT ")\n", oldest-1);
	if (oldest)
	    res = store->logger_api.flush(store, oldest-1);
	return res;
}

void
store_suspend_log(sqlstore *store)
{
	MT_lock_set(&store->lock);
	MT_lock_unset(&store->lock);
}

void
store_resume_log(sqlstore *store)
{
	MT_lock_set(&store->flush);
	MT_lock_unset(&store->flush);
}

static void
id_hash_clear_older(sql_hash *h, ulng oldest)
{
	if (h->entries == 0)
		return;
	for (int i = 0; i < h->size; i++) {
		sql_hash_e *e = h->buckets[i], *last = NULL, *first = NULL;

		while (e) {
			sql_hash_e *next = e->chain;
			sql_dependency_change *dc = e->value;

			if (dc->ts < oldest) {
				_DELETE(e->value);
				_DELETE(e);
				h->entries--;
			} else {
				if (last)
					last->chain = e;
				else
					first = e;
				last = e;
			}
			e = next;
		}
		if (last)
			last->chain = NULL;
		h->buckets[i] = first;
	}
}

static void
store_pending_changes(sqlstore *store, ulng oldest, sql_trans *tr)
{
	ulng oldest_changes = store_get_timestamp(store);
	if (!list_empty(store->changes)) { /* lets first cleanup old stuff */
		for(node *n=store->changes->h; n; ) {
			node *next = n->next;
			sql_change *c = n->data;

			assert(c->cleanup);
			if (c->cleanup(store, c, oldest)) {
				list_remove_node(store->changes, store, n);
				_DELETE(c);
			} else if (!c->handled && c->ts < oldest_changes) {
				oldest_changes = c->ts;
			}
			n = next;
		}
	}
	if (ATOMIC_GET(&store->nr_active) < 2) { /* one or no transaction running */
		dep_hash_clear(store->dependencies);
		dep_hash_clear(store->depchanges);
	} else {
		ulng stoldest = store_oldest(store, tr);
		id_hash_clear_older(store->dependencies, stoldest);
		id_hash_clear_older(store->depchanges, stoldest);
	}
	store->oldest_pending = oldest_changes;
}

#define IDLE_TIME	30			/* in seconds */

void
store_manager(sqlstore *store)
{
	MT_thread_setworking("sleeping");

	// In the main loop we always hold the lock except when sleeping or doing cleanups
	MT_lock_set(&store->flush);

	for (;;) {
		const int idle = ATOMIC_GET(&GDKdebug) & TESTINGMASK ? 5000 : IDLE_TIME * 1000000;
		/* if debug bit 1024 is set, attempt immediate log activation
		 * and clear the bit */
		if (store->debug&(128|1024) || ATOMIC_GET(&store->lastactive) + idle < (ATOMIC_BASE_TYPE) GDKusec()) {
			store->debug &= ~1024;
			MT_lock_unset(&store->flush);
			store_lock(store);
			if (ATOMIC_GET(&store->nr_active) == 0) {
				ulng oldest = store_timestamp(store)+1;
				store_pending_changes(store, oldest, NULL);
			}
			store_unlock(store);
			MT_lock_set(&store->flush);
			store->logger_api.activate(store); /* rotate to new log file */
			ATOMIC_SET(&store->lastactive, GDKusec());
		}

		if (GDKexiting())
			break;
		const int sleeptime = 100;
		MT_lock_unset(&store->flush);
		MT_sleep_ms(sleeptime);
		for (;;) {
			MT_lock_set(&store->commit);
			if (MT_lock_try(&store->flush))
				break;
			MT_lock_unset(&store->commit);
			MT_sleep_ms(sleeptime);
		}

		if (GDKexiting()) {
			MT_lock_unset(&store->commit);
			break;
		}

		if (store->logger_api.changes(store) <= 0) {
			TRC_DEBUG(SQL_STORE, "Store flusher, no changes\n");
			MT_lock_unset(&store->commit);
			continue;
		}
		MT_lock_unset(&store->commit);

		MT_thread_setworking("flushing");
		if (store_apply_deltas(store) != LOG_OK) {
			MT_lock_unset(&store->flush);
			if (!GDKexiting())
				GDKfatal("write-ahead logging failure");
		}

		if (GDKexiting())
			break;
		MT_thread_setworking("sleeping");
		TRC_DEBUG(SQL_STORE, "Store flusher done\n");
	}

	// End of loop, end of lock
	MT_lock_unset(&store->flush);
}

void
store_lock(sqlstore *store)
{
	MT_lock_set(&store->lock);
	/* tell GDK allocation functions to ignore limits */
	MT_thread_setworking("store locked");
}

void
store_unlock(sqlstore *store)
{
	TRC_DEBUG(SQL_STORE, "Store unlocked\n");
	/* tell GDK allocation functions to honor limits again */
	MT_thread_setworking("store unlocked");
	MT_lock_unset(&store->lock);
}

int
store_readonly(sqlstore *store)
{
	return store->readonly;
}

static bool
table_must_be_omitted(bool omitunlogged, BAT *table_ids, sql_table *t)
{
	if (omitunlogged && t->type == tt_unlogged_table)
		return true;
	sqlid table_id = t->base.id;
	if (table_ids != NULL && BUNfnd(table_ids, &table_id) != BUN_NONE)
		return true;
	return false;
}

static gdk_return
forbid_fkey_to_omitted_table(sql_trans *tr, BAT *table_ids, bool omitunlogged, BAT *key_ids)
{
	sqlstore *store = tr->store;

	struct os_iter schema_iter;
	os_iterator(&schema_iter, store->cat->schemas, tr, NULL);
	for (sql_schema *s = (sql_schema*)oi_next(&schema_iter); s; s = (sql_schema*)oi_next(&schema_iter)) {
		char *sname = s->base.name;
		struct os_iter table_iter;
		os_iterator(&table_iter, s->tables, tr, NULL);
		for (sql_table *t = (sql_table*)oi_next(&table_iter); t; t = (sql_table*)oi_next(&table_iter)) {
			char *tname = t->base.name;
			if (table_must_be_omitted(omitunlogged, table_ids, t)) {
				// if the table will be omitted we don't care if it
				// has foreign keys referencing other omitted tables.
				continue;
			}
			for (node *n = ol_first_node(t->keys); n; n = n->next) {
				sql_key *k = n->data;
					if (k->type != fkey)
					continue;
				sql_fkey *fk = (sql_fkey*)k;
				sqlid rkey = fk->rkey;
				if (BUNfnd(key_ids, &rkey) == BUN_NONE) {
					// this foreign key references a table that is not omitted.
					continue;
				}

				GDKerror("table %s.%s has foreign key reference to omitted table\n", sname, tname);
				return GDK_FAIL;
			}
		}
	}

	return GDK_SUCCEED;
}

static gdk_return
add_bat_to_omit(BAT *bat_ids, stream *col_ids, BAT *b, sql_base *obj)
{
	assert(b != NULL);
	assert(b->batRole == PERSISTENT);
	if (BUNappend(bat_ids, &b->batCacheid, 0) != GDK_SUCCEED) {
		GDKerror("Cannot append to temp bat for table ids to omit");
		return GDK_FAIL;
	}

	sqlid id = obj->id;
	if (mnstr_printf(col_ids, "%d\n", id) < 1) {
		GDKerror("Cannot append to temp buffer for table ids to omit");
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

static gdk_return
add_table_to_bats_to_omit(BAT *bat_ids, BAT *key_ids, stream *col_ids, sql_trans *tr, sql_schema *s, sql_table *t)
{
	gdk_return ret;
	sqlstore *store = tr->store;

	// Add the columns
	for (node *n = t->columns->l->h; n; n = n->next) {
		sql_column *c = n->data;
		// with QUICK, the BAT pointer does not have to be BBPunfixed.
		BAT *b = store->storage_api.bind_col(tr, c, QUICK);
		ret = add_bat_to_omit(bat_ids, col_ids, b, &c->base);
		if (ret != GDK_SUCCEED)
			return ret;
	}

	// Add the delete mask
	bat bat_id = ((storage *) ATOMIC_PTR_GET(&t->data))->cs.bid;
	BAT *b = BATdescriptor(bat_id);
	if (b == NULL || b->batRole != PERSISTENT) {
		GDKerror("Cannot find delete mask for table %s.%s (%d)", s->base.name, t->base.name, t->base.id);
		return GDK_FAIL;
	}
	ret = add_bat_to_omit(bat_ids, col_ids, b, &t->base);
	BBPunfix(bat_id);
	if (ret != GDK_SUCCEED)
		return ret;

	// Add the indices
	for (node *n = t->idxs->l->h; n; n = n->next) {
		sql_idx *i = n->data;
		// TODO maybe check i->type first
		BAT *b = store->storage_api.bind_idx(tr, i, QUICK);
		if (b == NULL || b->batRole != PERSISTENT)
			continue;
		ret = add_bat_to_omit(bat_ids, col_ids, b, &i->base);
		if (ret != GDK_SUCCEED)
			return ret;
	}

	// Enumerate the keys so we can check for foreign key constraints later
	for (node *n = ol_first_node(t->keys); n; n = n->next) {
		sql_key *k = n->data;
		sqlid key_id = k->base.id;
		if (BUNappend(key_ids, &key_id, 0) != GDK_SUCCEED) {
			GDKerror("cannot append key id to list of keys to watch out for");
			return GDK_FAIL;
		}
	}

	return GDK_SUCCEED;
}

static gdk_return
find_bats_to_omit(sql_trans *tr, BAT *bat_ids, stream *col_ids, bool omitunlogged, const char *omitids)
{
	gdk_return ret = GDK_FAIL;
	BAT *table_ids = NULL;
	BAT *key_ids = NULL;
	sqlstore *store = tr->store;

	// parse omitids
	if (omitids != NULL && omitids[0] != '\0') {
		table_ids = COLnew(0, TYPE_int, 0, TRANSIENT);
		if (table_ids == NULL) {
			GDKerror("Cannot create temp bat for table ids to omit");
			goto end;
		}
		const char *p = omitids;
		while (1) {
			char *rest;
			long nn = strtol(p, &rest, 10);
			if (nn < 0 || nn >= INT_MAX || (*rest != ',' && *rest != '\0')) {
				GDKerror("Invalid table id in omitids: %s", omitids);
				goto end;
			}
			int n = nn; // range check done above
			if (BUNappend(table_ids, &n, 0) != GDK_SUCCEED) {
				GDKerror("Cannot append to temp bat for table ids to omit");
				goto end;
			}
			if (*rest == '\0')
				break;
			p = rest + 1;
		}
	}

	if (!omitunlogged && table_ids == NULL) {
		// exit early
		ret = GDK_SUCCEED;
		goto end;
	}

	key_ids = COLnew(0, TYPE_int, 0, TRANSIENT);
	if (key_ids == NULL) {
		GDKerror("Cannot create temp bat for key ids of omitted tables");
		goto end;
	}

	// Iterate over the catalog to find the tables and add their various BATs
	// (columns, delete mask, indices..) to bat_ids.
	struct os_iter schema_iter;
	os_iterator(&schema_iter, store->cat->schemas, tr, NULL);
	for (sql_schema *s = (sql_schema*)oi_next(&schema_iter); s; s = (sql_schema*)oi_next(&schema_iter)) {
		struct os_iter table_iter;
		os_iterator(&table_iter, s->tables, tr, NULL);
		for (sql_table *t = (sql_table*)oi_next(&table_iter); t; t = (sql_table*)oi_next(&table_iter)) {
			if (!table_must_be_omitted(omitunlogged, table_ids, t))
				continue;
			if (add_table_to_bats_to_omit(bat_ids, key_ids, col_ids, tr, s, t) != GDK_SUCCEED) {
				// gdk error has already been set, go straight to end
				goto end;
			}
		}
	}

	if (forbid_fkey_to_omitted_table(tr, table_ids, omitunlogged, key_ids) != GDK_SUCCEED)
		goto end;

	ret = GDK_SUCCEED;
end:
	if (table_ids)
		BBPreclaim(table_ids);
	if (key_ids)
		BBPreclaim(key_ids);
	return ret;
}

// Helper function for tar_write_header.
// Our stream.h makes sure __attribute__ exists.
__attribute__((__format__(__printf__, 3, 4)))
static void
tar_write_header_field(char **cursor_ptr, size_t size, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	(void)vsnprintf(*cursor_ptr, size + 1, fmt, ap);
	va_end(ap);

	/* At first reading you might wonder why add `size` instead
	 * of the byte count returned by vsnprintf. The reason is
	 * that we want to move `*cursor_ptr` to the start of the next
	 * field, not to the unused part of this field.
	 */
	*cursor_ptr += size;
}

#define TAR_BLOCK_SIZE (512)

// Write a tar header to the given stream.
__attribute__((__warn_unused_result__))
static gdk_return
tar_write_header(stream *tarfile, const char *path, time_t mtime, int64_t size)
{
	char buf[TAR_BLOCK_SIZE] = {0};
	char *cursor = buf;
	char *size_field;
	char *chksum_field;

	// We set the uid/gid fields to 0 and the uname/gname fields to "".
	// When unpacking as a normal user, they are ignored and the files are
	// owned by that user. When unpacking as root it is reasonable that
	// the resulting files are owned by root.

	// The following is taken directly from the definition found
	// in /usr/include/tar.h on a Linux system.
	tar_write_header_field(&cursor, 100, "%s", path);   // name[100]
	tar_write_header_field(&cursor, 8, "0000644");      // mode[8]
	tar_write_header_field(&cursor, 8, "%07o", 0U);      // uid[8]
	tar_write_header_field(&cursor, 8, "%07o", 0U);      // gid[8]
	size_field = cursor;
	cursor += 12;                                      // size[12]
	tar_write_header_field(&cursor, 12, "%011lo", (unsigned long)mtime); // mtime[12]
	chksum_field = cursor; // use this later to set the computed checksum
	tar_write_header_field(&cursor, 8, "%8s", ""); // chksum[8]
	*cursor++ = '0'; // typeflag REGTYPE
	tar_write_header_field(&cursor, 100, "%s", "");  // linkname[100]
	tar_write_header_field(&cursor, 6, "%s", "ustar"); // magic[6]
	tar_write_header_field(&cursor, 2, "%02o", 0U); // version, not null terminated
	tar_write_header_field(&cursor, 32, "%s", ""); // uname[32]
	tar_write_header_field(&cursor, 32, "%s", ""); // gname[32]
	tar_write_header_field(&cursor, 8, "%07o", 0U); // devmajor[8]
	tar_write_header_field(&cursor, 8, "%07o", 0U); // devminor[8]
	tar_write_header_field(&cursor, 155, "%s", ""); // prefix[155]
	assert(cursor - buf == 500);

	int64_t max_oct_size = 077777777777;    // 0_777_7777_7777, 11 octal digits
	// max_oct_size = 077; // for testing
	if (size <= max_oct_size) {
		tar_write_header_field(&size_field, 12, "%011"PRIo64, size);      // size[12]
	} else {
		uint8_t *field = (uint8_t *)size_field;
		field[0] = 0x80;
		for (int i = 11; i >= 4; i--) {
			field[i] = size & 0xFF;
			size >>= 8;
		}
	}

	// checksum
	unsigned sum = 0;
	for (int i = 0; i < TAR_BLOCK_SIZE; i++)
		sum += (unsigned char) buf[i];

	tar_write_header_field(&chksum_field, 8, "%06o", sum);

	if (mnstr_write(tarfile, buf, TAR_BLOCK_SIZE, 1) != 1) {
		GDKerror("error writing tar header %s: %s", path, mnstr_peek_error(tarfile));
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

/* Write data to the stream, padding it with zeroes up to the next
 * multiple of TAR_BLOCK_SIZE.  Make sure all writes are in multiples
 * of TAR_BLOCK_SIZE.
 */
__attribute__((__warn_unused_result__))
static gdk_return
tar_write(stream *outfile, const char *path,  const char *data, size_t size)
{
	const size_t tail = size % TAR_BLOCK_SIZE;
	const size_t bulk = size - tail;

	if (bulk) {
		size_t written = mnstr_write(outfile, data, 1, bulk);
		if (written != bulk) {
			GDKerror("Wrote only %zu bytes of %s instead of first %zu", written, path, bulk);
			return GDK_FAIL;
		}
	}

	if (tail) {
		char buf[TAR_BLOCK_SIZE] = {0};
		memcpy(buf, data + bulk, tail);
		size_t written = mnstr_write(outfile, buf, 1, TAR_BLOCK_SIZE);
		if (written != TAR_BLOCK_SIZE) {
			GDKerror("Wrote only %zu tail bytes of %s instead of %d", written, path, TAR_BLOCK_SIZE);
			return GDK_FAIL;
		}
	}

	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static gdk_return
tar_write_data(stream *tarfile, const char *path, time_t mtime, const char *data, size_t size)
{
	gdk_return res;

	res = tar_write_header(tarfile, path, mtime, size);
	if (res != GDK_SUCCEED)
		return res;

	return tar_write(tarfile, path, data, size);
}

__attribute__((__warn_unused_result__))
static gdk_return
tar_copy_stream(stream *tarfile, const char *path, time_t mtime, stream *contents, int64_t size, char *buf, size_t bufsize)
{
	assert( (bufsize % TAR_BLOCK_SIZE) == 0);
	assert(bufsize >= TAR_BLOCK_SIZE);

	if (tar_write_header(tarfile, path, mtime, size) != GDK_SUCCEED)
		return GDK_FAIL;

	int64_t to_do = size;
	while (to_do > 0) {
		size_t chunk = (to_do <= (int64_t)bufsize) ? (size_t)to_do : bufsize;
		ssize_t nbytes = mnstr_read(contents, buf, 1, chunk);
		if (nbytes > 0) {
			if (tar_write(tarfile, path, buf, nbytes) != GDK_SUCCEED)
				return GDK_FAIL;
			to_do -= (int64_t)nbytes;
			continue;
		}
		// error handling
		if (nbytes < 0) {
			GDKerror("Error after reading %"PRId64"/%"PRId64" bytes: %s",
				size - to_do, size, mnstr_peek_error(contents));
			return GDK_FAIL;
		} else {
			GDKerror("Unexpected end of file after reading %"PRId64"/%"PRId64" bytes of %s",
				size - to_do, size, path);
			return GDK_FAIL;
		}
	}

	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static gdk_return
hot_snapshot_write_tar(stream *out, const char *prefix, const char *plan)
{
	if (plan == NULL)
		return GDK_FAIL;

	gdk_return ret = GDK_FAIL;
	const char *p = plan; // our cursor in the plan
	time_t timestamp = 1234567890; // dummy date, Sat 14 Feb 2009 12:31:30 AM CET
	// Name convention: _path for the absolute path
	// and _name for the corresponding local relative path
	char abs_src_path[2 * FILENAME_MAX];
	char *src_name = abs_src_path;
	char dest_path[100]; // size imposed by tar format.
	char *dest_name = dest_path + snprintf(dest_path, sizeof(dest_path), "%s/", prefix);
	stream *infile = NULL;
	const char *bufsize_env_var = "hot_snapshot_buffer_size";
	int bufsize = GDKgetenv_int(bufsize_env_var, 1024 * 1024);
	char *buffer = NULL;

	if (bufsize < TAR_BLOCK_SIZE || (bufsize % TAR_BLOCK_SIZE) != 0) {
		GDKerror("invalid value for setting %s=%d: must be a multiple of %d",
			bufsize_env_var, bufsize, TAR_BLOCK_SIZE);
		goto end;
	}
	buffer = GDKmalloc(bufsize);
	if (!buffer) {
		GDKerror("could not allocate buffer");
		goto end;
	}

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();

	int len;
	if (sscanf(p, "%[^\n]\n%n", abs_src_path, &len) != 1) {
		GDKerror("internal error: first line of plan is malformed");
		goto end;
	}
	p += len;
	src_name = abs_src_path + len - 1; // - 1 because len includes the trailing newline
	*src_name++ = DIR_SEP;

	// When testing it's sometimes useful to include the plan in the snapshot file
	// strcpy(dest_name, "_snapshot.plan");
	// if (tar_write_data(out, dest_path, timestamp, plan, strlen(plan)) != GDK_SUCCEED)
	// 	goto end;

	char command;
	int64_t size;
	while (sscanf(p, "%c %"SCNi64" %100s\n%n", &command, &size, src_name, &len) == 3) {
		GDK_CHECK_TIMEOUT_BODY(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(end, qry_ctx));
		p += len;
		strcpy(dest_name, src_name);
		if (size < 0) {
			GDKerror("malformed snapshot plan for %s: size %"PRId64" < 0", src_name, size);
			goto end;
		}
		switch (command) {
			case 'c':
				infile = open_rstream(abs_src_path);
				if (!infile) {
					GDKerror("%s", mnstr_peek_error(NULL));
					goto end;
				}
				if (tar_copy_stream(out, dest_path, timestamp, infile, size, buffer, (size_t)bufsize) != GDK_SUCCEED)
					goto end;
				close_stream(infile);
				infile = NULL;
				break;
			case 'w':
				if (tar_write_data(out, dest_path, timestamp, p, (size_t)size) != GDK_SUCCEED)
					goto end;
				p += (size_t)size;
				break;
			default:
				GDKerror("Unknown command in snapshot plan: %c (%s)", command, src_name);
				goto end;
		}
		mnstr_flush(out, MNSTR_FLUSH_ALL);
	}

	// write a trailing block of zeros. If it succeeds, this function succeeds.
	char *descr = "end-of-archive marker";
	char a;
	a = '\0';
	ret = tar_write(out, descr, &a, 1);
	if (ret == GDK_SUCCEED)
		ret = tar_write(out, descr, &a, 1);

end:
	free((char*)plan);
	GDKfree(buffer);
	if (infile)
		close_stream(infile);
	return ret;
}

/* Pick a name for the temporary tar file. Make sure it has the same extension
 * so as not to confuse the streams library.
 *
 * This function is not entirely safe as compared to for example mkstemp.
 */
__attribute__((__warn_unused_result__))
static str
pick_tmp_name(str filename)
{
	str name = GDKmalloc(strlen(filename) + 10);
	if (name == NULL) {
		GDKerror("malloc failed");
		return NULL;
	}
	strcpy(name, filename);

	// Look for an extension.
	// Make sure it's part of the basename

	char *ext = strrchr(name, '.');
	char *sep = strrchr(name, DIR_SEP);
	char *slash = strrchr(name, '/'); // on Windows, / and \ both work
	if (ext != NULL) {
		// is ext actually after sep and slash?
		if ((sep != NULL && sep > ext) || (slash != NULL && slash > ext))
			ext = NULL;
	}

	if (ext == NULL) {
		return strcat(name, "..tmp");
	} else {
		const char tmp[] = "..tmp.";
		size_t tmplen = strlen(tmp);
		memmove(ext + tmplen, ext, strlen(ext) + 1);
		memmove(ext, tmp, tmplen);
	}

	return name;
}

lng
store_hot_snapshot_to_stream(sql_trans *tr, stream *tar_stream, bool omitunlogged, const char *omitids)
{
	int locked = 0;
	lng result = 0;
	buffer *plan_buf = NULL;
	stream *plan_stream = NULL;
	buffer *col_ids_buf = NULL;
	stream *col_ids_stream = NULL;
	BAT *bats_to_omit = NULL;
	gdk_return r;
	sqlstore *store = tr->store;

	if (!store->logger_api.get_snapshot_files) {
		GDKerror("backend does not support hot snapshots");
		goto end;
	}

	bats_to_omit = COLnew(0, TYPE_int, 0, TRANSIENT);
	if (bats_to_omit == NULL) {
		GDKerror("Failed to allocate tmp bat");
		goto end;
	}

	col_ids_buf = buffer_create(64 * 1024);
	if (!col_ids_buf) {
		GDKerror("Failed to allocate col_ids buffer");
		goto end;
	}
	col_ids_stream = buffer_wastream(col_ids_buf, "write_snapshot_plan");
	if (!col_ids_stream) {
		GDKerror("Failed to allocate col_ids buffer stream");
		goto end;
	}

	if (find_bats_to_omit(tr, bats_to_omit, col_ids_stream, omitunlogged, omitids) != GDK_SUCCEED) {
		// GDKerror has already been set
		goto end;
	}
	mnstr_writeBte(col_ids_stream, '\0');
	close_stream(col_ids_stream);
	col_ids_stream = NULL;

	plan_buf = buffer_create(64 * 1024);
	if (!plan_buf) {
		GDKerror("Failed to allocate plan buffer");
		goto end;
	}
	plan_stream = buffer_wastream(plan_buf, "write_snapshot_plan");
	if (!plan_stream) {
		GDKerror("Failed to allocate plan buffer stream");
		goto end;
	}

	MT_lock_set(&store->flush);
	MT_lock_set(&store->lock);
	BBPtmlock();
	locked = 1;
	if (GDKexiting())
		goto end;

	r = store->logger_api.get_snapshot_files(store, bats_to_omit, plan_stream);
	if (r != GDK_SUCCEED)
		goto end; // should already have set a GDK error
	MT_lock_unset(&store->lock);
	locked = 2;

	if (col_ids_buf->buf[0] != '\0') {
		if (mnstr_printf(plan_stream, "w %zu %s\n%s", col_ids_buf->pos-1, "sql_logs/sql/log.omitted", col_ids_buf->buf) < 0) {
			GDKerror("Failed to write to plan stream");
			goto end;
		}
	}

	close_stream(plan_stream);
	plan_stream = NULL;

	r = hot_snapshot_write_tar(tar_stream, GDKgetenv("gdk_dbname"), buffer_get_buf(plan_buf));
	if (r != GDK_SUCCEED)
		goto end;

	// the original idea was to return a sort of sequence number of the
	// database that identifies exactly which version has been snapshotted
	// but no such number is available:
	// logger_functions.read_last_transaction_id is not implemented
	// anywhere.
	//
	// So we return a random positive integer instead.
	result = 42;

end:
	if (locked) {
		BBPtmunlock();
		if (locked == 1)
			MT_lock_unset(&store->lock);
		MT_lock_unset(&store->flush);
	}
	if (bats_to_omit)
		BBPunfix(bats_to_omit->batCacheid);
	if (plan_stream)
		close_stream(plan_stream);
	if (plan_buf)
		buffer_destroy(plan_buf);
	if (col_ids_stream)
		close_stream(col_ids_stream);
	if (col_ids_buf)
		buffer_destroy(col_ids_buf);
	return result;
}


lng
store_hot_snapshot(sql_trans *tx, str tarfile, bool omitunlogged, const char *omitids)
{
	lng result = 0;
	struct stat st = {0};
	char *tmppath = NULL;
	char *dirpath = NULL;
	int do_remove = 0;
	int dir_fd = -1;
	stream *tar_stream = NULL;
	buffer *plan_buf = NULL;
	stream *plan_stream = NULL;
	sqlstore *store = tx->store;

	if (!store->logger_api.get_snapshot_files) {
		GDKerror("backend does not support hot snapshots");
		goto end;
	}

	if (!MT_path_absolute(tarfile)) {
		GDKerror("Hot snapshot requires an absolute path");
		goto end;
	}

	if (MT_stat(tarfile, &st) == 0) {
		GDKerror("File already exists: %s", tarfile);
		goto end;
	}

	tmppath = pick_tmp_name(tarfile);
	if (tmppath == NULL) {
		goto end;
	}
	tar_stream = open_wstream(tmppath);
	if (!tar_stream) {
		GDKerror("%s", mnstr_peek_error(NULL));
		goto end;
	}
	do_remove = 1;

#ifdef HAVE_FSYNC
	// The following only makes sense on POSIX, where fsync'ing a file
	// guarantees the bytes of the file to go to disk, but not necessarily
	// guarantees the existence of the file in a directory to be persistent.
	// Hence the fsync-the-parent ceremony.

	// Set dirpath to the directory containing the tar file.
	// Call realpath(2) to make the path absolute so it has at least
	// one DIR_SEP in it. Realpath requires the file to exist so
	// we feed it tmppath rather than tarfile.
	dirpath = GDKmalloc(PATH_MAX);
	if (dirpath == NULL) {
		GDKsyserror("malloc failed");
		goto end;
	}
	if (realpath(tmppath, dirpath) == NULL) {
		GDKsyserror("couldn't resolve path %s: %s", tarfile, strerror(errno));
		goto end;
	}
	*strrchr(dirpath, DIR_SEP) = '\0';

	// Open the directory so we can call fsync on it.
	// We use raw posix calls because this is not available in the streams library
	// and I'm not quite sure what a generic streams-api should look like.
	dir_fd = open(dirpath, O_RDONLY); // ERROR no o_rdonly
	if (dir_fd < 0) {
		GDKsyserror("couldn't open directory %s", dirpath);
		goto end;
	}

	// Fsync the directory beforehand too.
	// Postgres believes this is necessary for durability.
	if (fsync(dir_fd) < 0) {
		GDKsyserror("First fsync on %s failed", dirpath);
		goto end;
	}
#else
	(void)dirpath;
#endif

	result = store_hot_snapshot_to_stream(tx, tar_stream, omitunlogged, omitids);
	if (result == 0)
		goto end;

	// Now sync and atomically rename the temp file to the real file,
	// also fsync'ing the directory
	mnstr_fsync(tar_stream);
	close_stream(tar_stream);
	tar_stream = NULL;
	if (MT_rename(tmppath, tarfile) < 0) {
		GDKsyserror("rename %s to %s failed", tmppath, tarfile);
		goto end;
	}
	do_remove = 0;
#ifdef HAVE_FSYNC
	// More POSIX fsync-the-parent-dir ceremony
	if (fsync(dir_fd) < 0) {
		GDKsyserror("fsync on dir %s failed", dirpath);
		goto end;
	}
#endif
end:
	GDKfree(dirpath);
	if (dir_fd >= 0)
		close(dir_fd);
	if (tar_stream)
		close_stream(tar_stream);
	if (plan_stream)
		close_stream(plan_stream);
	if (plan_buf)
		buffer_destroy(plan_buf);
	if (do_remove)
		(void) MT_remove(tmppath);	// Best effort, ignore the result
	GDKfree(tmppath);
	return result;
}

static int
column_dup(sql_trans *tr, sql_column *oc, sql_table *t, sql_column **cres)
{
	sqlstore *store = tr->store;
	sql_column *c = ZNEW(sql_column);
	int res = LOG_OK;

	base_init(NULL, &c->base, oc->base.id, 0, oc->base.name);
	c->type = oc->type;
	c->def = NULL;
	if (oc->def)
		c->def =_STRDUP(oc->def);
	c->null = oc->null;
	c->colnr = oc->colnr;
	c->unique = oc->unique;
	c->t = t;
	c->storage_type = NULL;
	if (oc->storage_type)
		c->storage_type =_STRDUP(oc->storage_type);
	ATOMIC_PTR_INIT(&c->data, NULL);

	if (isTable(c->t)) {
		if (isTempTable(c->t)) {
			if ((res = store->storage_api.create_col(tr, c))) {
				return res;
			}
		} else {
			ATOMIC_PTR_SET(&c->data, store->storage_api.col_dup(oc));
		}
	}
	*cres = c;
	return res;
}

static sql_kc *
kc_dup(sql_kc *kc, sql_table *t)
{
	sql_kc *nkc = ZNEW(sql_kc);
	sql_column *c = find_sql_column(t, kc->c->base.name);

	assert(c);
	nkc->c = c;
	c->unique = kc->c->unique;
	return nkc;
}

static int
key_dup(sql_trans *tr, sql_key *k, sql_table *t, sql_key **kres)
{
	sql_key *nk = (k->type != fkey) ? (sql_key *) ZNEW(sql_ukey) : (sql_key *) ZNEW(sql_fkey);
	node *n;
	int res = LOG_OK;

	base_init(NULL, &nk->base, k->base.id?k->base.id:next_oid(tr->store), 0, k->base.name);
	nk->type = k->type;
	nk->columns = list_create((fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = NULL;

	if (k->idx) {
		node *n = ol_find_name(t->idxs, nk->base.name);
		nk->idx = (sql_idx *)n->data;
		nk->idx->key = nk;
	}

	if (nk->type != fkey) {
		sql_ukey *tk = (sql_ukey *) nk;

		if (nk->type == pkey)
			t->pkey = tk;

		if (nk->type == ckey)
			nk->check = _STRDUP(k->check);
	} else {
		sql_fkey *fk = (sql_fkey *) nk;
		sql_fkey *ok = (sql_fkey *) k;

		fk->rkey = ok->rkey;
		fk->on_delete = ok->on_delete;
		fk->on_update = ok->on_update;
	}

	for (n = k->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nk->columns, kc_dup(okc, t));
	}

	if (isGlobal(t) &&
			((res = os_add(t->s->keys, tr, nk->base.name, dup_base(&nk->base))) ||
		     (res = os_add(tr->cat->objects, tr, nk->base.name, dup_base(&nk->base))))) {
		return res;
	}
	*kres = nk;
	return res;
}

static int
idx_dup(sql_trans *tr, sql_idx * i, sql_table *t, sql_idx **ires)
{
	sqlstore *store = tr->store;
	sql_idx *ni = ZNEW(sql_idx);
	node *n;
	int res = LOG_OK;

	base_init(NULL, &ni->base, i->base.id, 0, i->base.name);

	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;
	ATOMIC_PTR_INIT(&ni->data, NULL);

	if (isTable(i->t)) {
		if (isTempTable(i->t)) {
			if ((res = store->storage_api.create_idx(tr, ni))) {
				return res;
			}
		} else {
			ATOMIC_PTR_SET(&ni->data, store->storage_api.idx_dup(i));
		}
	}

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(ni->columns, kc_dup(okc, t));
	}
	if (isGlobal(t) && (res = os_add(t->s->idxs, tr, ni->base.name, dup_base(&ni->base)))) {
		return res;
	}
	*ires = ni;
	return res;
}

static int
part_dup(sql_trans *tr, sql_part *op, sql_table *mt, sql_part **pres)
{
	sql_part *p = ZNEW(sql_part);
	int res = LOG_OK;

	assert(isMergeTable(mt) || isReplicaTable(mt));
	base_init(NULL, &p->base, op->base.id, 0, op->base.name);
	p->with_nills = op->with_nills;
	p->t = mt;
	p->member = op->member;

	if (isRangePartitionTable(mt)) {
		p->part.range.minvalue = NEW_ARRAY(char, op->part.range.minlength);
		p->part.range.maxvalue = NEW_ARRAY(char, op->part.range.maxlength);
		memcpy(p->part.range.minvalue, op->part.range.minvalue, op->part.range.minlength);
		memcpy(p->part.range.maxvalue, op->part.range.maxvalue, op->part.range.maxlength);
		p->part.range.minlength = op->part.range.minlength;
		p->part.range.maxlength = op->part.range.maxlength;
	} else if (isListPartitionTable(mt)) {
		p->part.values = list_create((fdestroy) &part_value_destroy);
		for (node *n = op->part.values->h ; n ; n = n->next) {
			sql_part_value *prev = (sql_part_value*) n->data, *nextv = ZNEW(sql_part_value);
			nextv->value = NEW_ARRAY(char, prev->length);
			memcpy(nextv->value, prev->value, prev->length);
			nextv->length = prev->length;
			list_append(p->part.values, nextv);
		}
	}
	if (isGlobal(mt) && (res = os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))))
		return res;
	*pres = p;
	return res;
}

static int
trigger_dup(sql_trans *tr, sql_trigger *i, sql_table *t, sql_trigger **tres)
{
	sql_trigger *nt = ZNEW(sql_trigger);
	int res = LOG_OK;

	base_init(NULL, &nt->base, i->base.id, 0, i->base.name);

	nt->columns = list_create((fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = i->time;
	nt->orientation = i->orientation;
	nt->event = i->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (i->old_name)
		nt->old_name =_STRDUP(i->old_name);
	if (i->new_name)
		nt->new_name =_STRDUP(i->new_name);
	if (i->condition)
		nt->condition =_STRDUP(i->condition);
	nt->statement =_STRDUP(i->statement);

	for (node *n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nt->columns, kc_dup(okc, t));
	}
	if (isGlobal(t) && (res = os_add(t->s->triggers, tr, nt->base.name, dup_base(&nt->base))))
		return res;
	*tres = nt;
	return res;
}

static int
table_dup(sql_trans *tr, sql_table *ot, sql_schema *s, const char *name,
		  sql_table **tres, bool dup_global_as_global)
{
	sqlstore *store = tr->store;
	sql_table *t = ZNEW(sql_table);
	node *n;
	int res = LOG_OK;

	if (t == NULL)
		return LOG_ERR;
	base_init(NULL, &t->base, ot->base.id, 0, name?name:ot->base.name);
	t->type = ot->type;
	t->system = ot->system;
	t->bootstrap = ot->bootstrap;
	t->persistence = (s || dup_global_as_global)?ot->persistence:SQL_LOCAL_TEMP;
	t->commit_action = ot->commit_action;
	t->access = ot->access;
	t->query = (ot->query) ?_STRDUP(ot->query) : NULL;
	t->properties = ot->properties;

	t->columns = ol_new(NULL, (destroy_fptr) &column_destroy, store);
	t->idxs = ol_new(NULL, (destroy_fptr) &idx_destroy, store);
	t->keys = ol_new(NULL, (destroy_fptr) &key_destroy, store);
	t->triggers = ol_new(NULL, (destroy_fptr) &trigger_destroy, store);
	if (ot->members)
		t->members = list_create((fdestroy) &part_destroy);

	t->pkey = NULL;
	t->s = s ? s : tr->tmp;
	t->sz = ot->sz;
	ATOMIC_PTR_INIT(&t->data, NULL);

	if (isPartitionedByExpressionTable(ot)) {
		t->part.pexp = ZNEW(sql_expression);
		t->part.pexp->exp =_STRDUP(ot->part.pexp->exp);
		t->part.pexp->type = ot->part.pexp->type;
		t->part.pexp->cols = list_create((fdestroy) &int_destroy);
		for (n = ot->part.pexp->cols->h; n; n = n->next) {
			int *nid = MNEW(int);
			*nid = *(int *) n->data;
			list_append(t->part.pexp->cols, nid);
		}
	}
	if (ot->columns)
		for (n = ol_first_node(ot->columns); n; n = n->next) {
			sql_column *c = NULL;

			if ((res = column_dup(tr, n->data, t, &c)) || (res = ol_add(t->columns, &c->base)))
				goto cleanup;
			if (isPartitionedByColumnTable(ot) && ot->part.pcol->base.id == c->base.id)
				t->part.pcol = c;
		}
	if (ot->idxs)
		for (n = ol_first_node(ot->idxs); n; n = n->next) {
			sql_idx *i = NULL;

			if ((res = idx_dup(tr, n->data, t, &i)) || (res = ol_add(t->idxs, &i->base)))
				goto cleanup;
		}
	if (ot->keys)
		for (n = ol_first_node(ot->keys); n; n = n->next) {
			sql_key *k = NULL;

			if ((res = key_dup(tr, n->data, t, &k)) || (res = ol_add(t->keys, &k->base)))
				goto cleanup;
		}
	if (ot->triggers)
		for (n = ol_first_node(ot->triggers); n; n = n->next) {
			sql_trigger *tri = NULL;

			if ((res = trigger_dup(tr, n->data, t, &tri)) || (res = ol_add(t->triggers, &tri->base)))
				goto cleanup;
		}
	if (ot->members)
		for (n = ot->members->h; n; n = n->next) {
			sql_part *pt = NULL;

			if ((res = part_dup(tr, n->data, t, &pt)))
				goto cleanup;
			list_append(t->members, pt);
		}
	if (isTable(t)) {
		if (isTempTable(t)) {
			if ((res = store->storage_api.create_del(tr, t)))
				goto cleanup;
		} else {
			ATOMIC_PTR_SET(&t->data, store->storage_api.del_dup(ot));
		}
	}
	if ((res = os_add(isLocalTemp(t) ? tr->localtmps : t->s->tables, tr, t->base.name, &t->base)))
		goto cleanup;

cleanup:
	if (res) {
		t = NULL;
	}
	*tres = t;
	return res;
}

sql_table *
globaltmp_instantiate(sql_trans *tr, sql_table *ot)
{
	assert(isGlobal(ot)&& isTempTable(ot));
	sql_table *t = NULL;
	if (table_dup(tr, ot, NULL, NULL, &t, false) == LOG_OK)
		return t;
	return NULL;
}

static int
new_table(sql_trans *tr, sql_table *t, sql_table **tres)
{
	int res = LOG_OK;
	if (!isGlobalTemp(t))
		t = find_sql_table(tr, t->s, t->base.name); /* could have changed by depending changes */

	if (!isLocalTemp(t) && !isNew(t) && !os_obj_intransaction(t->s->tables, tr, &t->base))
		res = table_dup(tr, t, t->s, NULL, tres, true);
	else
		*tres = t;
	return res;
}

static sql_arg *
arg_dup(sql_trans *tr, sql_schema *s, sql_arg *oa)
{
	sql_arg *a = ZNEW(sql_arg);

	if (a) {
		a->name =_STRDUP(oa->name);
		a->inout = oa->inout;
		dup_sql_type(tr, s, &(oa->type), &(a->type));
	}
	return a;
}

static int
func_dup(sql_trans *tr, sql_func *of, sql_schema *s)
{
	sql_func *f = ZNEW(sql_func);

	/* 'func_dup' is aimed at FUNC_LANG_SQL functions ONLY, so f->imp and f->instantiated won't be set */
	base_init(NULL, &f->base, of->base.id, 0, of->base.name);
	f->mod =_STRDUP(of->mod);
	f->type = of->type;
	f->lang = of->lang;
	f->semantics = of->semantics;
	f->side_effect = of->side_effect;
	f->varres = of->varres;
	f->vararg = of->vararg;
	f->fix_scale = of->fix_scale;
	f->system = of->system;
	f->private = of->private;
	f->order_required = of->order_required;
	f->opt_order = of->opt_order;
	f->query = (of->query)?_STRDUP(of->query):NULL;
	f->s = s;
	f->sa = NULL;

	f->ops = list_create((fdestroy) &arg_destroy);
	for (node *n=of->ops->h; n; n = n->next)
		list_append(f->ops, arg_dup(tr, s, n->data));
	if (of->res) {
		f->res = list_create((fdestroy) &arg_destroy);
		for (node *n=of->res->h; n; n = n->next)
			list_append(f->res, arg_dup(tr, s, n->data));
	}

	return os_add(s->funcs, tr, f->base.name, &f->base);
}

static int
store_reset_sql_functions(sql_trans *tr, sqlid id)
{
	sqlstore *store = tr->store;
	int res = LOG_OK, sql_lang = (int) FUNC_LANG_SQL; /* functions other than SQL don't require to be instantiated again */
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *deps = find_sql_table(tr, syss, "dependencies");
	rids *sql_funcs = NULL, *depends = NULL, *joined = NULL;

	/* Find dependencies from the object */
	depends = store->table_api.rids_select(tr, find_sql_column(deps, "id"), &id, &id, NULL);
	if (depends == NULL)
		return -1;
	if (store->table_api.rids_empty(depends)) { /* nothing depends on the object, return */
		store->table_api.rids_destroy(depends);
		return res;
	}
	/* Get SQL functions */
	sql_table *funcs = find_sql_table(tr, syss, "functions");
	sql_column *func_id = find_sql_column(funcs, "id");
	if (!(sql_funcs = store->table_api.rids_select(tr, find_sql_column(funcs, "language"), &sql_lang, &sql_lang, NULL))) {
		store->table_api.rids_destroy(depends);
		return -1;
	}
	/* Do the semijoin */
	joined = store->table_api.rids_semijoin(tr, sql_funcs, func_id, depends, find_sql_column(deps, "depend_id"));
	store->table_api.rids_destroy(depends);
	if (!joined) {
		store->table_api.rids_destroy(sql_funcs);
		return -1;
	}

	for (oid rid = store->table_api.rids_next(joined); !is_oid_nil(rid); rid = store->table_api.rids_next(joined)) {
		sqlid fid = store->table_api.column_find_sqlid(tr, func_id, rid);
		sql_func *f = sql_trans_find_func(tr, fid); /* could have changed by depending changes */
		/* if it is on the same transaction, then don't dup it again */
		if (isNew(f) || os_obj_intransaction(f->s->funcs, tr, &f->base)) {
			if (f->instantiated) {
				/* clean backend code */
				backend_freecode(sql_shared_module_name, 0, f->imp);
				f->instantiated = FALSE;
				_DELETE(f->imp);
			}
		} else if ((res = func_dup(tr, f, f->s))) {
			store->table_api.rids_destroy(joined);
			return res;
		}
	}
	store->table_api.rids_destroy(joined);
	return res;
}

int
sql_trans_copy_key( sql_trans *tr, sql_table *t, sql_key *k, sql_key **kres)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	sql_table *syskc = find_sql_table(tr, syss, "objects");
	int neg = -1, action = -1, nr, res = LOG_OK;
	node *n;
	sql_key *nk;
	sql_table *dup = NULL;

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	if ((res = key_dup(tr, k, t, &nk)))
		return res;
	sql_fkey *fk = (sql_fkey*)nk;
	if ((res = ol_add(t->keys, &nk->base)))
		return res;

	if (nk->type == fkey)
		action = (fk->on_update<<8) + fk->on_delete;

	char *strnil = (char*)ATOMnilptr(TYPE_str);
	if ((res = store->table_api.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, &nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey : &neg, &action
	, (nk->type == ckey)? &nk->check : &strnil )))
		return res;

	if (nk->type == fkey) {
		sql_key *rkey = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)k)->rkey);
		if (!rkey)
			return LOG_ERR;

		if ((res = sql_trans_create_dependency(tr, rkey->base.id, nk->base.id, FKEY_DEPENDENCY)))
			return res;
		/* TODO this has to be cleaned out once the sql_cat.c cleanup is done */
		if (!isNew(rkey) && (res = sql_trans_add_dependency(tr, rkey->base.id, ddl)))
			return res;
		if (!isNew(rkey) && (res = sql_trans_add_dependency(tr, rkey->t->base.id, ddl))) /* this dependency is needed for merge tables */
			return res;
		if (!isNew(rkey) && isGlobal(rkey->t) && !isGlobalTemp(rkey->t) && (res = sql_trans_add_dependency(tr, rkey->t->base.id, dml))) /* disallow concurrent updates on other key */
			return res;
		if ((res = store_reset_sql_functions(tr, rkey->t->base.id))) /* reset sql functions depending on the table */
			return res;
	}

	for (n = nk->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *kc = n->data;

		if ((res = store->table_api.table_insert(tr, syskc, &nk->base.id, &kc->c->base.name, &nr, ATOMnilptr(TYPE_int))))
			return res;

		if (nk->type == fkey) {
			if ((res = sql_trans_create_dependency(tr, kc->c->base.id, nk->base.id, FKEY_DEPENDENCY)))
				return res;
		} else if (nk->type == ukey || nk->type == ckey) {
			if ((res = sql_trans_create_dependency(tr, kc->c->base.id, nk->base.id, KEY_DEPENDENCY)))
				return res;
		} else if (nk->type == pkey) {
			if ((res = sql_trans_create_dependency(tr, kc->c->base.id, nk->base.id, KEY_DEPENDENCY)))
				return res;
			if ((res = sql_trans_alter_null(tr, kc->c, 0)))
				return res;
		}

		/* TODO this has to be cleaned out too */
		if (!isNew(kc->c) && (res = sql_trans_add_dependency(tr, kc->c->base.id, ddl)))
			return res;
	}
	if ((res = store_reset_sql_functions(tr, t->base.id))) /* reset sql functions depending on the table */
		return res;

	/* TODO this has to be cleaned out too */
	if (!isNew(t) && (res = sql_trans_add_dependency(tr, t->base.id, ddl))) /* this dependency is needed for merge tables */
		return res;
	if (!isNew(t) && isGlobal(t) && !isGlobalTemp(t) && (res = sql_trans_add_dependency(tr, t->base.id, dml))) /* disallow concurrent updates on t */
		return res;
	if (kres)
		*kres = nk;
	return res;
}

int
sql_trans_copy_idx( sql_trans *tr, sql_table *t, sql_idx *i, sql_idx **ires)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(tr, syss, "idxs");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	node *n;
	int nr, res = LOG_OK, ncols = list_length(i->columns);
	sql_table *dup = NULL;

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	sql_idx *ni = ZNEW(sql_idx);
	base_init(NULL, &ni->base, i->base.id?i->base.id:next_oid(tr->store), true, i->base.name);
	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;
	ATOMIC_PTR_INIT(&ni->data, NULL);

	for (n = i->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(ni->columns, ic = kc_dup(okc, t));
		if (i->key && hash_index(i->type))
			ic->c->unique = (ncols == 1) ? 2 : MAX(ic->c->unique, 1);
		if ((res = store->table_api.table_insert(tr, sysic, &ni->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int)))) {
			idx_destroy(store, ni);
			return res;
		}
		if ((res = sql_trans_create_dependency(tr, ic->c->base.id, ni->base.id, INDEX_DEPENDENCY))) {
			idx_destroy(store, ni);
			return res;
		}
	}
	if ((res = ol_add(t->idxs, &ni->base)))
		return res;

	if (isGlobal(t) && (res = os_add(t->s->idxs, tr, ni->base.name, dup_base(&ni->base))))
		return res;
	if ((res = store_reset_sql_functions(tr, t->base.id))) /* reset sql functions depending on the table */
		return res;

	/* this dependency is needed for merge tables */
	if (!isNew(t) && (res = sql_trans_add_dependency(tr, t->base.id, ddl)))
		return res;
	if (!isNew(t) && isGlobal(t) && !isGlobalTemp(t) && (res = sql_trans_add_dependency(tr, t->base.id, dml)))
		return res;

	if (isDeclaredTable(i->t))
		if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
			if ((res = store->storage_api.create_idx(tr, ni))) {
				return res;
			}
	if (!isDeclaredTable(t))
		if ((res = store->table_api.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, &ni->base.name))) {
			return res;
		}

	if (ires)
		*ires = ni;
	return res;
}

int
sql_trans_copy_trigger( sql_trans *tr, sql_table *t, sql_trigger *tri, sql_trigger **tres)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systr = find_sql_table(tr, syss, "triggers");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	node *n;
	int nr, res = LOG_OK;
	sql_trigger *nt = ZNEW(sql_trigger);
	char *strnil = (char*)ATOMnilptr(TYPE_str);

	base_init(NULL, &nt->base, tri->base.id?tri->base.id:next_oid(tr->store), true, tri->base.name);
	nt->columns = list_create((fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = tri->time;
	nt->orientation = tri->orientation;
	nt->event = tri->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (tri->old_name)
		nt->old_name =_STRDUP(tri->old_name);
	if (tri->new_name)
		nt->new_name =_STRDUP(tri->new_name);
	if (tri->condition)
		nt->condition =_STRDUP(tri->condition);
	nt->statement =_STRDUP(tri->statement);

	for (n = tri->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(nt->columns, ic = kc_dup(okc, t));
		if ((res = store->table_api.table_insert(tr, sysic, &nt->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int)))) {
			trigger_destroy(store, nt);
			return res;
		}
		if ((res = sql_trans_create_dependency(tr, ic->c->base.id, nt->base.id, TRIGGER_DEPENDENCY))) {
			trigger_destroy(store, nt);
			return res;
		}
	}
	if ((res = ol_add(t->triggers, &nt->base)))
		return res;

	assert(isGlobal(t));
	if ((res = os_add(t->s->triggers, tr, nt->base.name, dup_base(&nt->base))))
		return res;
	if ((res = store_reset_sql_functions(tr, t->base.id))) /* reset sql functions depending on the table */
		return res;

	if (!isDeclaredTable(t))
		if ((res = store->table_api.table_insert(tr, systr, &nt->base.id, &nt->base.name, &t->base.id, &nt->time, &nt->orientation,
				&nt->event, (nt->old_name)?&nt->old_name:&strnil, (nt->new_name)?&nt->new_name:&strnil,
				(nt->condition)?&nt->condition:&strnil, &nt->statement))) {
			return res;
		}

	if (tres)
		*tres = nt;
	return res;
}

static int
type_digits(sql_subtype *type)
{
	int digits = type->digits;

	if (digits && type->type->eclass == EC_NUM) {
		if(type->type->localtype == TYPE_bte) {
			digits = 7;
		} else if(type->type->localtype == TYPE_sht) {
			digits = 15;
		} else if(type->type->localtype == TYPE_int) {
			digits = 31;
		} else if(type->type->localtype == TYPE_lng) {
			digits = 63;
#ifdef HAVE_HGE
		} else if(type->type->localtype == TYPE_hge) {
			digits = 127;
#endif
		}
	}
	return digits;
}

int
sql_trans_copy_column( sql_trans *tr, sql_table *t, sql_column *c, sql_column **cres)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
	sql_table *dup = NULL;
	int res = LOG_OK;

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	sql_column *col = ZNEW(sql_column);
	base_init(NULL, &col->base, c->base.id?c->base.id:next_oid(tr->store), true, c->base.name);
	dup_sql_type(tr, t->s, &(c->type), &(col->type));
	col->def = NULL;
	if (c->def)
		col->def =_STRDUP(c->def);
	col->null = c->null;
	col->colnr = c->colnr;
	col->unique = c->unique;
	col->t = t;
	col->storage_type = NULL;
	if (c->storage_type)
		col->storage_type =_STRDUP(c->storage_type);

	if ((res = ol_add(t->columns, &col->base)))
		return res;

	/* this dependency is needed for merge tables */
	if (!isNew(t) && (res = sql_trans_add_dependency(tr, t->base.id, ddl)))
		return res;
	if (!isNew(t) && isGlobal(t) && !isGlobalTemp(t) && (res = sql_trans_add_dependency(tr, t->base.id, dml)))
		return res;

	ATOMIC_PTR_INIT(&col->data, NULL);
	if (isDeclaredTable(c->t))
		if (isTable(t))
			if ((res = store->storage_api.create_col(tr, col))) {
				return res;
			}

	if (!isDeclaredTable(t)) {
		char *strnil = (char*)ATOMnilptr(TYPE_str);
		int digits = type_digits(&col->type);
		if ((res = store->table_api.table_insert(tr, syscolumn, &col->base.id, &col->base.name, &col->type.type->base.name,
					&digits, &col->type.scale, &t->base.id,
					(col->def) ? &col->def : &strnil, &col->null, &col->colnr,
					(col->storage_type) ? &col->storage_type : &strnil))) {
			return res;
		}
		if (c->type.type->s) { /* column depends on type */
			if ((res = sql_trans_create_dependency(tr, c->type.type->base.id, col->base.id, TYPE_DEPENDENCY))) {
				return res;
			}
			if (!isNew(c->type.type) && (res = sql_trans_add_dependency(tr, c->type.type->base.id, ddl))) {
				return res;
			}
		}
	}
	if (cres)
		*cres = col;
	return res;
}

static int
clean_predicates_and_propagate_to_parent(sql_trans *tr)
{
	int res = LOG_OK;

	if (!list_empty(tr->predicates)) {
		if (tr->parent) { /* propagate to the parent */
			for(node *n=tr->predicates->h; n && res == LOG_OK ; n = n->next) {
				pl *p = (pl*) n->data;
				atom *e1 = p->r ? atom_copy(NULL, p->r) : NULL, *e2 = p->f ? atom_copy(NULL, p->f) : NULL;

				res = sql_trans_add_predicate(tr->parent, p->c, p->cmp, e1, e2, p->anti, p->semantics);
			}
		}
		list_destroy(tr->predicates);
		tr->predicates = NULL;
	}
	if (!list_empty(tr->dependencies)) {
		if (tr->parent) { /* propagate to the parent */
			for(node *n=tr->dependencies->h; n && res == LOG_OK ; n = n->next) {
				sql_dependency_change *dp = (sql_dependency_change*)n->data;
				res = sql_trans_add_dependency(tr->parent, dp->objid, dp->type);
			}
		}
		list_destroy(tr->dependencies);
		tr->dependencies = NULL;
	}
	if (!list_empty(tr->depchanges)) {
		if (tr->parent) { /* propagate to the parent */
			for(node *n=tr->depchanges->h; n && res == LOG_OK ; n = n->next) {
				sql_dependency_change *dp = (sql_dependency_change*)n->data;
				res = sql_trans_add_dependency_change(tr->parent, dp->objid, dp->type);
			}
		}
		list_destroy(tr->depchanges);
		tr->depchanges = NULL;
	}
	return res;
}

static void
sql_trans_rollback(sql_trans *tr, bool commit_lock)
{
	sqlstore *store = tr->store;
	if (!list_empty(tr->changes)) {
		struct os_iter oi;
		os_iterator(&oi, tr->localtmps, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table *) b;
			if (t->commit_action == CA_DROP && !b->deleted) {
				(void) sql_trans_drop_table_id(tr, t->s, b->id, DROP_RESTRICT);// TODO transaction_layer_revamp: fix result code sql_trans_drop_table_id
			}
		}
		/* revert the change list */
		list *nl = tr->changes;
		list_revert(nl);

		/* rollback */
		if (!commit_lock)
			MT_lock_set(&store->commit);
		store_lock(store);
		ulng oldest = store_oldest(store, tr);
		ulng commit_ts = store_get_timestamp(store); /* use most recent timestamp such that we can cleanup safely */
		for(node *n=nl->h; n; n = n->next) {
			sql_change *c = n->data;

			if (!c)
				continue;
			if (c->commit)
				c->commit(tr, c, 0 /* ie rollback */, oldest);
			c->ts = commit_ts;
		}
		store_pending_changes(store, oldest, tr);
		for(node *n=nl->h; n; n = n->next) {
			sql_change *c = n->data;

			if (!c)
				continue;
			if (!c->cleanup) {
				_DELETE(c);
			} else if (c->cleanup && !c->cleanup(store, c, oldest)) {
				/* TODO change to node stealing (no allocs here) */
				store->changes = list_add(store->changes, c);
			} else
				_DELETE(c);
		}
		store_unlock(store);
		if (!commit_lock)
			MT_lock_unset(&store->commit);
		list_destroy(tr->changes);
		tr->changes = NULL;
		tr->logchanges = 0;
	} else {
		if (commit_lock || MT_lock_try(&store->commit)) {
			store_lock(store);
			ulng oldest = store_oldest(store, tr);
			store_pending_changes(store, oldest, tr);
			store_unlock(store);
			if (!commit_lock)
				MT_lock_unset(&store->commit);
		}
	}

	if (!list_empty(tr->predicates)) {
		list_destroy(tr->predicates);
		tr->predicates = NULL;
	}
	if (!list_empty(tr->dependencies)) {
		list_destroy(tr->dependencies);
		tr->dependencies = NULL;
	}
	if (!list_empty(tr->depchanges)) {
		list_destroy(tr->depchanges);
		tr->depchanges = NULL;
	}
}

sql_trans *
sql_trans_destroy(sql_trans *tr)
{
	sql_trans *res = tr->parent;

	TRC_DEBUG(SQL_STORE, "Destroy transaction: %p\n", tr);
	_DELETE(tr->name);
	assert(!tr->active || tr->parent);
	if (!list_empty(tr->changes))
		sql_trans_rollback(tr, false);
	sqlstore *store = tr->store;
	os_destroy(tr->localtmps, store);
	MT_lock_destroy(&tr->lock);
	if (!list_empty(tr->dropped))
		list_destroy(tr->dropped);
	if (!list_empty(tr->predicates))
		list_destroy(tr->predicates);
	_DELETE(tr);
	return res;
}

static sql_trans *
sql_trans_create_(sqlstore *store, sql_trans *parent, const char *name)
{
	if (name && !parent)		/* unlikely */
		return NULL;

	sql_trans *tr = ZNEW(sql_trans);

	if (!tr)
		return NULL;
	MT_lock_init(&tr->lock, "trans_lock");
	tr->parent = parent;
	if (name) {
		_DELETE(parent->name);
		parent->name = _STRDUP(name);
	}

	if (!parent) {
		tr->localtmps = os_new(NULL, (destroy_fptr) &table_destroy, true, true, false, false, store);
	}
	else {
		tr->localtmps = os_dup(parent->localtmps);
	}

	store_lock(store);
	tr->store = store;
	tr->tid = store_transaction_id(store);
	tr->cat = store->cat;
	if (!tr->cat) {
		store->cat = tr->cat = ZNEW(sql_catalog);
		store->cat->schemas = os_new(NULL, (destroy_fptr) &schema_destroy, false, true, true, true, store);
		store->cat->objects = os_new(NULL, (destroy_fptr) &key_destroy, false, false, true, false, store);
		ATOMIC_INIT(&store->cat->schema_version, 0);
	}
	tr->tmp = store->tmp;
	TRC_DEBUG(SQL_STORE, "New transaction: %p\n", tr);
	store_unlock(store);
	return tr;
}

static int
schema_dup(sql_trans *tr, sql_schema *s, const char *name, sql_schema **rs)
{
	sql_schema *ns = ZNEW(sql_schema);
	int res = LOG_OK;

	base_init(NULL, &ns->base, s->base.id, 0, name);
	ns->auth_id = s->auth_id;
	ns->owner = s->owner;
	ns->system = s->system;

	sqlstore *store = tr->store;
	assert(!isTempSchema(s)); // TODO transaction_layer_revamp: check if this is really true
	ns->tables = os_new(NULL, (destroy_fptr) &table_destroy, false, true, true, false, store);
	ns->seqs = os_new(NULL, (destroy_fptr) &seq_destroy, false, true, true, false, store);
	ns->keys = os_new(NULL, (destroy_fptr) &key_destroy, false, true, true, false, store);
	ns->idxs = os_new(NULL, (destroy_fptr) &idx_destroy, false, true, true, false, store);
	ns->triggers = os_new(NULL, (destroy_fptr) &trigger_destroy, false, true, true, false, store);
	ns->parts = os_new(NULL, (destroy_fptr) &part_destroy, false, false, true, false, store);

	/* table_dup will dup keys, idxs, triggers and parts */
	struct os_iter oi;
	os_iterator(&oi, s->tables, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_table *t = NULL;
		if ((res = table_dup(tr, (sql_table*)b, ns, NULL, &t, true))) {
			schema_destroy(tr->store, ns);
			return res;
		}
	}

	/* we can share the funcs and types */
	ns->funcs = os_dup(s->funcs);
	ns->types = os_dup(s->types);
	ns->store = s->store;
	ns->internal = NULL;
	*rs = ns;
	return res;
}

sql_trans *
sql_trans_create(sqlstore *store, sql_trans *parent, const char *name)
{
	sql_trans *tr = sql_trans_create_(store, parent, name);
	if (tr) {
		tr->ts = store_timestamp(store);
		tr->active = 1;
	}
	return tr;
}

static int
sql_trans_valid(sql_trans *tr)
{
	int ok = LOG_OK;
	sqlstore *store = tr->store;

	if (!list_empty(tr->predicates)) {
		/* for each predicate check if that table/column has changes */
		for(node *n = tr->predicates->h; n; n = n->next) {
			pl *p = n->data;
			sql_column *c = p->c;

			if (c->t && isTable(c->t) && !isNew(c) && !isTempTable(c->t)) {
				if ((ok = store->storage_api.tab_validate(tr, c->t, 0)))
					break;
			}
		}
	}
	return ok;
}

static inline int
transaction_add_hash_entry(sql_hash *h, sqlid id, sql_dependency_change_type tpe, ulng ts)
{
	sql_dependency_change *next_change = MNEW(sql_dependency_change);

	if (!next_change)
		return LOG_ERR;
	*next_change = (sql_dependency_change) {
		.objid = id,
		.type = tpe,
		.ts = ts
	};

	int key = h->key(next_change);
	sql_hash_e *he = h->buckets[key&(h->size-1)];

	for (; he ; he = he->chain) { /* find if the entry is already present */
		sql_dependency_change *schange = (sql_dependency_change*) he->value;

		if (schange->objid == id && schange->ts == ts && schange->type == tpe) {
			_DELETE(next_change);
			return LOG_OK;
		}
	}

	if (!hash_add(h, key, next_change)) {
		_DELETE(next_change);
		return LOG_ERR;
	}
	return LOG_OK;
}

static int
transaction_check_dependencies_and_removals(sql_trans *tr)
{
	int ok = LOG_OK;
	sqlstore *store = tr->store;

	/* test dependencies and removals crossed for conflicts */
	if (!list_empty(tr->dependencies) && !hash_empty(store->depchanges)) {
		for (node *n = tr->dependencies->h; n && ok == LOG_OK; n = n->next) {
			sql_dependency_change *lchange = (sql_dependency_change*) n->data;
			int key = store->depchanges->key(lchange);
			sql_hash_e *he = store->depchanges->buckets[key&(store->depchanges->size-1)];

			for (; he && ok == LOG_OK; he = he->chain) {
				sql_dependency_change *schange = (sql_dependency_change*) he->value;

				/* conflict only if transactions overlap */
				if (schange->ts >= tr->ts && lchange->objid == schange->objid && lchange->type == schange->type)
					ok = LOG_CONFLICT;
			}
		}
	}
	if (ok == LOG_OK && !list_empty(tr->depchanges) && !hash_empty(store->dependencies)) {
		for (node *n = tr->depchanges->h; n && ok == LOG_OK; n = n->next) {
			sql_dependency_change *lchange = (sql_dependency_change*) n->data;
			int key = store->dependencies->key(lchange);
			sql_hash_e *he = store->dependencies->buckets[key&(store->dependencies->size-1)];

			for (; he && ok == LOG_OK; he = he->chain) {
				sql_dependency_change *schange = (sql_dependency_change*) he->value;

				/* conflict only if transactions overlap */
				if (schange->ts >= tr->ts && lchange->objid == schange->objid && lchange->type == schange->type)
					ok = LOG_CONFLICT;
			}
		}
	}
	return ok;
}

int
sql_trans_commit(sql_trans *tr)
{
	int ok = LOG_OK;
	sqlstore *store = tr->store;

	if (!list_empty(tr->changes)) {
		struct os_iter oi;
		os_iterator(&oi, tr->localtmps, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table *) b;
			if (t->commit_action == CA_DROP && !b->deleted) {
				(void) sql_trans_drop_table_id(tr, t->s, b->id, DROP_RESTRICT);;// TODO transaction_layer_revamp: fix result code sql_trans_drop_table_id
			}
		}

		bool flush = false;
		ulng commit_ts = 0, oldest = 0, log_file_id = 0;

		MT_lock_set(&store->commit);

		if (!tr->parent && !list_empty(tr->predicates)) {
			ok = sql_trans_valid(tr);
			if (ok != LOG_OK) {
				sql_trans_rollback(tr, true);
				MT_lock_unset(&store->commit);
				return ok == LOG_CONFLICT ? SQL_CONFLICT : SQL_ERR;
			}
		}

		if (!tr->parent &&
			(!list_empty(tr->dependencies) || !list_empty(tr->depchanges))) {
			ok = transaction_check_dependencies_and_removals(tr);
			if (ok != LOG_OK) {
				sql_trans_rollback(tr, true);
				MT_lock_unset(&store->commit);
				return ok == LOG_CONFLICT ? SQL_CONFLICT : SQL_ERR;
			}
		}

		/* log changes should only be done if there is something to log */
		const bool log = !tr->parent && tr->logchanges > 0;

		if (log) {
			const lng min_changes = ATOMIC_GET(&GDKdebug) & TESTINGMASK ? 5 : 1000000;
			flush = (tr->logchanges > min_changes && list_empty(store->changes));
		}

		if (flush)
			MT_lock_set(&store->flush);
		if (log) {
			ok = store->logger_api.log_tstart(store, flush, &log_file_id); /* wal start */
			/* log */
			for(node *n=tr->changes->h; n && ok == LOG_OK; n = n->next) {
				sql_change *c = n->data;

				if (c->log && ok == LOG_OK)
					ok = c->log(tr, c);
			}
			if (ok == LOG_OK && !list_empty(store->seqchanges)) {
				sequences_lock(store);
				for(node *n = store->seqchanges->h; n; ) {
					node *next = n->next;
					log_store_sequence(store, n->data);
					list_remove_node(store->seqchanges, NULL, n);
					n = next;
				}
				sequences_unlock(store);
			}
			if (ok == LOG_OK && store->prev_oid != (sqlid) ATOMIC_GET(&store->obj_id)) {
				if (!flush)
					MT_lock_set(&store->flush);
				ok = store->logger_api.log_tsequence(store, OBJ_SID, (sqlid) ATOMIC_GET(&store->obj_id));
				if (!flush)
					MT_lock_unset(&store->flush);
			}
			store->prev_oid = (sqlid) ATOMIC_GET(&store->obj_id);


			if (ok == LOG_OK)
				ok = store->logger_api.log_tend(store); /* wal end */
		}
		store_lock(store);

		if (tr->parent) {
			commit_ts = oldest = tr->parent->tid;
			tr->parent->logchanges += tr->logchanges;
		} else {
			commit_ts = store_timestamp(store);
			oldest = store_oldest(store, tr);
		}
		tr->logchanges = 0;
		TRC_DEBUG(SQL_STORE, "Forwarding changes (" ULLFMT ", " ULLFMT ") -> " ULLFMT "\n", tr->tid, tr->ts, commit_ts);
		/* apply committed changes */
		if (ATOMIC_GET(&store->nr_active) == 1 && !tr->parent)
			oldest = commit_ts;
		store_pending_changes(store, oldest, tr);
		for(node *n=tr->changes->h; n && ok == LOG_OK; n = n->next) {
			sql_change *c = n->data;

			if (c->commit && ok == LOG_OK)
				ok = c->commit(tr, c, commit_ts, oldest);
			else
				c->obj->new = 0;
			c->ts = commit_ts;
		}
		/* propagate transaction dependencies to the storage only if other transactions are running */
		if (ok == LOG_OK && !tr->parent && ATOMIC_GET(&store->nr_active) > 1) {
			if (!list_empty(tr->dependencies)) {
				for (node *n = tr->dependencies->h; n && ok == LOG_OK; n = n->next) {
					sql_dependency_change *lchange = (sql_dependency_change*) n->data;
					ok = transaction_add_hash_entry(store->dependencies, lchange->objid, lchange->type, commit_ts);
				}
			}
			if (!list_empty(tr->depchanges)) {
				for (node *n = tr->depchanges->h; n && ok == LOG_OK; n = n->next) {
					sql_dependency_change *lchange = (sql_dependency_change*) n->data;
					ok = transaction_add_hash_entry(store->depchanges, lchange->objid, lchange->type, commit_ts);
				}
			}
		}
		/* garbage collect */
		for(node *n=tr->changes->h; n && ok == LOG_OK; ) {
			node *next = n->next;
			sql_change *c = n->data;

			n->data = NULL;
			if (c) {
				if (!c->cleanup || c->cleanup(store, c, oldest)) {
					_DELETE(c);
				} else if (tr->parent) { /* need to keep everything */
					tr->parent->changes = list_add(tr->parent->changes, c);
				} else {
					store->changes = list_add(store->changes, c);
				}
			}
			n = next;
		}
		tr->active = 2; /* small hack enabling to signal that this transaction has committed */
		tr->ts = commit_ts; /* fix profiler output */
		store_unlock(store);
		/* flush the log structure */
		if (log) {
			if (!flush)
				MT_lock_unset(&store->commit); /* release the commit log when flushing to disk */
			if (ok == LOG_OK)
				ok = store->logger_api.log_tflush(store, log_file_id, commit_ts); /* flush/sync */
			if (!flush)
				MT_lock_set(&store->commit); /* release the commit log when flushing to disk */
			if (flush)
				MT_lock_unset(&store->flush);
		}
		MT_lock_unset(&store->commit);
		if (ok == LOG_OK) {
			list_destroy(tr->changes);
			tr->changes = NULL;
		}
	} else if (ATOMIC_GET(&store->nr_active) == 1) { /* just me cleanup */
		MT_lock_set(&store->commit);
		store_lock(store);
		ulng oldest = store_timestamp(store);
		store_pending_changes(store, oldest, tr);
		store_unlock(store);
		MT_lock_unset(&store->commit);
	}

	if (ok == LOG_OK)
		ok = clean_predicates_and_propagate_to_parent(tr);

	return (ok==LOG_OK)?SQL_OK:SQL_ERR;
}

static sql_table *
find_table_by_columnid(sql_trans *tr, const char *schema, sqlid id)
{
	sqlstore *store = tr->store;
	sql_table *syscolumn = find_sql_table(tr, find_sql_schema(tr, schema), "_columns");

	oid rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"), &id, NULL);
	if (!is_oid_nil(rid)) {
		sqlid tid = store->table_api.column_find_sqlid(tr, find_sql_column(syscolumn, "table_id"), rid);
		return sql_trans_find_table(tr, tid);
	}
	return NULL;
}

static int
sql_trans_drop_all_dependencies(sql_trans *tr, sqlid id, sql_dependency type)
{
	sqlid dep_id=0, t_id = -1;
	sht dep_type = 0;
	list *dep = sql_trans_get_dependents(tr, id, type, NULL);
	node *n;
	int res = LOG_OK;

	if (!dep)
		return -1;

	n = dep->h;

	while (n && !res) {
		dep_id = *(sqlid*) n->data;
		dep_type = (sql_dependency) *(sht*) n->next->data;

		if (!list_find_id(tr->dropped, dep_id)) {

			switch (dep_type) {
				case SCHEMA_DEPENDENCY:
					res = sql_trans_drop_schema(tr, dep_id, DROP_CASCADE);
					break;
				case TABLE_DEPENDENCY:
				case VIEW_DEPENDENCY: {
					sql_table *t = sql_trans_find_table(tr, dep_id);
					if (t)
						res = sql_trans_drop_table_id(tr, t->s, dep_id, DROP_CASCADE);
					} break;
				case COLUMN_DEPENDENCY: {
					if ((t_id = sql_trans_get_dependency_type(tr, dep_id, TABLE_DEPENDENCY)) > 0) {
						sql_table *t = sql_trans_find_table(tr, dep_id);
						if (t && ol_length(t->columns) == 1) /* only column left, drop the table instead */
							res = sql_trans_drop_table_id(tr, t->s, t->base.id, DROP_CASCADE);
						else if (t)
							res = sql_trans_drop_column(tr, t, dep_id, DROP_CASCADE);
					}
					} break;
				case TRIGGER_DEPENDENCY: {
					sql_trigger *t = sql_trans_find_trigger(tr, dep_id);
					if (t && !list_find_id(tr->dropped, t->t->base.id)) /* table not yet dropped */
						 res = sql_trans_drop_trigger(tr, t->t->s, dep_id, DROP_CASCADE);
					} break;
				case KEY_DEPENDENCY:
				case FKEY_DEPENDENCY: {
					sql_key *k = sql_trans_find_key(tr, dep_id);
					if (k && !list_find_id(tr->dropped, k->t->base.id)) /* table not yet dropped */
						res = sql_trans_drop_key(tr, k->t->s, dep_id, DROP_CASCADE);
					} break;
				case INDEX_DEPENDENCY: {
					sql_idx *i = sql_trans_find_idx(tr, dep_id);
					if (i && !list_find_id(tr->dropped, i->t->base.id)) /* table not yet dropped */
						res = sql_trans_drop_idx(tr, i->t->s, dep_id, DROP_CASCADE);
					} break;
				case PROC_DEPENDENCY:
				case FUNC_DEPENDENCY: {
					sql_func *f = sql_trans_find_func(tr, dep_id);
					if (f)
						res = sql_trans_drop_func(tr, f->s, dep_id, DROP_CASCADE);
					} break;
				case TYPE_DEPENDENCY: {
					/* Unlike other dependencies, for type dependencies,
					   the dependent object depends on the type, rather the other way around.
					   At this moment, only functions or columns depend on types, so try both */
					sql_table *t = NULL;
					sql_func *f = sql_trans_find_func(tr, dep_id);
					if (f) {
						res = sql_trans_drop_func(tr, f->s, dep_id, DROP_CASCADE);
					} else if ((t = find_table_by_columnid(tr, "sys", dep_id)) ||
							   (t = find_table_by_columnid(tr, "tmp", dep_id))) {
						if (ol_length(t->columns) == 1) /* only column left, drop the table instead */
							res = sql_trans_drop_table_id(tr, t->s, t->base.id, DROP_CASCADE);
						else
							res = sql_trans_drop_column(tr, t, dep_id, DROP_CASCADE);
					}
					} break;
				case USER_DEPENDENCY:  /*TODO schema and users dependencies*/
					break;
			}
		}

		n = n->next->next;
	}
	list_destroy(dep);
	return res;
}

static int
sys_drop_ic(sql_trans *tr, sql_idx * i, sql_kc *kc)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysic, "id"), &i->base.id, find_sql_column(sysic, "name"), kc->c->base.name, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, sysic, rid)))
		return res;
	return res;
}

static int
sql_trans_drop_any_comment(sql_trans *tr, sqlid id)
{
	sqlstore *store = tr->store;
	sql_table *comments;
	sql_column *id_col;
	oid row;

	comments = find_sql_table(tr, find_sql_schema(tr, "sys"), "comments");
	if (!comments) /* for example during upgrades */
		return 0;

	id_col = find_sql_column(comments, "id");
	assert(id_col);

	row = store->table_api.column_find_row(tr, id_col, &id, NULL);
	if (!is_oid_nil(row) && store->table_api.table_delete(tr, comments, row))
		return -2;
	return 0;
}

static int
sys_drop_idx(sql_trans *tr, sql_idx * i, int drop_action)
{
	sqlstore *store = tr->store;
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(tr, syss, "idxs");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysidx, "id"), &i->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, sysidx, rid)))
		return res;
	if ((res = sql_trans_drop_any_comment(tr, i->base.id)))
		return res;
	for (n = i->columns->h; n; n = n->next) {
		sql_kc *ic = n->data;

		if (i->key && hash_index(i->type)) { /* update new column's unique value */
			int unique = 0;
			sqlid cid = ic->c->base.id;
			struct os_iter oi;

			os_iterator(&oi, i->t->s->idxs, tr, NULL);
			for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
				sql_idx *ti = (sql_idx*)b;

				if (ti->base.id != i->base.id && ti->key && hash_index(ti->type)) {
					bool found = false;
					for (node *m = ti->columns->h; m && !found; m = m->next) {
						sql_kc *tic = m->data;

						found |= tic->c->base.id == cid;
					}
					if (found)
						unique = MAX(unique, list_length(ti->columns) == 1 ? 2 : 1);
				}
			}
			ic->c->unique = unique;
		}
		if ((res = sys_drop_ic(tr, i, ic)))
			return res;
	}

	i->base.deleted = 1;
	if (!isNew(i) && !isTempTable(i->t))
		if ((res = store->storage_api.drop_idx(tr, (sql_idx*)dup_base(&i->base))))
			return res;

	/* remove idx from schema and table */
	if (isGlobal(i->t) && (res = os_del(i->t->s->idxs, tr, i->base.name, dup_base(&i->base))))
		return res;
	if (!isNew(i) && (res = sql_trans_add_dependency_change(tr, i->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, i->base.id)))
		return res;

	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, i->base.id, INDEX_DEPENDENCY)))
		return res;
	return res;
}

static int
sys_drop_kc(sql_trans *tr, sql_key *k, sql_kc *kc)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(tr, syss, "objects");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(syskc, "id"), &k->base.id, find_sql_column(syskc, "name"), kc->c->base.name, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, syskc, rid)))
		return res;
	return res;
}

static int
sys_drop_key(sql_trans *tr, sql_key *k, int drop_action)
{
	sqlstore *store = tr->store;
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(syskey, "id"), &k->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, syskey, rid)))
		return res;

	for (n = k->columns->h; n; n = n->next) {
		sql_kc *kc = n->data;
		if ((res = sys_drop_kc(tr, k, kc)))
			return res;
	}
	/* remove key from schema */
	if (isGlobal(k->t) && (res = os_del(k->t->s->keys, tr, k->base.name, dup_base(&k->base))))
		return res;
	if (isGlobal(k->t) && (res = os_del(tr->cat->objects, tr, k->base.name, dup_base(&k->base))))
		return res;
	if (k->t->pkey == (sql_ukey*)k)
		k->t->pkey = NULL;

	if (!isNew(k) && (res = sql_trans_add_dependency_change(tr, k->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, k->base.id)))
		return res;

	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, k->base.id, (k->type == fkey) ? FKEY_DEPENDENCY : KEY_DEPENDENCY)))
		return res;
	return res;
}

static int
sys_drop_tc(sql_trans *tr, sql_trigger * i, sql_kc *kc)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(tr, syss, "objects");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(systc, "id"), &i->base.id, find_sql_column(systc, "name"), kc->c->base.name, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, systc, rid)))
		return res;
	return res;
}

static int
sys_drop_sequence(sql_trans *tr, sql_sequence * seq, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(tr, syss, "sequences");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysseqs, "id"), &seq->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;

	if ((res = store->table_api.table_delete(tr, sysseqs, rid)))
		return res;
	if (!isNew(seq) && (res = sql_trans_add_dependency_change(tr, seq->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, seq->base.id)))
		return res;
	if ((res = sql_trans_drop_any_comment(tr, seq->base.id)))
		return res;
	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, seq->base.id, SEQ_DEPENDENCY)))
		return res;
	return res;
}

static int
sys_drop_default_object(sql_trans *tr, sql_column *col, int drop_action)
{
	const char next_value_for[] = "next value for ";
	int res = LOG_OK;

	/* Drop sequence for generated column if it's the case */
	if (col->def && !strncmp(col->def, next_value_for, strlen(next_value_for))) {
		sql_schema *s = NULL;
		sql_sequence *seq = NULL;
		char *schema = NULL, *seq_name = NULL;

		extract_schema_and_sequence_name(NULL, col->def + strlen(next_value_for), &schema, &seq_name);
		if (!schema || !seq_name || !(s = find_sql_schema(tr, schema))) {
			/* didn't find the schema so no generated sequence */
			_DELETE(schema);
			_DELETE(seq_name);
			return res;
		}

		seq = find_sql_sequence(tr, s, seq_name);
		_DELETE(schema);
		_DELETE(seq_name);
		if (seq && sql_trans_get_dependency_type(tr, seq->base.id, BEDROPPED_DEPENDENCY) > 0) {
			if ((res = sys_drop_sequence(tr, seq, drop_action)))
				return res;
			if ((res = os_del(s->seqs, tr, seq->base.name, dup_base(&seq->base))))
				return res;
		}
	}
	return res;
}

static int
sql_trans_drop_obj_priv(sql_trans *tr, sqlid obj_id)
{
	sqlstore *store = tr->store;
	sql_schema *sys = find_sql_schema(tr, "sys");
	sql_table *privs = find_sql_table(tr, sys, "privileges");
	int res = LOG_OK;

	assert(sys && privs);
	/* select privileges of this obj_id */
	rids *A = store->table_api.rids_select(tr, find_sql_column(privs, "obj_id"), &obj_id, &obj_id, NULL);
	if (A == NULL)
		return LOG_ERR;
	/* remove them */
	for(oid rid = store->table_api.rids_next(A); !is_oid_nil(rid) && res == LOG_OK; rid = store->table_api.rids_next(A))
		res = store->table_api.table_delete(tr, privs, rid);
	store->table_api.rids_destroy(A);
	return res;
}

static int
sys_drop_trigger(sql_trans *tr, sql_trigger * i)
{
	sqlstore *store = tr->store;
	node *n;
	sql_schema *syss = find_sql_schema(tr, (!i->t || isGlobal(i->t))?"sys":"tmp");
	sql_table *systrigger = find_sql_table(tr, syss, "triggers");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(systrigger, "id"), &i->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, systrigger, rid)))
		return res;

	if (i->t) {
		for (n = i->columns->h; n; n = n->next) {
			sql_kc *tc = n->data;

			if ((res = sys_drop_tc(tr, i, tc)))
				return res;
		}
	}
	/* remove trigger from schema */
	if ((res = os_del(i->t?i->t->s->triggers:syss->triggers, tr, i->base.name, dup_base(&i->base))))
		return res;
	if (!isNew(i) && (res = sql_trans_add_dependency_change(tr, i->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, i->base.id)))
		return res;
	return res;
}

static int
sys_drop_column(sql_trans *tr, sql_column *col, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"), &col->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if ((res = store->table_api.table_delete(tr, syscolumn, rid)))
		return res;
	if (!isNew(col) && (res = sql_trans_add_dependency_change(tr, col->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, col->base.id)))
		return res;
	if ((res = sql_trans_drop_any_comment(tr, col->base.id)))
		return res;
	if ((res = sql_trans_drop_obj_priv(tr, col->base.id)))
		return res;
	if ((res = sys_drop_default_object(tr, col, drop_action)))
		return res;

	col->base.deleted = 1;
	if (!isNew(col) && !isTempTable(col->t))
		if ((res = store->storage_api.drop_col(tr, (sql_column*)dup_base(&col->base))))
			return res;

	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, col->base.id, COLUMN_DEPENDENCY)))
		return res;
	if (col->type.type->s && (res = sql_trans_drop_dependency(tr, col->type.type->base.id, col->base.id, TYPE_DEPENDENCY)))
		return res;
	return res;
}

static int
sys_drop_keys(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;
	int res = LOG_OK;

	if (ol_length(t->keys))
		for (n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;

			if ((res = sys_drop_key(tr, k, drop_action)))
				return res;
		}
	return res;
}

static int
sys_drop_idxs(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;
	int res = LOG_OK;

	if (ol_length(t->idxs))
		for (n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *k = n->data;

			if ((res = sys_drop_idx(tr, k, drop_action)))
				return res;
		}
	return res;
}

static int
sys_drop_triggers(sql_trans *tr, sql_table *t)
{
	node *n;
	int res = LOG_OK;

	if (ol_length(t->triggers))
		for (n = ol_first_node(t->triggers); n; n = n->next) {
			sql_trigger *i = n->data;

			if ((res = sys_drop_trigger(tr, i)))
				return res;
		}
	return res;
}

static int
sys_drop_columns(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;
	int res = LOG_OK;

	if (ol_length(t->columns))
		for (n = t->columns->l->h; n; n = n->next) {
			sql_column *c = n->data;

			if ((res = sys_drop_column(tr, c, drop_action)))
				return res;
		}
	return res;
}

static int
sys_drop_part(sql_trans *tr, sql_part *pt, int drop_action)
{
	sqlstore *store = tr->store;
	sql_table *mt = pt->t;
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(tr, syss, "objects");
	oid obj_oid = store->table_api.column_find_row(tr, find_sql_column(sysobj, "id"), &pt->base.id, NULL);
	int res = LOG_OK;

	(void)drop_action;
	if (is_oid_nil(obj_oid))
		return -1;

	if ((res = store->table_api.table_delete(tr, sysobj, obj_oid)))
		return res;
	if (isRangePartitionTable(mt)) {
		if (isPartitionedByColumnTable(pt->t))
			col_set_range(tr, pt, false);
		sql_table *ranges = find_sql_table(tr, syss, "range_partitions");
		assert(ranges);
		oid rid = store->table_api.column_find_row(tr, find_sql_column(ranges, "table_id"), &pt->member, NULL);
		if ((res = store->table_api.table_delete(tr, ranges, rid)))
			return res;
	} else if (isListPartitionTable(mt)) {
		sql_table *values = find_sql_table(tr, syss, "value_partitions");
		assert(values);
		rids *rs = store->table_api.rids_select(tr, find_sql_column(values, "table_id"), &pt->member, &pt->member, NULL);
		if (rs == NULL)
			return LOG_ERR;
		for (oid rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
			if ((res = store->table_api.table_delete(tr, values, rid))) {
				store->table_api.rids_destroy(rs);
				return res;
			}
		}
		store->table_api.rids_destroy(rs);
	}
	/* merge table depends on part table */
	if (!isNew(pt) && (res = sql_trans_add_dependency_change(tr, mt->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependency(tr, pt->member, mt->base.id, TABLE_DEPENDENCY)))
		return res;

	if ((res = os_del(mt->s->parts, tr, pt->base.name, dup_base(&pt->base))))
		return res;
	return res;
}

static int
sys_drop_members(sql_trans *tr, sql_table *t, int drop_action)
{
	int res = LOG_OK;

	if (!list_empty(t->members)) {
		for (node *n = t->members->h; n; ) {
			sql_part *pt = n->data;

			n = n->next;
			if ((res = sys_drop_part(tr, pt, drop_action)))
				return res;
		}
	}
	return res;
}

static int
sys_drop_parts(sql_trans *tr, sql_table *t, int drop_action)
{
	int res = LOG_OK;

	for(sql_part *pt = partition_find_part(tr, t, NULL); pt; pt = partition_find_part(tr, t, pt)) {
		if ((res = sql_trans_del_table(tr, pt->t, t, drop_action)))
			return res;
	}
	return res;
}

static int
sys_drop_table(sql_trans *tr, sql_table *t, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(tr, syss, "_tables");
	sql_column *syscol = find_sql_column(systable, "id");
	oid rid = store->table_api.column_find_row(tr, syscol, &t->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;

	if ((res = store->table_api.table_delete(tr, systable, rid)))
		return res;
	if ((res = sys_drop_keys(tr, t, drop_action)))
		return res;
	if ((res = sys_drop_idxs(tr, t, drop_action)))
		return res;
	if ((res = sys_drop_triggers(tr, t)))
		return res;

	if (partition_find_part(tr, t, NULL))
		if ((res = sys_drop_parts(tr, t, drop_action)))
			return res;

	if (isMergeTable(t) || isReplicaTable(t))
		if ((res = sys_drop_members(tr, t, drop_action)))
			return res;

	if (isRangePartitionTable(t) || isListPartitionTable(t)) {
		sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
		assert(partitions);
		sql_column *pcols = find_sql_column(partitions, "table_id");
		assert(pcols);
		rids *rs = store->table_api.rids_select(tr, pcols, &t->base.id, &t->base.id, NULL);
		if (!rs)
			return -1;
		oid poid;
		if (rs == NULL)
			return LOG_ERR;
		if ((poid = store->table_api.rids_next(rs)) != oid_nil) {
			if ((res = store->table_api.table_delete(tr, partitions, poid))) {
				store->table_api.rids_destroy(rs);
				return res;
			}
		}
		store->table_api.rids_destroy(rs);
	}

	if ((res = sql_trans_drop_any_comment(tr, t->base.id)))
		return res;
	if (!isNew(t) && (res = sql_trans_add_dependency_change(tr, t->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, t->base.id)))
		return res;
	if ((res = sql_trans_drop_obj_priv(tr, t->base.id)))
		return res;
	if ((res = sys_drop_columns(tr, t, drop_action)))
		return res;

	if (isTable(t) && !isNew(t))
		if ((res = store->storage_api.drop_del(tr, t)))
			return res;

	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, t->base.id, !isView(t) ? TABLE_DEPENDENCY : VIEW_DEPENDENCY)))
		return res;
	return res;
}

static int
sys_drop_type(sql_trans *tr, sql_type *type, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_type = find_sql_table(tr, syss, "types");
	sql_column *sys_type_col = find_sql_column(sys_tab_type, "id");
	oid rid = store->table_api.column_find_row(tr, sys_type_col, &type->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;

	if ((res = store->table_api.table_delete(tr, sys_tab_type, rid)))
		return res;
	if (!isNew(type) && (res = sql_trans_add_dependency_change(tr, type->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, type->base.id)))
		return res;

	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, type->base.id, TYPE_DEPENDENCY)))
		return res;
	return res;
}

static int
sys_drop_func(sql_trans *tr, sql_func *func, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_func = find_sql_table(tr, syss, "functions");
	sql_column *sys_func_col = find_sql_column(sys_tab_func, "id");
	oid rid_func = store->table_api.column_find_row(tr, sys_func_col, &func->base.id, NULL);
	if (is_oid_nil(rid_func))
		return -1;
	sql_table *sys_tab_args = find_sql_table(tr, syss, "args");
	sql_column *sys_args_col = find_sql_column(sys_tab_args, "func_id");
	rids *args = store->table_api.rids_select(tr, sys_args_col, &func->base.id, &func->base.id, NULL);
	int res = LOG_OK;

	if (args == NULL)
		return LOG_ERR;
	for (oid r = store->table_api.rids_next(args); !is_oid_nil(r); r = store->table_api.rids_next(args)) {
		if ((res = store->table_api.table_delete(tr, sys_tab_args, r))) {
			store->table_api.rids_destroy(args);
			return res;
		}
	}
	store->table_api.rids_destroy(args);

	assert(!is_oid_nil(rid_func));
	if ((res = store->table_api.table_delete(tr, sys_tab_func, rid_func)))
		return res;

	if (!isNew(func) && (res = sql_trans_add_dependency_change(tr, func->base.id, ddl)))
		return res;
	if ((res = sql_trans_drop_dependencies(tr, func->base.id)))
		return res;
	if ((res = sql_trans_drop_any_comment(tr, func->base.id)))
		return res;
	if ((res = sql_trans_drop_obj_priv(tr, func->base.id)))
		return res;

	if (drop_action && (res = sql_trans_drop_all_dependencies(tr, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY)))
		return res;
	if (!func->vararg && func->ops) {
		for (node *n = func->ops->h; n; n = n->next) {
			sql_arg *a = n->data;

			if (a->type.type->s && (res = sql_trans_drop_dependency(tr, a->type.type->base.id, func->base.id, TYPE_DEPENDENCY)))
				return res;
		}
	}
	if (!func->varres && func->res) {
		for (node *n = func->res->h; n; n = n->next) {
			sql_arg *a = n->data;

			if (a->type.type->s && (res = sql_trans_drop_dependency(tr, a->type.type->base.id, func->base.id, TYPE_DEPENDENCY)))
				return res;
		}
	}
	return res;
}

static int
sys_drop_types(sql_trans *tr, sql_schema *s, int drop_action)
{
	int res = LOG_OK;
	struct os_iter oi;
	os_iterator(&oi, s->types, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_type *t = (sql_type*)b;

		if ((res = sys_drop_type(tr, t, drop_action)))
			return res;
	}
	return res;
}

static int
sys_drop_tables(sql_trans *tr, sql_schema *s, int drop_action)
{
	int res = LOG_OK;
	struct os_iter oi;
	os_iterator(&oi, s->tables, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_table *t = (sql_table*)b;

		if ((res = sys_drop_table(tr, t, drop_action)))
			return res;
	}
	return res;
}

static int
sys_drop_funcs(sql_trans *tr, sql_schema *s, int drop_action)
{
	int res = LOG_OK;
	struct os_iter oi;
	os_iterator(&oi, s->funcs, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_func *f = (sql_func*)b;

		if ((res = sys_drop_func(tr, f, drop_action)))
			return res;
	}
	return res;
}

static int
sys_drop_sequences(sql_trans *tr, sql_schema *s, int drop_action)
{
	int res = LOG_OK;
	struct os_iter oi;
	os_iterator(&oi, s->seqs, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_sequence *seq = (sql_sequence*)b;

		if ((res = sys_drop_sequence(tr, seq, drop_action)))
			return res;
	}
	return res;
}

int
sql_trans_create_type(sql_trans *tr, sql_schema *s, const char *sqlname, unsigned int digits, unsigned int scale, int radix, const char *impl)
{
	sqlstore *store = tr->store;
	sql_type *t;
	sql_table *systype;
	int localtype = ATOMindex(impl);
	sql_class eclass = EC_EXTERNAL;
	int eclass_cast = (int) eclass, res = LOG_OK;

	if (localtype < 0)
		return -4;
	t = ZNEW(sql_type);
	systype = find_sql_table(tr, find_sql_schema(tr, "sys"), "types");
	base_init(NULL, &t->base, next_oid(tr->store), true, sqlname);
	t->impl =_STRDUP(impl);
	t->digits = digits;
	t->scale = scale;
	t->radix = radix;
	t->eclass = eclass;
	t->localtype = localtype;
	t->s = s;

	if ((res = os_add(s->types, tr, t->base.name, &t->base)))
		return res;
	if ((res = store->table_api.table_insert(tr, systype, &t->base.id, &t->impl, &t->base.name, &t->digits, &t->scale, &radix, &eclass_cast, &s->base.id)))
		return res;
	return res;
}

int
sql_trans_drop_type(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sql_type *t = sql_trans_find_type(tr, s, id);
	int res = LOG_OK;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid* local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = t->base.id;
		list_append(tr->dropped, local_id);
	}

	if ((res = sys_drop_type(tr, t, drop_action)))
		return res;
	if ((res = os_del(s->types, tr, t->base.name, dup_base(&t->base))))
		return res;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

sql_func *
create_sql_func(sqlstore *store, allocator *sa, const char *func, list *args, list *res, sql_ftype type, sql_flang lang, const char *mod,
				const char *impl, const char *query, bit varres, bit vararg, bit system, bit side_effect, bit order_required, bit opt_order)
{
	sql_func *t = SA_ZNEW(sa, sql_func);

	base_init(sa, &t->base, next_oid(store), true, func);
	assert(mod);
	t->imp = (impl)?SA_STRDUP(sa, impl):NULL;
	t->mod = SA_STRDUP(sa, mod);
	t->type = type;
	t->lang = lang;
	t->instantiated = lang != FUNC_LANG_SQL && lang != FUNC_LANG_MAL;
	t->semantics = TRUE;
	t->side_effect = side_effect;
	t->varres = varres;
	t->vararg = vararg;
	t->ops = args;
	t->res = res;
	t->query = (query)?SA_STRDUP(sa, query):NULL;
	t->fix_scale = SCALE_EQ;
	t->s = NULL;
	t->system = system;
	t->order_required = order_required;
	t->opt_order = opt_order;
	return t;
}

int
sql_trans_create_func(sql_func **fres, sql_trans *tr, sql_schema *s, const char *func, list *args, list *ffres, sql_ftype type, sql_flang lang,
					  const char *mod, const char *impl, const char *query, bit varres, bit vararg, bit system, bit side_effect, bit order_required, bit opt_order)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysfunc = find_sql_table(tr, syss, "functions");
	sql_table *sysarg = find_sql_table(tr, syss, "args");
	node *n;
	int number = 0, ftype = (int) type, flang = (int) lang, res = LOG_OK;
	bit semantics = TRUE;
	bte order_spec = order_required?2:opt_order?1:0;

	sql_func *t = ZNEW(sql_func);
	base_init(NULL, &t->base, next_oid(tr->store), true, func);
	assert(mod);
	t->imp = (impl)?_STRDUP(impl):NULL;
	t->mod =_STRDUP(mod);
	t->type = type;
	t->lang = lang;
	t->instantiated = lang != FUNC_LANG_SQL && lang != FUNC_LANG_MAL;
	t->semantics = semantics;
	t->side_effect = side_effect;
	t->varres = varres;
	t->vararg = vararg;
	t->ops = list_create((fdestroy) &arg_destroy);
	t->fix_scale = SCALE_EQ;
	t->system = system;
	t->order_required = order_required;
	t->opt_order = opt_order;
	for (n=args->h; n; n = n->next)
		list_append(t->ops, arg_dup(tr, s, n->data));
	if (ffres) {
		t->res = list_create((fdestroy) &arg_destroy);
		for (n=ffres->h; n; n = n->next)
			list_append(t->res, arg_dup(tr, s, n->data));
	}
	t->query = (query)?_STRDUP(query):NULL;
	t->s = s;

	if ((res = os_add(s->funcs, tr, t->base.name, &t->base)))
		return res;
	if ((res = store->table_api.table_insert(tr, sysfunc, &t->base.id, &t->base.name, query?(char**)&query:&t->imp, &t->mod, &flang, &ftype, &side_effect,
			&varres, &vararg, &s->base.id, &system, &semantics, &order_spec)))
		return res;
	if (t->res) for (n = t->res->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid(tr->store);
		if ((res = store->table_api.table_insert(tr, sysarg, &id, &t->base.id, &a->name, &a->type.type->base.name, &a->type.digits, &a->type.scale, &a->inout, &number)))
			return res;
	}
	if (t->ops) for (n = t->ops->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid(tr->store);
		if ((res = store->table_api.table_insert(tr, sysarg, &id, &t->base.id, &a->name, &a->type.type->base.name, &a->type.digits, &a->type.scale, &a->inout, &number)))
			return res;
	}
	*fres = t;
	return res;
}

int
sql_trans_drop_func(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sql_base *b = os_find_id(s->funcs, tr, id);
	int res = LOG_OK;

	if (!b)
		return res;

	sql_func *func = (sql_func*)b;
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = func->base.id;
		list_append(tr->dropped, local_id);
	}

	if ((res = sys_drop_func(tr, func, DROP_CASCADE)))
		return res;
	if ((res = os_del(s->funcs, tr, func->base.name, dup_base(&func->base))))
		return res;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

static int
build_drop_func_list_item(sql_trans *tr, sql_schema *s, sqlid id)
{
	sql_base *b = os_find_id(s->funcs, tr, id);
	int res = LOG_OK;

	if (b) {
		sql_func *func = (sql_func*)b;
		if ((res = sys_drop_func(tr, func, DROP_CASCADE)))
			return res;
		if ((res = os_del(s->funcs, tr, func->base.name, dup_base(&func->base))))
			return res;
	}
	return res;
}

int
sql_trans_drop_all_func(sql_trans *tr, sql_schema *s, list *list_func, int drop_action)
{
	list *to_drop = NULL;
	int res = LOG_OK;

	(void) drop_action;
	if (!tr->dropped && !(tr->dropped = list_create((fdestroy) &id_destroy)))
		return -1;

	for (node *n = list_func->h; n ; n = n->next ) {
		sql_func *func = (sql_func *) n->data;

		if (!list_find_id(tr->dropped, func->base.id)) {
			sqlid *local_id = MNEW(sqlid);
			if (!local_id) {
				list_destroy(tr->dropped);
				tr->dropped = NULL;
				if (to_drop)
					list_destroy(to_drop);
				return -1;
			}
			if (!to_drop && !(to_drop = list_create(NULL))) {
				list_destroy(tr->dropped);
				tr->dropped = NULL;
				return -1;
			}
			*local_id = func->base.id;
			list_append(tr->dropped, local_id);
			list_append(to_drop, func);
		}
	}

	if (to_drop) {
		for (node *n = to_drop->h; n ; n = n->next ) {
			sql_func *func = (sql_func *) n->data;
			if ((res = build_drop_func_list_item(tr, s, func->base.id))) {
				list_destroy(tr->dropped);
				tr->dropped = NULL;
				list_destroy(to_drop);
				return res;
			}
		}
		list_destroy(to_drop);
	}

	list_destroy(tr->dropped);
	tr->dropped = NULL;
	return res;
}

int
sql_trans_create_schema(sql_trans *tr, const char *name, sqlid auth_id, sqlid owner, sqlid *schema_id_ptr)
{
	sqlstore *store = tr->store;
	sql_schema *s = ZNEW(sql_schema);
	sql_table *sysschema = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");
	int res = LOG_OK;

	base_init(NULL, &s->base, next_oid(tr->store), true, name);
	s->auth_id = auth_id;
	s->owner = owner;
	s->system = FALSE;
	assert(!isTempSchema(s)); // TODO transaction_layer_revamp: check if this is really true
	s->tables = os_new(NULL, (destroy_fptr) &table_destroy, false, true, true, false, store);
	s->types = os_new(NULL, (destroy_fptr) &type_destroy, false, true, true, false, store);
	s->funcs = os_new(NULL, (destroy_fptr) &func_destroy, false, false, false, false, store);
	s->seqs = os_new(NULL, (destroy_fptr) &seq_destroy, false, true, true, false, store);
	s->keys = os_new(NULL, (destroy_fptr) &key_destroy, false, true, true, false, store);
	s->idxs = os_new(NULL, (destroy_fptr) &idx_destroy, false, true, true, false, store);
	s->triggers = os_new(NULL, (destroy_fptr) &trigger_destroy, false, true, true, false, store);
	s->parts = os_new(NULL, (destroy_fptr) &part_destroy, false, false, true, false, store);
	s->store = tr->store;

	if ((res = store->table_api.table_insert(tr, sysschema, &s->base.id, &s->base.name, &s->auth_id, &s->owner, &s->system))) {
		schema_destroy(store, s);
		return res;
	}
	if ((res = os_add(tr->cat->schemas, tr, s->base.name, &s->base)))
		return res;
	if ((res = sql_trans_add_dependency(tr, s->auth_id, ddl)))
		return res;
	if ((res = sql_trans_add_dependency(tr, s->owner, ddl)))
		return res;
	if (schema_id_ptr)
		*schema_id_ptr = s->base.id;
	return res;
}

int
sql_trans_rename_schema(sql_trans *tr, sqlid id, const char *new_name)
{
	sqlstore *store = tr->store;
	table_functions table_api = store->table_api;
	sql_table *schemas = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");
	sql_schema *s = find_sql_schema_id(tr, id), *ns = NULL;
	oid rid;
	int res = LOG_OK;

	assert(!strNil(new_name));

	rid = table_api.column_find_row(tr, find_sql_column(schemas, "id"), &id, NULL);
	assert(!is_oid_nil(rid));
	if ((res = table_api.column_update_value(tr, find_sql_column(schemas, "name"), rid, (void*) new_name)))
		return res;

	if (!isNew(s) && (res = sql_trans_add_dependency_change(tr, id, ddl)))
		return res;

	/* delete schema, add schema */
	if ((res = os_del(tr->cat->schemas, tr, s->base.name, dup_base(&s->base))))
		return res;

	if ((res = schema_dup(tr, s, new_name, &ns)) ||
		(res = os_add(tr->cat->schemas, tr, ns->base.name, &ns->base))) {
		return res;
	}

	return res;
}

int
sql_trans_change_schema_authorization(sql_trans *tr, sqlid id, sqlid auth_id)
{
	sqlstore *store = tr->store;
	sql_table *sysschema = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");
	sql_schema *s = find_sql_schema_id(tr, id), *ns = NULL;
	oid rid;
	int res = LOG_OK;

	assert(auth_id);
	s->auth_id = auth_id;

	rid = store->table_api.column_find_row(tr, find_sql_column(sysschema, "id"), &id, NULL);
	assert(!is_oid_nil(rid));
	if ((res = store->table_api.column_update_value(tr, find_sql_column(sysschema, "authorization"), rid, &auth_id)))
		return res;

	if (!isNew(s) && (res = sql_trans_add_dependency_change(tr, id, ddl)))
		return res;
	/* delete schema, add schema */
	if ((res = os_del(tr->cat->schemas, tr, s->base.name, dup_base(&s->base))))
		return res;
	if ((res = schema_dup(tr, s, s->base.name, &ns)) || (res = os_add(tr->cat->schemas, tr, ns->base.name, &ns->base))) {
		return res;
	}
	return res;
}

int
sql_trans_drop_schema(sql_trans *tr, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *s = find_sql_schema_id(tr, id);
	sql_table *sysschema = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysschema, "id"), &s->base.id, NULL);
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return 0;
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid* local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = s->base.id;
		list_append(tr->dropped, local_id);
	}

	if ((res = store->table_api.table_delete(tr, sysschema, rid)))
		return res;
	if (!isNew(s) && (res = sql_trans_add_dependency_change(tr, id, ddl)))
		return res;
	if ((res = sys_drop_funcs(tr, s, drop_action)))
		return res;
	if ((res = sys_drop_tables(tr, s, drop_action)))
		return res;
	if ((res = sys_drop_types(tr, s, drop_action)))
		return res;
	if ((res = sys_drop_sequences(tr, s, drop_action)))
		return res;
	if ((res = sql_trans_drop_any_comment(tr, s->base.id)))
		return res;
	if ((res = sql_trans_drop_obj_priv(tr, s->base.id)))
		return res;

	if ((res = os_del(tr->cat->schemas, tr, s->base.name, dup_base(&s->base))))
		return res;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

static int
sql_trans_propagate_dependencies_parents(sql_trans *tr, sql_table *mt, bool *child_of_partitioned)
{
	int res = LOG_OK;
	sql_part *pt = NULL;

	for (; mt; mt = pt?pt->t:NULL) {
		if ((res = store_reset_sql_functions(tr, mt->base.id))) /* reset sql functions depending on the table */
			return res;
		if (!isNew(mt) && (res = sql_trans_add_dependency(tr, mt->base.id, ddl))) /* protect from another transaction changing the table's schema */
			return res;
		if (child_of_partitioned)
			*child_of_partitioned |= (isRangePartitionTable(mt) || isListPartitionTable(mt));
		pt = partition_find_part(tr, mt, NULL);
	}
	return res;
}

static int
sql_trans_propagate_dependencies_children(sql_trans *tr, sql_table *pt, bool child_of_partitioned)
{
	int res = LOG_OK;

	if (THRhighwater()) /* for now throw malloc failure error */
		return -1;

	if ((res = store_reset_sql_functions(tr, pt->base.id))) /* reset sql functions depending on the table */
		return res;
	if (!isNew(pt)) {
		if ((res = sql_trans_add_dependency(tr, pt->base.id, ddl))) /* protect from another transaction changing the table's schema */
			return res;
		if ((res = sql_trans_add_dependency_change(tr, pt->base.id, ddl))) /* protect from being added twice */
			return res;
		if (child_of_partitioned && isTable(pt) && (res = sql_trans_add_dependency(tr, pt->base.id, dml))) /* disallow concurrent updates on pt */
			return res;
	}
	if ((isMergeTable(pt) || isReplicaTable(pt)) && !list_empty(pt->members)) {
		for (node *nt = pt->members->h; nt; nt = nt->next) {
			sql_part *pd = nt->data;
			sql_table *t = find_sql_table_id(tr, pt->s, pd->member);

			child_of_partitioned |= (isRangePartitionTable(t) || isListPartitionTable(t));
			if ((res = sql_trans_propagate_dependencies_children(tr, t, child_of_partitioned)))
				return res;
		}
	}
	return res;
}

int
sql_trans_add_table(sql_trans *tr, sql_table *mt, sql_table *pt)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(tr, syss, "objects");
	int res = LOG_OK;
	sql_table *dup = NULL;
	bool child_of_partitioned = false;

	/* merge table depends on part table */
	if ((res = sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY)))
		return res;
	assert(isMergeTable(mt) || isReplicaTable(mt));

	if ((res = new_table(tr, mt, &dup)))
		return res;
	mt = dup;
	if (!mt->members)
		mt->members = list_create((fdestroy) &part_destroy);
	sql_part *p = ZNEW(sql_part);
	p->t = mt;
	p->member = pt->base.id;

	base_init(NULL, &p->base, next_oid(store), true, pt->base.name);
	list_append(mt->members, p);
	if ((res = store->table_api.table_insert(tr, sysobj, &p->base.id, &p->base.name, &mt->base.id, &pt->base.id)))
		return res;
	if ((res = os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))))
		return res;
	if ((res = sql_trans_propagate_dependencies_parents(tr, mt, &child_of_partitioned)))
		return res;
	if ((res = sql_trans_propagate_dependencies_children(tr, pt, child_of_partitioned)))
		return res;
	return res;
}

int
sql_trans_add_range_partition(sql_trans *tr, sql_table *mt, sql_table *pt, sql_subtype tpe, ptr min, ptr max,
		bit with_nills, int update, sql_part **err)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(tr, syss, "objects");
	sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
	sql_table *ranges = find_sql_table(tr, syss, "range_partitions");
	sql_part *p;
	int localtype = tpe.type->localtype, res = LOG_OK;
	ValRecord vmin, vmax;
	size_t smin, smax;
	bit to_insert = with_nills;
	oid rid;
	ptr ok;
	sql_table *dup = NULL;

	assert(sysobj);
	assert(partitions);
	assert(ranges);

	vmin = vmax = (ValRecord) {.vtype = TYPE_void,};

	if ((res = new_table(tr, mt, &dup)))
		return res;
	mt = dup;
	if (!mt->members)
		mt->members = list_create((fdestroy) &part_destroy);
	if (min) {
		ok = VALinit(&vmin, localtype, min);
		if (ok && localtype != TYPE_str)
			ok = VALconvert(TYPE_str, &vmin);
	} else {
		ok = VALinit(&vmin, TYPE_str, ATOMnilptr(TYPE_str));
		min = (ptr) ATOMnilptr(localtype);
	}
	if (!ok) {
		res = -1;
		goto finish;
	}
	smin = ATOMlen(localtype, min);
	if (smin > STORAGE_MAX_VALUE_LENGTH) {
		res = -10;
		goto finish;
	}

	if (max) {
		ok = VALinit(&vmax, localtype, max);
		if (ok && localtype != TYPE_str)
			ok = VALconvert(TYPE_str, &vmax);
	} else {
		ok = VALinit(&vmax, TYPE_str, ATOMnilptr(TYPE_str));
		max = (ptr) ATOMnilptr(localtype);
	}
	if (!ok) {
		res = -1;
		goto finish;
	}
	smax = ATOMlen(localtype, max);
	if (smax > STORAGE_MAX_VALUE_LENGTH) {
		res = -11;
		goto finish;
	}

	if (!update) {
		p = ZNEW(sql_part);
		base_init(NULL, &p->base, next_oid(store), true, pt->base.name);
		assert(isMergeTable(mt) || isReplicaTable(mt));
		p->t = mt;
		assert(pt);
		p->member = pt->base.id;
	} else {
		node *n = members_find_child_id(mt->members, pt->base.id);
		p = (sql_part*) n->data;
	}

	/* add range partition values */
	if (update) {
		_DELETE(p->part.range.minvalue);
		_DELETE(p->part.range.maxvalue);
	}
	p->part.range.minvalue = NEW_ARRAY(char, smin);
	p->part.range.maxvalue = NEW_ARRAY(char, smax);
	memcpy(p->part.range.minvalue, min, smin);
	memcpy(p->part.range.maxvalue, max, smax);
	p->part.range.minlength = smin;
	p->part.range.maxlength = smax;
	p->with_nills = with_nills;

	if (!update) {
		*err = list_append_with_validate(mt->members, p, &localtype, sql_range_part_validate_and_insert);
		if (*err)
			part_destroy(store, p);
	} else {
		*err = list_transverse_with_validate(mt->members, p, &localtype, sql_range_part_validate_and_insert);
	}
	if (*err) {
		res = -12;
		goto finish;
	}

	if (isPartitionedByColumnTable(p->t))
		col_set_range(tr, p, true);

	if (!update) {
		rid = store->table_api.column_find_row(tr, find_sql_column(partitions, "table_id"), &mt->base.id, NULL);
		assert(!is_oid_nil(rid));

		/* add merge table dependency */
		if ((res = sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY)))
			goto finish;
		sqlid id = store->table_api.column_find_sqlid(tr, find_sql_column(partitions, "id"), rid);
		if ((res = store->table_api.table_insert(tr, sysobj, &p->base.id, &p->base.name, &mt->base.id, &pt->base.id)))
			goto finish;
		char *vmin_val = VALget(&vmin);
		char *vmax_val = VALget(&vmax);
		if ((res = store->table_api.table_insert(tr, ranges, &pt->base.id, &id, &vmin_val, &vmax_val, &to_insert)))
			goto finish;
	} else {
		sql_column *cmin = find_sql_column(ranges, "minimum"), *cmax = find_sql_column(ranges, "maximum"),
				   *wnulls = find_sql_column(ranges, "with_nulls");

		rid = store->table_api.column_find_row(tr, find_sql_column(ranges, "table_id"), &pt->base.id, NULL);
		assert(!is_oid_nil(rid));

		if ((res = store->table_api.column_update_value(tr, cmin, rid, VALget(&vmin))))
			goto finish;
		if ((res = store->table_api.column_update_value(tr, cmax, rid, VALget(&vmax))))
			goto finish;
		if ((res = store->table_api.column_update_value(tr, wnulls, rid, &to_insert)))
			goto finish;
	}

	if (!update && (res = os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))))
		goto finish;
	if ((res = sql_trans_propagate_dependencies_parents(tr, mt, NULL)))
		goto finish;
	res = sql_trans_propagate_dependencies_children(tr, pt, true);

finish:
	VALclear(&vmin);
	VALclear(&vmax);
	return res;
}

int
sql_trans_add_value_partition(sql_trans *tr, sql_table *mt, sql_table *pt, sql_subtype tpe, list* vals, bit with_nills,
		int update, sql_part **err)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(tr, syss, "objects");
	sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
	sql_table *values = find_sql_table(tr, syss, "value_partitions");
	sql_part *p;
	oid rid;
	int localtype = tpe.type->localtype, i = 0, res = LOG_OK;
	sql_table *dup = NULL;

	assert(sysobj);
	assert(partitions);
	assert(values);

	if ((res = new_table(tr, mt, &dup)))
		return res;
	mt = dup;
	if (!mt->members)
		mt->members = list_create((fdestroy) &part_destroy);
	if (!update) {
		p = ZNEW(sql_part);
		base_init(NULL, &p->base, next_oid(store), true, pt->base.name);
		assert(isMergeTable(mt) || isReplicaTable(mt));
		p->t = mt;
		assert(pt);
		p->member = pt->base.id;
	} else {
		rids *rs;
		node *n = members_find_child_id(mt->members, pt->base.id);
		p = (sql_part*) n->data;

		rs = store->table_api.rids_select(tr, find_sql_column(values, "table_id"), &pt->base.id, &pt->base.id, NULL);
		if (rs == NULL)
			return LOG_ERR;
		for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
			if ((res = store->table_api.table_delete(tr, values, rid))) { /* eliminate the old values */
				store->table_api.rids_destroy(rs);
				return res;
			}
		}
		store->table_api.rids_destroy(rs);
	}
	p->with_nills = with_nills;

	rid = store->table_api.column_find_row(tr, find_sql_column(partitions, "table_id"), &mt->base.id, NULL);
	assert(!is_oid_nil(rid));

	sqlid id = store->table_api.column_find_sqlid(tr, find_sql_column(partitions, "id"), rid);

	if (with_nills) { /* store the null value first */
		ValRecord vnnil;
		if (VALinit(&vnnil, TYPE_str, ATOMnilptr(TYPE_str)) == NULL) {
			if (!update)
				part_destroy(store, p);
			list_destroy2(vals, store);
			return -1;
		}
		char *vnnil_val = VALget(&vnnil);
		if ((res = store->table_api.table_insert(tr, values, &pt->base.id, &id, &vnnil_val))) {
			list_destroy2(vals, store);
			return res;
		}
		VALclear(&vnnil);
	}

	for (node *n = vals->h ; n ; n = n->next) {
		sql_part_value *next = (sql_part_value*) n->data;
		ValRecord vvalue;
		ptr ok;

		if (ATOMlen(localtype, next->value) > STORAGE_MAX_VALUE_LENGTH) {
			if (!update)
				part_destroy(store, p);
			list_destroy2(vals, store);
			return -i - 10;
		}
		ok = VALinit(&vvalue, localtype, next->value);
		if (ok && localtype != TYPE_str)
			ok = VALconvert(TYPE_str, &vvalue);
		if (!ok) {
			if (!update)
				part_destroy(store, p);
			VALclear(&vvalue);
			list_destroy2(vals, store);
			return -i - 10;
		}
		char *vvalue_val = VALget(&vvalue);
		if ((res = store->table_api.table_insert(tr, values, &pt->base.id, &id, &vvalue_val))) {
			VALclear(&vvalue);
			list_destroy2(vals, store);
			return res;
		}

		VALclear(&vvalue);
		i++;
	}

	if (p->part.values)
		list_destroy2(p->part.values, store);
	p->part.values = vals;

	if (!update) {
		*err = list_append_with_validate(mt->members, p, &localtype, sql_values_part_validate_and_insert);
		if (*err)
			part_destroy(store, p);
	} else {
		*err = list_transverse_with_validate(mt->members, p, &localtype, sql_values_part_validate_and_insert);
	}
	if (*err)
		return -4;

	if (!update) {
		/* add merge table dependency */
		if ((res = sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY)))
			return res;
		if ((res = store->table_api.table_insert(tr, sysobj, &p->base.id, &p->base.name, &mt->base.id, &pt->base.id)))
			return res;
		if ((res = os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))))
			return res;
	}
	if ((res = sql_trans_propagate_dependencies_parents(tr, mt, NULL)))
		return res;
	if ((res = sql_trans_propagate_dependencies_children(tr, pt, true)))
		return res;
	return res;
}

int
sql_trans_rename_table(sql_trans *tr, sql_schema *s, sqlid id, const char *new_name)
{
	sqlstore *store = tr->store;
	sql_table *systable = find_sql_table(tr, find_sql_schema(tr, isTempSchema(s) ? "tmp":"sys"), "_tables");
	sql_table *t = find_sql_table_id(tr, s, id), *dup = NULL;
	oid rid;
	int res = LOG_OK;

	assert(!strNil(new_name));

	rid = store->table_api.column_find_row(tr, find_sql_column(systable, "id"), &id, NULL);
	assert(!is_oid_nil(rid));
	if ((res = store->table_api.column_update_value(tr, find_sql_column(systable, "name"), rid, (void*) new_name)))
		return res;

	if (isGlobal(t)) {
		if (!isNew(t) && (res = sql_trans_add_dependency_change(tr, id, ddl)))
			return res;
		if ((res = os_del(s->tables, tr, t->base.name, dup_base(&t->base))))
			return res;
	} else {
		assert(isTempTable(t));
		sql_base *b = os_find_id(tr->localtmps, tr, t->base.id);
		if ((res = os_del(tr->localtmps, tr, b->name, dup_base(b))))
			return res;
	}

	if ((res = table_dup(tr, t, t->s, new_name, &dup, true)))
		return res;
	return res;
}

int
sql_trans_set_table_schema(sql_trans *tr, sqlid id, sql_schema *os, sql_schema *ns)
{
	sqlstore *store = tr->store;
	sql_table *systable = find_sql_table(tr, find_sql_schema(tr, isTempSchema(os) ? "tmp":"sys"), "_tables");
	sql_base *b = os_find_id(os->tables, tr, id);
	sql_table *t = (sql_table*)b, *dup = NULL;
	oid rid;
	int res = LOG_OK;

	rid = store->table_api.column_find_row(tr, find_sql_column(systable, "id"), &t->base.id, NULL);
	assert(!is_oid_nil(rid));
	if ((res = store->table_api.column_update_value(tr, find_sql_column(systable, "schema_id"), rid, &(ns->base.id))))
		return res;

	if (!isNew(t) && (res = sql_trans_add_dependency_change(tr, id, ddl)))
		return res;
	if ((res = os_del(os->tables, tr, t->base.name, dup_base(&t->base))))
		return res;
	return table_dup(tr, t, ns, NULL, &dup, true);
}

int
sql_trans_del_table(sql_trans *tr, sql_table *mt, sql_table *pt, int drop_action)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;
	sql_table *dup = NULL, *mt_it;
	sql_part *mti_part = NULL;

	if ((res = new_table(tr, mt, &dup)))
		return res;
	mt = dup;
	node *n = members_find_child_id(mt->members, pt->base.id); /* get sqlpart id*/
	sqlid part_id = ((sql_part*)n->data)->base.id;
	sql_base *b = os_find_id(mt->s->parts, tr, part_id); /* fetch updated part */
	sql_part *p = (sql_part*)b;

	if ((res = sys_drop_part(tr, p, drop_action)))
		return res;
	/*Clean the part from members*/
	list_remove_node(mt->members, store, n);

	if (drop_action == DROP_CASCADE && (res = sql_trans_drop_table_id(tr, mt->s, pt->base.id, drop_action)))
		return res;
	for (mt_it = mt; mt_it; mt_it = mti_part?mti_part->t:NULL) {
		/* functions depending on parent tables need to be recompiled, ugh */
		if ((res = store_reset_sql_functions(tr, mt_it->base.id))) /* reset sql functions depending on the table */
			return res;
		mti_part = partition_find_part(tr, mt_it, NULL);
	}
	if ((res = store_reset_sql_functions(tr, pt->base.id))) /* reset sql functions depending on the table */
		return res;
	return res;
}

int
sql_trans_create_table(sql_table **tres, sql_trans *tr, sql_schema *s, const char *name, const char *sql, int tt, bit system,
					   int persistence, int commit_action, int sz, bte properties)
{
	sqlstore *store = tr->store;
	sql_table *t = create_sql_table_with_id(NULL, next_oid(tr->store), name, tt, system, persistence, commit_action, properties);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(tr, syss, "_tables");
	sht ca;
	int res = LOG_OK;

	/* temps all belong to a special tmp schema and only views/remote have a query */
	assert( (isTable(t) ||
		(!isTempTable(t) || (strcmp(s->base.name, "tmp") == 0) || isDeclaredTable(t))) || (isView(t) && !sql) || (isRemote(t) && !sql));

	t->query = sql ?_STRDUP(sql) : NULL;
	t->s = s;
	t->sz = sz;
	if (sz < 0)
		t->sz = COLSIZE;

	if ((res = os_add(isGlobal(t)?s->tables:tr->localtmps, tr, t->base.name, &t->base)))
		return res;

	if (isUnloggedTable(t))
		t->persistence = SQL_PERSIST; // It's not a temporary

	if (isRemote(t))
		t->persistence = SQL_REMOTE;

	if (isTable(t))
		if ((res = store->storage_api.create_del(tr, t))) {
			return res;
		}
	if (isPartitionedByExpressionTable(t)) {
		t->part.pexp = ZNEW(sql_expression);
		t->part.pexp->type = *sql_fetch_localtype(TYPE_void); /* leave it non-initialized, at the backend the copy of this table will get the type */
		t->part.pexp->cols = list_create((fdestroy) &int_destroy);
	}

	ca = t->commit_action;
	if (!isDeclaredTable(t)) {
		char *strnil = (char*)ATOMnilptr(TYPE_str);
		if ((res = store->table_api.table_insert(tr, systable, &t->base.id, &t->base.name, &s->base.id,
										  (t->query) ? &t->query : &strnil, &t->type, &t->system, &ca, &t->access))) {
			return res;
		}
	}
	*tres = t;
	return res;
}

int
sql_trans_set_partition_table(sql_trans *tr, sql_table *t)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if (t && (isRangePartitionTable(t) || isListPartitionTable(t))) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
		sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
		assert(partitions);
		sqlid next = next_oid(tr->store);
		if (isPartitionedByColumnTable(t)) {
			assert(t->part.pcol);
			if ((res = store->table_api.table_insert(tr, partitions, &next, &t->base.id, &t->part.pcol->base.id, &ATOMnilptr(TYPE_str), &t->properties)))
				return res;
		} else if (isPartitionedByExpressionTable(t)) {
			assert(t->part.pexp->exp);
			if (strlen(t->part.pexp->exp) > STORAGE_MAX_VALUE_LENGTH)
				return -4;
			if ((res = store->table_api.table_insert(tr, partitions, &next, &t->base.id, ATOMnilptr(TYPE_int), &t->part.pexp->exp, &t->properties)))
				return res;
		} else {
			assert(0);
		}
	}
	return res;
}

sql_key *
create_sql_kc(sqlstore *store, allocator *sa, sql_key *k, sql_column *c)
{
	sql_kc *kc = SA_ZNEW(sa, sql_kc);

	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx)
		create_sql_ic(store, sa, k->idx, c);
	if (k->type == pkey)
		c->null = 0;
	return k;
}

sql_key *
create_sql_ukey(sqlstore *store, allocator *sa, sql_table *t, const char *name, key_type kt, const char* check)
{
	sql_key *nk = NULL;
	sql_ukey *tk;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);
	tk = (sql_ukey *) nk;
	assert(name);

	base_init(sa, &nk->base, next_oid(store), true, name);
	nk->type = kt;
	nk->columns = SA_LIST(sa, (fdestroy) NULL);
	nk->idx = NULL;
	nk->t = t;
	nk->check = check ? SA_STRDUP(sa, check) : NULL;

	if (nk->type == pkey)
		t->pkey = tk;
	if (ol_add(t->keys, &nk->base))
		return NULL;
	return nk;
}

sql_fkey *
create_sql_fkey(sqlstore *store, allocator *sa, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
	sql_key *nk;
	sql_fkey *fk = NULL;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);

	assert(name);
	base_init(sa, &nk->base, next_oid(store), true, name);
	nk->type = kt;
	nk->columns = SA_LIST(sa, (fdestroy) NULL);
	nk->t = t;
	nk->idx = create_sql_idx(store, sa, t, name, (nk->type == fkey) ? join_idx : hash_idx);
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;

	fk->rkey = rkey->base.id;
	if (ol_add(t->keys, &nk->base))
		return NULL;
	return (sql_fkey*) nk;
}

sql_idx *
create_sql_idx(sqlstore *store, allocator *sa, sql_table *t, const char *name, idx_type it)
{
	sql_idx *ni = SA_ZNEW(sa, sql_idx);

	base_init(sa, &ni->base, next_oid(store), true, name);
	ni->columns = SA_LIST(sa, (fdestroy) NULL);
	ni->t = t;
	ni->type = it;
	ni->key = NULL;
	if (ol_add(t->idxs, &ni->base))
		return NULL;
	return ni;
}

sql_idx *
create_sql_ic(sqlstore *store, allocator *sa, sql_idx *i, sql_column *c)
{
	sql_kc *ic = SA_ZNEW(sa, sql_kc);

	ic->c = c;
	list_append(i->columns, ic);

	(void)store;
	return i;
}

sql_idx *
create_sql_idx_done(sql_trans *tr, sql_idx *i)
{
	(void) tr;
	if (i && i->key && hash_index(i->type)) {
		int ncols = list_length(i->columns);
		for (node *n = i->columns->h ; n ; n = n->next) {
			sql_kc *kc = n->data;

			kc->c->unique = (ncols == 1) ? 2 : MAX(kc->c->unique, 1);
		}
	}
	return i;
}

static sql_column *
create_sql_column_with_id(allocator *sa, sqlid id, sql_table *t, const char *name, sql_subtype *tpe)
{
	sql_column *col = SA_ZNEW(sa, sql_column);

	base_init(sa, &col->base, id, true, name);
	col->type = *tpe;
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;

	if (ol_add(t->columns, &col->base))
		return NULL;
	ATOMIC_PTR_INIT(&col->data, NULL);
	return col;
}

sql_column *
create_sql_column(sqlstore *store, allocator *sa, sql_table *t, const char *name, sql_subtype *tpe)
{
	return create_sql_column_with_id(sa, next_oid(store), t, name, tpe);
}

int
sql_trans_drop_table(sql_trans *tr, sql_schema *s, const char *name, int drop_action)
{
	sql_table *t = find_sql_table(tr, s, name);

	if (!t) {
		TRC_ERROR(SQL_STORE, "sql_trans_drop_table: Table %s.%s does not exist\n", s->base.name, name);
		return -1;
	}

	sql_table *gt = t;
	if (t && isTempTable(t)) {
		gt = (sql_table*)os_find_id(s->tables, tr, t->base.id);
	}

	assert(t == gt || !gt || (isTempTable(gt) && !isLocalTemp(gt) && isLocalTemp(t)));

	int res = LOG_OK;

	if ((drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) &&
	    tr->dropped && list_find_id(tr->dropped, t->base.id))
		return res;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = t->base.id;
		list_append(tr->dropped, local_id);
	}

	if (!isDeclaredTable(t))
		if ((res = sys_drop_table(tr, gt?gt:t, drop_action)))
			return res;

	if (isNew(t))
		t->base.deleted = 1;
	if (gt && (res = os_del(s->tables, tr, gt->base.name, dup_base(&gt->base))))
		return res;
	if (t != gt && (res = os_del(tr->localtmps, tr, t->base.name, dup_base(&t->base))))
		return res;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

int
sql_trans_drop_table_id(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sql_table *t = find_sql_table_id(tr, s, id);

	if (t)
		return sql_trans_drop_table(tr, s, t->base.name, drop_action);
	else
		return SQL_ERR;
}

BUN
sql_trans_clear_table(sql_trans *tr, sql_table *t)
{
	sqlstore *store = tr->store;
	return store->storage_api.clear_table(tr, t);
}

int
sql_trans_create_column(sql_column **rcol, sql_trans *tr, sql_table *t, const char *name, sql_subtype *tpe)
{
	sqlstore *store = tr->store;
	sql_column *col;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
	int res = LOG_OK;

	if (!tpe)
		return -1; /* TODO not sure what to do here */

	col = create_sql_column_with_id(NULL, next_oid(tr->store), t, name, tpe);

	if (isTable(col->t))
		if ((res = store->storage_api.create_col(tr, col))) {
			return res;
		}
	if (!isDeclaredTable(t)) {
		char *strnil = (char*)ATOMnilptr(TYPE_str);
		int digits = type_digits(&col->type);
		if ((res = store->table_api.table_insert(tr, syscolumn, &col->base.id, &col->base.name, &col->type.type->base.name, &digits, &col->type.scale,
										  &t->base.id, (col->def) ? &col->def : &strnil, &col->null, &col->colnr, (col->storage_type) ? &col->storage_type : &strnil))) {
			return res;
		}
	}

	if (tpe->type->s) {/* column depends on type */
		if ((res = sql_trans_create_dependency(tr, tpe->type->base.id, col->base.id, TYPE_DEPENDENCY))) {
			return res;
		}
		if (!isNew(tpe->type) && (res = sql_trans_add_dependency(tr, tpe->type->base.id, ddl))) {
			return res;
		}
	}
	*rcol = col;
	return res;
}

void
drop_sql_column(sql_table *t, sqlid id, int drop_action)
{
	node *n = ol_find_id(t->columns, id);
	sql_column *col = n->data;

	col->drop_action = drop_action;
	col->base.deleted = 1;
	//ol_del(t->columns, t->s->store, n);
}

void
drop_sql_idx(sql_table *t, sqlid id)
{
	node *n = ol_find_id(t->idxs, id);
	sql_idx *i = n->data;

	i->base.deleted = 1;
	//ol_del(t->idxs, t->s->store, n);
}

void
drop_sql_key(sql_table *t, sqlid id, int drop_action)
{
	node *n = ol_find_id(t->keys, id);
	sql_key *k = n->data;

	k->drop_action = drop_action;
	k->base.deleted = 1;
	//ol_del(t->keys, t->s->store, n);
}

int
sql_trans_rename_column(sql_trans *tr, sql_table *t, sqlid id, const char *old_name, const char *new_name)
{
	sqlstore *store = tr->store;
	sql_table *syscolumn = find_sql_table(tr, find_sql_schema(tr, isGlobal(t)?"sys":"tmp"), "_columns");
	oid rid;
	int res = LOG_OK;
	sql_table *dup = NULL;
	node *n;

	assert(!strNil(new_name));

	rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"), &id, NULL);
	assert(!is_oid_nil(rid));
	if ((res = store->table_api.column_update_value(tr, find_sql_column(syscolumn, "name"), rid, (void*) new_name)))
		return res;

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	if (!(n = ol_find_name(t->columns, old_name)))
		return -1;
	sql_column *c = n->data;

	if (!isNew(c) && (res = sql_trans_add_dependency_change(tr, c->t->base.id, ddl)))
		return res;
	if (!isNew(c) && (res = sql_trans_add_dependency_change(tr, id, ddl)))
		return res;

	_DELETE(c->base.name);
	c->base.name =_STRDUP(new_name);
	if (ol_rehash(t->columns, old_name, n) == NULL)
		return -1;
	return res;
}

static int
new_column(sql_trans *tr, sql_column *col, sql_column **cres)
{
	int res = LOG_OK;
	sql_table *t = NULL;

	if ((res = new_table(tr, col->t, &t)))
		return res;
	*cres = find_sql_column(t, col->base.name);
	return res;
}

int
sql_trans_drop_column(sql_trans *tr, sql_table *t, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	node *n = NULL;
	sql_table *syscolumn = find_sql_table(tr, find_sql_schema(tr, isGlobal(t)?"sys":"tmp"), "_columns");
	sql_column *col = NULL, *cid = find_sql_column(syscolumn, "id"), *cnr = find_sql_column(syscolumn, "number");
	int res = LOG_OK;
	sql_table *dup = NULL;

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	for (node *nn = t->columns->l->h ; nn ; nn = nn->next) {
		sql_column *next = (sql_column *) nn->data;
		if (next->base.id == id) {
			n = nn;
			col = next;
		} else if (col) { /* if the column to be dropped was found, decrease the column number for others after it */
			next->colnr--;

			if (!isDeclaredTable(t)) {
				oid rid = store->table_api.column_find_row(tr, cid, &next->base.id, NULL);
				assert(!is_oid_nil(rid));
				if ((res = store->table_api.column_update_value(tr, cnr, rid, &next->colnr)))
					return res;
			}
		}
	}

	assert(n && col); /* the column to be dropped must have been found */

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = col->base.id;
		list_append(tr->dropped, local_id);
	}

	if (!isNew(col) && (res = sql_trans_add_dependency_change(tr, col->t->base.id, ddl)))
		return res;
	if (!isNew(col) && isGlobal(col->t) && !isGlobalTemp(col->t) && (res = sql_trans_add_dependency(tr, col->t->base.id, dml)))
		return res;
	if ((res = sys_drop_column(tr, col, drop_action)))
		return res;

	if (isNew(col)) { /* remove create from changes */
		trans_del(tr, &col->base);
		if (!isNew(col->t))
			column_destroy(store, col);
	}
	ol_del(t->columns, store, n);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

int
sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if (col->null != isnull) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"),
					  &col->base.id, NULL);
		sql_column *dup = NULL;

		if (is_oid_nil(rid))
			return -1;
		if ((res = store->table_api.column_update_value(tr, find_sql_column(syscolumn, "null"), rid, &isnull)))
			return res;

		if ((res = new_column(tr, col, &dup)))
			return res;
		col = dup;
		dup->null = isnull;

		/* disallow concurrent updates on the column if not null is set */
		/* this dependency is needed for merge tables */
		if (!isNew(col) && (res = sql_trans_add_dependency(tr, col->t->base.id, ddl)))
			return res;
		if (!isnull && !isNew(col) && isGlobal(col->t) && !isGlobalTemp(col->t) && (res = sql_trans_add_dependency(tr, col->t->base.id, dml)))
			return res;
		if ((res = store_reset_sql_functions(tr, col->t->base.id))) /* reset sql functions depending on the table */
			return res;
		if (isNew(col) || isnull) /* new ie can still change, or persistent only widen semantics */
			store->storage_api.col_not_null(tr, col, !isnull);
	}
	return res;
}

int
sql_trans_alter_access(sql_trans *tr, sql_table *t, sht access)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if (t->access != access) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
		sql_table *systable = find_sql_table(tr, syss, "_tables");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(systable, "id"),
					  &t->base.id, NULL);
		sql_table *dup = NULL;

		if (is_oid_nil(rid))
			return -1;
		if ((res = store->table_api.column_update_value(tr, find_sql_column(systable, "access"), rid, &access)))
			return res;
		if ((res = new_table(tr, t, &dup)))
			return res;
		t = dup;
		t->access = access;
		if (!isNew(t) && isGlobal(t) && !isGlobalTemp(t) && (res = sql_trans_add_dependency(tr, t->base.id, dml)))
			return res;
		if ((res = store_reset_sql_functions(tr, t->base.id))) /* reset sql functions depending on the table */
			return res;
	}
	return res;
}

int
sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if ((col->def || val) && (!col->def || !val || strcmp(col->def, val) != 0)) {
		void *p = val ? val : (void *) ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "default");
		oid rid = store->table_api.column_find_row(tr, col_ids, &col->base.id, NULL);
		sql_column *dup = NULL;

		if (is_oid_nil(rid))
			return -1;
		if ((res = sys_drop_default_object(tr, col, 0)))
			return res;
		if ((res = store->table_api.column_update_value(tr, col_dfs, rid, p)))
			return res;

		if ((res = new_column(tr, col, &dup)))
			return res;
		_DELETE(dup->def);
		if (val)
			dup->def =_STRDUP(val);
		if ((res = store_reset_sql_functions(tr, col->t->base.id))) /* reset sql functions depending on the table */
			return res;
	}
	return res;
}

int
sql_trans_alter_storage(sql_trans *tr, sql_column *col, char *storage)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if ((col->storage_type || storage) && (!col->storage_type || !storage || strcmp(col->storage_type, storage) != 0)) {
		void *p = storage ? storage : (void *) ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "storage");
		oid rid = store->table_api.column_find_row(tr, col_ids, &col->base.id, NULL);
		sql_column *dup = NULL;

		if (is_oid_nil(rid))
			return -1;
		if ((res = store->table_api.column_update_value(tr, col_dfs, rid, p)))
			return res;

		if ((res = new_column(tr, col, &dup)))
			return res;
		_DELETE(dup->storage_type);
		if (storage)
			dup->storage_type =_STRDUP(storage);
		if (!isNew(col) && isGlobal(col->t) && !isGlobalTemp(col->t) && (res = sql_trans_add_dependency(tr, col->t->base.id, dml)))
			return res;
		if ((res = store_reset_sql_functions(tr, col->t->base.id))) /* reset sql functions depending on the table */
			return res;
	}
	return res;
}

int
sql_trans_alter_type(sql_trans *tr, sql_column *col, sql_subtype *t)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if (subtype_cmp(&col->type, t) != 0) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_type = find_sql_column(syscolumn, "type");
		sql_column *col_digits = find_sql_column(syscolumn, "type_digits");
		sql_column *col_scale = find_sql_column(syscolumn, "type_scale");
		oid rid = store->table_api.column_find_row(tr, col_ids, &col->base.id, NULL);
		sql_column *dup = NULL;
		int digits = type_digits(t);
		int scale = t->scale;

		if (is_oid_nil(rid))
			return -1;
		if ((res = store->table_api.column_update_value(tr, col_type, rid, t->type->base.name)))
			return res;
		if ((res = store->table_api.column_update_value(tr, col_digits, rid, &digits)))
			return res;
		if ((res = store->table_api.column_update_value(tr, col_scale, rid, &scale)))
			return res;

		if ((res = new_column(tr, col, &dup)))
			return res;
		if (!isNew(col) && isGlobal(col->t) && !isGlobalTemp(col->t) && (res = sql_trans_add_dependency(tr, col->t->base.id, dml)))
			return res;
		if ((res = store_reset_sql_functions(tr, col->t->base.id))) /* reset sql functions depending on the table */
			return res;
		store->storage_api.col_subtype(tr, dup, t);
		dup->type = *t;
	}
	return res;
}

int
sql_trans_is_sorted( sql_trans *tr, sql_column *col )
{
	sqlstore *store = tr->store;
	if (col && isTable(col->t) && store->storage_api.sorted_col && store->storage_api.sorted_col(tr, col))
		return 1;
	return 0;
}

int
sql_trans_is_unique( sql_trans *tr, sql_column *col )
{
	sqlstore *store = tr->store;
	if (col && isTable(col->t) && store->storage_api.unique_col && store->storage_api.unique_col(tr, col))
		return 1;
	return 0;
}

int
sql_trans_is_duplicate_eliminated( sql_trans *tr, sql_column *col )
{
	sqlstore *store = tr->store;
	if (col && isTable(col->t) && store->storage_api.double_elim_col)
		return store->storage_api.double_elim_col(tr, col);
	return 0;
}

int
sql_trans_col_stats( sql_trans *tr, sql_column *col, bool *nonil, bool *unique, double *unique_est, ValPtr min, ValPtr max )
{
	sqlstore *store = tr->store;
	if (col && isTable(col->t) && store->storage_api.col_stats)
		return store->storage_api.col_stats(tr, col, nonil, unique, unique_est, min, max);
	return 0;
}

size_t
sql_trans_dist_count( sql_trans *tr, sql_column *col )
{
	sqlstore *store = tr->store;

	if (col && isTable(col->t)) {
		if (!col->dcount)
			col->dcount = store->storage_api.dcount_col(tr, col);
		return col->dcount;
	}
	return 0;
}

int
sql_trans_ranges( sql_trans *tr, sql_column *col, void **min, void **max )
{
	sqlstore *store = tr->store;

	*min = NULL;
	*max = NULL;
	if (col && isTable(col->t)) {
		if (!col->min || !col->max)
			(void) store->storage_api.min_max_col(tr, col);
		*min = col->min;
		*max = col->max;
	}
	return *min != NULL && *max != NULL;
}

int
sql_trans_create_ukey(sql_key **kres, sql_trans *tr, sql_table *t, const char *name, key_type kt, const char* check)
{
/* can only have keys between persistent tables */
	sqlstore *store = tr->store;
	int neg = -1, action = -1, res = LOG_OK;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	sql_ukey *uk = NULL;
	sql_table *dup = NULL;

	if (isTempTable(t))
		return -1; /* TODO not sure here */

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	nk = (kt != fkey) ? (sql_key *) ZNEW(sql_ukey)
	: (sql_key *) ZNEW(sql_fkey);

	assert(name);
	base_init(NULL, &nk->base, next_oid(tr->store), true, name);
	nk->type = kt;
	nk->columns = list_create((fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = NULL;

	uk = (sql_ukey *) nk;

	if (nk->type == pkey)
		t->pkey = uk;

	if ((res = ol_add(t->keys, &nk->base)))
		return res;
	if ((res = os_add(t->s->keys, tr, nk->base.name, dup_base(&nk->base))) ||
		(isGlobal(t) && (res = os_add(tr->cat->objects, tr, nk->base.name, dup_base(&nk->base)))))
		return res;

	const char *strnil = (const char*)ATOMnilptr(TYPE_str);
	if ((res = store->table_api.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, &nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey : &neg, &action, (check) ? &check : &strnil)))
		return res;
	*kres = nk;
	return res;
}

int
sql_trans_create_fkey(sql_fkey **kres, sql_trans *tr, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
/* can only have keys between persistent tables */
	sqlstore *store = tr->store;
	int neg = -1, action = (on_update<<8) + on_delete, res = LOG_OK;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	sql_fkey *fk = NULL;
	sql_table *dup = NULL;

	if (isTempTable(t))
		return -1; /* TODO not sure here */

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	nk = (kt != fkey) ? (sql_key *) ZNEW(sql_ukey)
	: (sql_key *) ZNEW(sql_fkey);

	assert(name);
	base_init(NULL, &nk->base, next_oid(tr->store), true, name);
	nk->type = kt;
	nk->columns = list_create((fdestroy) &kc_destroy);
	nk->t = t;
	if ((res = sql_trans_create_idx(&nk->idx, tr, t, name, (nk->type == fkey) ? join_idx : hash_idx)))
		return res;
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;

	fk->rkey = rkey->base.id;

	if ((res = ol_add(t->keys, &nk->base)))
		return res;
	if ((res = os_add(t->s->keys, tr, nk->base.name, dup_base(&nk->base))) ||
		(isGlobal(t) && (res = os_add(tr->cat->objects, tr, nk->base.name, dup_base(&nk->base)))))
		return res;

	if ((res = store->table_api.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, &nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey : &neg, &action)))
		return res;

	if ((res = sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey, nk->base.id, FKEY_DEPENDENCY)))
		return res;
	*kres = (sql_fkey*) nk;
	return res;
}

int
sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c)
{
	sqlstore *store = tr->store;
	sql_kc *kc = ZNEW(sql_kc);
	int nr = list_length(k->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(tr, syss, "objects");
	int res = LOG_OK;

	assert(c);
	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx && (res = sql_trans_create_ic(tr, k->idx, c)))
		return res;

	if (k->type == pkey) {
		if ((res = sql_trans_create_dependency(tr, c->base.id, k->base.id, KEY_DEPENDENCY)))
			return res;
		if ((res = sql_trans_alter_null(tr, c, 0))) /* should never trigger error */
			return res;
	}

	if ((res = store->table_api.table_insert(tr, syskc, &k->base.id, &kc->c->base.name, &nr, ATOMnilptr(TYPE_int))))
		return res;
	return res;
}

int
sql_trans_create_fkc(sql_trans *tr, sql_fkey *fk, sql_column *c)
{
	sqlstore *store = tr->store;
	sql_key *k = (sql_key *) fk;
	sql_kc *kc = ZNEW(sql_kc);
	int nr = list_length(k->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(tr, syss, "objects");
	int res = LOG_OK;

	assert(c);
	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx && (res = sql_trans_create_ic(tr, k->idx, c)))
		return res;

	if ((res = sql_trans_create_dependency(tr, c->base.id, k->base.id, FKEY_DEPENDENCY)))
		return res;

	if ((res = store->table_api.table_insert(tr, syskc, &k->base.id, &kc->c->base.name, &nr, ATOMnilptr(TYPE_int))))
		return res;
	return res;
}

static sql_idx *
table_has_idx( sql_table *t, list *keycols)
{
	node *n, *m, *o;
	char *found = NULL;
	int len = list_length(keycols);
	found = NEW_ARRAY(char, len);
	if (!found)
		return NULL;
	if (t->idxs) for ( n = ol_first_node(t->idxs); n; n = n->next ) {
		sql_idx *i = n->data;
		int nr;

		memset(found, 0, len);
		for (m = keycols->h, nr = 0; m; m = m->next, nr++ ) {
			sql_kc *kc = m->data;

			for (o = i->columns->h; o; o = o->next) {
				sql_kc *ikc = o->data;

				if (kc->c == ikc->c) {
					found[nr] = 1;
					break;
				}
			}
		}
		for (nr = 0; nr<len; nr++)
			if (!found[nr])
				break;
		if (nr == len) {
			_DELETE(found);
			return i;
		}
	}
	if (found)
		_DELETE(found);
	return NULL;
}

sql_key *
key_create_done(sql_trans *tr, allocator *sa, sql_key *k)
{
	sql_idx *i;
	sqlstore *store = tr->store;

	if (k->type != fkey && k->type != ckey) {
		if ((i = table_has_idx(k->t, k->columns)) != NULL) {
			/* use available hash, or use the order */
			if (hash_index(i->type)) {
				k->idx = i;
				if (!k->idx->key)
					k->idx->key = k;
			}
		}

		/* we need to create an index */
		k->idx = create_sql_idx(store, sa, k->t, k->base.name, hash_idx);
		k->idx->key = k;

		for (node *n=k->columns->h; n; n = n->next) {
			sql_kc *kc = n->data;

			create_sql_ic(store, sa, k->idx, kc->c);
		}
	}
	k->idx = create_sql_idx_done(tr, k->idx);
	return k;
}

int
sql_trans_key_done(sql_trans *tr, sql_key *k)
{
	sql_idx *i;
	int res = LOG_OK;

	if (k->type != fkey) {
		if ((i = table_has_idx(k->t, k->columns)) != NULL) {
			/* use available hash, or use the order */
			if (hash_index(i->type)) {
				k->idx = i;
				if (!k->idx->key)
					k->idx->key = k;
			}
			return res;
		}

		/* we need to create an index */
		if ((res = sql_trans_create_idx(&k->idx, tr, k->t, k->base.name, hash_idx)))
			return res;
		k->idx->key = k;

		for (node *n=k->columns->h; n; n = n->next) {
			sql_kc *kc = n->data;

			if ((res = sql_trans_create_ic(tr, k->idx, kc->c)))
				return res;
		}
	}
	k->idx = create_sql_idx_done(tr, k->idx);
	return res;
}

int
sql_trans_drop_key(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_base *b = os_find_id(s->keys, tr, id);
	sql_key *k = (sql_key*)b;
	sql_table *t = k->t;
	int res = LOG_OK;
	sql_table *dup = NULL;

	if ((res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	k = (sql_key*)os_find_id(s->keys, tr, id); /* fetch updated key */

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = k->base.id;
		list_append(tr->dropped, local_id);
	}

	if (k->idx && (res = sql_trans_drop_idx(tr, s, k->idx->base.id, drop_action)))
		return res;
	if ((res = store_reset_sql_functions(tr, t->base.id))) /* reset sql functions depending on the table */
		return res;

	if ((res = sys_drop_key(tr, k, drop_action)))
		return res;

	/*Clean the key from the keys*/
	node *n = ol_find_name(k->t->keys, k->base.name);
	if (n)
		ol_del(k->t->keys, store, n);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

int
sql_trans_create_idx(sql_idx **i, sql_trans *tr, sql_table *t, const char *name, idx_type it)
{
	/* can only have idxs between persistent tables */
	sqlstore *store = tr->store;
	sql_idx *ni = ZNEW(sql_idx);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(tr, syss, "idxs");
	int res = LOG_OK;

	assert(it != oph_idx && it != no_idx && it != new_idx_types);
	assert(name);
	base_init(NULL, &ni->base, next_oid(tr->store), true, name);
	ni->type = it;
	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->key = NULL;

	if ((res = ol_add(t->idxs, &ni->base)))
		return res;
	if (isGlobal(t) && (res = os_add(t->s->idxs, tr, ni->base.name, dup_base(&ni->base))))
		return res;

	ATOMIC_PTR_INIT(&ni->data, NULL);
	if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
		if ((res = store->storage_api.create_idx(tr, ni))) {
			return res;
		}
	if (!isDeclaredTable(t))
		if ((res = store->table_api.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, &ni->base.name))) {
			return res;
		}
	*i = ni;
	return res;
}

int
sql_trans_create_ic(sql_trans *tr, sql_idx *i, sql_column *c)
{
	sqlstore *store = tr->store;
	sql_kc *ic = ZNEW(sql_kc);
	int nr = list_length(i->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	int res = LOG_OK;

	assert(c);
	ic->c = c;
	list_append(i->columns, ic);

	if ((res = store->table_api.table_insert(tr, sysic, &i->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int))))
		return res;
	return res;
}

int
sql_trans_drop_idx(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_idx *i = schema_find_idx_id(tr, s, id);
	int res = LOG_OK;

	if (!i) /* already dropped */
		return res;

	sql_table *t = NULL;
	if ((res = new_table(tr, i->t, &t)))
		return res;
	i = schema_find_idx_id(tr, s, id); /* fetch updated idx */

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = i->base.id;
		list_append(tr->dropped, local_id);
	}

	if (!isNew(i) && (res = sql_trans_add_dependency_change(tr, i->t->base.id, ddl)))
		return res;
	if (!isNew(i) && isGlobal(i->t) && !isGlobalTemp(i->t) && (res = sql_trans_add_dependency(tr, i->t->base.id, dml)))
		return res;
	if ((res = sys_drop_idx(tr, i, drop_action)))
		return res;
	if ((res = store_reset_sql_functions(tr, i->t->base.id))) /* reset sql functions depending on the table */
		return res;

	node *n = ol_find_name(i->t->idxs, i->base.name);
	if (n)
		ol_del(i->t->idxs, store, n);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}


static int
sql_trans_create_table_trigger(sql_trigger **tres, sql_trans *tr, sql_table *t, const char *name,
	sht time, sht orientation, sht event, const char *old_name, const char *new_name,
	const char *condition, const char *statement )
{
	sqlstore *store = tr->store;
	sql_schema *syss = (t != NULL) ? find_sql_schema(tr, isGlobal(t) ? "sys":"tmp") : find_sql_schema(tr, "sys");
	sql_table *systrigger = find_sql_table(tr, syss, "triggers");
	char *strnil = (char*)ATOMnilptr(TYPE_str);
	sql_table *dup = NULL;
	int res = LOG_OK;

	assert(name);

	if ( t && (res = new_table(tr, t, &dup)))
		return res;
	t = dup;
	sql_trigger *nt = ZNEW(sql_trigger);
	base_init(NULL, &nt->base, next_oid(tr->store), true, name);
	nt->columns = list_create((fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = time;
	nt->orientation = orientation;
	nt->event = event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (old_name)
		nt->old_name =_STRDUP(old_name);
	if (new_name)
		nt->new_name =_STRDUP(new_name);
	if (condition)
		nt->condition =_STRDUP(condition);
	nt->statement =_STRDUP(statement);
	if(t) {
		assert(isGlobal(t));
		if ((res = ol_add(t->triggers, &nt->base)) ||
			(res = os_add(t->s->triggers, tr, nt->base.name, dup_base(&nt->base)))) {
			return res;
		}
	}
	sqlid tid = t? t->base.id : int_nil;

	if ((res = store->table_api.table_insert(tr, systrigger, &nt->base.id, &nt->base.name, &tid, &nt->time, &nt->orientation,
							 &nt->event, (nt->old_name)?&nt->old_name:&strnil, (nt->new_name)?&nt->new_name:&strnil,
							 (nt->condition)?&nt->condition:&strnil, &nt->statement))) {
		return res;
	}
	*tres = nt;
	return res;
}

int
sql_trans_create_trigger(sql_trigger **tres, sql_trans *tr, sql_table *t, const char *name,
	sht time, sht orientation, sht event, const char *old_name, const char *new_name,
	const char *condition, const char *statement )
{
	if (t)
		return sql_trans_create_table_trigger(
				tres, tr, t, name, time,
			   	orientation, event, old_name,
			   	new_name, condition, statement);

	// triggers not bound to objects (e.g. table)
	// are added to sys->triggers

	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *systrigger = find_sql_table(tr, syss, "triggers");
	char *strnil = (char*)ATOMnilptr(TYPE_str);
	int res = LOG_OK;

	assert(name);

	sql_trigger *nt = ZNEW(sql_trigger);
	base_init(NULL, &nt->base, next_oid(tr->store), true, name);
	nt->time = time;
	nt->orientation = orientation;
	nt->event = event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (old_name)
		nt->old_name =_STRDUP(old_name);
	if (new_name)
		nt->new_name =_STRDUP(new_name);
	if (condition)
		nt->condition =_STRDUP(condition);
	nt->statement =_STRDUP(statement);
	if ((res = os_add(syss->triggers, tr, nt->base.name, &nt->base))) {
		trigger_destroy(store, nt);
		return res;
	}
	sqlid tid = int_nil;

	if ((res = store->table_api.table_insert(tr, systrigger, &nt->base.id, &nt->base.name, &tid, &nt->time, &nt->orientation,
							 &nt->event, (nt->old_name)?&nt->old_name:&strnil, (nt->new_name)?&nt->new_name:&strnil,
							 (nt->condition)?&nt->condition:&strnil, &nt->statement))) {
		trigger_destroy(store, nt);
		return res;
	}

	*tres = nt;
	return res;
}

int
sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_base *b = os_find_id(s->triggers, tr, id);
	int res = LOG_OK;

	if (!b) /* already dropped */
		return res;

	sql_trigger *i = (sql_trigger*)b;
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = i->base.id;
		list_append(tr->dropped, local_id);
	}

	if (i->t) {
		if ((res = store_reset_sql_functions(tr, i->t->base.id))) /* reset sql functions depending on the table */
			return res;
		node *n = ol_find_name(i->t->triggers, i->base.name);
		if (n)
			ol_del(i->t->triggers, store, n);
	}
	if ((res = sys_drop_trigger(tr, i)))
		return res;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

static sql_sequence *
create_sql_sequence_with_id(allocator *sa, sqlid id, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
					lng cacheinc, bit cycle)
{
	sql_sequence *seq = SA_ZNEW(sa, sql_sequence);

	assert(name);
	base_init(sa, &seq->base, id, true, name);
	seq->start = start;
	seq->minvalue = min;
	seq->maxvalue = max;
	seq->increment = inc;
	seq->cacheinc = cacheinc;
	seq->cycle = cycle;
	seq->s = s;
	return seq;
}

sql_sequence *
create_sql_sequence(sqlstore *store, allocator *sa, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
					lng cacheinc, bit cycle)
{
	return create_sql_sequence_with_id(sa, next_oid(store), s, name, start, min, max, inc, cacheinc, cycle);
}

int
sql_trans_create_sequence(sql_trans *tr, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
						  lng cacheinc, bit cycle, bit bedropped)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(tr, syss, "sequences");
	sql_sequence *seq = create_sql_sequence_with_id(NULL, next_oid(tr->store), s, name, start, min, max, inc, cacheinc, cycle);
	int res = LOG_OK;

	if ((res = os_add(s->seqs, tr, seq->base.name, &seq->base)))
		return res;
	if ((res = store->table_api.table_insert(tr, sysseqs, &seq->base.id, &s->base.id, &seq->base.name, &seq->start, &seq->minvalue,
							 &seq->maxvalue, &seq->increment, &seq->cacheinc, &seq->cycle)))
		return res;

	/*Create a BEDROPPED dependency for a SERIAL COLUMN*/
	if (bedropped) {
		if ((res = sql_trans_create_dependency(tr, seq->base.id, seq->base.id, BEDROPPED_DEPENDENCY)))
			return res;
		if (!isNew(seq) && (res = sql_trans_add_dependency(tr, seq->base.id, ddl)))
			return res;
	}
	return res;
}

int
sql_trans_drop_sequence(sql_trans *tr, sql_schema *s, sql_sequence *seq, int drop_action)
{
	int res = LOG_OK;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid* local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) &id_destroy);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = seq->base.id;
		list_append(tr->dropped, local_id);
	}

	if ((res = sys_drop_sequence(tr, seq, drop_action)))
		return res;
	if ((res = os_del(s->seqs, tr, seq->base.name, dup_base(&seq->base))))
		return res;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return res;
}

int
sql_trans_alter_sequence(sql_trans *tr, sql_sequence *seq, lng min, lng max, lng inc, lng cache, bit cycle)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(tr, syss, "sequences");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
	sql_column *c;
	int res = LOG_OK;

	if (is_oid_nil(rid))
		return -1;
	if (!is_lng_nil(min) && seq->minvalue != min) {
		seq->minvalue = min;
		c = find_sql_column(seqs, "minvalue");
		if ((res = store->table_api.column_update_value(tr, c, rid, &seq->minvalue)))
			return res;
	}
	if (!is_lng_nil(max) && seq->maxvalue != max) {
		seq->maxvalue = max;
		c = find_sql_column(seqs, "maxvalue");
		if ((res = store->table_api.column_update_value(tr, c, rid, &seq->maxvalue)))
			return res;
	}
	if (!is_lng_nil(inc) && seq->increment != inc) {
		seq->increment = inc;
		c = find_sql_column(seqs, "increment");
		if ((res = store->table_api.column_update_value(tr, c, rid, &seq->increment)))
			return res;
	}
	if (!is_lng_nil(cache) && seq->cacheinc != cache) {
		seq->cacheinc = cache;
		c = find_sql_column(seqs, "cacheinc");
		if ((res = store->table_api.column_update_value(tr, c, rid, &seq->cacheinc)))
			return res;
	}
	if (!is_bit_nil(cycle) && seq->cycle != cycle) {
		seq->cycle = cycle != 0;
		c = find_sql_column(seqs, "cycle");
		if ((res = store->table_api.column_update_value(tr, c, rid, &seq->cycle)))
			return res;
	}
	return res;
}

int
sql_trans_sequence_restart(sql_trans *tr, sql_sequence *seq, lng start)
{
	int res = LOG_OK;
	sqlstore *store = tr->store;

	if (!is_lng_nil(start) && seq->start != start) { /* new valid value, change */
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *seqs = find_sql_table(tr, syss, "sequences");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
		sql_column *c = find_sql_column(seqs, "start");

		assert(!is_oid_nil(rid));
		seq->start = start;
		if ((res = store->table_api.column_update_value(tr, c, rid, &start)))
			return res;
	}
	return seq_restart(tr->store, seq, start) ? 0 : -4;
}

sql_session *
sql_session_create(sqlstore *store, allocator *sa, int ac)
{
	sql_session *s;

	if (store->singleuser > 1) {
		TRC_ERROR(SQL_STORE, "No second connection allowed in singleuser mode\n");
		return NULL;
	}

	s = ZNEW(sql_session);
	if (!s)
		return NULL;
	s->sa = sa;
	assert(sa);
	s->tr = sql_trans_create_(store, NULL, NULL);
	if (!s->tr) {
		_DELETE(s);
		return NULL;
	}
	s->tr->active = 0;
	if (!sql_session_reset(s, ac)) {
		sql_trans_destroy(s->tr);
		_DELETE(s);
		return NULL;
	}
	if (store->singleuser)
		store->singleuser = 2;
	return s;
}

void
sql_session_destroy(sql_session *s)
{
	if (s->tr) {
		sqlstore *store = s->tr->store;
		if (store->singleuser)
			store->singleuser = 1;
	}
	// TODO check if s->tr is not always there
	assert(!s->tr || s->tr->active == 0);
	if (s->tr)
		sql_trans_destroy(s->tr);
	_DELETE(s);
}

int
sql_session_reset(sql_session *s, int ac)
{
	if (!s->tr)
		return 0;

	assert(s->tr && s->tr->active == 0);
	s->schema_name = s->def_schema_name;
	s->schema = NULL;
	s->auto_commit = s->ac_on_commit = ac;
	s->level = tr_serializable;
	return 1;
}

int
sql_trans_begin(sql_session *s)
{
	sql_trans *tr = s->tr;
	sqlstore *store = tr->store;

	store_lock(store);
	TRC_DEBUG(SQL_STORE, "Enter sql_trans_begin for transaction: " ULLFMT "\n", tr->tid);
	tr->ts = store_timestamp(store);
	if (s->schema_name && !(s->schema = find_sql_schema(tr, s->schema_name)))
		s->schema_name = s->def_schema_name;
	if (!s->schema_name)
		s->schema_name = "sys";
	if (s->schema_name && !(s->schema = find_sql_schema(tr, s->schema_name))) {
		TRC_DEBUG(SQL_STORE, "Exit sql_trans_begin for transaction: " ULLFMT " with error, the schema %s was not found\n", tr->tid, s->schema_name);
		store_unlock(store);
		return -3;
	}
	tr->active = 1;

	int res = ATOMIC_GET(&s->schema_version) ?
		ATOMIC_GET(&s->schema_version) != ATOMIC_GET(&tr->cat->schema_version) : 0;
	ATOMIC_SET(&s->schema_version, tr->cat->schema_version);

	ATOMIC_INC(&store->nr_active);
	list_append(store->active, tr);

	TRC_DEBUG(SQL_STORE, "Exit sql_trans_begin for transaction: " ULLFMT "\n", tr->tid);
	store_unlock(store);
	s->status = tr->status = 0;
	return res;
}

int
sql_trans_end(sql_session *s, int ok)
{
	int res = SQL_OK;
	TRC_DEBUG(SQL_STORE, "End of transaction: " ULLFMT "\n", s->tr->tid);
	if (ok == SQL_OK) {
		assert(!s->status && !s->tr->status);
		res = sql_trans_commit(s->tr);
	}
	if (ok == SQL_ERR || res != SQL_OK) /* if a conflict happened, it was already rollbacked */
		sql_trans_rollback(s->tr, false);
	assert(s->tr->active);
	sqlstore *store = s->tr->store;
	store_lock(store);
	s->tr->active = 0;
	s->tr->status = 0;
	s->auto_commit = s->ac_on_commit;
	s->schema = NULL;
	list_remove_data(store->active, NULL, s->tr);
	ATOMIC_SET(&store->lastactive, GDKusec());
	ATOMIC_DEC(&store->nr_active);
	ulng oldest = store_get_timestamp(store);
	if (store->active && store->active->h) {
		for(node *n = store->active->h; n; n = n->next) {
			sql_trans *tr = n->data;
			if (tr->ts < oldest)
				oldest = tr->ts;
		}
	}
	ATOMIC_SET(&store->oldest, oldest);
	assert(list_length(store->active) == (int) ATOMIC_GET(&store->nr_active));
	store_unlock(store);
	return res;
}

void
find_partition_type(sql_subtype *tpe, sql_table *mt)
{
	if (isPartitionedByColumnTable(mt)) {
		*tpe = mt->part.pcol->type;
	} else if (isPartitionedByExpressionTable(mt)) {
		*tpe = mt->part.pexp->type;
	} else {
		assert(0);
	}
}

static int
convert_part_values(sql_trans *tr, sql_table *mt )
{
	sql_subtype found = { 0 };
	int localtype;
	find_partition_type(&found, mt);
	localtype = found.type->localtype;

	if (localtype != TYPE_str && mt->members && list_length(mt->members)) {
		for (node *n = mt->members->h; n; n = n->next) {
			sql_part *p = n->data;

			if (isListPartitionTable(mt)) {
				for (node *m = p->part.values->h; m; m = m->next) {
					sql_part_value *v = (sql_part_value*) m->data, ov = *v;
					ValRecord vvalue;
					ptr ok;

					vvalue = (ValRecord) {.vtype = TYPE_void,};
					ok = VALinit(&vvalue, TYPE_str, v->value);
					if (ok)
						ok = VALconvert(localtype, &vvalue);
					if (ok) {
						ok = v->value = NEW_ARRAY(char, vvalue.len);
						if (ok) {
							memcpy(v->value, VALget(&vvalue), vvalue.len);
							v->length = vvalue.len;
						}
					}
					VALclear(&vvalue);
					if (!ok)
						return -1;
					_DELETE(ov.value);
				}
			} else if (isRangePartitionTable(mt)) {
				ValRecord vmin, vmax;
				ptr ok;

				vmin = vmax = (ValRecord) {.vtype = TYPE_void,};
				ok = VALinit(&vmin, TYPE_str, p->part.range.minvalue);
				if (ok)
					ok = VALinit(&vmax, TYPE_str, p->part.range.maxvalue);
				_DELETE(p->part.range.minvalue);
				_DELETE(p->part.range.maxvalue);
				if (ok) {
					if (strNil((const char *)VALget(&vmin)) &&
						strNil((const char *)VALget(&vmax))) {
						const void *nil_ptr = ATOMnilptr(localtype);
						size_t nil_len = ATOMlen(localtype, nil_ptr);

						p->part.range.minvalue = NEW_ARRAY(char, nil_len);
						p->part.range.maxvalue = NEW_ARRAY(char, nil_len);
						if (p->part.range.minvalue == NULL ||
							p->part.range.maxvalue == NULL) {
							ok = NULL;
							_DELETE(p->part.range.minvalue);
							_DELETE(p->part.range.maxvalue);
						} else {
							memcpy(p->part.range.minvalue, nil_ptr, nil_len);
							memcpy(p->part.range.maxvalue, nil_ptr, nil_len);
							p->part.range.minlength = nil_len;
							p->part.range.maxlength = nil_len;
						}
					} else {
						ok = VALconvert(localtype, &vmin);
						if (ok)
							ok = VALconvert(localtype, &vmax);
						if (ok) {
							p->part.range.minvalue = NEW_ARRAY(char, vmin.len);
							p->part.range.maxvalue = NEW_ARRAY(char, vmax.len);
							if (p->part.range.minvalue == NULL ||
								p->part.range.maxvalue == NULL) {
								ok = NULL;
								_DELETE(p->part.range.minvalue);
								_DELETE(p->part.range.maxvalue);
							} else {
								memcpy(p->part.range.minvalue, VALget(&vmin), vmin.len);
								memcpy(p->part.range.maxvalue, VALget(&vmax), vmax.len);
								p->part.range.minlength = vmin.len;
								p->part.range.maxlength = vmax.len;
							}
						}
					}
					if (ok && isPartitionedByColumnTable(p->t))
						col_set_range(tr, p, true);
				}
				VALclear(&vmin);
				VALclear(&vmax);
				if (!ok)
					return -1;
			}
		}
	}
	return 0;
}

int
sql_trans_convert_partitions(sql_trans *tr)
{
	struct os_iter si;
	os_iterator(&si, tr->cat->schemas, tr, NULL);
	for(sql_base *b = oi_next(&si); b; b = oi_next(&si)) {
		sql_schema *ss = (sql_schema*)b;
		struct os_iter oi;
		os_iterator(&oi, ss->tables, tr, NULL);
		for(sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *tt = (sql_table*)b;
			if (isPartitionedByColumnTable(tt) || isPartitionedByExpressionTable(tt)) {
				if (convert_part_values(tr, tt) < 0)
					return -1;
			}
		}
	}
	return 0;
}

void
store_printinfo(sqlstore *store)
{
	if (!MT_lock_trytime(&store->commit, 1000)) {
		printf("WAL is currently locked, so no WAL information\n");
		return;
	}
	printf("WAL:\n");
	printf("SQL store oldest pending "ULLFMT"\n", store->oldest_pending);
	log_printinfo(store->logger);
	MT_lock_unset(&store->commit);
}
