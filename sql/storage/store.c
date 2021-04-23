/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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

/* version 05.23.00 of catalog */
#define CATALOG_VERSION 52300	/* first after Oct2020 */

static int sys_drop_table(sql_trans *tr, sql_table *t, int drop_action);

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

static ulng
store_oldest_given_max(sqlstore *store, ulng commit_ts)
{
	if (ATOMIC_GET(&store->nr_active) <= 1)
		return commit_ts;
	return store->oldest;
}

ulng
store_oldest(sqlstore *store)
{
	return store_oldest_given_max(store, TRANSACTION_ID_BASE);
}

static inline bool
instore(sqlid id)
{
	if (id >= 2000 && id <= 2164)
		return true;
	return false;
}

static void
id_destroy(sqlstore *store, int *id)
{
	(void)store;
	GDKfree(id);
}

static void
type_destroy(sqlstore *store, sql_type *t)
{
	(void)store;
	assert(t->base.refcnt > 0);
	if (--(t->base.refcnt) > 0)
		return;
	_DELETE(t->sqlname);
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
	if (--(f->base.refcnt) > 0)
		return;
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
	if (--(s->base.refcnt) > 0)
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
	if (--(k->base.refcnt) > 0)
		return;
	list_destroy2(k->columns, store);
	k->columns = NULL;
	_DELETE(k->base.name);
	_DELETE(k);
}

static void
idx_destroy(sqlstore *store, sql_idx * i)
{
	assert(i->base.refcnt > 0);
	if (--(i->base.refcnt) > 0)
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
	if (--(t->base.refcnt) > 0)
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

static void
column_destroy(sqlstore *store, sql_column *c)
{
	assert(c->base.refcnt > 0);
	if (--(c->base.refcnt) > 0)
		return;
	if (ATOMIC_PTR_GET(&c->data))
		store->storage_api.destroy_col(store, c);
	_DELETE(c->min);
	_DELETE(c->max);
	_DELETE(c->def);
	_DELETE(c->base.name);
	_DELETE(c);
}

static void
int_destroy(sqlstore *store, int *v)
{
	(void)store;
	_DELETE(v);
}

static void
table_destroy(sqlstore *store, sql_table *t)
{
	assert(t->base.refcnt > 0);
	if (--(t->base.refcnt) > 0)
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
	if (--(p->base.refcnt) > 0)
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
	if (--(s->base.refcnt) > 0)
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
load_keycolumn(sql_trans *tr, sql_key *k, oid rid)
{
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

	v = store->table_api.column_find_string_start(tr, find_sql_column(objects, "name"), rid, &cbat);
	kc->c = find_sql_column(k->t, v);
	store->table_api.column_find_string_end(cbat);
	list_append(k->columns, kc);
	assert(kc->c);
}

static sql_key *
load_key(sql_trans *tr, sql_table *t, oid rid)
{
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *keys = find_sql_table(tr, syss, "keys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sql_column *kc_id, *kc_nr;
	key_type ktype;
	rids *rs;
	sqlid kid;
	oid r = oid_nil;
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

 	ktype = (key_type) store->table_api.column_find_int(tr, find_sql_column(keys, "type"), rid);
	nk = (ktype != fkey)?(sql_key*)SA_ZNEW(tr->sa, sql_ukey):(sql_key*)SA_ZNEW(tr->sa, sql_fkey);
 	kid = store->table_api.column_find_sqlid(tr, find_sql_column(keys, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(keys, "name"), rid, &cbat);
	base_init(tr->sa, &nk->base, kid, 0, v);
	store->table_api.column_find_string_end(cbat);
	nk->type = ktype;
	nk->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	nk->t = t;

	if (ktype == ukey || ktype == pkey) {
		sql_ukey *uk = (sql_ukey *) nk;

		if (ktype == pkey)
			t->pkey = uk;
	} else {
		sql_fkey *fk = (sql_fkey *) nk;
		int action;

		action = store->table_api.column_find_int(tr, find_sql_column(keys, "action"), rid);
		fk->on_delete = action & 255;
		fk->on_update = (action>>8) & 255;

		fk->rkey = store->table_api.column_find_sqlid(tr, find_sql_column(keys, "rkey"), rid);
	}

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = store->table_api.rids_select(tr, kc_id, &nk->base.id, &nk->base.id, NULL);
	rs = store->table_api.rids_orderby(tr, rs, kc_nr);
	for (r = store->table_api.rids_next(rs); !is_oid_nil(r); r = store->table_api.rids_next(rs))
		load_keycolumn(tr, nk, r);
	store->table_api.rids_destroy(rs);

	/* find idx with same name */
	sql_base *i = os_find_name(nk->t->s->idxs, tr, nk->base.name);
	if (i) {
		nk->idx = (sql_idx*)i;
		nk->idx->key = nk;
	}
	return nk;
}

static void
load_idxcolumn(sql_trans *tr, sql_idx * i, oid rid)
{
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

	v = store->table_api.column_find_string_start(tr, find_sql_column(objects, "name"), rid, &cbat);
	kc->c = find_sql_column(i->t, v);
	store->table_api.column_find_string_end(cbat);
	assert(kc->c);
	list_append(i->columns, kc);
	if (hash_index(i->type))
		kc->c->unique = 1;
	if (hash_index(i->type) && list_length(i->columns) > 1) {
		/* Correct the unique flag of the keys first column */
		kc->c->unique = list_length(i->columns);
		if (kc->c->unique == 2) {
			sql_kc *ic1 = i->columns->h->data;
			ic1->c->unique ++;
		}
	}
}

static sql_idx *
load_idx(sql_trans *tr, sql_table *t, oid rid)
{
	sql_idx *ni = SA_ZNEW(tr->sa, sql_idx);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *idxs = find_sql_table(tr, syss, "idxs");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sql_column *kc_id, *kc_nr;
	rids *rs;
	sqlid iid;
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

	iid = store->table_api.column_find_sqlid(tr, find_sql_column(idxs, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(idxs, "name"), rid, &cbat);
	base_init(tr->sa, &ni->base, iid, 0, v);
	store->table_api.column_find_string_end(cbat);
	ni->type = (idx_type) store->table_api.column_find_int(tr, find_sql_column(idxs, "type"), rid);
	ni->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	ni->t = t;
	ni->key = NULL;

	if (isTable(ni->t) && idx_has_column(ni->type))
		store->storage_api.create_idx(tr, ni);

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = store->table_api.rids_select(tr, kc_id, &ni->base.id, &ni->base.id, NULL);
	rs = store->table_api.rids_orderby(tr, rs, kc_nr);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs))
		load_idxcolumn(tr, ni, rid);
	store->table_api.rids_destroy(rs);
	return ni;
}

static void
load_triggercolumn(sql_trans *tr, sql_trigger * i, oid rid)
{
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

	v = store->table_api.column_find_string_start(tr, find_sql_column(objects, "name"), rid, &cbat);
	kc->c = find_sql_column(i->t, v);
	store->table_api.column_find_string_end(cbat);
	list_append(i->columns, kc);
	assert(kc->c);
}

static sql_trigger *
load_trigger(sql_trans *tr, sql_table *t, oid rid)
{
	sql_trigger *nt = SA_ZNEW(tr->sa, sql_trigger);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *triggers = find_sql_table(tr, syss, "triggers");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sql_column *kc_id, *kc_nr;
	sqlid tid;
	rids *rs;
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

	tid = store->table_api.column_find_sqlid(tr, find_sql_column(triggers, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(triggers, "name"), rid, &cbat);
	base_init(tr->sa, &nt->base, tid, 0, v);
	store->table_api.column_find_string_end(cbat);

	nt->time = store->table_api.column_find_sht(tr, find_sql_column(triggers, "time"), rid);
	nt->orientation = store->table_api.column_find_sht(tr, find_sql_column(triggers, "orientation"),rid);
	nt->event = store->table_api.column_find_sht(tr, find_sql_column(triggers, "event"), rid);

	v = store->table_api.column_find_string_start(tr, find_sql_column(triggers, "old_name"), rid, &cbat);
	if (!strNil(v))
		nt->old_name = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(triggers, "new_name"), rid, &cbat);
	if (!strNil(v))
		nt->new_name = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(triggers, "condition"), rid, &cbat);
	if (!strNil(v))
		nt->condition = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(triggers, "statement"), rid, &cbat);
	if (!strNil(v))
		nt->statement = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);

	nt->t = t;
	nt->columns = list_new(tr->sa, (fdestroy) &kc_destroy);

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = store->table_api.rids_select(tr, kc_id, &nt->base.id, &nt->base.id, NULL);
	rs = store->table_api.rids_orderby(tr, rs, kc_nr);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs))
		load_triggercolumn(tr, nt, rid);
	store->table_api.rids_destroy(rs);
	return nt;
}

static sql_column *
load_column(sql_trans *tr, sql_table *t, oid rid)
{
	sql_column *c = SA_ZNEW(tr->sa, sql_column);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *columns = find_sql_table(tr, syss, "_columns");
	sqlid cid;
	sqlstore *store = tr->store;
	str v, def, tpe, st;
	ptr cbat;
	int sz, d;

	cid = store->table_api.column_find_sqlid(tr, find_sql_column(columns, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(columns, "name"), rid, &cbat);
	base_init(tr->sa, &c->base, cid, 0, v);
	store->table_api.column_find_string_end(cbat);

	sz = store->table_api.column_find_int(tr, find_sql_column(columns, "type_digits"), rid);
	d = store->table_api.column_find_int(tr, find_sql_column(columns, "type_scale"), rid);
	tpe = store->table_api.column_find_string_start(tr, find_sql_column(columns, "type"), rid, &cbat);
	if (!sql_find_subtype(&c->type, tpe, sz, d)) {
		sql_type *lt = sql_trans_bind_type(tr, t->s, tpe);
		if (lt == NULL) {
			TRC_ERROR(SQL_STORE, "SQL type '%s' is missing\n", tpe);
			store->table_api.column_find_string_end(cbat);
			return NULL;
		}
		sql_init_subtype(&c->type, lt, sz, d);
	}
	store->table_api.column_find_string_end(cbat);
	c->def = NULL;
	def = store->table_api.column_find_string_start(tr, find_sql_column(columns, "default"), rid, &cbat);
	if (!strNil(def))
		c->def = SA_STRDUP(tr->sa, def);
	store->table_api.column_find_string_end(cbat);
	c->null = (bit) store->table_api.column_find_bte(tr, find_sql_column(columns, "null"), rid);
	c->colnr = store->table_api.column_find_int(tr, find_sql_column(columns, "number"), rid);
	c->unique = 0;
	c->storage_type = NULL;
	st = store->table_api.column_find_string_start(tr, find_sql_column(columns, "storage"), rid, &cbat);
	if (!strNil(st))
		c->storage_type = SA_STRDUP(tr->sa, st);
	store->table_api.column_find_string_end(cbat);
	c->t = t;
	if (isTable(c->t))
		store->storage_api.create_col(tr, c);
	c->sorted = sql_trans_is_sorted(tr, c);
	c->dcount = 0;
	TRC_DEBUG(SQL_STORE, "Load column: %s\n", c->base.name);
	return c;
}

static int
load_range_partition(sql_trans *tr, sql_schema *syss, sql_part *pt)
{
	sql_table *ranges = find_sql_table(tr, syss, "range_partitions");
	oid rid;
	rids *rs;
	sqlstore *store = tr->store;

	rs = store->table_api.rids_select(tr, find_sql_column(ranges, "table_id"), &pt->member, &pt->member, NULL);
	if ((rid = store->table_api.rids_next(rs)) != oid_nil) {
		ptr cbat;
		str v;

		pt->with_nills = (bit) store->table_api.column_find_bte(tr, find_sql_column(ranges, "with_nulls"), rid);
		v = store->table_api.column_find_string_start(tr, find_sql_column(ranges, "minimum"), rid, &cbat);
		pt->part.range.minvalue = SA_STRDUP(tr->sa, v);
		pt->part.range.minlength = strLen(v);
		store->table_api.column_find_string_end(cbat);
		v = store->table_api.column_find_string_start(tr, find_sql_column(ranges, "maximum"), rid, &cbat);
		pt->part.range.maxvalue = SA_STRDUP(tr->sa, v);
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
	sql_table *values = find_sql_table(tr, syss, "value_partitions");
	list *vals = NULL;
	oid rid;
	rids *rs = store->table_api.rids_select(tr, find_sql_column(values, "table_id"), &pt->member, &pt->member, NULL);

	vals = SA_LIST(tr->sa, (fdestroy) &part_value_destroy);
	if (!vals) {
		store->table_api.rids_destroy(rs);
		return -1;
	}

	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		ptr cbat;
		str v;

		v = store->table_api.column_find_string_start(tr, find_sql_column(values, "value"), rid, &cbat);
		if (strNil(v)) { /* check for null value */
			pt->with_nills = true;
		} else {
			sql_part_value *nextv = SA_ZNEW(tr->sa, sql_part_value);
			nextv->value = SA_STRDUP(tr->sa, v);
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
	sql_part *pt = SA_ZNEW(tr->sa, sql_part);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(tr, syss, "objects");
	sqlid id;
	sqlstore *store = tr->store;
	str v;
	ptr cbat;

	assert(isMergeTable(mt) || isReplicaTable(mt));
	id = store->table_api.column_find_sqlid(tr, find_sql_column(objects, "id"), rid);
	if (is_int_nil(id)) { /* upgrade case, the id it's not initialized */
		id = store_next_oid(store);
		store->table_api.column_update_value(tr, find_sql_column(objects, "id"), rid, &id);
	}
	v = store->table_api.column_find_string_start(tr, find_sql_column(objects, "name"), rid, &cbat);
	base_init(tr->sa, &pt->base, id, 0, v);
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

static sql_base *
dup_base(sql_base *b)
{
	b->refcnt++;
	return b;
}

static sql_table *
load_table(sql_trans *tr, sql_schema *s, sqlid tid, subrids *nrs)
{
	sqlstore *store = tr->store;
	sql_table *t = SA_ZNEW(tr->sa, sql_table);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *tables = find_sql_table(tr, syss, "_tables");
	sql_table *idxs = find_sql_table(tr, syss, "idxs");
	sql_table *keys = find_sql_table(tr, syss, "keys");
	sql_table *triggers = find_sql_table(tr, syss, "triggers");
	sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
	sql_column *idx_table_id, *key_table_id, *trigger_table_id, *partitions_table_id;
	oid rid;
	sqlid pcolid = int_nil;
	rids *rs;
	str v, exp = NULL;
	ptr cbat;

	rid = store->table_api.column_find_row(tr, find_sql_column(tables, "id"), &tid, NULL);
	v = store->table_api.column_find_string_start(tr, find_sql_column(tables, "name"), rid, &cbat);
	base_init(tr->sa, &t->base, tid, 0, v);
	store->table_api.column_find_string_end(cbat);
	t->query = NULL;
	v = store->table_api.column_find_string_start(tr, find_sql_column(tables, "query"), rid, &cbat);
	if (!strNil(v))
		t->query = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	t->type = store->table_api.column_find_sht(tr, find_sql_column(tables, "type"), rid);
	t->system = (bit) store->table_api.column_find_bte(tr, find_sql_column(tables, "system"), rid);
	t->commit_action = (ca_t) store->table_api.column_find_sht(tr, find_sql_column(tables, "commit_action"),rid);
	t->persistence = SQL_PERSIST;
	if (t->commit_action)
		t->persistence = SQL_GLOBAL_TEMP;
	if (isRemote(t))
		t->persistence = SQL_REMOTE;
	t->access = store->table_api.column_find_sht(tr, find_sql_column(tables, "access"),rid);

	t->pkey = NULL;
	t->s = s;
	t->sz = COLSIZE;

	t->columns = ol_new(tr->sa, (destroy_fptr) &column_destroy);
	t->idxs = ol_new(tr->sa, (destroy_fptr) &idx_destroy);
	t->keys = ol_new(tr->sa, (destroy_fptr) &key_destroy);
	t->triggers = ol_new(tr->sa, (destroy_fptr) &trigger_destroy);
	if (isMergeTable(t) || isReplicaTable(t))
		t->members = list_new(tr->sa, (fdestroy) &part_destroy);

	if (isTable(t)) {
		if (store->storage_api.create_del(tr, t) != LOG_OK) {
			TRC_DEBUG(SQL_STORE, "Load table '%s' is missing 'deletes'", t->base.name);
			return NULL;
		}
	}

	TRC_DEBUG(SQL_STORE, "Load table: %s\n", t->base.name);

	partitions_table_id = find_sql_column(partitions, "table_id");
	rs = store->table_api.rids_select(tr, partitions_table_id, &t->base.id, &t->base.id, NULL);
	if ((rid = store->table_api.rids_next(rs)) != oid_nil) {
		t->properties |= store->table_api.column_find_bte(tr, find_sql_column(partitions, "type"), rid);

		if (isPartitionedByColumnTable(t)) {
			pcolid = store->table_api.column_find_sqlid(tr, find_sql_column(partitions, "column_id"), rid);
		} else {
			v = store->table_api.column_find_string_start(tr, find_sql_column(partitions, "expression"), rid, &cbat);
			assert(!strNil(v));
			exp = SA_STRDUP(tr->sa, v);
			store->table_api.column_find_string_end(cbat);
		}
	}
	store->table_api.rids_destroy(rs);

	assert((!isRangePartitionTable(t) && !isListPartitionTable(t)) || (!exp && !is_int_nil(pcolid)) || (exp && is_int_nil(pcolid)));
	if (isPartitionedByExpressionTable(t)) {
		t->part.pexp = SA_ZNEW(tr->sa, sql_expression);
		t->part.pexp->exp = exp;
		t->part.pexp->type = *sql_bind_localtype("void"); /* initialized at initialize_sql_parts */
		t->part.pexp->cols = SA_LIST(tr->sa, (fdestroy) &int_destroy);
	}
	for (rid = store->table_api.subrids_next(nrs); !is_oid_nil(rid); rid = store->table_api.subrids_next(nrs)) {
		sql_column* next = load_column(tr, t, rid);
		if (next == NULL)
			return NULL;
		ol_add(t->columns, &next->base);
		if (pcolid == next->base.id) {
			t->part.pcol = next;
		}
	}

	if (!isKindOfTable(t))
		return t;

	/* load idx's first as the may be needed by the keys */
	idx_table_id = find_sql_column(idxs, "table_id");
	rs = store->table_api.rids_select(tr, idx_table_id, &t->base.id, &t->base.id, NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_idx *i = load_idx(tr, t, rid);

		ol_add(t->idxs, &i->base);
		if (os_add(s->idxs, tr, i->base.name, dup_base(&i->base))) {
			idx_destroy(store, i);
			table_destroy(store, t);
			return NULL;
		}
	}
	store->table_api.rids_destroy(rs);

	key_table_id = find_sql_column(keys, "table_id");
	rs = store->table_api.rids_select(tr, key_table_id, &t->base.id, &t->base.id, NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_key *k = load_key(tr, t, rid);

		ol_add(t->keys, &k->base);
		if (os_add(s->keys, tr, k->base.name, dup_base(&k->base)) ||
			os_add(tr->cat->objects, tr, k->base.name, dup_base(&k->base))) {
			key_destroy(store, k);
			table_destroy(store, t);
			return NULL;
		}
	}
	store->table_api.rids_destroy(rs);

	trigger_table_id = find_sql_column(triggers, "table_id");
	rs = store->table_api.rids_select(tr, trigger_table_id, &t->base.id, &t->base.id,NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_trigger *k = load_trigger(tr, t, rid);

		ol_add(t->triggers, &k->base);
		if (os_add(s->triggers, tr, k->base.name, dup_base(&k->base))) {
			trigger_destroy(store, k);
			table_destroy(store, t);
		}
	}
	store->table_api.rids_destroy(rs);
	return t;
}

static sql_type *
load_type(sql_trans *tr, sql_schema *s, oid rid)
{
	sqlstore *store = tr->store;
	sql_type *t = SA_ZNEW(tr->sa, sql_type);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *types = find_sql_table(tr, syss, "types");
	sqlid tid;
	str v;
	ptr cbat;

	tid = store->table_api.column_find_sqlid(tr, find_sql_column(types, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(types, "systemname"), rid, &cbat);
	base_init(tr->sa, &t->base, tid, 0, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(types, "sqlname"), rid, &cbat);
	t->sqlname = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	t->digits = store->table_api.column_find_int(tr, find_sql_column(types, "digits"), rid);
	t->scale = store->table_api.column_find_int(tr, find_sql_column(types, "scale"), rid);
	t->radix = store->table_api.column_find_int(tr, find_sql_column(types, "radix"), rid);
	t->eclass = (sql_class)store->table_api.column_find_int(tr, find_sql_column(types, "eclass"), rid);
	t->localtype = ATOMindex(t->base.name);
	t->bits = 0;
	t->s = s;
	return t;
}

static sql_arg *
load_arg(sql_trans *tr, sql_func *f, oid rid)
{
	sqlstore *store = tr->store;
	sql_arg *a = SA_ZNEW(tr->sa, sql_arg);
	unsigned int digits, scale;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *args = find_sql_table(tr, syss, "args");
	str v, tpe;
	ptr cbat;

	v = store->table_api.column_find_string_start(tr, find_sql_column(args, "name"), rid, &cbat);
	a->name = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	a->inout = store->table_api.column_find_bte(tr, find_sql_column(args, "inout"), rid);
	digits = store->table_api.column_find_int(tr, find_sql_column(args, "type_digits"), rid);
	scale = store->table_api.column_find_int(tr, find_sql_column(args, "type_scale"), rid);

	tpe = store->table_api.column_find_string_start(tr, find_sql_column(args, "type"), rid, &cbat);
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
	sql_func *t = SA_ZNEW(tr->sa, sql_func);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *funcs = find_sql_table(tr, syss, "functions");
	oid rid;
	bool update_env;	/* hacky way to update env function */
	str v;
	ptr cbat;

	rid = store->table_api.column_find_row(tr, find_sql_column(funcs, "id"), &fid, NULL);
	v = store->table_api.column_find_string_start(tr, find_sql_column(funcs, "name"), rid, &cbat);
	update_env = strcmp(v, "env") == 0;
	base_init(tr->sa, &t->base, fid, 0, v);
	store->table_api.column_find_string_end(cbat);
	v = store->table_api.column_find_string_start(tr, find_sql_column(funcs, "func"), rid, &cbat);
	update_env = update_env && strstr(v, "EXTERNAL NAME sql.sql_environment") != NULL;
	if (update_env) {
		/* see creation of env in sql_create_env()
		 * also see upgrade code in sql_upgrades.c */
		v = "CREATE FUNCTION env() RETURNS TABLE( name varchar(1024), value varchar(2048)) EXTERNAL NAME inspect.\"getEnvironment\";";
	}
	t->imp = SA_STRDUP(tr->sa, v);
	store->table_api.column_find_string_end(cbat);
	if (update_env) {
		v = "inspect";
	} else {
		v = store->table_api.column_find_string_start(tr, find_sql_column(funcs, "mod"), rid, &cbat);
	}
	t->mod = SA_STRDUP(tr->sa, v);	if (!update_env) store->table_api.column_find_string_end(cbat);
	t->lang = (sql_flang) store->table_api.column_find_int(tr, find_sql_column(funcs, "language"), rid);
	t->sql = (t->lang==FUNC_LANG_SQL||t->lang==FUNC_LANG_MAL);
	t->type = (sql_ftype) store->table_api.column_find_int(tr, find_sql_column(funcs, "type"), rid);
	t->side_effect = (bit) store->table_api.column_find_bte(tr, find_sql_column(funcs, "side_effect"), rid);
	if (t->type==F_FILT)
		t->side_effect=FALSE;
	t->varres = (bit) store->table_api.column_find_bte(tr, find_sql_column(funcs, "varres"), rid);
	t->vararg = (bit) store->table_api.column_find_bte(tr, find_sql_column(funcs, "vararg"), rid);
	t->system = (bit) store->table_api.column_find_bte(tr, find_sql_column(funcs, "system"), rid);
	t->semantics = (bit) store->table_api.column_find_bte(tr, find_sql_column(funcs, "semantics"), rid);
	t->res = NULL;
	t->s = s;
	t->fix_scale = SCALE_EQ;
	t->sa = tr->sa;
	/* convert old PYTHON2 and PYTHON2_MAP to PYTHON and PYTHON_MAP
	 * see also function sql_update_jun2020() in sql_upgrades.c */
	if ((int) t->lang == 8)		/* old FUNC_LANG_PY2 */
		t->lang = FUNC_LANG_PY;
	else if ((int) t->lang == 9)	/* old FUNC_LANG_MAP_PY2 */
		t->lang = FUNC_LANG_MAP_PY;
	if (t->lang != FUNC_LANG_INT) {
		t->query = t->imp;
		t->imp = NULL;
	}

	TRC_DEBUG(SQL_STORE, "Load function: %s\n", t->base.name);

	t->ops = list_new(tr->sa, (fdestroy) &arg_destroy);
	if (rs) {
		for (rid = store->table_api.subrids_next(rs); !is_oid_nil(rid); rid = store->table_api.subrids_next(rs)) {
			sql_arg *a = load_arg(tr, t, rid);

			if (a == NULL)
				return NULL;
			if (a->inout == ARG_OUT) {
				if (!t->res)
					t->res = list_new(tr->sa, (fdestroy) &arg_destroy);
				list_append(t->res, a);
			} else {
				list_append(t->ops, a);
			}
		}
	}
	if (t->type == F_FUNC && !t->res)
		t->type = F_PROC;
	t->side_effect = (t->type==F_FILT || (t->res && (t->lang==FUNC_LANG_SQL || !list_empty(t->ops))))?FALSE:TRUE;
	return t;
}

static sql_sequence *
load_seq(sql_trans *tr, sql_schema * s, oid rid)
{
	sqlstore *store = tr->store;
	sql_sequence *seq = SA_ZNEW(tr->sa, sql_sequence);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(tr, syss, "sequences");
	sqlid sid;
	str v;
	ptr cbat;

	sid = store->table_api.column_find_sqlid(tr, find_sql_column(seqs, "id"), rid);
	v = store->table_api.column_find_string_start(tr, find_sql_column(seqs, "name"), rid, &cbat);
	base_init(tr->sa, &seq->base, sid, 0, v);
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
	ptr cbat;

	sid = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "id"), rid);
	s = find_sql_schema_id(tr, sid);

	if (s==NULL)
		return ;

	TRC_DEBUG(SQL_STORE, "Update schema: %s %d\n", s->base.name, s->base.id);

	_DELETE(s->base.name);
	v = store->table_api.column_find_string_start(tr, find_sql_column(ss, "name"), rid, &cbat);
	base_init(tr->sa, &s->base, sid, 0, v);
	store->table_api.column_find_string_end(cbat);
	s->auth_id = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "authorization"), rid);
	s->system = (bit) store->table_api.column_find_bte(tr, find_sql_column(ss, "system"), rid);
	s->owner = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "owner"), rid);
}

static sql_schema *
load_schema(sql_trans *tr, oid rid)
{
	sqlstore *store = tr->store;
	sql_schema *s = NULL, *syss = find_sql_schema(tr, "sys");
	sql_table *ss = find_sql_table(tr, syss, "schemas");
	sql_table *types = find_sql_table(tr, syss, "types");
	sql_table *tables = find_sql_table(tr, syss, "_tables");
	sql_table *funcs = find_sql_table(tr, syss, "functions");
	sql_table *seqs = find_sql_table(tr, syss, "sequences");
	sqlid sid;
	sql_column *type_schema, *type_id, *table_schema, *table_id;
	sql_column *func_schema, *func_id, *seq_schema, *seq_id;
	rids *rs;
	str v;
	ptr cbat;

	sid = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "id"), rid);
	if (instore(sid)) {
		s = find_sql_schema_id(tr, sid);

		if (s==NULL) {
			v = store->table_api.column_find_string_start(tr, find_sql_column(ss, "name"), rid, &cbat);
			s = find_sql_schema(tr, v);
			store->table_api.column_find_string_end(cbat);
			if (s == NULL) {
				GDKerror("SQL schema missing or incompatible, rebuild from archive");
				return NULL;
			}
		}
		s->base.id = sid;
	} else {
		s = SA_ZNEW(tr->sa, sql_schema);
		if (s == NULL)
			return NULL;
		v = store->table_api.column_find_string_start(tr, find_sql_column(ss, "name"), rid, &cbat);
		base_init(tr->sa, &s->base, sid, 0, v);
		store->table_api.column_find_string_end(cbat);
		s->auth_id = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "authorization"), rid);
		s->system = (bit) store->table_api.column_find_bte(tr, find_sql_column(ss, "system"), rid);
		s->owner = store->table_api.column_find_sqlid(tr, find_sql_column(ss, "owner"), rid);

		s->tables = os_new(tr->sa, (destroy_fptr) &table_destroy, false, true);
		s->types = os_new(tr->sa, (destroy_fptr) &type_destroy, false, false);
		s->funcs = os_new(tr->sa, (destroy_fptr) &func_destroy, false, false);
		s->seqs = os_new(tr->sa, (destroy_fptr) &seq_destroy, false, true);
		s->keys = os_new(tr->sa, (destroy_fptr) &key_destroy, false, true);
		s->idxs = os_new(tr->sa, (destroy_fptr) &idx_destroy, false, true);
		s->triggers = os_new(tr->sa, (destroy_fptr) &trigger_destroy, false, true);
		s->parts = os_new(tr->sa, (destroy_fptr) &part_destroy, false, false);
	}

	TRC_DEBUG(SQL_STORE, "Load schema: %s %d\n", s->base.name, s->base.id);

	sqlid tmpid = FUNC_OIDS;

	/* first load simple types */
	type_schema = find_sql_column(types, "schema_id");
	type_id = find_sql_column(types, "id");
	rs = store->table_api.rids_select(tr, type_schema, &s->base.id, &s->base.id, type_id, &tmpid, NULL, NULL);
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_type *t = load_type(tr, s, rid);
		if (os_add(s->types, tr, t->base.name, &t->base)) {
			type_destroy(store, t);
			schema_destroy(store, s);
			return NULL;
		}
	}
	store->table_api.rids_destroy(rs);

	/* second tables */
	table_schema = find_sql_column(tables, "schema_id");
	table_id = find_sql_column(tables, "id");
	/* all tables with id >= id */
	rs = store->table_api.rids_select(tr, table_schema, &sid, &sid, table_id, &tmpid, NULL, NULL);
	if (rs && !store->table_api.rids_empty(rs)) {
		sql_table *columns = find_sql_table(tr, syss, "_columns");
		sql_column *column_table_id = find_sql_column(columns, "table_id");
		sql_column *column_number = find_sql_column(columns, "number");
		subrids *nrs = store->table_api.subrids_create(tr, rs, table_id, column_table_id, column_number);
		sqlid tid;

		for (tid = store->table_api.subrids_nextid(nrs); tid >= 0; tid = store->table_api.subrids_nextid(nrs)) {
			if (!instore(tid)) {
				sql_table *t = load_table(tr, s, tid, nrs);
				if (t == NULL) {
					store->table_api.subrids_destroy(nrs);
					store->table_api.rids_destroy(rs);
					schema_destroy(store, s);
					return NULL;
				}
				if (os_add(s->tables, tr, t->base.name, &t->base)) {
					table_destroy(store, t);
					schema_destroy(store, s);
					return NULL;
				}
			} else
				while (!is_oid_nil(store->table_api.subrids_next(nrs)))
					;
		}
		store->table_api.subrids_destroy(nrs);
	}
	store->table_api.rids_destroy(rs);

	/* next functions which could use these types */
	func_schema = find_sql_column(funcs, "schema_id");
	func_id = find_sql_column(funcs, "id");
	rs = store->table_api.rids_select(tr, func_schema, &s->base.id, &s->base.id, func_id, &tmpid, NULL, NULL);
	if (rs && !store->table_api.rids_empty(rs)) {
		sql_table *args = find_sql_table(tr, syss, "args");
		sql_column *arg_func_id = find_sql_column(args, "func_id");
		sql_column *arg_number = find_sql_column(args, "number");
		subrids *nrs = store->table_api.subrids_create(tr, rs, func_id, arg_func_id, arg_number);
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
				func_destroy(store, f);
				schema_destroy(store, s);
				return NULL;
			}
		}
		/* Handle all procedures without arguments (no args) */
		rs = store->table_api.rids_diff(tr, rs, func_id, nrs, arg_func_id);
		for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
			fid = store->table_api.column_find_sqlid(tr, func_id, rid);
			f = load_func(tr, s, fid, NULL);
			if (f == NULL) {
				store->table_api.subrids_destroy(nrs);
				store->table_api.rids_destroy(rs);
				schema_destroy(store, s);
				return NULL;
			}
			if (os_add(s->funcs, tr, f->base.name, &f->base)) {
				func_destroy(store, f);
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
	for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
		sql_sequence *seq = load_seq(tr, s, rid);
		if (os_add(s->seqs, tr, seq->base.name, &seq->base)) {
			seq_destroy(store, seq);
			schema_destroy(store, s);
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

			rs = store->table_api.rids_orderby(tr, rs, mt_sub);
			for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
				sql_part *pt = load_part(tr, t, rid);
				if (isRangePartitionTable(t)) {
					load_range_partition(tr, syss, pt);
				} else if (isListPartitionTable(t)) {
					load_value_partition(tr, syss, pt);
				}
				if (os_add(s->parts, tr, pt->base.name, dup_base(&pt->base))) {
					part_destroy(store, pt);
					schema_destroy(store, s);
					return NULL;
				}
			}
			store->table_api.rids_destroy(rs);
		}
	}
	return s;
}

void
sql_trans_update_schemas(sql_trans* tr)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(tr, syss, "schemas");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	rids *schemas = store->table_api.rids_select(tr, sysschema_ids, NULL, NULL);
	oid rid;

	TRC_DEBUG(SQL_STORE, "Update schemas\n");

	for (rid = store->table_api.rids_next(schemas); !is_oid_nil(rid); rid = store->table_api.rids_next(schemas)) {
		sql_trans_update_schema(tr, rid);
	}
	store->table_api.rids_destroy(schemas);
}

static bool
load_trans(sql_trans* tr)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(tr, syss, "schemas");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	rids *schemas = store->table_api.rids_select(tr, sysschema_ids, NULL, NULL);
	oid rid;

	TRC_DEBUG(SQL_STORE, "Load transaction\n");

	for (rid = store->table_api.rids_next(schemas); !is_oid_nil(rid); rid = store->table_api.rids_next(schemas)) {
		sql_schema *ns = load_schema(tr, rid);
		if (ns == NULL)
			return false;
		if (!instore(ns->base.id)) {
			if (os_add(tr->cat->schemas, tr, ns->base.name, &ns->base)) {
				sql_trans_destroy(tr);
				return false;
			}
			if (isTempSchema(ns))
				tr->tmp = ns;
		}
	}
	store->table_api.rids_destroy(schemas);
	return true;
}

static sqlid
next_oid(sqlstore *store)
{
	sqlid id = 0;
	MT_lock_set(&store->lock);
	id = store->obj_id++;
	MT_lock_unset(&store->lock);
	return id;
}

sqlid
store_next_oid(sqlstore *store)
{
	return next_oid(store);
}

static void
insert_schemas(sql_trans *tr)
{
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
		store->table_api.table_insert(tr, sysschema, &s->base.id, &s->base.name, &s->auth_id, &s->owner, &s->system);
		struct os_iter oi;
		os_iterator(&oi, s->tables, tr, NULL);
		for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
			sql_table *t = (sql_table*)b;
			sht ca = t->commit_action;

			store->table_api.table_insert(tr, systable, &t->base.id, &t->base.name, &s->base.id, &strnil, &t->type, &t->system, &ca, &t->access);
			for (o = t->columns->l->h; o; o = o->next) {
				sql_column *c = o->data;

				store->table_api.table_insert(tr, syscolumn, &c->base.id, &c->base.name, &c->type.type->sqlname, &c->type.digits, &c->type.scale, &t->base.id, (c->def) ? &c->def : &strnil, &c->null, &c->colnr, (c->storage_type)? &c->storage_type : &strnil);
			}
		}
	}
}

static void
insert_types(sql_trans *tr, sql_table *systype)
{
	sqlstore *store = tr->store;
	for (node *n = types->h; n; n = n->next) {
		sql_type *t = n->data;
		int radix = t->radix, eclass = (int) t->eclass;
		sqlid next_schema = t->s ? t->s->base.id : 0;

		store->table_api.table_insert(tr, systype, &t->base.id, &t->base.name, &t->sqlname, &t->digits, &t->scale, &radix, &eclass, &next_schema);
	}
}

static void
insert_args(sql_trans *tr, sql_table *sysarg, list *args, sqlid funcid, const char *arg_def, int *number)
{
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
		store->table_api.table_insert(tr, sysarg, &id, &funcid, &next_name, &a->type.type->sqlname, &a->type.digits, &a->type.scale, &a->inout, &next_number);
	}
}

static void
insert_functions(sql_trans *tr, sql_table *sysfunc, list *funcs_list, sql_table *sysarg)
{
	sqlstore *store = tr->store;
	for (node *n = funcs_list->h; n; n = n->next) {
		sql_func *f = n->data;
		bit se = (f->type == F_AGGR) ? FALSE : f->side_effect;
		int number = 0, ftype = (int) f->type, flang = (int) FUNC_LANG_INT;
		sqlid next_schema = f->s ? f->s->base.id : 0;

		store->table_api.table_insert(tr, sysfunc, &f->base.id, &f->base.name, &f->imp, &f->mod, &flang, &ftype, &se, &f->varres, &f->vararg, &next_schema, &f->system, &f->semantics);
		if (f->res)
			insert_args(tr, sysarg, f->res, f->base.id, "res_%d", &number);
		if (f->ops)
			insert_args(tr, sysarg, f->ops, f->base.id, "arg_%d", &number);
	}
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
bootstrap_create_column(sql_trans *tr, sql_table *t, char *name, sqlid id, char *sqltype, unsigned int digits)
{
	sqlstore *store = tr->store;
	sql_column *col = SA_ZNEW(tr->sa, sql_column);

	if (store->obj_id <= id)
		store->obj_id = id+1;
	TRC_DEBUG(SQL_STORE, "Create column: %s\n", name);

	base_init(tr->sa, &col->base, id, t->base.flags, name);
	assert(col->base.id > 0);
	sql_find_subtype(&col->type, sqltype, digits, 0);
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;
	ol_add(t->columns, &col->base);

	if (isTable(col->t))
		store->storage_api.create_col(tr, col);
	return col;
}

static sql_table *
create_sql_table_with_id(sql_allocator *sa, sqlid id, const char *name, sht type, bit system, int persistence, int commit_action, bte properties)
{
	sql_table *t = SA_ZNEW(sa, sql_table);

	assert((persistence==SQL_PERSIST ||
		persistence==SQL_DECLARED_TABLE ||
		commit_action) && commit_action>=0);
	assert(id);
	base_init(sa, &t->base, id, TR_NEW, name);
	t->type = type;
	t->system = system;
	t->persistence = (temp_t)persistence;
	t->commit_action = (ca_t)commit_action;
	t->query = NULL;
	t->access = 0;
	t->columns = ol_new(sa, (destroy_fptr) &column_destroy);
	t->idxs = ol_new(sa, (destroy_fptr) &idx_destroy);
	t->keys = ol_new(sa, (destroy_fptr) &key_destroy);
	t->triggers = ol_new(sa, (destroy_fptr) &trigger_destroy);
	if (isMergeTable(t) || isReplicaTable(t))
		t->members = list_new(sa, (fdestroy) &part_destroy);
	t->pkey = NULL;
	t->sz = COLSIZE;
	t->s = NULL;
	t->properties = properties;
	memset(&t->part, 0, sizeof(t->part));
	return t;
}

sql_table *
create_sql_table(sqlstore *store, sql_allocator *sa, const char *name, sht type, bit system, int persistence, int commit_action, bte properties)
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
			lt = find_sql_type(tr, s, nc->type->sqlname);
		} else {
			/* Current user type belongs to another schema in the current transaction. Search there for current user type. */
			lt = sql_trans_bind_type(tr, NULL, nc->type->sqlname);
		}
		if (lt == NULL)
			GDKfatal("SQL type %s missing", nc->type->sqlname);
		sql_init_subtype(nc, lt, nc->digits, nc->scale);
	}
}

static sql_column *
dup_sql_column(sql_allocator *sa, sql_table *t, sql_column *c)
{
	sql_column *col = SA_ZNEW(sa, sql_column);

	base_init(sa, &col->base, c->base.id, c->base.flags, c->base.name);
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
	col->sorted = c->sorted;
	col->dcount = c->dcount;
	ol_add(t->columns, &col->base);
	return col;
}

static sql_part *
dup_sql_part(sql_allocator *sa, sql_table *mt, sql_part *op)
{
	sql_part *p = SA_ZNEW(sa, sql_part);

	base_init(sa, &p->base, op->base.id, op->base.flags, op->base.name);
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
dup_sql_table(sql_allocator *sa, sql_table *t)
{
	node *n;
	sql_table *nt = create_sql_table_with_id(sa, t->base.id, t->base.name, t->type, t->system, SQL_DECLARED_TABLE, t->commit_action, t->properties);

	nt->base.flags = t->base.flags;

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
bootstrap_create_table(sql_trans *tr, sql_schema *s, char *name, sqlid id)
{
	sqlstore *store = tr->store;
	int istmp = isTempSchema(s);
	int persistence = istmp?SQL_GLOBAL_TEMP:SQL_PERSIST;
	sht commit_action = istmp?CA_PRESERVE:CA_COMMIT;
	sql_table *t;

	if (store->obj_id <= id)
		store->obj_id = id+1;
	t = create_sql_table_with_id(tr->sa, id, name, tt_table, 1, persistence, commit_action, 0);
	t->bootstrap = 1;

	TRC_DEBUG(SQL_STORE, "Create table: %s\n", name);

	t->base.flags = s->base.flags;
	t->query = NULL;
	t->s = s;
	if (os_add(s->tables, tr, name, &t->base)) {
		table_destroy(store, t);
		return NULL;
	}

	if (isTable(t))
		store->storage_api.create_del(tr, t);
	return t;
}

static sql_schema *
bootstrap_create_schema(sql_trans *tr, char *name, sqlid id, sqlid auth_id, int owner)
{
	sqlstore *store = tr->store;
	sql_schema *s = SA_ZNEW(tr->sa, sql_schema);

	if (store->obj_id <= id)
		store->obj_id = id+1;
	TRC_DEBUG(SQL_STORE, "Create schema: %s %d %d\n", name, auth_id, owner);

	if (strcmp(name, dt_schema) == 0) {
		base_init(tr->sa, &s->base, (sqlid) FUNC_OIDS - 1, TR_NEW, name);
	} else {
		base_init(tr->sa, &s->base, id, TR_NEW, name);
	}
	s->auth_id = auth_id;
	s->owner = owner;
	s->system = TRUE;
	s->tables = os_new(tr->sa, (destroy_fptr) &table_destroy, false, true);
	s->types = os_new(tr->sa, (destroy_fptr) &type_destroy, false, false);
	s->funcs = os_new(tr->sa, (destroy_fptr) &func_destroy, false, false);
	s->seqs = os_new(tr->sa, (destroy_fptr) &seq_destroy, false, true);
	s->keys = os_new(tr->sa, (destroy_fptr) &key_destroy, false, true);
	s->idxs = os_new(tr->sa, (destroy_fptr) &idx_destroy, false, true);
	s->triggers = os_new(tr->sa, (destroy_fptr) &trigger_destroy, false, true);
	s->parts = os_new(tr->sa, (destroy_fptr) &part_destroy, false, false);
	if (os_add(tr->cat->schemas, tr, s->base.name, &s->base)) {
		schema_destroy(store, s);
		return NULL;
	}
	if (isTempSchema(s))
		tr->tmp = s;

	s->store = tr->store;
	return s;
}

static sqlstore *
store_load(sqlstore *store, sql_allocator *pa)
{
	sql_allocator *sa;
	sql_trans *tr;
	sql_table *t, *types, *functions, *arguments;
	sql_schema *s;

	lng lng_store_oid;

	store->sa = pa;
	sa = sa_create(pa);
	if (!sa || !store->sa)
		return NULL;

	store->first = store->logger_api.log_isnew(store);

	types_init(store->sa); /* initialize global lists of types and functions, TODO: needs to move */

	/* we store some spare oids */
	store->obj_id = FUNC_OIDS;

	if (!sequences_init())
		return NULL;
	tr = sql_trans_create(store, NULL, NULL);
	if (!tr)
		return NULL;
	tr->store = store;

	/* for now use malloc and free */
	store->active = list_create(NULL);

	if (store->first) {
		/* cannot initialize database in readonly mode */
		if (store->readonly)
			return NULL;
		if (!tr) {
			TRC_CRITICAL(SQL_STORE, "Failed to start a transaction while loading the storage\n");
			return NULL;
		}
	}
	tr->active = 1;

	s = bootstrap_create_schema(tr, "sys", 2000, ROLE_SYSADMIN, USER_MONETDB);
	if (!store->first)
		s->base.flags = 0;

	t = bootstrap_create_table(tr, s, "schemas", 2001);
	bootstrap_create_column(tr, t, "id", 2002, "int", 32);
	bootstrap_create_column(tr, t, "name", 2003, "varchar", 1024);
	bootstrap_create_column(tr, t, "authorization", 2004, "int", 32);
	bootstrap_create_column(tr, t, "owner", 2005, "int", 32);
	bootstrap_create_column(tr, t, "system", 2006, "boolean", 1);

	types = t = bootstrap_create_table(tr, s, "types", 2007);
	bootstrap_create_column(tr, t, "id", 2008, "int", 32);
	bootstrap_create_column(tr, t, "systemname", 2009, "varchar", 256);
	bootstrap_create_column(tr, t, "sqlname", 2010, "varchar", 1024);
	bootstrap_create_column(tr, t, "digits", 2011, "int", 32);
	bootstrap_create_column(tr, t, "scale", 2012, "int", 32);
	bootstrap_create_column(tr, t, "radix", 2013, "int", 32);
	bootstrap_create_column(tr, t, "eclass", 2014, "int", 32);
	bootstrap_create_column(tr, t, "schema_id", 2015, "int", 32);

	functions = t = bootstrap_create_table(tr, s, "functions", 2016);
	bootstrap_create_column(tr, t, "id", 2017, "int", 32);
	bootstrap_create_column(tr, t, "name", 2018, "varchar", 256);
	bootstrap_create_column(tr, t, "func", 2019, "varchar", 8196);
	bootstrap_create_column(tr, t, "mod", 2020, "varchar", 8196);

	/* language asm=0, sql=1, R=2, C=3, J=4 */
	bootstrap_create_column(tr, t, "language", 2021, "int", 32);

	/* func, proc, aggr or filter */
	bootstrap_create_column(tr, t, "type", 2022, "int", 32);
	bootstrap_create_column(tr, t, "side_effect", 2023, "boolean", 1);
	bootstrap_create_column(tr, t, "varres", 2024, "boolean", 1);
	bootstrap_create_column(tr, t, "vararg", 2025, "boolean", 1);
	bootstrap_create_column(tr, t, "schema_id", 2026, "int", 32);
	bootstrap_create_column(tr, t, "system", 2027, "boolean", 1);
	bootstrap_create_column(tr, t, "semantics", 2162, "boolean", 1);

	arguments = t = bootstrap_create_table(tr, s, "args", 2028);
	bootstrap_create_column(tr, t, "id", 2029, "int", 32);
	bootstrap_create_column(tr, t, "func_id", 2030, "int", 32);
	bootstrap_create_column(tr, t, "name", 2031, "varchar", 256);
	bootstrap_create_column(tr, t, "type", 2032, "varchar", 1024);
	bootstrap_create_column(tr, t, "type_digits", 2033, "int", 32);
	bootstrap_create_column(tr, t, "type_scale", 2034, "int", 32);
	bootstrap_create_column(tr, t, "inout", 2035, "tinyint", 8);
	bootstrap_create_column(tr, t, "number", 2036, "int", 32);

	t = bootstrap_create_table(tr, s, "sequences", 2037);
	bootstrap_create_column(tr, t, "id", 2038, "int", 32);
	bootstrap_create_column(tr, t, "schema_id", 2039, "int", 32);
	bootstrap_create_column(tr, t, "name", 2040, "varchar", 256);
	bootstrap_create_column(tr, t, "start", 2041, "bigint", 64);
	bootstrap_create_column(tr, t, "minvalue", 2042, "bigint", 64);
	bootstrap_create_column(tr, t, "maxvalue", 2043, "bigint", 64);
	bootstrap_create_column(tr, t, "increment", 2044, "bigint", 64);
	bootstrap_create_column(tr, t, "cacheinc", 2045, "bigint", 64);
	bootstrap_create_column(tr, t, "cycle", 2046, "boolean", 1);

	t = bootstrap_create_table(tr, s, "table_partitions", 2047);
	bootstrap_create_column(tr, t, "id", 2048, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2049, "int", 32);
	bootstrap_create_column(tr, t, "column_id", 2050, "int", 32);
	bootstrap_create_column(tr, t, "expression", 2051, "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "type", 2052, "tinyint", 8);

	t = bootstrap_create_table(tr, s, "range_partitions", 2053);
	bootstrap_create_column(tr, t, "table_id", 2054, "int", 32);
	bootstrap_create_column(tr, t, "partition_id", 2055, "int", 32);
	bootstrap_create_column(tr, t, "minimum", 2056, "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "maximum", 2057, "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "with_nulls", 2058, "boolean", 1);

	t = bootstrap_create_table(tr, s, "value_partitions", 2059);
	bootstrap_create_column(tr, t, "table_id", 2060, "int", 32);
	bootstrap_create_column(tr, t, "partition_id", 2061, "int", 32);
	bootstrap_create_column(tr, t, "value", 2062, "varchar", STORAGE_MAX_VALUE_LENGTH);

	t = bootstrap_create_table(tr, s, "dependencies", 2063);
	bootstrap_create_column(tr, t, "id", 2064, "int", 32);
	bootstrap_create_column(tr, t, "depend_id", 2065, "int", 32);
	bootstrap_create_column(tr, t, "depend_type", 2066, "smallint", 16);


	t = bootstrap_create_table(tr, s, "_tables", 2067);
	bootstrap_create_column(tr, t, "id", 2068, "int", 32);
	bootstrap_create_column(tr, t, "name", 2069, "varchar", 1024);
	bootstrap_create_column(tr, t, "schema_id", 2070, "int", 32);
	bootstrap_create_column(tr, t, "query", 2071, "varchar", 1 << 20);
	bootstrap_create_column(tr, t, "type", 2072, "smallint", 16);
	bootstrap_create_column(tr, t, "system", 2073, "boolean", 1);
	bootstrap_create_column(tr, t, "commit_action", 2074, "smallint", 16);
	bootstrap_create_column(tr, t, "access", 2075, "smallint", 16);

	t = bootstrap_create_table(tr, s, "_columns", 2076);
	bootstrap_create_column(tr, t, "id", 2077, "int", 32);
	bootstrap_create_column(tr, t, "name", 2078, "varchar", 1024);
	bootstrap_create_column(tr, t, "type", 2079, "varchar", 1024);
	bootstrap_create_column(tr, t, "type_digits", 2080, "int", 32);
	bootstrap_create_column(tr, t, "type_scale", 2081, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2082, "int", 32);
	bootstrap_create_column(tr, t, "default", 2083, "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "null", 2084, "boolean", 1);
	bootstrap_create_column(tr, t, "number", 2085, "int", 32);
	bootstrap_create_column(tr, t, "storage", 2086, "varchar", 2048);

	t = bootstrap_create_table(tr, s, "keys", 2087);
	bootstrap_create_column(tr, t, "id", 2088, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2089, "int", 32);
	bootstrap_create_column(tr, t, "type", 2090, "int", 32);
	bootstrap_create_column(tr, t, "name", 2091, "varchar", 1024);
	bootstrap_create_column(tr, t, "rkey", 2092, "int", 32);
	bootstrap_create_column(tr, t, "action", 2093, "int", 32);

	t = bootstrap_create_table(tr, s, "idxs", 2094);
	bootstrap_create_column(tr, t, "id", 2095, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2096, "int", 32);
	bootstrap_create_column(tr, t, "type", 2097, "int", 32);
	bootstrap_create_column(tr, t, "name", 2098, "varchar", 1024);

	t = bootstrap_create_table(tr, s, "triggers", 2099);
	bootstrap_create_column(tr, t, "id", 2100, "int", 32);
	bootstrap_create_column(tr, t, "name", 2101, "varchar", 1024);
	bootstrap_create_column(tr, t, "table_id", 2102, "int", 32);
	bootstrap_create_column(tr, t, "time", 2103, "smallint", 16);
	bootstrap_create_column(tr, t, "orientation", 2104, "smallint", 16);
	bootstrap_create_column(tr, t, "event", 2105, "smallint", 16);
	bootstrap_create_column(tr, t, "old_name", 2106, "varchar", 1024);
	bootstrap_create_column(tr, t, "new_name", 2107, "varchar", 1024);
	bootstrap_create_column(tr, t, "condition", 2108, "varchar", 2048);
	bootstrap_create_column(tr, t, "statement", 2109, "varchar", 2048);

	t = bootstrap_create_table(tr, s, "objects", 2110);
	bootstrap_create_column(tr, t, "id", 2111, "int", 32);
	bootstrap_create_column(tr, t, "name", 2112, "varchar", 1024);
	bootstrap_create_column(tr, t, "nr", 2113, "int", 32);
	bootstrap_create_column(tr, t, "sub", 2163, "int", 32);

	s = bootstrap_create_schema(tr, "tmp", 2114, ROLE_SYSADMIN, USER_MONETDB);
	store->tmp = s;

	t = bootstrap_create_table(tr, s, "_tables", 2115);
	bootstrap_create_column(tr, t, "id", 2116, "int", 32);
	bootstrap_create_column(tr, t, "name", 2117, "varchar", 1024);
	bootstrap_create_column(tr, t, "schema_id", 2118, "int", 32);
	bootstrap_create_column(tr, t, "query", 2119, "varchar", 1 << 20);
	bootstrap_create_column(tr, t, "type", 2120, "smallint", 16);
	bootstrap_create_column(tr, t, "system", 2121, "boolean", 1);
	bootstrap_create_column(tr, t, "commit_action", 2122, "smallint", 16);
	bootstrap_create_column(tr, t, "access", 2123, "smallint", 16);

	t = bootstrap_create_table(tr, s, "_columns", 2124);
	bootstrap_create_column(tr, t, "id", 2125, "int", 32);
	bootstrap_create_column(tr, t, "name", 2126, "varchar", 1024);
	bootstrap_create_column(tr, t, "type", 2127, "varchar", 1024);
	bootstrap_create_column(tr, t, "type_digits", 2128, "int", 32);
	bootstrap_create_column(tr, t, "type_scale", 2129, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2130, "int", 32);
	bootstrap_create_column(tr, t, "default", 2131, "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "null", 2132, "boolean", 1);
	bootstrap_create_column(tr, t, "number", 2133, "int", 32);
	bootstrap_create_column(tr, t, "storage", 2134, "varchar", 2048);

	t = bootstrap_create_table(tr, s, "keys", 2135);
	bootstrap_create_column(tr, t, "id", 2136, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2137, "int", 32);
	bootstrap_create_column(tr, t, "type", 2138, "int", 32);
	bootstrap_create_column(tr, t, "name", 2139, "varchar", 1024);
	bootstrap_create_column(tr, t, "rkey", 2140, "int", 32);
	bootstrap_create_column(tr, t, "action", 2141, "int", 32);

	t = bootstrap_create_table(tr, s, "idxs", 2142);
	bootstrap_create_column(tr, t, "id", 2143, "int", 32);
	bootstrap_create_column(tr, t, "table_id", 2144, "int", 32);
	bootstrap_create_column(tr, t, "type", 2145, "int", 32);
	bootstrap_create_column(tr, t, "name", 2146, "varchar", 1024);

	t = bootstrap_create_table(tr, s, "triggers", 2147);
	bootstrap_create_column(tr, t, "id", 2148, "int", 32);
	bootstrap_create_column(tr, t, "name", 2149, "varchar", 1024);
	bootstrap_create_column(tr, t, "table_id", 2150, "int", 32);
	bootstrap_create_column(tr, t, "time", 2151, "smallint", 16);
	bootstrap_create_column(tr, t, "orientation", 2152, "smallint", 16);
	bootstrap_create_column(tr, t, "event", 2153, "smallint", 16);
	bootstrap_create_column(tr, t, "old_name", 2154, "varchar", 1024);
	bootstrap_create_column(tr, t, "new_name", 2155, "varchar", 1024);
	bootstrap_create_column(tr, t, "condition", 2156, "varchar", 2048);
	bootstrap_create_column(tr, t, "statement", 2157, "varchar", 2048);

	t = bootstrap_create_table(tr, s, "objects", 2158);
	bootstrap_create_column(tr, t, "id", 2159, "int", 32);
	bootstrap_create_column(tr, t, "name", 2160, "varchar", 1024);
	bootstrap_create_column(tr, t, "nr", 2161, "int", 32);
	bootstrap_create_column(tr, t, "sub", 2164, "int", 32);

	(void) bootstrap_create_schema(tr, dt_schema, -1, ROLE_SYSADMIN, USER_MONETDB);

	if (store->first) {
		insert_types(tr, types);
		insert_functions(tr, functions, funcs, arguments);
		insert_schemas(tr);

	} else {
		tr->active = 0;
	}

	if (sql_trans_commit(tr) != SQL_OK)
		TRC_CRITICAL(SQL_STORE, "Cannot commit initial transaction\n");
	tr->ts = store_timestamp(store);

	store->logger_api.get_sequence(store, OBJ_SID, &lng_store_oid);
	store->prev_oid = (sqlid)lng_store_oid;
	if (store->obj_id < store->prev_oid)
		store->obj_id = store->prev_oid;

	/* load remaining schemas, tables, columns etc */
	tr->active = 1;
	if (!store->first && !load_trans(tr)) {
		return NULL;
	}
	if (sql_trans_commit(tr) != SQL_OK)
		TRC_CRITICAL(SQL_STORE, "Cannot commit loaded objects transaction\n");
	tr->active = 0;
	sql_trans_destroy(tr);
	store->initialized = 1;
	return store;
}

sqlstore *
store_init(sql_allocator *pa, int debug, store_type store_tpe, int readonly, int singleuser)
{
	sqlstore *store = ZNEW(sqlstore);

	if (!store)
		return NULL;

	*store = (sqlstore) {
		.readonly = readonly,
		.singleuser = singleuser,
		.debug = debug,
		.transaction = ATOMIC_VAR_INIT(TRANSACTION_ID_BASE),
	};

	(void)store_timestamp(store); /* increment once */
	MT_lock_init(&store->lock, "sqlstore_lock");
	MT_lock_init(&store->flush, "sqlstore_flush");
	for(int i = 0; i<NR_TABLE_LOCKS; i++)
		MT_lock_init(&store->table_locks[i], "sqlstore_table");

	MT_lock_set(&store->lock);
	MT_lock_set(&store->flush);

	/* initialize empty bats */
	switch (store_tpe) {
	case store_bat:
	case store_mem:
		if (bat_utils_init() == -1) {
			MT_lock_unset(&store->lock);
			MT_lock_unset(&store->flush);
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
		return NULL;
	}

	/* create the initial store structure or re-load previous data */
	MT_lock_unset(&store->lock);
	MT_lock_unset(&store->flush);
	return store_load(store, pa);
}

// All this must only be accessed while holding the store->flush.
// The exception is flush_now, which can be set by anyone at any
// time and therefore needs some special treatment.
static struct {
	// These two are inputs, set from outside the store_manager
	bool enabled;
	ATOMIC_TYPE flush_now;
	// These are state set from within the store_manager
	bool working;
	int countdown_ms;
	unsigned int cycle;
	char *reason_to;
	char *reason_not_to;
} flusher = {
	.flush_now = ATOMIC_VAR_INIT(0),
	.enabled = true,
};

static void
flusher_new_cycle(void)
{
	int cycle_time = GDKdebug & FORCEMITOMASK ? 500 : 50000;

	// do not touch .enabled and .flush_now, those are inputs
	flusher.working = false;
	flusher.countdown_ms = cycle_time;
	flusher.cycle += 1;
	flusher.reason_to = NULL;
	flusher.reason_not_to = NULL;
}

void
store_exit(sqlstore *store)
{
	sql_allocator *sa = store->sa;
	MT_lock_set(&store->lock);
	MT_lock_set(&store->flush);

	TRC_DEBUG(SQL_STORE, "Store locked\n");

	if (store->cat) {
		MT_lock_unset(&store->flush);
		MT_lock_unset(&store->lock);
		if (store->changes) {
			ulng oldest = store_timestamp(store)+1;
			for(node *n=store->changes->h; n; n = n->next) {
				sql_change *c = n->data;

				if (c->cleanup && !c->cleanup(store, c, oldest, oldest))
					assert(0);
				else
					_DELETE(c);
			}
			list_destroy(store->changes);
		}
		os_destroy(store->cat->objects, store);
		os_destroy(store->cat->schemas, store);
		_DELETE(store->cat);
		sequences_exit();
		MT_lock_set(&store->lock);
		MT_lock_set(&store->flush);
	}
	store->logger_api.destroy(store);

	list_destroy(store->active);

	TRC_DEBUG(SQL_STORE, "Store unlocked\n");
	MT_lock_unset(&store->flush);
	MT_lock_unset(&store->lock);
	sa_destroy(sa);
	_DELETE(store);
}

/* call locked! */
static int
store_apply_deltas(sqlstore *store)
{
	int res = LOG_OK;

	flusher.working = true;

	store_lock(store);
	ulng oldest = store_oldest(store);
	store_unlock(store);
	if (oldest)
	    res = store->logger_api.flush(store, oldest-1);
	flusher.working = false;
	return res;
}


/* Call while holding store->flush */
static void
wait_until_flusher_idle(sqlstore *store)
{
	while (flusher.working) {
		const int sleeptime = 100;
		MT_lock_unset(&store->lock);
		MT_sleep_ms(sleeptime);
		MT_lock_set(&store->lock);
	}
}
void
store_suspend_log(sqlstore *store)
{
	MT_lock_set(&store->lock);
	flusher.enabled = false;
	wait_until_flusher_idle(store);
	MT_lock_unset(&store->lock);
}

void
store_resume_log(sqlstore *store)
{
	MT_lock_set(&store->flush);
	flusher.enabled = true;
	MT_lock_unset(&store->flush);
}

void
store_manager(sqlstore *store)
{
	MT_thread_setworking("sleeping");

	// In the main loop we always hold the lock except when sleeping
	MT_lock_set(&store->flush);

	for (;;) {
		int res;

		if (store->logger_api.changes(store) <= 0) {
			if (GDKexiting())
				break;
			const int sleeptime = 100;
			MT_lock_unset(&store->flush);
			MT_sleep_ms(sleeptime);
			flusher.countdown_ms -= sleeptime;
			MT_lock_set(&store->flush);
			continue;
		}
		if (GDKexiting())
			break;

		MT_thread_setworking("flushing");
		res = store_apply_deltas(store);

		if (res != LOG_OK) {
			MT_lock_unset(&store->flush);
			GDKfatal("write-ahead logging failure, disk full?");
		}

		if (GDKexiting())
			break;
		flusher_new_cycle();
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

// Helper function for tar_write_header.
// Our stream.h makes sure __attribute__ exists.
static void __attribute__((__format__(__printf__, 3, 4)))
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
static gdk_return
tar_write_header(stream *tarfile, const char *path, time_t mtime, size_t size)
{
	char buf[TAR_BLOCK_SIZE] = {0};
	char *cursor = buf;
	char *chksum;

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
	tar_write_header_field(&cursor, 12, "%011zo", size);      // size[12]
	tar_write_header_field(&cursor, 12, "%011lo", (unsigned long)mtime); // mtime[12]
	chksum = cursor; // use this later to set the computed checksum
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

	unsigned sum = 0;
	for (int i = 0; i < TAR_BLOCK_SIZE; i++)
		sum += (unsigned char) buf[i];

	tar_write_header_field(&chksum, 8, "%06o", sum);

	if (mnstr_write(tarfile, buf, TAR_BLOCK_SIZE, 1) != 1) {
		char *err = mnstr_error(tarfile);
		GDKerror("error writing tar header %s: %s", path, err);
		free(err);
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

/* Write data to the stream, padding it with zeroes up to the next
 * multiple of TAR_BLOCK_SIZE.  Make sure all writes are in multiples
 * of TAR_BLOCK_SIZE.
 */
static gdk_return
tar_write(stream *outfile, const char *data, size_t size)
{
	const size_t tail = size % TAR_BLOCK_SIZE;
	const size_t bulk = size - tail;

	if (bulk) {
		size_t written = mnstr_write(outfile, data, 1, bulk);
		if (written != bulk) {
			GDKerror("Wrote only %zu bytes instead of first %zu", written, bulk);
			return GDK_FAIL;
		}
	}

	if (tail) {
		char buf[TAR_BLOCK_SIZE] = {0};
		memcpy(buf, data + bulk, tail);
		size_t written = mnstr_write(outfile, buf, 1, TAR_BLOCK_SIZE);
		if (written != TAR_BLOCK_SIZE) {
			GDKerror("Wrote only %zu tail bytes instead of %d", written, TAR_BLOCK_SIZE);
			return GDK_FAIL;
		}
	}

	return GDK_SUCCEED;
}

static gdk_return
tar_write_data(stream *tarfile, const char *path, time_t mtime, const char *data, size_t size)
{
	gdk_return res;

	res = tar_write_header(tarfile, path, mtime, size);
	if (res != GDK_SUCCEED)
		return res;

	return tar_write(tarfile, data, size);
}

static gdk_return
tar_copy_stream(stream *tarfile, const char *path, time_t mtime, stream *contents, ssize_t size)
{
	const ssize_t bufsize = 64 * 1024;
	gdk_return ret = GDK_FAIL;
	ssize_t file_size;
	char *buf = NULL;
	ssize_t to_read;

	file_size = getFileSize(contents);
	if (file_size < size) {
		GDKerror("Have to copy %zd bytes but only %zd exist in %s", size, file_size, path);
		goto end;
	}

	assert( (bufsize % TAR_BLOCK_SIZE) == 0);
	assert(bufsize >= TAR_BLOCK_SIZE);

	buf = GDKmalloc(bufsize);
	if (!buf) {
		GDKerror("could not allocate buffer");
		goto end;
	}

	if (tar_write_header(tarfile, path, mtime, size) != GDK_SUCCEED)
		goto end;

	to_read = size;

	while (to_read > 0) {
		ssize_t chunk = (to_read <= bufsize) ? to_read : bufsize;
		ssize_t nbytes = mnstr_read(contents, buf, 1, chunk);
		if (nbytes != chunk) {
			char *err = mnstr_error(contents);
			GDKerror("Read only %zd/%zd bytes of component %s: %s", nbytes, chunk, path, err);
			free(err);
			goto end;
		}
		ret = tar_write(tarfile, buf, chunk);
		if (ret != GDK_SUCCEED)
			goto end;
		to_read -= chunk;
	}

	ret = GDK_SUCCEED;
end:
	if (buf)
		GDKfree(buf);
	return ret;
}

static gdk_return
hot_snapshot_write_tar(stream *out, const char *prefix, char *plan)
{
	gdk_return ret = GDK_FAIL;
	const char *p = plan; // our cursor in the plan
	time_t timestamp = 0;
	// Name convention: _path for the absolute path
	// and _name for the corresponding local relative path
	char abs_src_path[2 * FILENAME_MAX];
	char *src_name = abs_src_path;
	char dest_path[100]; // size imposed by tar format.
	char *dest_name = dest_path + snprintf(dest_path, sizeof(dest_path), "%s/", prefix);
	stream *infile = NULL;

	int len;
	if (sscanf(p, "%[^\n]\n%n", abs_src_path, &len) != 1) {
		GDKerror("internal error: first line of plan is malformed");
		goto end;
	}
	p += len;
	src_name = abs_src_path + len - 1; // - 1 because len includes the trailing newline
	*src_name++ = DIR_SEP;

	char command;
	long size;
	while (sscanf(p, "%c %ld %100s\n%n", &command, &size, src_name, &len) == 3) {
		p += len;
		strcpy(dest_name, src_name);
		if (size < 0) {
			GDKerror("malformed snapshot plan for %s: size %ld < 0", src_name, size);
			goto end;
		}
		switch (command) {
			case 'c':
				infile = open_rstream(abs_src_path);
				if (!infile) {
					GDKerror("%s", mnstr_peek_error(NULL));
					goto end;
				}
				if (tar_copy_stream(out, dest_path, timestamp, infile, size) != GDK_SUCCEED)
					goto end;
				close_stream(infile);
				infile = NULL;
				break;
			case 'w':
				if (tar_write_data(out, dest_path, timestamp, p, size) != GDK_SUCCEED)
					goto end;
				p += size;
				break;
			default:
				GDKerror("Unknown command in snapshot plan: %c (%s)", command, src_name);
				goto end;
		}
		mnstr_flush(out, MNSTR_FLUSH_ALL);
	}

	// write a trailing block of zeros. If it succeeds, this function succeeds.
	char a;
	a = '\0';
	ret = tar_write(out, &a, 1);
	if (ret == GDK_SUCCEED)
		ret = tar_write(out, &a, 1);

end:
	free(plan);
	if (infile)
		close_stream(infile);
	return ret;
}

/* Pick a name for the temporary tar file. Make sure it has the same extension
 * so as not to confuse the streams library.
 *
 * This function is not entirely safe as compared to for example mkstemp.
 */
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
		char *tmp = "..tmp.";
		size_t tmplen = strlen(tmp);
		memmove(ext + tmplen, ext, strlen(ext) + 1);
		memmove(ext, tmp, tmplen);
	}

	return name;
}

lng
store_hot_snapshot_to_stream(sqlstore *store, stream *tar_stream)
{
	int locked = 0;
	lng result = 0;
	buffer *plan_buf = NULL;
	stream *plan_stream = NULL;
	gdk_return r;

	if (!store->logger_api.get_snapshot_files) {
		GDKerror("backend does not support hot snapshots");
		goto end;
	}

	plan_buf = buffer_create(64 * 1024);
	if (!plan_buf) {
		GDKerror("Failed to allocate plan buffer");
		goto end;
	}
	plan_stream = buffer_wastream(plan_buf, "write_snapshot_plan");
	if (!plan_stream) {
		GDKerror("Failed to allocate buffer stream");
		goto end;
	}

	MT_lock_set(&store->flush);
	MT_lock_set(&store->lock);
	locked = 1;
	wait_until_flusher_idle(store);
	if (GDKexiting())
		goto end;

	r = store->logger_api.get_snapshot_files(store, plan_stream);
	if (r != GDK_SUCCEED)
		goto end; // should already have set a GDK error
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
		MT_lock_unset(&store->lock);
		MT_lock_unset(&store->flush);
	}
	if (plan_stream)
		close_stream(plan_stream);
	if (plan_buf)
		buffer_destroy(plan_buf);
	return result;
}


lng
store_hot_snapshot(sqlstore *store, str tarfile)
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

	result = store_hot_snapshot_to_stream(store, tar_stream);
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

static sql_column *
column_dup(sql_trans *tr, sql_column *oc, sql_table *t)
{
	sqlstore *store = tr->store;
	sql_allocator *sa = tr->sa;
	sql_column *c = SA_ZNEW(sa, sql_column);

	base_init(sa, &c->base, oc->base.id, 0, oc->base.name);
	c->type = oc->type;
	c->def = NULL;
	if (oc->def)
		c->def = SA_STRDUP(sa, oc->def);
	c->null = oc->null;
	c->colnr = oc->colnr;
	c->unique = oc->unique;
	c->t = t;
	c->storage_type = NULL;
	if (oc->storage_type)
		c->storage_type = SA_STRDUP(sa, oc->storage_type);

	if (isTable(c->t)) {
		if (isTempTable(c->t)) {
			if (store->storage_api.create_col(tr, c) != LOG_OK)
				return NULL;
		} else {
			ATOMIC_PTR_SET(&c->data, store->storage_api.col_dup(oc));
		}
	}
	return c;
}

static sql_kc *
kc_dup(sql_trans *tr, sql_kc *kc, sql_table *t)
{
	sql_allocator *sa = tr->sa;
	sql_kc *nkc = SA_ZNEW(sa, sql_kc);
	sql_column *c = find_sql_column(t, kc->c->base.name);

	assert(c);
	nkc->c = c;
	c->unique = kc->c->unique;
	return nkc;
}

static sql_key *
key_dup(sql_trans *tr, sql_key *k, sql_table *t)
{
	sql_allocator *sa = tr->sa;
	sql_key *nk = (k->type != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey)
	    : (sql_key *) SA_ZNEW(sa, sql_fkey);
	node *n;

	base_init(sa, &nk->base, k->base.id?k->base.id:next_oid(tr->store), 0, k->base.name);
	nk->type = k->type;
	nk->columns = list_new(sa, (fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = NULL;

	if (k->idx) {
		sql_base *b = os_find_name(nk->t->s->idxs, tr, nk->base.name);

		if (b) {
			nk->idx = (sql_idx *)b;
			nk->idx->key = nk;
		}
	}

	if (nk->type != fkey) {
		sql_ukey *tk = (sql_ukey *) nk;

		if (nk->type == pkey)
			t->pkey = tk;
	} else {
		sql_fkey *fk = (sql_fkey *) nk;
		sql_fkey *ok = (sql_fkey *) k;

		fk->rkey = ok->rkey;
		fk->on_delete = ok->on_delete;
		fk->on_update = ok->on_update;
	}

	for (n = k->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nk->columns, kc_dup(tr, okc, t));
	}

	if (isGlobal(t) &&
			(os_add(t->s->keys, tr, nk->base.name, dup_base(&nk->base)) ||
			 os_add(tr->cat->objects, tr, nk->base.name, dup_base(&nk->base)))) {
		key_destroy(tr->store, nk);
		return NULL;
	}
	return nk;
}


static sql_idx *
idx_dup(sql_trans *tr, sql_idx * i, sql_table *t)
{
	sqlstore *store = tr->store;
	sql_allocator *sa = tr->sa;
	sql_idx *ni = SA_ZNEW(sa, sql_idx);
	node *n;

	base_init(sa, &ni->base, i->base.id, 0, i->base.name);

	ni->columns = list_new(sa, (fdestroy) &kc_destroy);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;

	if (isTable(i->t)) {
		if (isTempTable(i->t)) {
			if (store->storage_api.create_idx(tr, ni) != LOG_OK)
				return NULL;
		} else {
			ATOMIC_PTR_SET(&ni->data, store->storage_api.idx_dup(i));
		}
	}

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(ni->columns, kc_dup(tr, okc, t));
	}
	if (isGlobal(t) && os_add(t->s->idxs, tr, ni->base.name, dup_base(&ni->base))) {
		idx_destroy(store, ni);
		return NULL;
	}
	return ni;
}

static sql_part *
part_dup(sql_trans *tr, sql_part *op, sql_table *mt)
{
	sql_allocator *sa = tr->sa;
	sql_part *p = SA_ZNEW(sa, sql_part);

	assert(isMergeTable(mt) || isReplicaTable(mt));
	base_init(sa, &p->base, op->base.id, 0, op->base.name);
	p->with_nills = op->with_nills;
	p->t = mt;
	p->member = op->member;

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
	if (os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))) {
		part_destroy(tr->store, p);
		return NULL;
	}
	return p;
}

static sql_trigger *
trigger_dup(sql_trans *tr, sql_trigger * i, sql_table *t)
{
	sql_allocator *sa = tr->sa;
	sql_trigger *nt = SA_ZNEW(sa, sql_trigger);

	base_init(sa, &nt->base, i->base.id, 0, i->base.name);

	nt->columns = list_new(sa, (fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = i->time;
	nt->orientation = i->orientation;
	nt->event = i->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (i->old_name)
		nt->old_name = SA_STRDUP(sa, i->old_name);
	if (i->new_name)
		nt->new_name = SA_STRDUP(sa, i->new_name);
	if (i->condition)
		nt->condition = SA_STRDUP(sa, i->condition);
	nt->statement = SA_STRDUP(sa, i->statement);

	for (node *n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nt->columns, kc_dup(tr, okc, t));
	}
	if (isGlobal(t) && os_add(t->s->triggers, tr, nt->base.name, dup_base(&nt->base))) {
		trigger_destroy(tr->store, nt);
		return NULL;
	}
	return nt;
}

static sql_table *
table_dup(sql_trans *tr, sql_table *ot, sql_schema *s, const char *name)
{
	sqlstore *store = tr->store;
	sql_allocator *sa = tr->sa;
	sql_table *t = SA_ZNEW(sa, sql_table);
	node *n;

	base_init(sa, &t->base, ot->base.id, 0, name?name:ot->base.name);
	t->type = ot->type;
	t->system = ot->system;
	t->bootstrap = ot->bootstrap;
	t->persistence = ot->persistence;
	t->commit_action = ot->commit_action;
	t->access = ot->access;
	t->query = (ot->query) ? SA_STRDUP(sa, ot->query) : NULL;
	t->properties = ot->properties;

	t->columns = ol_new(sa, (destroy_fptr) &column_destroy);
	t->idxs = ol_new(sa, (destroy_fptr) &idx_destroy);
	t->keys = ol_new(sa, (destroy_fptr) &key_destroy);
	t->triggers = ol_new(sa, (destroy_fptr) &trigger_destroy);
	if (ot->members)
		t->members = list_new(sa, (fdestroy) &part_destroy);

	t->pkey = NULL;
	t->s = s;
	t->sz = ot->sz;

	if (isGlobal(t) && os_add(t->s->tables, tr, t->base.name, &t->base)) {
		table_destroy(store, t);
		return NULL;
	}

	if (isPartitionedByExpressionTable(ot)) {
		t->part.pexp = SA_ZNEW(sa, sql_expression);
		t->part.pexp->exp = SA_STRDUP(sa, ot->part.pexp->exp);
		t->part.pexp->type = ot->part.pexp->type;
		t->part.pexp->cols = SA_LIST(sa, (fdestroy) &int_destroy);
		for (n = ot->part.pexp->cols->h; n; n = n->next) {
			int *nid = SA_NEW(sa, int);
			*nid = *(int *) n->data;
			list_append(t->part.pexp->cols, nid);
		}
	}
	if (ot->columns)
		for (n = ol_first_node(ot->columns); n; n = n->next) {
			sql_column *c = n->data, *col = column_dup(tr, c, t);
			if (isPartitionedByColumnTable(ot) && ot->part.pcol->base.id == c->base.id)
				t->part.pcol = col;
			ol_add(t->columns, &col->base);
		}
	if (ot->idxs)
		for (n = ol_first_node(ot->idxs); n; n = n->next) {
			sql_idx *i = idx_dup(tr, n->data, t);
			ol_add(t->idxs, &i->base);
		}
	if (ot->keys)
		for (n = ol_first_node(ot->keys); n; n = n->next) {
			sql_key *k = key_dup(tr, n->data, t);
			ol_add(t->keys, &k->base);
		}
	if (ot->triggers)
		for (n = ol_first_node(ot->triggers); n; n = n->next) {
			sql_trigger *k = trigger_dup(tr, n->data, t);
			ol_add(t->triggers, &k->base);
		}
	if (ot->members)
		for (n = ot->members->h; n; n = n->next)
			list_append(t->members, part_dup(tr, n->data, t));
	if (isTable(t)) {
		if (isTempTable(t)) {
			if (store->storage_api.create_del(tr, t) != LOG_OK) {
				table_destroy(store, t);
				return NULL;
			}
		} else {
			ATOMIC_PTR_SET(&t->data, store->storage_api.del_dup(ot));
		}
	}
	return t;
}

static sql_table*
new_table( sql_trans *tr, sql_table *t)
{
//	t = find_sql_table_id(tr, t->s, t->base.id); /* could have changed by depending changes */
	t = find_sql_table(tr, t->s, t->base.name); /* could have changed by depending changes */
	if (!isLocalTemp(t) && !os_obj_intransaction(t->s->tables, tr, &t->base))
		t = table_dup(tr, t, t->s, NULL);
	return t;
}

	sql_key *
sql_trans_copy_key( sql_trans *tr, sql_table *t, sql_key *k)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	sql_table *syskc = find_sql_table(tr, syss, "objects");
	int neg = -1, action = -1, nr;
	node *n;

	t = new_table(tr, t);
	if (!t)
		return NULL;
	sql_key *nk = key_dup(tr, k, t);
	sql_fkey *fk = (sql_fkey*)nk;
	ol_add(t->keys, &nk->base);

	if (nk->type == fkey)
		action = (fk->on_update<<8) + fk->on_delete;

	if (store->table_api.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, &nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey : &neg, &action))
		return NULL;

	if (nk->type == fkey)
		sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey, nk->base.id, FKEY_DEPENDENCY);

	for (n = nk->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *kc = n->data;

		if (store->table_api.table_insert(tr, syskc, &nk->base.id, &kc->c->base.name, &nr, ATOMnilptr(TYPE_int)))
			return NULL;

		if (nk->type == fkey)
			sql_trans_create_dependency(tr, kc->c->base.id, nk->base.id, FKEY_DEPENDENCY);
		else if (nk->type == ukey)
			sql_trans_create_dependency(tr, kc->c->base.id, nk->base.id, KEY_DEPENDENCY);
		else if (nk->type == pkey) {
			sql_trans_create_dependency(tr, kc->c->base.id, nk->base.id, KEY_DEPENDENCY);
			sql_trans_alter_null(tr, kc->c, 0);
		}
	}
	return nk;
}

sql_idx *
sql_trans_copy_idx( sql_trans *tr, sql_table *t, sql_idx *i)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(tr, syss, "idxs");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	node *n;
	int nr, unique = 0;
	sql_idx *ni = SA_ZNEW(tr->sa, sql_idx);

	t = new_table(tr, t);
	if (!t)
		return NULL;
	base_init(tr->sa, &ni->base, i->base.id?i->base.id:next_oid(tr->store), TR_NEW, i->base.name);

	ni->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;

	if (i->type == hash_idx && list_length(i->columns) == 1)
		unique = 1;
	for (n = i->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(ni->columns, ic = kc_dup(tr, okc, t));
		if (ic->c->unique != (unique & !okc->c->null))
			okc->c->unique = ic->c->unique = (unique & (!okc->c->null));

		if (store->table_api.table_insert(tr, sysic, &ni->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int)))
			return NULL;

		sql_trans_create_dependency(tr, ic->c->base.id, ni->base.id, INDEX_DEPENDENCY);
	}
	ol_add(t->idxs, &ni->base);
	if (os_add(t->s->idxs, tr, ni->base.name, dup_base(&ni->base))) {
		idx_destroy(store, ni);
		return NULL;
	}

	if (isDeclaredTable(i->t))
		if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
			if (store->storage_api.create_idx(tr, ni) != LOG_OK)
				return NULL;
	if (!isDeclaredTable(t))
		if (store->table_api.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, &ni->base.name))
			return NULL;
	return ni;
}

sql_trigger *
sql_trans_copy_trigger( sql_trans *tr, sql_table *t, sql_trigger *tri)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systr = find_sql_table(tr, syss, "triggers");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	node *n;
	int nr;
	sql_trigger *nt = SA_ZNEW(tr->sa, sql_trigger);
	char *strnil = (char*)ATOMnilptr(TYPE_str);

	base_init(tr->sa, &nt->base, tri->base.id?tri->base.id:next_oid(tr->store), TR_NEW, tri->base.name);

	nt->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = tri->time;
	nt->orientation = tri->orientation;
	nt->event = tri->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (tri->old_name)
		nt->old_name = SA_STRDUP(tr->sa, tri->old_name);
	if (tri->new_name)
		nt->new_name = SA_STRDUP(tr->sa, tri->new_name);
	if (tri->condition)
		nt->condition = SA_STRDUP(tr->sa, tri->condition);
	nt->statement = SA_STRDUP(tr->sa, tri->statement);

	for (n = tri->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(nt->columns, ic = kc_dup(tr, okc, t));
		if (store->table_api.table_insert(tr, sysic, &nt->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int)))
			return NULL;
		sql_trans_create_dependency(tr, ic->c->base.id, nt->base.id, TRIGGER_DEPENDENCY);
	}
	ol_add(t->triggers, &nt->base);
	if (os_add(t->s->triggers, tr, nt->base.name, dup_base(&nt->base))) {
		trigger_destroy(store, nt);
		return NULL;
	}

	if (!isDeclaredTable(t))
		if (store->table_api.table_insert(tr, systr, &nt->base.id, &nt->base.name, &t->base.id, &nt->time, &nt->orientation,
				&nt->event, (nt->old_name)?&nt->old_name:&strnil, (nt->new_name)?&nt->new_name:&strnil,
				(nt->condition)?&nt->condition:&strnil, &nt->statement))
			return NULL;
	return nt;
}

static int
sql_trans_cname_conflict(sql_table *t, const char *extra, const char *cname)
{
	int res = 0;
	const char *tmp = cname;

	if (extra) {
		tmp = sql_message("%s_%s", extra, cname);
	} else {
		tmp = cname;
	}
	if (find_sql_column(t, tmp))
		res = 1;
	if (tmp != cname) {
		char *ntmp = (char*)tmp;
		_DELETE(ntmp);
	}
	return res;
}

static int
sql_trans_tname_conflict( sql_trans *tr, sql_schema *s, const char *extra, const char *tname, const char *cname)
{
	char *tp;
	char *tmp;
	sql_table *t = NULL;

	if (extra) {
		tmp = sql_message("%s_%s", extra, tname);
	} else {
		tmp = _STRDUP(tname);
	}
	tp = tmp;
	while ((tp = strchr(tp, '_')) != NULL) {
		*tp = 0;
		t = find_sql_table(tr, s, tmp);
		if (t && sql_trans_cname_conflict(t, tp+1, cname)) {
			_DELETE(tmp);
			return 1;
		}
		*tp++ = '_';
	}
	_DELETE(tmp);
	tmp = _STRDUP(cname);
	tp = tmp;
	while ((tp = strchr(tp, '_')) != NULL) {
		char *ntmp;
		*tp = 0;
		ntmp = sql_message("%s_%s", tname, tmp);
		t = find_sql_table(tr, s, ntmp);
		if (t && sql_trans_cname_conflict(t, NULL, tp+1)) {
			_DELETE(ntmp);
			_DELETE(tmp);
			return 1;
		}
		_DELETE(ntmp);
		*tp++ = '_';
	}
	_DELETE(tmp);
	t = find_sql_table(tr, s, tname);
	if (t && sql_trans_cname_conflict(t, NULL, cname))
		return 1;
	return 0;
}

static int
sql_trans_name_conflict( sql_trans *tr, const char *sname, const char *tname, const char *cname)
{
	char *sp;
	sql_schema *s = NULL;

	sp = strchr(sname, '_');
	if (!sp && strchr(tname, '_') == 0 && strchr(cname, '_') == 0)
		return 0;

	if (sp) {
		char *tmp = SA_STRDUP(tr->sa, sname);
		sp = tmp;
		while ((sp = strchr(sp, '_')) != NULL) {
			*sp = 0;
			s = find_sql_schema(tr, tmp);
			if (s && sql_trans_tname_conflict(tr, s, sp+1, tname, cname))
				return 1;
			*sp++ = '_';
		}
	} else {
		s = find_sql_schema(tr, sname);
		if (s)
			return sql_trans_tname_conflict(tr, s, NULL, tname, cname);
	}
	return 0;
}

sql_column *
sql_trans_copy_column( sql_trans *tr, sql_table *t, sql_column *c)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");

	if (t->system && sql_trans_name_conflict(tr, t->s->base.name, t->base.name, c->base.name))
		return NULL;
	t = new_table(tr, t);
	if (!t)
		return NULL;
	sql_column *col = SA_ZNEW(tr->sa, sql_column);
	base_init(tr->sa, &col->base, c->base.id?c->base.id:next_oid(tr->store), TR_NEW, c->base.name);
	dup_sql_type(tr, t->s, &(c->type), &(col->type));
	col->def = NULL;
	if (c->def)
		col->def = SA_STRDUP(tr->sa, c->def);
	col->null = c->null;
	col->colnr = c->colnr;
	col->unique = c->unique;
	col->t = t;
	col->storage_type = NULL;
	if (c->storage_type)
		col->storage_type = SA_STRDUP(tr->sa, c->storage_type);

	ol_add(t->columns, &col->base);

	if (isDeclaredTable(c->t))
		if (isTable(t))
			if (store->storage_api.create_col(tr, col) != LOG_OK)
				return NULL;
	if (!isDeclaredTable(t)) {
		char *strnil = (char*)ATOMnilptr(TYPE_str);
		if (store->table_api.table_insert(tr, syscolumn, &col->base.id, &col->base.name, &col->type.type->sqlname,
					&col->type.digits, &col->type.scale, &t->base.id,
					(col->def) ? &col->def : &strnil, &col->null, &col->colnr,
					(col->storage_type) ? &col->storage_type : &strnil) != LOG_OK)
			return NULL;
		if (c->type.type->s) /* column depends on type */
			sql_trans_create_dependency(tr, c->type.type->base.id, col->base.id, TYPE_DEPENDENCY);
	}
	return col;
}

static void
sql_trans_rollback(sql_trans *tr)
{
	sqlstore *store = tr->store;
	ulng commit_ts = 0; /* invalid ts, ie rollback */

	/* move back deleted */
	if (tr->localtmps.dset) {
		for(node *n=tr->localtmps.dset->h; n; ) {
			node *next = n->next;
			sql_table *tt = n->data;
			if (!isNew(tt))
				list_prepend(tr->localtmps.set, dup_base(&tt->base));
			n = next;
		}
	}
	if (tr->changes) {
		/* revert the change list */
		list *nl = SA_LIST(tr->sa, (fdestroy) NULL);
		for(node *n=tr->changes->h; n; n = n->next)
			list_prepend(nl, n->data);

		/* rollback */
		ulng oldest = store_oldest(store);
		for(node *n=nl->h; n; n = n->next) {
			sql_change *c = n->data;

			if (c->commit)
				c->commit(tr, c, commit_ts, oldest);
		}
		if (!list_empty(store->changes)) { /* lets first cleanup old stuff */
			for(node *n=store->changes->h; n; ) {
				node *next = n->next;
				sql_change *c = n->data;

				if (!c->cleanup) {
					_DELETE(c);
				} else if (c->cleanup && c->cleanup(store, c, commit_ts, oldest)) {
					list_remove_node(store->changes, store, n);
					_DELETE(c);
				}
				n = next;
			}
		}
		for(node *n=nl->h; n; n = n->next) {
			sql_change *c = n->data;

			if (!c->cleanup) {
				_DELETE(c);
			} else if (c->cleanup && !c->cleanup(store, c, commit_ts, oldest)) {
				store->changes = sa_list_append(tr->sa, store->changes, c);
			} else
				_DELETE(c);
		}
		list_destroy(nl);
		list_destroy(tr->changes);
		tr->changes = NULL;
		tr->logchanges = 0;
	}
	if (tr->localtmps.dset) {
		list_destroy2(tr->localtmps.dset, tr->store);
		tr->localtmps.dset = NULL;
	}
	if (cs_size(&tr->localtmps)) {
		/* cleanup new */
		if (tr->localtmps.nelm) {
			for(node *n=tr->localtmps.nelm; n; ) {
				node *next = n->next;
				//sql_table *tt = n->data;
				list_remove_node(tr->localtmps.set, store, n);
				//(void) sql_trans_drop_table_id(tr, tt->s, tt->base.id, DROP_RESTRICT);
				n = next;
			}
			tr->localtmps.nelm = NULL;
		}
		/* handle content */
		for(node *n=tr->localtmps.set->h; n; ) {
			node *next = n->next;
			sql_table *tt = n->data;

			if (tt->commit_action == CA_DROP) {
				(void) sql_trans_drop_table_id(tr, tt->s, tt->base.id, DROP_RESTRICT);
				/*
				   } else if (tt->commit_action != CA_PRESERVE || tt->commit_action == CA_DELETE) {
				   sql_trans_clear_table(tr, tt);
				   */
			}
			n = next;
		}
	}
}

sql_trans *
sql_trans_destroy(sql_trans *tr)
{
	sql_trans *res = tr->parent;

	TRC_DEBUG(SQL_STORE, "Destroy transaction: %p\n", tr);
	if (tr->name) {
		_DELETE(tr->name);
		tr->name = NULL;
	}
	if (tr->changes)
		sql_trans_rollback(tr);
	cs_destroy(&tr->localtmps, tr->store);
	_DELETE(tr);
	return res;
}

static sql_trans *
sql_trans_create_(sqlstore *store, sql_trans *parent, const char *name)
{
	sql_trans *tr = ZNEW(sql_trans);

	if (!tr)
		return NULL;

	//tr->sa = store->sa;
	tr->sa = NULL;
	tr->store = store;
	tr->tid = store_transaction_id(store);

	if (name) {
		if (!parent)
			return NULL;
		parent->name = SA_STRDUP(parent->sa, name);
	}
	tr->cat = store->cat;
	if (!tr->cat) {
		store->cat = tr->cat = SA_ZNEW(tr->sa, sql_catalog);
		store->cat->schemas = os_new(tr->sa, (destroy_fptr) &schema_destroy, false, true);
		store->cat->objects = os_new(tr->sa, (destroy_fptr) &key_destroy, false, false);
	}
	tr->tmp = store->tmp;
	cs_new(&tr->localtmps, tr->sa, (fdestroy) &table_destroy);
	tr->parent = parent;
	TRC_DEBUG(SQL_STORE, "New transaction: %p\n", tr);
	return tr;
}

static sql_schema *
schema_dup(sql_trans *tr, sql_schema *s, const char *name)
{
	sql_schema *ns = SA_ZNEW(tr->sa, sql_schema);

	base_init(tr->sa, &ns->base, s->base.id, 0, name);
	ns->auth_id = s->auth_id;
	ns->owner = s->owner;
	ns->system = s->system;

	ns->tables = os_new(tr->sa, (destroy_fptr) &table_destroy, isTempSchema(s), true);
	ns->seqs = os_new(tr->sa, (destroy_fptr) &seq_destroy, isTempSchema(s), true);
	ns->keys = os_new(tr->sa, (destroy_fptr) &key_destroy, isTempSchema(s), true);
	ns->idxs = os_new(tr->sa, (destroy_fptr) &idx_destroy, isTempSchema(s), true);
	ns->triggers = os_new(tr->sa, (destroy_fptr) &trigger_destroy, isTempSchema(s), true);
	ns->parts = os_new(tr->sa, (destroy_fptr) &part_destroy, isTempSchema(s), false);

	/* table_dup will dup keys, idxs, triggers and parts */
	struct os_iter oi;
	os_iterator(&oi, s->tables, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_table *t = table_dup(tr, (sql_table*)b, s, NULL);
		if (os_add(ns->tables, tr, t->base.name, &t->base)) {
			table_destroy(tr->store, t);
			schema_destroy(tr->store, ns);
			return NULL;
		}
	}

	/* we can share the funcs and types */
	ns->funcs = os_dup(s->funcs);
	ns->types = os_dup(s->types);
	ns->store = s->store;
	ns->internal = NULL;
	return ns;
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

int
sql_trans_commit(sql_trans *tr)
{
	int ok = LOG_OK;
	sqlstore *store = tr->store;
	ulng commit_ts = tr->parent ? tr->parent->tid : store_timestamp(store);
	ulng oldest = store_oldest_given_max(store, commit_ts);

	/* write phase */
	TRC_DEBUG(SQL_STORE, "Forwarding changes (" ULLFMT ", " ULLFMT ") -> " ULLFMT "\n", tr->tid, tr->ts, commit_ts);
	if (!list_empty(store->changes)) { /* lets first cleanup old stuff */
		for(node *n=store->changes->h; n; ) {
			node *next = n->next;
			sql_change *c = n->data;

			if (!c->cleanup) {
				_DELETE(c);
			} else if (c->cleanup && c->cleanup(store, c, commit_ts, oldest)) {
				list_remove_node(store->changes, store, n);
				_DELETE(c);
			}
			n = next;
		}
	}
	if (tr->changes) {
		int min_changes = GDKdebug & FORCEMITOMASK ? 5 : 100000;
		int flush = (tr->logchanges > min_changes && !store->changes);
		/* log changes should only be done if there is something to log */
		if (tr->logchanges > 0) {
			ok = store->logger_api.log_tstart(store, commit_ts, flush);
			/* log */
			for(node *n=tr->changes->h; n && ok == LOG_OK; n = n->next) {
				sql_change *c = n->data;

				if (c->log && ok == LOG_OK)
					ok = c->log(tr, c);
			}
			//saved_id = store->logger_api.log_save_id(store);
			if (ok == LOG_OK && store->prev_oid != store->obj_id)
				ok = store->logger_api.log_sequence(store, OBJ_SID, store->obj_id);
			store->prev_oid = store->obj_id;
			if (ok == LOG_OK && !flush)
				ok = store->logger_api.log_tend(store);
		}
		tr->logchanges = 0;
		/* apply committed changes */
		for(node *n=tr->changes->h; n && ok == LOG_OK; n = n->next) {
			sql_change *c = n->data;

			if (c->commit && ok == LOG_OK)
				ok = c->commit(tr, c, commit_ts, oldest);
			else
				c->obj->flags = 0;
		}
		/* flush logger after changes got applied */
		if (ok == LOG_OK && flush)
			ok = store->logger_api.log_tend(store);
		/* garbage collect */
		for(node *n=tr->changes->h; n && ok == LOG_OK; ) {
			node *next = n->next;
			sql_change *c = n->data;

			if (!c->cleanup || c->cleanup(store, c, commit_ts, oldest)) {
				_DELETE(c);
			} else if (tr->parent) { /* need to keep everything */
				tr->parent->changes = sa_list_append(tr->sa, tr->parent->changes, c);
			} else {
				store->changes = sa_list_append(tr->sa, store->changes, c);
			}
			n = next;
		}
		list_destroy(tr->changes);
		tr->changes = NULL;
	}
	/* drop local temp tables with commit action CA_DROP, after cleanup */
	if (cs_size(&tr->localtmps)) {
		for(node *n=tr->localtmps.set->h; n; ) {
			node *next = n->next;
			sql_table *tt = n->data;

			if (tt->commit_action == CA_DROP)
				(void) sql_trans_drop_table_id(tr, tt->s, tt->base.id, DROP_RESTRICT);
			n = next;
		}
	}
	if (tr->localtmps.dset) {
		list_destroy2(tr->localtmps.dset, store);
		tr->localtmps.dset = NULL;
	}
	tr->localtmps.nelm = NULL;
	tr->ts = commit_ts;
	return (ok==LOG_OK)?SQL_OK:SQL_ERR;
}

static int
sql_trans_drop_all_dependencies(sql_trans *tr, sqlid id, sql_dependency type)
{
	sqlid dep_id=0, t_id = -1;
	sht dep_type = 0;
	list *dep = sql_trans_get_dependencies(tr, id, type, NULL);
	node *n;

	if (!dep)
		return DEPENDENCY_CHECK_ERROR;

	n = dep->h;

	while (n) {
		dep_id = *(sqlid*) n->data;
		dep_type = (sql_dependency) *(sht*) n->next->data;

		if (!list_find_id(tr->dropped, dep_id)) {

			switch (dep_type) {
				case SCHEMA_DEPENDENCY:
					if (sql_trans_drop_schema(tr, dep_id, DROP_CASCADE))
						return DEPENDENCY_CHECK_ERROR;
					break;
				case TABLE_DEPENDENCY:
				case VIEW_DEPENDENCY: {
										  sql_table *t = sql_trans_find_table(tr, dep_id);
										  if (t && sql_trans_drop_table_id(tr, t->s, dep_id, DROP_CASCADE))
												return DEPENDENCY_CHECK_ERROR;
									  } break;
				case COLUMN_DEPENDENCY: {
											if ((t_id = sql_trans_get_dependency_type(tr, dep_id, TABLE_DEPENDENCY)) > 0) {
												sql_table *t = sql_trans_find_table(tr, dep_id);
												if (t && sql_trans_drop_column(tr, t, dep_id, DROP_CASCADE))
													return DEPENDENCY_CHECK_ERROR;
											}
										} break;
				case TRIGGER_DEPENDENCY: {
										  sql_trigger *t = sql_trans_find_trigger(tr, dep_id);
										  if (t && !list_find_id(tr->dropped, t->t->base.id) && /* table not jet dropped */
											  sql_trans_drop_trigger(tr, t->t->s, dep_id, DROP_CASCADE))
												return DEPENDENCY_CHECK_ERROR;
										 } break;
				case KEY_DEPENDENCY:
				case FKEY_DEPENDENCY: {
										  sql_key *k = sql_trans_find_key(tr, dep_id);
										  if (k && !list_find_id(tr->dropped, k->t->base.id) && /* table not jet dropped */
										      sql_trans_drop_key(tr, k->t->s, dep_id, DROP_CASCADE))
												return DEPENDENCY_CHECK_ERROR;
									  } break;
				case INDEX_DEPENDENCY: {
										  sql_idx *i = sql_trans_find_idx(tr, dep_id);
										  if (i && !list_find_id(tr->dropped, i->t->base.id) && /* table not jet dropped */
										      sql_trans_drop_idx(tr, i->t->s, dep_id, DROP_CASCADE))
												return DEPENDENCY_CHECK_ERROR;
									   } break;
				case PROC_DEPENDENCY:
				case FUNC_DEPENDENCY: {
										  sql_func *f = sql_trans_find_func(tr, dep_id);
										  if (sql_trans_drop_func(tr, f->s, dep_id, DROP_CASCADE))
												return DEPENDENCY_CHECK_ERROR;
									  } break;
				case TYPE_DEPENDENCY: {
										  sql_type *t = sql_trans_find_type(tr, NULL, dep_id);
										  if (sql_trans_drop_type(tr, t->s, dep_id, DROP_CASCADE))
												return DEPENDENCY_CHECK_ERROR;
									  } break;
				case USER_DEPENDENCY:  /*TODO schema and users dependencies*/
									  break;
			}
		}

		n = n->next->next;
	}
	list_destroy(dep);
	return DEPENDENCY_CHECK_OK;
}

static int
sys_drop_ic(sql_trans *tr, sql_idx * i, sql_kc *kc)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(tr, syss, "objects");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysic, "id"), &i->base.id, find_sql_column(sysic, "name"), kc->c->base.name, NULL);

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, sysic, rid))
		return -2;
	return 0;
}

static int
sql_trans_drop_any_comment(sql_trans *tr, sqlid id)
{
	sqlstore *store = tr->store;
	sql_schema *sys;
	sql_column *id_col;
	sql_table *comments;
	oid row;

	sys = find_sql_schema(tr, "sys");
	assert(sys);

	comments = find_sql_table(tr, sys, "comments");
	if (!comments) /* for example during upgrades */
		return 0;

	id_col = find_sql_column(comments, "id");
	assert(id_col);

	row = store->table_api.column_find_row(tr, id_col, &id, NULL);
	if (!is_oid_nil(row)) {
		if (store->table_api.table_delete(tr, comments, row))
			return -1;
	}
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

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, sysidx, rid))
		return -2;
	if (sql_trans_drop_any_comment(tr, i->base.id))
		return -3;
	for (n = i->columns->h; n; n = n->next) {
		sql_kc *ic = n->data;
		if (sys_drop_ic(tr, i, ic))
			return -4;
	}

	/* remove idx from schema and table*/
	if (isGlobal(i->t))
		if (os_del(i->t->s->idxs, tr, i->base.name, dup_base(&i->base)))
			return -5;
	sql_trans_drop_dependencies(tr, i->base.id);

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, i->base.id, INDEX_DEPENDENCY);
	return 0;
}

static int
sys_drop_kc(sql_trans *tr, sql_key *k, sql_kc *kc)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(tr, syss, "objects");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(syskc, "id"), &k->base.id, find_sql_column(syskc, "name"), kc->c->base.name, NULL);

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, syskc, rid))
		return -2;
	return 0;
}

static int
sys_drop_key(sql_trans *tr, sql_key *k, int drop_action)
{
	sqlstore *store = tr->store;
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(syskey, "id"), &k->base.id, NULL);

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, syskey, rid))
		return -2;

	for (n = k->columns->h; n; n = n->next) {
		sql_kc *kc = n->data;
		if (sys_drop_kc(tr, k, kc))
			return -3;
	}
	/* remove key from schema */
	if (isGlobal(k->t)) {
		if (os_del(k->t->s->keys, tr, k->base.name, dup_base(&k->base)))
			return -4;
		if (os_del(tr->cat->objects, tr, k->base.name, dup_base(&k->base)))
			return -5;
	}
	if (k->t->pkey == (sql_ukey*)k)
		k->t->pkey = NULL;

	sql_trans_drop_dependencies(tr, k->base.id);

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, k->base.id, (k->type == fkey) ? FKEY_DEPENDENCY : KEY_DEPENDENCY);
	return 0;
}

static int
sys_drop_tc(sql_trans *tr, sql_trigger * i, sql_kc *kc)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(tr, syss, "objects");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(systc, "id"), &i->base.id, find_sql_column(systc, "name"), kc->c->base.name, NULL);

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, systc, rid))
		return -2;
	return 0;
}

static int
sys_drop_sequence(sql_trans *tr, sql_sequence * seq, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(tr, syss, "sequences");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysseqs, "id"), &seq->base.id, NULL);

	if (is_oid_nil(rid))
		return -1;

	if (store->table_api.table_delete(tr, sysseqs, rid))
		return -2;
	sql_trans_drop_dependencies(tr, seq->base.id);
	if (sql_trans_drop_any_comment(tr, seq->base.id))
		return -4;
	if (drop_action)
		sql_trans_drop_all_dependencies(tr, seq->base.id, SEQ_DEPENDENCY);
	return 0;
}

static int
sys_drop_statistics(sql_trans *tr, sql_column *col)
{
	sqlstore *store = tr->store;
	if (isGlobal(col->t)) {
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *sysstats = find_sql_table(tr, syss, "statistics");

		oid rid = store->table_api.column_find_row(tr, find_sql_column(sysstats, "column_id"), &col->base.id, NULL);

		if (is_oid_nil(rid)) /* no statistics */
			return 0;

		if (store->table_api.table_delete(tr, sysstats, rid))
			return -2;
	}
	return 0;
}

static int
sys_drop_default_object(sql_trans *tr, sql_column *col, int drop_action)
{
	const char *next_value_for = "next value for ";

	/* Drop sequence for generated column if it's the case */
	if (col->def && !strncmp(col->def, next_value_for, strlen(next_value_for))) {
		sql_schema *s = NULL;
		sql_sequence *seq = NULL;
		char *schema = NULL, *seq_name = NULL;

		extract_schema_and_sequence_name(tr->sa, col->def + strlen(next_value_for), &schema, &seq_name);
		if (!schema || !seq_name || !(s = find_sql_schema(tr, schema))) {
			_DELETE(schema);
			_DELETE(seq_name);
			return -1;
		}

		seq = find_sql_sequence(tr, s, seq_name);
		_DELETE(schema);
		_DELETE(seq_name);
		if (seq && sql_trans_get_dependency_type(tr, seq->base.id, BEDROPPED_DEPENDENCY) > 0) {
			if (sys_drop_sequence(tr, seq, drop_action))
				return -2;
			if (os_del(s->seqs, tr, seq->base.name, dup_base(&seq->base)))
				return -2;
		}
	}
	return 0;
}

static int
sql_trans_drop_obj_priv(sql_trans *tr, sqlid obj_id)
{
	sqlstore *store = tr->store;
	sql_schema *sys = find_sql_schema(tr, "sys");
	sql_table *privs = find_sql_table(tr, sys, "privileges");

	assert(sys && privs);
	/* select privileges of this obj_id */
	rids *A = store->table_api.rids_select(tr, find_sql_column(privs, "obj_id"), &obj_id, &obj_id, NULL);
	/* remove them */
	for(oid rid = store->table_api.rids_next(A); !is_oid_nil(rid); rid = store->table_api.rids_next(A))
		if (store->table_api.table_delete(tr, privs, rid))
			return -1;
	store->table_api.rids_destroy(A);
	return 0;
}

static int
sys_drop_trigger(sql_trans *tr, sql_trigger * i)
{
	sqlstore *store = tr->store;
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systrigger = find_sql_table(tr, syss, "triggers");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(systrigger, "id"), &i->base.id, NULL);

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, systrigger, rid))
		return -2;

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *tc = n->data;

		if (sys_drop_tc(tr, i, tc))
			return -3;
	}
	/* remove trigger from schema */
	if (isGlobal(i->t))
		if (os_del(i->t->s->triggers, tr, i->base.name, dup_base(&i->base)))
			return -4;
	sql_trans_drop_dependencies(tr, i->base.id);
	return 0;
}

static int
sys_drop_column(sql_trans *tr, sql_column *col, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"),
			&col->base.id, NULL);

	if (is_oid_nil(rid))
		return -1;
	if (store->table_api.table_delete(tr, syscolumn, rid))
		return -2;
	sql_trans_drop_dependencies(tr, col->base.id);
	if (sql_trans_drop_any_comment(tr, col->base.id))
		return -4;
	if (sql_trans_drop_obj_priv(tr, col->base.id))
		return -5;
	if (sys_drop_default_object(tr, col, drop_action))
		return -6;

	if (sys_drop_statistics(tr, col))
		return -7;
	if (drop_action)
		sql_trans_drop_all_dependencies(tr, col->base.id, COLUMN_DEPENDENCY);
	if (col->type.type->s)
		sql_trans_drop_dependency(tr, col->base.id, col->type.type->base.id, TYPE_DEPENDENCY);
	return 0;
}

static int
sys_drop_keys(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (ol_length(t->keys))
		for (n = ol_first_node(t->keys); n; n = n->next) {
			sql_key *k = n->data;

			if (sys_drop_key(tr, k, drop_action))
				return -1;
		}
	return 0;
}

static int
sys_drop_idxs(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (ol_length(t->idxs))
		for (n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *k = n->data;

			if (sys_drop_idx(tr, k, drop_action))
				return -1;
		}
	return 0;
}

static int
sys_drop_triggers(sql_trans *tr, sql_table *t)
{
	node *n;

	if (ol_length(t->triggers))
		for (n = ol_first_node(t->triggers); n; n = n->next) {
			sql_trigger *i = n->data;

			if (sys_drop_trigger(tr, i))
				return -1;
		}
	return 0;
}

static int
sys_drop_columns(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (ol_length(t->columns))
		for (n = t->columns->l->h; n; n = n->next) {
			sql_column *c = n->data;

			if (sys_drop_column(tr, c, drop_action))
				return -1;
		}
	return 0;
}

static int
sys_drop_part(sql_trans *tr, sql_part *pt, int drop_action)
{
	sqlstore *store = tr->store;
	sql_table *mt = pt->t;
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(tr, syss, "objects");
	oid obj_oid = store->table_api.column_find_row(tr, find_sql_column(sysobj, "id"), &pt->base.id, NULL);

	(void)drop_action;
	if (is_oid_nil(obj_oid))
		return -1;

	if (isRangePartitionTable(mt)) {
		sql_table *ranges = find_sql_table(tr, syss, "range_partitions");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(ranges, "table_id"), &pt->member, NULL);
		if (store->table_api.table_delete(tr, ranges, rid))
			return -2;
	} else if (isListPartitionTable(mt)) {
		sql_table *values = find_sql_table(tr, syss, "value_partitions");
		rids *rs = store->table_api.rids_select(tr, find_sql_column(values, "table_id"), &pt->member, &pt->member, NULL);
		for (oid rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs))
			if (store->table_api.table_delete(tr, values, rid))
				return -3;
		store->table_api.rids_destroy(rs);
	}
	/* merge table depends on part table */
	sql_trans_drop_dependency(tr, pt->member, mt->base.id, TABLE_DEPENDENCY);

	if (os_del(mt->s->parts, tr, pt->base.name, dup_base(&pt->base)))
		return -4;
	if (store->table_api.table_delete(tr, sysobj, obj_oid))
		return -5;
	return 0;
}

static int
sys_drop_members(sql_trans *tr, sql_table *t, int drop_action)
{
	if (!list_empty(t->members)) {
		for (node *n = t->members->h; n; ) {
			sql_part *pt = n->data;

			n = n->next;
			/*
			   if ((drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) &&
			   tr->dropped && list_find_id(tr->dropped, pt->base.id))
			   continue;
			   */

			if (sys_drop_part(tr, pt, drop_action))
				return -1;
		}
	}
	return 0;
}

static int
sys_drop_parts(sql_trans *tr, sql_table *t, int drop_action)
{
	for(sql_part *pt = partition_find_part(tr, t, NULL); pt; pt = partition_find_part(tr, t, pt)) {
		if (sql_trans_del_table(tr, pt->t, t, drop_action))
			return -1;
	}
	return 0;
}

static int
sys_drop_table(sql_trans *tr, sql_table *t, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(tr, syss, "_tables");
	sql_column *syscol = find_sql_column(systable, "id");
	oid rid = store->table_api.column_find_row(tr, syscol, &t->base.id, NULL);

	if (is_oid_nil(rid))
		return -1;

	if (store->table_api.table_delete(tr, systable, rid))
		return -2;
	if (sys_drop_keys(tr, t, drop_action))
		return -3;
	if (sys_drop_idxs(tr, t, drop_action))
		return -4;
	if (sys_drop_triggers(tr, t))
		return -4;

	if (partition_find_part(tr, t, NULL))
		if (sys_drop_parts(tr, t, drop_action))
			return -5;

	if (isMergeTable(t) || isReplicaTable(t))
		if (sys_drop_members(tr, t, drop_action))
			return -6;

	if (isRangePartitionTable(t) || isListPartitionTable(t)) {
		sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
		sql_column *pcols = find_sql_column(partitions, "table_id");
		rids *rs = store->table_api.rids_select(tr, pcols, &t->base.id, &t->base.id, NULL);
		oid poid;
		if ((poid = store->table_api.rids_next(rs)) != oid_nil)
			if (store->table_api.table_delete(tr, partitions, poid))
				return -7;
		store->table_api.rids_destroy(rs);
	}

	if (sql_trans_drop_any_comment(tr, t->base.id))
		return -8;
	sql_trans_drop_dependencies(tr, t->base.id);
	if (sql_trans_drop_obj_priv(tr, t->base.id))
		return -9;

	if (sys_drop_columns(tr, t, drop_action))
		return -10;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, t->base.id, !isView(t) ? TABLE_DEPENDENCY : VIEW_DEPENDENCY);
	return 0;
}

static int
sys_drop_type(sql_trans *tr, sql_type *type, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_type = find_sql_table(tr, syss, "types");
	sql_column *sys_type_col = find_sql_column(sys_tab_type, "id");
	oid rid = store->table_api.column_find_row(tr, sys_type_col, &type->base.id, NULL);

	if (is_oid_nil(rid))
		return -1;

	if (store->table_api.table_delete(tr, sys_tab_type, rid))
		return -2;
	sql_trans_drop_dependencies(tr, type->base.id);

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, type->base.id, TYPE_DEPENDENCY);
	return 0;
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

	for (oid r = store->table_api.rids_next(args); !is_oid_nil(r); r = store->table_api.rids_next(args))
		if (store->table_api.table_delete(tr, sys_tab_args, r))
			return -2;
	store->table_api.rids_destroy(args);

	assert(!is_oid_nil(rid_func));
	if (store->table_api.table_delete(tr, sys_tab_func, rid_func))
		return -3;

	sql_trans_drop_dependencies(tr, func->base.id);
	if (sql_trans_drop_any_comment(tr, func->base.id))
		return -4;
	if (sql_trans_drop_obj_priv(tr, func->base.id))
		return -4;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
	return 0;
}

static int
sys_drop_types(sql_trans *tr, sql_schema *s, int drop_action)
{
	struct os_iter oi;
	os_iterator(&oi, s->types, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_type *t = (sql_type*)b;

		if (sys_drop_type(tr, t, drop_action))
			return -1;
	}
	return 0;
}

static int
sys_drop_tables(sql_trans *tr, sql_schema *s, int drop_action)
{
	struct os_iter oi;
	os_iterator(&oi, s->tables, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_table *t = (sql_table*)b;

		if (sys_drop_table(tr, t, drop_action))
			return -1;
	}
	return 0;
}

static int
sys_drop_funcs(sql_trans *tr, sql_schema *s, int drop_action)
{
	struct os_iter oi;
	os_iterator(&oi, s->funcs, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_func *f = (sql_func*)b;

		if (sys_drop_func(tr, f, drop_action))
			return -1;
	}
	return 0;
}

static int
sys_drop_sequences(sql_trans *tr, sql_schema *s, int drop_action)
{
	struct os_iter oi;
	os_iterator(&oi, s->seqs, tr, NULL);
	for (sql_base *b = oi_next(&oi); b; b = oi_next(&oi)) {
		sql_sequence *seq = (sql_sequence*)b;

		if (sys_drop_sequence(tr, seq, drop_action))
			return -1;
	}
	return 0;
}

sql_type *
sql_trans_create_type(sql_trans *tr, sql_schema *s, const char *sqlname, unsigned int digits, unsigned int scale, int radix, const char *impl)
{
	sqlstore *store = tr->store;
	sql_type *t;
	sql_table *systype;
	int localtype = ATOMindex(impl);
	sql_class eclass = EC_EXTERNAL;
	int eclass_cast = (int) eclass;

	if (localtype < 0)
		return NULL;
	t = SA_ZNEW(tr->sa, sql_type);
	systype = find_sql_table(tr, find_sql_schema(tr, "sys"), "types");
	base_init(tr->sa, &t->base, next_oid(tr->store), TR_NEW, impl);
	t->sqlname = SA_STRDUP(tr->sa, sqlname);
	t->digits = digits;
	t->scale = scale;
	t->radix = radix;
	t->eclass = eclass;
	t->localtype = localtype;
	t->s = s;

	if (os_add(s->types, tr, t->base.name, &t->base)) {
		type_destroy(store, t);
		return NULL;
	}
	if (store->table_api.table_insert(tr, systype, &t->base.id, &t->base.name, &t->sqlname, &t->digits, &t->scale, &radix, &eclass_cast, &s->base.id))
		return NULL;
	return t;
}

int
sql_trans_drop_type(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sql_type *t = sql_trans_find_type(tr, s, id);

	if (sys_drop_type(tr, t, drop_action))
		return -1;
	if (os_del(s->types, tr, t->base.name, dup_base(&t->base)))
		return -2;
	return 0;
}

sql_func *
create_sql_func(sqlstore *store, sql_allocator *sa, const char *func, list *args, list *res, sql_ftype type, sql_flang lang, const char *mod,
		const char *impl, const char *query, bit varres, bit vararg, bit system)
{
	sql_func *t = SA_ZNEW(sa, sql_func);

	base_init(sa, &t->base, next_oid(store), TR_NEW, func);
	assert(impl && mod);
	t->imp = (impl)?SA_STRDUP(sa, impl):NULL;
	t->mod = (mod)?SA_STRDUP(sa, mod):NULL;
	t->type = type;
	t->lang = lang;
	t->sql = (lang==FUNC_LANG_SQL||lang==FUNC_LANG_MAL);
	t->semantics = TRUE;
	t->side_effect = (type==F_FILT || (res && (lang==FUNC_LANG_SQL || !list_empty(args))))?FALSE:TRUE;
	t->varres = varres;
	t->vararg = vararg;
	t->ops = args;
	t->res = res;
	t->query = (query)?SA_STRDUP(sa, query):NULL;
	t->fix_scale = SCALE_EQ;
	t->s = NULL;
	t->system = system;
	return t;
}

static sql_arg *
arg_dup(sql_trans *tr, sql_schema *s, sql_arg *oa)
{
	sql_arg *a = SA_ZNEW(tr->sa, sql_arg);

	if (a) {
		a->name = SA_STRDUP(tr->sa, oa->name);
		a->inout = oa->inout;
		dup_sql_type(tr, s, &(oa->type), &(a->type));
	}
	return a;
}

sql_func *
sql_trans_create_func(sql_trans *tr, sql_schema *s, const char *func, list *args, list *res, sql_ftype type, sql_flang lang,
		const char *mod, const char *impl, const char *query, bit varres, bit vararg, bit system)
{
	sqlstore *store = tr->store;
	sql_func *t = SA_ZNEW(tr->sa, sql_func);
	sql_table *sysfunc = find_sql_table(tr, find_sql_schema(tr, "sys"), "functions");
	sql_table *sysarg = find_sql_table(tr, find_sql_schema(tr, "sys"), "args");
	node *n;
	int number = 0, ftype = (int) type, flang = (int) lang;
	bit se;

	base_init(tr->sa, &t->base, next_oid(tr->store), TR_NEW, func);
	assert(impl && mod);
	t->imp = (impl)?SA_STRDUP(tr->sa, impl):NULL;
	t->mod = (mod)?SA_STRDUP(tr->sa, mod):NULL;
	t->type = type;
	t->lang = lang;
	t->sql = (lang==FUNC_LANG_SQL||lang==FUNC_LANG_MAL);
	t->semantics = TRUE;
	se = t->side_effect = (type==F_FILT || (res && (lang==FUNC_LANG_SQL || !list_empty(args))))?FALSE:TRUE;
	t->varres = varres;
	t->vararg = vararg;
	t->ops = SA_LIST(tr->sa, (fdestroy) &arg_destroy);
	t->fix_scale = SCALE_EQ;
	t->system = system;
	for (n=args->h; n; n = n->next)
		list_append(t->ops, arg_dup(tr, s, n->data));
	if (res) {
		t->res = SA_LIST(tr->sa, (fdestroy) &arg_destroy);
		for (n=res->h; n; n = n->next)
			list_append(t->res, arg_dup(tr, s, n->data));
	}
	t->query = (query)?SA_STRDUP(tr->sa, query):NULL;
	t->s = s;

	if (os_add(s->funcs, tr, t->base.name, &t->base)) {
		func_destroy(store, t);
		return NULL;
	}
	if (store->table_api.table_insert(tr, sysfunc, &t->base.id, &t->base.name, query?(char**)&query:&t->imp, &t->mod, &flang, &ftype, &se,
			&t->varres, &t->vararg, &s->base.id, &t->system, &t->semantics))
		return NULL;
	if (t->res) for (n = t->res->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid(tr->store);
		if (store->table_api.table_insert(tr, sysarg, &id, &t->base.id, &a->name, &a->type.type->sqlname, &a->type.digits, &a->type.scale, &a->inout, &number))
			return NULL;
	}
	if (t->ops) for (n = t->ops->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid(tr->store);
		if (store->table_api.table_insert(tr, sysarg, &id, &t->base.id, &a->name, &a->type.type->sqlname, &a->type.digits, &a->type.scale, &a->inout, &number))
			return NULL;
	}
	return t;
}

int
sql_trans_drop_func(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sql_base *b = os_find_id(s->funcs, tr, id);

	if (!b)
		return 0;

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

	if (sys_drop_func(tr, func, DROP_CASCADE))
		return -2;
	if (os_del(s->funcs, tr, func->base.name, dup_base(&func->base)))
		return -3;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

static int
build_drop_func_list_item(sql_trans *tr, sql_schema *s, sqlid id)
{
	sql_base *b = os_find_id(s->funcs, tr, id);

	if (b) {
		sql_func *func = (sql_func*)b;
		if (sys_drop_func(tr, func, DROP_CASCADE))
			return -1;
		if (os_del(s->funcs, tr, func->base.name, dup_base(&func->base)))
			return -2;
	}
	return 0;
}

int
sql_trans_drop_all_func(sql_trans *tr, sql_schema *s, list *list_func, int drop_action)
{
	node *n = NULL;
	sql_func *func = NULL;
	list* to_drop = NULL;

	(void) drop_action;

	if (!tr->dropped) {
		tr->dropped = list_create((fdestroy) &id_destroy);
		if (!tr->dropped)
			return -1;
	}
	for (n = list_func->h; n ; n = n->next ) {
		func = (sql_func *) n->data;

		if (! list_find_id(tr->dropped, func->base.id)){
			sqlid *local_id = MNEW(sqlid);
			if (!local_id) {
				list_destroy(tr->dropped);
				tr->dropped = NULL;
				if (to_drop)
					list_destroy(to_drop);
				return -1;
			}
			if (!to_drop) {
				to_drop = list_create(NULL);
				if (!to_drop) {
					list_destroy(tr->dropped);
					return -1;
				}
			}
			*local_id = func->base.id;
			list_append(tr->dropped, local_id);
			list_append(to_drop, func);
			//sql_trans_drop_func(tr, s, func->base.id, drop_action ? DROP_CASCADE : DROP_RESTRICT);
		}
	}

	if (to_drop) {
		for (n = to_drop->h; n ; n = n->next ) {
			func = (sql_func *) n->data;
			if (build_drop_func_list_item(tr, s, func->base.id))
				return -2;
		}
		list_destroy(to_drop);
	}

	if ( tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_schema *
sql_trans_create_schema(sql_trans *tr, const char *name, sqlid auth_id, sqlid owner)
{
	sqlstore *store = tr->store;
	sql_schema *s = SA_ZNEW(tr->sa, sql_schema);
	sql_table *sysschema = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");

	base_init(tr->sa, &s->base, next_oid(tr->store), TR_NEW, name);
	s->auth_id = auth_id;
	s->owner = owner;
	s->system = FALSE;
	s->tables = os_new(tr->sa, (destroy_fptr) &table_destroy, isTempSchema(s), true);
	s->types = os_new(tr->sa, (destroy_fptr) &type_destroy, isTempSchema(s), false);
	s->funcs = os_new(tr->sa, (destroy_fptr) &func_destroy, isTempSchema(s), false);
	s->seqs = os_new(tr->sa, (destroy_fptr) &seq_destroy, isTempSchema(s), true);
	s->keys = os_new(tr->sa, (destroy_fptr) &key_destroy, isTempSchema(s), true);
	s->idxs = os_new(tr->sa, (destroy_fptr) &idx_destroy, isTempSchema(s), true);
	s->triggers = os_new(tr->sa, (destroy_fptr) &trigger_destroy, isTempSchema(s), true);
	s->parts = os_new(tr->sa, (destroy_fptr) &part_destroy, isTempSchema(s), false);
	s->store = tr->store;

	if (os_add(tr->cat->schemas, tr, s->base.name, &s->base)) {
		schema_destroy(store, s);
		return NULL;
	}
	if (store->table_api.table_insert(tr, sysschema, &s->base.id, &s->base.name, &s->auth_id, &s->owner, &s->system))
		return NULL;
	return s;
}

sql_schema*
sql_trans_rename_schema(sql_trans *tr, sqlid id, const char *new_name)
{
	sqlstore *store = tr->store;
	sql_table *sysschema = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");
	sql_schema *s = find_sql_schema_id(tr, id);
	oid rid;

	assert(!strNil(new_name));

	/* delete schema, add schema */
	if (os_del(tr->cat->schemas, tr, s->base.name, dup_base(&s->base)))
		return NULL;
	sql_schema *ns = schema_dup(tr, s, new_name);
	if (os_add(tr->cat->schemas, tr, ns->base.name, &ns->base)) {
		schema_destroy(store, ns);
		return NULL;
	}

	rid = store->table_api.column_find_row(tr, find_sql_column(sysschema, "id"), &s->base.id, NULL);
	assert(!is_oid_nil(rid));
	store->table_api.column_update_value(tr, find_sql_column(sysschema, "name"), rid, (void*) new_name);
	return s;
}

int
sql_trans_drop_schema(sql_trans *tr, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_schema *s = find_sql_schema_id(tr, id);
	sql_table *sysschema = find_sql_table(tr, find_sql_schema(tr, "sys"), "schemas");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(sysschema, "id"), &s->base.id, NULL);

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

	if (store->table_api.table_delete(tr, sysschema, rid))
		return -1;
	if (sys_drop_funcs(tr, s, drop_action))
		return -1;
	if (sys_drop_tables(tr, s, drop_action))
		return -1;
	if (sys_drop_types(tr, s, drop_action))
		return -2;
	if (sys_drop_sequences(tr, s, drop_action))
		return -3;
	if (sql_trans_drop_any_comment(tr, s->base.id))
		return -4;
	if (sql_trans_drop_obj_priv(tr, s->base.id))
		return -5;

	if (os_del(tr->cat->schemas, tr, s->base.name, dup_base(&s->base)))
		return -6;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_table *
sql_trans_add_table(sql_trans *tr, sql_table *mt, sql_table *pt)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(tr, syss, "objects");
	sql_part *p = SA_ZNEW(tr->sa, sql_part);

	/* merge table depends on part table */
	sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
	assert(isMergeTable(mt) || isReplicaTable(mt));
	mt = new_table(tr, mt);
	if (!mt)
		return NULL;
	if (!mt->members)
		mt->members = list_new(tr->sa, (fdestroy) &part_destroy);
	p->t = mt;
	p->member = pt->base.id;

	base_init(tr->sa, &p->base, next_oid(store), TR_NEW, pt->base.name);
	list_append(mt->members, p);
	if (os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))) {
		part_destroy(store, p);
		return NULL;
	}
	if (store->table_api.table_insert(tr, sysobj, &p->base.id, &p->base.name, &mt->base.id, &pt->base.id))
		return NULL;
	return mt;
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
	int localtype = tpe.type->localtype, res = 0;
	ValRecord vmin, vmax;
	size_t smin, smax;
	bit to_insert = with_nills;
	oid rid;
	ptr ok;

	vmin = vmax = (ValRecord) {.vtype = TYPE_void,};

	mt = new_table(tr, mt);
	if (!mt)
		return -1;
	if (!mt->members)
		mt->members = list_new(tr->sa, (fdestroy) &part_destroy);
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
		res = -2;
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
		res = -2;
		goto finish;
	}

	if (!update) {
		p = SA_ZNEW(tr->sa, sql_part);
		base_init(tr->sa, &p->base, next_oid(store), TR_NEW, pt->base.name);
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
	p->part.range.minvalue = SA_NEW_ARRAY(tr->sa, char, smin);
	p->part.range.maxvalue = SA_NEW_ARRAY(tr->sa, char, smax);
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
		res = -4;
		goto finish;
	}

	if (!update) {
		rid = store->table_api.column_find_row(tr, find_sql_column(partitions, "table_id"), &mt->base.id, NULL);
		assert(!is_oid_nil(rid));

		/* add merge table dependency */
		sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
		sqlid id = store->table_api.column_find_sqlid(tr, find_sql_column(partitions, "id"), rid);
		if (store->table_api.table_insert(tr, sysobj, &p->base.id, &p->base.name, &mt->base.id, &pt->base.id)) {
			res = -6;
			goto finish;
		}
		char *vmin_val = VALget(&vmin);
		char *vmax_val = VALget(&vmax);
		if (store->table_api.table_insert(tr, ranges, &pt->base.id, &id, &vmin_val, &vmax_val, &to_insert)) {
			res = -6;
			goto finish;
		}
	} else {
		sql_column *cmin = find_sql_column(ranges, "minimum"), *cmax = find_sql_column(ranges, "maximum"),
				   *wnulls = find_sql_column(ranges, "with_nulls");

		rid = store->table_api.column_find_row(tr, find_sql_column(ranges, "table_id"), &pt->base.id, NULL);
		assert(!is_oid_nil(rid));

		store->table_api.column_update_value(tr, cmin, rid, VALget(&vmin));
		store->table_api.column_update_value(tr, cmax, rid, VALget(&vmax));
		store->table_api.column_update_value(tr, wnulls, rid, &to_insert);
	}

	if (!update && os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))) {
		part_destroy(store, p);
		res = -5;
	}
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
	int localtype = tpe.type->localtype, i = 0;

	mt = new_table(tr, mt);
	if (!mt)
		return -1;
	if (!mt->members)
		mt->members = list_new(tr->sa, (fdestroy) &part_destroy);
	if (!update) {
		p = SA_ZNEW(tr->sa, sql_part);
		base_init(tr->sa, &p->base, next_oid(store), TR_NEW, pt->base.name);
		assert(isMergeTable(mt) || isReplicaTable(mt));
		p->t = mt;
		assert(pt);
		p->member = pt->base.id;
	} else {
		rids *rs;
		node *n = members_find_child_id(mt->members, pt->base.id);
		p = (sql_part*) n->data;

		rs = store->table_api.rids_select(tr, find_sql_column(values, "table_id"), &pt->base.id, &pt->base.id, NULL);
		for (rid = store->table_api.rids_next(rs); !is_oid_nil(rid); rid = store->table_api.rids_next(rs)) {
			if (store->table_api.table_delete(tr, values, rid)) /* eliminate the old values */
				return -2;
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
		if (store->table_api.table_insert(tr, values, &pt->base.id, &id, &vnnil_val)) {
			list_destroy2(vals, store);
			return -1;
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
			return -i - 1;
		}
		ok = VALinit(&vvalue, localtype, next->value);
		if (ok && localtype != TYPE_str)
			ok = VALconvert(TYPE_str, &vvalue);
		if (!ok) {
			if (!update)
				part_destroy(store, p);
			VALclear(&vvalue);
			list_destroy2(vals, store);
			return -i - 1;
		}
		char *vvalue_val = VALget(&vvalue);
		if (store->table_api.table_insert(tr, values, &pt->base.id, &id, &vvalue_val)) {
			VALclear(&vvalue);
			list_destroy2(vals, store);
			return -i - 1;
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
		return -1;

	if (!update) {
		/* add merge table dependency */
		sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
		if (store->table_api.table_insert(tr, sysobj, &p->base.id, &p->base.name, &mt->base.id, &pt->base.id))
			return -1;
		if (os_add(mt->s->parts, tr, p->base.name, dup_base(&p->base))) {
			part_destroy(store, p);
			return -1;
		}
	}
	return 0;
}

sql_table*
sql_trans_rename_table(sql_trans *tr, sql_schema *s, sqlid id, const char *new_name)
{
	sqlstore *store = tr->store;
	sql_table *systable = find_sql_table(tr, find_sql_schema(tr, isTempSchema(s) ? "tmp":"sys"), "_tables");
	sql_table *t = find_sql_table_id(tr, s, id);
	oid rid;

	assert(!strNil(new_name));

	if (isGlobal(t)) {
		if (os_del(s->tables, tr, t->base.name, dup_base(&t->base)))
			return NULL;
	} else {
		node *n = cs_find_id(&tr->localtmps, t->base.id);
		if (n)
			cs_del(&tr->localtmps, tr->store, n, t->base.flags);
	}
	t = table_dup(tr, t, t->s, new_name);
	if (!isGlobal(t))
		cs_add(&tr->localtmps, t, TR_NEW);

	rid = store->table_api.column_find_row(tr, find_sql_column(systable, "id"), &t->base.id, NULL);
	assert(!is_oid_nil(rid));
	store->table_api.column_update_value(tr, find_sql_column(systable, "name"), rid, (void*) new_name);
	return t;
}

sql_table*
sql_trans_set_table_schema(sql_trans *tr, sqlid id, sql_schema *os, sql_schema *ns)
{
	sqlstore *store = tr->store;
	sql_table *systable = find_sql_table(tr, find_sql_schema(tr, isTempSchema(os) ? "tmp":"sys"), "_tables");
	sql_base*b = os_find_id(os->tables, tr, id);
	sql_table *t = (sql_table*)b;
	oid rid;

	rid = store->table_api.column_find_row(tr, find_sql_column(systable, "id"), &t->base.id, NULL);
	assert(!is_oid_nil(rid));
	store->table_api.column_update_value(tr, find_sql_column(systable, "schema_id"), rid, &(ns->base.id));

	if (os_del(os->tables, tr, t->base.name, dup_base(&t->base)))
		return NULL;
	return table_dup(tr, t, ns, NULL);
}

int
sql_trans_del_table(sql_trans *tr, sql_table *mt, sql_table *pt, int drop_action)
{
	sqlstore *store = tr->store;

	mt = new_table(tr, mt);
	if (!mt)
		return -1;
	node *n = members_find_child_id(mt->members, pt->base.id); /* get sqlpart id*/
	sqlid part_id = ((sql_part*)n->data)->base.id;
	sql_base *b = os_find_id(mt->s->parts, tr, part_id); /* fetch updated part */
	sql_part *p = (sql_part*)b;

	sys_drop_part(tr, p, drop_action);

	/*Clean the part from members*/
	list_remove_node(mt->members, store, n);

	if (drop_action == DROP_CASCADE)
		sql_trans_drop_table_id(tr, mt->s, pt->base.id, drop_action);
	return 0;
}

sql_table *
sql_trans_create_table(sql_trans *tr, sql_schema *s, const char *name, const char *sql, int tt, bit system,
					   int persistence, int commit_action, int sz, bte properties)
{
	sqlstore *store = tr->store;
	sql_table *t = create_sql_table_with_id(tr->sa, next_oid(tr->store), name, tt, system, persistence, commit_action, properties);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(tr, syss, "_tables");
	sht ca;

	/* temps all belong to a special tmp schema and only views/remote have a query */
	assert( (isTable(t) ||
		(!isTempTable(t) || (strcmp(s->base.name, "tmp") == 0) || isDeclaredTable(t))) || (isView(t) && !sql) || (isRemote(t) && !sql));

	t->query = sql ? SA_STRDUP(tr->sa, sql) : NULL;
	t->s = s;
	t->sz = sz;
	if (sz < 0)
		t->sz = COLSIZE;
	if (isGlobal(t)) {
		if (os_add(s->tables, tr, t->base.name, &t->base)) {
			//table_destroy(store, t);
			return NULL;
		}
	} else
		cs_add(&tr->localtmps, t, TR_NEW);
	if (isRemote(t))
		t->persistence = SQL_REMOTE;

	if (isTable(t))
		if (store->storage_api.create_del(tr, t) != LOG_OK)
			return NULL;
	if (isPartitionedByExpressionTable(t)) {
		t->part.pexp = SA_ZNEW(tr->sa, sql_expression);
		t->part.pexp->type = *sql_bind_localtype("void"); /* leave it non-initialized, at the backend the copy of this table will get the type */
		t->part.pexp->cols = SA_LIST(tr->sa, (fdestroy) &int_destroy);
	}

	ca = t->commit_action;
	if (!isDeclaredTable(t)) {
		char *strnil = (char*)ATOMnilptr(TYPE_str);
		if (store->table_api.table_insert(tr, systable, &t->base.id, &t->base.name, &s->base.id,
								 (t->query) ? &t->query : &strnil, &t->type, &t->system, &ca, &t->access))
			return NULL;
	}
	return t;
}

int
sql_trans_set_partition_table(sql_trans *tr, sql_table *t)
{
	sqlstore *store = tr->store;
	if (t && (isRangePartitionTable(t) || isListPartitionTable(t))) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
		sql_table *partitions = find_sql_table(tr, syss, "table_partitions");
		sqlid next = next_oid(tr->store);
		if (isPartitionedByColumnTable(t)) {
			assert(t->part.pcol);
			if (store->table_api.table_insert(tr, partitions, &next, &t->base.id, &t->part.pcol->base.id, &ATOMnilptr(TYPE_str), &t->properties))
				return -1;
		} else if (isPartitionedByExpressionTable(t)) {
			assert(t->part.pexp->exp);
			if (strlen(t->part.pexp->exp) > STORAGE_MAX_VALUE_LENGTH)
				return -1;
			if (store->table_api.table_insert(tr, partitions, &next, &t->base.id, ATOMnilptr(TYPE_int), &t->part.pexp->exp, &t->properties))
				return -1;
		} else {
			assert(0);
		}
	}
	return 0;
}

sql_key *
create_sql_kc(sqlstore *store, sql_allocator *sa, sql_key *k, sql_column *c)
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

sql_ukey *
create_sql_ukey(sqlstore *store, sql_allocator *sa, sql_table *t, const char *name, key_type kt)
{
	sql_key *nk = NULL;
	sql_ukey *tk;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);
 	tk = (sql_ukey *) nk;
	assert(name);

	base_init(sa, &nk->base, next_oid(store), TR_NEW, name);

	nk->type = kt;
	nk->columns = SA_LIST(sa, (fdestroy) NULL);
	nk->idx = NULL;
	nk->t = t;

	if (nk->type == pkey)
		t->pkey = tk;
	ol_add(t->keys, &nk->base);
	return tk;
}

sql_fkey *
create_sql_fkey(sqlstore *store, sql_allocator *sa, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
	sql_key *nk;
	sql_fkey *fk = NULL;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);

	assert(name);
	base_init(sa, &nk->base, next_oid(store), TR_NEW, name);

	nk->type = kt;
	nk->columns = SA_LIST(sa, (fdestroy) NULL);
	nk->t = t;
	nk->idx = create_sql_idx(store, sa, t, name, (nk->type == fkey) ? join_idx : hash_idx);
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;

	fk->rkey = rkey->base.id;
	ol_add(t->keys, &nk->base);
	return (sql_fkey*) nk;
}

sql_idx *
create_sql_ic(sqlstore *store, sql_allocator *sa, sql_idx *i, sql_column *c)
{
	sql_kc *ic = SA_ZNEW(sa, sql_kc);

	ic->c = c;
	list_append(i->columns, ic);

	(void)store;
	if (hash_index(i->type) && list_length(i->columns) > 1) {
		/* Correct the unique flag of the keys first column */
		c->unique = list_length(i->columns);
		if (c->unique == 2) {
			sql_kc *ic1 = i->columns->h->data;
			ic1->c->unique ++;
		}
	}

	/* should we switch to oph_idx ? */
	if (i->type == hash_idx && list_length(i->columns) == 1 && ic->c->sorted) {
		/*i->type = oph_idx;*/
		i->type = no_idx;
	}
	return i;
}

sql_idx *
create_sql_idx(sqlstore *store, sql_allocator *sa, sql_table *t, const char *name, idx_type it)
{
	sql_idx *ni = SA_ZNEW(sa, sql_idx);

	base_init(sa, &ni->base, next_oid(store), TR_NEW, name);
	ni->base.new = 1;

	ni->columns = SA_LIST(sa, (fdestroy) NULL);
	ni->t = t;
	ni->type = it;
	ni->key = NULL;
	ol_add(t->idxs, &ni->base);
	return ni;
}

static sql_column *
create_sql_column_with_id(sql_allocator *sa, sqlid id, sql_table *t, const char *name, sql_subtype *tpe)
{
	sql_column *col = SA_ZNEW(sa, sql_column);

	base_init(sa, &col->base, id, TR_NEW, name);
	col->type = *tpe;
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;

	ol_add(t->columns, &col->base);
	return col;
}

sql_column *
create_sql_column(sqlstore *store, sql_allocator *sa, sql_table *t, const char *name, sql_subtype *tpe)
{
	return create_sql_column_with_id(sa, next_oid(store), t, name, tpe);
}

int
sql_trans_drop_table(sql_trans *tr, sql_schema *s, const char *name, int drop_action)
{
	sql_table *t = find_sql_table(tr, s, name);
	int ok = LOG_OK, is_global = isGlobal(t);
	node *n = NULL;

	if (!is_global)
		n = cs_find_id(&tr->localtmps, t->base.id);

	if ((drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) &&
	    tr->dropped && list_find_id(tr->dropped, t->base.id))
		return 0;

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
		if (sys_drop_table(tr, t, drop_action))
			return -1;

	if (is_global) {
		if (os_del(s->tables, tr, t->base.name, dup_base(&t->base)))
			return -2;
	} else if (n)
		cs_del(&tr->localtmps, tr->store, n, 0);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return ok;
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

sql_column *
sql_trans_create_column(sql_trans *tr, sql_table *t, const char *name, sql_subtype *tpe)
{
	sqlstore *store = tr->store;
	sql_column *col;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(tr, syss, "_columns");

	if (!tpe)
		return NULL;

	if (t->system && sql_trans_name_conflict(tr, t->s->base.name, t->base.name, name))
		return NULL;
	col = create_sql_column_with_id(tr->sa, next_oid(tr->store), t, name, tpe);

	if (isTable(col->t))
		if (store->storage_api.create_col(tr, col) != LOG_OK)
			return NULL;
	if (!isDeclaredTable(t)) {
		char *strnil = (char*)ATOMnilptr(TYPE_str);
		if (store->table_api.table_insert(tr, syscolumn, &col->base.id, &col->base.name, &col->type.type->sqlname, &col->type.digits, &col->type.scale, &t->base.id, (col->def) ? &col->def : &strnil, &col->null, &col->colnr, (col->storage_type) ? &col->storage_type : &strnil))
			return NULL;
	}

	if (tpe->type->s) /* column depends on type */
		sql_trans_create_dependency(tr, tpe->type->base.id, col->base.id, TYPE_DEPENDENCY);
	return col;
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

sql_column*
sql_trans_rename_column(sql_trans *tr, sql_table *t, const char *old_name, const char *new_name)
{
	sqlstore *store = tr->store;
	sql_table *syscolumn = find_sql_table(tr, find_sql_schema(tr, isGlobal(t)?"sys":"tmp"), "_columns");
	oid rid;

	assert(!strNil(new_name));

	t = new_table(tr, t);
	if (!t)
		return NULL;
	node *n = ol_find_name(t->columns, old_name);
	if (!n)
		return NULL;
	sql_column *c = n->data;

	_DELETE(c->base.name);
	c->base.name = SA_STRDUP(tr->sa, new_name);
	if (ol_rehash(t->columns, old_name, n) == NULL)
		return NULL;

	rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"), &c->base.id, NULL);
	assert(!is_oid_nil(rid));
	store->table_api.column_update_value(tr, find_sql_column(syscolumn, "name"), rid, (void*) new_name);
	return c;
}

static sql_column*
new_column(sql_trans *tr, sql_column *col)
{
		sql_table *t = new_table(tr, col->t);
		if (!t)
			return NULL;
		return find_sql_column(t, col->base.name);
}

int
sql_trans_drop_column(sql_trans *tr, sql_table *t, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	node *n = NULL;
	sql_table *syscolumn = find_sql_table(tr, find_sql_schema(tr, isGlobal(t)?"sys":"tmp"), "_columns");
	sql_column *col = NULL, *cid = find_sql_column(syscolumn, "id"), *cnr = find_sql_column(syscolumn, "number");

	t = new_table(tr, t);
	if (!t)
		return -1;
	for (node *nn = t->columns->l->h ; nn ; nn = nn->next) {
		sql_column *next = (sql_column *) nn->data;
		if (next->base.id == id) {
			n = nn;
			col = next;
		} else if (col) { /* if the column to be dropped was found, decrease the column number for others after it */
			oid rid;
			next->colnr--;

			rid = store->table_api.column_find_row(tr, cid, &next->base.id, NULL);
			assert(!is_oid_nil(rid));
			store->table_api.column_update_value(tr, cnr, rid, &next->colnr);
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

	if (sys_drop_column(tr, col, drop_action))
		return -1;

	ol_del(t->columns, store, n);
	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_column *
sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull)
{
	sqlstore *store = tr->store;

	if (col->null != isnull) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(syscolumn, "id"),
					  &col->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		store->table_api.column_update_value(tr, find_sql_column(syscolumn, "null"), rid, &isnull);

		col = new_column(tr, col);
		col->null = isnull;
	}
	return col;
}

sql_table *
sql_trans_alter_access(sql_trans *tr, sql_table *t, sht access)
{
	sqlstore *store = tr->store;
	if (t->access != access) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
		sql_table *systable = find_sql_table(tr, syss, "_tables");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(systable, "id"),
					  &t->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		store->table_api.column_update_value(tr, find_sql_column(systable, "access"), rid, &access);
		t = new_table(tr, t);
		if (!t)
			return NULL;
		t->access = access;
	}
	return t;
}

sql_column *
sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val)
{
	sqlstore *store = tr->store;
	if (!col->def && !val)
		return col;	/* no change */

	if (!col->def || !val || strcmp(col->def, val) != 0) {
		void *p = val ? val : (void *) ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "default");
		oid rid = store->table_api.column_find_row(tr, col_ids, &col->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		if (sys_drop_default_object(tr, col, 0))
			return NULL;
		store->table_api.column_update_value(tr, col_dfs, rid, p);

		col = new_column(tr, col);
		_DELETE(col->def);
		col->def = NULL;
		if (val)
			col->def = SA_STRDUP(tr->sa, val);
	}
	return col;
}

sql_column *
sql_trans_alter_storage(sql_trans *tr, sql_column *col, char *storage)
{
	sqlstore *store = tr->store;
	if (!col->storage_type && !storage)
		return col;	/* no change */

	if (!col->storage_type || !storage || strcmp(col->storage_type, storage) != 0) {
		void *p = storage ? storage : (void *) ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(tr, syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "storage");
		oid rid = store->table_api.column_find_row(tr, col_ids, &col->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		store->table_api.column_update_value(tr, col_dfs, rid, p);

		col = new_column(tr, col);
		col->storage_type = NULL;
		if (storage)
			col->storage_type = SA_STRDUP(tr->sa, storage);
	}
	return col;
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
	if (col && isTable(col->t) && EC_VARCHAR(col->type.type->eclass) && store->storage_api.double_elim_col)
		return store->storage_api.double_elim_col(tr, col);
	return 0;
}

size_t
sql_trans_dist_count( sql_trans *tr, sql_column *col )
{
	sqlstore *store = tr->store;
	if (col->dcount)
		return col->dcount;

	if (col && isTable(col->t)) {
		/* get from statistics */
		sql_schema *sys = find_sql_schema(tr, "sys");
		sql_table *stats = find_sql_table(tr, sys, "statistics");
		if (stats) {
			sql_column *stats_column_id = find_sql_column(stats, "column_id");
			oid rid = store->table_api.column_find_row(tr, stats_column_id, &col->base.id, NULL);
			if (!is_oid_nil(rid)) {
				col->dcount = (size_t) store->table_api.column_find_lng(tr, find_sql_column(stats, "unique"), rid);
			} else { /* sample and put in statistics */
				col->dcount = store->storage_api.dcount_col(tr, col);
			}
		}
		return col->dcount;
	}
	return 0;
}

int
sql_trans_ranges( sql_trans *tr, sql_column *col, char **min, char **max )
{
	sqlstore *store = tr->store;
	*min = NULL;
	*max = NULL;
	if (col && isTable(col->t)) {
		/* get from statistics */
		sql_schema *sys = find_sql_schema(tr, "sys");
		sql_table *stats = find_sql_table(tr, sys, "statistics");

		if (col->min && col->max) {
			*min = col->min;
			*max = col->max;
			return 1;
		}
		if (stats) {
			sql_column *stats_column_id = find_sql_column(stats, "column_id");
			oid rid = store->table_api.column_find_row(tr, stats_column_id, &col->base.id, NULL);
			if (!is_oid_nil(rid)) {
				ptr cbat;
				char *v;
				sql_column *stats_min = find_sql_column(stats, "minval");
				sql_column *stats_max = find_sql_column(stats, "maxval");

				v = store->table_api.column_find_string_start(tr, stats_min, rid, &cbat);
				*min = col->min = SA_STRDUP(tr->sa, v);
				store->table_api.column_find_string_end(cbat);
				v = store->table_api.column_find_string_start(tr, stats_max, rid, &cbat);
				*max = col->max = SA_STRDUP(tr->sa, v);
				store->table_api.column_find_string_end(cbat);
				return 1;
			}
		}
	}
	return 0;
}

sql_key *
sql_trans_create_ukey(sql_trans *tr, sql_table *t, const char *name, key_type kt)
{
/* can only have keys between persistent tables */
	sqlstore *store = tr->store;
	int neg = -1;
	int action = -1;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	sql_ukey *uk = NULL;

	if (isTempTable(t))
		return NULL;

	t = new_table(tr, t);
	if (!t)
		return NULL;
	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(tr->sa, sql_ukey)
	: (sql_key *) SA_ZNEW(tr->sa, sql_fkey);

	assert(name);
	base_init(tr->sa, &nk->base, next_oid(tr->store), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = NULL;

	uk = (sql_ukey *) nk;

	if (nk->type == pkey)
		t->pkey = uk;

	ol_add(t->keys, &nk->base);
	if (os_add(t->s->keys, tr, nk->base.name, dup_base(&nk->base)) ||
		os_add(tr->cat->objects, tr, nk->base.name, dup_base(&nk->base))) {
		key_destroy(store, nk);
		return NULL;
	}

	if (store->table_api.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, &nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey : &neg, &action ))
		return NULL;
	return nk;
}

sql_fkey *
sql_trans_create_fkey(sql_trans *tr, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
/* can only have keys between persistent tables */
	sqlstore *store = tr->store;
	int neg = -1;
	int action = (on_update<<8) + on_delete;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(tr, syss, "keys");
	sql_fkey *fk = NULL;

	if (isTempTable(t))
		return NULL;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(tr->sa, sql_ukey)
	: (sql_key *) SA_ZNEW(tr->sa, sql_fkey);

	assert(name);
	base_init(tr->sa, &nk->base, next_oid(tr->store), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = sql_trans_create_idx(tr, t, name, (nk->type == fkey) ? join_idx : hash_idx);
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;

	fk->rkey = rkey->base.id;

	ol_add(t->keys, &nk->base);
	if (os_add(t->s->keys, tr, nk->base.name, dup_base(&nk->base)) ||
		os_add(tr->cat->objects, tr, nk->base.name, dup_base(&nk->base))) {
		key_destroy(store, nk);
		return NULL;
	}

	if (store->table_api.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, &nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey : &neg, &action))
		return NULL;

	sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey, nk->base.id, FKEY_DEPENDENCY);
	return (sql_fkey*) nk;
}

sql_key *
sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c )
{
	sqlstore *store = tr->store;
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(k->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(tr, syss, "objects");

	assert(c);
	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx)
		sql_trans_create_ic(tr, k->idx, c);

	if (k->type == pkey) {
		sql_trans_create_dependency(tr, c->base.id, k->base.id, KEY_DEPENDENCY);
		sql_trans_alter_null(tr, c, 0);
	}

	if (store->table_api.table_insert(tr, syskc, &k->base.id, &kc->c->base.name, &nr, ATOMnilptr(TYPE_int)))
		return NULL;
	return k;
}

sql_fkey *
sql_trans_create_fkc(sql_trans *tr, sql_fkey *fk, sql_column *c )
{
	sqlstore *store = tr->store;
	sql_key *k = (sql_key *) fk;
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(k->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(tr, syss, "objects");

	assert(c);
	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx)
		sql_trans_create_ic(tr, k->idx, c);

	sql_trans_create_dependency(tr, c->base.id, k->base.id, FKEY_DEPENDENCY);

	if (store->table_api.table_insert(tr, syskc, &k->base.id, &kc->c->base.name, &nr, ATOMnilptr(TYPE_int)))
		return NULL;
	return (sql_fkey*)k;
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
key_create_done(sqlstore *store, sql_allocator *sa, sql_key *k)
{
	node *n;
	sql_idx *i;

	/* for now we only mark the end of unique/primary key definitions */
	if (k->type == fkey)
		return k;

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

	for (n=k->columns->h; n; n = n->next) {
		sql_kc *kc = n->data;

		create_sql_ic(store, sa, k->idx, kc->c);
	}
	return k;
}

sql_key *
sql_trans_key_done(sql_trans *tr, sql_key *k)
{
	node *n;
	sql_idx *i;

	/* for now we only mark the end of unique/primary key definitions */
	if (k->type == fkey)
		return k;

	if ((i = table_has_idx(k->t, k->columns)) != NULL) {
		/* use available hash, or use the order */
		if (hash_index(i->type)) {
			k->idx = i;
			if (!k->idx->key)
				k->idx->key = k;
		}
		return k;
	}

	/* we need to create an index */
	k->idx = sql_trans_create_idx(tr, k->t, k->base.name, hash_idx);
	k->idx->key = k;

	for (n=k->columns->h; n; n = n->next) {
		sql_kc *kc = n->data;

		sql_trans_create_ic(tr, k->idx, kc->c);
	}
	return k;
}

int
sql_trans_drop_key(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_base *b = os_find_id(s->keys, tr, id);
	sql_key *k = (sql_key*)b;
	sql_table *t = k->t;

	t = new_table(tr, t);
	if (!t)
		return -1;
	k = (sql_key*)os_find_id(s->keys, tr, id); /* fetch updated key */

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id) {
			return -1;
		}

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

	if (k->idx)
		sql_trans_drop_idx(tr, s, k->idx->base.id, drop_action);

	if (!isTempTable(k->t))
		sys_drop_key(tr, k, drop_action);

	/*Clean the key from the keys*/
	node *n = ol_find_name(k->t->keys, k->base.name);
	if (n)
		ol_del(k->t->keys, store, n);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_idx *
sql_trans_create_idx(sql_trans *tr, sql_table *t, const char *name, idx_type it)
{
	/* can only have idxs between persistent tables */
	sqlstore *store = tr->store;
	sql_idx *ni = SA_ZNEW(tr->sa, sql_idx);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(tr, syss, "idxs");

	assert(name);
	base_init(tr->sa, &ni->base, next_oid(tr->store), TR_NEW, name);
	ni->type = it;
	ni->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	ni->t = t;
	ni->key = NULL;

	ol_add(t->idxs, &ni->base);
	if (os_add(t->s->idxs, tr, ni->base.name, dup_base(&ni->base))) {
		idx_destroy(store, ni);
		return NULL;
	}

	if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
		store->storage_api.create_idx(tr, ni);
	if (!isDeclaredTable(t))
		if (store->table_api.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, &ni->base.name))
			return NULL;
	return ni;
}

sql_idx *
sql_trans_create_ic(sql_trans *tr, sql_idx * i, sql_column *c)
{
	sqlstore *store = tr->store;
	sql_kc *ic = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(i->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(tr, syss, "objects");

	assert(c);
	ic->c = c;
	list_append(i->columns, ic);

	if (hash_index(i->type) && list_length(i->columns) > 1) {
		/* Correct the unique flag of the keys first column */
		c->unique = list_length(i->columns);
		if (c->unique == 2) {
			sql_kc *ic1 = i->columns->h->data;
			ic1->c->unique ++;
		}
	}

	/* should we switch to oph_idx ? */
#if 0
	if (i->type == hash_idx && list_length(i->columns) == 1 &&
	    store->storage_api.count_col(tr, ic->c) && store->storage_api.sorted_col(tr, ic->c)) {
		sql_table *sysidx = find_sql_table(tr, syss, "idxs");
		sql_column *sysidxid = find_sql_column(sysidx, "id");
		sql_column *sysidxtype = find_sql_column(sysidx, "type");
		oid rid = store->table_api.column_find_row(tr, sysidxid, &i->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		/*i->type = oph_idx;*/
		i->type = no_idx;
		store->table_api.column_update_value(tr, sysidxtype, rid, &i->type);
	}
#endif
	if (store->table_api.table_insert(tr, sysic, &i->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int)))
		return NULL;
	return i;
}

int
sql_trans_drop_idx(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_base *b = os_find_id(s->idxs, tr, id);

	if (!b) /* already dropped */
		return 0;

	sql_idx *i = (sql_idx*)b;
	sql_table *t = new_table(tr, i->t);
	if (!t)
		return -1;
	i = (sql_idx*)os_find_id(t->s->idxs, tr, id); /* fetch updated idx */

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id) {
			return -1;
		}

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

	if (!isTempTable(i->t))
		sys_drop_idx(tr, i, drop_action);

	node *n = ol_find_name(i->t->idxs, i->base.name);
	if (n)
		ol_del(i->t->idxs, store, n);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_trigger *
sql_trans_create_trigger(sql_trans *tr, sql_table *t, const char *name,
	sht time, sht orientation, sht event, const char *old_name, const char *new_name,
	const char *condition, const char *statement )
{
	sqlstore *store = tr->store;
	sql_trigger *nt = SA_ZNEW(tr->sa, sql_trigger);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systrigger = find_sql_table(tr, syss, "triggers");
	char *strnil = (char*)ATOMnilptr(TYPE_str);

	assert(name);

	t = new_table(tr, t);
	if (!t)
		return NULL;
	base_init(tr->sa, &nt->base, next_oid(tr->store), TR_NEW, name);
	nt->columns = list_new(tr->sa, (fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = time;
	nt->orientation = orientation;
	nt->event = event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (old_name)
		nt->old_name = SA_STRDUP(tr->sa, old_name);
	if (new_name)
		nt->new_name = SA_STRDUP(tr->sa, new_name);
	if (condition)
		nt->condition = SA_STRDUP(tr->sa, condition);
	nt->statement = SA_STRDUP(tr->sa, statement);

	ol_add(t->triggers, &nt->base);
	if (os_add(t->s->triggers, tr, nt->base.name, dup_base(&nt->base))) {
		trigger_destroy(store, nt);
		return NULL;
	}

	if (store->table_api.table_insert(tr, systrigger, &nt->base.id, &nt->base.name, &t->base.id, &nt->time, &nt->orientation,
							 &nt->event, (nt->old_name)?&nt->old_name:&strnil, (nt->new_name)?&nt->new_name:&strnil,
							 (nt->condition)?&nt->condition:&strnil, &nt->statement))
		return NULL;
	return nt;
}

sql_trigger *
sql_trans_create_tc(sql_trans *tr, sql_trigger * i, sql_column *c )
{
	sqlstore *store = tr->store;
	sql_kc *ic = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(i->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(tr, syss, "objects");

	assert(c);
	ic->c = c;
	list_append(i->columns, ic);
	if (store->table_api.table_insert(tr, systc, &i->base.id, &ic->c->base.name, &nr, ATOMnilptr(TYPE_int)))
		return NULL;
	return i;
}

int
sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	sqlstore *store = tr->store;
	sql_base *b = os_find_id(s->triggers, tr, id);

	if (!b) /* already dropped */
		return 0;

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

	if (sys_drop_trigger(tr, i))
		return -1;
	node *n = ol_find_name(i->t->triggers, i->base.name);
	if (n)
		ol_del(i->t->triggers, store, n);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

static sql_sequence *
create_sql_sequence_with_id(sql_allocator *sa, sqlid id, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
					lng cacheinc, bit cycle)
{
	sql_sequence *seq = SA_ZNEW(sa, sql_sequence);

	assert(name);
	base_init(sa, &seq->base, id, TR_NEW, name);
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
create_sql_sequence(sqlstore *store, sql_allocator *sa, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
					lng cacheinc, bit cycle)
{
	return create_sql_sequence_with_id(sa, next_oid(store), s, name, start, min, max, inc, cacheinc, cycle);
}

sql_sequence *
sql_trans_create_sequence(sql_trans *tr, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
						  lng cacheinc, bit cycle, bit bedropped)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(tr, syss, "sequences");
	sql_sequence *seq = create_sql_sequence_with_id(tr->sa, next_oid(tr->store), s, name, start, min, max, inc, cacheinc, cycle);

	if (os_add(s->seqs, tr, seq->base.name, &seq->base)) {
		seq_destroy(store, seq);
		return NULL;
	}
	if (store->table_api.table_insert(tr, sysseqs, &seq->base.id, &s->base.id, &seq->base.name, &seq->start, &seq->minvalue,
							 &seq->maxvalue, &seq->increment, &seq->cacheinc, &seq->cycle))
		return NULL;

	/*Create a BEDROPPED dependency for a SERIAL COLUMN*/
	if (bedropped)
		sql_trans_create_dependency(tr, seq->base.id, seq->base.id, BEDROPPED_DEPENDENCY);
	return seq;
}

int
sql_trans_drop_sequence(sql_trans *tr, sql_schema *s, sql_sequence *seq, int drop_action)
{
	if (sys_drop_sequence(tr, seq, drop_action))
		return -1;
	if (os_del(s->seqs, tr, seq->base.name, dup_base(&seq->base)))
		return -2;
	return 0;
}

sql_sequence *
sql_trans_alter_sequence(sql_trans *tr, sql_sequence *seq, lng min, lng max, lng inc, lng cache, bit cycle)
{
	sqlstore *store = tr->store;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(tr, syss, "sequences");
	oid rid = store->table_api.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
	sql_column *c;

	if (is_oid_nil(rid))
		return NULL;
	if (!is_lng_nil(min) && seq->minvalue != min) {
		seq->minvalue = min;
		c = find_sql_column(seqs, "minvalue");
		store->table_api.column_update_value(tr, c, rid, &seq->minvalue);
	}
	if (!is_lng_nil(max) && seq->maxvalue != max) {
		seq->maxvalue = max;
		c = find_sql_column(seqs, "maxvalue");
		store->table_api.column_update_value(tr, c, rid, &seq->maxvalue);
	}
	if (!is_lng_nil(inc) && seq->increment != inc) {
		seq->increment = inc;
		c = find_sql_column(seqs, "increment");
		store->table_api.column_update_value(tr, c, rid, &seq->increment);
	}
	if (!is_lng_nil(cache) && seq->cacheinc != cache) {
		seq->cacheinc = cache;
		c = find_sql_column(seqs, "cacheinc");
		store->table_api.column_update_value(tr, c, rid, &seq->cacheinc);
	}
	if (!is_lng_nil(cycle) && seq->cycle != cycle) {
		seq->cycle = cycle != 0;
		c = find_sql_column(seqs, "cycle");
		store->table_api.column_update_value(tr, c, rid, &seq->cycle);
	}
	return seq;
}

sql_sequence *
sql_trans_sequence_restart(sql_trans *tr, sql_sequence *seq, lng start)
{
	sqlstore *store = tr->store;
	if (!is_lng_nil(start) && seq->start != start) { /* new valid value, change */
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *seqs = find_sql_table(tr, syss, "sequences");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
		sql_column *c = find_sql_column(seqs, "start");

		assert(!is_oid_nil(rid));
		seq->start = start;
		store->table_api.column_update_value(tr, c, rid, &start);
	}
	return seq_restart(tr->store, seq, start) ? seq : NULL;
}

sql_sequence *
sql_trans_seqbulk_restart(sql_trans *tr, seqbulk *sb, lng start)
{
	sqlstore *store = tr->store;
	sql_sequence *seq = sb->seq;
	if (!is_lng_nil(start) && seq->start != start) { /* new valid value, change */
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *seqs = find_sql_table(tr, syss, "sequences");
		oid rid = store->table_api.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
		sql_column *c = find_sql_column(seqs, "start");

		assert(!is_oid_nil(rid));
		seq->start = start;
		store->table_api.column_update_value(tr, c, rid, &start);
	}
	return seqbulk_restart(tr->store, sb, start) ? seq : NULL;
}

sql_session *
sql_session_create(sqlstore *store, sql_allocator *sa, int ac)
{
	sql_session *s;

	if (store->singleuser > 1)
		return NULL;

	s = SA_ZNEW(/*sa*/NULL, sql_session);
	if (!s)
		return NULL;
	s->sa = sa;
	assert(sa);
	s->tr = sql_trans_create_(store, NULL, NULL);
	if (!s->tr) {
		return NULL;
	}
	s->schema_name = NULL;
	s->tr->active = 0;
	if (!sql_session_reset(s, ac)) {
		sql_trans_destroy(s->tr);
		return NULL;
	}
	if (store->singleuser)
		store->singleuser++;
	return s;
}

void
sql_session_destroy(sql_session *s)
{
	if (s->tr) {
		sqlstore *store = s->tr->store;
		store->singleuser--;
	}
	assert(!s->tr || s->tr->active == 0);
	if (s->tr)
		sql_trans_destroy(s->tr);
	_DELETE(s);
}

int
sql_session_reset(sql_session *s, int ac)
{
	char *def_schema_name = SA_STRDUP(s->sa, "sys");

	if (!s->tr || !def_schema_name)
		return 0;

	assert(s->tr && s->tr->active == 0);
	s->schema_name = def_schema_name;
	s->schema = NULL;
	s->auto_commit = s->ac_on_commit = ac;
	s->level = ISO_SERIALIZABLE;
	return 1;
}

int
sql_trans_begin(sql_session *s)
{
	sql_trans *tr = s->tr;
	sqlstore *store = tr->store;

	TRC_DEBUG(SQL_STORE, "Enter sql_trans_begin for transaction: " ULLFMT "\n", tr->tid);
	tr->ts = store_timestamp(store);
	tr->active = 1;
	s->schema = find_sql_schema(tr, s->schema_name);
	s->tr = tr;

	(void) ATOMIC_INC(&store->nr_active);
	list_append(store->active, s);

	s->status = 0;
	TRC_DEBUG(SQL_STORE, "Exit sql_trans_begin for transaction: " ULLFMT "\n", tr->tid);
	return 0;
}

int
sql_trans_end(sql_session *s, int commit)
{
	int ok = SQL_OK;
	TRC_DEBUG(SQL_STORE, "End of transaction: " ULLFMT "\n", s->tr->tid);
	if (commit) {
		ok = sql_trans_commit(s->tr);
	}  else {
		sql_trans_rollback(s->tr);
	}
	s->tr->active = 0;
	s->auto_commit = s->ac_on_commit;
	sqlstore *store = s->tr->store;
	list_remove_data(store->active, NULL, s);
	(void) ATOMIC_DEC(&store->nr_active);
	if (store->active && store->active->h) {
		ulng oldest = TRANSACTION_ID_BASE;
		for(node *n = store->active->h; n; n = n->next) {
			sql_session *s = n->data;
			if (s->tr->ts < oldest)
				oldest = s->tr->ts;
		}
		store->oldest = oldest;
	}
	assert(list_length(store->active) == (int) ATOMIC_GET(&store->nr_active));
	return ok;
}

int
sql_trans_create_role(sql_trans *tr, str auth, sqlid grantor)
{
	sqlstore *store = tr->store;
	sqlid id;
	sql_schema *sys = find_sql_schema(tr, "sys");
	sql_table *auths = find_sql_table(tr, sys, "auths");
	sql_column *auth_name = find_sql_column(auths, "name");

	if (!is_oid_nil(store->table_api.column_find_row(tr, auth_name, auth, NULL)))
		return -1;

	id = store_next_oid(tr->store);
	if (store->table_api.table_insert(tr, auths, &id, &auth, &grantor))
		return -2;
	return 0;
}
