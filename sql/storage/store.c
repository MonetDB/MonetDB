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

#include "bat/bat_utils.h"
#include "bat/bat_storage.h"
#include "bat/bat_table.h"
#include "bat/bat_logger.h"

/* version 05.22.05 of catalog */
#define CATALOG_VERSION 52205	/* first in Oct2020 */
int catalog_version = 0;

static MT_Lock bs_lock = MT_LOCK_INITIALIZER("bs_lock");
static sqlid store_oid = 0;
static sqlid prev_oid = 0;
static sqlid *store_oids = NULL;
static int nstore_oids = 0;
static size_t new_trans_size = 0;
sql_trans *gtrans = NULL;
/* keep list(s) of active and passive sessions (active started a transaction) */
list *active_sessions = NULL;
list *passive_sessions = NULL;
sql_allocator *store_sa = NULL;
ATOMIC_TYPE transactions = ATOMIC_VAR_INIT(0);
ATOMIC_TYPE nr_sessions = ATOMIC_VAR_INIT(0);
ATOMIC_TYPE store_nr_active = ATOMIC_VAR_INIT(0);
store_type active_store_type = store_bat;
int store_readonly = 0;
int store_singleuser = 0;
int store_initialized = 0;
int store_debug = 0;

store_functions store_funcs;
table_functions table_funcs;
logger_functions logger_funcs;

static int schema_number = 0; /* each committed schema change triggers a new
				 schema number (session wise unique number) */

#define MAX_SPARES 32
static sql_trans *spare_trans[MAX_SPARES];
static int spares = 0;

static int
key_cmp(sql_key *k, sqlid *id)
{
	if (k && id &&k->base.id == *id)
		return 0;
	return 1;
}

#define MAX_MAP 1024
static int map_first = 0;
static int map_last = 0;

static int map_timestamp[MAX_MAP];
static lng map_log_saved_id[MAX_MAP];

static void
map_add(int ts, lng saved_id)
{
	if (map_first != map_last) {
		int p = map_last-1;
		if (p < 0)
			p = MAX_MAP-1;
		if (map_log_saved_id[p] == saved_id) {
			map_timestamp[p] = ts;
			return;
		}
	}
	map_timestamp[map_last] = ts;
	map_log_saved_id[map_last] = saved_id;
	map_last++;
	if (map_last==MAX_MAP)
		map_last = 0;
	if (map_last == map_first) {
		map_first++;
		if (map_first==MAX_MAP)
			map_first = 0;
	}
}

static int
oldest_active_tid(void)
{
	if (active_sessions && active_sessions->h) {
		sql_session *s = active_sessions->h->data;
		return s->tr->stime;
	}
	return -1;
}

static lng
map_find_oldest_saved_id(int oldest_active_ts)
{
	int i;
	lng saved_id = 0;

	if (oldest_active_ts >= 0) {
		for(i = map_first; i != map_last && map_timestamp[i] < oldest_active_ts; i=((i+1)%MAX_MAP) )
			saved_id = map_log_saved_id[i];
	} else {
		i = map_last-1;
		saved_id = map_log_saved_id[i];
	}
	if (saved_id)
		map_first = i;
	return saved_id;
}

static int stamp = 1;

static int timestamp(void) {
	return stamp++;
}

static inline bool
instore(sqlid id, sqlid maxid)
{
	if (store_oids == NULL)
		return id < maxid;
	int lo = 0, hi = nstore_oids - 1;
	if (id < store_oids[0] || id > store_oids[hi])
		return false;
	while (hi > lo) {
		int mid = (lo + hi) / 2;
		if (store_oids[mid] == id)
			return true;
		if (store_oids[mid] < id)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return store_oids[lo] == id;
}

void
key_destroy(sql_key *k)
{
	node *n;

	/* remove key from schema */
	list_remove_data(k->t->s->keys,k);
	if (k->type == ukey || k->type == pkey) {
		sql_ukey *uk = (sql_ukey *) k;
		if (uk->keys) {
			for (n = uk->keys->h; n; n= n->next) {
	       	         	sql_fkey *fk = (sql_fkey *) n->data;
				fk->rkey = NULL;
			}
			list_destroy(uk->keys);
			uk->keys = NULL;
		}
	}
	/* we need to be removed from the rkey list */
	if (k->type == fkey) {
		sql_fkey *fk = (sql_fkey *) k;

		if (fk->rkey) {
			n = list_find_name(fk->rkey->keys, fk->k.base.name);
			list_remove_node(fk->rkey->keys, n);
		}
		fk->rkey = NULL;
	}
	list_destroy(k->columns);
	k->columns = NULL;
	if ((k->type == pkey) && (k->t->pkey == (sql_ukey *) k))
		k->t->pkey = NULL;
}

void
idx_destroy(sql_idx * i)
{
	if (--(i->base.refcnt) > 0)
		return;
	if (i->po)
		idx_destroy(i->po);
	/* remove idx from schema */
	list_remove_data(i->t->s->idxs, i);
	list_destroy(i->columns);
	i->columns = NULL;
	if (isTable(i->t))
		store_funcs.destroy_idx(NULL, i);
}

static void
trigger_destroy(sql_trigger *tr)
{
	/* remove trigger from schema */
	list_remove_data(tr->t->s->triggers, tr);
	if (tr->columns) {
		list_destroy(tr->columns);
		tr->columns = NULL;
	}
}

void
column_destroy(sql_column *c)
{
	if (--(c->base.refcnt) > 0)
		return;
	if (c->po)
		column_destroy(c->po);
	if (isTable(c->t))
		store_funcs.destroy_col(NULL, c);
}

void
table_destroy(sql_table *t)
{
	if (--(t->base.refcnt) > 0)
		return;
	if (t->members) {
		list_destroy(t->members);
		t->members = NULL;
	}
	cs_destroy(&t->keys);
	cs_destroy(&t->idxs);
	cs_destroy(&t->triggers);
	cs_destroy(&t->columns);
	if (t->po)
		table_destroy(t->po);
	if (isTable(t))
		store_funcs.destroy_del(NULL, t);
}

static void
table_cleanup(sql_table *t)
{
	if (t->keys.dset) {
		list_destroy(t->keys.dset);
		t->keys.dset = NULL;
	}
	if (t->idxs.dset) {
		list_destroy(t->idxs.dset);
		t->idxs.dset = NULL;
	}
	if (t->triggers.dset) {
		list_destroy(t->triggers.dset);
		t->triggers.dset = NULL;
	}
	if (t->columns.dset) {
		list_destroy(t->columns.dset);
		t->columns.dset = NULL;
	}
}

static void
table_reset_parent(sql_table *t, sql_trans *tr)
{
	sql_table *p = t->po;
	int istmp = isTempSchema(t->s) && t->base.allocated;

	if (isTable(t) && !istmp)
		store_funcs.bind_del_data(tr, t);
	t->po = NULL;
	if (t->idxs.set) {
		for(node *n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;

			if (isTable(i->t) && idx_has_column(i->type) && !istmp)
				store_funcs.bind_idx_data(tr, i);
			if (i->po)
				idx_destroy(i->po);
			i->po = NULL;
			i->base.wtime = 1;
		}
	}
	assert(t->idxs.dset == NULL);
	if (t->columns.set) {
		for(node *n = t->columns.set->h; n; n = n->next) {
			sql_column *c = n->data;

			if (isTable(c->t) && !istmp)
				store_funcs.bind_col_data(tr, c);
			if (c->po)
				column_destroy(c->po);
			c->po = NULL;
			c->base.wtime = 1;
		}
	}
	assert(t->columns.dset == NULL);
	if (p)
		table_destroy(p);
	if (isTable(t)) {
		assert(t->base.allocated);
	    t->base.wtime = 1;
	}
}

void
schema_destroy(sql_schema *s)
{
	cs_destroy(&s->parts);
	cs_destroy(&s->tables);
	cs_destroy(&s->funcs);
	cs_destroy(&s->types);
	list_destroy(s->keys);
	list_destroy(s->idxs);
	list_destroy(s->triggers);
	s->keys = NULL;
	s->idxs = NULL;
	s->triggers = NULL;
}

static void
schema_cleanup(sql_schema *s)
{
	if (s->tables.set)
		for (node *n = s->tables.set->h; n; n = n->next)
			table_cleanup(n->data);
	if (s->tables.dset) {
		list_destroy(s->tables.dset);
		s->tables.dset = NULL;
	}
	if (s->funcs.dset) {
		list_destroy(s->funcs.dset);
		s->funcs.dset = NULL;
	}
	if (s->types.dset) {
		list_destroy(s->types.dset);
		s->types.dset = NULL;
	}
	if (s->parts.dset) {
		list_destroy(s->parts.dset);
		s->parts.dset = NULL;
	}
}

static void
schema_reset_parent(sql_schema *s, sql_trans *tr)
{
	if (s->tables.set)
		for (node *n = s->tables.set->h; n; n = n->next)
			table_reset_parent(n->data, tr);
	assert(s->tables.dset == NULL);
}

static void
trans_drop_tmp(sql_trans *tr)
{
	sql_schema *tmp;

	if (!tr)
		return;

	tmp = find_sql_schema(tr, "tmp");

	if (tmp->tables.set) {
		node *n;
		for (n = tmp->tables.set->h; n; ) {
			node *nxt = n->next;
			sql_table *t = n->data;

			if (t->persistence == SQL_LOCAL_TEMP)
				cs_remove_node(&tmp->tables, n);
			n = nxt;
		}
	}
}

sql_trans *
sql_trans_destroy(sql_trans *t, bool try_spare)
{
	sql_trans *res = t->parent;

	TRC_DEBUG(SQL_STORE, "Destroy transaction: %p\n", t);

	if (t->sa->nr > 2*new_trans_size)
		try_spare = false;
	if (res == gtrans && spares < ((GDKdebug & FORCEMITOMASK) ? 0 : MAX_SPARES) && !t->name && try_spare) {
		TRC_DEBUG(SQL_STORE, "Spared '%d' transactions '%p'\n", spares, t);
		trans_drop_tmp(t);
		spare_trans[spares++] = t;
		return res;
	}

	if (t->name)
		t->name = NULL;

	cs_destroy(&t->schemas);
	sa_destroy(t->sa);
	_DELETE(t);
	(void) ATOMIC_DEC(&transactions);
	return res;
}

static void
trans_cleanup(sql_trans *t)
{
	for (node *m = t->schemas.set->h; m; m = m->next)
		schema_cleanup(m->data);
	if (t->schemas.dset) {
		list_destroy(t->schemas.dset);
		t->schemas.dset = NULL;
	}
}

static void
trans_reset_parent(sql_trans *t)
{
	for (node *m = t->schemas.set->h; m; m = m->next)
		schema_reset_parent(m->data, t);
	t->parent = NULL;
}


static void
destroy_spare_transactions(void)
{
	int i, s = spares;

	spares = (GDKdebug & FORCEMITOMASK)? 2 : MAX_SPARES; /* ie now there not spared anymore */
	for (i = 0; i < s; i++) {
		sql_trans_destroy(spare_trans[i], false);
	}
	spares = 0;
}

static int
tr_flag(sql_base * b, int flags)
{
	if (!newFlagSet(flags))
		return flags;
	return b->flags;
}

static void
load_keycolumn(sql_trans *tr, sql_key *k, oid rid)
{
	void *v;
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");

	v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	kc->c = find_sql_column(k->t, v); 	_DELETE(v);
	list_append(k->columns, kc);
	assert(kc->c);
}

static void *
find_key( sql_trans *tr, sql_table *t, sqlid rkey)
{
	node *n, *m;

	if ((n = list_find(t->s->keys, &rkey, (fcmp) &key_cmp))){
		return n->data;
	}
	for (n = tr->schemas.set->h; n; n = n->next) {
		sql_schema *s = n->data;

		if ((m = list_find(s->keys, &rkey, (fcmp) &key_cmp))){
			return m->data;
		}
	}
	return NULL;
}

static sql_key *
load_key(sql_trans *tr, sql_table *t, oid rid)
{
	void *v;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *keys = find_sql_table(syss, "keys");
	sql_table *objects = find_sql_table(syss, "objects");
	sql_column *kc_id, *kc_nr;
	key_type ktype;
	node *n;
	rids *rs;
	sqlid kid;
	oid r = oid_nil;

	v = table_funcs.column_find_value(tr, find_sql_column(keys, "type"), rid);
 	ktype = (key_type) *(int *)v;		_DELETE(v);
	nk = (ktype != fkey)?(sql_key*)SA_ZNEW(tr->sa, sql_ukey):(sql_key*)SA_ZNEW(tr->sa, sql_fkey);
	v = table_funcs.column_find_value(tr, find_sql_column(keys, "id"), rid);
 	kid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(keys, "name"), rid);
	base_init(tr->sa, &nk->base, kid, 0, v);	_DELETE(v);
	nk->type = ktype;
	nk->columns = list_new(tr->sa, (fdestroy) NULL);
	nk->t = t;

	if (ktype == ukey || ktype == pkey) {
		sql_ukey *uk = (sql_ukey *) nk;

		uk->keys = NULL;

		if (ktype == pkey)
			t->pkey = uk;
	} else {
		sql_fkey *fk = (sql_fkey *) nk;
		int action;

		v = table_funcs.column_find_value(tr, find_sql_column(keys, "action"), rid);
		action = *(int *)v;		_DELETE(v);
		fk->rkey = NULL;
		fk->on_delete = action & 255;
		fk->on_update = (action>>8) & 255;
	}

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = table_funcs.rids_select(tr, kc_id, &nk->base.id, &nk->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, kc_nr);
	for (r = table_funcs.rids_next(rs); !is_oid_nil(r); r = table_funcs.rids_next(rs))
		load_keycolumn(tr, nk, r);
	table_funcs.rids_destroy(rs);

	/* find idx with same name */
	n = list_find_name(nk->t->s->idxs, nk->base.name);
	if (n) {
		nk->idx = (sql_idx *) n->data;
		nk->idx->key = nk;
	}

	if (ktype == fkey) {
		sql_fkey *fk = (sql_fkey *) nk;
		sqlid rkey;
		sql_ukey *uk = NULL;

		v = table_funcs.column_find_value(tr, find_sql_column(keys, "rkey"), rid);
 		rkey = *(sqlid *)v; 		_DELETE(v);
		if ((uk = find_key(tr, t, rkey)) != NULL) {
			fk->rkey = uk;
			if (!uk->keys)
				uk->keys = list_new(tr->sa, NULL);
			if (!list_find(uk->keys, &fk->k.base.id, (fcmp) &key_cmp))
				list_append(uk->keys, fk);
		}
	} else {		/* could be a set of rkeys */
		sql_ukey *uk = (sql_ukey *) nk;
		sql_column *key_rkey = find_sql_column(keys, "rkey");

		rs = table_funcs.rids_select(tr, key_rkey, &nk->base.id, &nk->base.id, NULL);

		for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
			sqlid fkey;
			sql_fkey *fk;

			v = table_funcs.column_find_value(tr, find_sql_column(keys, "id"), rid);
			fkey = *(sqlid *)v; 	_DELETE(v);

			if ((fk = find_key(tr, t, fkey)) != NULL) {
				if (!uk->keys)
					uk->keys = list_new(tr->sa, NULL);
				if (!list_find(uk->keys, &fk->k.base.id, (fcmp) &key_cmp))
					list_append(uk->keys, fk);
				fk->rkey = uk;
			}
		}
		table_funcs.rids_destroy(rs);
	}
	return nk;
}

static void
load_idxcolumn(sql_trans *tr, sql_idx * i, oid rid)
{
	void *v;
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");

	v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	kc->c = find_sql_column(i->t, v); 	_DELETE(v);
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
	void *v;
	sql_idx *ni = SA_ZNEW(tr->sa, sql_idx);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *idxs = find_sql_table(syss, "idxs");
	sql_table *objects = find_sql_table(syss, "objects");
	sql_column *kc_id, *kc_nr;
	rids *rs;
	sqlid iid;

	v = table_funcs.column_find_value(tr, find_sql_column(idxs, "id"), rid);
	iid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(idxs, "name"), rid);
	base_init(tr->sa, &ni->base, iid, 0, v);	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(idxs, "type"), rid);
	ni->type = (idx_type) *(int*)v;		_DELETE(v);
	ni->columns = list_new(tr->sa, (fdestroy) NULL);
	ni->t = t;
	ni->key = NULL;

	if (isTable(ni->t) && idx_has_column(ni->type))
		store_funcs.create_idx(tr, ni);

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = table_funcs.rids_select(tr, kc_id, &ni->base.id, &ni->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, kc_nr);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs))
		load_idxcolumn(tr, ni, rid);
	table_funcs.rids_destroy(rs);
	return ni;
}

static void
load_triggercolumn(sql_trans *tr, sql_trigger * i, oid rid)
{
	void *v;
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");

	v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	kc->c = find_sql_column(i->t, v); 	_DELETE(v);
	list_append(i->columns, kc);
	assert(kc->c);
}

static sql_trigger *
load_trigger(sql_trans *tr, sql_table *t, oid rid)
{
	void *v;
	sql_trigger *nt = SA_ZNEW(tr->sa, sql_trigger);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *triggers = find_sql_table(syss, "triggers");
	sql_table *objects = find_sql_table(syss, "objects");
	sql_column *kc_id, *kc_nr;
	sqlid tid;
	rids *rs;

	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "id"), rid);
	tid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "name"), rid);
	base_init(tr->sa, &nt->base, tid, 0, v);	_DELETE(v);

	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "time"), rid);
	nt->time = *(sht*)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "orientation"),rid);
	nt->orientation = *(sht*)v;		_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "event"), rid);
	nt->event = *(sht*)v;			_DELETE(v);

	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "old_name"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), v) != 0)
		nt->old_name = sa_strdup(tr->sa, v);
	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "new_name"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), v) != 0)
		nt->new_name = sa_strdup(tr->sa, v);
	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "condition"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), v) != 0)
		nt->condition = sa_strdup(tr->sa, v);
	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "statement"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), v) != 0)
		nt->statement = sa_strdup(tr->sa, v);
	_DELETE(v);

	nt->t = t;
	nt->columns = list_new(tr->sa, (fdestroy) NULL);

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = table_funcs.rids_select(tr, kc_id, &nt->base.id, &nt->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, kc_nr);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs))
		load_triggercolumn(tr, nt, rid);
	table_funcs.rids_destroy(rs);
	return nt;
}

static sql_column *
load_column(sql_trans *tr, sql_table *t, oid rid)
{
	void *v;
	char *def, *tpe, *st;
	int sz, d;
	sql_column *c = SA_ZNEW(tr->sa, sql_column);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *columns = find_sql_table(syss, "_columns");
	sqlid cid;

	v = table_funcs.column_find_value(tr, find_sql_column(columns, "id"), rid);
	cid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "name"), rid);
	base_init(tr->sa, &c->base, cid, 0, v);	_DELETE(v);

	tpe = table_funcs.column_find_value(tr, find_sql_column(columns, "type"), rid);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "type_digits"), rid);
	sz = *(int *)v;				_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "type_scale"), rid);
	d = *(int *)v;				_DELETE(v);
	if (!sql_find_subtype(&c->type, tpe, sz, d)) {
		sql_type *lt = sql_trans_bind_type(tr, t->s, tpe);
		if (lt == NULL) {
			TRC_ERROR(SQL_STORE, "SQL type '%s' is missing\n", tpe);
			_DELETE(tpe);
			return NULL;
		}
		sql_init_subtype(&c->type, lt, sz, d);
	}
	_DELETE(tpe);
	c->def = NULL;
	def = table_funcs.column_find_value(tr, find_sql_column(columns, "default"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), def) != 0)
		c->def = sa_strdup(tr->sa, def);
	_DELETE(def);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "null"), rid);
	c->null = *(bit *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "number"), rid);
	c->colnr = *(int *)v;			_DELETE(v);
	c->unique = 0;
	c->storage_type = NULL;
	st = table_funcs.column_find_value(tr, find_sql_column(columns, "storage"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), st) != 0)
		c->storage_type = sa_strdup(tr->sa, st);
	_DELETE(st);
	c->t = t;
	if (isTable(c->t))
		store_funcs.create_col(tr, c);
	c->sorted = sql_trans_is_sorted(tr, c);
	c->dcount = 0;
	c->base.stime = c->base.wtime = tr->wstime;
	TRC_DEBUG(SQL_STORE, "Load column: %s\n", c->base.name);
	return c;
}

static int
load_range_partition(sql_trans *tr, sql_schema *syss, sql_part *pt)
{
	sql_table *ranges = find_sql_table(syss, "range_partitions");
	oid rid;
	rids *rs;
	sql_subtype *empty = sql_bind_localtype("void");

	pt->tpe = *empty;
	rs = table_funcs.rids_select(tr, find_sql_column(ranges, "table_id"), &pt->base.id, &pt->base.id, NULL);
	if ((rid = table_funcs.rids_next(rs)) != oid_nil) {
		void *v1, *v2, *v3;
		ValRecord vmin, vmax;
		ptr ok;

		vmin = vmax = (ValRecord) {.vtype = TYPE_void,};

		v1 = table_funcs.column_find_value(tr, find_sql_column(ranges, "minimum"), rid);
		v2 = table_funcs.column_find_value(tr, find_sql_column(ranges, "maximum"), rid);
		ok = VALinit(&vmin, TYPE_str, v1);
		if (ok)
			ok = VALinit(&vmax, TYPE_str, v2);
		_DELETE(v1);
		_DELETE(v2);
		if (ok) {
			v3 = table_funcs.column_find_value(tr, find_sql_column(ranges, "with_nulls"), rid);
			pt->with_nills = *((bit*)v3);
			_DELETE(v3);

			pt->part.range.minvalue = sa_alloc(tr->sa, vmin.len);
			pt->part.range.maxvalue = sa_alloc(tr->sa, vmax.len);
			memcpy(pt->part.range.minvalue, VALget(&vmin), vmin.len);
			memcpy(pt->part.range.maxvalue, VALget(&vmax), vmax.len);
			pt->part.range.minlength = vmin.len;
			pt->part.range.maxlength = vmax.len;
		}
		VALclear(&vmin);
		VALclear(&vmax);
		if (!ok) {
			table_funcs.rids_destroy(rs);
			return -1;
		}
	}
	table_funcs.rids_destroy(rs);
	return 0;
}

static int
load_value_partition(sql_trans *tr, sql_schema *syss, sql_part *pt)
{
	sql_table *values = find_sql_table(syss, "value_partitions");
	list *vals = NULL;
	oid rid;
	rids *rs = table_funcs.rids_select(tr, find_sql_column(values, "table_id"), &pt->base.id, &pt->base.id, NULL);
	int i = 0;
	sql_subtype *empty = sql_bind_localtype("void");

	vals = list_new(tr->sa, (fdestroy) NULL);
	if (!vals) {
		table_funcs.rids_destroy(rs);
		return -1;
	}

	pt->tpe = *empty;

	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
		sql_part_value* nextv;
		ValRecord vvalue;
		ptr ok;

		vvalue = (ValRecord) {.vtype = TYPE_void,};
		void *v = table_funcs.column_find_value(tr, find_sql_column(values, "value"), rid);
		ok = VALinit(&vvalue, TYPE_str, v);
		_DELETE(v);

		if (ok) {
			if (VALisnil(&vvalue)) { /* check for null value */
				pt->with_nills = true;
			} else {
				nextv = SA_ZNEW(tr->sa, sql_part_value);
				nextv->value = sa_alloc(tr->sa, vvalue.len);
				memcpy(nextv->value, VALget(&vvalue), vvalue.len);
				nextv->length = vvalue.len;
				list_append(vals, nextv);
			}
		}
		VALclear(&vvalue);
		if (!ok) {
			table_funcs.rids_destroy(rs);
			list_destroy(vals);
			return -i - 1;
		}
		i++;
	}
	table_funcs.rids_destroy(rs);
	pt->part.values = vals;
	return 0;
}

static sql_part*
load_part(sql_trans *tr, sql_table *mt, oid rid)
{
	void *v;
	sql_part *pt = SA_ZNEW(tr->sa, sql_part);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");
	sqlid id;

	assert(isMergeTable(mt) || isReplicaTable(mt));
	v = table_funcs.column_find_value(tr, find_sql_column(objects, "nr"), rid);
	id = *(sqlid*)v; _DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	base_init(tr->sa, &pt->base, id, 0, v);	_DELETE(v);
	sql_table *member = find_sql_table_id(mt->s, pt->base.id);
	assert(member);
	pt->t = mt;
	pt->member = member;
	member->partition++;
	list_append(mt->members, pt);
	return pt;
}

void
sql_trans_update_tables(sql_trans* tr, sql_schema *s)
{
	(void)tr;
	(void)s;
}

static sql_table *
load_table(sql_trans *tr, sql_schema *s, sqlid tid, subrids *nrs)
{
	void *v;
	sql_table *t = SA_ZNEW(tr->sa, sql_table);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *tables = find_sql_table(syss, "_tables");
	sql_table *idxs = find_sql_table(syss, "idxs");
	sql_table *keys = find_sql_table(syss, "keys");
	sql_table *triggers = find_sql_table(syss, "triggers");
	sql_table *partitions = find_sql_table(syss, "table_partitions");
	char *query;
	sql_column *idx_table_id, *key_table_id, *trigger_table_id, *partitions_table_id;
	oid rid;
	sqlid pcolid = int_nil;
	void* exp = NULL;
	rids *rs;

	rid = table_funcs.column_find_row(tr, find_sql_column(tables, "id"), &tid, NULL);
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "name"), rid);
	base_init(tr->sa, &t->base, tid, 0, v);	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "query"), rid);
	t->query = NULL;
	query = (char *)v;
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), query) != 0)
		t->query = sa_strdup(tr->sa, query);
	_DELETE(query);
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "type"), rid);
	t->type = *(sht *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "system"), rid);
	t->system = *(bit *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "commit_action"),rid);
	t->commit_action = (ca_t)*(sht *)v;	_DELETE(v);
	t->persistence = SQL_PERSIST;
	if (t->commit_action)
		t->persistence = SQL_GLOBAL_TEMP;
	if (isStream(t))
		t->persistence = SQL_STREAM;
	if (isRemote(t))
		t->persistence = SQL_REMOTE;
	t->cleared = 0;
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "access"),rid);
	t->access = *(sht*)v;	_DELETE(v);
	t->base.stime = t->base.wtime = tr->wstime;

	t->pkey = NULL;
	t->s = s;
	t->sz = COLSIZE;

	cs_new(&t->columns, tr->sa, (fdestroy) &column_destroy);
	cs_new(&t->idxs, tr->sa, (fdestroy) &idx_destroy);
	cs_new(&t->keys, tr->sa, (fdestroy) &key_destroy);
	cs_new(&t->triggers, tr->sa, (fdestroy) &trigger_destroy);
	if (isMergeTable(t) || isReplicaTable(t))
		t->members = list_new(tr->sa, (fdestroy) NULL);

	if (isTable(t)) {
		if (store_funcs.create_del(tr, t) != LOG_OK) {
			TRC_DEBUG(SQL_STORE, "Load table '%s' is missing 'deletes'", t->base.name);
			t->persistence = SQL_GLOBAL_TEMP;
		}
	}

	TRC_DEBUG(SQL_STORE, "Load table: %s\n", t->base.name);

	partitions_table_id = find_sql_column(partitions, "table_id");
	rs = table_funcs.rids_select(tr, partitions_table_id, &t->base.id, &t->base.id, NULL);
	if ((rid = table_funcs.rids_next(rs)) != oid_nil) {
		v = table_funcs.column_find_value(tr, find_sql_column(partitions, "type"), rid);
		t->properties |= *(bte*)v;
		_DELETE(v);

		if (isPartitionedByColumnTable(t)) {
			v = table_funcs.column_find_value(tr, find_sql_column(partitions, "column_id"), rid);
			pcolid = *((sqlid*)v);
		} else {
			v = table_funcs.column_find_value(tr, find_sql_column(partitions, "expression"), rid);
			if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), v) == 0)
				assert(0);
			exp = sa_strdup(tr->sa, v);
		}
		_DELETE(v);
	}
	table_funcs.rids_destroy(rs);

	assert((!isRangePartitionTable(t) && !isListPartitionTable(t)) || (!exp && !is_int_nil(pcolid)) || (exp && is_int_nil(pcolid)));
	if (isPartitionedByExpressionTable(t)) {
		t->part.pexp = SA_ZNEW(tr->sa, sql_expression);
		t->part.pexp->exp = exp;
		t->part.pexp->type = *sql_bind_localtype("void"); /* initialized at initialize_sql_parts */
		t->part.pexp->cols = sa_list(tr->sa);
	}
	for (rid = table_funcs.subrids_next(nrs); !is_oid_nil(rid); rid = table_funcs.subrids_next(nrs)) {
		sql_column* next = load_column(tr, t, rid);
		if (next == NULL)
			return NULL;
		cs_add(&t->columns, next, 0);
		if (pcolid == next->base.id) {
			t->part.pcol = next;
		}
	}

	if (!isKindOfTable(t))
		return t;

	/* load idx's first as the may be needed by the keys */
	idx_table_id = find_sql_column(idxs, "table_id");
	rs = table_funcs.rids_select(tr, idx_table_id, &t->base.id, &t->base.id, NULL);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
		sql_idx *i = load_idx(tr, t, rid);

		cs_add(&t->idxs, i, 0);
		list_append(s->idxs, i);
	}
	table_funcs.rids_destroy(rs);

	key_table_id = find_sql_column(keys, "table_id");
	rs = table_funcs.rids_select(tr, key_table_id, &t->base.id, &t->base.id, NULL);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
		sql_key *k = load_key(tr, t, rid);

		cs_add(&t->keys, k, 0);
		list_append(s->keys, k);
	}
	table_funcs.rids_destroy(rs);

	trigger_table_id = find_sql_column(triggers, "table_id");
	rs = table_funcs.rids_select(tr, trigger_table_id, &t->base.id, &t->base.id,NULL);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
		sql_trigger *k = load_trigger(tr, t, rid);

		cs_add(&t->triggers, k, 0);
		list_append(s->triggers, k);
	}
	table_funcs.rids_destroy(rs);
	return t;
}

static sql_type *
load_type(sql_trans *tr, sql_schema *s, oid rid)
{
	void *v;
	sql_type *t = SA_ZNEW(tr->sa, sql_type);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *types = find_sql_table(syss, "types");
	sqlid tid;

	v = table_funcs.column_find_value(tr, find_sql_column(types, "id"), rid);
	tid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "systemname"), rid);
	base_init(tr->sa, &t->base, tid, 0, v);	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "sqlname"), rid);
	t->sqlname = (v)?sa_strdup(tr->sa, v):NULL; 	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "digits"), rid);
	t->digits = *(int *)v; 			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "scale"), rid);
	t->scale = *(int *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "radix"), rid);
	t->radix = *(int *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "eclass"), rid);
	t->eclass = (sql_class)(*(int *)v);			_DELETE(v);
	t->localtype = ATOMindex(t->base.name);
	t->bits = 0;
	t->s = s;
	return t;
}

static sql_arg *
load_arg(sql_trans *tr, sql_func * f, oid rid)
{
	void *v;
	sql_arg *a = SA_ZNEW(tr->sa, sql_arg);
	char *tpe;
	unsigned int digits, scale;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *args = find_sql_table(syss, "args");

	(void)f;
	v = table_funcs.column_find_value(tr, find_sql_column(args, "name"), rid);
	a->name = sa_strdup(tr->sa, v);	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(args, "inout"), rid);
	a->inout = *(bte *)v;	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(args, "type_digits"), rid);
	digits = *(int *)v;	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(args, "type_scale"), rid);
	scale = *(int *)v;	_DELETE(v);

	tpe = table_funcs.column_find_value(tr, find_sql_column(args, "type"), rid);
	if (!sql_find_subtype(&a->type, tpe, digits, scale)) {
		sql_type *lt = sql_trans_bind_type(tr, f->s, tpe);
		if (lt == NULL) {
			TRC_ERROR(SQL_STORE, "SQL type '%s' is missing\n", tpe);
			_DELETE(tpe);
			return NULL;
		}
		sql_init_subtype(&a->type, lt, digits, scale);
	}
	_DELETE(tpe);
	return a;
}

static sql_func *
load_func(sql_trans *tr, sql_schema *s, sqlid fid, subrids *rs)
{
	void *v;
	sql_func *t = SA_ZNEW(tr->sa, sql_func);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *funcs = find_sql_table(syss, "functions");
	oid rid;
	bool update_env;	/* hacky way to update env function */

	rid = table_funcs.column_find_row(tr, find_sql_column(funcs, "id"), &fid, NULL);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "name"), rid);
	update_env = strcmp(v, "env") == 0;
	base_init(tr->sa, &t->base, fid, 0, v); 	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "func"), rid);
	update_env = update_env && strstr(v, "EXTERNAL NAME sql.sql_environment") != NULL;
	if (update_env) {
		/* see creation of env in sql_create_env()
		 * also see upgrade code in sql_upgrades.c */
		_DELETE(v);
		v = "CREATE FUNCTION env() RETURNS TABLE( name varchar(1024), value varchar(2048)) EXTERNAL NAME inspect.\"getEnvironment\";";
	}
	t->imp = (v)?sa_strdup(tr->sa, v):NULL;	if (!update_env) _DELETE(v);
	if (update_env) {
		v = "inspect";
	} else {
		v = table_funcs.column_find_value(tr, find_sql_column(funcs, "mod"), rid);
	}
	t->mod = (v)?sa_strdup(tr->sa, v):NULL;	if (!update_env) _DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "language"), rid);
	t->lang = (sql_flang) *(int *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "type"), rid);
	t->sql = (t->lang==FUNC_LANG_SQL||t->lang==FUNC_LANG_MAL);
	t->type = (sql_ftype) *(int *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "side_effect"), rid);
	t->side_effect = *(bit *)v;		_DELETE(v);
	if (t->type==F_FILT)
		t->side_effect=FALSE;
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "varres"), rid);
	t->varres = *(bit *)v;	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "vararg"), rid);
	t->vararg = *(bit *)v;	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "system"), rid);
	t->system = *(bit *)v;	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "semantics"), rid);
	t->semantics = *(bit *)v;		_DELETE(v);
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

	t->ops = list_new(tr->sa, (fdestroy)NULL);
	if (rs) {
		for (rid = table_funcs.subrids_next(rs); !is_oid_nil(rid); rid = table_funcs.subrids_next(rs)) {
			sql_arg *a = load_arg(tr, t, rid);

			if (a == NULL)
				return NULL;
			if (a->inout == ARG_OUT) {
				if (!t->res)
					t->res = sa_list(tr->sa);
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

void
reset_functions(sql_trans *tr)
{
	node *n, *m;

	for (n = tr->schemas.set->h; n; n = n->next) {
		sql_schema *s = n->data;

		if (s->funcs.set) for (m = s->funcs.set->h; m; m = m->next) {
			sql_func *f = m->data;

			if (f->sql)
				f->sql = 1;
		}
	}
}

static sql_sequence *
load_seq(sql_trans *tr, sql_schema * s, oid rid)
{
	void *v;
	sql_sequence *seq = SA_ZNEW(tr->sa, sql_sequence);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(syss, "sequences");
	sqlid sid;

	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "id"), rid);
	sid = *(sqlid *)v; 			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "name"), rid);
	base_init(tr->sa, &seq->base, sid, 0, v); _DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "start"), rid);
	seq->start = *(lng *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "minvalue"), rid);
	seq->minvalue = *(lng *)v;		_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "maxvalue"), rid);
	seq->maxvalue = *(lng *)v; 		_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "increment"), rid);
	seq->increment = *(lng *)v; 		_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "cacheinc"), rid);
	seq->cacheinc = *(lng *)v;		_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "cycle"), rid);
	seq->cycle = *(bit *)v;			_DELETE(v);
	seq->s = s;
	return seq;
}

static void
sql_trans_update_schema(sql_trans *tr, oid rid)
{
	void *v;
	sql_schema *s = NULL, *syss = find_sql_schema(tr, "sys");
	sql_table *ss = find_sql_table(syss, "schemas");
	sqlid sid;

	v = table_funcs.column_find_value(tr, find_sql_column(ss, "id"), rid);
	sid = *(sqlid *)v; 	_DELETE(v);
	s = find_sql_schema_id(tr, sid);

	if (s==NULL)
		return ;

	TRC_DEBUG(SQL_STORE, "Update schema: %s %d\n", s->base.name, s->base.id);

	v = table_funcs.column_find_value(tr, find_sql_column(ss, "name"), rid);
	base_init(tr->sa, &s->base, sid, 0, v); _DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(ss, "authorization"), rid);
	s->auth_id = *(sqlid *)v; 	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(ss, "system"), rid);
	s->system = *(bit *)v;          _DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(ss, "owner"), rid);
	s->owner = *(sqlid *)v;		_DELETE(v);
}

static void
part_destroy(sql_part *p)
{
	node *n;
	if (p->t && p->t->members && (n=list_find(p->t->members, p, (fcmp) NULL)) != NULL)
		list_remove_node(p->t->members, n);
	if (p && p->member)
		p->member->partition--;
}

static sql_schema *
load_schema(sql_trans *tr, sqlid id, oid rid)
{
	void *v;
	sql_schema *s = NULL, *syss = find_sql_schema(tr, "sys");
	sql_table *ss = find_sql_table(syss, "schemas");
	sql_table *types = find_sql_table(syss, "types");
	sql_table *tables = find_sql_table(syss, "_tables");
	sql_table *funcs = find_sql_table(syss, "functions");
	sql_table *seqs = find_sql_table(syss, "sequences");
	sqlid sid;
	sql_column *type_schema, *type_id, *table_schema, *table_id;
	sql_column *func_schema, *func_id, *seq_schema, *seq_id;
	rids *rs;

	v = table_funcs.column_find_value(tr, find_sql_column(ss, "id"), rid);
	sid = *(sqlid *)v; 	_DELETE(v);
	if (instore(sid, id)) {
		s = find_sql_schema_id(tr, sid);

		if (s==NULL) {
			char *name;

			v = table_funcs.column_find_value(tr, find_sql_column(ss, "name"), rid);
			name = (char*)v;
			s = find_sql_schema(tr, name);
			_DELETE(v);
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
		v = table_funcs.column_find_value(tr, find_sql_column(ss, "name"), rid);
		base_init(tr->sa, &s->base, sid, 0, v); _DELETE(v);
		v = table_funcs.column_find_value(tr, find_sql_column(ss, "authorization"), rid);
		s->auth_id = *(sqlid *)v; 	_DELETE(v);
		v = table_funcs.column_find_value(tr, find_sql_column(ss, "system"), rid);
		s->system = *(bit *)v;          _DELETE(v);
		v = table_funcs.column_find_value(tr, find_sql_column(ss, "owner"), rid);
		s->owner = *(sqlid *)v;		_DELETE(v);
		s->keys = list_new(tr->sa, (fdestroy) NULL);
		s->idxs = list_new(tr->sa, (fdestroy) NULL);
		s->triggers = list_new(tr->sa, (fdestroy) NULL);

		cs_new(&s->tables, tr->sa, (fdestroy) &table_destroy);
		cs_new(&s->types, tr->sa, (fdestroy) NULL);
		cs_new(&s->funcs, tr->sa, (fdestroy) NULL);
		cs_new(&s->seqs, tr->sa, (fdestroy) NULL);
		cs_new(&s->parts, tr->sa, (fdestroy) &part_destroy);
	}

	TRC_DEBUG(SQL_STORE, "Load schema: %s %d\n", s->base.name, s->base.id);

	sqlid tmpid = store_oids ? FUNC_OIDS : id;

	/* first load simple types */
	type_schema = find_sql_column(types, "schema_id");
	type_id = find_sql_column(types, "id");
	rs = table_funcs.rids_select(tr, type_schema, &s->base.id, &s->base.id, type_id, &tmpid, NULL, NULL);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs))
		cs_add(&s->types, load_type(tr, s, rid), 0);
	table_funcs.rids_destroy(rs);

	/* second tables */
	table_schema = find_sql_column(tables, "schema_id");
	table_id = find_sql_column(tables, "id");
	/* all tables with id >= id */
	rs = table_funcs.rids_select(tr, table_schema, &sid, &sid, table_id, &tmpid, NULL, NULL);
	if (rs && !table_funcs.rids_empty(rs)) {
		sql_table *columns = find_sql_table(syss, "_columns");
		sql_column *column_table_id = find_sql_column(columns, "table_id");
		sql_column *column_number = find_sql_column(columns, "number");
		subrids *nrs = table_funcs.subrids_create(tr, rs, table_id, column_table_id, column_number);
		sqlid tid;

		for (tid = table_funcs.subrids_nextid(nrs); tid >= 0; tid = table_funcs.subrids_nextid(nrs)) {
			if (!instore(tid, id)) {
				sql_table *t = load_table(tr, s, tid, nrs);
				if (t == NULL) {
					table_funcs.subrids_destroy(nrs);
					table_funcs.rids_destroy(rs);
					return NULL;
				}
				cs_add(&s->tables, t, 0);
			} else
				while (!is_oid_nil(table_funcs.subrids_next(nrs)))
					;
		}
		table_funcs.subrids_destroy(nrs);
	}
	table_funcs.rids_destroy(rs);

	/* next functions which could use these types */
	func_schema = find_sql_column(funcs, "schema_id");
	func_id = find_sql_column(funcs, "id");
	rs = table_funcs.rids_select(tr, func_schema, &s->base.id, &s->base.id, func_id, &tmpid, NULL, NULL);
	if (rs && !table_funcs.rids_empty(rs)) {
		sql_table *args = find_sql_table(syss, "args");
		sql_column *arg_func_id = find_sql_column(args, "func_id");
		sql_column *arg_number = find_sql_column(args, "number");
		subrids *nrs = table_funcs.subrids_create(tr, rs, func_id, arg_func_id, arg_number);
		sqlid fid;
		sql_func *f;

		for (fid = table_funcs.subrids_nextid(nrs); fid >= 0; fid = table_funcs.subrids_nextid(nrs)) {
			f = load_func(tr, s, fid, nrs);
			if (f == NULL) {
				table_funcs.subrids_destroy(nrs);
				table_funcs.rids_destroy(rs);
				return NULL;
			}
			cs_add(&s->funcs, f, 0);
		}
		/* Handle all procedures without arguments (no args) */
		rs = table_funcs.rids_diff(tr, rs, func_id, nrs, arg_func_id);
		for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
			void *v = table_funcs.column_find_value(tr, func_id, rid);
			fid = *(sqlid*)v; _DELETE(v);
			f = load_func(tr, s, fid, NULL);
			if (f == NULL) {
				table_funcs.subrids_destroy(nrs);
				table_funcs.rids_destroy(rs);
				return NULL;
			}
			cs_add(&s->funcs, f, 0);
		}
		table_funcs.subrids_destroy(nrs);
	}
	table_funcs.rids_destroy(rs);

	/* last sequence numbers */
	seq_schema = find_sql_column(seqs, "schema_id");
	seq_id = find_sql_column(seqs, "id");
	rs = table_funcs.rids_select(tr, seq_schema, &s->base.id, &s->base.id, seq_id, &tmpid, NULL, NULL);
	for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs))
		cs_add(&s->seqs, load_seq(tr, s, rid), 0);
	table_funcs.rids_destroy(rs);

	if (s->tables.set) {
		for (node *n = s->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;
			if (isMergeTable(t) || isReplicaTable(t)) {
				sql_table *objects = find_sql_table(syss, "objects");
				sql_column *mt_id = find_sql_column(objects, "id");
				sql_column *mt_nr = find_sql_column(objects, "nr");
				rids *rs = table_funcs.rids_select(tr, mt_id, &t->base.id, &t->base.id, NULL);

				rs = table_funcs.rids_orderby(tr, rs, mt_nr);
				for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
					sql_part *pt = load_part(tr, t, rid);
					if (isRangePartitionTable(t)) {
						load_range_partition(tr, syss, pt);
					} else if (isListPartitionTable(t)) {
						load_value_partition(tr, syss, pt);
					}
					cs_add(&s->parts, pt, 0);
				}
				table_funcs.rids_destroy(rs);
			}
		}
	}
	return s;
}

static sql_trans *
create_trans(sql_allocator *sa)
{
	sql_trans *t = ZNEW(sql_trans);

	if (!t)
		return NULL;

	t->sa = sa;
	t->name = NULL;
	t->wtime = 0;
	t->stime = 0;
	t->wstime = timestamp();
	t->schema_updates = 0;
	t->status = 0;

	t->parent = NULL;

	cs_new(&t->schemas, t->sa, (fdestroy) &schema_destroy);
	return t;
}

void
sql_trans_update_schemas(sql_trans* tr)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(syss, "schemas");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	rids *schemas = table_funcs.rids_select(tr, sysschema_ids, NULL, NULL);
	oid rid;

	TRC_DEBUG(SQL_STORE, "Update schemas\n");

	for (rid = table_funcs.rids_next(schemas); !is_oid_nil(rid); rid = table_funcs.rids_next(schemas)) {
		sql_trans_update_schema(tr, rid);
	}
	table_funcs.rids_destroy(schemas);
}

static bool
load_trans(sql_trans* tr, sqlid id)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(syss, "schemas");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	rids *schemas = table_funcs.rids_select(tr, sysschema_ids, NULL, NULL);
	oid rid;

	TRC_DEBUG(SQL_STORE, "Load transaction\n");

	for (rid = table_funcs.rids_next(schemas); !is_oid_nil(rid); rid = table_funcs.rids_next(schemas)) {
		sql_schema *ns = load_schema(tr, id, rid);
		if (ns == NULL)
			return false;
		if (!instore(ns->base.id, id))
			cs_add(&tr->schemas, ns, 0);
	}
	table_funcs.rids_destroy(schemas);
	return true;
}

static int
store_upgrade_ids(sql_trans* tr)
{
	node *n, *m, *o;
	for (n = tr->schemas.set->h; n; n = n->next) {
		sql_schema *s = n->data;

		if (isDeclaredSchema(s))
			continue;
		if (s->tables.set == NULL)
			continue;
		for (m = s->tables.set->h; m; m = m->next) {
			sql_table *t = m->data;

			if (!isTable(t))
				continue;
			if (store_funcs.upgrade_del(t) != LOG_OK)
				return SQL_ERR;
			for (o = t->columns.set->h; o; o = o->next) {
				sql_column *c = o->data;

				if (store_funcs.upgrade_col(c) != LOG_OK)
					return SQL_ERR;
			}
			if (t->idxs.set == NULL)
				continue;
			for (o = t->idxs.set->h; o; o = o->next) {
				sql_idx *i = o->data;

				if (store_funcs.upgrade_idx(i) != LOG_OK)
					return SQL_ERR;
			}
		}
	}
	store_apply_deltas(true);
	logger_funcs.with_ids();
	return SQL_OK;
}

static sqlid
next_oid(void)
{
	sqlid id = 0;
	MT_lock_set(&bs_lock);
	id = store_oid++;
	MT_lock_unset(&bs_lock);
	return id;
}

sqlid
store_next_oid(void)
{
	return next_oid();
}

static void
insert_schemas(sql_trans *tr)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(syss, "schemas");
	sql_table *systable = find_sql_table(syss, "_tables");
	sql_table *syscolumn = find_sql_table(syss, "_columns");
	node *n, *m, *o;

	for (n = tr->schemas.set->h; n; n = n->next) {
		sql_schema *s = n->data;

		if (isDeclaredSchema(s))
			continue;
		table_funcs.table_insert(tr, sysschema, &s->base.id, s->base.name, &s->auth_id, &s->owner, &s->system);
		for (m = s->tables.set->h; m; m = m->next) {
			sql_table *t = m->data;
			sht ca = t->commit_action;

			table_funcs.table_insert(tr, systable, &t->base.id, t->base.name, &s->base.id, ATOMnilptr(TYPE_str), &t->type, &t->system, &ca, &t->access);
			for (o = t->columns.set->h; o; o = o->next) {
				sql_column *c = o->data;

				table_funcs.table_insert(tr, syscolumn, &c->base.id, c->base.name, c->type.type->sqlname, &c->type.digits, &c->type.scale, &t->base.id, (c->def) ? c->def : ATOMnilptr(TYPE_str), &c->null, &c->colnr, (c->storage_type)? c->storage_type : ATOMnilptr(TYPE_str));
			}
		}
	}
}

static void
insert_types(sql_trans *tr, sql_table *systype)
{
	for (node *n = types->h; n; n = n->next) {
		sql_type *t = n->data;
		int radix = t->radix, eclass = (int) t->eclass;
		sqlid next_schema = t->s ? t->s->base.id : 0;

		table_funcs.table_insert(tr, systype, &t->base.id, t->base.name, t->sqlname, &t->digits, &t->scale, &radix, &eclass, &next_schema);
	}
}

static void
insert_args(sql_trans *tr, sql_table *sysarg, list *args, sqlid funcid, const char *arg_def, int *number)
{
	for (node *n = args->h; n; n = n->next) {
		sql_arg *a = n->data;
		sqlid id = next_oid();
		int next_number = (*number)++;
		char buf[32], *next_name;

		if (a->name) {
			next_name = a->name;
		} else {
			snprintf(buf, sizeof(buf), arg_def, next_number);
			next_name = buf;
		}
		table_funcs.table_insert(tr, sysarg, &id, &funcid, next_name, a->type.type->sqlname, &a->type.digits, &a->type.scale, &a->inout, &next_number);
	}
}

static void
insert_functions(sql_trans *tr, sql_table *sysfunc, list *funcs_list, sql_table *sysarg)
{
	for (node *n = funcs_list->h; n; n = n->next) {
		sql_func *f = n->data;
		bit se = (f->type == F_AGGR) ? FALSE : f->side_effect;
		int number = 0, ftype = (int) f->type, flang = (int) FUNC_LANG_INT;
		sqlid next_schema = f->s ? f->s->base.id : 0;

		table_funcs.table_insert(tr, sysfunc, &f->base.id, f->base.name, f->imp, f->mod, &flang, &ftype, &se, &f->varres, &f->vararg, &next_schema, &f->system, &f->semantics);
		if (f->res)
			insert_args(tr, sysarg, f->res, f->base.id, "res_%d", &number);
		if (f->ops)
			insert_args(tr, sysarg, f->ops, f->base.id, "arg_%d", &number);
	}
}

static int
table_next_column_nr(sql_table *t)
{
	int nr = cs_size(&t->columns);
	if (nr) {
		node *n = cs_last_node(&t->columns);
		if (n) {
			sql_column *c = n->data;

			nr = c->colnr+1;
		}
	}
	return nr;
}

static sql_column *
bootstrap_create_column(sql_trans *tr, sql_table *t, char *name, char *sqltype, unsigned int digits)
{
	sql_column *col = SA_ZNEW(tr->sa, sql_column);

	TRC_DEBUG(SQL_STORE, "Create column: %s\n", name);

	if (store_oids) {
		sqlid *idp = logger_funcs.log_find_table_value("sys__columns_id", "sys__columns_name", name, "sys__columns_table_id", &t->base.id, NULL, NULL);
		base_init(tr->sa, &col->base, *idp, t->base.flags, name);
		store_oids[nstore_oids++] = *idp;
		GDKfree(idp);
	} else {
		base_init(tr->sa, &col->base, next_oid(), t->base.flags, name);
	}
	sql_find_subtype(&col->type, sqltype, digits, 0);
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;
	cs_add(&t->columns, col, TR_NEW);
	col->base.wtime = col->t->base.wtime = col->t->s->base.wtime = tr->wtime = tr->wstime;

	if (isTable(col->t))
		store_funcs.create_col(tr, col);
	tr->schema_updates ++;
	return col;
}

static sql_table *
create_sql_table_with_id(sql_allocator *sa, sqlid id, const char *name, sht type, bit system, int persistence, int commit_action, bte properties)
{
	sql_table *t = SA_ZNEW(sa, sql_table);

	assert(sa);
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
	cs_new(&t->columns, sa, (fdestroy) &column_destroy);
	cs_new(&t->idxs, sa, (fdestroy) &idx_destroy);
	cs_new(&t->keys, sa, (fdestroy) &key_destroy);
	cs_new(&t->triggers, sa, (fdestroy) &trigger_destroy);
	if (isMergeTable(t) || isReplicaTable(t))
		t->members = list_new(sa, (fdestroy) NULL);
	t->pkey = NULL;
	t->sz = COLSIZE;
	t->cleared = 0;
	t->s = NULL;
	t->properties = properties;
	memset(&t->part, 0, sizeof(t->part));
	return t;
}

sql_table *
create_sql_table(sql_allocator *sa, const char *name, sht type, bit system, int persistence, int commit_action, bte properties)
{
	return create_sql_table_with_id(sa, next_oid(), name, type, system, persistence, commit_action, properties);
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
			lt = find_sql_type(s, nc->type->base.name);
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
dup_sql_column(sql_allocator *sa, sql_table *t, sql_column *c)
{
	sql_column *col = SA_ZNEW(sa, sql_column);

	base_init(sa, &col->base, c->base.id, c->base.flags, c->base.name);
	col->type = c->type; /* Both types belong to the same transaction, so no dup_sql_type call is needed */
	col->def = NULL;
	if (c->def)
		col->def = sa_strdup(sa, c->def);
	col->null = c->null;
	col->colnr = c->colnr;
	col->t = t;
	col->unique = c->unique;
	col->storage_type = NULL;
	if (c->storage_type)
		col->storage_type = sa_strdup(sa, c->storage_type);
	col->sorted = c->sorted;
	col->dcount = c->dcount;
	cs_add(&t->columns, col, TR_NEW);
	return col;
}

static sql_part *
dup_sql_part(sql_allocator *sa, sql_table *mt, sql_part *op)
{
	sql_part *p = SA_ZNEW(sa, sql_part);

	base_init(sa, &p->base, op->base.id, op->base.flags, op->base.name);
	p->tpe = op->tpe; /* No dup_sql_type call I think */
	p->with_nills = op->with_nills;

	if (isRangePartitionTable(mt)) {
		p->part.range.minvalue = sa_alloc(sa, op->part.range.minlength);
		p->part.range.maxvalue = sa_alloc(sa, op->part.range.maxlength);
		memcpy(p->part.range.minvalue, op->part.range.minvalue, op->part.range.minlength);
		memcpy(p->part.range.maxvalue, op->part.range.maxvalue, op->part.range.maxlength);
		p->part.range.minlength = op->part.range.minlength;
		p->part.range.maxlength = op->part.range.maxlength;
	} else if (isListPartitionTable(mt)) {
		p->part.values = list_new(sa, (fdestroy) NULL);
		for (node *n = op->part.values->h ; n ; n = n->next) {
			sql_part_value *prev = (sql_part_value*) n->data, *nextv = SA_ZNEW(sa, sql_part_value);
			nextv->value = sa_alloc(sa, prev->length);
			memcpy(nextv->value, prev->value, prev->length);
			nextv->length = prev->length;
			list_append(p->part.values, nextv);
		}
	}
	list_append(mt->members, p);
	p->t = mt;
	p->member = find_sql_table_id(mt->s, op->member->base.id);
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
	nt->partition = t->partition;
	nt->query = (t->query) ? sa_strdup(sa, t->query) : NULL;
	nt->s = t->s;

	if (isPartitionedByExpressionTable(nt)) {
		nt->part.pexp = SA_ZNEW(sa, sql_expression);
		nt->part.pexp->exp = sa_strdup(sa, t->part.pexp->exp);
		nt->part.pexp->type = t->part.pexp->type; /* No dup_sql_type call needed */
		nt->part.pexp->cols = sa_list(sa);
		for (n = t->part.pexp->cols->h; n; n = n->next) {
			int *nid = sa_alloc(sa, sizeof(int));
			*nid = *(int *) n->data;
			list_append(nt->part.pexp->cols, nid);
		}
	}

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_column *dup = dup_sql_column(sa, nt, c);
		if (isPartitionedByColumnTable(nt) && c->base.id == t->part.pcol->base.id)
			nt->part.pcol = dup;
	}

	nt->columns.dset = NULL;
	nt->columns.nelm = NULL;

	if (t->members)
		for (n = t->members->h; n; n = n->next)
			dup_sql_part(sa, nt, n->data);
	return nt;
}

static sql_table *
bootstrap_create_table(sql_trans *tr, sql_schema *s, char *name)
{
	int istmp = isTempSchema(s);
	int persistence = istmp?SQL_GLOBAL_TEMP:SQL_PERSIST;
	sht commit_action = istmp?CA_PRESERVE:CA_COMMIT;
	sql_table *t;
	if (store_oids) {
		sqlid *idp = logger_funcs.log_find_table_value("sys__tables_id", "sys__tables_name", name, "sys__tables_schema_id", &s->base.id, NULL, NULL);
		t = create_sql_table_with_id(tr->sa, *idp, name, tt_table, 1, persistence, commit_action, 0);
		store_oids[nstore_oids++] = *idp;
		GDKfree(idp);
	} else {
		t = create_sql_table(tr->sa, name, tt_table, 1, persistence, commit_action, 0);
	}
	t->bootstrap = 1;

	TRC_DEBUG(SQL_STORE, "Create table: %s\n", name);

	t->base.flags = s->base.flags;
	t->query = NULL;
	t->s = s;
	t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	cs_add(&s->tables, t, TR_NEW);

	if (isTable(t))
		store_funcs.create_del(tr, t);
	tr->schema_updates ++;
	return t;
}

static sql_schema *
bootstrap_create_schema(sql_trans *tr, char *name, sqlid auth_id, int owner)
{
	sql_schema *s = SA_ZNEW(tr->sa, sql_schema);

	TRC_DEBUG(SQL_STORE, "Create schema: %s %d %d\n", name, auth_id, owner);

	if (store_oids) {
		sqlid *idp = logger_funcs.log_find_table_value("sys_schemas_id", "sys_schemas_name", name, NULL, NULL);
		if (idp == NULL && strcmp(name, dt_schema) == 0)
			base_init(tr->sa, &s->base, (sqlid) FUNC_OIDS - 1, TR_NEW, name);
		else {
			base_init(tr->sa, &s->base, *idp, TR_NEW, name);
			store_oids[nstore_oids++] = *idp;
			GDKfree(idp);
		}
	} else {
		base_init(tr->sa, &s->base, next_oid(), TR_NEW, name);
	}
	s->auth_id = auth_id;
	s->owner = owner;
	s->system = TRUE;
	cs_new(&s->tables, tr->sa, (fdestroy) &table_destroy);
	cs_new(&s->types, tr->sa, (fdestroy) NULL);
	cs_new(&s->funcs, tr->sa, (fdestroy) NULL);
	cs_new(&s->seqs, tr->sa, (fdestroy) NULL);
	cs_new(&s->parts, tr->sa, (fdestroy) &part_destroy);
	s->keys = list_new(tr->sa, (fdestroy) NULL);
	s->idxs = list_new(tr->sa, (fdestroy) NULL);
	s->triggers = list_new(tr->sa, (fdestroy) NULL);
	s->base.wtime = tr->wtime = tr->wstime;
	cs_add(&tr->schemas, s, TR_NEW);

	tr->schema_updates ++;
	return s;
}

static int
store_schema_number(void)
{
	return schema_number;
}

static int
store_load(sql_allocator *pa) {
	int first;

	sql_allocator *sa;
	sql_trans *tr;
	sql_table *t, *types, *functions, *arguments;
	sql_schema *s, *p = NULL;

	lng lng_store_oid;
	sqlid id = 0;

	store_sa = sa_create(pa);
	sa = sa_create(pa);
	if (!sa || !store_sa)
		return -1;

	first = logger_funcs.log_isnew();

	types_init(store_sa);

	/* we store some spare oids */
	store_oid = FUNC_OIDS;

	if (!sequences_init())
		return -1;
	ATOMIC_SET(&transactions, 0);
	gtrans = tr = create_trans(sa);
	gtrans->stime = timestamp();
	if (!gtrans)
		return -1;

	/* for now use malloc and free */
	active_sessions = list_create(NULL);
	passive_sessions = list_create(NULL);

	if (first) {
		/* cannot initialize database in readonly mode */
		if (store_readonly)
			return -1;
		tr = sql_trans_create(NULL, NULL, true);
		if (!tr) {
			TRC_CRITICAL(SQL_STORE, "Failed to start a transaction while loading the storage\n");
			return -1;
		}
	} else {
		if (!(store_oids = GDKzalloc(300 * sizeof(sqlid)))) { /* 150 suffices */
			TRC_CRITICAL(SQL_STORE, "Allocation failure while loading the storage\n");
			return -1;
		}
	}
	tr->active = 1;

	s = bootstrap_create_schema(tr, "sys", ROLE_SYSADMIN, USER_MONETDB);
	if (!first)
		s->base.flags = 0;

	t = bootstrap_create_table(tr, s, "schemas");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 1024);
	bootstrap_create_column(tr, t, "authorization", "int", 32);
	bootstrap_create_column(tr, t, "owner", "int", 32);
	bootstrap_create_column(tr, t, "system", "boolean", 1);

	types = t = bootstrap_create_table(tr, s, "types");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "systemname", "varchar", 256);
	bootstrap_create_column(tr, t, "sqlname", "varchar", 1024);
	bootstrap_create_column(tr, t, "digits", "int", 32);
	bootstrap_create_column(tr, t, "scale", "int", 32);
	bootstrap_create_column(tr, t, "radix", "int", 32);
	bootstrap_create_column(tr, t, "eclass", "int", 32);
	bootstrap_create_column(tr, t, "schema_id", "int", 32);

	functions = t = bootstrap_create_table(tr, s, "functions");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 256);
	bootstrap_create_column(tr, t, "func", "varchar", 8196);
	bootstrap_create_column(tr, t, "mod", "varchar", 8196);

	/* language asm=0, sql=1, R=2, C=3, J=4 */
	bootstrap_create_column(tr, t, "language", "int", 32);

	/* func, proc, aggr or filter */
	bootstrap_create_column(tr, t, "type", "int", 32);
	bootstrap_create_column(tr, t, "side_effect", "boolean", 1);
	bootstrap_create_column(tr, t, "varres", "boolean", 1);
	bootstrap_create_column(tr, t, "vararg", "boolean", 1);
	bootstrap_create_column(tr, t, "schema_id", "int", 32);
	bootstrap_create_column(tr, t, "system", "boolean", 1);
	bootstrap_create_column(tr, t, "semantics", "boolean", 1);

	arguments = t = bootstrap_create_table(tr, s, "args");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "func_id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 256);
	bootstrap_create_column(tr, t, "type", "varchar", 1024);
	bootstrap_create_column(tr, t, "type_digits", "int", 32);
	bootstrap_create_column(tr, t, "type_scale", "int", 32);
	bootstrap_create_column(tr, t, "inout", "tinyint", 8);
	bootstrap_create_column(tr, t, "number", "int", 32);

	t = bootstrap_create_table(tr, s, "sequences");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "schema_id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 256);
	bootstrap_create_column(tr, t, "start", "bigint", 64);
	bootstrap_create_column(tr, t, "minvalue", "bigint", 64);
	bootstrap_create_column(tr, t, "maxvalue", "bigint", 64);
	bootstrap_create_column(tr, t, "increment", "bigint", 64);
	bootstrap_create_column(tr, t, "cacheinc", "bigint", 64);
	bootstrap_create_column(tr, t, "cycle", "boolean", 1);

	t = bootstrap_create_table(tr, s, "table_partitions");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "table_id", "int", 32);
	bootstrap_create_column(tr, t, "column_id", "int", 32);
	bootstrap_create_column(tr, t, "expression", "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "type", "tinyint", 8);

	t = bootstrap_create_table(tr, s, "range_partitions");
	bootstrap_create_column(tr, t, "table_id", "int", 32);
	bootstrap_create_column(tr, t, "partition_id", "int", 32);
	bootstrap_create_column(tr, t, "minimum", "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "maximum", "varchar", STORAGE_MAX_VALUE_LENGTH);
	bootstrap_create_column(tr, t, "with_nulls", "boolean", 1);

	t = bootstrap_create_table(tr, s, "value_partitions");
	bootstrap_create_column(tr, t, "table_id", "int", 32);
	bootstrap_create_column(tr, t, "partition_id", "int", 32);
	bootstrap_create_column(tr, t, "value", "varchar", STORAGE_MAX_VALUE_LENGTH);

	t = bootstrap_create_table(tr, s, "dependencies");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "depend_id", "int", 32);
	bootstrap_create_column(tr, t, "depend_type", "smallint", 16);

	while(s) {
		t = bootstrap_create_table(tr, s, "_tables");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "schema_id", "int", 32);
		bootstrap_create_column(tr, t, "query", "varchar", 1 << 20);
		bootstrap_create_column(tr, t, "type", "smallint", 16);
		bootstrap_create_column(tr, t, "system", "boolean", 1);
		bootstrap_create_column(tr, t, "commit_action", "smallint", 16);
		bootstrap_create_column(tr, t, "access", "smallint", 16);

		t = bootstrap_create_table(tr, s, "_columns");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "type", "varchar", 1024);
		bootstrap_create_column(tr, t, "type_digits", "int", 32);
		bootstrap_create_column(tr, t, "type_scale", "int", 32);
		bootstrap_create_column(tr, t, "table_id", "int", 32);
		bootstrap_create_column(tr, t, "default", "varchar", STORAGE_MAX_VALUE_LENGTH);
		bootstrap_create_column(tr, t, "null", "boolean", 1);
		bootstrap_create_column(tr, t, "number", "int", 32);
		bootstrap_create_column(tr, t, "storage", "varchar", 2048);

		t = bootstrap_create_table(tr, s, "keys");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "table_id", "int", 32);
		bootstrap_create_column(tr, t, "type", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "rkey", "int", 32);
		bootstrap_create_column(tr, t, "action", "int", 32);

		t = bootstrap_create_table(tr, s, "idxs");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "table_id", "int", 32);
		bootstrap_create_column(tr, t, "type", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);

		t = bootstrap_create_table(tr, s, "triggers");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "table_id", "int", 32);
		bootstrap_create_column(tr, t, "time", "smallint", 16);
		bootstrap_create_column(tr, t, "orientation", "smallint", 16);
		bootstrap_create_column(tr, t, "event", "smallint", 16);
		bootstrap_create_column(tr, t, "old_name", "varchar", 1024);
		bootstrap_create_column(tr, t, "new_name", "varchar", 1024);
		bootstrap_create_column(tr, t, "condition", "varchar", 2048);
		bootstrap_create_column(tr, t, "statement", "varchar", 2048);

		t = bootstrap_create_table(tr, s, "objects");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "nr", "int", 32);

		if (!p) {
			p = s;
			/* now the same tables for temporaries */
			s = bootstrap_create_schema(tr, "tmp", ROLE_SYSADMIN, USER_MONETDB);
		} else {
			s = NULL;
		}
	}

	if (first) {
		insert_types(tr, types);
		insert_functions(tr, functions, funcs, arguments);
		insert_schemas(tr);

		if (sql_trans_commit(tr) != SQL_OK) {
			TRC_CRITICAL(SQL_STORE, "Cannot commit initial transaction\n");
		}
		sql_trans_destroy(tr, true);
		tr = gtrans;
	} else {
		tr->active = 0;
		GDKqsort(store_oids, NULL, NULL, nstore_oids, sizeof(sqlid), 0, TYPE_int, false, false);
		store_oid = store_oids[nstore_oids - 1] + 1;
	}

	id = store_oid; /* db objects up till id are already created */
	logger_funcs.get_sequence(OBJ_SID, &lng_store_oid);
	prev_oid = (sqlid)lng_store_oid;
	if (store_oid < prev_oid)
		store_oid = prev_oid;

	/* load remaining schemas, tables, columns etc */
	tr->active = 1;
	if (!first && !load_trans(gtrans, id)) {
		GDKfree(store_oids);
		store_oids = NULL;
		nstore_oids = 0;
		return -1;
	}
	tr->active = 0;
	store_initialized = 1;
	GDKfree(store_oids);
	store_oids = NULL;
	nstore_oids = 0;
	if (logger_funcs.log_needs_update())
		if (store_upgrade_ids(gtrans) != SQL_OK)
			TRC_CRITICAL(SQL_STORE, "Cannot commit upgrade transaction\n");
	return first;
}

int
store_init(sql_allocator *pa, int debug, store_type store, int readonly, int singleuser)
{
	int v = 1;

	store_readonly = readonly;
	store_singleuser = singleuser;
	store_debug = debug;

	MT_lock_set(&bs_lock);

	/* initialize empty bats */
	switch (store) {
	case store_bat:
	case store_mem:
		if (bat_utils_init() == -1) {
			MT_lock_unset(&bs_lock);
			return -1;
		}
		bat_storage_init(&store_funcs);
		bat_table_init(&table_funcs);
		bat_logger_init(&logger_funcs);
		break;
	default:
		break;
	}
	active_store_type = store;
	if (!logger_funcs.create ||
	    logger_funcs.create(debug, "sql_logs", CATALOG_VERSION*v) != LOG_OK) {
		MT_lock_unset(&bs_lock);
		return -1;
	}

	/* create the initial store structure or re-load previous data */
	MT_lock_unset(&bs_lock);
	return store_load(pa);
}

static int
store_needs_vacuum( sql_trans *tr )
{
	size_t max_dels = GDKdebug & FORCEMITOMASK ? 1 : 128;
	sql_schema *s = find_sql_schema(tr, "sys");
	node *n;

	for ( n = s->tables.set->h; n; n = n->next) {
		sql_table *t = n->data;
		sql_column *c = t->columns.set->h->data;

		if (!t->system)
			continue;
		/* no inserts, updates and enough deletes ? */
		if (store_funcs.count_col(tr, c, 0) == 0 &&
		    store_funcs.count_upd(tr, t) == 0 &&
		    store_funcs.count_del(tr, t) >= max_dels)
			return 1;
	}
	return 0;
}

static int
store_vacuum( sql_trans *tr )
{
	/* tables */
	size_t max_dels = GDKdebug & FORCEMITOMASK ? 1 : 128;
	sql_schema *s = find_sql_schema(tr, "sys");
	node *n;

	for ( n = s->tables.set->h; n; n = n->next) {
		sql_table *t = n->data;
		sql_column *c = t->columns.set->h->data;

		if (!t->system)
			continue;
		if (store_funcs.count_col(tr, c, 0) == 0 &&
		    store_funcs.count_upd(tr, t) == 0 &&
		    store_funcs.count_del(tr, t) >= max_dels)
			if (table_funcs.table_vacuum(tr, t) != SQL_OK)
				return -1;
	}
	return 0;
}

// All this must only be accessed while holding the bs_lock.
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

/* Determine whether this is a good moment to flush the log.
 * Note: this function clears flusher.flush_now if it was set,
 * so if it returns true you must either flush the log or
 * set flush_log to true again, otherwise the request will
 * be lost.
 *
 * This is done this way because flush_now can be set at any time
 * without first obtaining bs_lock. To avoid time-of-check-to-time-of-use
 * issues, this function both checks and clears the flag.
 */
static bool
flusher_should_run(void)
{
	// We will flush if we have a reason to and no reason not to.
	char *reason_to = NULL, *reason_not_to = NULL;
	int changes;

	if (logger_funcs.changes() >= 1000000)
		ATOMIC_SET(&flusher.flush_now, 1);

	if (flusher.countdown_ms <= 0)
		reason_to = "timer expired";

	int many_changes = GDKdebug & FORCEMITOMASK ? 100 : 100000;
	if ((changes = logger_funcs.changes()) >= many_changes)
		reason_to = "many changes";
	else if (changes == 0)
		reason_not_to = "no changes";

	// Read and clear flush_now. If we decide not to flush
	// we'll put it back.
	bool my_flush_now = (bool) ATOMIC_XCG(&flusher.flush_now, 0);
	if (my_flush_now) {
		reason_to = "user request";
		reason_not_to = NULL;
	}

	if (ATOMIC_GET(&store_nr_active) > 0)
		reason_not_to = "awaiting idle time";

	if (!flusher.enabled && !my_flush_now)
		reason_not_to = "disabled";

	bool do_it = (reason_to && !reason_not_to);

	TRC_DEBUG_IF(SQL_STORE)
	{
		if (reason_to != flusher.reason_to || reason_not_to != flusher.reason_not_to) {
			TRC_DEBUG_ENDIF(SQL_STORE, "Store flusher: %s, reason to flush: %s, reason not to: %s\n",
										do_it ? "flushing" : "not flushing",
										reason_to ? reason_to : "none",
										reason_not_to ? reason_not_to : "none");
		}
	}

	flusher.reason_to = reason_to;
	flusher.reason_not_to = reason_not_to;

	// Remember the request for next time.
	if (!do_it && my_flush_now)
		ATOMIC_SET(&flusher.flush_now, 1);

	return do_it;
}

void
store_exit(void)
{
	MT_lock_set(&bs_lock);

	TRC_DEBUG(SQL_STORE, "Store locked\n");

	/* busy wait till the logmanager is ready */
	while (flusher.working) {
		MT_lock_unset(&bs_lock);
		MT_sleep_ms(100);
		MT_lock_set(&bs_lock);
	}

	if (gtrans) {
		MT_lock_unset(&bs_lock);
		sequences_exit();
		MT_lock_set(&bs_lock);
	}
	if (spares > 0)
		destroy_spare_transactions();

	logger_funcs.destroy();

	/* Open transactions have a link to the global transaction therefore
	   we need busy waiting until all transactions have ended or
	   (current implementation) simply keep the gtrans alive and simply
	   exit (but leak memory).
	 */
	if (!ATOMIC_GET(&transactions)) {
		sql_trans_destroy(gtrans, false);
		gtrans = NULL;
	}
	list_destroy(active_sessions);
	list_destroy(passive_sessions);
	if (store_sa)
		sa_destroy(store_sa);

	TRC_DEBUG(SQL_STORE, "Store unlocked\n");
	MT_lock_unset(&bs_lock);
	store_initialized=0;
}

static void
cleanup_table(sql_table *t)
{
	for (node *n = passive_sessions->h; n; n=n->next) {
		sql_session *s = n->data;

		for (node *m = s->tr->schemas.set->h; m; m = m->next) {
			sql_schema * schema = m->data;
			node *o = find_sql_table_node(schema, t->base.id);
			if (o) {
				list_remove_node(schema->tables.set, o);
				break;
			}
		}
	}
	if (spares) {
		for (int i = 0; i<spares; i++) {
			for (node *m = spare_trans[i]->schemas.set->h; m; m = m->next) {
				sql_schema * schema = m->data;

				if (schema->tables.dset) {
					list_destroy(schema->tables.dset);
					schema->tables.dset = NULL;
				}
				node *o = find_sql_table_node(schema, t->base.id);
				if (o) {
					list_remove_node(schema->tables.set, o);
					break;
				}
			}
		}
	}
}

static sql_trans * trans_init(sql_trans *tr, sql_trans *otr);

/* call locked! */
int
store_apply_deltas(bool not_locked)
{
	int res = LOG_OK;

	flusher.working = true;
	/* make sure we reset all transactions on re-activation */
	gtrans->wstime = timestamp();
	/* cleanup drop tables, columns and idxs first */
	if (/* DISABLES CODE */ (0))
	trans_cleanup(gtrans);

	int tid = oldest_active_tid();
	lng lid = map_find_oldest_saved_id(tid);
	if (lid) {
		for (node *m = gtrans->schemas.set->h; m; m = m->next) {
			sql_schema *s = m->data;
			if (s->tables.dset) {
				for (node *o, *n = s->tables.dset->h; n; n = o) {
					o = n->next;
					sql_table *b = n->data;
					if (b->base.wtime < tid || tid < 0) { /* deleted before the oldest transaction, time to remove */
						if (b->base.refcnt > 1)
							cleanup_table(b);
						assert(b->base.refcnt == 1);
						list_remove_node(s->tables.dset, n);
					}
				}
			}
		}
	}

	if (store_funcs.gtrans_update)
		store_funcs.gtrans_update(gtrans);
	res = logger_funcs.flush(lid);
	if (res == LOG_OK) {
		if (!not_locked)
			MT_lock_unset(&bs_lock);
		res = logger_funcs.cleanup();
		if (!not_locked)
			MT_lock_set(&bs_lock);
	}
	if (/* DISABLES CODE */ (0) && /*gtrans->sa->nr > 2*new_trans_size &&*/ !(ATOMIC_GET(&nr_sessions)) /* only save when there are no dependencies on the gtrans */) {
		sql_trans *ntrans = sql_trans_create(gtrans, NULL, false);

		trans_init(ntrans, gtrans);
		if (spares > 0)
			destroy_spare_transactions();
		trans_reset_parent(ntrans);

		sql_trans_destroy(gtrans, false);
		gtrans = ntrans;
	}
	flusher.working = false;

	return res;
}

void
store_flush_log(void)
{
	if (logger_funcs.changes() >= 1000000)
		ATOMIC_SET(&flusher.flush_now, 1);
}

/* Call while holding bs_lock */
static void
wait_until_flusher_idle(void)
{
	while (flusher.working) {
		const int sleeptime = 100;
		MT_lock_unset(&bs_lock);
		MT_sleep_ms(sleeptime);
		MT_lock_set(&bs_lock);
	}
}
void
store_suspend_log(void)
{
	MT_lock_set(&bs_lock);
	flusher.enabled = false;
	wait_until_flusher_idle();
	MT_lock_unset(&bs_lock);
}

void
store_resume_log(void)
{
	MT_lock_set(&bs_lock);
	flusher.enabled = true;
	MT_lock_unset(&bs_lock);
}

void
store_manager(void)
{
	MT_thread_setworking("sleeping");

	// In the main loop we always hold the lock except when sleeping
	MT_lock_set(&bs_lock);

	for (;;) {
		int res;

		if (!flusher_should_run()) {
			if (GDKexiting())
				break;
			const int sleeptime = 100;
			MT_lock_unset(&bs_lock);
			MT_sleep_ms(sleeptime);
			flusher.countdown_ms -= sleeptime;
			MT_lock_set(&bs_lock);
			continue;
		}

		MT_thread_setworking("flushing");
		res = store_apply_deltas(false);

		if (res != LOG_OK) {
			MT_lock_unset(&bs_lock);
			GDKfatal("write-ahead logging failure, disk full?");
		}

		flusher_new_cycle();
		MT_thread_setworking("sleeping");
		TRC_DEBUG(SQL_STORE, "Store flusher done\n");
	}

	// End of loop, end of lock
	MT_lock_unset(&bs_lock);
}

void
idle_manager(void)
{
	const int sleeptime = GDKdebug & FORCEMITOMASK ? 10 : 50;
	const int timeout = GDKdebug & FORCEMITOMASK ? 50 : 5000;

	MT_thread_setworking("sleeping");
	while (!GDKexiting()) {
		sql_session *s;
		int t;

		for (t = timeout; t > 0; t -= sleeptime) {
			MT_sleep_ms(sleeptime);
			if (GDKexiting())
				return;
		}
		/* cleanup any collected intermediate storage */
		store_funcs.cleanup();
		MT_lock_set(&bs_lock);
		if (ATOMIC_GET(&store_nr_active) || GDKexiting() || !store_needs_vacuum(gtrans)) {
			MT_lock_unset(&bs_lock);
			continue;
		}

		s = sql_session_create(0);
		if (!s) {
			MT_lock_unset(&bs_lock);
			continue;
		}
		MT_thread_setworking("vacuuming");
		sql_trans_begin(s);
		if (store_vacuum( s->tr ) == 0)
			sql_trans_commit(s->tr);
		sql_trans_end(s, 1);
		sql_session_destroy(s);

		MT_lock_unset(&bs_lock);
		MT_thread_setworking("sleeping");
	}
}

void
store_lock(void)
{
	MT_lock_set(&bs_lock);
	/* tell GDK allocation functions to ignore limits */
	MT_thread_setworking("store locked");
}

void
store_unlock(void)
{
	TRC_DEBUG(SQL_STORE, "Store unlocked\n");
	/* tell GDK allocation functions to honor limits again */
	MT_thread_setworking("store unlocked");
	MT_lock_unset(&bs_lock);
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
static str pick_tmp_name(str filename)
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

extern lng
store_hot_snapshot_to_stream(stream *tar_stream)
{
	int locked = 0;
	lng result = 0;
	buffer *plan_buf = NULL;
	stream *plan_stream = NULL;
	gdk_return r;

	if (!logger_funcs.get_snapshot_files) {
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

	MT_lock_set(&bs_lock);
	locked = 1;
	wait_until_flusher_idle();
	if (GDKexiting())
		goto end;

	r = logger_funcs.get_snapshot_files(plan_stream);
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
	if (locked)
		MT_lock_unset(&bs_lock);
	if (plan_stream)
		close_stream(plan_stream);
	if (plan_buf)
		buffer_destroy(plan_buf);
	return result;
}


extern lng
store_hot_snapshot(str tarfile)
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

	if (!logger_funcs.get_snapshot_files) {
		GDKerror("backend does not support hot snapshots");
		goto end;
	}

	if (!MT_path_absolute(tarfile)) {
		GDKerror("Hot snapshot requires an absolute path");
		goto end;
	}

	if (stat(tarfile, &st) == 0) {
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

	result = store_hot_snapshot_to_stream(tar_stream);
	if (result == 0)
		goto end;

	// Now sync and atomically rename the temp file to the real file,
	// also fsync'ing the directory
	mnstr_fsync(tar_stream);
	close_stream(tar_stream);
	tar_stream = NULL;
	if (rename(tmppath, tarfile) < 0) {
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
		(void) remove(tmppath);	// Best effort, ignore the result
	GDKfree(tmppath);
	return result;
}

static sql_kc *
kc_dup_(sql_trans *tr, int flags, sql_kc *kc, sql_table *t, int copy)
{
	sql_allocator *sa = (newFlagSet(flags) && !copy)?tr->parent->sa:tr->sa;
	sql_kc *nkc = SA_ZNEW(sa, sql_kc);
	sql_column *c = find_sql_column(t, kc->c->base.name);

	assert(c);
	nkc->c = c;
	c->unique = kc->c->unique;
	return nkc;
}

static sql_kc *
kc_dup(sql_trans *tr, int flags, sql_kc *kc, sql_table *t)
{
	return kc_dup_(tr, flags, kc, t, 0);
}

static sql_key *
key_dup_(sql_trans *tr, int flags, sql_key *k, sql_table *t, int copy)
{
	sql_trans *ltr = (newFlagSet(flags) && !copy)?tr->parent:tr;
	sql_allocator *sa = ltr->sa;
	sql_key *nk = (k->type != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey)
	    : (sql_key *) SA_ZNEW(sa, sql_fkey);
	node *n;

	base_init(sa, &nk->base, k->base.id, tr_flag(&k->base, flags), k->base.name);

	nk->type = k->type;
	nk->columns = list_new(sa, (fdestroy) NULL);
	nk->t = t;
	nk->idx = NULL;

	if (k->idx) {
		node *n = list_find_name(nk->t->s->idxs, nk->base.name);

		if (n) {
			nk->idx = (sql_idx *) n->data;
			nk->idx->key = nk;
		}
	}

	if (nk->type != fkey) {
		sql_ukey *tk = (sql_ukey *) nk;

		tk->keys = NULL;

		if (nk->type == pkey)
			t->pkey = tk;
	} else {
		sql_fkey *tk = (sql_fkey *) nk;

		tk->rkey = NULL;
	}

	for (n = k->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nk->columns, kc_dup_(tr, flags, okc, t, copy));
	}

	if (nk->type == fkey) {
		sql_fkey *fk = (sql_fkey *) nk;
		sql_fkey *ok = (sql_fkey *) k;
		node *n = NULL;

		if (ok->rkey) {
			sql_schema *s;

			if ((s=find_sql_schema_id(ltr, ok->rkey->k.t->s->base.id)) == NULL)
		       		s = nk->t->s;
			n = list_find(s->keys, &ok->rkey->k.base.id, (fcmp) &key_cmp);
			if (n) {
				sql_ukey *uk = n->data;

				fk->rkey = uk;
				if (!uk->keys)
					uk->keys = list_new(sa, NULL);
				if (!list_find(uk->keys, &fk->k.base.id, (fcmp) &key_cmp))
					list_append(uk->keys, fk);
				else
					assert(0);
			}
		}
		fk->on_delete = ok->on_delete;
		fk->on_update = ok->on_update;
	} else {		/* could be a set of rkeys */
		sql_ukey *uk = (sql_ukey *) nk;
		sql_ukey *ok = (sql_ukey *) k;
		node *m;

		if (ok->keys)
			for (m = ok->keys->h; m; m = m->next) {
				sql_schema *s;
				sql_fkey *ofk = m->data;
				node *n = NULL;

				if ((s=find_sql_schema_id(ltr, ofk->k.t->s->base.id)) == NULL)
		       			s = nk->t->s;
			       	n = list_find(s->keys, &ofk->k.base.id, (fcmp) &key_cmp);
				if (n) {
					sql_fkey *fk = n->data;

					if (!uk->keys)
						uk->keys = list_new(sa, NULL);
					if (!list_find(uk->keys, &fk->k.base.id, (fcmp) &key_cmp))
						list_append(uk->keys, fk);
					fk->rkey = uk;
				}
			}
	}
	list_append(t->s->keys, nk);
	if (!copy && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(k);
	return nk;
}

static sql_key *
key_dup(sql_trans *tr, int flags, sql_key *k, sql_table *t)
{
	return key_dup_(tr, flags, k, t, 0);
}

sql_key *
sql_trans_copy_key( sql_trans *tr, sql_table *t, sql_key *k)
{
	sql_key *nk = key_dup_(tr, TR_NEW, k, t, 1);
	sql_fkey *fk = (sql_fkey*)nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(syss, "keys");
	sql_table *syskc = find_sql_table(syss, "objects");
	int neg = -1, action = -1, nr;
	node *n;

	cs_add(&t->keys, nk, TR_NEW);

	if (nk->type == fkey)
		action = (fk->on_update<<8) + fk->on_delete;

	assert( nk->type != fkey || ((sql_fkey*)nk)->rkey);
	table_funcs.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey->k.base.id : &neg, &action);

	if (nk->type == fkey)
		sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey->k.base.id, nk->base.id, FKEY_DEPENDENCY);

	for (n = nk->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *kc = n->data;

		table_funcs.table_insert(tr, syskc, &k->base.id, kc->c->base.name, &nr);

		if (nk->type == fkey)
			sql_trans_create_dependency(tr, kc->c->base.id, k->base.id, FKEY_DEPENDENCY);
		else if (nk->type == ukey)
			sql_trans_create_dependency(tr, kc->c->base.id, k->base.id, KEY_DEPENDENCY);
		else if (nk->type == pkey) {
			sql_trans_create_dependency(tr, kc->c->base.id, k->base.id, KEY_DEPENDENCY);
			sql_trans_alter_null(tr, kc->c, 0);
		}
	}

	syskey->base.wtime = syskey->s->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return nk;
}

#define obj_ref(o,n,flags) 		\
 	if (newFlagSet(flags)) { /* create new parent */		\
		o->po = n;		\
		n->base.refcnt++;	\
	} else {			\
		n->po = o;		\
		o->base.refcnt++;	\
	}

static sql_idx *
idx_dup(sql_trans *tr, int flags, sql_idx * i, sql_table *t)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_idx *ni = SA_ZNEW(sa, sql_idx);
	node *n;

	base_init(sa, &ni->base, i->base.id, tr_flag(&i->base, flags), i->base.name);

	ni->columns = list_new(sa, (fdestroy) NULL);
	obj_ref(i,ni,flags);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;

	/* Needs copy when committing (ie from tr to gtrans) and
	 * on savepoints from tr->parent to new tr */
	if (flags) {
		ni->base.allocated = i->base.allocated;
		ni->data = i->data;
		i->base.allocated = 0;
		ni->base.wtime = i->base.wtime;
		i->data = NULL;
	} else
	if ((isNew(i) && newFlagSet(flags) && tr->parent == gtrans) ||
	    (i->base.allocated && tr->parent != gtrans))
		if (isTable(ni->t))
			store_funcs.dup_idx(tr, i, ni);

	if (isNew(i) && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(i);

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(ni->columns, kc_dup(tr, flags, okc, t));
	}
	list_append(t->s->idxs, ni);
	return ni;
}

sql_idx *
sql_trans_copy_idx( sql_trans *tr, sql_table *t, sql_idx *i)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(syss, "idxs");
	sql_table *sysic = find_sql_table(syss, "objects");
	node *n;
	int nr, unique = 0;
	sql_idx *ni = SA_ZNEW(tr->sa, sql_idx);

	base_init(tr->sa, &ni->base, i->base.id, TR_NEW, i->base.name);

	ni->columns = list_new(tr->sa, (fdestroy) NULL);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;

	if (i->type == hash_idx && list_length(i->columns) == 1)
		unique = 1;
	for (n = i->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(ni->columns, ic = kc_dup_(tr, TR_NEW, okc, t, 1));
		if (ic->c->unique != (unique & !okc->c->null)) {
			ic->c->base.wtime = tr->wstime;
			okc->c->unique = ic->c->unique = (unique & (!okc->c->null));
		}

		table_funcs.table_insert(tr, sysic, &ni->base.id, ic->c->base.name, &nr);
		sysic->base.wtime = sysic->s->base.wtime = tr->wtime = tr->wstime;

		sql_trans_create_dependency(tr, ic->c->base.id, i->base.id, INDEX_DEPENDENCY);
	}
	list_append(t->s->idxs, ni);
	cs_add(&t->idxs, ni, TR_NEW);

	if (isDeclaredTable(i->t))
	if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
		if (store_funcs.create_idx(tr, ni) != LOG_OK)
			return NULL;
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, ni->base.name);
	ni->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return ni;
}

sql_trigger *
sql_trans_copy_trigger( sql_trans *tr, sql_table *t, sql_trigger *tri)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systr = find_sql_table(syss, "triggers");
	sql_table *sysic = find_sql_table(syss, "objects");
	node *n;
	int nr;
	sql_trigger *nt = SA_ZNEW(tr->sa, sql_trigger);
	const char *nilptr = ATOMnilptr(TYPE_str);

	base_init(tr->sa, &nt->base, tri->base.id, TR_NEW, tri->base.name);

	nt->columns = list_new(tr->sa, (fdestroy) NULL);
	nt->t = t;
	nt->time = tri->time;
	nt->orientation = tri->orientation;
	nt->event = tri->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (tri->old_name)
		nt->old_name = sa_strdup(tr->sa, tri->old_name);
	if (tri->new_name)
		nt->new_name = sa_strdup(tr->sa, tri->new_name);
	if (tri->condition)
		nt->condition = sa_strdup(tr->sa, tri->condition);
	nt->statement = sa_strdup(tr->sa, tri->statement);

	for (n = tri->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(nt->columns, ic = kc_dup_(tr, TR_NEW, okc, t, 1));
		table_funcs.table_insert(tr, sysic, &nt->base.id, ic->c->base.name, &nr);
		sysic->base.wtime = sysic->s->base.wtime = tr->wtime = tr->wstime;
		sql_trans_create_dependency(tr, ic->c->base.id, tri->base.id, TRIGGER_DEPENDENCY);
	}
	list_append(t->s->triggers, nt);
	cs_add(&t->triggers, nt, TR_NEW);

	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, systr, &nt->base.id, nt->base.name, &t->base.id, &nt->time, &nt->orientation,
								 &nt->event, (nt->old_name)?nt->old_name:nilptr, (nt->new_name)?nt->new_name:nilptr,
								 (nt->condition)?nt->condition:nilptr, nt->statement);
	nt->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return nt;
}

sql_part *
sql_trans_copy_part( sql_trans *tr, sql_table *t, sql_part *pt)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(syss, "objects");
	sql_part *npt = SA_ZNEW(tr->sa, sql_part);

	base_init(tr->sa, &npt->base, pt->base.id, TR_NEW, pt->base.name);

	if (isRangePartitionTable(t) || isListPartitionTable(t))
		dup_sql_type(tr, t->s, &(pt->tpe), &(npt->tpe));
	else
		npt->tpe = pt->tpe;
	npt->with_nills = pt->with_nills;
	npt->t = t;

	assert(isMergeTable(npt->t) || isReplicaTable(npt->t));
	if (isRangePartitionTable(t)) {
		npt->part.range.minvalue = sa_alloc(tr->sa, pt->part.range.minlength);
		npt->part.range.maxvalue = sa_alloc(tr->sa, pt->part.range.maxlength);
		memcpy(npt->part.range.minvalue, pt->part.range.minvalue, pt->part.range.minlength);
		memcpy(npt->part.range.maxvalue, pt->part.range.maxvalue, pt->part.range.maxlength);
		npt->part.range.minlength = pt->part.range.minlength;
		npt->part.range.maxlength = pt->part.range.maxlength;
	} else if (isListPartitionTable(t)) {
		npt->part.values = list_new(tr->sa, (fdestroy) NULL);
		for (node *n = pt->part.values->h ; n ; n = n->next) {
			sql_part_value *prev = (sql_part_value*) n->data, *nextv = SA_ZNEW(tr->sa, sql_part_value);
			nextv->value = sa_alloc(tr->sa, prev->length);
			memcpy(nextv->value, prev->value, prev->length);
			nextv->length = prev->length;
			list_append(npt->part.values, nextv);
		}
	}

	list_append(t->members, npt);

	sql_trans_create_dependency(tr, npt->base.id, t->base.id, TABLE_DEPENDENCY);
	table_funcs.table_insert(tr, sysic, &t->base.id, npt->base.name, &npt->base.id);

	npt->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return npt;
}

static sql_trigger *
trigger_dup(sql_trans *tr, int flags, sql_trigger * i, sql_table *t)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_trigger *nt = SA_ZNEW(sa, sql_trigger);

	base_init(sa, &nt->base, i->base.id, tr_flag(&i->base, flags), i->base.name);

	nt->columns = list_new(sa, (fdestroy) NULL);
	nt->t = t;
	nt->time = i->time;
	nt->orientation = i->orientation;
	nt->event = i->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (i->old_name)
		nt->old_name = sa_strdup(sa, i->old_name);
	if (i->new_name)
		nt->new_name = sa_strdup(sa, i->new_name);
	if (i->condition)
		nt->condition = sa_strdup(sa, i->condition);
	nt->statement = sa_strdup(sa, i->statement);

	for (node *n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nt->columns, kc_dup(tr, flags, okc, t));
	}
	list_append(t->s->triggers, nt);
	if (newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(i);
	return nt;
}

/* flags 0, dup from parent to new tr
 *	 TR_NEW, dup from child tr to parent
 * */
static sql_column *
column_dup(sql_trans *tr, int flags, sql_column *oc, sql_table *t)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_column *c = SA_ZNEW(sa, sql_column);

	base_init(sa, &c->base, oc->base.id, tr_flag(&oc->base, flags), oc->base.name);
	obj_ref(oc,c,flags);
	dup_sql_type((newFlagSet(flags))?tr->parent:tr, t->s, &(oc->type), &(c->type));
	c->def = NULL;
	if (oc->def)
		c->def = sa_strdup(sa, oc->def);
	c->null = oc->null;
	c->colnr = oc->colnr;
	c->unique = oc->unique;
	c->t = t;
	c->storage_type = NULL;
	if (oc->storage_type)
		c->storage_type = sa_strdup(sa, oc->storage_type);

	/* Needs copy when committing (ie from tr to gtrans) and
	 * on savepoints from tr->parent to new tr */
	if (flags) {
		c->base.allocated = oc->base.allocated;
		c->data = oc->data;
		oc->base.allocated = 0;
		c->base.wtime = oc->base.wtime;
		oc->data = NULL;
	} else
	if ((isNew(oc) && newFlagSet(flags) && tr->parent == gtrans) ||
	    (oc->base.allocated && tr->parent != gtrans))
		if (isTable(c->t))
			store_funcs.dup_col(tr, oc, c);
	if (isNew(oc) && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(oc);
	return c;
}

static sql_part *
part_dup(sql_trans *tr, int flags, sql_part *op, sql_schema *s)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_part *p = SA_ZNEW(sa, sql_part);
	sql_table *mt = find_sql_table_id(s, op->t->base.id);
	sql_table *member = find_sql_table_id(s, op->member->base.id);

	base_init(sa, &p->base, op->base.id, tr_flag(&op->base, flags), op->base.name);
	if (isRangePartitionTable(mt) || isListPartitionTable(mt))
		dup_sql_type(tr, mt->s, &(op->tpe), &(p->tpe));
	else
		p->tpe = op->tpe;
	p->with_nills = op->with_nills;
	assert(isMergeTable(mt) || isReplicaTable(mt));
	p->t = mt;
	assert(member);
	p->member = member;
	member->partition++;
	list_append(mt->members, p);
	if (newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(op);

	if (isRangePartitionTable(mt)) {
		p->part.range.minvalue = sa_alloc(sa, op->part.range.minlength);
		p->part.range.maxvalue = sa_alloc(sa, op->part.range.maxlength);
		memcpy(p->part.range.minvalue, op->part.range.minvalue, op->part.range.minlength);
		memcpy(p->part.range.maxvalue, op->part.range.maxvalue, op->part.range.maxlength);
		p->part.range.minlength = op->part.range.minlength;
		p->part.range.maxlength = op->part.range.maxlength;
	} else if (isListPartitionTable(mt)) {
		p->part.values = list_new(sa, (fdestroy) NULL);
		for (node *n = op->part.values->h ; n ; n = n->next) {
			sql_part_value *prev = (sql_part_value*) n->data, *nextv = SA_ZNEW(sa, sql_part_value);
			nextv->value = sa_alloc(sa, prev->length);
			memcpy(nextv->value, prev->value, prev->length);
			nextv->length = prev->length;
			list_append(p->part.values, nextv);
		}
	}
	return p;
}

static int
sql_trans_cname_conflict( sql_trans *tr, sql_table *t, const char *extra, const char *cname)
{
	const char *tmp;

	if (extra) {
		tmp = sa_message(tr->sa, "%s_%s", extra, cname);
	} else {
       		tmp = cname;
	}
	if (find_sql_column(t, tmp))
		return 1;
	return 0;
}

static int
sql_trans_tname_conflict( sql_trans *tr, sql_schema *s, const char *extra, const char *tname, const char *cname)
{
	char *tp;
	char *tmp;
	sql_table *t = NULL;

	if (extra) {
		tmp = sa_message(tr->sa, "%s_%s", extra, tname);
	} else {
       		tmp = sa_strdup(tr->sa, tname);
	}
	tp = tmp;
	while ((tp = strchr(tp, '_')) != NULL) {
		*tp = 0;
		t = find_sql_table(s, tmp);
		if (t && sql_trans_cname_conflict(tr, t, tp+1, cname))
			return 1;
		*tp++ = '_';
	}
       	tmp = sa_strdup(tr->sa, cname);
	tp = tmp;
	while ((tp = strchr(tp, '_')) != NULL) {
		char *ntmp;
		*tp = 0;
		ntmp = sa_message(tr->sa, "%s_%s", tname, tmp);
		t = find_sql_table(s, ntmp);
		if (t && sql_trans_cname_conflict(tr, t, NULL, tp+1))
			return 1;
		*tp++ = '_';
	}
	t = find_sql_table(s, tname);
	if (t && sql_trans_cname_conflict(tr, t, NULL, cname))
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
		char *tmp = sa_strdup(tr->sa, sname);
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
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(syss, "_columns");
	sql_column *col = SA_ZNEW(tr->sa, sql_column);

	if (t->system && sql_trans_name_conflict(tr, t->s->base.name, t->base.name, c->base.name))
		return NULL;
	base_init(tr->sa, &col->base, c->base.id, TR_NEW, c->base.name);
	dup_sql_type(tr, t->s, &(c->type), &(col->type));
	col->def = NULL;
	if (c->def)
		col->def = sa_strdup(tr->sa, c->def);
	col->null = c->null;
	col->colnr = c->colnr;
	col->unique = c->unique;
	col->t = t;
	col->storage_type = NULL;
	if (c->storage_type)
		col->storage_type = sa_strdup(tr->sa, c->storage_type);

	cs_add(&t->columns, col, TR_NEW);

	if (isDeclaredTable(c->t))
		if (isTable(t))
			if (store_funcs.create_col(tr, col) != LOG_OK)
				return NULL;
	if (!isDeclaredTable(t)) {
		table_funcs.table_insert(tr, syscolumn, &col->base.id, col->base.name, col->type.type->sqlname,
								 &col->type.digits, &col->type.scale, &t->base.id,
								 (col->def) ? col->def : ATOMnilptr(TYPE_str), &col->null, &col->colnr,
								 (col->storage_type) ? col->storage_type : ATOMnilptr(TYPE_str));
		col->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
		if (c->type.type->s) /* column depends on type */
			sql_trans_create_dependency(tr, c->type.type->base.id, col->base.id, TYPE_DEPENDENCY);
	}
	if (isGlobal(t))
		tr->schema_updates ++;
	return col;
}

static sql_table *
table_dup(sql_trans *tr, int flags, sql_table *ot, sql_schema *s)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_table *t = SA_ZNEW(sa, sql_table);
	node *n;

	base_init(sa, &t->base, ot->base.id, tr_flag(&ot->base, flags), ot->base.name);
	obj_ref(ot,t,flags);
	t->type = ot->type;
	t->system = ot->system;
	t->bootstrap = ot->bootstrap;
	t->persistence = ot->persistence;
	t->commit_action = ot->commit_action;
	t->access = ot->access;
	t->query = (ot->query) ? sa_strdup(sa, ot->query) : NULL;
	t->properties = ot->properties;

	cs_new(&t->columns, sa, (fdestroy) &column_destroy);
	cs_new(&t->keys, sa, (fdestroy) &key_destroy);
	cs_new(&t->idxs, sa, (fdestroy) &idx_destroy);
	cs_new(&t->triggers, sa, (fdestroy) &trigger_destroy);
	if (ot->members)
		t->members = list_new(sa, (fdestroy) NULL);

	t->pkey = NULL;

	/* Needs copy when committing (ie from tr to gtrans) and
	 * on savepoints from tr->parent to new tr */
	if (flags) {
		assert(t->data == NULL);
		t->base.allocated = ot->base.allocated;
		t->base.wtime = ot->base.wtime;
		t->data = ot->data;
		ot->base.allocated = 0;
		ot->data = NULL;
	} else
	if ((isNew(ot) && newFlagSet(flags) && tr->parent == gtrans) ||
	    (ot->base.allocated && tr->parent != gtrans))
		if (isTable(t))
			store_funcs.dup_del(tr, ot, t);

	t->s = s;
	t->sz = ot->sz;
	t->cleared = 0;

	if (isPartitionedByExpressionTable(ot)) {
		t->part.pexp = SA_ZNEW(sa, sql_expression);
		t->part.pexp->exp = sa_strdup(sa, ot->part.pexp->exp);
		dup_sql_type((newFlagSet(flags))?tr->parent:tr, t->s, &(ot->part.pexp->type), &(t->part.pexp->type));
		t->part.pexp->cols = sa_list(sa);
		for (n = ot->part.pexp->cols->h; n; n = n->next) {
			int *nid = sa_alloc(sa, sizeof(int));
			*nid = *(int *) n->data;
			list_append(t->part.pexp->cols, nid);
		}
	}
	if (ot->columns.set) {
		for (n = ot->columns.set->h; n; n = n->next) {
			sql_column *c = n->data, *copy = column_dup(tr, flags, c, t);

			if (isPartitionedByColumnTable(ot) && ot->part.pcol->base.id == c->base.id)
				t->part.pcol = copy;
			cs_add(&t->columns, copy, tr_flag(&c->base, flags));
		}
		if (tr->parent == gtrans)
			ot->columns.nelm = NULL;
	}
	if (ot->idxs.set) {
		for (n = ot->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;

			cs_add(&t->idxs, idx_dup(tr, flags, i, t), tr_flag(&i->base, flags));
		}
		if (tr->parent == gtrans)
			ot->idxs.nelm = NULL;
	}
	if (ot->keys.set) {
		for (n = ot->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			cs_add(&t->keys, key_dup(tr, flags, k, t), tr_flag(&k->base, flags));
		}
		if (tr->parent == gtrans)
			ot->keys.nelm = NULL;
	}
	if (ot->triggers.set) {
		for (n = ot->triggers.set->h; n; n = n->next) {
			sql_trigger *k = n->data;

			cs_add(&t->triggers, trigger_dup(tr, flags, k, t), tr_flag(&k->base, flags));
		}
		if (tr->parent == gtrans)
			ot->triggers.nelm = NULL;
	}
	if (isNew(ot) && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(ot);
	return t;
}

static sql_type *
type_dup(sql_trans *tr, int flags, sql_type *ot, sql_schema *s)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_type *t = SA_ZNEW(sa, sql_type);

	base_init(sa, &t->base, ot->base.id, tr_flag(&ot->base, flags), ot->base.name);

	t->sqlname = sa_strdup(sa, ot->sqlname);
	t->digits = ot->digits;
	t->scale = ot->scale;
	t->radix = ot->radix;
	t->eclass = ot->eclass;
	t->bits = ot->bits;
	t->localtype = ot->localtype;
	t->s = s;
	if (isNew(ot) && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(ot);
	return t;
}

static sql_arg *
arg_dup(sql_trans *tr, sql_schema *s, sql_arg *oa)
{
	sql_arg *a = SA_ZNEW(tr->sa, sql_arg);

	if (a) {
		a->name = sa_strdup(tr->sa, oa->name);
		a->inout = oa->inout;
		dup_sql_type(tr, s, &(oa->type), &(a->type));
	}
	return a;
}

static sql_func *
func_dup(sql_trans *tr, int flags, sql_func *of, sql_schema *s)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_func *f = SA_ZNEW(sa, sql_func);
	node *n;

	base_init(sa, &f->base, of->base.id, tr_flag(&of->base, flags), of->base.name);

	f->imp = (of->imp)?sa_strdup(sa, of->imp):NULL;
	f->mod = (of->mod)?sa_strdup(sa, of->mod):NULL;
	f->type = of->type;
	f->query = (of->query)?sa_strdup(sa, of->query):NULL;
	f->lang = of->lang;
	f->sql = of->sql;
	f->side_effect = of->side_effect;
	f->varres = of->varres;
	f->vararg = of->vararg;
	f->ops = list_new(sa, of->ops->destroy);
	f->fix_scale = of->fix_scale;
	f->system = of->system;
	f->semantics = of->semantics;
	for (n=of->ops->h; n; n = n->next)
		list_append(f->ops, arg_dup(newFlagSet(flags)?tr->parent:tr, s, n->data));
	if (of->res) {
		f->res = list_new(sa, of->res->destroy);
		for (n=of->res->h; n; n = n->next)
			list_append(f->res, arg_dup(newFlagSet(flags)?tr->parent:tr, s, n->data));
	}
	f->s = s;
	f->sa = sa;
	if (isNew(of) && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(of);
	return f;
}

static sql_sequence *
seq_dup(sql_trans *tr, int flags, sql_sequence *oseq, sql_schema *s)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_sequence *seq = SA_ZNEW(sa, sql_sequence);

	base_init(sa, &seq->base, oseq->base.id, tr_flag(&oseq->base, flags), oseq->base.name);

	seq->start = oseq->start;
	seq->minvalue = oseq->minvalue;
	seq->maxvalue = oseq->maxvalue;
	seq->increment = oseq->increment;
	seq->cacheinc = oseq->cacheinc;
	seq->cycle = oseq->cycle;
	seq->s = s;
	if (isNew(oseq) && newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(oseq);
	return seq;
}

static sql_schema *
schema_dup(sql_trans *tr, int flags, sql_schema *os, sql_trans *o)
{
	sql_allocator *sa = (newFlagSet(flags))?tr->parent->sa:tr->sa;
	sql_schema *s = SA_ZNEW(sa, sql_schema);
	node *n;

	(void) o;
	base_init(sa, &s->base, os->base.id, tr_flag(&os->base, flags), os->base.name);

	s->auth_id = os->auth_id;
	s->owner = os->owner;
	s->system = os->system;
	cs_new(&s->tables, sa, (fdestroy) &table_destroy);
	cs_new(&s->types, sa, (fdestroy) NULL);
	cs_new(&s->funcs, sa, (fdestroy) NULL);
	cs_new(&s->seqs, sa, (fdestroy) NULL);
	cs_new(&s->parts, sa, (fdestroy) &part_destroy);
	s->keys = list_new(sa, (fdestroy) NULL);
	s->idxs = list_new(sa, (fdestroy) NULL);
	s->triggers = list_new(sa, (fdestroy) NULL);

	if (os->types.set) {
		for (n = os->types.set->h; n; n = n->next) {
			cs_add(&s->types, type_dup(tr, flags, n->data, s), tr_flag(&os->base, flags));
		}
		if (tr->parent == gtrans)
			os->types.nelm = NULL;
	}
	if (os->tables.set) {
		for (n = os->tables.set->h; n; n = n->next) {
			sql_table *ot = n->data;

			if (ot->persistence != SQL_LOCAL_TEMP)
				cs_add(&s->tables, table_dup(tr, flags, ot, s), tr_flag(&ot->base, flags));
		}
		if (tr->parent == gtrans)
			os->tables.nelm = NULL;
	}
	if (os->funcs.set) {
		for (n = os->funcs.set->h; n; n = n->next) {
			cs_add(&s->funcs, func_dup(tr, flags, n->data, s), tr_flag(&os->base, flags));
		}
		if (tr->parent == gtrans)
			os->funcs.nelm = NULL;
	}
	if (os->seqs.set) {
		for (n = os->seqs.set->h; n; n = n->next) {
			cs_add(&s->seqs, seq_dup(tr, flags, n->data, s), tr_flag(&os->base, flags));
		}
		if (tr->parent == gtrans)
			os->seqs.nelm = NULL;
	}
	if (os->parts.set) {
		for (n = os->parts.set->h; n; n = n->next) {
			sql_part *pt = n->data;
			sql_part *dupped = part_dup(tr, flags, pt, s);

			cs_add(&s->parts, dupped, tr_flag(&pt->base, flags));
		}
		if (tr->parent == gtrans)
			os->parts.nelm = NULL;
	}
	if (newFlagSet(flags) && tr->parent == gtrans)
		removeNewFlag(os);
	return s;
}

static void
_trans_init(sql_trans *tr, sql_trans *otr)
{
	tr->wtime = 0;
	tr->stime = otr->wtime;
	tr->wstime = timestamp();
	tr->schema_updates = 0;
	tr->dropped = NULL;
	tr->status = 0;

	tr->schema_number = store_schema_number();
	tr->parent = otr;
}

static sql_trans *
trans_init(sql_trans *tr, sql_trans *otr)
{
	node *m,*n;

	_trans_init(tr, otr);

	for (m = otr->schemas.set->h, n = tr->schemas.set->h; m && n; m = m->next, n = n->next ) {
		sql_schema *ps = m->data; /* parent transactions schema */
		sql_schema *s = n->data;
		int istmp = isTempSchema(ps);

		if (s->base.id == ps->base.id) {
			node *k, *l;

			s->base.rtime = s->base.wtime = 0;
			s->base.stime = ps->base.wtime;

			if (ps->tables.set && s->tables.set)
			for (k = ps->tables.set->h, l = s->tables.set->h; k && l; l = l->next ) {
				sql_table *pt = k->data; /* parent transactions table */
				sql_table *t = l->data;

				if (t->persistence == SQL_LOCAL_TEMP) /* skip local tables */
					continue;

				t->base.rtime = t->base.wtime = 0;
				t->base.stime = pt->base.wtime;
//				assert(t->base.stime > 0 || !isTable(t));
				if (!istmp && !t->base.allocated) {
					t->data = NULL;
				}
				assert (istmp || !t->base.allocated);
				assert (otr != gtrans || !isTable(pt) || pt->data);

				if (pt->base.id == t->base.id) {
					node *i, *j;

					for (i = pt->columns.set->h, j = t->columns.set->h; i && j; i = i->next, j = j->next ) {
						sql_column *pc = i->data; /* parent transactions column */
						sql_column *c = j->data;

						if (pc->base.id == c->base.id) {
							c->colnr = pc->colnr;
							c->base.rtime = c->base.wtime = 0;
							c->base.stime = pc->base.wtime;
							if (!istmp && !c->base.allocated)
								c->data = NULL;
							assert (istmp || !c->base.allocated);
						} else {
							/* for now assert */
							assert(0);
						}
					}
					if (pt->idxs.set && t->idxs.set)
					for (i = pt->idxs.set->h, j = t->idxs.set->h; i && j; i = i->next, j = j->next ) {
						sql_idx *pc = i->data; /* parent transactions column */
						sql_idx *c = j->data;

						if (pc->base.id == c->base.id) {
							c->base.rtime = c->base.wtime = 0;
							c->base.stime = pc->base.wtime;
							if (!istmp && !c->base.allocated)
								c->data = NULL;
							assert (istmp || !c->base.allocated);
						} else {
							/* for now assert */
							assert(0);
						}
					}
				} else {
					/* for now assert */
					assert(0);
				}
				k = k->next;
			}
			if (ps->seqs.set && s->seqs.set) {
				for (k = ps->seqs.set->h, l = s->seqs.set->h; k && l; k = k->next, l = l->next ) {
					sql_sequence *pt = k->data; /* parent transactions sequence */
					sql_sequence *t = l->data;

					t->base.rtime = t->base.wtime = 0;
					t->base.stime = pt->base.wtime;
				}
			}
			if (ps->funcs.set && s->funcs.set) {
				for (k = ps->funcs.set->h, l = s->funcs.set->h; k && l; k = k->next, l = l->next ) {
					sql_func *pt = k->data; /* parent transactions func */
					sql_func *t = l->data;

					t->base.rtime = t->base.wtime = 0;
					t->base.stime = pt->base.wtime;
				}
			}
			if (ps->types.set && s->types.set) {
				for (k = ps->types.set->h, l = s->types.set->h; k && l; k = k->next, l = l->next ) {
					sql_type *pt = k->data; /* parent transactions type */
					sql_type *t = l->data;

					t->base.rtime = t->base.wtime = 0;
					t->base.stime = pt->base.wtime;
				}
			}
			if (ps->parts.set && s->parts.set) {
				for (k = ps->parts.set->h, l = s->parts.set->h; k && l; k = k->next, l = l->next ) {
					sql_part *pc = k->data; /* parent transactions part */
					sql_part *c = l->data;

					if (pc->base.id == c->base.id) {
						c->base.rtime = c->base.wtime = 0;
						c->base.stime = pc->base.wtime;
					} else {
						/* for now assert */
						assert(0);
					}
				}
			}
		} else {
			/* for now assert */
			assert(0);
		}
	}
	tr->name = NULL;
	TRC_DEBUG(SQL_STORE, "Transaction '%p' init: %d, %d, %d\n", tr, tr->wstime, tr->stime, tr->schema_number);
	return tr;
}

static sql_trans *
trans_dup(sql_trans *ot, const char *newname)
{
	node *n;
	sql_trans *t = ZNEW(sql_trans);

	if (!t)
		return NULL;

	t->sa = sa_create(NULL);
	if (!t->sa) {
		_DELETE(t);
		return NULL;
	}
	_trans_init(t, ot);

	cs_new(&t->schemas, t->sa, (fdestroy) &schema_destroy);

	/* name the old transaction */
	if (newname) {
		assert(ot->name == NULL);
		ot->name = sa_strdup(ot->sa, newname);
	}

	if (ot->schemas.set) {
		for (n = ot->schemas.set->h; n; n = n->next) {
			cs_add(&t->schemas, schema_dup(t, 0, n->data, t), 0);
		}
		if (ot == gtrans)
			ot->schemas.nelm = NULL;
	}
	new_trans_size = t->sa->nr;
	return t;
}

#define R_SNAPSHOT 	1
#define R_LOG 		2
#define R_APPLY 	3

typedef int (*rfufunc) (sql_trans *tr, int oldest, sql_base * fs, sql_base * ts, int mode);
typedef sql_base *(*rfcfunc) (sql_trans *tr, sql_base * b, int mode);
typedef int (*rfdfunc) (sql_trans *tr, sql_base * b, int mode);
typedef sql_base *(*dupfunc) (sql_trans *tr, int flags, sql_base * b, sql_base * p);
typedef void (*cleanupfunc) (sql_base *b);

static sql_table *
conditional_table_dup(sql_trans *tr, int flags, sql_table *ot, sql_schema *s)
{
	int p = (tr->parent == gtrans);

	/* persistent columns need to be dupped */
	if ((p && isGlobal(ot)) ||
	    /* allways dup in recursive mode */
	    tr->parent != gtrans)
		return table_dup(tr, flags, ot, s);
	else if (!isGlobal(ot)) { /* is local temp, may need to be cleared */
		if (ot->commit_action == CA_DELETE) {
			sql_trans_clear_table(tr, ot);
		} else if (ot->commit_action == CA_DROP) {
			(void) sql_trans_drop_table(tr, ot->s, ot->base.id, DROP_RESTRICT);
		}
	}
	return NULL;
}

static int
rollforward_changeset_updates(sql_trans *tr, int oldest, changeset * fs, changeset * ts, sql_base * b, rfufunc rollforward_updates, rfcfunc rollforward_creates, rfdfunc rollforward_deletes, dupfunc fd, cleanupfunc cf, int mode)
{
	int ok = LOG_OK;
	int apply = (mode == R_APPLY);
	node *n = NULL;

	/* delete removed bases */
	if (fs->dset) {
#if 0
		if (!apply && ts->dset && oldest) {
			for (node *o, *n = ts->dset->h; n; n = o) {
				o = n->next;
				sql_base *b = n->data;
				if (b->wtime < oldest) { /* deleted before the oldest transaction, time to remove */
					if (b->refcnt > 1 && cf)
						cf(b);
					assert(b->refcnt == 1);
					list_remove_node(ts->dset, n);
				}
			}
		}
#endif
		for (n = fs->dset->h; ok == LOG_OK && n; n = n->next) {
			sql_base *fb = n->data;
			node *tbn = cs_find_id(ts, fb->id);

			if (tbn) {
				sql_base *tb = tbn->data;

				if (!apply && rollforward_deletes)
					ok = rollforward_deletes(tr, tb, mode);
				if (apply) {
					if (ts->nelm == tbn)
						ts->nelm = tbn->next;
					if (!ts->dset)
						ts->dset = list_new(tr->parent->sa, ts->destroy);
					tb->wtime = fb->wtime;
					list_move_data(ts->set, ts->dset, tb);
				}
			}
		}
		if (apply) {
			list_destroy(fs->dset);
			fs->dset = NULL;
		}
		/*
		if (!apply && ts->dset) {
			for (n = ts->dset->h; ok == LOG_OK && n; n = n->next) {
				sql_base *tb = n->data;

				if (rollforward_deletes)
					ok = rollforward_deletes(tr, tb, mode);
			}
		}
		*/
		if (apply && ts->dset && !cf) {
			list_destroy(ts->dset);
			ts->dset = NULL;
		} else if (apply && ts->dset && oldest && cf) {
			for (node *o, *n = ts->dset->h; n; n = o) {
				o = n->next;
				sql_base *b = n->data;
				if (b->wtime < oldest || oldest < 0) { /* deleted before the oldest transaction, time to remove */
					if (b->refcnt > 1 && cf)
						cf(b);
					assert(b->refcnt == 1);
					list_remove_node(ts->dset, n);
				}
			}
		}
	}
	/* changes to the existing bases */
	if (fs->set) {
		/* update existing */
		if (rollforward_updates) {
			for (n = fs->set->h; ok == LOG_OK && n && n != fs->nelm; n = n->next) {
				sql_base *fb = n->data;

				if (fb->wtime && !newFlagSet(fb->flags)) {
					node *tbn = cs_find_id(ts, fb->id);

					assert(fb->rtime <= fb->wtime);
					if (tbn) {
						sql_base *tb = tbn->data;

						ok = rollforward_updates(tr, oldest, fb, tb, mode);

						/* update timestamps */
						if (apply && fb->rtime && fb->rtime > tb->rtime)
							tb->rtime = fb->rtime;
						if (apply && fb->wtime && fb->wtime > tb->wtime)
							tb->wtime = fb->wtime;
						if (apply)
							fb->stime = tb->stime = tb->wtime;
						assert(!apply || tb->rtime <= tb->wtime);
					}
				}
			}
		}
		/* add the new bases */
		if (fd && rollforward_creates) {
			for (n = fs->nelm; ok == LOG_OK && n; ) {
				node *nxt = n->next;
				sql_base *fb = n->data;

				if (apply) {
					sql_base *tb = fd(tr, TR_NEW, fb, b);

					/* conditional add the new bases */
					if (tb) {
						sql_base *r = rollforward_creates(tr, tb, mode);

						if (r)
							cs_add(ts, r, TR_NEW);
						else
							ok = LOG_ERR;
						fb->flags = 0;
						tb->flags = 0;
						fb->stime = tb->stime = tb->wtime;
					}
				} else if (!rollforward_creates(tr, fb, mode)) {
					ok = LOG_ERR;
				}
				n = nxt;
			}
			if (apply)
				fs -> nelm = NULL;
		}
	}
	return ok;
}

static int
rollforward_changeset_creates(sql_trans *tr, changeset * cs, rfcfunc rf, int mode)
{
	int apply = (mode == R_APPLY);

	if (cs->set) {
		node *n;

		for (n = cs->set->h; n; n = n->next) {
			sql_base *b = n->data;

			if (!rf(tr, b, mode))
				return LOG_ERR;

			if (apply)
				b->flags = 0;
		}
		if (apply)
			cs->nelm = NULL;
	}
	return LOG_OK;
}

static int
rollforward_changeset_deletes(sql_trans *tr, changeset * cs, rfdfunc rf, int mode)
{
	int apply = (mode == R_APPLY);
	int ok = LOG_OK;

	if (!cs)
		return ok;
	if (cs->dset) {
		node *n;

		for (n = cs->dset->h; ok == LOG_OK && n; n = n->next) {
			sql_base *b = n->data;

			ok = rf(tr, b, mode);
		}
		if (apply) {
			list_destroy(cs->dset);
			cs->dset = NULL;
		}
	}
	if (cs->set) {
		node *n;

		for (n = cs->set->h; ok == LOG_OK && n; n = n->next) {
			sql_base *b = n->data;

			ok = rf(tr, b, mode);
		}
	}
	return ok;
}

static sql_idx *
rollforward_create_idx(sql_trans *tr, sql_idx * i, int mode)
{
	if (isTable(i->t) && idx_has_column(i->type)) {
		int p = (tr->parent == gtrans && !isTempTable(i->t));

		if ((p && mode == R_SNAPSHOT && store_funcs.snapshot_create_idx(tr, i) != LOG_OK) ||
		    (p && mode == R_LOG && store_funcs.log_create_idx(tr, i) != LOG_OK) ||
		    (mode == R_APPLY && store_funcs.create_idx(tr, i) != LOG_OK))
		return NULL;
	}
	return i;
}

static sql_key *
rollforward_create_key(sql_trans *tr, sql_key *k, int mode)
{
	(void) tr;
	(void) mode;
	return k;
}

static sql_trigger *
rollforward_create_trigger(sql_trans *tr, sql_trigger *k, int mode)
{
	(void) tr;
	(void) mode;
	return k;
}

static sql_type *
rollforward_create_type(sql_trans *tr, sql_type *k, int mode)
{
	(void) tr;
	(void) mode;
	return k;
}

static sql_func *
rollforward_create_func(sql_trans *tr, sql_func *k, int mode)
{
	(void) tr;
	(void) mode;
	return k;
}

static sql_sequence *
rollforward_create_seq(sql_trans *tr, sql_sequence *k, int mode)
{
	(void) tr;
	(void) mode;
	return k;
}

static sql_column *
rollforward_create_column(sql_trans *tr, sql_column *c, int mode)
{
	if (isTable(c->t)) {
		int p = (tr->parent == gtrans && !isTempTable(c->t));

		if ((p && mode == R_SNAPSHOT && store_funcs.snapshot_create_col(tr, c) != LOG_OK) ||
		    (p && mode == R_LOG && store_funcs.log_create_col(tr, c) != LOG_OK) ||
		    (mode == R_APPLY && store_funcs.create_col(tr, c) != LOG_OK))
		return NULL;
	}
	return c;
}

static sql_part *
rollforward_create_part(sql_trans *tr, sql_part *p, int mode)
{
	(void) tr;
	if (mode == R_APPLY) {
		sql_table *mt = p->t;
		//sql_table *pt = find_sql_table_id(mt->s, p->base.id);

		assert(isMergeTable(mt) || isReplicaTable(mt));
		(void)mt;
	}
	return p;
}

static int
rollforward_drop_part(sql_trans *tr, sql_part *p, int mode)
{
	(void) tr;
	if (mode == R_APPLY) {
		sql_table *mt = p->t;
		//sql_table *pt = find_sql_table_id(mt->s, p->base.id);

		assert(isMergeTable(mt) || isReplicaTable(mt));
		(void)mt;
	}
	return LOG_OK;
}

static sql_table *
rollforward_create_table(sql_trans *tr, sql_table *t, int mode)
{
	int ok = LOG_OK;
	TRC_DEBUG(SQL_STORE, "Create table: %s\n", t->base.name);

	if (isKindOfTable(t) && isGlobal(t)) {
		int p = (tr->parent == gtrans && !isTempTable(t));

		/* only register columns without commit action tables */
		ok = rollforward_changeset_creates(tr, &t->columns, (rfcfunc) &rollforward_create_column, mode);

		if (isTable(t)) {
			if (p && mode == R_SNAPSHOT)
				store_funcs.snapshot_create_del(tr, t);
			else if (p && mode == R_LOG)
				store_funcs.log_create_del(tr, t);
			else if (mode == R_APPLY)
				store_funcs.create_del(tr, t);
		}

		if (ok == LOG_OK)
			ok = rollforward_changeset_creates(tr, &t->keys, (rfcfunc) &rollforward_create_key, mode);
		if (ok == LOG_OK)
			ok = rollforward_changeset_creates(tr, &t->idxs, (rfcfunc) &rollforward_create_idx, mode);
		if (ok == LOG_OK)
			ok = rollforward_changeset_creates(tr, &t->triggers, (rfcfunc) &rollforward_create_trigger, mode);
	}
	if (ok != LOG_OK) {
		assert(0);
		return NULL;
	}
	return t;
}

static int
rollforward_drop_column(sql_trans *tr, sql_column *c, int mode)
{
	if (isTable(c->t)) {
		int p = (tr->parent == gtrans && !isTempTable(c->t));

		if (p && mode == R_LOG)
			return store_funcs.log_destroy_col(tr, c);
		else if (mode == R_APPLY)
			return store_funcs.destroy_col(tr, c);
	}
	return LOG_OK;
}

static int
rollforward_drop_idx(sql_trans *tr, sql_idx * i, int mode)
{
	int ok = LOG_OK;

	if (isTable(i->t)) {
		int p = (tr->parent == gtrans && !isTempTable(i->t));

		if (p && mode == R_LOG)
			ok = store_funcs.log_destroy_idx(tr, i);
		else if (mode == R_APPLY)
			ok = store_funcs.destroy_idx(tr, i);
	}
	/* remove idx from schema */
	if (mode == R_APPLY)
		list_remove_data(i->t->s->idxs, i);
	return ok;
}

static int
rollforward_drop_key(sql_trans *tr, sql_key *k, int mode)
{
	node *n = NULL;
	sql_fkey *fk = NULL;

	(void) tr;		/* unused! */
	if (mode != R_APPLY)
		return LOG_OK;
	/* remove key from schema */
	list_remove_data(k->t->s->keys, k);
	if (k->t->pkey == (sql_ukey*)k)
		k->t->pkey = NULL;
	if (k->type == fkey) {
		fk = (sql_fkey *) k;

		if (fk->rkey) {
			n = list_find_name(fk->rkey->keys, fk->k.base.name);
			list_remove_node(fk->rkey->keys, n);
		}
		fk->rkey = NULL;
	}
	if (k->type == pkey) {
		sql_ukey *uk = (sql_ukey *) k;

		if (uk->keys)
			for (n = uk->keys->h; n; n= n->next) {
				fk = (sql_fkey *) n->data;
				fk->rkey = NULL;
			}
	}
	return LOG_OK;
}

static int
rollforward_drop_trigger(sql_trans *tr, sql_trigger *i, int mode)
{
	(void)tr;
	if (mode == R_APPLY)
		list_remove_data(i->t->s->triggers, i);
	return LOG_OK;
}

static int
rollforward_drop_seq(sql_trans *tr, sql_sequence *seq, int mode)
{
	(void)tr;
	(void)seq;
	(void)mode;
	/* TODO drop sequence? */
	return LOG_OK;
}

static int
rollforward_drop_type(sql_trans *tr, sql_type *t, int mode)
{
	(void)tr;
	(void)t;
	(void)mode;
	return LOG_OK;
}

static int
rollforward_drop_func(sql_trans *tr, sql_func *f, int mode)
{
	(void)tr;
	(void)f;
	(void)mode;
	return LOG_OK;
}

static int
rollforward_drop_table(sql_trans *tr, sql_table *t, int mode)
{
	int ok = LOG_OK;

	if (isTable(t)) {
		int p = (tr->parent == gtrans && !isTempTable(t));

		if (p && mode == R_LOG)
			ok = store_funcs.log_destroy_del(tr, t);
		else if (mode == R_APPLY)
			ok = store_funcs.destroy_del(tr, t);
	}
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &t->columns, (rfdfunc) &rollforward_drop_column, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &t->idxs, (rfdfunc) &rollforward_drop_idx, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &t->keys, (rfdfunc) &rollforward_drop_key, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &t->triggers, (rfdfunc) &rollforward_drop_trigger, mode);
	return ok;
}

static int
rollforward_drop_schema(sql_trans *tr, sql_schema *s, int mode)
{
	int ok = LOG_OK;

	ok = rollforward_changeset_deletes(tr, &s->types, (rfdfunc) &rollforward_drop_type, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &s->parts, (rfdfunc) &rollforward_drop_part, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &s->tables, (rfdfunc) &rollforward_drop_table, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &s->funcs, (rfdfunc) &rollforward_drop_func, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &s->seqs, (rfdfunc) &rollforward_drop_seq, mode);
	return ok;
}

static sql_schema *
rollforward_create_schema(sql_trans *tr, sql_schema *s, int mode)
{
	if (rollforward_changeset_creates(tr, &s->types, (rfcfunc) &rollforward_create_type, mode) != LOG_OK)
		return NULL;
	if (rollforward_changeset_creates(tr, &s->tables, (rfcfunc) &rollforward_create_table, mode) != LOG_OK)
		return NULL;
	if (rollforward_changeset_creates(tr, &s->funcs, (rfcfunc) &rollforward_create_func, mode) != LOG_OK)
		return NULL;
	if (rollforward_changeset_creates(tr, &s->seqs, (rfcfunc) &rollforward_create_seq, mode) != LOG_OK)
		return NULL;
	if (rollforward_changeset_creates(tr, &s->parts, (rfcfunc) &rollforward_create_part, mode) != LOG_OK)
		return NULL;
	return s;
}

static int
rollforward_update_part(sql_trans *tr, int oldest, sql_base *fpt, sql_base *tpt, int mode)
{
	(void)oldest;
	if (mode == R_APPLY) {
		sql_part *pt = (sql_part *) tpt;
		sql_part *opt = (sql_part *) fpt;

		pt->with_nills = opt->with_nills;
		if (isRangePartitionTable(opt->t)) {
			pt->part.range.minvalue = sa_alloc(tr->parent->sa, opt->part.range.minlength);
			pt->part.range.maxvalue = sa_alloc(tr->parent->sa, opt->part.range.maxlength);
			memcpy(pt->part.range.minvalue, opt->part.range.minvalue, opt->part.range.minlength);
			memcpy(pt->part.range.maxvalue, opt->part.range.maxvalue, opt->part.range.maxlength);
			pt->part.range.minlength = opt->part.range.minlength;
			pt->part.range.maxlength = opt->part.range.maxlength;
		} else if (isListPartitionTable(opt->t)) {
			pt->part.values = list_new(tr->parent->sa, (fdestroy) NULL);
			for (node *n = opt->part.values->h ; n ; n = n->next) {
				sql_part_value *prev = (sql_part_value*) n->data, *nextv = SA_ZNEW(tr->parent->sa, sql_part_value);
				nextv->value = sa_alloc(tr->parent->sa, prev->length);
				memcpy(nextv->value, prev->value, prev->length);
				nextv->length = prev->length;
				list_append(pt->part.values, nextv);
			}
		}
	}
	return LOG_OK;
}

static int
rollforward_update_table(sql_trans *tr, int oldest, sql_table *ft, sql_table *tt, int mode)
{
	int p = (tr->parent == gtrans && !isTempTable(ft));
	int ok = LOG_OK;

	/* cannot update views */
	if (isView(ft))
		return ok;

	if (mode == R_APPLY && ok == LOG_OK) {
		ft->cleared = 0;
		tt->access = ft->access;

		if (strcmp(tt->base.name, ft->base.name) != 0) { /* apply possible renaming */
			list_hash_delete(tt->s->tables.set, tt, NULL);
			tt->base.name = sa_strdup(tr->parent->sa, ft->base.name);
			if (!list_hash_add(tt->s->tables.set, tt, NULL))
				ok = LOG_ERR;
		}
	}

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &ft->triggers, &tt->triggers, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_trigger, (rfdfunc) &rollforward_drop_trigger, (dupfunc) &trigger_dup, (cleanupfunc) NULL, mode);

	if (isTempTable(ft))
		return ok;

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &ft->columns, &tt->columns, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_column, (rfdfunc) &rollforward_drop_column, (dupfunc) &column_dup, (cleanupfunc) NULL, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &ft->idxs, &tt->idxs, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_idx, (rfdfunc) &rollforward_drop_idx, (dupfunc) &idx_dup, (cleanupfunc) NULL, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &ft->keys, &tt->keys, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_key, (rfdfunc) &rollforward_drop_key, (dupfunc) &key_dup, (cleanupfunc) NULL, mode);

	if (ok != LOG_OK)
		return LOG_ERR;

	if (isTable(ft)) {
		if (p && mode == R_SNAPSHOT) {
			ok = store_funcs.snapshot_table(tr, ft, tt);
		} else if (p && mode == R_LOG) {
			ok = store_funcs.log_table(tr, ft, tt);
		} else if (mode == R_APPLY) {
			assert(cs_size(&tt->columns) == cs_size(&ft->columns));
			TRC_DEBUG(SQL_STORE, "Update table: %s\n", tt->base.name);
			ok = store_funcs.update_table(tr, ft, tt);
		}
	}

	return ok;
}

static int
rollforward_update_seq(sql_trans *tr, int oldest, sql_sequence *ft, sql_sequence *tt, int mode)
{
	(void)tr;
	(void)oldest;
	if (mode != R_APPLY)
		return LOG_OK;
	if (ft->start != tt->start)
		tt->start = ft->start;
	tt->minvalue = ft->minvalue;
	tt->maxvalue = ft->maxvalue;
	tt->increment = ft->increment;
	tt->cacheinc = ft->cacheinc;
	tt->cycle = ft->cycle;
	return LOG_OK;
}

static int
rollforward_update_schema(sql_trans *tr, int oldest, sql_schema *fs, sql_schema *ts, int mode)
{
	int apply = (mode == R_APPLY);
	int ok = LOG_OK;

	if (apply && isTempSchema(fs)) {
		if (fs->tables.set) {
			node *n;
			for (n = fs->tables.set->h; n; ) {
				node *nxt = n->next;
				sql_table *t = n->data;

				if ((isTable(t) && isGlobal(t) &&
				    t->commit_action != CA_PRESERVE) ||
				    t->commit_action == CA_DELETE) {
					sql_trans_clear_table(tr, t);
				} else if (t->commit_action == CA_DROP) {
					if (sql_trans_drop_table(tr, t->s, t->base.id, DROP_RESTRICT))
						ok = LOG_ERR;
				}
				n = nxt;
			}
		}
	}

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &fs->types, &ts->types, &ts->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_type, (rfdfunc) &rollforward_drop_type, (dupfunc) &type_dup, (cleanupfunc) NULL, mode);

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &fs->tables, &ts->tables, &ts->base, (rfufunc) &rollforward_update_table, (rfcfunc) &rollforward_create_table, (rfdfunc) &rollforward_drop_table, (dupfunc) &conditional_table_dup, (cleanupfunc) cleanup_table, mode);

	if (ok == LOG_OK) /* last as it may require complex (table) types */
		ok = rollforward_changeset_updates(tr, oldest, &fs->funcs, &ts->funcs, &ts->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_func, (rfdfunc) &rollforward_drop_func, (dupfunc) &func_dup, (cleanupfunc) NULL, mode);

	if (ok == LOG_OK) /* last as it may require complex (table) types */
		ok = rollforward_changeset_updates(tr, oldest, &fs->seqs, &ts->seqs, &ts->base, (rfufunc) &rollforward_update_seq, (rfcfunc) &rollforward_create_seq, (rfdfunc) &rollforward_drop_seq, (dupfunc) &seq_dup, (cleanupfunc) NULL, mode);
 	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &fs->parts, &ts->parts, &ts->base, (rfufunc) &rollforward_update_part, (rfcfunc) &rollforward_create_part, (rfdfunc) &rollforward_drop_part, (dupfunc) &part_dup, (cleanupfunc) NULL, mode);
	if (apply && ok == LOG_OK && ts->parts.dset) {
		list_destroy(ts->parts.dset);
		ts->parts.dset = NULL;
	}

	if (apply && ok == LOG_OK && strcmp(ts->base.name, fs->base.name) != 0) { /* apply possible renaming */
		list_hash_delete(tr->schemas.set, ts, NULL);
		ts->base.name = sa_strdup(tr->parent->sa, fs->base.name);
		if (!list_hash_add(tr->schemas.set, ts, NULL))
			ok = LOG_ERR;
	}

	return ok;
}

static int
rollforward_trans(sql_trans *tr, int oldest, int mode)
{
	int ok = LOG_OK;

	if (mode == R_APPLY && tr->parent && tr->wtime > tr->parent->wtime) {
		tr->parent->wtime = tr->wtime;
		tr->parent->schema_updates += tr->schema_updates;
	}

	if (tr->moved_tables) {
		for (node *n = tr->moved_tables->h ; n ; n = n->next) {
			sql_moved_table *smt = (sql_moved_table*) n->data;
			sql_schema *pfrom = find_sql_schema_id(tr->parent, smt->from->base.id);
			sql_schema *pto = find_sql_schema_id(tr->parent, smt->to->base.id);
			sql_table *pt = find_sql_table_id(pfrom, smt->t->base.id);

			assert(pfrom && pto && pt);
			cs_move(&pfrom->tables, &pto->tables, pt);
			pt->s = pto;
		}
		tr->moved_tables = NULL;
	}

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, oldest, &tr->schemas, &tr->parent->schemas, (sql_base *) tr->parent, (rfufunc) &rollforward_update_schema, (rfcfunc) &rollforward_create_schema, (rfdfunc) &rollforward_drop_schema, (dupfunc) &schema_dup, (cleanupfunc) NULL, mode);
	if (mode == R_APPLY) {
		if (tr->parent == gtrans) {
			if (gtrans->stime < tr->stime)
				gtrans->stime = tr->stime;
			if (gtrans->wstime < tr->wstime)
				gtrans->wstime = tr->wstime;

			if (tr->schema_updates)
				schema_number++;
		}
	}
	return ok;
}

static int
validate_tables(sql_schema *s, sql_schema *os)
{
	node *n, *o, *p;

	if (cs_size(&s->tables))
		for (n = s->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;

			if (!t->base.wtime && !t->base.rtime)
				continue;

			sql_table *ot = find_sql_table_id(os, t->base.id);

			if (!ot && os->tables.dset && list_find_base_id(os->tables.dset, t->base.id) != NULL) {
				/* dropped table */
				return 0;
			} else if (ot && isKindOfTable(ot) && isKindOfTable(t) && !isDeclaredTable(ot) && !isDeclaredTable(t)) {
				if ((t->base.wtime && (t->base.wtime < ot->base.rtime || t->base.wtime < ot->base.wtime)) ||
				    (t->base.rtime && (t->base.rtime < ot->base.wtime)))
					return 0;
				for (o = t->columns.set->h, p = ot->columns.set->h; o && p; o = o->next, p = p->next) {
					sql_column *c = o->data;
					sql_column *oc = p->data;

					if (!c->base.wtime && !c->base.rtime)
						continue;

					/* t wrote, ie. check read and write time */
					/* read or write after t's write */
					if (c->base.wtime && (c->base.wtime < oc->base.rtime
							  ||  c->base.wtime < oc->base.wtime))
						return 0;
					/* commited write before t's read */
					if (c->base.rtime && c->base.rtime < oc->base.wtime)
						return 0;
				}
			}
		}
	return 1;
}

/* merge any changes from the global transaction into the local transaction */
typedef int (*resetf) (sql_trans *tr, sql_base * fs, sql_base * pfs);

static int
reset_changeset(sql_trans *tr, changeset * fs, changeset * pfs, sql_base *b, resetf rf, dupfunc fd)
{
	int ok = LOG_OK;
	node *m = NULL, *n = NULL;

	(void)tr;
	/* first delete created */
	if (fs->nelm) {
		for (n = fs->nelm; n; ) {
			node *nxt = n->next;

			cs_remove_node(fs, n);
			n = nxt;
		}
		fs->nelm = NULL;
	}
	/* scan through the parent set,
		if child has it simply reset it (if needed)
		else add a new or add again the old
	*/
	if (fs->set)
		n = fs->set->h;
	if (pfs->set) {
		for (m = pfs->set->h; ok == LOG_OK && m && n; ) {
			sql_base *fb = n->data;
			sql_base *pfb = m->data;

			/* lists ordered on id */
			/* changes to the existing bases */
			if (fb->id == pfb->id) {
				if (rf)
					ok = rf(tr, fb, pfb);
				n = n->next;
				m = m->next;
				TRC_DEBUG(SQL_STORE, "%s\n", (fb->name) ? fb->name : "help");
			} else if (fb->id < pfb->id) {
				node *t = n->next;

				TRC_DEBUG_IF(SQL_STORE)
				{
					sql_base *b = n->data;
					TRC_DEBUG_ENDIF(SQL_STORE, "Free: %s\n", (b->name) ? b->name : "help");
				}

				cs_remove_node(fs, n);
				n = t;
			} else { /* a new id */
				sql_base *r = fd(tr, 0, pfb, b);
				/* cs_add_before add r to fs before node n */
				cs_add_before(fs, n, r);
				m = m->next;
				TRC_DEBUG(SQL_STORE, "New: %s\n", (r->name) ? r->name : "help");
			}
		}
		/* add new bases */
		for (; ok == LOG_OK && m; m = m->next ) {
			sql_base *pfb = m->data;
			sql_base *r = fd(tr, 0, pfb, b);
			cs_add(fs, r, 0);
			TRC_DEBUG(SQL_STORE, "New: %s\n", (r->name) ? r->name : "help");
		}
		while ( ok == LOG_OK && n) { /* remove remaining old stuff */
			node *t = n->next;

			TRC_DEBUG_IF(SQL_STORE)
			{
				sql_base *b = n->data;
				TRC_DEBUG_ENDIF(SQL_STORE, "Free: %s\n", (b->name) ? b->name : "help");
			}

			cs_remove_node(fs, n);
			n = t;
		}
	}
	if (fs->dset) {
		list_destroy(fs->dset);
		fs->dset = NULL;
	}
	return ok;
}

static int
reset_idx(sql_trans *tr, sql_idx *fi, sql_idx *pfi)
{
	(void)tr;
	/* did we access the idx or is the global changed after we started */
	if (fi->base.rtime || fi->base.wtime || fi->base.stime < pfi->base.wtime) {
		if (isTable(fi->t))
			store_funcs.destroy_idx(NULL, fi);
	}
	return LOG_OK;
}

static int
reset_type(sql_trans *tr, sql_type *ft, sql_type *pft)
{
	/* did we access the type or is the global changed after we started */
	if (ft->base.rtime || ft->base.wtime || ft->base.stime < pft->base.wtime) {

		ft->sqlname = pft->sqlname;
		ft->radix = pft->radix;
		ft->eclass = pft->eclass;
		ft->bits = pft->bits;
		ft->localtype = pft->localtype;
		ft->digits = pft->digits;
		ft->scale = pft->scale;
		ft->s = find_sql_schema_id(tr, pft->s->base.id);
	}
	return LOG_OK;
}

static int
reset_func(sql_trans *tr, sql_func *ff, sql_func *pff)
{
	/* did we access the type or is the global changed after we started */
	if (ff->base.rtime || ff->base.wtime || ff->base.stime < pff->base.wtime) {

		ff->imp = pff->imp;
		ff->mod = pff->mod;
		ff->type = pff->type;
		ff->query = pff->query;
		ff->lang = pff->lang;
		ff->sql = pff->sql;
		ff->side_effect = pff->side_effect;
		ff->varres = pff->varres;
		ff->vararg = pff->vararg;
		ff->ops = pff->ops;
		ff->res = pff->res;
		ff->fix_scale = pff->fix_scale;
		ff->system = pff->system;
		ff->semantics = pff->semantics;
		ff->s = find_sql_schema_id(tr, pff->s->base.id);
		ff->sa = tr->sa;
	}
	return LOG_OK;
}

static int
reset_column(sql_trans *tr, sql_column *fc, sql_column *pfc)
{
	/* did we access the column or is the global changed after we started */
	if (fc->base.rtime || fc->base.wtime || fc->base.stime < pfc->base.wtime) {

		if (isTable(fc->t))
			store_funcs.destroy_col(NULL, fc);

		/* apply possible renaming -> transaction rollbacks or when it starts, inherit from the previous transaction */
		if (strcmp(fc->base.name, pfc->base.name) != 0) {
			list_hash_delete(fc->t->columns.set, fc, NULL);
			fc->base.name = sa_strdup(tr->parent->sa, pfc->base.name);
			if (!list_hash_add(fc->t->columns.set, fc, NULL))
				return LOG_ERR;
		}

		fc->null = pfc->null;
		fc->unique = pfc->unique;
		fc->colnr = pfc->colnr;
		fc->storage_type = NULL;
		if (pfc->storage_type)
			fc->storage_type = pfc->storage_type;
		fc->def = NULL;
		if (pfc->def)
			fc->def = pfc->def;
		fc->min = fc->max = NULL;
	}
	return LOG_OK;
}

static int
reset_seq(sql_trans *tr, sql_sequence *ft, sql_sequence *pft)
{
	(void) tr;
	ft->start = pft->start;
	ft->minvalue = pft->minvalue;
	ft->maxvalue = pft->maxvalue;
	ft->increment = pft->increment;
	ft->cacheinc = pft->cacheinc;
	ft->cycle = pft->cycle;
	return LOG_OK;
}

static int
reset_part(sql_trans *tr, sql_part *ft, sql_part *pft)
{
	if (ft->base.rtime || ft->base.wtime || ft->base.stime < pft->base.wtime) {

		if (pft->t) {
			sql_table *mt = pft->t;
			sql_schema *s = find_sql_schema_id(tr, mt->s->base.id);
			if (s) {
				sql_table *fmt = find_sql_table_id(s, mt->base.id);
				assert(isMergeTable(fmt) || isReplicaTable(fmt));
				ft->t = fmt;

				ft->member = find_sql_table_id(s, pft->base.id);
				assert(ft->t && ft->member);
			}
			if (s && (isRangePartitionTable(mt) || isListPartitionTable(mt)))
				dup_sql_type(tr, s, &(pft->tpe), &(ft->tpe));
			else
				ft->tpe = pft->tpe;
		} else {
			ft->t = NULL;
			ft->tpe = pft->tpe;
		}

		ft->with_nills = pft->with_nills;
		if (pft->t && isRangePartitionTable(pft->t)) {
			ft->part.range = pft->part.range;
		} else if (pft->t && isListPartitionTable(pft->t)) {
			ft->part.values = pft->part.values;
		}
	}
	return LOG_OK;
}

static int
reset_table(sql_trans *tr, sql_table *ft, sql_table *pft)
{
	if (isView(ft))
		return LOG_OK;

	/* did we access the table or did the global change */
	if (ft->base.rtime || ft->base.wtime || ft->base.stime < pft->base.wtime) {
		int ok = LOG_OK;

		if (isTable(ft) && !isTempTable(ft))
			store_funcs.destroy_del(NULL, ft);

		ft->cleared = 0;
		ft->access = pft->access;

		/* apply possible renaming -> transaction rollbacks or when it starts, inherit from the previous transaction */
		if (strcmp(ft->base.name, pft->base.name) != 0) {
			list_hash_delete(ft->s->tables.set, ft, NULL);
			ft->base.name = sa_strdup(tr->parent->sa, pft->base.name);
			if (!list_hash_add(ft->s->tables.set, ft, NULL))
				ok = LOG_ERR;
		}

		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->triggers, &pft->triggers, &ft->base, (resetf) NULL, (dupfunc) &trigger_dup);

		if (isTempTable(ft))
			return ok;

		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->columns, &pft->columns, &ft->base, (resetf) &reset_column, (dupfunc) &column_dup);
		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->idxs, &pft->idxs, &ft->base, (resetf) &reset_idx, (dupfunc) &idx_dup);
		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->keys, &pft->keys, &ft->base, (resetf) NULL, (dupfunc) &key_dup);

		return ok;
	}
	return LOG_OK;
}

static int
reset_schema(sql_trans *tr, sql_schema *fs, sql_schema *pfs)
{
	int ok = LOG_OK;

	if (isTempSchema(fs)) { /* only add new globaly created temps and remove globaly removed temps */
		if (fs->tables.set) {
			node *n = NULL, *m = NULL;
			if (pfs->tables.set)
				m = pfs->tables.set->h;
			for (n = fs->tables.set->h; ok == LOG_OK && m && n; ) {
				sql_table *ftt = n->data;
				sql_table *pftt = m->data;

				/* lists ordered on id */
				/* changes to the existing bases */
				if (ftt->base.id == pftt->base.id) { /* global temp */
					n = n->next;
					m = m->next;
				} else if (ftt->base.id < pftt->base.id) { /* local temp or old global ? */
					node *t = n->next;

					if (isGlobal(ftt)) /* remove old global */
						cs_remove_node(&fs->tables, n);
					n = t;
				} else { /* a new global */
					sql_table *ntt = table_dup(tr, 0, pftt, fs);

					/* cs_add_before add ntt to fs before node n */
					cs_add_before(&fs->tables, n, ntt);
					m = m->next;
				}
			}
			/* add new globals */
			for (; ok == LOG_OK && m; m = m->next ) {
				sql_table *pftt = m->data;
				sql_table *ntt = table_dup(tr, 0, pftt, fs);

				assert(isGlobal(ntt));
				/* cs_add_before add ntt to fs before node n */
				cs_add_before(&fs->tables, n, ntt);
			}
			while ( ok == LOG_OK && n) { /* remove remaining old stuff */
				sql_table *ftt = n->data;
				node *t = n->next;

				if (isGlobal(ftt)) /* remove old global */
					cs_remove_node(&fs->tables, n);
				n = t;
			}
		}
	}

	/* apply possible renaming -> transaction rollbacks or when it starts, inherit from the previous transaction */
	if (strcmp(fs->base.name, pfs->base.name) != 0) {
		list_hash_delete(tr->schemas.set, fs, NULL);
		fs->base.name = sa_strdup(tr->parent->sa, pfs->base.name);
		if (!list_hash_add(tr->schemas.set, fs, NULL))
			ok = LOG_ERR;
	}

	if (ok == LOG_OK)
		ok = reset_changeset(tr, &fs->types, &pfs->types, &fs->base, (resetf) &reset_type, (dupfunc) &type_dup);
	if (ok == LOG_OK)
		ok = reset_changeset(tr, &fs->funcs, &pfs->funcs, &fs->base, (resetf) &reset_func, (dupfunc) &func_dup);
	if (ok == LOG_OK)
		ok = reset_changeset(tr, &fs->seqs, &pfs->seqs, &fs->base, (resetf) &reset_seq, (dupfunc) &seq_dup);
	if (!isTempSchema(fs) && ok == LOG_OK)
		ok = reset_changeset(tr, &fs->tables, &pfs->tables, &fs->base, (resetf) &reset_table, (dupfunc) &table_dup);
	if (!isTempSchema(fs) && ok == LOG_OK)
		ok = reset_changeset(tr, &fs->parts, &pfs->parts, &fs->base, (resetf) &reset_part, (dupfunc) &part_dup);
	return ok;
}

static int
reset_trans(sql_trans *tr, sql_trans *ptr)
{
	int res = reset_changeset(tr, &tr->schemas, &ptr->schemas, (sql_base *)tr->parent, (resetf) &reset_schema, (dupfunc) &schema_dup);
	TRC_DEBUG(SQL_STORE, "Reset transaction: %d\n", tr->wtime);
	return res;
}

sql_trans *
sql_trans_create(sql_trans *parent, const char *name, bool try_spare)
{
	sql_trans *tr = NULL;

	if (gtrans) {
		 if (!parent && spares > 0 && !name && try_spare) {
			tr = spare_trans[--spares];
			TRC_DEBUG(SQL_STORE, "Reuse transaction: %p - Spares: %d\n", tr, spares);
		} else {
			tr = trans_dup((parent) ? parent : gtrans, name);
			TRC_DEBUG(SQL_STORE, "New transaction: %p\n", tr);
			if (tr)
				(void) ATOMIC_INC(&transactions);
		}
	}
	return tr;
}

bool
sql_trans_validate(sql_trans *tr)
{
	node *n;

	/* depends on the iso level */

	/* If only 'inserts' occurred on the read columns the repeatable reads
	   iso level can continue */

	/* the hard case */
	if (cs_size(&tr->schemas))
		for (n = tr->schemas.set->h; n; n = n->next) {
			sql_schema *s = n->data;
			sql_schema *os;

			if (isTempSchema(s))
				continue;

 			os = find_sql_schema_id(tr->parent, s->base.id);
			if (os && (s->base.wtime != 0 || s->base.rtime != 0)) {
				if (!validate_tables(s, os))
					return false;
			}
		}
	return true;
}

static int
save_tables_snapshots(sql_schema *s)
{
	node *n;

	if (cs_size(&s->tables))
		for (n = s->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;

			if (!t->base.wtime)
				continue;

			if (isKindOfTable(t) && !isDeclaredTable(t)) {
				if (store_funcs.save_snapshot(t) != LOG_OK)
					return SQL_ERR;
			}
		}
	return SQL_OK;
}

int
sql_save_snapshots(sql_trans *tr)
{
	node *n;

	if (cs_size(&tr->schemas)) {
		for (n = tr->schemas.set->h; n; n = n->next) {
			sql_schema *s = n->data;

			if (isTempSchema(s))
				continue;

			if (s->base.wtime != 0)
				if (save_tables_snapshots(s) != SQL_OK)
					return SQL_ERR;
		}
	}
	return SQL_OK;
}

int
sql_trans_commit(sql_trans *tr)
{
	int ok = LOG_OK;
	int oldest = (tr->parent== gtrans)?oldest_active_tid():-1;

	/* write phase */
	TRC_DEBUG(SQL_STORE, "Forwarding changes (%d, %d) (%d, %d)\n", gtrans->stime, tr->stime, gtrans->wstime, tr->wstime);
	/* snap shots should be saved first */
	if (tr->parent == gtrans) {
		lng saved_id;

		ok = rollforward_trans(tr, oldest, R_SNAPSHOT);

		if (ok == LOG_OK)
			ok = logger_funcs.log_tstart();
		saved_id = logger_funcs.log_save_id();

		if (ok == LOG_OK)
			ok = rollforward_trans(tr, oldest, R_LOG);
		if (ok == LOG_OK && prev_oid != store_oid)
			ok = logger_funcs.log_sequence(OBJ_SID, store_oid);
		prev_oid = store_oid;
		if (ok == LOG_OK)
			ok = logger_funcs.log_tend();
		map_add(tr->wstime, saved_id);
	}
	if (ok == LOG_OK) {
		/* It is save to rollforward the changes now. In case
		   of failure, the log will be replayed. */
		ok = rollforward_trans(tr, oldest, R_APPLY);
	}
	TRC_DEBUG(SQL_STORE, "Done forwarding changes '%d' and '%d'\n", gtrans->stime, gtrans->wstime);
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
					//FIXME malloc failure scenario!
					(void) sql_trans_drop_schema(tr, dep_id, DROP_CASCADE);
					break;
				case TABLE_DEPENDENCY:
				case VIEW_DEPENDENCY: {
					sql_table *t = sql_trans_find_table(tr, dep_id);
					(void) sql_trans_drop_table(tr, t->s, dep_id, DROP_CASCADE);
				} break;
				case COLUMN_DEPENDENCY: {
					if ((t_id = sql_trans_get_dependency_type(tr, dep_id, TABLE_DEPENDENCY)) > 0) {
						sql_table *t = sql_trans_find_table(tr, dep_id);
						if (t)
							(void) sql_trans_drop_column(tr, t, dep_id, DROP_CASCADE);
					}
				} break;
				case TRIGGER_DEPENDENCY: {
					sql_trigger *t = sql_trans_find_trigger(tr, dep_id);
					(void) sql_trans_drop_trigger(tr, t->t->s, dep_id, DROP_CASCADE);
				} break;
				case KEY_DEPENDENCY:
				case FKEY_DEPENDENCY: {
					sql_key *k = sql_trans_find_key(tr, dep_id);
					(void) sql_trans_drop_key(tr, k->t->s, dep_id, DROP_CASCADE);
				} break;
				case INDEX_DEPENDENCY: {
					sql_idx *i = sql_trans_find_idx(tr, dep_id);
					(void) sql_trans_drop_idx(tr, i->t->s, dep_id, DROP_CASCADE);
				} break;
				case PROC_DEPENDENCY:
				case FUNC_DEPENDENCY: {
					sql_func *f = sql_trans_find_func(tr, dep_id);
					(void) sql_trans_drop_func(tr, f->s, dep_id, DROP_CASCADE);
				} break;
				case TYPE_DEPENDENCY: {
					sql_type *t = sql_trans_find_type(tr, dep_id);
					sql_trans_drop_type(tr, t->s, dep_id, DROP_CASCADE);
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

static void
sys_drop_kc(sql_trans *tr, sql_key *k, sql_kc *kc)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(syss, "objects");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(syskc, "id"), &k->base.id, find_sql_column(syskc, "name"), kc->c->base.name, NULL);

	if (is_oid_nil(rid))
		return ;
	table_funcs.table_delete(tr, syskc, rid);

	if (isGlobal(k->t))
		tr->schema_updates ++;
}

static void
sys_drop_ic(sql_trans *tr, sql_idx * i, sql_kc *kc)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(syss, "objects");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(sysic, "id"), &i->base.id, find_sql_column(sysic, "name"), kc->c->base.name, NULL);

	if (is_oid_nil(rid))
		return ;
	table_funcs.table_delete(tr, sysic, rid);

	if (isGlobal(i->t))
		tr->schema_updates ++;
}

static void
sys_drop_idx(sql_trans *tr, sql_idx * i, int drop_action)
{
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(syss, "idxs");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(sysidx, "id"), &i->base.id, NULL);

	if (is_oid_nil(rid))
		return ;
	table_funcs.table_delete(tr, sysidx, rid);
	sql_trans_drop_any_comment(tr, i->base.id);
	for (n = i->columns->h; n; n = n->next) {
		sql_kc *ic = n->data;
		sys_drop_ic(tr, i, ic);
	}

	/* remove idx from schema and table*/
	list_remove_data(i->t->s->idxs, i);
	sql_trans_drop_dependencies(tr, i->base.id);

	if (isGlobal(i->t))
		tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, i->base.id, INDEX_DEPENDENCY);
}

static void
sys_drop_key(sql_trans *tr, sql_key *k, int drop_action)
{
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(syss, "keys");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(syskey, "id"), &k->base.id, NULL);

	if (is_oid_nil(rid))
		return ;
	table_funcs.table_delete(tr, syskey, rid);

	for (n = k->columns->h; n; n = n->next) {
		sql_kc *kc = n->data;
		sys_drop_kc(tr, k, kc);
	}
	/* remove key from schema */
	list_remove_data(k->t->s->keys, k);
	if (k->t->pkey == (sql_ukey*)k)
		k->t->pkey = NULL;
	if (k->type == fkey) {
		sql_fkey *fk = (sql_fkey *) k;

		assert(fk->rkey);
		if (fk->rkey) {
			n = list_find_name(fk->rkey->keys, fk->k.base.name);
			list_remove_node(fk->rkey->keys, n);
		}
		fk->rkey = NULL;
	}

	if (isGlobal(k->t))
		tr->schema_updates ++;

	sql_trans_drop_dependencies(tr, k->base.id);

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, k->base.id, (k->type == fkey) ? FKEY_DEPENDENCY : KEY_DEPENDENCY);
}

static void
sys_drop_tc(sql_trans *tr, sql_trigger * i, sql_kc *kc)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(syss, "objects");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(systc, "id"), &i->base.id, find_sql_column(systc, "name"), kc->c->base.name, NULL);

	if (is_oid_nil(rid))
		return ;
	table_funcs.table_delete(tr, systc, rid);
	if (isGlobal(i->t))
		tr->schema_updates ++;
}

static void
sys_drop_trigger(sql_trans *tr, sql_trigger * i)
{
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systrigger = find_sql_table(syss, "triggers");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(systrigger, "id"), &i->base.id, NULL);

	if (is_oid_nil(rid))
		return ;
	table_funcs.table_delete(tr, systrigger, rid);

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *tc = n->data;

		sys_drop_tc(tr, i, tc);
	}
	/* remove trigger from schema */
	list_remove_data(i->t->s->triggers, i);
	sql_trans_drop_dependencies(tr, i->base.id);
	if (isGlobal(i->t))
		tr->schema_updates ++;
}

static void
sys_drop_sequence(sql_trans *tr, sql_sequence * seq, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(syss, "sequences");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(sysseqs, "id"), &seq->base.id, NULL);

	if (is_oid_nil(rid))
		return ;

	table_funcs.table_delete(tr, sysseqs, rid);
	sql_trans_drop_dependencies(tr, seq->base.id);
	sql_trans_drop_any_comment(tr, seq->base.id);
	if (drop_action)
		sql_trans_drop_all_dependencies(tr, seq->base.id, SEQ_DEPENDENCY);
}

static void
sys_drop_statistics(sql_trans *tr, sql_column *col)
{
	if (isGlobal(col->t)) {
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *sysstats = find_sql_table(syss, "statistics");

		oid rid = table_funcs.column_find_row(tr, find_sql_column(sysstats, "column_id"), &col->base.id, NULL);

		if (is_oid_nil(rid))
			return ;

		table_funcs.table_delete(tr, sysstats, rid);
	}
}

static int
sys_drop_default_object(sql_trans *tr, sql_column *col, int drop_action)
{
	const char *next_value_for = "next value for ";

	/* Drop sequence for generated column if it's the case */
	if (col->def && !strncmp(col->def, next_value_for, strlen(next_value_for))) {
		sql_schema *s = NULL;
		sql_sequence *seq = NULL;
		node *n = NULL;
		char *schema = NULL, *seq_name = NULL;

		extract_schema_and_sequence_name(tr->sa, col->def + strlen(next_value_for), &schema, &seq_name);
		if (!schema || !seq_name || !(s = find_sql_schema(tr, schema)))
			return -1;

		n = cs_find_name(&s->seqs, seq_name);
		seq = find_sql_sequence(s, seq_name);
		if (seq && n && sql_trans_get_dependency_type(tr, seq->base.id, BEDROPPED_DEPENDENCY) > 0) {
			sys_drop_sequence(tr, seq, drop_action);
			seq->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
			cs_del(&s->seqs, n, seq->base.flags);
		}
	}
	return 0;
}

static int
sys_drop_column(sql_trans *tr, sql_column *col, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(syss, "_columns");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(syscolumn, "id"),
				  &col->base.id, NULL);

	if (is_oid_nil(rid))
		return 0;
	table_funcs.table_delete(tr, syscolumn, rid);
	sql_trans_drop_dependencies(tr, col->base.id);
	sql_trans_drop_any_comment(tr, col->base.id);
	sql_trans_drop_obj_priv(tr, col->base.id);
	if (sys_drop_default_object(tr, col, drop_action) == -1)
		return -1;

	if (isGlobal(col->t))
		tr->schema_updates ++;

	sys_drop_statistics(tr, col);
	if (drop_action)
		sql_trans_drop_all_dependencies(tr, col->base.id, COLUMN_DEPENDENCY);
	if (col->type.type->s)
		sql_trans_drop_dependency(tr, col->base.id, col->type.type->base.id, TYPE_DEPENDENCY);
	return 0;
}

static void
sys_drop_keys(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (cs_size(&t->keys))
		for (n = t->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			sys_drop_key(tr, k, drop_action);
		}
}

static void
sys_drop_idxs(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (cs_size(&t->idxs))
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *k = n->data;

			sys_drop_idx(tr, k, drop_action);
		}
}

static int
sys_drop_columns(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (cs_size(&t->columns))
		for (n = t->columns.set->h; n; n = n->next) {
			sql_column *c = n->data;

			if (sys_drop_column(tr, c, drop_action))
				return -1;
		}
	return 0;
}

static void
sys_drop_part(sql_trans *tr, sql_table *t, int drop_action)
{
	while(t->partition>0) {
		sql_part *pt = partition_find_part(tr, t, NULL);

		assert(pt);
		sql_trans_del_table(tr, pt->t, t, drop_action);
	}
}

static void
sys_drop_parts(sql_trans *tr, sql_table *t, int drop_action)
{
	if (!list_empty(t->members)) {
		for (node *n = t->members->h; n; ) {
			sql_part *pt = n->data;

			n = n->next;
			if ((drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) &&
				tr->dropped && list_find_id(tr->dropped, pt->base.id))
				continue;

			sql_trans_del_table(tr, t, find_sql_table_id(t->s, pt->base.id), drop_action);
		}
	}
}

static int
sys_drop_table(sql_trans *tr, sql_table *t, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(syss, "_tables");
	sql_column *syscol = find_sql_column(systable, "id");
	oid rid = table_funcs.column_find_row(tr, syscol, &t->base.id, NULL);

	if (is_oid_nil(rid))
		return 0;
	table_funcs.table_delete(tr, systable, rid);
	sys_drop_keys(tr, t, drop_action);
	sys_drop_idxs(tr, t, drop_action);

	if (isPartition(t))
		sys_drop_part(tr, t, drop_action);

	if (isMergeTable(t) || isReplicaTable(t))
		sys_drop_parts(tr, t, drop_action);

	if (isRangePartitionTable(t) || isListPartitionTable(t)) {
		sql_table *partitions = find_sql_table(syss, "table_partitions");
		sql_column *pcols = find_sql_column(partitions, "table_id");
		rids *rs = table_funcs.rids_select(tr, pcols, &t->base.id, &t->base.id, NULL);
		oid poid;
		if ((poid = table_funcs.rids_next(rs)) != oid_nil)
			table_funcs.table_delete(tr, partitions, poid);
		table_funcs.rids_destroy(rs);
	}

	sql_trans_drop_any_comment(tr, t->base.id);
	sql_trans_drop_dependencies(tr, t->base.id);
	sql_trans_drop_obj_priv(tr, t->base.id);

	if (sys_drop_columns(tr, t, drop_action))
		return -1;

	if (isGlobal(t))
		tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, t->base.id, !isView(t) ? TABLE_DEPENDENCY : VIEW_DEPENDENCY);
	return 0;
}

static void
sys_drop_type(sql_trans *tr, sql_type *type, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_type = find_sql_table(syss, "types");
	sql_column *sys_type_col = find_sql_column(sys_tab_type, "id");
	oid rid = table_funcs.column_find_row(tr, sys_type_col, &type->base.id, NULL);

	if (is_oid_nil(rid))
		return ;

	table_funcs.table_delete(tr, sys_tab_type, rid);
	sql_trans_drop_dependencies(tr, type->base.id);

	tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, type->base.id, TYPE_DEPENDENCY);
}

static void
sys_drop_func(sql_trans *tr, sql_func *func, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_func = find_sql_table(syss, "functions");
	sql_column *sys_func_col = find_sql_column(sys_tab_func, "id");
	oid rid_func = table_funcs.column_find_row(tr, sys_func_col, &func->base.id, NULL);
	if (is_oid_nil(rid_func))
		return ;
	sql_table *sys_tab_args = find_sql_table(syss, "args");
	sql_column *sys_args_col = find_sql_column(sys_tab_args, "func_id");
	rids *args = table_funcs.rids_select(tr, sys_args_col, &func->base.id, &func->base.id, NULL);

	for (oid r = table_funcs.rids_next(args); !is_oid_nil(r); r = table_funcs.rids_next(args))
		table_funcs.table_delete(tr, sys_tab_args, r);
	table_funcs.rids_destroy(args);

	assert(!is_oid_nil(rid_func));
	table_funcs.table_delete(tr, sys_tab_func, rid_func);

	sql_trans_drop_dependencies(tr, func->base.id);
	sql_trans_drop_any_comment(tr, func->base.id);
	sql_trans_drop_obj_priv(tr, func->base.id);

	tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
}

static void
sys_drop_types(sql_trans *tr, sql_schema *s, int drop_action)
{
	node *n;

	if (cs_size(&s->types))
		for (n = s->types.set->h; n; n = n->next) {
			sql_type *t = n->data;

			sys_drop_type(tr, t, drop_action);
		}
}

static int
sys_drop_tables(sql_trans *tr, sql_schema *s, int drop_action)
{
	node *n;

	if (cs_size(&s->tables))
		for (n = s->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;

			if (sys_drop_table(tr, t, drop_action))
				return -1;
		}
	return 0;
}

static void
sys_drop_funcs(sql_trans *tr, sql_schema *s, int drop_action)
{
	node *n;

	if (cs_size(&s->funcs))
		for (n = s->funcs.set->h; n; n = n->next) {
			sql_func *f = n->data;

			sys_drop_func(tr, f, drop_action);
		}
}

static void
sys_drop_sequences(sql_trans *tr, sql_schema *s, int drop_action)
{
	node *n;

	if (cs_size(&s->seqs))
		for (n = s->seqs.set->h; n; n = n->next) {
			sql_sequence *seq = n->data;

			sys_drop_sequence(tr, seq, drop_action);
		}
}

sql_type *
sql_trans_create_type(sql_trans *tr, sql_schema *s, const char *sqlname, unsigned int digits, unsigned int scale, int radix, const char *impl)
{
	sql_type *t;
	sql_table *systype;
	int localtype = ATOMindex(impl);
	sql_class eclass = EC_EXTERNAL;
	int eclass_cast = (int) eclass;

	if (localtype < 0)
		return NULL;
	t = SA_ZNEW(tr->sa, sql_type);
	systype = find_sql_table(find_sql_schema(tr, "sys"), "types");
	base_init(tr->sa, &t->base, next_oid(), TR_NEW, impl);
	t->sqlname = sa_strdup(tr->sa, sqlname);
	t->digits = digits;
	t->scale = scale;
	t->radix = radix;
	t->eclass = eclass;
	t->localtype = localtype;
	t->s = s;

	cs_add(&s->types, t, TR_NEW);
	table_funcs.table_insert(tr, systype, &t->base.id, t->base.name, t->sqlname, &t->digits, &t->scale, &radix, &eclass_cast, &s->base.id);

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	return t;
}

int
sql_trans_drop_type(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	node *n = find_sql_type_node(s, id);
	sql_type *t = n->data;

	sys_drop_type(tr, t, drop_action);

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	cs_del(&s->types, n, t->base.flags);
	return 1;
}

sql_func *
create_sql_func(sql_allocator *sa, const char *func, list *args, list *res, sql_ftype type, sql_flang lang, const char *mod,
				const char *impl, const char *query, bit varres, bit vararg, bit system)
{
	sql_func *t = SA_ZNEW(sa, sql_func);

	base_init(sa, &t->base, next_oid(), TR_NEW, func);
	assert(impl && mod);
	t->imp = (impl)?sa_strdup(sa, impl):NULL;
	t->mod = (mod)?sa_strdup(sa, mod):NULL;
	t->type = type;
	t->lang = lang;
	t->sql = (lang==FUNC_LANG_SQL||lang==FUNC_LANG_MAL);
	t->semantics = TRUE;
	t->side_effect = (type==F_FILT || (res && (lang==FUNC_LANG_SQL || !list_empty(args))))?FALSE:TRUE;
	t->varres = varres;
	t->vararg = vararg;
	t->ops = args;
	t->res = res;
	t->query = (query)?sa_strdup(sa, query):NULL;
	t->fix_scale = SCALE_EQ;
	t->s = NULL;
	t->system = system;
	return t;
}

sql_func *
sql_trans_create_func(sql_trans *tr, sql_schema *s, const char *func, list *args, list *res, sql_ftype type, sql_flang lang,
					  const char *mod, const char *impl, const char *query, bit varres, bit vararg, bit system)
{
	sql_func *t = SA_ZNEW(tr->sa, sql_func);
	sql_table *sysfunc = find_sql_table(find_sql_schema(tr, "sys"), "functions");
	sql_table *sysarg = find_sql_table(find_sql_schema(tr, "sys"), "args");
	node *n;
	int number = 0, ftype = (int) type, flang = (int) lang;
	bit se;

	base_init(tr->sa, &t->base, next_oid(), TR_NEW, func);
	assert(impl && mod);
	t->imp = (impl)?sa_strdup(tr->sa, impl):NULL;
	t->mod = (mod)?sa_strdup(tr->sa, mod):NULL;
	t->type = type;
	t->lang = lang;
	t->sql = (lang==FUNC_LANG_SQL||lang==FUNC_LANG_MAL);
	t->semantics = TRUE;
	se = t->side_effect = (type==F_FILT || (res && (lang==FUNC_LANG_SQL || !list_empty(args))))?FALSE:TRUE;
	t->varres = varres;
	t->vararg = vararg;
	t->ops = sa_list(tr->sa);
	t->fix_scale = SCALE_EQ;
	t->system = system;
	for (n=args->h; n; n = n->next)
		list_append(t->ops, arg_dup(tr, s, n->data));
	if (res) {
		t->res = sa_list(tr->sa);
		for (n=res->h; n; n = n->next)
			list_append(t->res, arg_dup(tr, s, n->data));
	}
	t->query = (query)?sa_strdup(tr->sa, query):NULL;
	t->s = s;

	cs_add(&s->funcs, t, TR_NEW);
	table_funcs.table_insert(tr, sysfunc, &t->base.id, t->base.name, query?query:t->imp, t->mod, &flang, &ftype, &se,
							 &t->varres, &t->vararg, &s->base.id, &t->system, &t->semantics);
	if (t->res) for (n = t->res->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid();
		table_funcs.table_insert(tr, sysarg, &id, &t->base.id, a->name, a->type.type->sqlname, &a->type.digits, &a->type.scale, &a->inout, &number);
	}
	if (t->ops) for (n = t->ops->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid();
		table_funcs.table_insert(tr, sysarg, &id, &t->base.id, a->name, a->type.type->sqlname, &a->type.digits, &a->type.scale, &a->inout, &number);
	}

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	return t;
}

int
sql_trans_drop_func(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	node *n = find_sql_func_node(s, id);
	sql_func *func = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = func->base.id;
		list_append(tr->dropped, local_id);
	}

	sys_drop_func(tr, func, DROP_CASCADE);

	func->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	cs_del(&s->funcs, n, func->base.flags);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

static void
build_drop_func_list_item(sql_trans *tr, sql_schema *s, sqlid id)
{
	node *n = find_sql_func_node(s, id);
	sql_func *func = n->data;

	sys_drop_func(tr, func, DROP_CASCADE);

	func->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	cs_del(&s->funcs, n, func->base.flags);
}

int
sql_trans_drop_all_func(sql_trans *tr, sql_schema *s, list *list_func, int drop_action)
{
	node *n = NULL;
	sql_func *func = NULL;
	list* to_drop = NULL;

	(void) drop_action;

	if (!tr->dropped) {
		tr->dropped = list_create((fdestroy) GDKfree);
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
			build_drop_func_list_item(tr, s, func->base.id);
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
	sql_schema *s = SA_ZNEW(tr->sa, sql_schema);
	sql_table *sysschema = find_sql_table(find_sql_schema(tr, "sys"), "schemas");

	base_init(tr->sa, &s->base, next_oid(), TR_NEW, name);
	s->auth_id = auth_id;
	s->owner = owner;
	s->system = FALSE;
	cs_new(&s->tables, tr->sa, (fdestroy) &table_destroy);
	cs_new(&s->types, tr->sa, (fdestroy) NULL);
	cs_new(&s->funcs, tr->sa, (fdestroy) NULL);
	cs_new(&s->seqs, tr->sa, (fdestroy) NULL);
	cs_new(&s->parts, tr->sa, (fdestroy) &part_destroy);
	s->keys = list_new(tr->sa, (fdestroy) NULL);
	s->idxs = list_new(tr->sa, (fdestroy) NULL);
	s->triggers = list_new(tr->sa, (fdestroy) NULL);
	s->tr = tr;

	cs_add(&tr->schemas, s, TR_NEW);
	table_funcs.table_insert(tr, sysschema, &s->base.id, s->base.name, &s->auth_id, &s->owner, &s->system);
	s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	return s;
}

sql_schema*
sql_trans_rename_schema(sql_trans *tr, sqlid id, const char *new_name)
{
	sql_table *sysschema = find_sql_table(find_sql_schema(tr, "sys"), "schemas");
	node *n = find_sql_schema_node(tr, id);
	sql_schema *s = n->data;
	oid rid;

	assert(!strNil(new_name));

	list_hash_delete(tr->schemas.set, s, NULL); /* has to re-hash the entry in the changeset */
	s->base.name = sa_strdup(tr->sa, new_name);
	if (!list_hash_add(tr->schemas.set, s, NULL))
		return NULL;

	rid = table_funcs.column_find_row(tr, find_sql_column(sysschema, "id"), &s->base.id, NULL);
	assert(!is_oid_nil(rid));
	table_funcs.column_update_value(tr, find_sql_column(sysschema, "name"), rid, (void*) new_name);

	s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	return s;
}

int
sql_trans_drop_schema(sql_trans *tr, sqlid id, int drop_action)
{
	node *n = find_sql_schema_node(tr, id);
	sql_schema *s = n->data;
	sql_table *sysschema = find_sql_table(find_sql_schema(tr, "sys"), "schemas");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(sysschema, "id"), &s->base.id, NULL);

	if (is_oid_nil(rid))
		return 0;
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid* local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = s->base.id;
		list_append(tr->dropped, local_id);
	}

	table_funcs.table_delete(tr, sysschema, rid);
	sys_drop_funcs(tr, s, drop_action);
	if (sys_drop_tables(tr, s, drop_action))
		return -1;
	sys_drop_types(tr, s, drop_action);
	sys_drop_sequences(tr, s, drop_action);
	sql_trans_drop_any_comment(tr, s->base.id);
	sql_trans_drop_obj_priv(tr, s->base.id);

	s->base.wtime = tr->wtime = tr->wstime;
	tr->schema_updates ++;
	cs_del(&tr->schemas, n, s->base.flags);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_table *
sql_trans_add_table(sql_trans *tr, sql_table *mt, sql_table *pt)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(syss, "objects");
	sql_part *p = SA_ZNEW(tr->sa, sql_part);

	/* merge table depends on part table */
	sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
	assert(isMergeTable(mt) || isReplicaTable(mt));
	p->t = mt;
	p->member = pt;
	pt->partition++;
	base_init(tr->sa, &p->base, pt->base.id, TR_NEW, pt->base.name);
	cs_add(&mt->s->parts, p, TR_NEW);
	list_append(mt->members, p);
	mt->s->base.wtime = mt->base.wtime = pt->s->base.wtime = pt->base.wtime = p->base.wtime = tr->wtime = tr->wstime;
	table_funcs.table_insert(tr, sysobj, &mt->base.id, p->base.name, &p->base.id);
	if (isGlobal(mt))
		tr->schema_updates ++;
	return mt;
}

int
sql_trans_add_range_partition(sql_trans *tr, sql_table *mt, sql_table *pt, sql_subtype tpe, ptr min, ptr max,
							  bit with_nills, int update, sql_part **err)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(syss, "objects");
	sql_table *partitions = find_sql_table(syss, "table_partitions");
	sql_table *ranges = find_sql_table(syss, "range_partitions");
	sql_part *p;
	int localtype = tpe.type->localtype, res = 0;
	ValRecord vmin, vmax;
	size_t smin, smax;
	bit to_insert = with_nills;
	oid rid;
	ptr ok;
	sqlid *v;

	vmin = vmax = (ValRecord) {.vtype = TYPE_void,};

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
		base_init(tr->sa, &p->base, pt->base.id, TR_NEW, pt->base.name);
		assert(isMergeTable(mt) || isReplicaTable(mt));
		p->t = mt;
		assert(pt);
		p->member = pt;
		dup_sql_type(tr, mt->s, &tpe, &(p->tpe));
	} else {
		p = find_sql_part_id(mt, pt->base.id);
	}

	/* add range partition values */
	p->part.range.minvalue = sa_alloc(tr->sa, smin);
	p->part.range.maxvalue = sa_alloc(tr->sa, smax);
	memcpy(p->part.range.minvalue, min, smin);
	memcpy(p->part.range.maxvalue, max, smax);
	p->part.range.minlength = smin;
	p->part.range.maxlength = smax;
	p->with_nills = with_nills;

	if (!update) {
		*err = list_append_with_validate(mt->members, p, sql_range_part_validate_and_insert);
	} else {
		*err = list_traverse_with_validate(mt->members, p, sql_range_part_validate_and_insert);
	}
	if (*err) {
		res = -4;
		goto finish;
	}

	if (!update) {
		rid = table_funcs.column_find_row(tr, find_sql_column(partitions, "table_id"), &mt->base.id, NULL);
		assert(!is_oid_nil(rid));

		/* add merge table dependency */
		sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
		v = (sqlid*) table_funcs.column_find_value(tr, find_sql_column(partitions, "id"), rid);
		table_funcs.table_insert(tr, sysobj, &mt->base.id, p->base.name, &p->base.id);
		table_funcs.table_insert(tr, ranges, &pt->base.id, v, VALget(&vmin), VALget(&vmax), &to_insert);
		_DELETE(v);
	} else {
		sql_column *cmin = find_sql_column(ranges, "minimum"), *cmax = find_sql_column(ranges, "maximum"),
				   *wnulls = find_sql_column(ranges, "with_nulls");

		rid = table_funcs.column_find_row(tr, find_sql_column(ranges, "table_id"), &pt->base.id, NULL);
		assert(!is_oid_nil(rid));

		table_funcs.column_update_value(tr, cmin, rid, VALget(&vmin));
		table_funcs.column_update_value(tr, cmax, rid, VALget(&vmax));
		table_funcs.column_update_value(tr, wnulls, rid, &to_insert);
	}

	if (isGlobal(mt))
		tr->schema_updates ++;
	mt->s->base.wtime = mt->base.wtime = pt->s->base.wtime = pt->base.wtime = p->base.wtime = tr->wtime = tr->wstime;

	if (!update) {
		pt->partition++;
		cs_add(&mt->s->parts, p, TR_NEW);
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
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(syss, "objects");
	sql_table *partitions = find_sql_table(syss, "table_partitions");
	sql_table *values = find_sql_table(syss, "value_partitions");
	sql_part *p;
	oid rid;
	int localtype = tpe.type->localtype, i = 0;
	sqlid *v;

	if (!update) {
		p = SA_ZNEW(tr->sa, sql_part);
		base_init(tr->sa, &p->base, pt->base.id, TR_NEW, pt->base.name);
		assert(isMergeTable(mt) || isReplicaTable(mt));
		p->t = mt;
		assert(pt);
		p->member = pt;
		dup_sql_type(tr, mt->s, &tpe, &(p->tpe));
	} else {
		rids *rs;
		p = find_sql_part_id(mt, pt->base.id);

		rs = table_funcs.rids_select(tr, find_sql_column(values, "table_id"), &pt->base.id, &pt->base.id, NULL);
		for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
			table_funcs.table_delete(tr, values, rid); /* eliminate the old values */
		}
		table_funcs.rids_destroy(rs);
	}
	p->with_nills = with_nills;

	rid = table_funcs.column_find_row(tr, find_sql_column(partitions, "table_id"), &mt->base.id, NULL);
	assert(!is_oid_nil(rid));

	v = (sqlid*) table_funcs.column_find_value(tr, find_sql_column(partitions, "id"), rid);

	if (with_nills) { /* store the null value first */
		ValRecord vnnil;
		if (VALinit(&vnnil, TYPE_str, ATOMnilptr(TYPE_str)) == NULL) {
			_DELETE(v);
			return -1;
		}
		table_funcs.table_insert(tr, values, &pt->base.id, v, VALget(&vnnil));
		VALclear(&vnnil);
	}

	for (node *n = vals->h ; n ; n = n->next) {
		sql_part_value *next = (sql_part_value*) n->data;
		ValRecord vvalue;
		ptr ok;

		if (ATOMlen(localtype, next->value) > STORAGE_MAX_VALUE_LENGTH) {
			_DELETE(v);
			return -i - 1;
		}
		ok = VALinit(&vvalue, localtype, next->value);
		if (ok && localtype != TYPE_str)
			ok = VALconvert(TYPE_str, &vvalue);
		if (!ok) {
			_DELETE(v);
			VALclear(&vvalue);
			return -i - 1;
		}
		table_funcs.table_insert(tr, values, &pt->base.id, v, VALget(&vvalue));
		VALclear(&vvalue);
		i++;
	}
	_DELETE(v);

	p->part.values = vals;

	if (!update) {
		*err = list_append_with_validate(mt->members, p, sql_values_part_validate_and_insert);
	} else {
		*err = list_traverse_with_validate(mt->members, p, sql_values_part_validate_and_insert);
	}
	if (*err)
		return -1;

	if (!update) {
		/* add merge table dependency */
		sql_trans_create_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);
		table_funcs.table_insert(tr, sysobj, &mt->base.id, p->base.name, &p->base.id);
	}

	if (isGlobal(mt))
		tr->schema_updates ++;
	mt->s->base.wtime = mt->base.wtime = pt->s->base.wtime = pt->base.wtime = p->base.wtime = tr->wtime = tr->wstime;
	if (!update) {
		pt->partition++;
		cs_add(&mt->s->parts, p, TR_NEW);
	}
	return 0;
}

sql_table*
sql_trans_rename_table(sql_trans *tr, sql_schema *s, sqlid id, const char *new_name)
{
	sql_table *systable = find_sql_table(find_sql_schema(tr, isTempSchema(s) ? "tmp":"sys"), "_tables");
	node *n = find_sql_table_node(s, id);
	sql_table *t = n->data;
	oid rid;

	assert(!strNil(new_name));

	list_hash_delete(s->tables.set, t, NULL); /* has to re-hash the entry in the changeset */
	t->base.name = sa_strdup(tr->sa, new_name);
	if (!list_hash_add(s->tables.set, t, NULL))
		return NULL;

	rid = table_funcs.column_find_row(tr, find_sql_column(systable, "id"), &t->base.id, NULL);
	assert(!is_oid_nil(rid));
	table_funcs.column_update_value(tr, find_sql_column(systable, "name"), rid, (void*) new_name);

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return t;
}

sql_table*
sql_trans_set_table_schema(sql_trans *tr, sqlid id, sql_schema *os, sql_schema *ns)
{
	sql_table *systable = find_sql_table(find_sql_schema(tr, isTempSchema(os) ? "tmp":"sys"), "_tables");
	node *n = find_sql_table_node(os, id);
	sql_table *t = n->data;
	oid rid;
	sql_moved_table *m;

	rid = table_funcs.column_find_row(tr, find_sql_column(systable, "id"), &t->base.id, NULL);
	assert(!is_oid_nil(rid));
	table_funcs.column_update_value(tr, find_sql_column(systable, "schema_id"), rid, &(ns->base.id));

	cs_move(&os->tables, &ns->tables, t);
	t->s = ns;

	if (!tr->moved_tables)
		tr->moved_tables = sa_list(tr->sa);
	m = SA_ZNEW(tr->sa, sql_moved_table); //add transaction log entry
	m->from = os;
	m->to = ns;
	m->t = t;
	list_append(tr->moved_tables, m);

	t->base.wtime = os->base.wtime = ns->base.wtime = tr->wtime = tr->wstime;
	for (node *n = t->columns.set->h ; n ; n = n->next) {
		sql_column *col = (sql_column*) n->data;
		col->base.wtime = tr->wstime; /* the table's columns types have to be set again */
	}
	if (isGlobal(t))
		tr->schema_updates ++;
	return t;
}

sql_table *
sql_trans_del_table(sql_trans *tr, sql_table *mt, sql_table *pt, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(syss, "objects");
	node *n = cs_find_id(&mt->s->parts, pt->base.id);
	oid obj_oid = table_funcs.column_find_row(tr, find_sql_column(sysobj, "nr"), &pt->base.id, NULL), rid;
	sql_part *p = (sql_part*) n->data;

	if (is_oid_nil(obj_oid))
		return NULL;

	if (isRangePartitionTable(mt)) {
		sql_table *ranges = find_sql_table(syss, "range_partitions");
		rid = table_funcs.column_find_row(tr, find_sql_column(ranges, "table_id"), &pt->base.id, NULL);
		table_funcs.table_delete(tr, ranges, rid);
	} else if (isListPartitionTable(mt)) {
		sql_table *values = find_sql_table(syss, "value_partitions");
		rids *rs = table_funcs.rids_select(tr, find_sql_column(values, "table_id"), &pt->base.id, &pt->base.id, NULL);
		for (rid = table_funcs.rids_next(rs); !is_oid_nil(rid); rid = table_funcs.rids_next(rs)) {
			table_funcs.table_delete(tr, values, rid);
		}
		table_funcs.rids_destroy(rs);
	}
	/* merge table depends on part table */
	sql_trans_drop_dependency(tr, pt->base.id, mt->base.id, TABLE_DEPENDENCY);

	cs_del(&mt->s->parts, n, p->base.flags);
	list_remove_data(mt->members, p);
	pt->partition--;/* check other hierarchies? */
	p->member = NULL;
	table_funcs.table_delete(tr, sysobj, obj_oid);

	mt->s->base.wtime = mt->base.wtime = pt->s->base.wtime = pt->base.wtime = p->base.wtime = tr->wtime = tr->wstime;

	if (drop_action == DROP_CASCADE)
		sql_trans_drop_table(tr, mt->s, pt->base.id, drop_action);
	if (isGlobal(mt))
		tr->schema_updates ++;
	return mt;
}

sql_table *
sql_trans_create_table(sql_trans *tr, sql_schema *s, const char *name, const char *sql, int tt, bit system,
					   int persistence, int commit_action, int sz, bte properties)
{
	sql_table *t = create_sql_table(tr->sa, name, tt, system, persistence, commit_action, properties);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(syss, "_tables");
	sht ca;

	/* temps all belong to a special tmp schema and only views/remote
	   have a query */
	assert( (isTable(t) ||
		(!isTempTable(t) || (strcmp(s->base.name, "tmp") == 0) || isDeclaredTable(t))) || (isView(t) && !sql) || isStream(t) || (isRemote(t) && !sql));

	t->query = sql ? sa_strdup(tr->sa, sql) : NULL;
	t->s = s;
	t->sz = sz;
	if (sz < 0)
		t->sz = COLSIZE;
	cs_add(&s->tables, t, TR_NEW);
	if (isStream(t))
		t->persistence = SQL_STREAM;
	if (isRemote(t))
		t->persistence = SQL_REMOTE;

	if (isTable(t)) {
		if (store_funcs.create_del(tr, t) != LOG_OK) {
			TRC_DEBUG(SQL_STORE, "Load table '%s' is missing 'deletes'\n", t->base.name);
			t->persistence = SQL_GLOBAL_TEMP;
		}
	}
	if (isPartitionedByExpressionTable(t)) {
		t->part.pexp = SA_ZNEW(tr->sa, sql_expression);
		t->part.pexp->type = *sql_bind_localtype("void"); /* leave it non-initialized, at the backend the copy of this table will get the type */
	}

	ca = t->commit_action;
	if (!isDeclaredTable(t)) {
		table_funcs.table_insert(tr, systable, &t->base.id, t->base.name, &s->base.id,
								 (t->query) ? t->query : ATOMnilptr(TYPE_str), &t->type, &t->system, &ca, &t->access);
	}

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return t;
}

int
sql_trans_set_partition_table(sql_trans *tr, sql_table *t)
{
	if (t && (isRangePartitionTable(t) || isListPartitionTable(t))) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
		sql_table *partitions = find_sql_table(syss, "table_partitions");
		sqlid next = next_oid();
		if (isPartitionedByColumnTable(t)) {
			assert(t->part.pcol);
			table_funcs.table_insert(tr, partitions, &next, &t->base.id, &t->part.pcol->base.id, ATOMnilptr(TYPE_str), &t->properties);
		} else if (isPartitionedByExpressionTable(t)) {
			assert(t->part.pexp->exp);
			if (strlen(t->part.pexp->exp) > STORAGE_MAX_VALUE_LENGTH)
				return -1;
			table_funcs.table_insert(tr, partitions, &next, &t->base.id, ATOMnilptr(TYPE_int), t->part.pexp->exp, &t->properties);
		} else {
			assert(0);
		}
	}
	return 0;
}

sql_key *
create_sql_kc(sql_allocator *sa, sql_key *k, sql_column *c)
{
	sql_kc *kc = SA_ZNEW(sa, sql_kc);

	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx)
		create_sql_ic(sa, k->idx, c);
	if (k->type == pkey)
		c->null = 0;
	return k;
}

sql_ukey *
create_sql_ukey(sql_allocator *sa, sql_table *t, const char *name, key_type kt)
{
	sql_key *nk = NULL;
	sql_ukey *tk;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);
 	tk = (sql_ukey *) nk;
	assert(name);

	base_init(sa, &nk->base, next_oid(), TR_NEW, name);

	nk->type = kt;
	nk->columns = sa_list(sa);
	nk->idx = NULL;
	nk->t = t;

	tk->keys = NULL;
	if (nk->type == pkey)
		t->pkey = tk;
	cs_add(&t->keys, nk, TR_NEW);
	return tk;
}

sql_fkey *
create_sql_fkey(sql_allocator *sa, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
	sql_key *nk;
	sql_fkey *fk = NULL;
	sql_ukey *uk = (sql_ukey *) rkey;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);

	assert(name);
	base_init(sa, &nk->base, next_oid(), TR_NEW, name);
	nk->type = kt;
	nk->columns = sa_list(sa);
	nk->t = t;
	nk->idx = create_sql_idx(sa, t, name, (nk->type == fkey) ? join_idx : hash_idx);
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;

	fk->rkey = uk;
	cs_add(&t->keys, nk, TR_NEW);
	return (sql_fkey*) nk;
}

sql_idx *
create_sql_ic(sql_allocator *sa, sql_idx *i, sql_column *c)
{
	sql_kc *ic = SA_ZNEW(sa, sql_kc);

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
	if (i->type == hash_idx && list_length(i->columns) == 1 && ic->c->sorted) {
		/*i->type = oph_idx;*/
		i->type = no_idx;
	}
	return i;
}

sql_idx *
create_sql_idx(sql_allocator *sa, sql_table *t, const char *name, idx_type it)
{
	sql_idx *ni = SA_ZNEW(sa, sql_idx);

	base_init(sa, &ni->base, next_oid(), TR_NEW, name);
	ni->columns = sa_list(sa);
	ni->t = t;
	ni->type = it;
	ni->key = NULL;
	cs_add(&t->idxs, ni, TR_NEW);
	return ni;
}

sql_column *
create_sql_column(sql_trans *tr, sql_table *t, const char *name, sql_subtype *tpe)
{
	sql_column *col = SA_ZNEW(tr->sa, sql_column);

	base_init(tr->sa, &col->base, next_oid(), TR_NEW, name);
	col->type = *tpe;
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;

	cs_add(&t->columns, col, TR_NEW);
	return col;
}

int
sql_trans_drop_table(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	node *n = find_sql_table_node(s, id);
	sql_table *t = n->data;

	if ((drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) &&
	    tr->dropped && list_find_id(tr->dropped, t->base.id))
		return 0;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
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

	t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t) || (t->commit_action != CA_DROP))
		tr->schema_updates ++;
	cs_del(&s->tables, n, t->base.flags);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

BUN
sql_trans_clear_table(sql_trans *tr, sql_table *t)
{
	node *n = t->columns.set->h;
	sql_column *c = n->data;
	BUN sz = 0, nsz = 0;

	if (!isNew(t))
		t->cleared = 1;
	t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	c->base.wtime = tr->wstime;

	if ((nsz = store_funcs.clear_col(tr, c)) == BUN_NONE)
		return BUN_NONE;
	sz += nsz;
	if ((nsz = store_funcs.clear_del(tr, t)) == BUN_NONE)
		return BUN_NONE;
	sz -= nsz;

	for (n = n->next; n; n = n->next) {
		c = n->data;
		c->base.wtime = tr->wstime;

		if (store_funcs.clear_col(tr, c) == BUN_NONE)
			return BUN_NONE;
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *ci = n->data;

			ci->base.wtime = tr->wstime;
			if (isTable(ci->t) && idx_has_column(ci->type) &&
				store_funcs.clear_idx(tr, ci) == BUN_NONE)
				return BUN_NONE;
		}
	}
	return sz;
}

sql_column *
sql_trans_create_column(sql_trans *tr, sql_table *t, const char *name, sql_subtype *tpe)
{
	sql_column *col;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(syss, "_columns");

	if (!tpe)
		return NULL;

	if (t->system && sql_trans_name_conflict(tr, t->s->base.name, t->base.name, name))
		return NULL;
	col = create_sql_column(tr, t, name, tpe);

	if (isTable(col->t))
		if (store_funcs.create_col(tr, col) != LOG_OK)
			return NULL;
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, syscolumn, &col->base.id, col->base.name, col->type.type->sqlname, &col->type.digits, &col->type.scale, &t->base.id, (col->def) ? col->def : ATOMnilptr(TYPE_str), &col->null, &col->colnr, (col->storage_type) ? col->storage_type : ATOMnilptr(TYPE_str));

	col->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (tpe->type->s) /* column depends on type */
		sql_trans_create_dependency(tr, tpe->type->base.id, col->base.id, TYPE_DEPENDENCY);
	if (isGlobal(t))
		tr->schema_updates ++;
	return col;
}

void
drop_sql_column(sql_table *t, sqlid id, int drop_action)
{
	node *n = list_find_base_id(t->columns.set, id);
	sql_column *col = n->data;

	col->drop_action = drop_action;
	cs_del(&t->columns, n, 0);
}

void
drop_sql_idx(sql_table *t, sqlid id)
{
	node *n = list_find_base_id(t->idxs.set, id);

	cs_del(&t->idxs, n, 0);
}

void
drop_sql_key(sql_table *t, sqlid id, int drop_action)
{
	node *n = list_find_base_id(t->keys.set, id);
	sql_key *k = n->data;

	k->drop_action = drop_action;
	cs_del(&t->keys, n, 0);
}

sql_column*
sql_trans_rename_column(sql_trans *tr, sql_table *t, const char *old_name, const char *new_name)
{
	sql_table *syscolumn = find_sql_table(find_sql_schema(tr, isGlobal(t)?"sys":"tmp"), "_columns");
	sql_column *c = find_sql_column(t, old_name);
	oid rid;

	assert(!strNil(new_name));

	list_hash_delete(t->columns.set, c, NULL); /* has to re-hash the entry in the changeset */
	c->base.name = sa_strdup(tr->sa, new_name);
	if (!list_hash_add(t->columns.set, c, NULL))
		return NULL;

	rid = table_funcs.column_find_row(tr, find_sql_column(syscolumn, "id"), &c->base.id, NULL);
	assert(!is_oid_nil(rid));
	table_funcs.column_update_value(tr, find_sql_column(syscolumn, "name"), rid, (void*) new_name);

	c->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return c;
}

int
sql_trans_drop_column(sql_trans *tr, sql_table *t, sqlid id, int drop_action)
{
	node *n = NULL;
	sql_table *syscolumn = find_sql_table(find_sql_schema(tr, isGlobal(t)?"sys":"tmp"), "_columns");
	sql_column *col = NULL, *cid = find_sql_column(syscolumn, "id"), *cnr = find_sql_column(syscolumn, "number");

	for (node *nn = t->columns.set->h ; nn ; nn = nn->next) {
		sql_column *next = (sql_column *) nn->data;
		if (next->base.id == id) {
			n = nn;
			col = next;
		} else if (col) { /* if the column to be dropped was found, decrease the column number for others after it */
			oid rid;
			next->colnr--;

			rid = table_funcs.column_find_row(tr, cid, &next->base.id, NULL);
			assert(!is_oid_nil(rid));
			table_funcs.column_update_value(tr, cnr, rid, &next->colnr);

			next->base.wtime = tr->wtime = tr->wstime;
		}
	}

	assert(n && col); /* the column to be dropped must have been found */

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
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

	col->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	cs_del(&t->columns, n, col->base.flags);
	if (isGlobal(t))
		tr->schema_updates ++;

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_column *
sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull)
{
	if (col->null != isnull) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(syss, "_columns");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(syscolumn, "id"),
					  &col->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		table_funcs.column_update_value(tr, find_sql_column(syscolumn, "null"), rid, &isnull);
		col->null = isnull;
		col->base.wtime = col->t->base.wtime = col->t->s->base.wtime = tr->wtime = tr->wstime;
		if (isGlobal(col->t))
			tr->schema_updates ++;
	}

	return col;
}

sql_table *
sql_trans_alter_access(sql_trans *tr, sql_table *t, sht access)
{
	if (t->access != access) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
		sql_table *systable = find_sql_table(syss, "_tables");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(systable, "id"),
					  &t->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		table_funcs.column_update_value(tr, find_sql_column(systable, "access"), rid, &access);
		t->access = access;
		t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
		if (isGlobal(t))
			tr->schema_updates ++;
	}
	return t;
}

sql_column *
sql_trans_alter_default(sql_trans *tr, sql_column *col, char *val)
{
	if (!col->def && !val)
		return col;	/* no change */

	if (!col->def || !val || strcmp(col->def, val) != 0) {
		void *p = val ? val : (void *) ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "default");
		oid rid = table_funcs.column_find_row(tr, col_ids, &col->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		if (sys_drop_default_object(tr, col, 0) == -1)
			return NULL;
		table_funcs.column_update_value(tr, col_dfs, rid, p);
		col->def = NULL;
		if (val)
			col->def = sa_strdup(tr->sa, val);
		col->base.wtime = col->t->base.wtime = col->t->s->base.wtime = tr->wtime = tr->wstime;
		if (isGlobal(col->t))
			tr->schema_updates ++;
	}
	return col;
}

sql_column *
sql_trans_alter_storage(sql_trans *tr, sql_column *col, char *storage)
{
	if (!col->storage_type && !storage)
		return col;	/* no change */

	if (!col->storage_type || !storage || strcmp(col->storage_type, storage) != 0) {
		void *p = storage ? storage : (void *) ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp");
		sql_table *syscolumn = find_sql_table(syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "storage");
		oid rid = table_funcs.column_find_row(tr, col_ids, &col->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		table_funcs.column_update_value(tr, col_dfs, rid, p);
		col->storage_type = NULL;
		if (storage)
			col->storage_type = sa_strdup(tr->sa, storage);
		col->base.wtime = col->t->base.wtime = col->t->s->base.wtime = tr->wtime = tr->wstime;
		if (isGlobal(col->t))
			tr->schema_updates ++;
	}
	return col;
}

int
sql_trans_is_sorted( sql_trans *tr, sql_column *col )
{
	if (col && isTable(col->t) && store_funcs.sorted_col && store_funcs.sorted_col(tr, col))
		return 1;
	return 0;
}

int
sql_trans_is_unique( sql_trans *tr, sql_column *col )
{
	if (col && isTable(col->t) && store_funcs.unique_col && store_funcs.unique_col(tr, col))
		return 1;
	return 0;
}

int
sql_trans_is_duplicate_eliminated( sql_trans *tr, sql_column *col )
{
	if (col && isTable(col->t) && EC_VARCHAR(col->type.type->eclass) && store_funcs.double_elim_col)
		return store_funcs.double_elim_col(tr, col);
	return 0;
}

size_t
sql_trans_dist_count( sql_trans *tr, sql_column *col )
{
	if (col->dcount)
		return col->dcount;

	if (col && isTable(col->t)) {
		/* get from statistics */
		sql_schema *sys = find_sql_schema(tr, "sys");
		sql_table *stats = find_sql_table(sys, "statistics");
		if (stats) {
			sql_column *stats_column_id = find_sql_column(stats, "column_id");
			oid rid = table_funcs.column_find_row(tr, stats_column_id, &col->base.id, NULL);
			if (!is_oid_nil(rid)) {
				sql_column *stats_unique = find_sql_column(stats, "unique");
				void *v = table_funcs.column_find_value(tr, stats_unique, rid);

				col->dcount = *(size_t*)v;
				_DELETE(v);
			} else { /* sample and put in statistics */
				col->dcount = store_funcs.dcount_col(tr, col);
			}
		}
		return col->dcount;
	}
	return 0;
}

int
sql_trans_ranges( sql_trans *tr, sql_column *col, char **min, char **max )
{
	*min = NULL;
	*max = NULL;
	if (col && isTable(col->t)) {
		/* get from statistics */
		sql_schema *sys = find_sql_schema(tr, "sys");
		sql_table *stats = find_sql_table(sys, "statistics");

		if (col->min && col->max) {
			*min = col->min;
			*max = col->max;
			return 1;
		}
		if (stats) {
			sql_column *stats_column_id = find_sql_column(stats, "column_id");
			oid rid = table_funcs.column_find_row(tr, stats_column_id, &col->base.id, NULL);
			if (!is_oid_nil(rid)) {
				char *v;
				sql_column *stats_min = find_sql_column(stats, "minval");
				sql_column *stats_max = find_sql_column(stats, "maxval");

				v = table_funcs.column_find_value(tr, stats_min, rid);
				*min = col->min = sa_strdup(tr->sa, v);
				_DELETE(v);
				v = table_funcs.column_find_value(tr, stats_max, rid);
				*max = col->max = sa_strdup(tr->sa, v);
				_DELETE(v);
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
	int neg = -1;
	int action = -1;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(syss, "keys");
	sql_ukey *uk = NULL;

	if (isTempTable(t))
		return NULL;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(tr->sa, sql_ukey)
	: (sql_key *) SA_ZNEW(tr->sa, sql_fkey);

	assert(name);
	base_init(tr->sa, &nk->base, next_oid(), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_new(tr->sa, (fdestroy) NULL);
	nk->t = t;
	nk->idx = NULL;

	uk = (sql_ukey *) nk;

	uk->keys = NULL;

	if (nk->type == pkey)
		t->pkey = uk;

	cs_add(&t->keys, nk, TR_NEW);
	list_append(t->s->keys, nk);

	table_funcs.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey->k.base.id : &neg, &action );

	syskey->base.wtime = syskey->s->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return nk;
}

sql_fkey *
sql_trans_create_fkey(sql_trans *tr, sql_table *t, const char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
/* can only have keys between persistent tables */
	int neg = -1;
	int action = (on_update<<8) + on_delete;
	sql_key *nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(syss, "keys");
	sql_fkey *fk = NULL;
	sql_ukey *uk = (sql_ukey *) rkey;

	if (isTempTable(t))
		return NULL;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(tr->sa, sql_ukey)
	: (sql_key *) SA_ZNEW(tr->sa, sql_fkey);

	assert(name);
	base_init(tr->sa, &nk->base, next_oid(), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_new(tr->sa, (fdestroy) NULL);
	nk->t = t;
	nk->idx = sql_trans_create_idx(tr, t, name, (nk->type == fkey) ? join_idx : hash_idx);
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;

	fk->rkey = uk;
	if (!uk->keys)
		uk->keys = list_new(tr->sa, NULL);
	list_append(uk->keys, fk);

	cs_add(&t->keys, nk, TR_NEW);
	list_append(t->s->keys, nk);

	table_funcs.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey->k.base.id : &neg, &action);

	sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey->k.base.id, nk->base.id, FKEY_DEPENDENCY);

	syskey->base.wtime = syskey->s->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return (sql_fkey*) nk;
}

sql_key *
sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c )
{
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(k->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(syss, "objects");

	assert(c);
	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx)
		sql_trans_create_ic(tr, k->idx, c);

	if (k->type == pkey) {
		sql_trans_create_dependency(tr, c->base.id, k->base.id, KEY_DEPENDENCY);
		sql_trans_alter_null(tr, c, 0);
	}

	table_funcs.table_insert(tr, syskc, &k->base.id, kc->c->base.name, &nr);

	syskc->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(k->t))
		tr->schema_updates ++;
	return k;
}

sql_fkey *
sql_trans_create_fkc(sql_trans *tr, sql_fkey *fk, sql_column *c )
{
	sql_key *k = (sql_key *) fk;
	sql_kc *kc = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(k->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(syss, "objects");

	assert(c);
	kc->c = c;
	list_append(k->columns, kc);
	if (k->idx)
		sql_trans_create_ic(tr, k->idx, c);

	sql_trans_create_dependency(tr, c->base.id, k->base.id, FKEY_DEPENDENCY);

	table_funcs.table_insert(tr, syskc, &k->base.id, kc->c->base.name, &nr);

	syskc->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(k->t))
		tr->schema_updates ++;
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
	if (t->idxs.set) for ( n = t->idxs.set->h; n; n = n->next ) {
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
key_create_done(sql_allocator *sa, sql_key *k)
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
	k->idx = create_sql_idx(sa, k->t, k->base.name, hash_idx);
	k->idx->key = k;

	for (n=k->columns->h; n; n = n->next) {
		sql_kc *kc = n->data;

		create_sql_ic(sa, k->idx, kc->c);
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
	node *n = list_find_base_id(s->keys, id);
	sql_key *k = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id) {
			return -1;
		}

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
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
	n = cs_find_name(&k->t->keys, k->base.name);
	if (n)
		cs_del(&k->t->keys, n, k->base.flags);

	k->base.wtime = k->t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(k->t))
		tr->schema_updates ++;

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
	sql_idx *ni = SA_ZNEW(tr->sa, sql_idx);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(syss, "idxs");

	assert(name);
	base_init(tr->sa, &ni->base, next_oid(), TR_NEW, name);
	ni->type = it;
	ni->columns = list_new(tr->sa, (fdestroy) NULL);
	ni->t = t;
	ni->key = NULL;

	cs_add(&t->idxs, ni, TR_NEW);
	list_append(t->s->idxs, ni);

	if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
		store_funcs.create_idx(tr, ni);
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, ni->base.name);
	ni->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return ni;
}

sql_idx *
sql_trans_create_ic(sql_trans *tr, sql_idx * i, sql_column *c)
{
	sql_kc *ic = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(i->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(syss, "objects");

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
	    store_funcs.count_col(tr, ic->c) && store_funcs.sorted_col(tr, ic->c)) {
		sql_table *sysidx = find_sql_table(syss, "idxs");
		sql_column *sysidxid = find_sql_column(sysidx, "id");
		sql_column *sysidxtype = find_sql_column(sysidx, "type");
		oid rid = table_funcs.column_find_row(tr, sysidxid, &i->base.id, NULL);

		if (is_oid_nil(rid))
			return NULL;
		/*i->type = oph_idx;*/
		i->type = no_idx;
		table_funcs.column_update_value(tr, sysidxtype, rid, &i->type);
	}
#endif

	table_funcs.table_insert(tr, sysic, &i->base.id, ic->c->base.name, &nr);
	sysic->base.wtime = sysic->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(i->t))
		tr->schema_updates ++;
	return i;
}

int
sql_trans_drop_idx(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	node *n = list_find_base_id(s->idxs, id);
	sql_idx *i;

	if (!n) /* already dropped */
		return 0;

	i = n->data;
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id) {
			return -1;
		}

		if (!tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
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

	i->base.wtime = i->t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(i->t))
		tr->schema_updates ++;
	n = cs_find_name(&i->t->idxs, i->base.name);
	if (n)
		cs_del(&i->t->idxs, n, i->base.flags);

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
	sql_trigger *ni = SA_ZNEW(tr->sa, sql_trigger);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systrigger = find_sql_table(syss, "triggers");
	const char *nilptr = ATOMnilptr(TYPE_str);

	assert(name);
	base_init(tr->sa, &ni->base, next_oid(), TR_NEW, name);
	ni->columns = list_new(tr->sa, (fdestroy) NULL);
	ni->t = t;
	ni->time = time;
	ni->orientation = orientation;
	ni->event = event;
	ni->old_name = ni->new_name = ni->condition = NULL;
	if (old_name)
		ni->old_name = sa_strdup(tr->sa, old_name);
	if (new_name)
		ni->new_name = sa_strdup(tr->sa, new_name);
	if (condition)
		ni->condition = sa_strdup(tr->sa, condition);
	ni->statement = sa_strdup(tr->sa, statement);

	cs_add(&t->triggers, ni, TR_NEW);
	list_append(t->s->triggers, ni);

	table_funcs.table_insert(tr, systrigger, &ni->base.id, ni->base.name, &t->base.id, &ni->time, &ni->orientation,
							 &ni->event, (ni->old_name)?ni->old_name:nilptr, (ni->new_name)?ni->new_name:nilptr,
							 (ni->condition)?ni->condition:nilptr, ni->statement);

	t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(t))
		tr->schema_updates ++;
	return ni;
}

sql_trigger *
sql_trans_create_tc(sql_trans *tr, sql_trigger * i, sql_column *c )
{
	sql_kc *ic = SA_ZNEW(tr->sa, sql_kc);
	int nr = list_length(i->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(syss, "objects");

	assert(c);
	ic->c = c;
	list_append(i->columns, ic);
	table_funcs.table_insert(tr, systc, &i->base.id, ic->c->base.name, &nr);
	systc->base.wtime = systc->s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(i->t))
		tr->schema_updates ++;
	return i;
}

int
sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, sqlid id, int drop_action)
{
	node *n = list_find_base_id(s->triggers, id);
	sql_trigger *i = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		sqlid *local_id = MNEW(sqlid);
		if (!local_id)
			return -1;

		if (! tr->dropped) {
			tr->dropped = list_create((fdestroy) GDKfree);
			if (!tr->dropped) {
				_DELETE(local_id);
				return -1;
			}
		}
		*local_id = i->base.id;
		list_append(tr->dropped, local_id);
	}

	sys_drop_trigger(tr, i);
	i->base.wtime = i->t->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	if (isGlobal(i->t))
		tr->schema_updates ++;
	n = cs_find_name(&i->t->triggers, i->base.name);
	if (n)
		cs_del(&i->t->triggers, n, i->base.flags);

	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	return 0;
}

sql_sequence *
create_sql_sequence(sql_allocator *sa, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
					lng cacheinc, bit cycle)
{
	sql_sequence *seq = SA_ZNEW(sa, sql_sequence);

	assert(name);
	base_init(sa, &seq->base, next_oid(), TR_NEW, name);
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
sql_trans_create_sequence(sql_trans *tr, sql_schema *s, const char *name, lng start, lng min, lng max, lng inc,
						  lng cacheinc, bit cycle, bit bedropped)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(syss, "sequences");
	sql_sequence *seq = create_sql_sequence(tr->sa, s, name, start, min, max, inc, cacheinc, cycle);

	cs_add(&s->seqs, seq, TR_NEW);
	table_funcs.table_insert(tr, sysseqs, &seq->base.id, &s->base.id, seq->base.name, &seq->start, &seq->minvalue,
							 &seq->maxvalue, &seq->increment, &seq->cacheinc, &seq->cycle);
	s->base.wtime = tr->wtime = tr->wstime;

	/*Create a BEDROPPED dependency for a SERIAL COLUMN*/
	if (bedropped)
		sql_trans_create_dependency(tr, seq->base.id, seq->base.id, BEDROPPED_DEPENDENCY);

	return seq;
}

void
sql_trans_drop_sequence(sql_trans *tr, sql_schema *s, sql_sequence *seq, int drop_action)
{
	node *n = cs_find_name(&s->seqs, seq->base.name);
	sys_drop_sequence(tr, seq, drop_action);
	seq->base.wtime = s->base.wtime = tr->wtime = tr->wstime;
	cs_del(&s->seqs, n, seq->base.flags);
	tr->schema_updates ++;
}

sql_sequence *
sql_trans_alter_sequence(sql_trans *tr, sql_sequence *seq, lng min, lng max, lng inc, lng cache, bit cycle)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(syss, "sequences");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
	sql_column *c;
	int changed = 0;

	if (is_oid_nil(rid))
		return NULL;
	if (!is_lng_nil(min) && seq->minvalue != min) {
		seq->minvalue = min;
		c = find_sql_column(seqs, "minvalue");
		table_funcs.column_update_value(tr, c, rid, &seq->minvalue);
	}
	if (!is_lng_nil(max) && seq->maxvalue != max) {
		seq->maxvalue = max;
		changed = 1;
		c = find_sql_column(seqs, "maxvalue");
		table_funcs.column_update_value(tr, c, rid, &seq->maxvalue);
	}
	if (!is_lng_nil(inc) && seq->increment != inc) {
		seq->increment = inc;
		changed = 1;
		c = find_sql_column(seqs, "increment");
		table_funcs.column_update_value(tr, c, rid, &seq->increment);
	}
	if (!is_lng_nil(cache) && seq->cacheinc != cache) {
		seq->cacheinc = cache;
		changed = 1;
		c = find_sql_column(seqs, "cacheinc");
		table_funcs.column_update_value(tr, c, rid, &seq->cacheinc);
	}
	if (!is_lng_nil(cycle) && seq->cycle != cycle) {
		seq->cycle = cycle != 0;
		changed = 1;
		c = find_sql_column(seqs, "cycle");
		table_funcs.column_update_value(tr, c, rid, &seq->cycle);
	}

	if (changed) {
		seq->base.wtime = seq->s->base.wtime = tr->wtime = tr->wstime;
		tr->schema_updates ++;
	}
	return seq;
}

sql_sequence *
sql_trans_sequence_restart(sql_trans *tr, sql_sequence *seq, lng start)
{
	if (!is_lng_nil(start) && seq->start != start) { /* new valid value, change */
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *seqs = find_sql_table(syss, "sequences");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
		sql_column *c = find_sql_column(seqs, "start");

		assert(!is_oid_nil(rid));
		seq->start = start;
		table_funcs.column_update_value(tr, c, rid, &start);

		seq->base.wtime = seq->s->base.wtime = tr->wtime = tr->wstime;
		tr->schema_updates ++;
	}
	return seq_restart(seq, start) ? seq : NULL;
}

sql_sequence *
sql_trans_seqbulk_restart(sql_trans *tr, seqbulk *sb, lng start)
{
	sql_sequence *seq = sb->seq;
	if (!is_lng_nil(start) && seq->start != start) { /* new valid value, change */
		sql_schema *syss = find_sql_schema(tr, "sys");
		sql_table *seqs = find_sql_table(syss, "sequences");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
		sql_column *c = find_sql_column(seqs, "start");

		assert(!is_oid_nil(rid));
		seq->start = start;
		table_funcs.column_update_value(tr, c, rid, &start);

		seq->base.wtime = seq->s->base.wtime = tr->wtime = tr->wstime;
		tr->schema_updates ++;
	}
	return seqbulk_restart(sb, start) ? seq : NULL;
}

sql_session *
sql_session_create(int ac)
{
	sql_session *s;

	if (store_singleuser && ATOMIC_GET(&nr_sessions))
		return NULL;

	s = ZNEW(sql_session);
	if (!s)
		return NULL;
	s->tr = sql_trans_create(NULL, NULL, true);
	if (!s->tr) {
		_DELETE(s);
		return NULL;
	}
	s->schema_name = NULL;
	s->tr->active = 0;
	list_append(passive_sessions, s);
	if (!sql_session_reset(s, ac)) {
		sql_trans_destroy(s->tr, true);
		_DELETE(s);
		return NULL;
	}
	(void) ATOMIC_INC(&nr_sessions);
	return s;
}

void
sql_session_destroy(sql_session *s)
{
	assert(!s->tr || s->tr->active == 0);
	if (s->tr)
		sql_trans_destroy(s->tr, true);
	if (s->schema_name)
		_DELETE(s->schema_name);
	list_remove_data(passive_sessions, s);
	(void) ATOMIC_DEC(&nr_sessions);
	_DELETE(s);
}

static void
sql_trans_reset_tmp(sql_trans *tr, int commit)
{
	sql_schema *tmp = find_sql_schema(tr, "tmp");

	if (commit == 0 && tmp->tables.nelm) {
		for (node *n = tmp->tables.nelm; n; ) {
			node *nxt = n->next;

			cs_remove_node(&tmp->tables, n);
			n = nxt;
		}
	}
	tmp->tables.nelm = NULL;
	if (tmp->tables.set) {
		node *n;
		for (n = tmp->tables.set->h; n; ) {
			node *nxt = n->next;
			sql_table *tt = n->data;

			if ((isGlobal(tt) && tt->commit_action != CA_PRESERVE) || tt->commit_action == CA_DELETE) {
				sql_trans_clear_table(tr, tt);
			} else if (tt->commit_action == CA_DROP) {
				(void) sql_trans_drop_table(tr, tt->s, tt->base.id, DROP_RESTRICT);
			}
			n = nxt;
		}
	}
}

int
sql_session_reset(sql_session *s, int ac)
{
	sql_schema *tmp;
	char *def_schema_name = _STRDUP("sys");

	if (!s->tr || !def_schema_name) {
		if (def_schema_name)
			_DELETE(def_schema_name);
		return 0;
	}

	/* TODO cleanup "dt" schema */
	tmp = find_sql_schema(s->tr, "tmp");

	if (tmp->tables.set) {
		for (node *n = tmp->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;

			if (isGlobal(t) && isKindOfTable(t))
				sql_trans_clear_table(s->tr, t);
		}
	}
	assert(s->tr && s->tr->active == 0);

	if (s->schema_name)
		_DELETE(s->schema_name);
	s->schema_name = def_schema_name;
	s->schema = NULL;
	s->auto_commit = s->ac_on_commit = ac;
	s->level = ISO_SERIALIZABLE;
	return 1;
}

int
sql_trans_begin(sql_session *s)
{
	const int sleeptime = GDKdebug & FORCEMITOMASK ? 10 : 50;

	sql_trans *tr;
	int snr;

	/* add wait when flush is realy needed */
	while ((store_debug&16)==16 && ATOMIC_GET(&flusher.flush_now)) {
		MT_lock_unset(&bs_lock);
		MT_sleep_ms(sleeptime);
		MT_lock_set(&bs_lock);
	}

	tr = s->tr;
	snr = tr->schema_number;
	TRC_DEBUG(SQL_STORE, "Enter sql_trans_begin for transaction: %d\n", snr);
	if (tr->parent && tr->parent == gtrans &&
	    (tr->stime < gtrans->wstime || tr->wtime ||
			store_schema_number() != snr)) {
		if (!list_empty(tr->moved_tables)) {
			sql_trans_destroy(tr, false);
			s->tr = tr = sql_trans_create(NULL, NULL, false);
		} else {
			reset_trans(tr, gtrans);
		}
	}
	if (tr->parent == gtrans)
		tr = trans_init(tr, tr->parent);
	tr->active = 1;
	s->schema = find_sql_schema(tr, s->schema_name);
	s->tr = tr;
	if (tr->parent == gtrans) {
		(void) ATOMIC_INC(&store_nr_active);
		list_move_data(passive_sessions, active_sessions, s);
	}
	s->status = 0;
	TRC_DEBUG(SQL_STORE, "Exit sql_trans_begin for transaction: %d\n", tr->schema_number);
	return snr != tr->schema_number;
}

void
sql_trans_end(sql_session *s, int commit)
{
	TRC_DEBUG(SQL_STORE, "End of transaction: %d\n", s->tr->schema_number);
	s->tr->active = 0;
	s->auto_commit = s->ac_on_commit;
	sql_trans_reset_tmp(s->tr, commit); /* reset temp schema */
	if (s->tr->parent == gtrans) {
		list_move_data(active_sessions, passive_sessions, s);
		(void) ATOMIC_DEC(&store_nr_active);
	}
	assert(list_length(active_sessions) == (int) ATOMIC_GET(&store_nr_active));
}

void
sql_trans_drop_any_comment(sql_trans *tr, sqlid id)
{
	sql_schema *sys;
	sql_column *id_col;
	sql_table *comments;
	oid row;

	sys = find_sql_schema(tr, "sys");
	assert(sys);

	comments = find_sql_table(sys, "comments");
	if (!comments) /* for example during upgrades */
		return;

	id_col = find_sql_column(comments, "id");
	assert(id_col);

	row = table_funcs.column_find_row(tr, id_col, &id, NULL);
	if (!is_oid_nil(row)) {
		table_funcs.table_delete(tr, comments, row);
	}
}

void
sql_trans_drop_obj_priv(sql_trans *tr, sqlid obj_id)
{
	sql_schema *sys = find_sql_schema(tr, "sys");
	sql_table *privs = find_sql_table(sys, "privileges");

	assert(sys && privs);
	/* select privileges of this obj_id */
	rids *A = table_funcs.rids_select(tr, find_sql_column(privs, "obj_id"), &obj_id, &obj_id, NULL);
	/* remove them */
	for(oid rid = table_funcs.rids_next(A); !is_oid_nil(rid); rid = table_funcs.rids_next(A))
		table_funcs.table_delete(tr, privs, rid);
	table_funcs.rids_destroy(A);
}
