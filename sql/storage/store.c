/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
#include "sql_types.h"
#include "sql_storage.h"
#include "store_dependency.h"
#include "store_connections.h"
#include "store_sequence.h"

#include <bat/bat_utils.h>
#include <bat/bat_storage.h>
#include <bat/bat_table.h>
#include <bat/bat_logger.h>

#include <restrict/restrict_storage.h>
#include <restrict/restrict_table.h>
#include <restrict/restrict_logger.h>

/* version 05.11.00 of catalog */
#define CATALOG_VERSION 51100
int catalog_version = 0;

static MT_Lock bs_lock;
static int store_oid = 0;
static int prev_oid = 0;
static int nr_sessions = 0;
static int transactions = 0;
sql_trans *gtrans = NULL;
int store_nr_active = 0;
store_type active_store_type = store_bat;

store_functions store_funcs;
table_functions table_funcs;
logger_functions logger_funcs;

static int schema_number = 0; /* each committed schema change triggers a new
				 schema number (session wise unique number) */
static int bs_debug = 0;

#define MAX_SPARES 32
static sql_trans *spare_trans[MAX_SPARES];
static int spares = 0;

int
key_cmp(sql_key *k, sqlid *id)
{
	if (k && id &&k->base.id == *id)
		return 0;
	return 1;
}

static int stamp = 1;

static int timestamp () {
	return stamp++;
}

void
kc_destroy(sql_kc *kc)
{
	_DELETE(kc);
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
	base_destroy(&k->base);
	if ((k->type == pkey) && (k->t->pkey == (sql_ukey *) k))
		k->t->pkey = NULL;
	_DELETE(k);
}

void
idx_destroy(sql_idx * i)
{
	/* remove idx from schema */
	list_remove_data(i->t->s->idxs, i);
	list_destroy(i->columns);
	if (isTable(i->t))
		store_funcs.destroy_idx(NULL, i);
	base_destroy(&i->base);
	_DELETE(i);
}

void
trigger_destroy(sql_trigger *tr)
{
	/* remove trigger from schema */
	list_remove_data(tr->t->s->triggers, tr);
	if (tr->old_name)
		_DELETE(tr->old_name);
	if (tr->new_name)
		_DELETE(tr->new_name);
	if (tr->condition)
		_DELETE(tr->condition);
	_DELETE(tr->statement);
	if (tr->columns)
		list_destroy(tr->columns);
	base_destroy(&tr->base);
	_DELETE(tr);
}

void
column_destroy(sql_column *c)
{
	if (isTable(c->t))
		store_funcs.destroy_col(NULL, c);
	if (c->def)
		_DELETE(c->def);
	base_destroy(&c->base);
	_DELETE(c);
}

void
table_destroy(sql_table *t)
{
	cs_destroy(&t->keys);
	cs_destroy(&t->idxs);
	cs_destroy(&t->triggers);
	cs_destroy(&t->columns);
	cs_destroy(&t->tables);
	if (isTable(t))
		store_funcs.destroy_del(NULL, t);
	base_destroy(&t->base);
	if (t->query)
		_DELETE(t->query);
	_DELETE(t);
}

void
seq_destroy(sql_sequence *s)
{
	base_destroy(&s->base);
	_DELETE(s);
}

void
schema_destroy(sql_schema *s)
{
	cs_destroy(&s->tables);
	cs_destroy(&s->funcs);
	cs_destroy(&s->types);
	list_destroy(s->keys);
	list_destroy(s->idxs);
	list_destroy(s->triggers);
	base_destroy(&s->base);
	_DELETE(s);
}

/*#define STORE_DEBUG 1*/ 

sql_trans *
sql_trans_destroy(sql_trans *t)
{
	sql_trans *res = t->parent;

	transactions--;
#ifdef STORE_DEBUG
	fprintf(stderr, "#destroy trans (%p)\n", t);
#endif

	if (res == gtrans && spares < MAX_SPARES && !t->name) {
#ifdef STORE_DEBUG
		fprintf(stderr, "#spared (%d) trans (%p)\n", spares, t);
#endif
		spare_trans[spares++] = t;
		return res;
	}

	if (t->name) {
		_DELETE(t->name);
		t->name = NULL;
	}

	cs_destroy(&t->schemas);

	_DELETE(t);
	return res;
}

static void
destroy_spare_transactions() 
{
	int i, s = spares;

	spares = MAX_SPARES; /* ie now there not spared anymore */
	for (i = 0; i < s; i++) 
		sql_trans_destroy(spare_trans[i]);
}

static int
tr_flag(sql_base * b, int flag)
{
	if (flag == TR_OLD)
		return flag;
	return b->flag;
}

static void
load_keycolumn(sql_trans *tr, sql_key *k, oid rid)
{
	void *v;
	sql_kc *kc = ZNEW(sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");

	v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	kc->c = find_sql_column(k->t, v); 	_DELETE(v);
	list_append(k->columns, kc);
	assert(kc->c);
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
	nk = (ktype != fkey)?(sql_key*)ZNEW(sql_ukey):(sql_key*)ZNEW(sql_fkey);
	v = table_funcs.column_find_value(tr, find_sql_column(keys, "id"), rid);
 	kid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(keys, "name"), rid);
	base_init(NULL, &nk->base, kid, TR_OLD, v);	_DELETE(v);
	nk->type = ktype;
	nk->columns = list_create((fdestroy) &kc_destroy);
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
	for(r = table_funcs.rids_next(rs); r != oid_nil; r = table_funcs.rids_next(rs)) 
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

		v = table_funcs.column_find_value(tr, find_sql_column(keys, "rkey"), rid);
 		rkey = *(sqlid *)v; 		_DELETE(v);
		if ((n = list_find(t->s->keys, &rkey, (fcmp) &key_cmp))){
			sql_ukey *uk = n->data;

			fk->rkey = uk;
			if (!uk->keys)
				uk->keys = list_create(NULL);
			list_append(uk->keys, fk);
		}
	} else {		/* could be a set of rkeys */
		sql_ukey *uk = (sql_ukey *) nk;
		sql_column *key_rkey = find_sql_column(keys, "rkey");

		rs = table_funcs.rids_select(tr, key_rkey, &nk->base.id, &nk->base.id, NULL);

		for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
			sqlid fkey;

			v = table_funcs.column_find_value(tr, find_sql_column(keys, "id"), rid);
			fkey = *(sqlid *)v; 	_DELETE(v);

			if ((n = list_find(t->s->keys, &fkey, (fcmp)&key_cmp))){
				sql_fkey *fk = n->data;

				if (!uk->keys)
					uk->keys = list_create(NULL);
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
	sql_kc *kc = ZNEW(sql_kc);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");

	v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	kc->c = find_sql_column(i->t, v); 	_DELETE(v);
	assert(kc->c);
	list_append(i->columns, kc);
	if (hash_index(i->type)) 
		kc->c->unique = 1;
}

static sql_idx *
load_idx(sql_trans *tr, sql_table *t, oid rid)
{
	void *v;
	sql_idx *ni = ZNEW(sql_idx);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *idxs = find_sql_table(syss, "idxs");
	sql_table *objects = find_sql_table(syss, "objects");
	sql_column *kc_id, *kc_nr;
	rids *rs;
	sqlid iid;

	v = table_funcs.column_find_value(tr, find_sql_column(idxs, "id"), rid);
	iid = *(sqlid *)v;			_DELETE(v);	
	v = table_funcs.column_find_value(tr, find_sql_column(idxs, "name"), rid);
	base_init(NULL, &ni->base, iid, TR_OLD, v);	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(idxs, "type"), rid);
	ni->type = (idx_type) *(int*)v;		_DELETE(v);
	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->key = NULL;

	if (isTable(ni->t) && idx_has_column(ni->type))
		store_funcs.create_idx(tr, ni);

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = table_funcs.rids_select(tr, kc_id, &ni->base.id, &ni->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, kc_nr); 
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
		load_idxcolumn(tr, ni, rid);
	table_funcs.rids_destroy(rs);
	return ni;
}

static void
load_triggercolumn(sql_trans *tr, sql_trigger * i, oid rid)
{
	void *v;
	sql_kc *kc = ZNEW(sql_kc);
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
	sql_trigger *nt = ZNEW(sql_trigger);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *triggers = find_sql_table(syss, "triggers");
	sql_table *objects = find_sql_table(syss, "objects");
	sql_column *kc_id, *kc_nr;
	sqlid tid;
	rids *rs;

	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "id"), rid);
	tid = *(sqlid *)v;			_DELETE(v);	
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "name"), rid);
	base_init(NULL, &nt->base, tid, TR_OLD, v);	_DELETE(v);

	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "time"), rid);
	nt->time = *(sht*)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "orientation"),rid);
	nt->orientation = *(sht*)v;		_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(triggers, "event"), rid);
	nt->event = *(sht*)v;			_DELETE(v);

	nt->old_name = table_funcs.column_find_value(tr, find_sql_column(triggers, "old_name"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), nt->old_name) != 0) {
		_DELETE(nt->old_name);	
		nt->old_name = NULL;
	}
	nt->new_name = table_funcs.column_find_value(tr, find_sql_column(triggers, "new_name"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), nt->new_name) != 0) {
		_DELETE(nt->new_name);	
		nt->new_name = NULL;
	}
	nt->condition = table_funcs.column_find_value(tr, find_sql_column(triggers, "condition"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), nt->condition) != 0) {
		_DELETE(nt->condition);	
		nt->condition = NULL;
	}
	nt->statement = table_funcs.column_find_value(tr, find_sql_column(triggers, "statement"), rid);

	nt->t = t;
	nt->columns = list_create((fdestroy) &kc_destroy);

	kc_id = find_sql_column(objects, "id");
	kc_nr = find_sql_column(objects, "nr");
	rs = table_funcs.rids_select(tr, kc_id, &nt->base.id, &nt->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, kc_nr); 
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
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
	sql_column *c = ZNEW(sql_column);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *columns = find_sql_table(syss, "_columns");
	sqlid cid;

	v = table_funcs.column_find_value(tr, find_sql_column(columns, "id"), rid);
	cid = *(sqlid *)v;			_DELETE(v);	
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "name"), rid);
	base_init(NULL, &c->base, cid, TR_OLD, v);	_DELETE(v);

	tpe = table_funcs.column_find_value(tr, find_sql_column(columns, "type"), rid);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "type_digits"), rid);
	sz = *(int *)v;				_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "type_scale"), rid);
	d = *(int *)v;				_DELETE(v);
	if (!sql_find_subtype(&c->type, tpe, sz, d))
		sql_init_subtype(&c->type, sql_trans_bind_type(tr, t->s, tpe), sz, d);
	_DELETE(tpe);
	c->def = NULL;
	def = table_funcs.column_find_value(tr, find_sql_column(columns, "default"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), def) != 0)
		c->def = def;
	else
		_DELETE(def);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "null"), rid);
	c->null = *(bit *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(columns, "number"), rid);
	c->colnr = *(int *)v;			_DELETE(v);
	c->unique = 0;
	c->storage_type = NULL;
	st = table_funcs.column_find_value(tr, find_sql_column(columns, "storage"), rid);
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), st) != 0)
		c->storage_type = st;
	else
		_DELETE(st);
	c->t = t;
	if (isTable(c->t))
		store_funcs.create_col(tr, c);
	if (bs_debug)
		fprintf(stderr, "#\t\tload column %s\n", c->base.name);
	return c;
}

static void
load_merge_table_parts(sql_trans *tr, sql_table *t, oid rid)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");
	void *v = table_funcs.column_find_value(tr, find_sql_column(objects, "name"), rid);
	sql_table *tp = find_sql_table(t->s, v); 	_DELETE(v);

	assert(tp);
	cs_add(&t->tables, tp, TR_OLD);
}

static void
load_merge_tables(sql_trans *tr, sql_schema *s)
{
	node *n;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *objects = find_sql_table(syss, "objects");
	sql_column *mt_id = find_sql_column(objects, "id");
	sql_column *mt_nr = find_sql_column(objects, "nr");

	if (s->tables.set)
	for (n=s->tables.set->h; n; n = n->next) {
		sql_table *t = n->data;
		oid r = oid_nil;

		if (isMergeTable(t)) {
			rids *rs = table_funcs.rids_select(tr, mt_id, &t->base.id, &t->base.id, NULL);

			rs = table_funcs.rids_orderby(tr, rs, mt_nr); 
			for(r = table_funcs.rids_next(rs); r != oid_nil; r = table_funcs.rids_next(rs)) 
				load_merge_table_parts(tr, t, r);
			table_funcs.rids_destroy(rs);
		}
	}
}

static sql_table *
load_table(sql_trans *tr, sql_schema *s, oid rid)
{
	void *v;
	sql_table *t = ZNEW(sql_table);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *tables = find_sql_table(syss, "_tables");
	sql_table *columns = find_sql_table(syss, "_columns");
	sql_table *idxs = find_sql_table(syss, "idxs");
	sql_table *keys = find_sql_table(syss, "keys");
	sql_table *triggers = find_sql_table(syss, "triggers");
	char *query;
	sql_column *column_table_id, *column_number, *idx_table_id;
	sql_column *key_table_id, *trigger_table_id;
	sqlid tid;
	rids *rs;

	v = table_funcs.column_find_value(tr, find_sql_column(tables, "id"), rid);
	tid = *(sqlid *)v;			_DELETE(v);	
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "name"), rid);
	base_init(NULL, &t->base, tid, TR_OLD, v);	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "query"), rid);
	t->query = NULL;
	query = (char *)v;
	if (ATOMcmp(TYPE_str, ATOMnilptr(TYPE_str), query) != 0)
		t->query = query;
	else
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
	t->cleared = 0;
	v = table_funcs.column_find_value(tr, find_sql_column(tables, "readonly"),rid);
	t->readonly = *(bit *)v;	_DELETE(v);

	t->pkey = NULL;
	t->s = s;
	t->sz = COLSIZE;

	cs_init(&t->columns, (fdestroy) &column_destroy);
	cs_init(&t->idxs, (fdestroy) &idx_destroy);
	cs_init(&t->keys, (fdestroy) &key_destroy);
	cs_init(&t->triggers, (fdestroy) &trigger_destroy);
	cs_init(&t->tables, (fdestroy) &table_destroy);

	if (isTable(t))
		store_funcs.create_del(tr, t);

	if (bs_debug)
		fprintf(stderr, "#\tload table %s\n", t->base.name);

	column_table_id = find_sql_column(columns, "table_id");
	column_number = find_sql_column(columns, "number");
	rs = table_funcs.rids_select(tr, column_table_id, &t->base.id, &t->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, column_number); 
	
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
		cs_add(&t->columns, load_column(tr, t, rid), TR_OLD);
	table_funcs.rids_destroy(rs);

	if (!isTable(t)) 
		return t;

	/* load idx's first as the may be needed by the keys */
	idx_table_id = find_sql_column(idxs, "table_id");
	rs = table_funcs.rids_select(tr, idx_table_id, &t->base.id, &t->base.id, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
		sql_idx *i = load_idx(tr, t, rid);

		cs_add(&t->idxs, i, TR_OLD);
		list_append(s->idxs, i);
	}
	table_funcs.rids_destroy(rs);

	key_table_id = find_sql_column(keys, "table_id");
	rs = table_funcs.rids_select(tr, key_table_id, &t->base.id, &t->base.id, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
		sql_key *k = load_key(tr, t, rid);

		cs_add(&t->keys, k, TR_OLD);
		list_append(s->keys, k);
	}
	table_funcs.rids_destroy(rs);

	trigger_table_id = find_sql_column(triggers, "table_id");
	rs = table_funcs.rids_select(tr, trigger_table_id, &t->base.id, &t->base.id,NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
		sql_trigger *k = load_trigger(tr, t, rid);

		cs_add(&t->triggers, k, TR_OLD);
		list_append(s->triggers, k);
	}
	table_funcs.rids_destroy(rs);
	return t;
}

static sql_type *
load_type(sql_trans *tr, sql_schema *s, oid rid)
{
	void *v;
	sql_type *t = ZNEW(sql_type);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *types = find_sql_table(syss, "types");
	sqlid tid;

	v = table_funcs.column_find_value(tr, find_sql_column(types, "id"), rid);
	tid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "systemname"), rid);
	base_init(NULL, &t->base, tid, TR_OLD, v);	_DELETE(v);
	t->sqlname = table_funcs.column_find_value(tr, find_sql_column(types, "sqlname"), rid);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "digits"), rid);
	t->digits = *(int *)v; 			_DELETE(v); 
	v = table_funcs.column_find_value(tr, find_sql_column(types, "scale"), rid);
	t->scale = *(int *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "radix"), rid);
	t->radix = *(int *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(types, "eclass"), rid);
	t->eclass = *(int *)v;			_DELETE(v);
	t->localtype = ATOMindex(t->base.name);
	t->bits = 0;
	t->s = s;
	return t;
}

static sql_table *
schema_get_table(sql_schema *s, sqlid id)
{
	if (s) {
		node *n;

		for (n=s->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;
			if (t->base.id == id)
				return t;
		}
	}
	return NULL;
}

static sql_arg *
load_arg(sql_trans *tr, sql_func * f, oid rid)
{
	void *v;
	sql_arg *a = ZNEW(sql_arg);
	char *tpe;
	int digits, scale;
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *args = find_sql_table(syss, "args");

	(void)f;
	a->name = table_funcs.column_find_value(tr, find_sql_column(args, "name"), rid);
	tpe = table_funcs.column_find_value(tr, find_sql_column(args, "type"), rid);
	v = table_funcs.column_find_value(tr, find_sql_column(args, "type_digits"), rid);
	digits = *(int *)v;	_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(args, "type_scale"), rid);
	scale = *(int *)v;	_DELETE(v);
	if (!sql_find_subtype(&a->type, tpe, digits, scale))
		sql_init_subtype(&a->type, sql_trans_bind_type(tr, f->s, tpe), digits, scale);
	_DELETE(tpe);

	/* complex (table) types */
	if (a->type.type->localtype == TYPE_bat) 
		a->type.comp_type = schema_get_table(f->s, digits);
	return a;
}

static sql_func *
load_func(sql_trans *tr, sql_schema *s, oid rid)
{
	void *v;
	sql_func *t = ZNEW(sql_func);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *funcs = find_sql_table(syss, "functions");
	sql_table *args = find_sql_table(syss, "args");
	sql_column *arg_func_id, *arg_number;
	int first = 1;
	sqlid fid;
	rids *rs;

	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "id"), rid);
	fid = *(sqlid *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "name"), rid);
	base_init(NULL, &t->base, fid, TR_OLD, v); 	_DELETE(v);
	t->imp = table_funcs.column_find_value(tr, find_sql_column(funcs, "func"), rid);
	t->mod = table_funcs.column_find_value(tr, find_sql_column(funcs, "mod"), rid);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "sql"), rid);
	t->sql = *(bit *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "aggr"), rid);
	t->aggr = *(bit *)v;			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(funcs, "side_effect"), rid);
	t->side_effect = *(bit *)v;		_DELETE(v);
	t->res.scale = t->res.digits = 0;
	t->res.type = NULL;
	t->is_func = 0;
	t->s = s;
	if (t->sql) {
		t->query = t->imp;
		t->imp = NULL;
	}

	arg_func_id = find_sql_column(args, "func_id");
	arg_number = find_sql_column(args, "number");
	rs = table_funcs.rids_select(tr, arg_func_id, &t->base.id, &t->base.id, NULL);
	rs = table_funcs.rids_orderby(tr, rs, arg_number); 

	if (bs_debug)
		fprintf(stderr, "#\tload func %s\n", t->base.name);

	t->ops = list_create((fdestroy)&arg_destroy);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) {
		sql_arg *a = load_arg(tr, t, rid);
		if (first) {
			first = 0;
			if (strcmp(a->name, "result") == 0) {
				t -> res = a->type;
				t->is_func = 1;
				arg_destroy(a);
			} else {
				list_append(t->ops, a);
			}
		} else {
			list_append(t->ops, a);
		}
	}
	table_funcs.rids_destroy(rs);
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
	sql_sequence *seq = ZNEW(sql_sequence);
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *seqs = find_sql_table(syss, "sequences");
	sqlid sid;

	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "id"), rid);
	sid = *(sqlid *)v; 			_DELETE(v);
	v = table_funcs.column_find_value(tr, find_sql_column(seqs, "name"), rid);
	base_init(NULL, &seq->base, sid, TR_OLD, v); _DELETE(v);
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
	seq->cycle = *(bit *)v;			_DELETE(v)
	seq->s = s;
	return seq;
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
	if (sid < id) {
		node *n = find_sql_schema_node(tr, NULL, sid);

		if (n==NULL) {
			char *name;

			v = table_funcs.column_find_value(tr, find_sql_column(ss, "name"), rid);
			name = (char*)v;
			n = find_sql_schema_node(tr, name, -1);
			_DELETE(v);
			if (n == NULL) 
				GDKfatal("SQL schema missing or incompatible, rebuild from archive");
		}
		s = n->data;
		s->base.id = sid;
	} else {
		s = ZNEW(sql_schema);
		v = table_funcs.column_find_value(tr, find_sql_column(ss, "name"), rid);
		base_init(NULL, &s->base, sid, TR_OLD, v); _DELETE(v);
		v = table_funcs.column_find_value(tr, 
			find_sql_column(ss, "authorization"), rid);
		s->auth_id = *(sqlid *)v; 	_DELETE(v);
		v = table_funcs.column_find_value(tr, find_sql_column(ss, "owner"), rid);
		s->owner = *(sqlid *)v;		_DELETE(v);
		s->keys = list_create((fdestroy) NULL);
		s->idxs = list_create((fdestroy) NULL);
		s->triggers = list_create((fdestroy) NULL);

		cs_init(&s->tables, (fdestroy) &table_destroy);
		cs_init(&s->types, (fdestroy) &type_destroy);
		cs_init(&s->funcs, (fdestroy) &func_destroy);
		cs_init(&s->seqs, (fdestroy) &seq_destroy);
	}

	if (bs_debug)
		fprintf(stderr, "#load schema %s %d\n", s->base.name, s->base.id);

	/* first load simple types */
	type_schema = find_sql_column(types, "schema_id");
	type_id = find_sql_column(types, "id");
	rs = table_funcs.rids_select(tr, type_schema, &s->base.id, &s->base.id, type_id, &id, NULL, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
	    	cs_add(&s->types, load_type(tr, s, rid), TR_OLD);
	table_funcs.rids_destroy(rs);

	/* second tables (and complex types) */
	table_schema = find_sql_column(tables, "schema_id");
	table_id = find_sql_column(tables, "id");
	rs = table_funcs.rids_select(tr, table_schema, &sid, &sid, table_id, &id, NULL, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
	    	cs_add(&s->tables, load_table(tr, s, rid), TR_OLD);
	table_funcs.rids_destroy(rs);
	load_merge_tables(tr, s);

	/* next functions which could use these types */
	func_schema = find_sql_column(funcs, "schema_id");
	func_id = find_sql_column(funcs, "id");
	rs = table_funcs.rids_select(tr, func_schema, &s->base.id, &s->base.id, func_id, &id, NULL, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
		cs_add(&s->funcs, load_func(tr, s, rid), TR_OLD);
	table_funcs.rids_destroy(rs);

	/* last sequence numbers */
	seq_schema = find_sql_column(seqs, "schema_id");
	seq_id = find_sql_column(seqs, "id");
	rs = table_funcs.rids_select(tr, seq_schema, &s->base.id, &s->base.id, seq_id, &id, NULL, NULL);
	for(rid = table_funcs.rids_next(rs); rid != oid_nil; rid = table_funcs.rids_next(rs)) 
		cs_add(&s->seqs, load_seq(tr, s, rid), TR_OLD);
	table_funcs.rids_destroy(rs);
	return s;
}

static sql_trans *
create_trans(backend_stack stk)
{
	sql_trans *t = ZNEW(sql_trans);

	t->name = NULL;
	t->wtime = t->rtime = 0;
	t->stime = timestamp ();
	t->schema_updates = 0;
	t->status = 0;

	t->parent = NULL;
	t->stk = stk;

	cs_init(&t->schemas, (fdestroy) &schema_destroy);
	return t;
}

void
load_trans(sql_trans* tr, sqlid id)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysschema = find_sql_table(syss, "schemas");
	sql_column *sysschema_ids = find_sql_column(sysschema, "id");
	rids *schemas = table_funcs.rids_select(tr, sysschema_ids, NULL, NULL);
	oid rid;
	
	if (bs_debug)
		fprintf(stderr, "#load trans\n");

	for(rid = table_funcs.rids_next(schemas); rid != oid_nil; rid = table_funcs.rids_next(schemas)) {
		sql_schema *ns = load_schema(tr, id, rid);
		if (ns && ns->base.id > id)
			cs_add(&tr->schemas, ns, TR_OLD);
	}
	table_funcs.rids_destroy(schemas);
}

static sqlid
next_oid(void)
{
	int id = 0;
	MT_lock_set(&bs_lock, "next_oid");
	id = store_oid++;
	MT_lock_unset(&bs_lock, "next_oid");
	return id;
}

int
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
		table_funcs.table_insert(tr, sysschema, &s->base.id, s->base.name, &s->auth_id, &s->owner);
		for (m = s->tables.set->h; m; m = m->next) {
			sql_table *t = m->data;
			sht ca = t->commit_action;

			table_funcs.table_insert(tr, systable, &t->base.id, t->base.name, &s->base.id, ATOMnilptr(TYPE_str), &t->type, &t->system, &ca, &t->readonly);
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
	int zero = 0;
	node *n;

	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;
		int radix = t->radix;
		int eclass = t->eclass;

		if (t->s)
			table_funcs.table_insert(tr, systype, &t->base.id, t->base.name, t->sqlname, &t->digits, &t->scale, &radix, &eclass, &t->s->base.id);
		else
			table_funcs.table_insert(tr, systype, &t->base.id, t->base.name, t->sqlname, &t->digits, &t->scale, &radix, &eclass, &zero);
	}
}

static void
insert_functions(sql_trans *tr, sql_table *sysfunc, sql_table *sysarg)
{
	int zero = 0;
	bit F = FALSE;
	node *n = NULL, *m = NULL;

	for (n = funcs->h; n; n = n->next) {
		sql_func *f = n->data;
		bit sql = f->sql;
		bit se = f->side_effect;
		sqlid id;
		int number = 0;
		char arg_nme[] = "arg_0";

		if (f->s)
			table_funcs.table_insert(tr, sysfunc, &f->base.id, f->base.name, f->imp, f->mod, &sql, &F, &se, &f->s->base.id);
		else
			table_funcs.table_insert(tr, sysfunc, &f->base.id, f->base.name, f->imp, f->mod, &sql, &F, &se, &zero);
		
		if (f->res.type) {
			char *name = "result";

			id = next_oid();
			table_funcs.table_insert(tr, sysarg, &id, &f->base.id, name, f->res.type->sqlname, &f->res.digits, &f->res.scale, &number);
			number++;
		}
		for (m = f->ops->h; m; m = m->next, number++) {
			sql_arg *a = m->data;

			id = next_oid();
			if (a->name) {
				table_funcs.table_insert(tr, sysarg, &id, &f->base.id, a->name, a->type.type->sqlname, &a->type.digits, &a->type.scale, &number);
			} else {
				arg_nme[4] = '0' + number;
				table_funcs.table_insert(tr, sysarg, &id, &f->base.id, arg_nme, a->type.type->sqlname, &a->type.digits, &a->type.scale, &number);
			}
		}
	}
}

static void
insert_aggrs(sql_trans *tr, sql_table *sysfunc, sql_table *sysarg)
{
	int zero = 0;
	bit T = TRUE, F = FALSE;
	node *n = NULL;

	for (n = aggrs->h; n; n = n->next) {
		char *name1 = "result";
		char *name2 = "arg";
		sql_func *aggr = n->data;
		sqlid id;
		int number = 0;

		if (aggr->s)
			table_funcs.table_insert(tr, sysfunc, &aggr->base.id, aggr->base.name, aggr->imp, aggr->mod, &F, &T, &F, &aggr->s->base.id);
		else
			table_funcs.table_insert(tr, sysfunc, &aggr->base.id, aggr->base.name, aggr->imp, aggr->mod, &F, &T, &F, &zero);
		
		id = next_oid();
		table_funcs.table_insert(tr, sysarg, &id, &aggr->base.id, name1, aggr->res.type->sqlname, &aggr->res.digits, &aggr->res.scale, &number);

		if (aggr->ops->h) {
			sql_arg *arg = aggr->ops->h->data;

			number++;
			id = next_oid();
			table_funcs.table_insert(tr, sysarg, &id, &aggr->base.id, name2, arg->type.type->sqlname, &arg->type.digits, &arg->type.scale, &number);
		}
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
bootstrap_create_column(sql_trans *tr, sql_table *t, char *name, char *sqltype, int digits)
{
	sql_column *col = ZNEW(sql_column);

	if (bs_debug)
		fprintf(stderr, "#bootstrap_create_column %s\n", name );

	base_init(NULL, &col->base, next_oid(), t->base.flag, name);
	sql_find_subtype(&col->type, sqltype, digits, 0);
	col->def = NULL;
	col->null = 1;
	col->colnr = table_next_column_nr(t);
	col->t = t;
	col->unique = 0;
	col->storage_type = NULL;
	cs_add(&t->columns, col, TR_NEW);

	if (isTable(col->t))
		store_funcs.create_col(tr, col);
	tr->schema_updates ++;
	return col;
}

sql_table *
create_sql_table(sql_allocator *sa, char *name, sht type, bit system, int persistence, int commit_action)
{
	sql_table *t = (sa)?SA_ZNEW(sa, sql_table):ZNEW(sql_table);

	assert((persistence==SQL_PERSIST ||
		persistence==SQL_DECLARED_TABLE || 
		commit_action) && commit_action>=0);
	base_init(sa, &t->base, next_oid(), TR_NEW, name);
	t->type = type;
	t->system = system;
	t->persistence = (temp_t)persistence;
	t->commit_action = (ca_t)commit_action;
	t->query = NULL;
	t->readonly = 0;
	(sa)?cs_new(&t->columns, sa):cs_init(&t->columns, (fdestroy) &column_destroy);
	(sa)?cs_new(&t->idxs, sa):cs_init(&t->idxs, (fdestroy) &idx_destroy);
	(sa)?cs_new(&t->keys, sa):cs_init(&t->keys, (fdestroy) &key_destroy);
	(sa)?cs_new(&t->triggers, sa):cs_init(&t->triggers, (fdestroy) &trigger_destroy);
	t->pkey = NULL;
	t->sz = COLSIZE;
	t->cleared = 0;
	t->s = NULL;
	return t;
}

sql_column *
dup_sql_column(sql_allocator *sa, sql_table *t, sql_column *c)
{
	sql_column *col = SA_ZNEW(sa, sql_column);

	base_init(sa, &col->base, c->base.id, TR_NEW, c->base.name);
	col->type = c->type;
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
	col->sorted = sql_trans_is_sorted(NULL, c);
	cs_add(&t->columns, col, TR_NEW);
	return col;
}

sql_table *
dup_sql_table(sql_allocator *sa, sql_table *t)
{
	node *n;
	sql_table *nt = create_sql_table(sa, t->base.name, t->type, t->system, SQL_DECLARED_TABLE, t->commit_action);

	for (n = t->columns.set->h; n; n = n->next) 
		dup_sql_column(sa, nt, n->data);
	nt->columns.dset = NULL;
	nt->columns.nelm = NULL;
	
/*
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			mvc_copy_idx(sql, nt, i);
		}
	}
	if (t->keys.set) {
		for (n = t->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			mvc_copy_key(sql, nt, k);
		}
	}
*/
	/* TODO copy triggers */

	return nt;
}

static sql_table *
bootstrap_create_table(sql_trans *tr, sql_schema *s, char *name)
{
	int istmp = isTempSchema(s);
	int persistence = istmp?SQL_GLOBAL_TEMP:SQL_PERSIST;
	sht commit_action = istmp?CA_PRESERVE:CA_COMMIT;
	sql_table *t = create_sql_table(NULL, name, tt_table, 1, persistence, commit_action);

	if (bs_debug)
		fprintf(stderr, "#bootstrap_create_table %s\n", name );

	t->base.flag = s->base.flag;
	t->query = NULL;
	t->s = s;
	cs_add(&s->tables, t, TR_NEW);

	if (isTable(t))
		store_funcs.create_del(tr, t);
	tr->schema_updates ++;
	return t;
}


static sql_schema *
bootstrap_create_schema(sql_trans *tr, char *name, int auth_id, int owner)
{
	sql_schema *s = ZNEW(sql_schema);

	if (bs_debug)
		fprintf(stderr, "#bootstrap_create_schema %s %d %d\n", name, auth_id, owner);

	base_init(NULL, &s->base, next_oid(), TR_NEW, name);
	s->auth_id = auth_id;
	s->owner = owner;
	cs_init(&s->tables, (fdestroy) &table_destroy);
	cs_init(&s->types, (fdestroy) &type_destroy);
	cs_init(&s->funcs, (fdestroy) &func_destroy);
	cs_init(&s->seqs, (fdestroy) &seq_destroy);
	s->keys = list_create((fdestroy) NULL);
	s->idxs = list_create((fdestroy) NULL);
	s->triggers = list_create((fdestroy) NULL);

	cs_add(&tr->schemas, s, TR_NEW);

	tr->schema_updates ++;
	return s;
}

static int
store_schema_number()
{
	return schema_number;
}

int
store_init(int debug, store_type store, char *logdir, char *dbname, backend_stack stk)
{
	sqlid id = 0;
	lng lng_store_oid;
	int first = 1;
	sql_schema *s, *p = NULL;
	sql_table *t, *types, *funcs, *args;
	sql_trans *tr;
	int v = 1;

	bs_debug = debug;

	/* initialize empty bats */
	if (store == store_bat ||
	    store == store_su ||
	    store == store_ro ||
	    store == store_suro) 
		bat_utils_init();
	if (store == store_bat) {
		bat_storage_init(&store_funcs);
		bat_table_init(&table_funcs);
		bat_logger_init(&logger_funcs);
	} else if (store == store_su) {
		su_storage_init(&store_funcs);
		su_table_init(&table_funcs);
		su_logger_init(&logger_funcs);
	} else if (store == store_ro) {
		ro_storage_init(&store_funcs);
		ro_table_init(&table_funcs);
		ro_logger_init(&logger_funcs);
	} else if (store == store_suro) {
		suro_storage_init(&store_funcs);
		suro_table_init(&table_funcs);
		suro_logger_init(&logger_funcs);
	}
	active_store_type = store;
	if (!logger_funcs.create ||
	    logger_funcs.create(logdir, dbname, CATALOG_VERSION*v) == LOG_ERR)
		return -1;

	MT_lock_init(&bs_lock, "SQL_bs_lock");
	types_init(debug);

#define FUNC_OIDS 2000
	assert( store_oid <= FUNC_OIDS );
	/* we store some spare oids */
	store_oid = FUNC_OIDS;

	sequences_init();
	gtrans = tr = create_trans(stk);

	if (logger_funcs.log_isnew()) {
		/* cannot initialize database in readonly mode */
		if (store == store_ro)
			return -1;
		tr = sql_trans_create(stk, NULL, NULL);
	} else {
		first = 0;
	}

	s = bootstrap_create_schema(tr, "sys", ROLE_SYSADMIN, USER_MONETDB);
	if (!first) 
		s->base.flag = TR_OLD;

	t = bootstrap_create_table(tr, s, "schemas");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 1024);
	bootstrap_create_column(tr, t, "authorization", "int", 32);
	bootstrap_create_column(tr, t, "owner", "int", 32);

	types = t = bootstrap_create_table(tr, s, "types");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "systemname", "varchar", 256);
	bootstrap_create_column(tr, t, "sqlname", "varchar", 1024);
	bootstrap_create_column(tr, t, "digits", "int", 32);
	bootstrap_create_column(tr, t, "scale", "int", 32);
	bootstrap_create_column(tr, t, "radix", "int", 32);
	bootstrap_create_column(tr, t, "eclass", "int", 32);
	bootstrap_create_column(tr, t, "schema_id", "int", 32);

	funcs = t = bootstrap_create_table(tr, s, "functions");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 256);
	bootstrap_create_column(tr, t, "func", "varchar", 8196);
	bootstrap_create_column(tr, t, "mod", "varchar", 8196);
	/* sql or database internal */
	bootstrap_create_column(tr, t, "sql", "boolean", 1);
	/* aggr or func */
	bootstrap_create_column(tr, t, "aggr", "boolean", 1);
	bootstrap_create_column(tr, t, "side_effect", "boolean", 1);
	bootstrap_create_column(tr, t, "schema_id", "int", 32);

	args = t = bootstrap_create_table(tr, s, "args");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "func_id", "int", 32);
	bootstrap_create_column(tr, t, "name", "varchar", 256);
	bootstrap_create_column(tr, t, "type", "varchar", 1024);
	bootstrap_create_column(tr, t, "type_digits", "int", 32);
	bootstrap_create_column(tr, t, "type_scale", "int", 32);
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

	t = bootstrap_create_table(tr, s, "dependencies");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "depend_id", "int", 32);
	bootstrap_create_column(tr, t, "depend_type", "smallint", 16);

	t = bootstrap_create_table(tr, s, "connections");
	bootstrap_create_column(tr, t, "id", "int", 32);
	bootstrap_create_column(tr, t, "server", "char", 1024);
	bootstrap_create_column(tr, t, "port", "int", 32);
	bootstrap_create_column(tr, t, "db", "char", 64);
	bootstrap_create_column(tr, t, "db_alias", "char", 1024);
	bootstrap_create_column(tr, t, "user", "char", 1024);
	bootstrap_create_column(tr, t, "password", "char", 1024);
	bootstrap_create_column(tr, t, "language", "char", 1024);

	while(s) {
		t = bootstrap_create_table(tr, s, "_tables");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "schema_id", "int", 32);
		bootstrap_create_column(tr, t, "query", "varchar", 2048);
		bootstrap_create_column(tr, t, "type", "smallint", 16);
		bootstrap_create_column(tr, t, "system", "boolean", 1);
		bootstrap_create_column(tr, t, "commit_action", "smallint", 16);
		bootstrap_create_column(tr, t, "readonly", "boolean", 1);

		t = bootstrap_create_table(tr, s, "_columns");
		bootstrap_create_column(tr, t, "id", "int", 32);
		bootstrap_create_column(tr, t, "name", "varchar", 1024);
		bootstrap_create_column(tr, t, "type", "varchar", 1024);
		bootstrap_create_column(tr, t, "type_digits", "int", 32);
		bootstrap_create_column(tr, t, "type_scale", "int", 32);
		bootstrap_create_column(tr, t, "table_id", "int", 32);
		bootstrap_create_column(tr, t, "default", "varchar", 2048);
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

	(void) bootstrap_create_schema(tr, dt_schema, ROLE_SYSADMIN, USER_MONETDB);

	if (first) {
		insert_types(tr, types);
		insert_functions(tr, funcs, args);
		insert_aggrs(tr, funcs, args);
		insert_schemas(tr);

		if (sql_trans_commit(tr) != SQL_OK)
			fprintf(stderr, "cannot commit initial transaction\n");
		sql_trans_destroy(tr);
	}

	id = store_oid; /* db objects up till id are already created */
	logger_funcs.get_sequence(OBJ_SID, &lng_store_oid);
	prev_oid = store_oid = (sqlid)lng_store_oid;

	/* load remaining schemas, tables, columns etc */
	if (!first)
		load_trans(gtrans, id);
	return first;
}

static int active = 1;
static int logging = 0;

void
store_exit(void)
{
	MT_lock_set(&bs_lock, "store_exit");

#ifdef STORE_DEBUG
	fprintf(stderr, "#store exit locked\n");
#endif
	active = 0;

	/* busy wait till the logmanager is ready */
	while (logging) {
		MT_lock_unset(&bs_lock, "store_exit");
		MT_sleep_ms(100);
		MT_lock_set(&bs_lock, "store_exit");
	}

	if (gtrans) {
		MT_lock_unset(&bs_lock, "store_exit");
		sequences_exit();
		MT_lock_set(&bs_lock, "store_exit");
	}
	if (spares > 0) 
		destroy_spare_transactions();

	logger_funcs.destroy();

	/* Open transactions have a link to the global transaction therefore
	   we need busy waiting until all transactions have ended or
	   (current implementation) simply keep the gtrans alive and simply
	   exit (but leak memory).
	 */ 
	if (!transactions) { 
		sql_trans_destroy(gtrans);
		gtrans = NULL;
	}
#ifdef STORE_DEBUG
	fprintf(stderr, "#store exit unlocked\n");
#endif
	MT_lock_unset(&bs_lock, "store_exit");
	types_exit();
}

/* call locked ! */
void
store_apply_deltas(void)
{
	int res = LOG_OK;

	logging = 1;
	/* make sure we reset all transactions on re-activation */
	gtrans->stime++;
	if (store_funcs.gtrans_update)
		store_funcs.gtrans_update(gtrans);
	res = logger_funcs.restart();
	if (logging && res == LOG_OK)
		res = logger_funcs.cleanup();
	logging = 0;
}

void
store_manager(void)
{
	while (active) {
		int res = LOG_OK;

		MT_sleep_ms(30000);
		MT_lock_set(&bs_lock, "store_manager");
		if (store_nr_active || !active || 
			logger_funcs.changes() < 1000) {
			MT_lock_unset(&bs_lock, "store_manager");
			continue;
		}
		logging = 1;
		/* make sure we reset all transactions on re-activation */
		gtrans->stime++;
		if (store_funcs.gtrans_update)
			store_funcs.gtrans_update(gtrans);
		res = logger_funcs.restart();
		MT_lock_unset(&bs_lock, "store_manager");
		if (logging && res == LOG_OK)
			res = logger_funcs.cleanup();
		logging = 0;
	}
}

void
minmax_manager(void)
{
	while (active) {
		MT_sleep_ms(30000);
		MT_lock_set(&bs_lock, "store_manager");
		if (store_nr_active || !active) {
			MT_lock_unset(&bs_lock, "store_manager");
			continue;
		}
		if (store_funcs.gtrans_minmax)
			store_funcs.gtrans_minmax(gtrans);
		MT_lock_unset(&bs_lock, "store_manager");
	}
}


void
store_lock(void)
{
	MT_lock_set(&bs_lock, "trans_lock");
#ifdef STORE_DEBUG
	fprintf(stderr, "#locked\n");
#endif
}

void
store_unlock(void)
{
#ifdef STORE_DEBUG
	fprintf(stderr, "#unlocked\n");
#endif
	MT_lock_unset(&bs_lock, "trans_unlock");
}

static sql_kc *
kc_dup(sql_trans *tr, int flag, sql_kc *kc, sql_table *t)
{
	sql_kc *nkc = ZNEW(sql_kc);
	sql_column *c = find_sql_column(t, kc->c->base.name);

	(void) tr;		/* unused! */
	(void) flag;
	assert(c);
	nkc->c = c;
	c->unique = kc->c->unique;
	return nkc;
}

static sql_key *
key_dup(sql_trans *tr, int flag, sql_key *k, sql_table *t)
{
	sql_key *nk = (k->type != fkey) ? (sql_key *) ZNEW(sql_ukey)
	    : (sql_key *) ZNEW(sql_fkey);
	node *n;

	base_init(NULL, &nk->base, k->base.id, tr_flag(&k->base, flag), k->base.name);

	nk->type = k->type;
	nk->columns = list_create((fdestroy) &kc_destroy);
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

		list_append(nk->columns, kc_dup(tr, flag, okc, t));
	}

	if (nk->type == fkey) {
		sql_fkey *fk = (sql_fkey *) nk;
		sql_fkey *ok = (sql_fkey *) k;
		node *n;

		if (ok->rkey) {
			n = list_find(t->s->keys, &ok->rkey->k.base.id, (fcmp) &key_cmp);

			if (n) {
				sql_ukey *uk = n->data;
	
				fk->rkey = uk;
				if (!uk->keys)
					uk->keys = list_create(NULL);
				list_append(uk->keys, fk);
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
				sql_fkey *ofk = m->data;
				node *n = list_find(t->s->keys, &ofk->k.base.id, (fcmp) &key_cmp);

				if (n) {
					sql_fkey *fk = n->data;

					if (!uk->keys)
						uk->keys = list_create(NULL);
					list_append(uk->keys, fk);
					fk->rkey = uk;
				}
			}
	}
	list_append(t->s->keys, nk);
	if (flag == TR_NEW && tr->parent == gtrans) 
		k->base.flag = TR_OLD;
	return nk;
}

sql_key *
sql_trans_copy_key( sql_trans *tr, sql_table *t, sql_key *k )
{
	sql_key *nk = key_dup(tr, TR_NEW, k, t);
	sql_fkey *fk = (sql_fkey*)nk;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(syss, "keys");
	sql_table *syskc = find_sql_table(syss, "objects");
	int neg = -1, action = -1, nr;
	node *n;

	cs_add(&t->keys, nk, TR_NEW);

	if (nk->type == fkey) 
		action = (fk->on_update<<8) + fk->on_delete;

	table_funcs.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey->k.base.id : &neg, &action);

	if (nk->type == fkey)
		sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey->k.base.id, nk->base.id, FKEY_DEPENDENCY);

	for (n = nk->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *kc = n->data;

		table_funcs.table_insert(tr, syskc, &k->base.id, kc->c->base.name, &nr);

		if (nk->type == fkey)
			sql_trans_create_dependency(tr, kc->c->base.id, k->base.id, FKEY_DEPENDENCY);
		if (nk->type == pkey) {
			sql_trans_create_dependency(tr, kc->c->base.id, k->base.id, KEY_DEPENDENCY);
			sql_trans_alter_null(tr, kc->c, 0);
		}
	}

	syskey->base.wtime = syskey->s->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return nk;
}

static sql_idx *
idx_dup(sql_trans *tr, int flag, sql_idx * i, sql_table *t)
{
	sql_idx *ni = ZNEW(sql_idx);
	node *n;

	base_init(NULL, &ni->base, i->base.id, tr_flag(&i->base, flag), i->base.name);

	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;

	if (isTable(ni->t))
		store_funcs.dup_idx(tr, i, ni);
	if (isNew(i) && flag == TR_NEW && tr->parent == gtrans) 
		i->base.flag = TR_OLD;

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(ni->columns, kc_dup(tr, flag, okc, t));
	}
	list_append(t->s->idxs, ni);
	return ni;
}

sql_idx *
sql_trans_copy_idx( sql_trans *tr, sql_table *t, sql_idx *i )
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(syss, "idxs");
	sql_table *sysic = find_sql_table(syss, "objects");
	node *n;
	int nr;
	sql_idx *ni = ZNEW(sql_idx);

	base_init(NULL, &ni->base, i->base.id, TR_NEW, i->base.name);

	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->type = i->type;
	ni->key = NULL;

	for (n = i->columns->h, nr = 0; n; n = n->next, nr++) {
		sql_kc *okc = n->data, *ic;

		list_append(ni->columns, ic = kc_dup(tr, TR_NEW, okc, t));

		table_funcs.table_insert(tr, sysic, &ni->base.id, ic->c->base.name, &nr);
		sysic->base.wtime = sysic->s->base.wtime = tr->wtime = tr->stime;
	}
	list_append(t->s->idxs, ni);
	cs_add(&t->idxs, ni, TR_NEW);

	if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
		store_funcs.create_idx(tr, ni);
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, ni->base.name);
	ni->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return ni;
}

static sql_trigger *
trigger_dup(sql_trans *tr, int flag, sql_trigger * i, sql_table *t)
{
	sql_trigger *nt = ZNEW(sql_trigger);
	node *n;

	base_init(NULL, &nt->base, i->base.id, tr_flag(&i->base, flag), i->base.name);

	nt->columns = list_create((fdestroy) &kc_destroy);
	nt->t = t;
	nt->time = i->time;
	nt->orientation = i->orientation;
	nt->event = i->event;
	nt->old_name = nt->new_name = nt->condition = NULL;
	if (i->old_name)
		nt->old_name = _strdup(i->old_name);
	if (i->new_name)
		nt->new_name = _strdup(i->new_name);
	if (i->condition)
		nt->condition = _strdup(i->condition);
	nt->statement = _strdup(i->statement);

	for (n = i->columns->h; n; n = n->next) {
		sql_kc *okc = n->data;

		list_append(nt->columns, kc_dup(tr, flag, okc, t));
	}
	list_append(t->s->triggers, nt);
	if (flag == TR_NEW && tr->parent == gtrans) 
		i->base.flag = TR_OLD;
	return nt;
}

static sql_column *
column_dup(sql_trans *tr, int flag, sql_column *oc, sql_table *t)
{
	sql_column *c = ZNEW(sql_column);

	base_init(NULL, &c->base, oc->base.id, tr_flag(&oc->base, flag), oc->base.name);
	c->type = oc->type;
	c->def = NULL;
	if (oc->def)
		c->def = _strdup(oc->def);
	c->null = oc->null;
	c->colnr = oc->colnr;
	c->unique = oc->unique;
	c->t = t;
	c->storage_type = NULL;
	if (oc->storage_type)
		c->storage_type = _strdup(oc->storage_type);

	if (isTable(c->t))
		store_funcs.dup_col(tr, oc, c);
	if (isNew(oc) && flag == TR_NEW && tr->parent == gtrans) 
		oc->base.flag = TR_OLD;
	return c;
}

sql_column *
sql_trans_copy_column( sql_trans *tr, sql_table *t, sql_column *c )
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(syss, "_columns");
	sql_column *col = ZNEW(sql_column);

	base_init(NULL, &col->base, c->base.id, TR_NEW, c->base.name);
	col->type = c->type;
	col->def = NULL;
	if (c->def)
		col->def = _strdup(c->def);
	col->null = c->null;
	col->colnr = c->colnr;
	col->unique = c->unique;
	col->t = t;
	col->storage_type = NULL;
	if (c->storage_type)
		col->storage_type = _strdup(c->storage_type);

	cs_add(&t->columns, col, TR_NEW);

	if (isTable(t))
		store_funcs.create_col(tr, col);
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, syscolumn, &col->base.id, col->base.name, col->type.type->sqlname, &col->type.digits, &col->type.scale, &t->base.id, (col->def) ? col->def : ATOMnilptr(TYPE_str), &col->null, &col->colnr, (col->storage_type) ? col->storage_type : ATOMnilptr(TYPE_str));
	col->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return col;
}

static sql_table *
schema_table_find(sql_schema *s, sql_table *ot)
{
	node *n;

	if (s) 
	for (n = s->tables.set->h; n; n = n->next) {
		sql_table *t = n->data;

		if (t->base.id == ot->base.id)
			return t;
	}
	assert(NULL);
	return NULL;
}

static sql_table *
table_find(sql_trans *tr, int flag, sql_table *ot, sql_table *omt)
{
	node *n;
	sql_schema *s = NULL;

	(void)flag;
	for (n = tr->schemas.set->h; n && !s; n = n->next) {
		sql_schema *ss = n->data;

		if (ss->base.id == omt->s->base.id)
			s = ss;
	}
	return schema_table_find(s, ot);
}

static sql_table *
table_dup(sql_trans *tr, int flag, sql_table *ot, sql_schema *s)
{
	node *n;
	sql_table *t = ZNEW(sql_table);

	base_init(NULL, &t->base, ot->base.id, tr_flag(&ot->base, flag), ot->base.name);

	t->type = ot->type;
	t->system = ot->system;
	t->persistence = ot->persistence;
	t->commit_action = ot->commit_action;
	t->readonly = ot->readonly;
	t->query = (ot->query) ? _strdup(ot->query) : NULL;

	cs_init(&t->columns, (fdestroy) &column_destroy);
	cs_init(&t->keys, (fdestroy) &key_destroy);
	cs_init(&t->idxs, (fdestroy) &idx_destroy);
	cs_init(&t->triggers, (fdestroy) &trigger_destroy);

	t->pkey = NULL;

	if (isTable(ot)) 
		store_funcs.dup_del(tr, ot, t);

	t->s = s;
	t->sz = ot->sz;
	t->cleared = 0;

	if (ot->columns.set) {
		for (n = ot->columns.set->h; n; n = n->next) {
			sql_column *c = n->data;

			cs_add(&t->columns, column_dup(tr, flag, c, t), tr_flag(&c->base, flag));
		}
		ot->columns.nelm = NULL;
	}
	/*
	if (ot->tables.set) {
		for (n = ot->tables.set->h; n; n = n->next) {
			sql_table *pt = n->data;

			cs_add(&t->tables, schema_table_find(s, pt), tr_flag(&pt->base, flag));
		}
		ot->tables.nelm = NULL;
	}
	*/
	if (ot->idxs.set) {
		for (n = ot->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;

			cs_add(&t->idxs, idx_dup(tr, flag, i, t), tr_flag(&i->base, flag));
		}
		ot->idxs.nelm = NULL;
	}
	if (ot->keys.set) {
		for (n = ot->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			cs_add(&t->keys, key_dup(tr, flag, k, t), tr_flag(&k->base, flag));
		}
		ot->keys.nelm = NULL;
	}
	if (ot->triggers.set) {
		for (n = ot->triggers.set->h; n; n = n->next) {
			sql_trigger *k = n->data;

			cs_add(&t->triggers, trigger_dup(tr, flag, k, t), tr_flag(&k->base, flag));
		}
		ot->triggers.nelm = NULL;
	}
	if (flag == TR_NEW && tr->parent == gtrans) 
		ot->base.flag = TR_OLD;
	return t;
}

static sql_type *
type_dup(sql_trans *tr, int flag, sql_type *ot, sql_schema * s)
{
	sql_type *t = ZNEW(sql_type);

	(void) tr;
	base_init(NULL, &t->base, ot->base.id, tr_flag(&ot->base, flag), ot->base.name);

	t->sqlname = _strdup(ot->sqlname);
	t->digits = ot->digits;
	t->scale = ot->scale;
	t->radix = ot->radix;
	t->eclass = ot->eclass;
	t->bits = ot->bits;
	t->localtype = ot->localtype;
	t->s = s;
	return t;
}

static sql_func *
func_dup(sql_trans *tr, int flag, sql_func *of, sql_schema * s)
{
	node *n;
	sql_func *f = ZNEW(sql_func);

	(void)tr;
	base_init(NULL, &f->base, of->base.id, tr_flag(&of->base, flag), of->base.name);

	f->imp = (of->imp)?_strdup(of->imp):NULL;
	f->mod = (of->mod)?_strdup(of->mod):NULL;
	f->query = (of->query)?_strdup(of->query):NULL;
	f->sql = of->sql;
	f->aggr = of->aggr;
	f->side_effect = of->side_effect;
	f->ops = list_create(of->ops->destroy);
	for(n=of->ops->h; n; n = n->next) 
		list_append(f->ops, arg_dup(n->data));
	f->res.type = NULL;
	if (of->res.type) {
		f->res = of->res;

		/* complex (table) types */
		if (f->res.type->localtype == TYPE_bat) 
			f->res.comp_type = schema_get_table(s, f->res.digits);
	}

	f->s = s;
	f->is_func = of->is_func;
	return f;
}

static sql_sequence *
seq_dup(sql_trans *tr, int flag, sql_sequence *oseq, sql_schema * s)
{
	sql_sequence *seq = ZNEW(sql_sequence);

	(void)tr;
	base_init(NULL, &seq->base, oseq->base.id, tr_flag(&oseq->base, flag), oseq->base.name);

	seq->start = oseq->start;
	seq->minvalue = oseq->minvalue;
	seq->maxvalue = oseq->maxvalue;
	seq->increment = oseq->increment;
	seq->cacheinc = oseq->cacheinc;
	seq->cycle = oseq->cycle;
	seq->s = s;
	return seq;
}

static void
merge_table_dup(sql_table *omt, sql_schema *s, int flag) 
{
	node *n;
	sql_table *mt = schema_table_find(s, omt);
	
	if (omt->tables.set) {
		for (n = omt->tables.set->h; n; n = n->next) {
			sql_table *pt = n->data;

			cs_add(&mt->tables, schema_table_find(s, pt), tr_flag(&pt->base, flag));
		}
		mt->tables.nelm = NULL;
	}
}

static sql_schema *
schema_dup(sql_trans *tr, int flag, sql_schema *os, sql_trans *o)
{
	node *n;
	sql_schema *s = ZNEW(sql_schema);

	(void) o;
	base_init(NULL, &s->base, os->base.id, tr_flag(&os->base, flag), os->base.name);

	s->auth_id = os->auth_id;
	s->owner = os->owner;
	cs_init(&s->tables, (fdestroy) &table_destroy);
	cs_init(&s->types, (fdestroy) &type_destroy);
	cs_init(&s->funcs, (fdestroy) &func_destroy);
	cs_init(&s->seqs, (fdestroy) &seq_destroy);
	s->keys = list_create((fdestroy) NULL);
	s->idxs = list_create((fdestroy) NULL);
	s->triggers = list_create((fdestroy) NULL);

	if (os->types.set) {
		for (n = os->types.set->h; n; n = n->next) {
			cs_add(&s->types, type_dup(tr, flag, n->data, s), tr_flag(&os->base, flag));
		}
		os->types.nelm = NULL;
	}
	if (os->tables.set) {
		for (n = os->tables.set->h; n; n = n->next) {
			sql_table *ot = n->data;

			if (ot->persistence != SQL_LOCAL_TEMP)
				cs_add(&s->tables, table_dup(tr, flag, ot, s), tr_flag(&ot->base, flag));
		}
		os->tables.nelm = NULL;
		for (n = os->tables.set->h; n; n = n->next) {
			sql_table *ot = n->data;

			if (ot->persistence != SQL_LOCAL_TEMP && isMergeTable(ot))
				merge_table_dup(ot, s, flag);
		}
		os->tables.nelm = NULL;
	}
	if (os->funcs.set) {
		for (n = os->funcs.set->h; n; n = n->next) {
			cs_add(&s->funcs, func_dup(tr, flag, n->data, s), tr_flag(&os->base, flag));
		}
		os->funcs.nelm = NULL;
	}
	if (os->seqs.set) {
		for (n = os->seqs.set->h; n; n = n->next) {
			cs_add(&s->seqs, seq_dup(tr, flag, n->data, s), tr_flag(&os->base, flag));
		}
		os->seqs.nelm = NULL;
	}
	if (flag == TR_NEW && tr->parent == gtrans) 
		os->base.flag = TR_OLD;
	return s;
}

static sql_trans *
trans_init(sql_trans *t, backend_stack stk, sql_trans *ot)
{
	t->wtime = t->rtime = 0;
	t->stime = timestamp ();
	t->schema_updates = 0;
	t->dropped = NULL;
	t->status = 0;
	if (ot != gtrans)
		t->schema_updates = ot->schema_updates;

	t->schema_number = store_schema_number();
	t->parent = ot;
	t->stk = stk;

	t->name = NULL;
	if (bs_debug) 
		fprintf(stderr, "#trans (%p) init (%d,%d)\n", 
			t, t->stime, t->schema_number ); 
	return t;
}

static sql_trans *
trans_dup(backend_stack stk, sql_trans *ot, char *newname)
{
	node *n;
	sql_trans *t = ZNEW(sql_trans);

	t = trans_init(t, stk, ot);

	cs_init(&t->schemas, (fdestroy) &schema_destroy);

	/* name the old transaction */
	if (newname) {
		assert(ot->name == NULL);
		ot->name = _strdup(newname);
	}

	if (ot->schemas.set) {
		for (n = ot->schemas.set->h; n; n = n->next) {
			cs_add(&t->schemas, schema_dup(t, TR_OLD, n->data, t), TR_OLD);
		}
		ot->schemas.nelm = NULL;
	}
	return t;
}

#define R_SNAPSHOT 	1
#define R_LOG 		2
#define R_APPLY 	3

typedef int (*rfufunc) (sql_trans *tr, sql_base * fs, sql_base * ts, int mode);
typedef sql_base *(*rfcfunc) (sql_trans *tr, sql_base * b, int mode);
typedef int (*rfdfunc) (sql_trans *tr, sql_base * b, int mode);
typedef sql_base *(*dupfunc) (sql_trans *tr, int flag, sql_base * b, sql_base * p);

static int
rollforward_changeset_updates(sql_trans *tr, changeset * fs, changeset * ts, sql_base * b, rfufunc rollforward_updates, rfcfunc rollforward_creates, rfdfunc rollforward_deletes, dupfunc fd, int mode)
{
	int ok = LOG_OK;
	int apply = (mode == R_APPLY);
	node *n = NULL;

	/* delete removed bases */
	if (fs->dset) {
		for (n = fs->dset->h; ok == LOG_OK && n; n = n->next) {
			sql_base *fb = n->data;
			node *tbn = cs_find_name(ts, fb->name);

			if (tbn) {
				sql_base *tb = tbn->data;

				if (rollforward_deletes)
					ok = rollforward_deletes(tr, tb, mode);
				if (apply) {
					if (ts->nelm == tbn)
						ts->nelm = tbn->next;
					if (tr->parent != gtrans) {
						if (!ts->dset)
							ts->dset = list_create(ts->destroy);
						list_move_data(ts->set, ts->dset, tb);
					} else {
						list_remove_node(ts->set, tbn);
					}
				}
			}
		}
		if (apply) {
			list_destroy(fs->dset);
			fs->dset = NULL;
		}
	}
	/* changes to the existing bases */
	if (fs->set) {
		/* update existing */
		if (rollforward_updates) {
			for (n = fs->set->h; ok == LOG_OK && n && n != fs->nelm; n = n->next) {
				sql_base *fb = n->data;

				if (fb->wtime && fb->flag == TR_OLD) {
					node *tbn = cs_find_id(ts, fb->id);

					if (tbn) {
						sql_base *tb = tbn->data;

						/* update timestamps */
						if (apply && fb->rtime && tr->stime > tb->rtime)
							tb->rtime = tr->stime;
						if (apply && fb->wtime && tr->stime > tb->wtime)
							tb->wtime = tr->stime;

						ok = rollforward_updates(tr, fb, tb, mode);
					}
				}
				if (apply)
					fb->rtime = fb->wtime = 0;
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
						fb->rtime = fb->wtime = 0;
						fb->flag = TR_OLD;
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

			if (apply) {
				b->rtime = b->wtime = 0;
				b->flag = TR_OLD;
			}
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
		    (mode == R_APPLY &&  store_funcs.create_col(tr, c) != LOG_OK))
		return NULL;
	}
	return c;
}

static sql_table *
rollforward_add_table(sql_trans *tr, sql_table *t, int mode)
{
	(void) tr;
	(void) mode;
	return t;
}

static sql_table *
rollforward_del_table(sql_trans *tr, sql_table *t, int mode)
{
	(void) tr;
	(void) mode;
	return t;
}

static sql_table *
rollforward_create_table(sql_trans *tr, sql_table *t, int mode)
{
	int ok = LOG_OK;

	if (bs_debug) 
		fprintf(stderr, "#create table %s\n", t->base.name);

	if (isTable(t) && isGlobal(t)) {
		int p = (tr->parent == gtrans && !isTempTable(t));

		/* only register columns without commit action tables */
		rollforward_changeset_creates(tr, &t->columns, (rfcfunc) &rollforward_create_column, mode);
		if (p && mode == R_SNAPSHOT)
			store_funcs.snapshot_create_del(tr, t);
		else if (p && mode == R_LOG)
			store_funcs.log_create_del(tr, t);
		else if (mode == R_APPLY)
			store_funcs.create_del(tr, t);
		rollforward_changeset_creates(tr, &t->tables, (rfcfunc) &rollforward_add_table, mode);
		rollforward_changeset_creates(tr, &t->keys, (rfcfunc) &rollforward_create_key, mode);
		rollforward_changeset_creates(tr, &t->idxs, (rfcfunc) &rollforward_create_idx, mode);
		rollforward_changeset_creates(tr, &t->triggers, (rfcfunc) &rollforward_create_trigger, mode);
	}
	if (ok != LOG_OK)
		return NULL;
	return t;
}

static int
rollforward_drop_column(sql_trans *tr, sql_column *c, int mode)
{
	if (isTable(c->t)) {
		int p = (tr->parent == gtrans);

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
		int p = (tr->parent == gtrans);

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
rollforward_drop_trigger(sql_trans *tr, sql_trigger * i, int mode)
{
	(void)tr;
	if (mode == R_APPLY)
		list_remove_data(i->t->s->triggers, i);
	return LOG_OK;
}

static int
rollforward_drop_seq(sql_trans *tr, sql_sequence * seq, int mode)
{
	(void)tr;
	(void)seq;
	(void)mode;
	/* TODO drop sequence? */
	return LOG_OK;
}

static int
rollforward_drop_table(sql_trans *tr, sql_table *t, int mode)
{
	int ok = LOG_OK;

	if (isTable(t)) {
		int p = (tr->parent == gtrans);

		if (p && mode == R_LOG)
			ok = store_funcs.log_destroy_del(tr, t);
		else if (mode == R_APPLY)
			ok = store_funcs.destroy_del(tr, t);
	}
	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &t->columns, (rfdfunc) &rollforward_drop_column, mode);
 	if (ok == LOG_OK)
		ok = rollforward_changeset_deletes(tr, &t->tables, (rfdfunc) NULL, mode);
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

	ok = rollforward_changeset_deletes(tr, &s->seqs, (rfdfunc) &rollforward_drop_seq, mode);
	if (ok == LOG_OK)
		return rollforward_changeset_deletes(tr, &s->tables, (rfdfunc) &rollforward_drop_table, mode);
	return ok;
}

static sql_schema *
rollforward_create_schema(sql_trans *tr, sql_schema *s, int mode)
{
	if (rollforward_changeset_creates(tr, &s->tables, (rfcfunc) &rollforward_create_table, mode) != LOG_OK)
		return NULL;
	return s;
}

static int
rollforward_update_table(sql_trans *tr, sql_table *ft, sql_table *tt, int mode)
{
	int p = (tr->parent == gtrans && !isTempTable(ft));
	int ok = LOG_OK;

	/* cannot update views and temporary tables */
	if (!isTable(ft) || isTempTable(ft))
		return ok;

	ok = rollforward_changeset_updates(tr, &ft->columns, &tt->columns, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_column, (rfdfunc) &rollforward_drop_column, (dupfunc) &column_dup, mode);
 	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &ft->tables, &tt->tables, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_add_table, (rfdfunc) &rollforward_del_table, (dupfunc) &table_find, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &ft->idxs, &tt->idxs, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_idx, (rfdfunc) &rollforward_drop_idx, (dupfunc) &idx_dup, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &ft->keys, &tt->keys, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_key, (rfdfunc) &rollforward_drop_key, (dupfunc) &key_dup, mode);
	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &ft->triggers, &tt->triggers, &tt->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_trigger, (rfdfunc) &rollforward_drop_trigger, (dupfunc) &trigger_dup, mode);

	if (ok != LOG_OK) 
		return LOG_ERR;

	if (p && mode == R_SNAPSHOT) {
		ok = store_funcs.snapshot_table(tr, ft, tt);
	} else if (p && mode == R_LOG) {
		ok = store_funcs.log_table(tr, ft, tt);
	} else if (mode == R_APPLY) {
		assert(cs_size(&tt->columns) == cs_size(&ft->columns));
		if (ft->base.rtime)
			tt->base.rtime = tr->stime;
		tt->base.wtime = tr->stime;
		if (bs_debug) 
			fprintf(stderr, "#update table %s\n", tt->base.name);
		ok = store_funcs.update_table(tr, ft, tt);
		ft->cleared = 0;
		ft->base.rtime = ft->base.wtime = 0;
	}
	return ok;
}

static int
rollforward_update_seq(sql_trans *tr, sql_sequence *ft, sql_sequence *tt, int mode)
{
	(void)tr;
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

static sql_table *
conditional_table_dup(sql_trans *tr, int flag, sql_table *ot, sql_schema *s)
{
	int p = (tr->parent == gtrans);

	/* persistent columns need to be dupped */
	if ((p && isGlobal(ot)) ||
	    /* allways dup in recursive mode */
	    tr->parent != gtrans)
		return table_dup(tr, flag, ot, s);
	else if (!isGlobal(ot)){/* is local temp, may need to be cleared */
		if (ot->commit_action == CA_DELETE) {
			sql_trans_clear_table(tr, ot);
		} else if (ot->commit_action == CA_DROP) {
			sql_trans_drop_table(tr, ot->s, ot->base.id, DROP_RESTRICT);
		}
	}
	return NULL;
}

static int
rollforward_update_schema(sql_trans *tr, sql_schema *fs, sql_schema *ts, int mode)
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
					sql_trans_drop_table(tr, t->s, t->base.id, DROP_RESTRICT);
				}
				n = nxt;
			}
		}
		return ok;
	}

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &fs->types, &ts->types, &ts->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_type, (rfdfunc) NULL, (dupfunc) &type_dup, mode);

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &fs->tables, &ts->tables, &ts->base, (rfufunc) &rollforward_update_table, (rfcfunc) &rollforward_create_table, (rfdfunc) &rollforward_drop_table, (dupfunc) &conditional_table_dup, mode);

	if (ok == LOG_OK) /* last as it may require complex (table) types */
		ok = rollforward_changeset_updates(tr, &fs->funcs, &ts->funcs, &ts->base, (rfufunc) NULL, (rfcfunc) &rollforward_create_func, (rfdfunc) NULL, (dupfunc) &func_dup, mode);

	if (ok == LOG_OK) /* last as it may require complex (table) types */
		ok = rollforward_changeset_updates(tr, &fs->seqs, &ts->seqs, &ts->base, (rfufunc) &rollforward_update_seq, (rfcfunc) &rollforward_create_seq, (rfdfunc) &rollforward_drop_seq, (dupfunc) &seq_dup, mode);

	return ok;
}

static int
rollforward_trans(sql_trans *tr, int mode)
{
	int ok = LOG_OK;

	if (mode == R_APPLY && tr->parent && tr->wtime > tr->parent->wtime) {
		tr->parent->wtime = tr->wtime;
		tr->parent->schema_updates = tr->schema_updates;
	}

	if (ok == LOG_OK)
		ok = rollforward_changeset_updates(tr, &tr->schemas, &tr->parent->schemas, (sql_base *) tr->parent, (rfufunc) &rollforward_update_schema, (rfcfunc) &rollforward_create_schema, (rfdfunc) &rollforward_drop_schema, (dupfunc) &schema_dup, mode);
	if (mode == R_APPLY) {
		if (tr->parent == gtrans) {
			gtrans->stime = tr->stime;
			
			if (tr->schema_updates) 
				schema_number++;
		}
		tr->wtime = tr->rtime = 0;
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
			sql_table *ot;

			if (!t->base.wtime && !t->base.rtime)
				continue;

 			ot = find_sql_table(os, t->base.name);
			if (ot && isTable(ot) && isTable(t)) {
				if (t->base.wtime && (t->base.wtime < ot->base.rtime || (t->base.wtime < ot->base.wtime && t->base.rtime))) 
					return 0;
				if (t->base.rtime && t->base.rtime < ot->base.wtime) 
					return 0;
				for (o = t->columns.set->h, p = ot->columns.set->h; o && p; o = o->next, p = p->next) {
					sql_column *c = o->data;
					sql_column *oc = p->data;

					/* t wrote, ie. check read and write time */
					/* read or write after t's write */
					if (c->base.wtime && (c->base.wtime < oc->base.rtime
							      /* allow for late appends, ie 
							       * wtime but no rtime 
							       */
							      || (c->base.wtime < oc->base.wtime && c->base.rtime))) {
						return 0;
					}
					/* commited write before t's read */
					if (c->base.rtime && c->base.rtime < oc->base.wtime) {
						return 0;
					}
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
			list_remove_node(fs->set, n);
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
				fb->rtime = fb->wtime = 0;
				n = n->next;
				m = m->next;
				if (bs_debug) 
					fprintf(stderr, "#reset_cs %s\n",
						(fb->name)?fb->name:"help");
			} else if (fb->id < pfb->id) {  
				node *t = n->next;
				if (bs_debug) {
					sql_base *b = n->data;
					fprintf(stderr, "#reset_cs free %s\n",
						(b->name)?b->name:"help");
				}
				list_remove_node(fs->set, n);
				n = t;
			} else { /* a new id */
				sql_base *r = fd(tr, TR_OLD, pfb,  b);
				/* cs_add_before add r to fs before node n */
				cs_add_before(fs, n, r);
				r->rtime = r->wtime = 0;
				m = m->next;
				if (bs_debug) {
					fprintf(stderr, "#reset_cs new %s\n",
						(r->name)?r->name:"help");
				}
			}
		}
		/* add new bases */
		for (; ok == LOG_OK && m; m = m->next ) {
			sql_base *pfb = m->data;
			sql_base *r = fd(tr, TR_OLD, pfb,  b);
			cs_add(fs, r, TR_OLD);
			r->rtime = r->wtime = 0;
			if (bs_debug) {
				fprintf(stderr, "#reset_cs new %s\n",
					(r->name)?r->name:"help");
			}
		}
		while ( ok == LOG_OK && n) { /* remove remaining old stuff */
			node *t = n->next;
			if (bs_debug) {
				sql_base *b = n->data;
				fprintf(stderr, "#reset_cs free %s\n",
					(b->name)?b->name:"help");
			}
			list_remove_node(fs->set, n);
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
	/* did we make changes or is the global changed after we started */
	if (fi->base.wtime || tr->stime < pfi->base.wtime) {
		if (isTable(fi->t)) {
			store_funcs.destroy_idx(NULL, fi);
			store_funcs.dup_idx(tr, pfi, fi);
		}
		fi->base.wtime = fi->base.rtime = 0;
	}
	return LOG_OK;
}

static int
reset_column(sql_trans *tr, sql_column *fc, sql_column *pfc)
{
	/* did we make changes or is the global changed after we started */
	if (fc->base.wtime || tr->stime < pfc->base.wtime) {
		if (isTable(fc->t)) {
			store_funcs.destroy_col(NULL, fc);
			store_funcs.dup_col(tr, pfc, fc);

			fc->null = pfc->null;
			fc->unique = pfc->unique;
			if (fc->storage_type) 
				_DELETE(fc->storage_type);
			fc->storage_type = NULL;
			if (pfc->storage_type)
				fc->storage_type = _strdup(pfc->storage_type);
			if (fc->def) 
				_DELETE(fc->def);
			fc->def = NULL;
			if (pfc->def)
				fc->def = _strdup(pfc->def);
		}
		fc->base.wtime = fc->base.rtime = 0;
	}
	return LOG_OK;
}

static int
reset_seq(sql_trans *tr, sql_sequence *ft, sql_sequence *pft)
{
	(void)tr;
	ft->start = pft->start;
	ft->minvalue = pft->minvalue;
	ft->maxvalue = pft->maxvalue;
	ft->increment = pft->increment;
	ft->cacheinc = pft->cacheinc;
	ft->cycle = pft->cycle;
	return LOG_OK;
}


static int
reset_table(sql_trans *tr, sql_table *ft, sql_table *pft)
{
	if (!isTable(ft) || isTempTable(ft))
		return LOG_OK;

	/* did we make changes or is the global changed after we started */
	if (ft->base.wtime || tr->stime < pft->base.wtime) {
		int ok = LOG_OK;

		if (isTable(ft)) {
			store_funcs.destroy_del(NULL, ft);
			store_funcs.dup_del(tr, pft, ft);
		}

		ft->base.wtime = ft->base.rtime = 0;
		ok = reset_changeset( tr, &ft->columns, &pft->columns, &ft->base, (resetf) &reset_column, (dupfunc) &column_dup);
		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->idxs, &pft->idxs, &ft->base, (resetf) &reset_idx, (dupfunc) &idx_dup);
		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->keys, &pft->keys, &ft->base, (resetf) NULL, (dupfunc) &key_dup);
		if (ok == LOG_OK)
			ok = reset_changeset( tr, &ft->triggers, &pft->triggers, &ft->base, (resetf) NULL, (dupfunc) &trigger_dup);
		return ok;
	}
	return LOG_OK;
}

static int
reset_schema(sql_trans *tr, sql_schema *fs, sql_schema *pfs)
{
	int ok = LOG_OK;

	/* did we make changes or is the global changed after we started */
	if (fs->base.wtime || tr->stime < pfs->base.wtime) {
		fs->base.wtime = fs->base.rtime = 0;

		ok = reset_changeset(tr, &fs->types, &pfs->types, &fs->base, (resetf) NULL, (dupfunc) &type_dup);
		if (ok == LOG_OK)
			ok = reset_changeset(tr, &fs->funcs, &pfs->funcs, &fs->base, (resetf) NULL, (dupfunc) &func_dup);

		if (ok == LOG_OK)
			ok = reset_changeset(tr, &fs->seqs, &pfs->seqs, &fs->base, (resetf) &reset_seq, (dupfunc) &seq_dup);

		if (ok == LOG_OK)
			return reset_changeset(tr, &fs->tables, &pfs->tables, &fs->base, (resetf) &reset_table, (dupfunc) &table_dup);
	}

	if (isTempSchema(fs)) {
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
					sql_trans_drop_table(tr, t->s, t->base.id, DROP_RESTRICT);
				}
				n = nxt;
			}
		}
		return ok;
	}
	return ok;
}

static int
reset_trans(sql_trans *tr, sql_trans *ptr)
{
	int res = reset_changeset(tr, &tr->schemas, &ptr->schemas, (sql_base *)tr->parent, (resetf) &reset_schema, (dupfunc) &schema_dup);
#ifdef STORE_DEBUG
	fprintf(stderr,"#reset trans %d\n", tr->wtime);
#endif
	tr->wtime = tr->rtime = 0;
	return res;
}

sql_trans *
sql_trans_create(backend_stack stk, sql_trans *parent, char *name)
{
	sql_trans *tr = NULL;

	transactions++;
	if (gtrans) {
		if (!parent && spares > 0 && !name) {
			tr = spare_trans[--spares];
#ifdef STORE_DEBUG
			fprintf(stderr, "#reuse trans (%p) %d\n", tr, spares);
#endif
		} else {
			tr = trans_dup(stk, (parent) ? parent : gtrans, name);
#ifdef STORE_DEBUG
			fprintf(stderr, "#new trans (%p)\n", tr);
#endif
		}
	}
	return tr;
}

int
sql_trans_validate(sql_trans *tr)
{
	node *n;

	/* depends on the iso level */

	if (tr->schema_number != store_schema_number())
		return 0;

	/* since we protect usage through private copies both the iso levels
	   read uncommited and read commited always succeed.
	if (tr->level == ISO_READ_UNCOMMITED || tr->level == ISO_READ_COMMITED)
		return 1;
	 */

	/* If only 'inserts' occurred on the read columns the repeatable reads
	   iso level can continue */

	/* the hard case */
	if (cs_size(&tr->schemas))
		for (n = tr->schemas.set->h; n; n = n->next) {
			sql_schema *s = n->data;
			sql_schema *os;

			if (isTempSchema(s))
				continue;

 			os = find_sql_schema(tr->parent, s->base.name);
			if (os/* || (s->base.wtime == 0 && s->base.rtime == 0)*/) {
				if (!validate_tables(s, os)) 
					return 0;
			}
		}
	return 1;
}

#ifdef CAT_DEBUG
void
catalog_corrupt( sql_trans *tr )
{
	node *k,*l;
	if (cs_size(&tr->schemas)) 
	for (k = tr->schemas.set->h; k; k = k->next) {
		sql_schema *s = k->data;

		if (cs_size(&s->tables))
		for (l = s->tables.set->h; l; l = l->next) {
			sql_table *t = l->data;

			if (!t->query && !isTempTable(t))
				table_check(tr, t);
		}
	}
}
#endif /*CAT_DEBUG*/

int
sql_trans_commit(sql_trans *tr)
{
	int ok = LOG_OK;

	/* write phase */
	if (bs_debug)
		fprintf(stderr, "#forwarding changes %d,%d\n", gtrans->stime, tr->stime);
	/* snap shots should be saved first */
	if (tr->parent == gtrans) {
		tr->stime = timestamp ();
		ok = rollforward_trans(tr, R_SNAPSHOT);

		if (ok == LOG_OK) 
			ok = logger_funcs.log_tstart();
		if (ok == LOG_OK) 
			ok = rollforward_trans(tr, R_LOG);
		if (ok == LOG_OK && prev_oid != store_oid)
			ok = logger_funcs.log_sequence(OBJ_SID, store_oid);
		prev_oid = store_oid;
		if (ok == LOG_OK)
			ok = logger_funcs.log_tend();
		tr->schema_number = store_schema_number();
	}
	if (ok == LOG_OK) {
		/* It is save to rollforward the changes now. In case 
		   of failure, the log will be replayed. */
		ok = rollforward_trans(tr, R_APPLY);
	}
	if (bs_debug)
		fprintf(stderr, "#done forwarding changes %d\n", gtrans->stime);
	return (ok==LOG_OK)?SQL_OK:SQL_ERR;
}


void
sql_trans_drop_all_dependencies(sql_trans *tr, sql_schema *s, int id, short type)
{
	int dep_id=0, t_id = -1;
	short dep_type = 0;
	sql_table *t = NULL;

	list *dep = sql_trans_get_dependencies(tr, id, type, NULL);
	node *n = dep->h, *t_n = NULL;


	while (n) {
		dep_id = *(int*) n->data;
		dep_type = *(short*) n->next->data;

		if (! list_find_id(tr->dropped, dep_id)) {

			switch (dep_type){
				case SCHEMA_DEPENDENCY :
							sql_trans_drop_schema(tr, dep_id, DROP_CASCADE);
							break;
				case TABLE_DEPENDENCY :
							sql_trans_drop_table(tr, s, dep_id, DROP_CASCADE);
							break;
				case COLUMN_DEPENDENCY :
							t_id = sql_trans_get_dependency_type(tr, dep_id, TABLE_DEPENDENCY);
							t_n = find_sql_table_node(s, NULL, t_id);
							t = t_n->data;
							sql_trans_drop_column(tr, t, dep_id, DROP_CASCADE);
							t_n = NULL;
							t = NULL;
							break;
				case VIEW_DEPENDENCY :
							sql_trans_drop_table(tr, s, dep_id, DROP_CASCADE );
							break;
				case TRIGGER_DEPENDENCY :
							sql_trans_drop_trigger(tr, s, dep_id, DROP_CASCADE);
								break;
				case KEY_DEPENDENCY :
							sql_trans_drop_key(tr, s, dep_id, DROP_CASCADE);
							break;
				case FKEY_DEPENDENCY :
							sql_trans_drop_key(tr, s, dep_id, DROP_CASCADE);
							break;
				case INDEX_DEPENDENCY :
							sql_trans_drop_idx(tr, s, dep_id, DROP_CASCADE);
							break;
				case PROC_DEPENDENCY :
				case FUNC_DEPENDENCY :
							sql_trans_drop_func(tr, s, dep_id, DROP_CASCADE);
							break;
				case USER_DEPENDENCY :  /*TODO schema and users dependencies*/
							break;
			}
		}
		
		n = n->next->next;	
	}
	list_destroy(dep);
}

static void
sys_drop_kc(sql_trans *tr, sql_key *k, sql_kc *kc)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskc = find_sql_table(syss, "objects");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(syskc, "id"), &k->base.id, NULL);

	(void) kc;		/* Stefan: unused!? */
	assert(rid != oid_nil);
	table_funcs.table_delete(tr, syskc, rid);

	if (isGlobal(k->t)) 
		tr->schema_updates ++;
}

static void
sys_drop_ic(sql_trans *tr, sql_idx * i, sql_kc *kc)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *sysic = find_sql_table(syss, "objects");
	sql_column *kc_id = find_sql_column(sysic, "id");
	oid rid = table_funcs.column_find_row(tr, kc_id, &i->base.id, NULL);

	(void) kc;		/* Stefan: unused!? */
	assert(rid != oid_nil);
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

	assert(rid != oid_nil);
	table_funcs.table_delete(tr, sysidx, rid);

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
		sql_trans_drop_all_dependencies(tr, i->t->s, i->base.id, INDEX_DEPENDENCY);
}

static void
sys_drop_key(sql_trans *tr, sql_key *k, int drop_action)
{
	node *n;
	sql_schema *syss = find_sql_schema(tr, isGlobal(k->t)?"sys":"tmp");
	sql_table *syskey = find_sql_table(syss, "keys");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(syskey, "id"), &k->base.id, NULL);

	assert(rid != oid_nil);
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
		sql_trans_drop_all_dependencies(tr, k->t->s, k->base.id, (k->type == fkey) ? FKEY_DEPENDENCY : KEY_DEPENDENCY);

}

static void
sys_drop_tc(sql_trans *tr, sql_trigger * i, sql_kc *kc)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(syss, "objects");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(systc, "id"), &i->base.id, NULL);

	(void) kc;		/* Stefan: unused!? */
	assert(rid != oid_nil);
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

	assert(rid != oid_nil);
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

	assert(rid != oid_nil);
	table_funcs.table_delete(tr, sysseqs, rid);
	sql_trans_drop_dependencies(tr, seq->base.id);

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, seq->s, seq->base.id, SEQ_DEPENDENCY);
		
}

static void
sys_drop_column(sql_trans *tr, sql_column *col, int drop_action)
{
	str seq_pos = NULL;
	const char *next_value_for = "next value for \"sys\".\"seq_";
	sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp"); 
	sql_table *syscolumn = find_sql_table(syss, "_columns");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(syscolumn, "id"),
				  &col->base.id, NULL);

	assert(rid != oid_nil);
	table_funcs.table_delete(tr, syscolumn, rid);
	sql_trans_drop_dependencies(tr, col->base.id);

	if (col->def && (seq_pos = strstr(col->def, next_value_for))) {
		sql_sequence * seq = NULL;
		char *seq_name = _strdup(seq_pos + (strlen(next_value_for) - strlen("seq_")));
		node *n = NULL;
		seq_name[strlen(seq_name)-1] = '\0';
		n = cs_find_name(&syss->seqs, seq_name);
		seq = find_sql_sequence(syss, seq_name);
		if (seq && sql_trans_get_dependency_type(tr, seq->base.id, BEDROPPED_DEPENDENCY)) {
			sys_drop_sequence(tr, seq, drop_action);		
			seq->base.wtime = syss->base.wtime = tr->wtime = tr->stime;
			cs_del(&syss->seqs, n, seq->base.flag);
		}
		_DELETE(seq_name);
	}
	
	if (isGlobal(col->t)) 
		tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, col->t->s, col->base.id, COLUMN_DEPENDENCY);
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

static void
sys_drop_columns(sql_trans *tr, sql_table *t, int drop_action)
{
	node *n;

	if (cs_size(&t->columns))
		for (n = t->columns.set->h; n; n = n->next) {
			sql_column *c = n->data;

			sys_drop_column(tr, c, drop_action);
		}
}

static void
sys_drop_table(sql_trans *tr, sql_table *t, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(syss, "_tables");
	sql_column *syscol = find_sql_column(systable, "id");
	oid rid = table_funcs.column_find_row(tr, syscol, &t->base.id, NULL);

	assert(rid != oid_nil);
	table_funcs.table_delete(tr, systable, rid);
	sys_drop_keys(tr, t, drop_action);
	sys_drop_idxs(tr, t, drop_action);

	sql_trans_drop_dependencies(tr, t->base.id);

	if (isTable(t))
		sys_drop_columns(tr, t, drop_action);

	if (isGlobal(t)) 
		tr->schema_updates ++;

	if (drop_action) 
		sql_trans_drop_all_dependencies(tr, t->s, t->base.id, isTable(t) ? TABLE_DEPENDENCY : VIEW_DEPENDENCY);
}

static void
sys_drop_type(sql_trans *tr, sql_type *type, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_type = find_sql_table(syss, "types");
	sql_column *sys_type_col = find_sql_column(sys_tab_type, "id");
	oid rid = table_funcs.column_find_row(tr, sys_type_col, &type->base.id, NULL);

	assert(rid != oid_nil);
	table_funcs.table_delete(tr, sys_tab_type, rid);

	sql_trans_drop_dependencies(tr, type->base.id);

	tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, type->s, type->base.id, TYPE_DEPENDENCY);
}


static void
sys_drop_func(sql_trans *tr, sql_func *func, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sys_tab_func = find_sql_table(syss, "functions");
	sql_column *sys_func_col = find_sql_column(sys_tab_func, "id");
	oid rid_func = table_funcs.column_find_row(tr, sys_func_col, &func->base.id, NULL);
	if (func->aggr) {
		sql_table *sys_tab_args = find_sql_table(syss, "args");
		sql_column *sys_args_col = find_sql_column(sys_tab_args, "func_id");
		oid rid_args = table_funcs.column_find_row(tr, sys_args_col, &func->base.id, NULL);
		assert(rid_args != oid_nil);
		table_funcs.table_delete(tr, sys_tab_args, rid_args);
	}

	assert(rid_func != oid_nil);
	table_funcs.table_delete(tr, sys_tab_func, rid_func);

	sql_trans_drop_dependencies(tr, func->base.id);

	tr->schema_updates ++;

	if (drop_action)
		sql_trans_drop_all_dependencies(tr, func->s, func->base.id, func->is_func ? FUNC_DEPENDENCY : PROC_DEPENDENCY);
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

static void
sys_drop_tables(sql_trans *tr, sql_schema *s, int drop_action)
{
	node *n;

	if (cs_size(&s->tables))
		for (n = s->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;

			sys_drop_table(tr, t, drop_action);
		}
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
sql_trans_create_type(sql_trans *tr, sql_schema * s, char *sqlname, int digits, int scale, int radix, char *impl)
{
	sql_type *t;
	sql_table *systype;
	int localtype = ATOMindex(impl);
	int eclass = EC_EXTERNAL;

	if (localtype < 0) 
		return NULL;
	t = ZNEW(sql_type);
	systype = find_sql_table(find_sql_schema(tr, "sys"), "types");
	base_init(NULL, &t->base, next_oid(), TR_NEW, impl);
	t->sqlname = _strdup(sqlname);
	t->digits = digits;
	t->scale = scale;
	t->radix = radix;
	t->eclass = eclass;
	t->localtype = localtype;
	t->s = s;

	cs_add(&s->types, t, TR_NEW);
	table_funcs.table_insert(tr, systype, &t->base.id, t->base.name, t->sqlname, &t->digits, &t->scale, &radix, &eclass, &s->base.id);

	t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	tr->schema_updates ++;
	return t;
}

sql_func *
sql_trans_create_func(sql_trans *tr, sql_schema * s, char *func, list *args, sql_subtype *res, bit aggr, char *mod, char *impl, char *query, int is_func)
{
	sql_func *t = ZNEW(sql_func);
	sql_table *sysfunc = find_sql_table(find_sql_schema(tr, "sys"), "functions");
	sql_table *sysarg = find_sql_table(find_sql_schema(tr, "sys"), "args");
	node *n;
	int number = 0;
	bit se, sql;

	base_init(NULL, &t->base, next_oid(), TR_NEW, func);
	assert(impl && mod);
	t->imp = (impl)?_strdup(impl):NULL;
	t->mod = (mod)?_strdup(mod):NULL; 
	sql = t->sql = (query)?1:0;
	t->aggr = aggr;
	se = t->side_effect = res?FALSE:TRUE;
	t->ops = list_dup(args, (fdup)&arg_dup);
	t->res.scale = t->res.digits = 0;
	t->res.type = NULL;
	t->is_func = is_func;
	t->query = (query)?_strdup(query):NULL;
	if (res)
		t->res = *res;
	t->s = s;

	cs_add(&s->funcs, t, TR_NEW);
	table_funcs.table_insert(tr, sysfunc, &t->base.id, t->base.name, query?query:t->imp, t->mod, &sql, &aggr, &se, &s->base.id);
	if (t->res.type) {
		char *name = "result";
		sqlid id = next_oid();

		table_funcs.table_insert(tr, sysarg, &id, &t->base.id, name, t->res.type->sqlname, &t->res.digits, &t->res.scale, &number);
		number++;
	}
	if (t->ops) for (n = t->ops->h; n; n = n->next, number++) {
		sql_arg *a = n->data;
		sqlid id = next_oid();

		table_funcs.table_insert(tr, sysarg, &id, &t->base.id, a->name, a->type.type->sqlname, &a->type.digits, &a->type.scale, &number);
	}
/*
	if (!aggr && list_length(args) > 0) {
		node *n = t->ops->h;
		sql_arg *a = n->data;
		t->res = a->type;
		list_remove_node(t->ops, n);
	}
*/

	t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	tr->schema_updates ++;
	return t;
}

void
sql_trans_drop_func(sql_trans *tr, sql_schema *s, int id, int drop_action)
{
	node *n = find_sql_func_node(s, NULL, id);
	sql_func *func = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = func->base.id;
		list_append(tr->dropped, local_id);
	}

	sys_drop_func(tr, func, DROP_CASCADE);

	func->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	tr->schema_updates ++;
	cs_del(&s->funcs, n, func->base.flag);
	
	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	
}

void
sql_trans_drop_all_func(sql_trans *tr, sql_schema *s, list * list_func, int drop_action)
{
	node *n = NULL;
	sql_func *func = NULL;

	if (!tr->dropped)
		tr->dropped = list_create((fdestroy) GDKfree);
	for (n = list_func->h; n ; n = n->next ) {
		func = (sql_func *) n->data;

		if (! list_find_id(tr->dropped, func->base.id)){ 
			int *local_id = NEW(int);

			*local_id = func->base.id;
			list_append(tr->dropped, local_id);
			sql_trans_drop_func(tr, s, func->base.id, drop_action ? DROP_CASCADE : DROP_RESTRICT);
		}
	}
	
	if ( tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
	
}

sql_schema *
sql_trans_create_schema(sql_trans *tr, char *name, int auth_id, int owner)
{
	sql_schema *s = ZNEW(sql_schema);
	sql_table *sysschema = find_sql_table(find_sql_schema(tr, "sys"), "schemas");

	base_init(NULL, &s->base, next_oid(), TR_NEW, name);
	s->auth_id = auth_id;
	s->owner = owner;
	cs_init(&s->tables, (fdestroy) &table_destroy);
	cs_init(&s->types, (fdestroy) &type_destroy);
	cs_init(&s->funcs, (fdestroy) &func_destroy);
	cs_init(&s->seqs, (fdestroy) &seq_destroy);
	s->keys = list_create((fdestroy) NULL);
	s->idxs = list_create((fdestroy) NULL);
	s->triggers = list_create((fdestroy) NULL);

	cs_add(&tr->schemas, s, TR_NEW);
	table_funcs.table_insert(tr, sysschema, &s->base.id, s->base.name, &s->auth_id, &s->owner);
	s->base.wtime = tr->wtime = tr->stime;
	tr->schema_updates ++;
	return s;
}

void
sql_trans_drop_schema(sql_trans *tr, int id, int drop_action)
{
	node *n = find_sql_schema_node(tr, NULL, id);
	sql_schema *s = n->data;
	sql_table *sysschema = find_sql_table(find_sql_schema(tr, "sys"), "schemas");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(sysschema, "id"), &s->base.id, NULL);

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = s->base.id;
		list_append(tr->dropped, local_id);
	} 

	assert(rid != oid_nil);
	table_funcs.table_delete(tr, sysschema, rid);
	sys_drop_funcs(tr, s, drop_action);
	sys_drop_tables(tr, s, drop_action);
	sys_drop_types(tr, s, drop_action);
	sys_drop_sequences(tr, s, drop_action);

	s->base.wtime = tr->wtime = tr->stime;
	tr->schema_updates ++;
	cs_del(&tr->schemas, n, s->base.flag);
	
	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
		
}

 sql_table *
sql_trans_add_table(sql_trans *tr, sql_table *mt, sql_table *pt)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(syss, "objects");
	int nr = list_length(mt->tables.set);

	/* TODO add dependency betweem mt/pt */
	cs_add(&mt->tables, pt, TR_NEW);
	mt->s->base.wtime = mt->base.wtime = tr->wtime = tr->stime;
	table_funcs.table_insert(tr, sysobj, &mt->base.id, pt->base.name, &nr);
	return mt;
}

sql_table *
sql_trans_del_table(sql_trans *tr, sql_table *mt, sql_table *pt, int drop_action)
{
	sql_schema *syss = find_sql_schema(tr, isGlobal(mt)?"sys":"tmp");
	sql_table *sysobj = find_sql_table(syss, "objects");
	node *n = cs_find_name(&mt->tables, pt->base.name);
	oid rid = table_funcs.column_find_row(tr, find_sql_column(sysobj, "name"), pt->base.name, NULL);

	/* TODO drop dependency betweem mt/pt */
	cs_del(&mt->tables, n, pt->base.flag);
	mt->s->base.wtime = mt->base.wtime = tr->wtime = tr->stime;
	table_funcs.table_delete(tr, sysobj, rid);
	if (drop_action == DROP_CASCADE)
		sql_trans_drop_table(tr, pt->s, pt->base.id, drop_action);
	return mt;
}

sql_table *
sql_trans_create_table(sql_trans *tr, sql_schema *s, char *name, char *sql, int tt, bit system, int persistence, int commit_action, int sz)
{
	sql_table *t = create_sql_table(NULL, name, tt, system, persistence, commit_action);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systable = find_sql_table(syss, "_tables");
	sht ca;

	/* temps all belong to a special tmp schema and only views
	   have a query */
	assert( (isTable(t) ||
		(!isTempTable(t) || (strcmp(s->base.name, "tmp") == 0) || isDeclaredTable(t))) || (isView(t) && !sql) || isStream(t));

	t->query = sql ? _strdup(sql) : NULL;
	t->s = s;
	t->sz = sz;
	if (sz < 0)
		t->sz = COLSIZE;
	cs_add(&s->tables, t, TR_NEW);

	if (isTable(t))
		store_funcs.create_del(tr, t);

	ca = t->commit_action;
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, systable, &t->base.id, t->base.name, &s->base.id,
			(t->query) ? t->query : ATOMnilptr(TYPE_str), &t->type,
			&t->system, &ca, &t->readonly);

	t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return t;
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
create_sql_ukey(sql_allocator *sa, sql_table *t, char *name, key_type kt)
{
	sql_key *nk = NULL;
	sql_ukey *tk;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);
 	tk = (sql_ukey *) nk;
	assert(name);

	base_init(sa, &nk->base, next_oid(), TR_NEW, name);

	nk->type = kt;
	nk->columns = list_new(sa);
	nk->idx = NULL;
	nk->t = t;

	tk->keys = NULL;
	if (nk->type == pkey)
		t->pkey = tk;
	cs_add(&t->keys, nk, TR_NEW);
	return tk;
}

sql_fkey *
create_sql_fkey(sql_allocator *sa, sql_table *t, char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
{
	sql_key *nk;
	sql_fkey *fk = NULL;
	sql_ukey *uk = (sql_ukey *) rkey;

	nk = (kt != fkey) ? (sql_key *) SA_ZNEW(sa, sql_ukey) : (sql_key *) SA_ZNEW(sa, sql_fkey);

	assert(name);
	base_init(sa, &nk->base, next_oid(), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_new(sa);
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
	if (i->type == hash_idx && list_length(i->columns) == 1 && ic->c->sorted) 
		//i->type = oph_idx;
		i->type = no_idx;

	return i;
}

sql_idx *
create_sql_idx(sql_allocator *sa, sql_table *t, char *name, idx_type it)
{
	sql_idx *ni = SA_ZNEW(sa, sql_idx);

	base_init(sa, &ni->base, next_oid(), TR_NEW, name);
	ni->columns = list_new(sa);
	ni->t = t;
	ni->type = it;
	ni->key = NULL;
	cs_add(&t->idxs, ni, TR_NEW);
	return ni;
}

sql_column *
create_sql_column(sql_allocator *sa, sql_table *t, char *name, sql_subtype *tpe)
{
	sql_column *col = (sa)?SA_ZNEW(sa, sql_column):ZNEW(sql_column);

	base_init(sa, &col->base, next_oid(), TR_NEW, name);
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

void
sql_trans_drop_table(sql_trans *tr, sql_schema *s, int id, int drop_action)
{
	node *n = find_sql_table_node(s, NULL, id);
	sql_table *t = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = t->base.id;
		list_append(tr->dropped, local_id);
	}
		
	if (!isDeclaredTable(t))
		sys_drop_table(tr, t, drop_action);

	t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t) || (t->commit_action != CA_DROP)) 
		tr->schema_updates ++;
	cs_del(&s->tables, n, t->base.flag);
	
	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
}

BUN
sql_trans_clear_table(sql_trans *tr, sql_table *t)
{
	node *n = t->columns.set->h;
	sql_column *c = n->data;
	BUN sz = 0;

	t->cleared = 1;
	t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	c->base.wtime = tr->stime;

	sz += store_funcs.clear_col(tr, c);
	sz -= store_funcs.clear_del(tr, t);

	for (n = n->next; n; n = n->next) {
		c = n->data;
		c->base.wtime = tr->stime;

		(void)store_funcs.clear_col(tr, c);
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *ci = n->data;

			ci->base.wtime = tr->stime;
			(void)store_funcs.clear_idx(tr, ci);
		}
	}
	return sz;
}

sql_column *
sql_trans_create_column(sql_trans *tr, sql_table *t, char *name, sql_subtype *tpe)
{
	sql_column *col;
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *syscolumn = find_sql_table(syss, "_columns");

	if (!tpe)
		return NULL;

 	col = create_sql_column(NULL, t, name, tpe );

	if (isTable(col->t))
		store_funcs.create_col(tr, col);
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, syscolumn, &col->base.id, col->base.name, col->type.type->sqlname, &col->type.digits, &col->type.scale, &t->base.id, (col->def) ? col->def : ATOMnilptr(TYPE_str), &col->null, &col->colnr, (col->storage_type) ? col->storage_type : ATOMnilptr(TYPE_str));

	col->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return col;
}

void 
drop_sql_column(sql_table *t, int id, int drop_action)
{
	node *n = list_find_base_id(t->columns.set, id);
	sql_column *col = n->data;

	col->drop_action = drop_action; 
	cs_del(&t->columns, n, TR_OLD);
}

void 
drop_sql_idx(sql_table *t, int id)
{
	node *n = list_find_base_id(t->idxs.set, id);

	cs_del(&t->idxs, n, TR_OLD);
}

void 
drop_sql_key(sql_table *t, int id, int drop_action)
{
	node *n = list_find_base_id(t->keys.set, id);
	sql_key *k = n->data;

	k->drop_action = drop_action; 
	cs_del(&t->keys, n, TR_OLD);
}



void
sql_trans_drop_column(sql_trans *tr, sql_table *t, int id, int drop_action)
{
	node *n = list_find_base_id(t->columns.set, id);
	sql_column *col = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = col->base.id;
		list_append(tr->dropped, local_id);
	}
	
	if (isTable(t))
		sys_drop_column(tr, col, drop_action);

	col->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	cs_del(&t->columns, n, col->base.flag);
	if (isGlobal(t)) 
		tr->schema_updates ++;
	
	if (drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
}

sql_column *
sql_trans_alter_null(sql_trans *tr, sql_column *col, int isnull)
{
	if (col->null != isnull) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp"); 
		sql_table *syscolumn = find_sql_table(syss, "_columns");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(syscolumn, "id"),
					  &col->base.id, NULL);

		assert(rid != oid_nil);
		table_funcs.column_update_value(tr, find_sql_column(syscolumn, "null"), rid, &isnull);
		col->null = isnull;
		col->base.wtime = col->t->base.wtime = col->t->s->base.wtime = tr->wtime = tr->stime;
		if (isGlobal(col->t)) 
			tr->schema_updates ++;
	}

	return col;
}

sql_table *
sql_trans_alter_readonly(sql_trans *tr, sql_table *t, int readonly)
{
	if (t->readonly != readonly) {
		sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp"); 
		sql_table *systable = find_sql_table(syss, "_tables");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(systable, "id"),
					  &t->base.id, NULL);

		assert(rid != oid_nil);
		table_funcs.column_update_value(tr, find_sql_column(systable, "readonly"), rid, &readonly);
		t->readonly = readonly;
		t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
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
		void *p = val ? val : ATOMnilptr(TYPE_str);
		sql_schema *syss = find_sql_schema(tr, isGlobal(col->t)?"sys":"tmp"); 
		sql_table *syscolumn = find_sql_table(syss, "_columns");
		sql_column *col_ids = find_sql_column(syscolumn, "id");
		sql_column *col_dfs = find_sql_column(syscolumn, "default");
		oid rid = table_funcs.column_find_row(tr, col_ids, &col->base.id, NULL);

		assert(rid != oid_nil);
		table_funcs.column_update_value(tr, col_dfs, rid, p);
		if (col->def)
			_DELETE(col->def);
		col->def = NULL;
		if (val)
			col->def = _strdup(val);
		col->base.wtime = col->t->base.wtime = col->t->s->base.wtime = tr->wtime = tr->stime;
		if (isGlobal(col->t)) 
			tr->schema_updates ++;
	}
	return col;
}

int
sql_trans_is_sorted( sql_trans *tr, sql_column *col )
{
	if (col && store_funcs.sorted_col(tr, col))
		return 1;
	return 0;
}

sql_key *
sql_trans_create_ukey(sql_trans *tr, sql_table *t, char *name, key_type kt)
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

	nk = (kt != fkey) ? (sql_key *) ZNEW(sql_ukey)
	: (sql_key *) ZNEW(sql_fkey);

	assert(name);
	base_init(NULL, &nk->base, next_oid(), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_create((fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = NULL;

	uk = (sql_ukey *) nk;

	uk->keys = NULL;

	if (nk->type == pkey) 
		t->pkey = uk;

	cs_add(&t->keys, nk, TR_NEW);
	list_append(t->s->keys, nk);

	table_funcs.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey->k.base.id : &neg, &action );

	syskey->base.wtime = syskey->s->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return nk;
}

sql_fkey *
sql_trans_create_fkey(sql_trans *tr, sql_table *t, char *name, key_type kt, sql_key *rkey, int on_delete, int on_update)
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

	nk = (kt != fkey) ? (sql_key *) ZNEW(sql_ukey)
	: (sql_key *) ZNEW(sql_fkey);

	assert(name);
	base_init(NULL, &nk->base, next_oid(), TR_NEW, name);
	nk->type = kt;
	nk->columns = list_create((fdestroy) &kc_destroy);
	nk->t = t;
	nk->idx = sql_trans_create_idx(tr, t, name, (nk->type == fkey) ? join_idx : hash_idx);
	nk->idx->key = nk;

	fk = (sql_fkey *) nk;

	fk->on_delete = on_delete;
	fk->on_update = on_update;	

	fk->rkey = uk;
	if (!uk->keys)
		uk->keys = list_create(NULL);
	list_append(uk->keys, fk);

	cs_add(&t->keys, nk, TR_NEW);
	list_append(t->s->keys, nk);

	table_funcs.table_insert(tr, syskey, &nk->base.id, &t->base.id, &nk->type, nk->base.name, (nk->type == fkey) ? &((sql_fkey *) nk)->rkey->k.base.id : &neg, &action);

	sql_trans_create_dependency(tr, ((sql_fkey *) nk)->rkey->k.base.id, nk->base.id, FKEY_DEPENDENCY);

	syskey->base.wtime = syskey->s->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return (sql_fkey*) nk;
}


sql_key *
sql_trans_create_kc(sql_trans *tr, sql_key *k, sql_column *c )
{
	sql_kc *kc = ZNEW(sql_kc);
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

	syskc->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(k->t)) 
		tr->schema_updates ++;
	return k;
}


sql_fkey *
sql_trans_create_fkc(sql_trans *tr, sql_fkey *fk, sql_column *c )
{
	sql_key *k = (sql_key *) fk;
	sql_kc *kc = ZNEW(sql_kc);
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

	syskc->base.wtime = tr->wtime = tr->stime;
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
		for(nr = 0; nr<len; nr++)
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
		return k;
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

void
sql_trans_drop_key(sql_trans *tr, sql_schema *s, int id, int drop_action)
{
	node *n = list_find_base_id(s->keys, id);
	sql_key *k = n->data;

	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = k->base.id;
		list_append(tr->dropped, local_id);
	}

	if (k->idx)
		sql_trans_drop_idx(tr, s, k->idx->base.id, drop_action);

	/*Clean the key from the keys*/
	n = cs_find_name(&k->t->keys, k->base.name);
	if (n)
		cs_del(&k->t->keys, n, k->base.flag);

	if (!isTempTable(k->t)) 
		sys_drop_key(tr, k, drop_action);

	k->base.wtime = k->t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(k->t)) 
		tr->schema_updates ++;

	if (  drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}

}

sql_idx *
sql_trans_create_idx(sql_trans *tr, sql_table *t, char *name, idx_type it)
{
	/* can only have idxs between persistent tables */
	sql_idx *ni = ZNEW(sql_idx);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *sysidx = find_sql_table(syss, "idxs");

	assert(name);
	base_init(NULL, &ni->base, next_oid(), TR_NEW, name);
	ni->type = it;
	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->key = NULL;

	cs_add(&t->idxs, ni, TR_NEW);
	list_append(t->s->idxs, ni);

	if (!isDeclaredTable(t) && isTable(ni->t) && idx_has_column(ni->type))
		store_funcs.create_idx(tr, ni);
	if (!isDeclaredTable(t))
		table_funcs.table_insert(tr, sysidx, &ni->base.id, &t->base.id, &ni->type, ni->base.name);
	ni->base.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return ni;
}

sql_idx *
sql_trans_create_ic(sql_trans *tr, sql_idx * i, sql_column *c)
{
	sql_kc *ic = ZNEW(sql_kc);
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
	if (0 && i->type == hash_idx && list_length(i->columns) == 1 &&
	    store_funcs.count_col(ic->c) && store_funcs.sorted_col(tr, ic->c)) {
		sql_table *sysidx = find_sql_table(syss, "idxs");
		sql_column *sysidxid = find_sql_column(sysidx, "id");
		sql_column *sysidxtype = find_sql_column(sysidx, "type");
		oid rid = table_funcs.column_find_row(tr, sysidxid, &i->base.id, NULL);
	
		//i->type = oph_idx;
		i->type = no_idx;
		table_funcs.column_update_value(tr, sysidxtype, rid, &i->type);
	}

	table_funcs.table_insert(tr, sysic, &i->base.id, ic->c->base.name, &nr);
	sysic->base.wtime = sysic->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(i->t)) 
		tr->schema_updates ++;
	return i;
}

void
sql_trans_drop_idx(sql_trans *tr, sql_schema *s, int id, int drop_action)
{
	node *n = list_find_base_id(s->idxs, id);
	sql_idx *i = n->data;
	
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = i->base.id;
		list_append(tr->dropped, local_id);
	}
	

	if (!isTempTable(i->t))
		sys_drop_idx(tr, i, drop_action);

	i->base.wtime = i->t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(i->t)) 
		tr->schema_updates ++;
	n = cs_find_name(&i->t->idxs, i->base.name);
	if (n)
		cs_del(&i->t->idxs, n, i->base.flag);
	
	if (  drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
}

sql_trigger *
sql_trans_create_trigger(sql_trans *tr, sql_table *t, char *name, 
	sht time, sht orientation, sht event, char *old_name, char *new_name,
	char *condition, char *statement )
{
	sql_trigger *ni = ZNEW(sql_trigger);
	sql_schema *syss = find_sql_schema(tr, isGlobal(t)?"sys":"tmp");
	sql_table *systrigger = find_sql_table(syss, "triggers");
	str nilptr = ATOMnilptr(TYPE_str);

	assert(name);
	base_init(NULL, &ni->base, next_oid(), TR_NEW, name);
	ni->columns = list_create((fdestroy) &kc_destroy);
	ni->t = t;
	ni->time = time;
	ni->orientation = orientation;
	ni->event = event;
	ni->old_name = ni->new_name = ni->condition = NULL; 
	if (old_name)
		ni->old_name = _strdup(old_name);
	if (new_name)
		ni->new_name = _strdup(new_name);
	if (condition)
		ni->condition = _strdup(condition);
	ni->statement = _strdup(statement);

	cs_add(&t->triggers, ni, TR_NEW);
	list_append(t->s->triggers, ni);

	table_funcs.table_insert(tr, systrigger, &ni->base.id, ni->base.name, &t->base.id, &ni->time, &ni->orientation, &ni->event, (ni->old_name)?ni->old_name:nilptr, (ni->new_name)?ni->new_name:nilptr, (ni->condition)?ni->condition:nilptr, ni->statement);

	t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(t)) 
		tr->schema_updates ++;
	return ni;
}

sql_trigger *
sql_trans_create_tc(sql_trans *tr, sql_trigger * i, sql_column *c )
{
	sql_kc *ic = ZNEW(sql_kc);
	int nr = list_length(i->columns);
	sql_schema *syss = find_sql_schema(tr, isGlobal(i->t)?"sys":"tmp");
	sql_table *systc = find_sql_table(syss, "objects");

	assert(c);
	ic->c = c;
	list_append(i->columns, ic);
	table_funcs.table_insert(tr, systc, &i->base.id, ic->c->base.name, &nr);
	systc->base.wtime = systc->s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(i->t)) 
		tr->schema_updates ++;
	return i;
}

void
sql_trans_drop_trigger(sql_trans *tr, sql_schema *s, int id, int drop_action)
{
	node *n = list_find_base_id(s->triggers, id);
	sql_trigger *i = n->data;
	
	if (drop_action == DROP_CASCADE_START || drop_action == DROP_CASCADE) {
		int *local_id = NEW(int);

		if (! tr->dropped) 
			tr->dropped = list_create((fdestroy) GDKfree);
		*local_id = i->base.id;
		list_append(tr->dropped, local_id);
	}
	
	sys_drop_trigger(tr, i);
	i->base.wtime = i->t->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	if (isGlobal(i->t)) 
		tr->schema_updates ++;
	n = cs_find_name(&i->t->triggers, i->base.name);
	if (n)
		cs_del(&i->t->triggers, n, i->base.flag);
	
	if (  drop_action == DROP_CASCADE_START && tr->dropped) {
		list_destroy(tr->dropped);
		tr->dropped = NULL;
	}
}

sql_sequence *
create_sql_sequence(sql_allocator *sa, sql_schema *s, char *name, lng start, lng min, lng max, lng inc, lng cacheinc, bit cycle) 
{
	sql_sequence *seq = (sa)?SA_ZNEW(sa, sql_sequence):ZNEW(sql_sequence);

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
sql_trans_create_sequence(sql_trans *tr, sql_schema *s, char *name, lng start, lng min, lng max, lng inc, lng cacheinc, bit cycle, bit bedropped )
{
	sql_schema *syss = find_sql_schema(tr, "sys");
	sql_table *sysseqs = find_sql_table(syss, "sequences");
	sql_sequence *seq = create_sql_sequence(NULL, s, name, start, min, max, inc, cacheinc, cycle);

	cs_add(&s->seqs, seq, TR_NEW);
	table_funcs.table_insert(tr, sysseqs, &seq->base.id, &s->base.id, seq->base.name, &seq->start, &seq->minvalue, &seq->maxvalue, &seq->increment, &seq->cacheinc, &seq->cycle);
	s->base.wtime = tr->wtime = tr->stime;

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
	seq->base.wtime = s->base.wtime = tr->wtime = tr->stime;
	cs_del(&s->seqs, n, seq->base.flag);
}

sql_sequence *
sql_trans_alter_sequence(sql_trans *tr, sql_sequence *seq, lng min, lng max, lng inc, lng cache, lng cycle)
{
	sql_schema *syss = find_sql_schema(tr, "sys"); 
	sql_table *seqs = find_sql_table(syss, "sequences");
	oid rid = table_funcs.column_find_row(tr, find_sql_column(seqs, "id"), &seq->base.id, NULL);
	sql_column *c;
	int changed = 0;

	assert(rid != oid_nil);
	if (min >= 0 && seq->minvalue != min) {
		seq->minvalue = min; 
		c = find_sql_column(seqs, "minvalue");
		table_funcs.column_update_value(tr, c, rid, &seq->minvalue);
	}
	if (max >= 0 && seq->maxvalue != max) {
		seq->maxvalue = max; 
		changed = 1;
		c = find_sql_column(seqs, "maxvalue");
		table_funcs.column_update_value(tr, c, rid, &seq->maxvalue);
	}
	if (inc >= 0 && seq->increment != inc) {
		seq->increment = inc; 
		changed = 1;
		c = find_sql_column(seqs, "increment");
		table_funcs.column_update_value(tr, c, rid, &seq->increment);
	}
	if (cache >= 0 && seq->cacheinc != cache) {
		seq->cacheinc = cache; 
		changed = 1;
		c = find_sql_column(seqs, "cacheinc");
		table_funcs.column_update_value(tr, c, rid, &seq->cacheinc);
	}
	if (seq->cycle != cycle) {
		seq->cycle = cycle != 0; 
		changed = 1;
		c = find_sql_column(seqs, "cycle");
		table_funcs.column_update_value(tr, c, rid, &seq->cycle);
	}

	if (changed) {
		seq->base.wtime = seq->s->base.wtime = tr->wtime = tr->stime;
		tr->schema_updates ++;
	}
	return seq;
}

lng 
sql_trans_sequence_restart(sql_trans *tr, sql_sequence *seq, lng start)
{
	if (seq->start != start) {
		sql_schema *syss = find_sql_schema(tr, "sys"); 
		sql_table *seqs = find_sql_table(syss, "sequences");
		oid rid = table_funcs.column_find_row(tr, find_sql_column(seqs, "id"),
				  &seq->base.id, NULL);
		sql_column *c = find_sql_column(seqs, "start");

		assert(rid != oid_nil);
		seq->start = start; 
		table_funcs.column_update_value(tr, c, rid, &seq->start);

		seq->base.wtime = seq->s->base.wtime = tr->wtime = tr->stime;
		tr->schema_updates ++;
	}
	seq_restart(seq, seq->start);
	return seq->start;
}

sql_session *
sql_session_create(backend_stack stk, int ac )
{
	sql_session *s = ZNEW(sql_session);

	if (!s)
		return NULL;
	s->tr = sql_trans_create(s->stk, NULL, NULL);
	s->schema_name = NULL;
	s->active = 0;
	s->stk = stk;
	sql_session_reset(s, ac);
	nr_sessions++;
	return s;
}

void
sql_session_destroy(sql_session *s) 
{
	if (s->tr)
		sql_trans_destroy(s->tr);
	if (s->schema_name)
		_DELETE(s->schema_name);
	_DELETE(s);
	nr_sessions--;
}

void
sql_session_reset(sql_session *s, int ac) 
{
	sql_schema *tmp;

	if (!s->tr)
		return;

	/* TODO cleanup "dt" schema */
	tmp = find_sql_schema(s->tr, "tmp");
		
	if (tmp->tables.set) {
		node *n;
		for (n = tmp->tables.set->h; n; n = n->next) {
			sql_table *t = n->data;

			if (isGlobal(t) && isTable(t))
				sql_trans_clear_table(s->tr, t);
		}
	}
	assert(s->active == 0);

	if (s->schema_name)
		_DELETE(s->schema_name);
	s->schema_name = _strdup("sys");
	s->schema = NULL;
	s->auto_commit = s->ac_on_commit = ac;
	s->level = ISO_SERIALIZABLE;
}

int
sql_trans_begin(sql_session *s)
{
	sql_trans *tr = s->tr;
	int snr = tr->schema_number;

#ifdef STORE_DEBUG
	fprintf(stderr,"#sql trans begin %d\n", snr);
#endif
	if (tr->stime < gtrans->stime || tr->wtime || 
			store_schema_number() != snr) 
		reset_trans(tr, gtrans);
	tr = trans_init(tr, tr->stk, tr->parent);
	s->active = 1;
	s->schema = find_sql_schema(tr, s->schema_name);
	s->tr = tr;
	store_nr_active ++;
	s->status = 0;
#ifdef STORE_DEBUG
	fprintf(stderr,"#sql trans begin (%d)\n", tr->schema_number);
#endif
	return snr != tr->schema_number;
}

void
sql_trans_end(sql_session *s)
{
#ifdef STORE_DEBUG
	fprintf(stderr,"#sql trans end (%d)\n", s->tr->schema_number);
#endif
	s->active = 0;
	s->auto_commit = s->ac_on_commit;
	store_nr_active --;
}
