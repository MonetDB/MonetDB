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
#include "restrict_storage.h"
#include <bat/bat_utils.h>
#include <sql_string.h>

#define SNAPSHOT_MINSIZE (1024)

static BAT *
bind_col(sql_trans *tr, sql_column *c, int access)
{
	BAT *b;
	sql_bat *bat = c->data;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access != RD_UPD && access != RD_INS);
 	b = temp_descriptor(bat->bid);
	assert(b);
	bat_set_access(b, BAT_READ);
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	return b;
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	BAT *b;
	sql_bat *bat = i->data;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access != RD_UPD && access != RD_INS);
 	b = temp_descriptor(bat->bid);
	assert(b);
	bat_set_access(b, BAT_READ);
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return b;
}

static BAT *
bind_del(sql_trans *tr, sql_table *t, int access)
{
	BAT *b;
	sql_bat *bat = t->data;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access == RDONLY);
	b = temp_descriptor(bat->bid);
	assert(b);
	t->s->base.rtime = t->base.rtime = tr->stime;
	return b;
}

/* we could have an unsafe mode which simply forgets the old values */
static void
update_bat( sql_bat *bat, BAT *upd, int isnew) 
{
	BAT *b;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
 	b = temp_descriptor(bat->bid);
	if (!isnew) {
		BAT *u = NULL;
		BAT *old = BATkdiff(b, upd), *r;

		if (bat->ubid) {
			u = temp_descriptor(bat->ubid);
		} else {
			u = bat_new(TYPE_oid, b->ttype, 1);
			bat->ubid = temp_create(u);
		}
		r = BATkdiff(old, u); /* don't keep allready updated values */ 
		bat_destroy(old);
		BATins(u, r, FALSE);
		bat_destroy(u);
		bat_destroy(r);
	}
	void_replace_bat(b, upd, TRUE);
	bat_destroy(b);
}

static void
update_val( sql_bat *bat, oid rid, void *upd, int isnew) 
{
	BAT *b;

	assert(rid != oid_nil);
	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	b = temp_descriptor(bat->bid);
	if (!isnew) {
		BAT *u = NULL;

		if (bat->ubid) {
			u = temp_descriptor(bat->ubid);
		} else {
			u = bat_new(TYPE_oid, b->ttype, 1);
			bat->ubid = temp_create(u);
		}
		if (BUNfnd(u, (ptr)&rid) == BUN_NONE) {
			BUN p;
			BATiter bi = bat_iterator(b);
			/* avoid "dereferencing type-punned pointer will break strict-aliasing rules" */
			ptr _rid = (ptr)&rid;
			BUNfndVOID(p, bi, _rid);  
			BUNins(u, &rid, BUNtail(bi,p), FALSE);
		}
		bat_destroy(u);
	}
	assert(rid != oid_nil);
#if SIZEOF_OID < SIZEOF_SSIZE_T
	assert((lng) rid <= (lng) GDK_oid_max);
#endif
	void_inplace(b, rid, upd, TRUE);
	bat_destroy(b);
}

/* for now we simply apply the updates, problem is we can't rollback */

static void
update_col(sql_trans *tr, sql_column *c, void *i, int tpe, oid rid)
{
	sql_bat *bat = c->data;

	c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->stime;
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		update_bat(bat, i, isNew(c));
	else 
		update_val(bat, rid, i, isNew(c));
}

static void 
update_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	sql_bat *bat = i->data;

	i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->stime;
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		update_bat(bat, ib, isNew(i));
	else
		assert(0);
}

static void
append_bat( sql_bat *bat, BAT *i ) 
{
	BAT *b = temp_descriptor(bat->bid);

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	bat->cnt += BATcount(i);
	if (BATcount(b) == 0 && !isVIEW(i) && i->htype == TYPE_void && i->ttype != TYPE_void){
		temp_destroy(bat->bid);
		bat->bid = temp_create(i);
		BATseqbase(i, 0);
	} else {
		BATappend(b, i, TRUE);
	}
	bat_destroy(b);
}

static void
append_val( sql_bat *bat, void *i ) 
{
	BAT *b;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
 	b = temp_descriptor(bat->bid);
	BUNappend(b, i, TRUE);
	bat->cnt ++;
	bat_destroy(b);
}

static void 
append_col(sql_trans *tr, sql_column *c, void *i, int tpe)
{
	sql_bat *bat = c->data;

	c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->stime;
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		append_bat(bat, i);
	else
		append_val(bat, i);
}

static void
append_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	sql_bat *bat = i->data;

	i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->stime;
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		append_bat(bat, ib);
	else
		append_val(bat, ib);
}

static void
delete_bat( sql_bat *bat, BAT *i ) 
{
	BAT *b = temp_descriptor(bat->bid);

	if (BATcount(b) == 0 && !isVIEW(i) && i->htype == TYPE_void && i->ttype != TYPE_void){
		temp_destroy(bat->bid);
		bat->bid = temp_create(i);
	} else {
		BATappend(b, i, TRUE);
	}
	bat_destroy(b);
}

static void
delete_val( sql_bat *bat, ptr i ) 
{
	BAT *b = temp_descriptor(bat->bid);

	BUNappend(b, i, TRUE);
	bat_destroy(b);
}

static void
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	node *n;
	sql_bat *bat = t->data;

	/* delete all cached copies */
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_bat *bat = c->data;

		if (bat->cached) {
			bat_destroy(bat->cached);
			bat->cached = NULL;
		}
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			sql_bat *bat = i->data;

			if (bat->cached) {
				bat_destroy(bat->cached);
				bat->cached = NULL;
			}
		}
	}


	t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	t->base.rtime = t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		delete_bat(bat, ib);
	else
		delete_val(bat, ib);
}

static size_t
count_col(sql_column *col)
{
	sql_bat *b = col->data;
	return b->cnt;
}

static size_t
count_idx(sql_idx *idx)
{
	sql_bat *b = idx->data;
	return b->cnt;
}

static int
sorted_col(sql_trans *tr, sql_column *col)
{
	BAT *b = bind_col(tr, col, RDONLY);
	int sorted = BATtordered(b);
	
	bat_destroy(b);
	return sorted;
}

static int 
load_bat( sql_bat *bat) 
{
	BAT *b;

	b = quick_descriptor(logger_find_bat(restrict_logger, bat->name));
	bat->bid = temp_create(b);
	bat->cnt = (size_t)BATcount(b);
	return LOG_OK;
}

static int
log_new_persistent_bat(sql_trans *tr, sql_bat *bat) 
{
	int ok = LOG_OK;
	BAT *b = temp_descriptor(bat->bid);

	(void)tr;
	logger_add_bat(restrict_logger, b, bat->name);
	ok = log_bat_persists(restrict_logger, b, bat->name);
	bat_destroy(b);
	return ok;
}

static int
snapshot_new_persistent_bat(sql_trans *tr, sql_bat *bat) 
{
	int ok = LOG_OK;
	BAT *b = temp_descriptor(bat->bid);

	(void)tr;
	/* snapshot large bats */
	bat_set_access(b, BAT_READ);
	if (BATcount(b) > SNAPSHOT_MINSIZE)
		BATmode(b, PERSISTENT);
	if (BATcount(b) > (BUN) REMAP_PAGE_MAXSIZE)
       		BATmmap(b, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
	bat_destroy(b);
	return ok;
}

static int
new_persistent_bat(sql_trans *tr, sql_bat *bat) 
{
	BAT *b = temp_descriptor(bat->bid);

	(void)tr;
	bat_set_access(b, BAT_READ);
	BATcommit(b);
	bat_destroy(b);
	return LOG_OK;
}

static int
create_col(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	int type = c->type.type->localtype;
	sql_bat *bat = c->data;
	if (!bat)
		c->data = bat = ZNEW(sql_bat);
	if (!bat->name) 
		bat->name = sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name);

	(void)tr;
	if (c->base.flag == TR_OLD && !isTempTable(c->t)){
		return load_bat(bat);
	} else if (bat->bid && !isTempTable(c->t)) {
		assert(active_store_type == store_su);
		return new_persistent_bat( tr, c->data);
	} else {
		if (!bat)
 			bat = c->data = ZNEW(sql_bat);
		if (!bat->bid) {
			BAT *b = bat_new(TYPE_void, type, c->t->sz);
			if (!b) 
				return LOG_ERR;
			bat->bid = temp_create(b);
			bat_destroy(b);
		}
	}
	return ok;
}

static int
log_create_col(sql_trans *tr, sql_column *c)
{
	assert(tr->parent == gtrans && !isTempTable(c->t));
	return log_new_persistent_bat( tr, c->data);
}


static int
snapshot_create_col(sql_trans *tr, sql_column *c)
{
	assert(tr->parent == gtrans && !isTempTable(c->t));
	return snapshot_new_persistent_bat( tr, c->data);
}

/* will be called for new idx's and when new index columns are create */
static int
create_idx(sql_trans *tr, sql_idx *ni)
{
	int ok = LOG_OK;
	sql_bat *bat = ni->data;
	int type = TYPE_wrd;

	if (ni->type == join_idx)
		type = TYPE_oid;

	/* no index bats for single column hash indices */
/* TODO single column indices ! 
		if (ni->key) {
			sql_kc *c = ni->columns->h->data;
			BAT *b = bind_col(tr->parent, c->c, RDONLY);

			BATkey(BATmirror(b), BOUND2BTRUE);
			bat_destroy(b);
		}
		return LOG_OK;
	}
*/

	if (!bat)
		ni->data = bat = ZNEW(sql_bat);
	if (!bat->name) 
		bat->name = sql_message("%s_%s_%s", ni->t->s->base.name, ni->t->base.name, ni->base.name);



	/* create bats for a loaded idx structure */
	if (ni->base.flag == TR_OLD && !isTempTable(ni->t)){
		return load_bat( bat);
	} else if (bat->bid && !isTempTable(ni->t)) { 
		assert(active_store_type == store_su);
		return new_persistent_bat( tr, ni->data);
	} else if (!bat->bid) {
		BAT *b = bat_new(TYPE_void, type, ni->t->sz);
		if (!b) 
			return LOG_ERR;
		bat->bid = temp_create(b);
		bat_destroy(b);
	}
	return ok;
}

static int
log_create_idx(sql_trans *tr, sql_idx *ni)
{
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return log_new_persistent_bat( tr, ni->data);
}

static int
snapshot_create_idx(sql_trans *tr, sql_idx *ni)
{
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return snapshot_new_persistent_bat( tr, ni->data);
}

static int
create_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	BAT *b;
	sql_bat *bat = t->data;

	if (!bat)
		bat = t->data = ZNEW(sql_bat);
	if (!bat->name)
		bat->name = sql_message("D_%s_%s", t->s->base.name, t->base.name);
	(void)tr;
	if (t->base.flag == TR_OLD && !isTempTable(t)) {
		b = quick_descriptor(logger_find_bat(restrict_logger, bat->name));
		bat->bid = temp_create(b);
	} else if (bat->bid && !isTempTable(t)) {
		assert(active_store_type == store_su);
		b = temp_descriptor(bat->bid);
		bat->bid = temp_create(b);
		bat_destroy(b);
	} else if (!bat->bid) {
		b = bat_new(TYPE_void, TYPE_oid, t->sz);
		bat_set_access(b, BAT_READ);
		bat->bid = temp_create(b);
		bat_destroy(b);
	}
	return ok;
}

static int
log_create_del(sql_trans *tr, sql_table *t)
{
	sql_bat *bat = t->data;
	BAT *b = temp_descriptor(bat->bid);
	int ok = LOG_OK;

	(void)tr;
	assert(!isTempTable(t));
	(void) logger_add_bat(restrict_logger, b, bat->name);
	ok = log_bat_persists(restrict_logger, b, bat->name);
	bat_destroy(b);
	return ok;
}

static int
snapshot_create_del(sql_trans *tr, sql_table *t)
{
	sql_bat *bat = t->data;
	BAT *b = temp_descriptor(bat->bid);

	(void)tr;
	/* snapshot large bats */
	bat_set_access(b, BAT_READ);
	if (BATcount(b) > SNAPSHOT_MINSIZE) 
		BATmode(b, PERSISTENT);
	if (BATcount(b) > (BUN) REMAP_PAGE_MAXSIZE)
       		BATmmap(b, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
	bat_destroy(b);
	return LOG_OK;
}

static int 
dup_bat(sql_trans *tr, sql_bat *obat, sql_bat *bat)
{
	(void)tr;
	if (!obat)
		return LOG_OK;
	bat->bid = obat->bid;
	bat->cnt = obat->cnt;
	if (bat->bid)
		temp_dup(bat->bid);
	bat->name = _strdup(obat->name);
	return LOG_OK;
}

static int 
dup_col(sql_trans *tr, sql_column *oc, sql_column *c )
{
	if (oc->data) {
		sql_bat *bat = c->data = ZNEW(sql_bat), *obat = oc->data;
		return dup_bat(tr, obat, bat);
	}
	return LOG_OK;
}

static int 
dup_idx(sql_trans *tr, sql_idx *i, sql_idx *ni )
{
	if (i->data) {
		sql_bat *bat = ni->data = ZNEW(sql_bat), *obat = i->data;
		return dup_bat(tr, obat, bat);
	}
	return LOG_OK;
}

static int
dup_del(sql_trans *tr, sql_table *ot, sql_table *t)
{
	sql_bat *bat = t->data = ZNEW(sql_bat), *obat = ot->data;

	bat->bid = obat->bid;
	if (bat->bid) 
		obat->bid = temp_copy(bat->bid, isTempTable(t));
	bat->name = _strdup(obat->name);
	(void)tr;
	return LOG_OK;
}

static int
log_destroy_bat(sql_trans *tr, sql_bat *b)
{
	log_bid bid;
	int ok = LOG_OK;

	(void)tr;
	if (!b)
		return ok;
	if (b->bid && b->name) {
		ok = log_bat_transient(restrict_logger, b->name);
		bid = logger_find_bat(restrict_logger, b->name);
		if (bid) 
			logger_del_bat(restrict_logger, bid);
	} 
	return ok;
}

/* For the single user case 
   the destroy functions should handle rollbacks of not commited changes
 */
static int
destroy_bat(sql_trans *tr, sql_bat *b, int rollback)
{
	int ok = LOG_OK;

	if ((tr && tr->parent == gtrans) || !b)
		return LOG_OK;
	/* also rollback non committed changes */
	if (rollback) {
		BAT *bd = temp_descriptor(b->bid);
		if (b->ubid) {
			BAT *u = temp_descriptor(b->ubid);
			void_replace_bat(bd, u, TRUE);
			bat_destroy(u);
		}
		/* rollback inserts */
		BATundo(bd);
		bat_destroy(bd);
	}
	if (b->name)
		_DELETE(b->name);
	if (b->bid) 
		temp_destroy(b->bid);
	if (b->ubid) 
		temp_destroy(b->ubid);
	if (b->cached)
		temp_destroy(b->cached->batCacheid);
	b->bid = b->ubid = 0;
	b->name = NULL;
	b->cached = NULL;
	_DELETE(b);
	return ok;
}

static int
destroy_col(sql_trans *tr, sql_column *c)
{
	int ok = destroy_bat(tr, c->data, (tr && tr->stime == c->base.wtime));
	c->data = NULL;
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_column *c)
{
	return log_destroy_bat(tr, c->data);
}

static int
destroy_idx(sql_trans *tr, sql_idx *i)
{
	int ok = destroy_bat(tr, i->data, (tr && tr->stime == i->base.wtime));
	i->data = NULL;
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_idx *i)
{
	return log_destroy_bat(tr, i->data);
}


static int
destroy_del(sql_trans *tr, sql_table *t)
{
	int ok = destroy_bat(tr, t->data, (tr && tr->stime == t->base.wtime));
	t->data = NULL;
	return ok;
}

static int
log_destroy_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	sql_bat *bat = t->data;

	(void)tr;
	if (bat->bid && bat->name) {
		log_bid bid;

		ok = log_bat_transient(restrict_logger, bat->name);
		bid = logger_find_bat(restrict_logger, bat->name);
		if (bid) 
			logger_del_bat(restrict_logger, bid);
	}
	return ok;
}

static BUN 
clear_bat(sql_trans *tr, sql_bat *bat)
{
	BUN sz = 0;

	(void)tr;
	if (bat->bid) {
		BAT *b = temp_descriptor(bat->bid);
		sz += BATcount(b);
		bat_clear(b);
		BATcommit(b);
		bat_destroy(b);
	}
	bat->cnt = 0;
	return sz;
}

static BUN 
clear_col(sql_trans *tr, sql_column *c)
{
	if (c->data)
		return clear_bat(tr, c->data);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	if (i->data)
		return clear_bat(tr, i->data);
	return 0;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	if (t->data && isTempTable(t))
		return clear_bat(tr, t->data);
	return 0;
}

static int 
tr_update_bat( sql_trans *tr, sql_bat *obat, sql_bat *cbat)
{
	int ok = LOG_OK;
	BAT *cb = temp_descriptor(cbat->bid);

	(void)tr;
	if (cbat->ubid) {
		temp_destroy(cbat->ubid);
		cbat->ubid = 0;
	}
	obat->cnt = cbat->cnt;
	BATcommit(cb);
	bat_destroy(cb);
	return ok;
}

static int
update_table(sql_trans *tr, sql_table *ft, sql_table *tt)
{
	int ok = LOG_OK;
	node *n, *m;

	if (ft->cleared) {
		(void)store_funcs.clear_del(tr->parent, tt);
		for (n = tt->columns.set->h; n; n = n->next) 
			(void)store_funcs.clear_col(tr->parent, n->data);
		if (tt->idxs.set) 
			for (n = tt->idxs.set->h; n; n = n->next) 
				(void)store_funcs.clear_idx(tr->parent, n->data);
	}
	tr_update_bat(tr, tt->data, ft->data);
	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
		sql_column *cc = n->data;
		sql_column *oc = m->data;

		if (!cc->base.wtime) 
			continue;
		tr_update_bat(tr, oc->data, cc->data);

		if (cc->base.rtime)
			oc->base.rtime = tr->stime;
		oc->base.wtime = tr->stime;
		cc->base.rtime = cc->base.wtime = 0;
	}
	if (ok == LOG_OK && tt->idxs.set) {
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;
			sql_bat *obat = oi->data;

			/* some indices have no bats */
			if (!obat)
				continue;

			if (!ci->base.wtime) 
				continue;
			tr_update_bat(tr, oi->data, ci->data);

			if (ci->base.rtime)
				oi->base.rtime = tr->stime;
			oi->base.wtime = tr->stime;
			ci->base.rtime = ci->base.wtime = 0;
		}
	}
	return ok;
}

static int 
tr_log_bat( sql_trans *tr, sql_bat *cbat, sql_bat *obat, int cleared)
{
	int ok = LOG_OK;
	BAT *cur = NULL;

	(void)tr;
	assert(tr->parent == gtrans);
	assert(store_nr_active == 1);

	if (cbat->name && cleared) 
		log_bat_clear(restrict_logger, cbat->name);

	cur = temp_descriptor(cbat->bid);
	/* handle bat swaps */
	if (cbat->bid != obat->bid) {
		logger_add_bat(restrict_logger, cur, obat->name);
		log_bat_persists(restrict_logger, cur, obat->name);
	}
	if (BUNlast(cur) > cur->batInserted) { 
		if (BATcount(cur) <= SNAPSHOT_MINSIZE) {
			ok = log_bat(restrict_logger, cur, cbat->name);
		} else {
			logger_add_bat(restrict_logger, cur, cbat->name);
			ok = log_bat_persists(restrict_logger, cur, cbat->name);
		}
	}

	bat_destroy(cur);
	return ok;
}

static int
log_table(sql_trans *tr, sql_table *ft, sql_table *tt)
{
	int ok = LOG_OK;
	node *n, *m;
	sql_bat *fdb = ft->data;
	BAT *db = NULL;

	assert(tr->parent == gtrans);
	assert (fdb->name);
	if (ft->cleared) 
		log_bat_clear(restrict_logger, fdb->name);

	db = temp_descriptor(fdb->bid);
	if (BUNlast(db) > db->batInserted || ft->cleared) 
		ok = log_bat(restrict_logger, db, fdb->name);
	bat_destroy(db);

	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
		sql_column *cc = n->data;
		sql_column *oc = m->data;

		if (!cc->base.wtime) 
			continue;
		tr_log_bat(tr, cc->data, oc->data, ft->cleared);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime)
				continue;

			tr_log_bat(tr, ci->data, oi->data, ft->cleared);
		}
	}
	return ok;
}

static int 
tr_snapshot_bat( sql_trans *tr, sql_bat *cbat, sql_bat *obat)
{
	BAT *cur;

	(void)tr;
	assert(tr->parent == gtrans);
	assert(store_nr_active == 1);

	/* handle bat swaps */
 	cur = temp_descriptor(cbat->bid);
	if (cbat->bid != obat->bid) {
		temp_destroy(obat->bid);
		obat->bid = cbat->bid = temp_create(cur);
		BATmode(cur, PERSISTENT);
	}
	if (BATcount(cur) > SNAPSHOT_MINSIZE) 
		BATmode(cur, PERSISTENT);
	if (BATcount(cur) > (BUN) REMAP_PAGE_MAXSIZE) 
       		BATmmap(cur, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
	bat_destroy(cur);
	return LOG_OK;
}

static int
snapshot_table(sql_trans *tr, sql_table *ft, sql_table *tt)
{
	int ok = LOG_OK;
	node *n, *m;

	assert(tr->parent == gtrans);

	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
		sql_column *cc = n->data;
		sql_column *oc = m->data;

		if (!cc->base.wtime) 
			continue;
		tr_snapshot_bat(tr, cc->data, oc->data);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime)
				continue;

			tr_snapshot_bat(tr, ci->data, oi->data);
		}
	}
	return ok;
}

int
su_storage_init( store_functions *sf)
{
	sf->bind_col = (bind_col_fptr)&bind_col;
	sf->bind_idx = (bind_idx_fptr)&bind_idx;
	sf->bind_del = (bind_del_fptr)&bind_del;

	sf->append_col = (append_col_fptr)&append_col;
	sf->append_idx = (append_idx_fptr)&append_idx;
	sf->update_col = (update_col_fptr)&update_col;
	sf->update_idx = (update_idx_fptr)&update_idx;
	sf->delete_tab = (delete_tab_fptr)&delete_tab;

	sf->count_col = (count_col_fptr)&count_col;
	sf->count_idx = (count_idx_fptr)&count_idx;
	sf->sorted_col = (sorted_col_fptr)&sorted_col;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->log_create_col = (create_col_fptr)&log_create_col;
	sf->log_create_idx = (create_idx_fptr)&log_create_idx;
	sf->log_create_del = (create_del_fptr)&log_create_del;

	sf->snapshot_create_col = (create_col_fptr)&snapshot_create_col;
	sf->snapshot_create_idx = (create_idx_fptr)&snapshot_create_idx;
	sf->snapshot_create_del = (create_del_fptr)&snapshot_create_del;

	sf->dup_col = (dup_col_fptr)&dup_col;
	sf->dup_idx = (dup_idx_fptr)&dup_idx;
	sf->dup_del = (dup_del_fptr)&dup_del;

	sf->destroy_col = (destroy_col_fptr)&destroy_col;
	sf->destroy_idx = (destroy_idx_fptr)&destroy_idx;
	sf->destroy_del = (destroy_del_fptr)&destroy_del;

	sf->log_destroy_col = (destroy_col_fptr)&log_destroy_col;
	sf->log_destroy_idx = (destroy_idx_fptr)&log_destroy_idx;
	sf->log_destroy_del = (destroy_del_fptr)&log_destroy_del;

	sf->clear_col = (clear_col_fptr)&clear_col;
	sf->clear_idx = (clear_idx_fptr)&clear_idx;
	sf->clear_del = (clear_del_fptr)&clear_del;

	sf->update_table = (update_table_fptr)&update_table;
	sf->log_table = (update_table_fptr)&log_table;
	sf->snapshot_table = (update_table_fptr)&snapshot_table;
	return LOG_OK;
}

int
ro_storage_init( store_functions *sf)
{
	sf->bind_col = (bind_col_fptr)&bind_col;
	sf->bind_idx = (bind_idx_fptr)&bind_idx;
	sf->bind_del = (bind_del_fptr)&bind_del;

	sf->append_col = (append_col_fptr)NULL;
	sf->append_idx = (append_idx_fptr)NULL;
	sf->update_col = (update_col_fptr)NULL;
	sf->update_idx = (update_idx_fptr)NULL;
	sf->delete_tab = (delete_tab_fptr)NULL;

	sf->count_col = (count_col_fptr)&count_col;
	sf->count_idx = (count_idx_fptr)&count_idx;
	sf->sorted_col = (sorted_col_fptr)&sorted_col;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->log_create_col = (create_col_fptr)NULL;
	sf->log_create_idx = (create_idx_fptr)NULL;
	sf->log_create_del = (create_del_fptr)NULL;

	sf->snapshot_create_col = (create_col_fptr)NULL;
	sf->snapshot_create_idx = (create_idx_fptr)NULL;
	sf->snapshot_create_del = (create_del_fptr)NULL;

	sf->dup_col = (dup_col_fptr)&dup_col;
	sf->dup_idx = (dup_idx_fptr)&dup_idx;
	sf->dup_del = (dup_del_fptr)&dup_del;

	sf->destroy_col = (destroy_col_fptr)&destroy_col;
	sf->destroy_idx = (destroy_idx_fptr)&destroy_idx;
	sf->destroy_del = (destroy_del_fptr)&destroy_del;

	sf->log_destroy_col = (destroy_col_fptr)NULL;
	sf->log_destroy_idx = (destroy_idx_fptr)NULL;
	sf->log_destroy_del = (destroy_del_fptr)NULL;

	sf->clear_col = (clear_col_fptr)&clear_col;
	sf->clear_idx = (clear_idx_fptr)&clear_idx;
	sf->clear_del = (clear_del_fptr)&clear_del;

	sf->update_table = (update_table_fptr)NULL;
	sf->log_table = (update_table_fptr)NULL;
	sf->snapshot_table = (update_table_fptr)NULL;
	sf->gtrans_update = (gtrans_update_fptr)NULL;
	return LOG_OK;
}

int
suro_storage_init( store_functions *sf)
{
	return ro_storage_init(sf);
}
