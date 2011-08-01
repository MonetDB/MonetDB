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
#include "bat_storage.h"
#include "bat_utils.h"
#include <sql_string.h>
#include <algebra.h>

#define SNAPSHOT_MINSIZE ((BUN) 1024)

BAT *
delta_bind_ubat(sql_delta *bat, int access)
{
	BAT *b;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access == RD_UPD);
	b = temp_descriptor(bat->ubid);
	assert(b);
	return b;
}

static BAT *
bind_ucol(sql_trans *tr, sql_column *c, int access)
{
	c->t->s->base.rtime = c->t->base.rtime = c->base.rtime = tr->stime;
	return delta_bind_ubat(c->data, access);
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_ubat(i->data, access);
}

BAT *
delta_bind_bat( sql_delta *bat, int access, int temp)
{
	BAT *b;

	assert(access == RDONLY || access == RD_INS);
	if (temp || access == RD_INS) {
		assert(bat->ibid);
		b = temp_descriptor(bat->ibid);
	} else if (!bat->bid) {
		int tt = 0;
		b = temp_descriptor(bat->ibid);
		tt = b->ttype;
		bat_destroy(b);
		b = e_BAT(tt);
	} else {
		b = temp_descriptor(bat->bid);
		bat_set_access(b, BAT_READ);
	}
	assert(b);
	return b;
}

static BAT *
bind_col(sql_trans *tr, sql_column *c, int access)
{
	if (access == RD_UPD)
		return bind_ucol(tr, c, access);
	if (tr)
		c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( c->data, access, isTemp(c));
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	if (access == RD_UPD)
		return bind_uidx(tr, i, access);
	if (tr)
		i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( i->data, access, isTemp(i));
}

BAT *
delta_bind_del(sql_dbat *bat, int access) 
{
	BAT *b;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access == RDONLY || access == RD_INS);
	assert(access!=RD_UPD);

	b = temp_descriptor(bat->dbid);
	assert(b);
	return b;
}


static BAT *
bind_del(sql_trans *tr, sql_table *t, int access)
{
	t->s->base.rtime = t->base.rtime = tr->stime;
	return delta_bind_del(t->data, access);
}

void
delta_update_bat( sql_delta *bat, BAT *upd, int is_new) 
{
	BAT *b;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (!is_new && bat->ubid) {
		BAT *ib = temp_descriptor(bat->ibid), *u = upd;

		if (BATcount(ib)) { 
			BAT *updins;

 			updins = BATsemijoin(upd, ib);
			void_replace_bat(ib, updins, TRUE);
			bat_destroy(updins);
#if 0
			BAT *b = temp_descriptor(bat->bid);
			u = BATsemijoin(upd, b);
#endif
		}
		bat_destroy(ib);

		b = temp_descriptor(bat->ubid);
		assert(b);
		if (isEUbat(b)){
			temp_destroy(bat->ubid);
			bat->ubid = temp_copy(b->batCacheid, FALSE);
			bat_destroy(b);
			b = temp_descriptor(bat->ubid);
		}
		BATkey(b, BOUND2BTRUE);
		BATins(b, u, TRUE);
		BATreplace(b, u, TRUE);
		if (upd != u)
			bat_destroy(u);
	} else if (is_new && bat->bid) { 
		BAT *ib = temp_descriptor(bat->ibid);
		b = temp_descriptor(bat->bid);
		if (BATcount(ib)) { 
			BAT *updins, *updcur;
 			updins = BATsemijoin(upd, ib);
			void_replace_bat(ib, updins, TRUE);
			bat_destroy(updins);
 			updcur = BATsemijoin(upd, b);
			void_replace_bat(b, updcur, TRUE);
			bat_destroy(updcur);
		} else {
			b = temp_descriptor(bat->bid);
			void_replace_bat(b, upd, TRUE);
		}
		bat_destroy(ib);
	} else {
		b = temp_descriptor(bat->ibid);
		void_replace_bat(b, upd, TRUE);
	}
	bat_destroy(b);
}

void
delta_update_val( sql_delta *bat, oid rid, void *upd) 
{
	BAT *b;

	assert(rid != oid_nil);

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (bat->ubid) {
		b = temp_descriptor(bat->ubid);
		assert(b);

		if (isEUbat(b)){
			temp_destroy(bat->ubid);
			bat->ubid = temp_copy(b->batCacheid, FALSE);
			bat_destroy(b);
			b = temp_descriptor(bat->ubid);
		}
		BATkey(b, BOUND2BTRUE);
		BUNins(b, (ptr) &rid, upd, TRUE);
	} else {
		b = temp_descriptor(bat->ibid);
		void_inplace(b, rid, upd, TRUE);
	}
	bat_destroy(b);
}

static void
update_col(sql_trans *tr, sql_column *c, void *i, int tpe, oid rid)
{
	sql_delta *bat = c->data;

	c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->stime;
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		delta_update_bat(bat, i, isNew(c));
	else 
		delta_update_val(bat, rid, i);
}

static void 
update_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	sql_delta *bat = i->data;

	i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->stime;
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		delta_update_bat(bat, ib, isNew(i));
	else
		assert(0);
}

void
delta_append_bat( sql_delta *bat, BAT *i ) 
{
	BAT *b = temp_descriptor(bat->ibid);

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	bat->cnt += BATcount(i);
	/* We simply use the to be inserted bat directly.
	 * Disabled this optimization: sometimes the bat is used later in the
	 * mal plan. 
	 * This should be solved by changing the input into a view (somehow)
	if (BATcount(b) == 0 && !isVIEW(i) && BBP_lrefs(i->batCacheid) <= 1 && i->htype == TYPE_void && i->ttype != TYPE_void && bat->ibase == i->H->seq){
		temp_destroy(bat->ibid);
		bat->ibid = temp_create(i);
		BATseqbase(i, bat->ibase);
	} else 
	 */
	if (!isEbat(b)){
		/* try to use mmap() */
		if (BATcount(b)+BATcount(i) > (BUN) REMAP_PAGE_MAXSIZE) { 
       			BATmmap(b, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 1);
    		}
		assert(b->T->heap.storage != STORE_PRIV);
		BATappend(b, i, TRUE);
	} else {
		temp_destroy(bat->ibid);
		bat->ibid = ebat2real(i->batCacheid, bat->ibase);
	}
	bat_destroy(b);
}

void
delta_append_val( sql_delta *bat, void *i ) 
{
	BAT *b = temp_descriptor(bat->ibid);

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (isEbat(b)) {
		bat_destroy(b);
		temp_destroy(bat->ibid);
		bat->ibid = ebat2real(bat->ibid, bat->ibase);
		b = temp_descriptor(bat->ibid);
	}
	BUNappend(b, i, TRUE);
	bat->cnt ++;
	bat_destroy(b);
}

static void 
append_col(sql_trans *tr, sql_column *c, void *i, int tpe)
{
	sql_delta *bat = c->data;

	/* appends only write */
	c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->stime;
	if (tpe == TYPE_bat)
		delta_append_bat(bat, i);
	else
		delta_append_val(bat, i);
}

static void
append_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	sql_delta *bat = i->data;

	/* appends only write */
	i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->stime;
	if (tpe == TYPE_bat)
		delta_append_bat(bat, ib);
	else
		delta_append_val(bat, ib);
}

void
delta_delete_bat( sql_dbat *bat, BAT *i ) 
{
	BAT *b = temp_descriptor(bat->dbid);

	bat->cnt += BATcount(i);
	if (BATcount(b) == 0 && !isVIEW(i) && i->htype == TYPE_void && i->ttype != TYPE_void){
		temp_destroy(bat->dbid);
		bat->dbid = temp_create(i);
	} else {
		if (isEbat(b)) {
			temp_destroy(bat->dbid);
			bat->dbid = temp_copy(b->batCacheid, FALSE);
			bat_destroy(b);
			b = temp_descriptor(bat->dbid);
		}
		assert(b->T->heap.storage != STORE_PRIV);
		BATappend(b, i, TRUE);
	}
	bat_destroy(b);
}

void
delta_delete_val( sql_dbat *bat, oid rid ) 
{
	BAT *b = temp_descriptor(bat->dbid);

	bat->cnt ++;
	if (isEbat(b)) {
		temp_destroy(bat->dbid);
		bat->dbid = temp_copy(b->batCacheid, FALSE);
		bat_destroy(b);
		b = temp_descriptor(bat->dbid);
	}
	BUNappend(b, (ptr)&rid, TRUE);
	bat_destroy(b);
}

static void
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	sql_dbat *bat = t->data;
	node *n;

	/* delete all cached copies */
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_delta *bat = c->data;

		if (bat->cached) {
			bat_destroy(bat->cached);
			bat->cached = NULL;
		}
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			sql_delta *bat = i->data;

			if (bat && bat->cached) {
				bat_destroy(bat->cached);
				bat->cached = NULL;
			}
		}
	}

	/* deletes only write */
	t->base.wtime = t->s->base.wtime = tr->wtime = tr->stime;
	if (tpe == TYPE_bat)
		delta_delete_bat(bat, ib);
	else
		delta_delete_val(bat, *(oid*)ib);
}

static size_t
count_col(sql_column *col)
{
	sql_delta *b = col->data;
	if (!b)
		return 1;
	return b->cnt;
}

static size_t
count_idx(sql_idx *idx)
{
	sql_delta *b = idx->data;
	if (!b)
		return 0;
	return b->cnt;
}


static sql_column *
find_col( sql_trans *tr, char *sname, char *tname, char *cname )
{
	sql_schema *s = find_sql_schema(tr, sname);
	sql_table *t = NULL;
	sql_column *c = NULL;

	if (s) 
		t = find_sql_table(s, tname);
	if (t) 
		c = find_sql_column(t, cname);
	return c;
}

static int
sorted_col(sql_trans *tr, sql_column *col)
{
	int sorted = 0;

	/* fallback to central bat */
	if (tr && tr->parent && !col->data) {
		col = find_col(tr->parent, 
			col->t->s->base.name, 
			col->t->base.name,
			col->base.name);
	}

	if (col && col->data) {
		BAT *b = bind_col(tr, col, RDONLY);

		sorted = BATtordered(b);
		bat_destroy(b);
	}
	return sorted;
}

int
load_delta(sql_delta *bat, int bid, int type)
{
	BAT *b;

	b = quick_descriptor(bid);
	if (!b)
		return LOG_ERR;
	bat->bid = temp_create(b);
	bat->ibase = BATcount(b);
	bat->cnt = bat->ibase; 
	bat->ubid = e_ubat(type);
	bat->ibid = e_bat(type);
	return LOG_OK;
}

static int 
load_bat(sql_delta *bat, int type) 
{
	int bid = logger_find_bat(bat_logger, bat->name);

	return load_delta(bat, bid, type);
}

int
log_create_delta(sql_delta *bat) 
{
	int ok = LOG_OK;
	BAT *b = (bat->bid)?
			temp_descriptor(bat->bid):
			temp_descriptor(bat->ibid);

	if (!bat->ubid) 
		bat->ubid = e_ubat(b->ttype);

	logger_add_bat(bat_logger, b, bat->name);
	if (ok == LOG_OK)
		ok = log_bat_persists(bat_logger, b, bat->name);
	bat_destroy(b);
	return ok;
}

static int
snapshot_new_persistent_bat(sql_trans *tr, sql_delta *bat) 
{
	int ok = LOG_OK;
	BAT *b = (bat->bid)?
			temp_descriptor(bat->bid):
			temp_descriptor(bat->ibid);

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

int
new_persistent_delta( sql_delta *bat, int sz )
{
	if (bat->bid) { /* result of alter ! */
		BAT *b = temp_descriptor(bat->bid);
		BAT *i = temp_descriptor(bat->ibid);

		bat->ibase = BATcount(b);
		bat->ibid = temp_copy(i->batCacheid, FALSE);
		bat_destroy(i);
		i = temp_descriptor(bat->ibid);
		bat_set_access(i, BAT_READ);
		BATseqbase(i, bat->ibase);
		bat_destroy(i);
	} else {
		BAT *i, *b = temp_descriptor(bat->ibid);
		int type = b->ttype;

		bat->bid = bat->ibid;
		bat->ibase = BATcount(b);
		bat_destroy(b);

		i = bat_new(TYPE_void, type, sz);
		bat_set_access(i, BAT_READ);
		BATseqbase(i, bat->ibase);
		bat->ibid = temp_create(i);
		bat_destroy(i);
	}
	return LOG_OK;
}

static int
new_persistent_bat(sql_trans *tr, sql_delta *bat, int sz) 
{
	(void)tr;
	return new_persistent_delta(bat, sz);
}

void
create_delta( sql_delta *d, BAT *b, BAT *i, bat u)
{
	d->cnt = BATcount(i);
	bat_set_access(i, BAT_READ);
	d->bid = 0;
	d->ibase = i->H->seq;
	d->ibid = temp_create(i);
	if (b) {
		d->cnt += BATcount(b);
		bat_set_access(b, BAT_READ);
		d->bid = temp_create(b);
	}
	d->ubid = u;
}

static bat
copyBat (bat i, int type, oid seq)
{
	BAT *b, *tb;
	bat res;

	if (!i)
		return i;
	tb = temp_descriptor(i);
	b = BATconst(tb, type, ATOMnilptr(type));
	bat_destroy(tb);
	if (isVIEW(b)) {
		tb = BATcopy(b, TYPE_void, b->ttype, TRUE);
		bat_destroy(b);
	} else {
		tb = b;
	}

	bat_set_access(tb, BAT_READ);
	BATseqbase(tb, seq);

	res = temp_create(tb);
	bat_destroy(tb);
	return res;
}

static int
create_col(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	int type = c->type.type->localtype;
	sql_delta *bat = c->data;

	if (!bat)
		c->data = bat = ZNEW(sql_delta);
	if (!bat->name) 
		bat->name = sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name);

	(void)tr;
	if (c->base.flag == TR_OLD && !isTempTable(c->t)){
		return load_bat(bat, type);
	} else if (bat && bat->ibid && !isTempTable(c->t)) {
		return new_persistent_bat(tr, c->data, c->t->sz);
	} else if (!bat->ibid) {
		sql_column *fc;
		size_t cnt = 0;

		/* alter ? */
		if (c->t->columns.set && (fc = c->t->columns.set->h->data) != NULL) {
			sql_delta *d = fc->data;
			cnt = d->cnt; 
		}
		if (cnt) {
			sql_delta *d = fc->data;

			bat->bid = copyBat(d->bid, type, 0);
			bat->ibid = copyBat(d->ibid, type, d->ibase);
			bat->ibase = d->ibase;
			bat->cnt = d->cnt;
			bat->ubid = e_ubat(type);
		} else {
			BAT *b = bat_new(TYPE_void, type, c->t->sz);
			if (!b) 
				return LOG_ERR;
			create_delta(c->data, NULL, b, 0);
			bat_destroy(b);
		}
	}
	return ok;
}

static int
log_create_col(sql_trans *tr, sql_column *c)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(c->t));
	return log_create_delta( c->data);
}


static int
snapshot_create_col(sql_trans *tr, sql_column *c)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(c->t));
	return snapshot_new_persistent_bat( tr, c->data);
}

/* will be called for new idx's and when new index columns are create */
static int
create_idx(sql_trans *tr, sql_idx *ni)
{
	int ok = LOG_OK;
	sql_delta *bat = ni->data;
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
		ni->data = bat = ZNEW(sql_delta);
	if (!bat->name) 
		bat->name = sql_message("%s_%s_%s", ni->t->s->base.name, ni->t->base.name, ni->base.name);

	/* create bats for a loaded idx structure */
	if (ni->base.flag == TR_OLD && !isTempTable(ni->t)){
		return load_bat(bat, type);
	} else if (bat->ibid) { /* create bats for a new persistent idx */
		return new_persistent_bat( tr, ni->data, ni->t->sz);
	} else if (!bat->ibid) {
		sql_column *c = ni->t->columns.set->h->data;
		sql_delta *d = c->data;

		/* Here we also handle indices created through alter stmts */
		/* These need to be created aligned to the existing data */

		bat->bid = copyBat(d->bid, type, 0);
		bat->ibid = copyBat(d->ibid, type, d->ibase);
		bat->ibase = d->ibase;
		bat->cnt = d->cnt;

		if (d->ubid) 
			bat->ubid = e_ubat(type);
	}
	return ok;
}

static int
log_create_idx(sql_trans *tr, sql_idx *ni)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return log_create_delta( ni->data);
}

static int
snapshot_create_idx(sql_trans *tr, sql_idx *ni)
{
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return snapshot_new_persistent_bat( tr, ni->data);
}

int
new_persistent_dbat( sql_dbat *bat)
{
	BAT *b = temp_descriptor(bat->dbid);

	bat->dbid = temp_create(b);
	bat_destroy(b);
	return LOG_OK;
}

int
load_dbat(sql_dbat *bat, int bid)
{
	BAT *b = quick_descriptor(bid);

	bat->dbid = temp_create(b);
	bat->cnt = BATcount(b); 
	return LOG_OK;
}


static int
create_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	BAT *b;
	sql_dbat *bat = t->data;

	if (!bat)
		bat = t->data = ZNEW(sql_dbat);
	if (!bat->dname)
		bat->dname = sql_message("D_%s_%s", t->s->base.name, t->base.name);
	(void)tr;
	if (t->base.flag == TR_OLD && !isTempTable(t)) {
		return load_dbat(bat, logger_find_bat(bat_logger, bat->dname));
	} else if (bat->dbid && !isTempTable(t)) {
		return new_persistent_dbat(bat);
	} else if (!bat->dbid) {
		b = bat_new(TYPE_void, TYPE_oid, t->sz);
		bat_set_access(b, BAT_READ);
		bat->dbid = temp_create(b);
		bat_destroy(b);
	}
	return ok;
}

int
log_create_dbat( sql_dbat *bat )
{
	BAT *b = temp_descriptor(bat->dbid);
	int ok = LOG_OK;

	(void) logger_add_bat(bat_logger, b, bat->dname);
	ok = log_bat_persists(bat_logger, b, bat->dname);
	bat_destroy(b);
	return ok;
}

static int
log_create_del(sql_trans *tr, sql_table *t)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(t));
	return log_create_dbat(t->data);
}

static int
snapshot_create_del(sql_trans *tr, sql_table *t)
{
	sql_dbat *bat = t->data;
	BAT *b = temp_descriptor(bat->dbid);

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

int
dup_delta(sql_trans *tr, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew, int temp, int sz)
{
	if (!obat)
		return LOG_OK;
	bat->ibid = obat->ibid;
	bat->bid = obat->bid;
	bat->ubid = obat->ubid;
	bat->ibase = obat->ibase;
	bat->cnt = obat->cnt;

	bat->name = _strdup(obat->name);

	assert(bat->ibid);
	if (bat->ibid) {
		BAT *b;
		if (temp) {
			bat->ibid = temp_copy(bat->ibid, 1);
		} else if (oc_isnew && !bat->bid) { 
			/* move the bat to the new col, fixup the old col*/
			b = bat_new(TYPE_void, type, sz);
			bat_set_access(b, BAT_READ);
			obat->ibid = temp_create(b);
			if (c_isnew && tr->parent == gtrans) { 
				/* new cols are moved to gtrans and bat.bid */
				temp_dup(bat->ibid);
				obat->bid = bat->ibid;
			} else if (!c_isnew) {  
				bat->bid = bat->ibid;

				b = bat_new(TYPE_void, type, sz);
				bat_set_access(b, BAT_READ);
				bat->ibid = temp_create(b);
			}
			if (obat->bid) {
				BAT *cb = temp_descriptor(obat->bid);
				bat->ibase = obat->ibase = BATcount(cb);
				BATseqbase(b, obat->ibase);
				bat_destroy(cb);
			}
			bat_destroy(b);
		} else { /* old column */
			bat->ibid = ebat_copy(bat->ibid, bat->ibase, 0); 
		}
	}
	if (!temp && bat->ibid) { 
		if (bat->ubid) {
			if (c_isnew && tr->parent == gtrans) { 
				obat->ubid = eubat_copy(bat->ubid, 0);
			} else {
				bat->ubid = eubat_copy(bat->ubid, 0); 
			}
		} else {
			bat->ubid = e_ubat(type);
			obat->ubid = e_ubat(type);
		}
	}
	if (bat->bid)
		temp_dup(bat->bid);
	return LOG_OK;
}

static int 
dup_bat(sql_trans *tr, sql_table *t, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew)
{
	return dup_delta( tr, obat, bat, type, oc_isnew, c_isnew, isTempTable(t), t->sz);
}

static int 
dup_col(sql_trans *tr, sql_column *oc, sql_column *c )
{
	if (oc->data) {
		int type = c->type.type->localtype;
		sql_delta *bat = c->data = ZNEW(sql_delta), *obat = oc->data;
		return dup_bat(tr, c->t, obat, bat, type, isNew(oc), c->base.flag == TR_NEW);
	}
	return LOG_OK;
}

static int 
dup_idx(sql_trans *tr, sql_idx *i, sql_idx *ni )
{
	if (i->data) {
		int type = (ni->type==join_idx)?TYPE_oid:TYPE_wrd;
		sql_delta *bat = ni->data = ZNEW(sql_delta), *obat = i->data;
		return dup_bat(tr, ni->t, obat, bat, type, isNew(i), ni->base.flag == TR_NEW);
	}
	return LOG_OK;
}

int
dup_dbat( sql_trans *tr, sql_dbat *obat, sql_dbat *bat, int is_new, int temp)
{
	bat->dbid = obat->dbid;
	bat->dname = _strdup(obat->dname);
	if (bat->dbid) {
		if (is_new) {
			obat->dbid = temp_copy(bat->dbid, temp);
		} else {
			bat->dbid = ebat_copy(bat->dbid, 0, temp);
		}
	}
	(void)tr;
	return LOG_OK;
}

static int
dup_del(sql_trans *tr, sql_table *ot, sql_table *t)
{
	sql_dbat *bat = t->data = ZNEW(sql_dbat), *obat = ot->data;
	return dup_dbat( tr, obat, bat, isNew(t), isTempTable(t));
}

int
log_destroy_delta(sql_delta *b)
{
	log_bid bid;
	int ok = LOG_OK;

	if (!b)
		return ok;
	if (b->bid && b->name) {
		ok = log_bat_transient(bat_logger, b->name);
		bid = logger_find_bat(bat_logger, b->name);
		if (bid) 
			logger_del_bat(bat_logger, bid);
	} 
	return ok;
}

static int
log_destroy_bat(sql_trans *tr, sql_delta *b)
{
	(void)tr;
	return log_destroy_delta(b);
}

int
destroy_delta(sql_delta *b)
{
	if (b->name)
		_DELETE(b->name);
	if (b->ibid)
		temp_destroy(b->ibid);
	if (b->ubid)
		temp_destroy(b->ubid);
	if (b->bid) 
		temp_destroy(b->bid);
	if (b->cached)
		temp_destroy(b->cached->batCacheid);
	b->bid = b->ibid = b->ubid = 0;
	b->name = NULL;
	b->cached = NULL;
	return LOG_OK;
}

static int
destroy_bat(sql_trans *tr, sql_delta *b)
{
	if ((tr && tr->parent == gtrans) || !b)
		return LOG_OK;
	destroy_delta(b);
	_DELETE(b);
	return LOG_OK;
}

static int
destroy_col(sql_trans *tr, sql_column *c)
{
	int ok = destroy_bat(tr, c->data);
	if (!tr || tr->parent != gtrans) 
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
	int ok = destroy_bat(tr, i->data);
	if (!tr || tr->parent != gtrans) 
		i->data = NULL;
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_idx *i)
{
	return log_destroy_bat(tr, i->data);
}

int
destroy_dbat(sql_dbat *bat)
{
	if (bat->dname)
		_DELETE(bat->dname);
	if (bat->dbid)
		temp_destroy(bat->dbid);
	bat->dbid = 0;
	bat->dname = NULL;
	return LOG_OK;
}

static int
destroy_del(sql_trans *tr, sql_table *t)
{
	int ok = 0;
	sql_dbat *bat = t->data;

	if ((tr && tr->parent == gtrans) || !bat)
		return LOG_OK;
	ok = destroy_dbat(bat);
	_DELETE(bat);
	t->data = NULL;
	return ok;
}

int 
log_destroy_dbat(sql_dbat *bat)
{
	int ok = LOG_OK;
	if (bat->dbid && bat->dname) {
		log_bid bid;

		ok = log_bat_transient(bat_logger, bat->dname);
		bid = logger_find_bat(bat_logger, bat->dname);
		if (bid) 
			logger_del_bat(bat_logger, bid);
	}
	return ok;
}

static int
log_destroy_del(sql_trans *tr, sql_table *t)
{
	(void)tr;
	return log_destroy_dbat(t->data);
}

BUN
clear_delta(sql_trans *tr, sql_delta *bat)
{
	BAT *b;
	BUN sz = 0;

	if (bat->ibid) {
		b = temp_descriptor(bat->ibid);
		sz += BATcount(b);
		bat_clear(b);
		BATcommit(b);
		bat_destroy(b);
	}
	if (bat->bid) {
		b = temp_descriptor(bat->bid);
		sz += BATcount(b);
		/* for transactions we simple switch to ibid only */
		if (tr != gtrans) {
			temp_destroy(bat->bid);
			bat->bid = 0;
		} else {
			bat_clear(b);
			BATcommit(b);
		}
		bat->ibase = 0;
		bat_destroy(b);
	}
	if (bat->ubid) { 
		b = temp_descriptor(bat->ubid);
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
		return clear_delta(tr, c->data);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	if (i->data)
		return clear_delta(tr, i->data);
	return 0;
}

BUN
clear_dbat(sql_trans *tr, sql_dbat *bat)
{
	BUN sz = 0;

	(void)tr;
	if (bat->dbid) {
		BAT *b = temp_descriptor(bat->dbid);

		sz += BATcount(b);
		bat_clear(b);
		BATcommit(b);
		bat_destroy(b);
	}
	bat->cnt = 0;
	return sz;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	return clear_dbat(tr, t->data);
}

static void
BATcleanProps( BAT *b )
{
	if (b->T->props) {
		PROPdestroy(b->T->props);
		b->T->props = NULL;
	}
}

static int 
gtr_update_delta( sql_trans *tr, sql_delta *cbat)
{
	int ok = LOG_OK;
	BAT *ups, *ins, *cur;

	(void)tr;
	assert(store_nr_active==0);

	cur = temp_descriptor(cbat->bid);
	ins = temp_descriptor(cbat->ibid);
	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins)) {
		if (BATcount(cur)+BATcount(ins) > (BUN) REMAP_PAGE_MAXSIZE) { /* try to use mmap() */
       			BATmmap(cur, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 1);
    		}
		assert(cur->T->heap.storage != STORE_PRIV);
		BATappend(cur,ins,TRUE);
		BATcleanProps(cur);
		temp_destroy(cbat->ibid);
		cbat->ibid = e_bat(cur->ttype);
	}
	bat_destroy(ins);

	ups = temp_descriptor(cbat->ubid);
	/* any updates */
	if (BUNlast(ups) > BUNfirst(ups)) {
		void_replace_bat(cur, ups, TRUE);
		temp_destroy(cbat->ubid);
		cbat->ubid = e_ubat(cur->ttype);
	}
	bat_destroy(ups);
	bat_destroy(cur);
	return ok;
}

static int
gtr_update_table(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	node *n;

	for (n = t->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *c = n->data;

		if (!c->base.wtime) 
			continue;
		ok = gtr_update_delta(tr, c->data);
	}
	if (ok == LOG_OK && t->idxs.set) {
		for (n = t->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats */
			if (!ci->base.wtime)
				continue;

			ok = gtr_update_delta(tr, ci->data);
		}
	}
	return ok;
}

typedef int (*gtr_update_table_fptr)( sql_trans *tr, sql_table *t);

static int
_gtr_update( sql_trans *tr, gtr_update_table_fptr gtr_update_table_f )
{
	int ok = LOG_OK;
	node *sn;

	for(sn = tr->schemas.set->h; sn && ok == LOG_OK; sn = sn->next) {
		sql_schema *s = sn->data;
		
		if (!isTempSchema(s) && s->tables.set) {
			node *n;
			for (n = s->tables.set->h; n && ok == LOG_OK; n = n->next) {
				sql_table *t = n->data;

				if (isTable(t) && isGlobal(t))
					ok = gtr_update_table_f(tr, t);
			}
		}
	}
	return LOG_OK;
}

static int
gtr_update( sql_trans *tr )
{
	return _gtr_update(tr, &gtr_update_table);
}

static int 
gtr_minmax_col( sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	sql_delta *cbat = c->data;
	BAT *cur;
	lng val;

	(void)tr;
	if (store_nr_active > 0)
		return LOG_ERR;

	/* allready set */
	if (!cbat || c->type.type->localtype >= TYPE_str || c->t->system)
		return ok;

	cur = temp_descriptor(cbat->bid);
	if (BATgetprop(cur, GDK_MIN_VALUE)) {
		bat_destroy(cur);
		return ok;
	}

	BATmin(cur, &val);
	BATsetprop(cur, GDK_MIN_VALUE, cur->ttype, &val);
	BATmax(cur, &val);
	BATsetprop(cur, GDK_MAX_VALUE, cur->ttype, &val);
	bat_destroy(cur);
	return ok;
}

static int
gtr_minmax_table(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	node *n;

	if (t->readonly) {
		for (n = t->columns.set->h; ok == LOG_OK && n; n = n->next) {
			sql_column *c = n->data;
	
			ok = gtr_minmax_col(tr, c);
		}
	}
	return ok;
}

static int
gtr_minmax( sql_trans *tr )
{
	return _gtr_update(tr, &gtr_minmax_table);
}

int 
tr_update_delta( sql_trans *tr, sql_delta *obat, sql_delta *cbat, BUN snapshot_minsize)
{
	int ok = LOG_OK;
	BAT *ups, *ins, *cur;
	int cleared = 0;

	(void)tr;
	assert(store_nr_active>0);

	/* for cleared tables the bid is reset */
	if (cbat->bid == 0) {
		cleared = 1;
		cbat->bid = obat->bid;
		temp_dup(cbat->bid);
	}

	cur = temp_descriptor(obat->bid);
	ins = temp_descriptor(cbat->ibid);
	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins) || cleared) {
		if (BUNlast(ins) > ins->batInserted && (store_nr_active > 1)) { 
			BAT *ci = temp_descriptor(obat->ibid);

			if (isEbat(ci)) {
				temp_destroy(obat->ibid);
				obat->ibid = temp_copy(ci->batCacheid, FALSE);
				bat_destroy(ci);
				ci = temp_descriptor(obat->ibid);
			}
			assert(obat->ibase == cbat->ibase);
			append_inserted(ci, ins);
			BATcleanProps(ci);
			bat_destroy(ci);
		}
		obat->cnt = cbat->cnt;
		if (store_nr_active == 1) { /* flush all */
			BAT *pi = temp_descriptor(obat->ibid);
			if (!BATcount(cur) && BATcount(ins) > snapshot_minsize){
				/* swap cur and ins */
				BAT *newcur = ins;

				temp_destroy(obat->bid);
				temp_destroy(cbat->bid);

				obat->bid = cbat->bid = temp_create(newcur);
				cbat->ibid = e_bat(cur->ttype);
				ins = temp_descriptor(cbat->ibid);
				bat_destroy(cur);
				cur = newcur;
			} else {
				if (BATcount(cur)+BATcount(ins) > (BUN) REMAP_PAGE_MAXSIZE) { /* try to use mmap() */
       					BATmmap(cur, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 1);
    				}
				assert(cur->T->heap.storage != STORE_PRIV);
				BATappend(cur,ins,TRUE);
				BATcleanProps(cur);
				temp_destroy(cbat->ibid);
				cbat->ibid = e_bat(cur->ttype);
			}
			bat_clear(pi);
			obat->ibase = cbat->ibase = BATcount(cur);
			BATseqbase(pi, obat->ibase);
			bat_destroy(pi);
		} else {
			BATcommit(ins);
		}
	}
	bat_destroy(ins);

	ups = temp_descriptor(cbat->ubid);
	/* any updates */
	if (BUNlast(ups) > BUNfirst(ups) || cleared ) {
		if ((BUNlast(ups) > ups->batInserted || BATdirty(ups)) && (store_nr_active > 1)) { 
			BAT *cu = temp_descriptor(obat->ubid);

			if (isEUbat(cu)) {
				temp_destroy(obat->ubid);
				obat->ubid = temp_copy(cu->batCacheid, FALSE);
				bat_destroy(cu);
				cu = temp_descriptor(obat->ubid);
			}
			BATkey(cu, BOUND2BTRUE);
			/* should be insert_inserted */
			BATins(cu, ups, TRUE);
			BATreplace(cu, ups, TRUE);
			BATcleanProps(cu);
			bat_destroy(cu);
		}
		if (store_nr_active == 1) { /* flush all */
			void_replace_bat(cur, ups, TRUE);
			/* cleanup the old deltas */
			temp_destroy(obat->ubid);
			obat->ubid = e_ubat(cur->ttype);
			temp_destroy(cbat->ubid);
			cbat->ubid = e_ubat(cur->ttype);
		} else {
			BATcommit(ups);
		}
	}
	bat_destroy(ups);
	bat_destroy(cur);
	return ok;
}

int
tr_update_dbat(sql_trans *tr, sql_dbat *tdb, sql_dbat *fdb, int cleared)
{
	int ok = LOG_OK;
	BAT *db = NULL;

	(void)tr;
	db = temp_descriptor(fdb->dbid);
	if (BUNlast(db) > db->batInserted || cleared) {
		BAT *odb = temp_descriptor(tdb->dbid);

		if (isEbat(odb)) {
			temp_destroy(tdb->dbid);
			tdb->dbid = temp_copy(odb->batCacheid, FALSE);
			bat_destroy(odb);
			odb = temp_descriptor(tdb->dbid);
		}
		append_inserted(odb, db);
		bat_destroy(odb);
		BATcommit(db);
		tdb->cnt = fdb->cnt;
	}
	bat_destroy(db);
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

	tr_update_dbat(tr, tt->data, ft->data, ft->cleared);
	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
		sql_column *cc = n->data;
		sql_column *oc = m->data;

		if (!cc->base.wtime) 
			continue;
		tr_update_delta(tr, oc->data, cc->data, SNAPSHOT_MINSIZE);

		if (cc->base.rtime)
			oc->base.rtime = tr->stime;
		oc->base.wtime = tr->stime;
		cc->base.rtime = cc->base.wtime = 0;
	}
	if (ok == LOG_OK && tt->idxs.set) {
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;

			/* some indices have no bats */
			if (!oi->data || !ci->base.wtime)
				continue;

			tr_update_delta(tr, oi->data, ci->data, SNAPSHOT_MINSIZE);

			if (ci->base.rtime)
				oi->base.rtime = tr->stime;
			oi->base.wtime = tr->stime;
			ci->base.rtime = ci->base.wtime = 0;
		}
	}
	return ok;
}

int 
tr_log_delta( sql_trans *tr, sql_delta *cbat, int cleared)
{
	int ok = LOG_OK;
	BAT *ups, *ins, *cur = NULL;

	(void)tr;
	assert(tr->parent == gtrans);
	if (cbat->name && cleared) 
		log_bat_clear(bat_logger, cbat->name);

	ins = temp_descriptor(cbat->ibid);
	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins)) {
		assert(store_nr_active>0);
		if (cbat->bid)
			cur = temp_descriptor(cbat->bid);
		if (BUNlast(ins) > ins->batInserted && (store_nr_active != 1 || (cur && BATcount(cur)) || BATcount(ins) <= SNAPSHOT_MINSIZE))
			ok = log_bat(bat_logger, ins, cbat->name);
		if (store_nr_active == 1 && 
		    ((!cur || !BATcount(cur)) && BATcount(ins) > SNAPSHOT_MINSIZE)) {
			/* log new snapshot */
			logger_add_bat(bat_logger, ins, cbat->name);
			ok = log_bat_persists(bat_logger, ins, cbat->name);
		}
		if (cur)
			bat_destroy(cur);
	}
	bat_destroy(ins);

	ups = temp_descriptor(cbat->ubid);
	/* any updates */
	if (ok == LOG_OK && (BUNlast(ups) > ups->batInserted || BATdirty(ups))) 
		ok = log_delta(bat_logger, ups, cbat->name);
	bat_destroy(ups);
	return ok;
}

int
tr_log_dbat(sql_trans *tr, sql_dbat *fdb, int cleared)
{
	int ok = LOG_OK;
	BAT *db = NULL;

	(void)tr;
	assert (fdb->dname);
	if (cleared) 
		log_bat_clear(bat_logger, fdb->dname);

	db = temp_descriptor(fdb->dbid);
	if (BUNlast(db) > db->batInserted || cleared) 
		ok = log_bat(bat_logger, db, fdb->dname);
	bat_destroy(db);
	return ok;
}

static int
log_table(sql_trans *tr, sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	assert(tr->parent == gtrans);
	ok = tr_log_dbat(tr, ft->data, ft->cleared);
	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if (!cc->base.wtime) 
			continue;
		ok = tr_log_delta(tr, cc->data, ft->cleared);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime)
				continue;

			ok = tr_log_delta(tr, ci->data, ft->cleared);
		}
	}
	return ok;
}

static int 
tr_snapshot_bat( sql_trans *tr, sql_delta *cbat)
{
	int ok = LOG_OK;

	assert(tr->parent == gtrans);
	assert(store_nr_active>0);

	(void)tr;
	if (store_nr_active == 1) { 
		BAT *cur = NULL, *ins = temp_descriptor(cbat->ibid);

		/* any inserts */
		if (BUNlast(ins) > BUNfirst(ins)) {
			if (cbat->bid)
				cur = temp_descriptor(cbat->bid);
		        if ((!cur || !BATcount(cur)) && BATcount(ins) > SNAPSHOT_MINSIZE) {
				bat_set_access(ins, BAT_READ);
				BATmode(ins, PERSISTENT);
				if (BATcount(ins) > (BUN) REMAP_PAGE_MAXSIZE)
       					BATmmap(ins, STORE_MMAP, STORE_MMAP, STORE_MMAP, STORE_MMAP, 0);
			}
			if (cur)
				bat_destroy(cur);
		}
		bat_destroy(ins);
	}
	return ok;
}

static int
snapshot_table(sql_trans *tr, sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	assert(tr->parent == gtrans);

	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if (!cc->base.wtime) 
			continue;
		tr_snapshot_bat(tr, cc->data);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime)
				continue;

			tr_snapshot_bat(tr, ci->data);
		}
	}
	return ok;
}

int
bat_storage_init( store_functions *sf)
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
	sf->gtrans_update = (gtrans_update_fptr)&gtr_update;
	sf->gtrans_minmax = (gtrans_update_fptr)&gtr_minmax;
	return LOG_OK;
}

