/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_storage.h"
#include "bat_utils.h"
#include <sql_string.h>
#include <algebra.h>
#include <gdk_atoms.h>

#define SNAPSHOT_MINSIZE ((BUN) 1024*128)

static sql_trans *
oldest_active_transaction(void)
{
	sql_session *s = active_sessions->h->data;
	return s->tr;
}

sql_delta *
timestamp_delta( sql_delta *d, int ts)
{
	while (d->next && d->wtime > ts) 
		d = d->next;
	if (0 && d && d->cached) {
		bat_destroy(d->cached);
		d->cached = NULL;
	}
	return d;
}

sql_dbat *
timestamp_dbat( sql_dbat *d, int ts)
{
	while (d->next && d->wtime > ts) 
		d = d->next;
	if (0 && d && d->cached) {
		bat_destroy(d->cached);
		d->cached = NULL;
	}
	return d;
}


static BAT *
delta_bind_del(sql_dbat *bat, int access) 
{
	BAT *b;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access == RDONLY || access == RD_INS);
	assert(access != RD_UPD_ID && access != RD_UPD_VAL);

	b = temp_descriptor(bat->dbid);
	assert(b);
	return b;
}

static BAT *
bind_del(sql_trans *tr, sql_table *t, int access)
{
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, tr->stime);
	}
	t->s->base.rtime = t->base.rtime = tr->stime;
	return delta_bind_del(t->data, access);
}

static BAT *
delta_bind_ubat(sql_delta *bat, int access, int type)
{
	BAT *b;

#ifdef NDEBUG
	(void) access; /* satisfy compiler */
#endif
	assert(access == RD_UPD_ID || access == RD_UPD_VAL);
	if (bat->uibid && bat->uvbid) {
		if (access == RD_UPD_ID)
			b = temp_descriptor(bat->uibid);
		else
			b = temp_descriptor(bat->uvbid);
	} else {
		if (access == RD_UPD_ID)
			b = temp_descriptor(e_bat(TYPE_oid));
		else
			b = temp_descriptor(e_bat(type));
	}
	assert(b);
	return b;
}

static BAT *
bind_ucol(sql_trans *tr, sql_column *c, int access)
{
	BAT *u = NULL;

	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->stime);
	}
	if (!c->t->data) {
		sql_table *ot = tr_find_table(tr->parent, c->t);
		c->t->data = timestamp_dbat(ot->data, tr->stime);
	}
	c->t->s->base.rtime = c->t->base.rtime = c->base.rtime = tr->stime;
	u = delta_bind_ubat(c->data, access, c->type.type->localtype);
	return u;
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	BAT *u = NULL;

	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, tr->stime);
	}
	if (!i->t->data) {
		sql_table *ot = tr_find_table(tr->parent, i->t);
		i->t->data = timestamp_dbat(ot->data, tr->stime);
	}
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	u = delta_bind_ubat(i->data, access, (oid_index(i->type))?TYPE_oid:TYPE_wrd);
	return u;
}

static BAT *
delta_bind_bat( sql_delta *bat, int access, int temp)
{
	BAT *b;

	assert(access == RDONLY || access == RD_INS || access == QUICK);
	assert(bat != NULL);
	if (access == QUICK)
		return quick_descriptor(bat->bid);
	if (temp || access == RD_INS) {
		assert(bat->ibid);
		b = temp_descriptor(bat->ibid);
		if (BATcount(b) && bat->uibid && bat->uvbid) {
			/* apply updates to the inserted */
			BAT *ui = temp_descriptor(bat->uibid), *uv = temp_descriptor(bat->uvbid), *nui = ui, *nuv = uv, *o;
			
			if (BATcount(nui)) {
				o = BATthetaselect(ui, NULL, &b->hseqbase, ">=");
				nui = BATproject(o, ui);
				bat_destroy(ui);
				nuv = BATproject(o, uv);
				bat_destroy(uv);
				bat_destroy(o);
				void_replace_bat(b, nui, nuv, TRUE);
			}
			bat_destroy(nui);
			bat_destroy(nuv);
		}
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
	if (!isTable(c->t)) 
		return NULL;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->stime);
	}
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_ucol(tr, c, access);
	if (tr)
		c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( c->data, access, isTemp(c));
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	if (!isTable(i->t)) 
		return NULL;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, tr->stime);
	}
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_uidx(tr, i, access);
	if (tr)
		i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( i->data, access, isTemp(i));
}

static int
delta_update_bat( sql_delta *bat, BAT *tids, BAT *updates, int is_new) 
{
	BAT *b, *ui = NULL, *uv = NULL;

	if (!BATcount(tids))
		return LOG_OK;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (!is_new && bat->uibid && bat->uvbid) {
		BAT *ib = temp_descriptor(bat->ibid), *otids = tids;

		if (BATcount(ib)) { 
			BAT *nui = tids, *nuv = updates, *o;

			o = BATthetaselect(tids, NULL, &ib->hseqbase, ">=");
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			assert(BATcount(nui) == BATcount(nuv));
			bat_destroy(o);
			void_replace_bat(ib, nui, nuv, TRUE);
			bat_destroy(nui);
			bat_destroy(nuv);

			o = BATthetaselect(tids, NULL, &ib->hseqbase, "<");
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			assert(BATcount(nui) == BATcount(nuv));
			bat_destroy(o);

			tids = nui;
			updates = nuv;
		}
		bat_destroy(ib);

		ui = temp_descriptor(bat->uibid);
		uv = temp_descriptor(bat->uvbid);
		assert(ui && uv);
		if (isEbat(ui)){
			temp_destroy(bat->uibid);
			bat->uibid = temp_copy(ui->batCacheid, FALSE);
			if (bat->uibid == BID_NIL) 
				return LOG_ERR;
			bat_destroy(ui);
			ui = temp_descriptor(bat->uibid);
		}
		if (isEbat(uv)){
			temp_destroy(bat->uvbid);
			bat->uvbid = temp_copy(uv->batCacheid, FALSE);
			if (bat->uvbid == BID_NIL) 
				return LOG_ERR;
			bat_destroy(uv);
			uv = temp_descriptor(bat->uvbid);
		}
		BATappend(ui, tids, TRUE);
		BATappend(uv, updates, TRUE);
		assert(BATcount(tids) == BATcount(updates));
		bat_destroy(ui);
		bat_destroy(uv);
		if (tids != otids) {
			bat_destroy(tids);
			bat_destroy(updates);
			tids = otids;
		}
	} else if (is_new && bat->bid) { 
		BAT *ib = temp_descriptor(bat->ibid);
		b = temp_descriptor(bat->bid);

		if (BATcount(ib)) { 
			BAT *nui = tids, *nuv = updates, *o;

			o = BATthetaselect(tids, NULL, &ib->hseqbase, ">=");
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			assert(BATcount(nui) == BATcount(nuv));
			bat_destroy(o);
			void_replace_bat(ib, nui, nuv, TRUE);
			bat_destroy(nui);
			bat_destroy(nuv);

			o = BATthetaselect(tids, NULL, &ib->hseqbase, "<");
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			assert(BATcount(nui) == BATcount(nuv));
			bat_destroy(o);
			void_replace_bat(b, nui, nuv, TRUE);
			bat_destroy(nui);
			bat_destroy(nuv);
		} else {
			b = temp_descriptor(bat->bid);
			void_replace_bat(b, tids, updates, TRUE);
		}
		bat_destroy(ib);
		bat_destroy(b);
	} else {
		b = temp_descriptor(bat->ibid);
		void_replace_bat(b, tids, updates, TRUE);
		bat_destroy(b);
	}
	bat->ucnt += BATcount(tids);
	return LOG_OK;
}

static int
delta_update_val( sql_delta *bat, oid rid, void *upd) 
{
	BAT *b = NULL;

	assert(rid != oid_nil);

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (bat->uibid && bat->uvbid) {
		BAT *ib = temp_descriptor(bat->ibid);

		if (BATcount(ib) && ib->hseqbase <= rid) { 
			void_inplace(ib, rid, upd, TRUE);
		} else {
			BAT *ui = temp_descriptor(bat->uibid);
			BAT *uv = temp_descriptor(bat->uvbid);

			if (isEbat(ui)){
				temp_destroy(bat->uibid);
				bat->uibid = temp_copy(ui->batCacheid, FALSE);
				if (bat->uibid == BID_NIL) 
					return LOG_ERR;
				bat_destroy(ui);
				ui = temp_descriptor(bat->uibid);
			}
			if (isEbat(uv)){
				temp_destroy(bat->uvbid);
				bat->uvbid = temp_copy(uv->batCacheid, FALSE);
				if (bat->uvbid == BID_NIL) 
					return LOG_ERR;
				bat_destroy(uv);
				uv = temp_descriptor(bat->uvbid);
			}
			BUNappend(ui, (ptr) &rid, TRUE);
			BUNappend(uv, (ptr) upd, TRUE);
			bat->ucnt++;
			bat_destroy(ui);
			bat_destroy(uv);
		}
		bat_destroy(ib);
	} else {
		b = temp_descriptor(bat->ibid);
		void_inplace(b, rid, upd, TRUE);
		bat_destroy(b);
	}
	return LOG_OK;
}

static int
dup_delta(sql_trans *tr, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew, int temp, int sz)
{
	if (!obat)
		return LOG_OK;
	bat->ibid = obat->ibid;
	bat->bid = obat->bid;
	bat->uibid = obat->uibid;
	bat->uvbid = obat->uvbid;
	bat->ibase = obat->ibase;
	bat->cnt = obat->cnt;
	bat->ucnt = obat->ucnt;
	bat->wtime = obat->wtime;

	bat->name = _STRDUP(obat->name);

	if (!bat->ibid)
		return LOG_OK;
	if (bat->ibid) {
		BAT *b;
		if (temp) {
			bat->ibid = temp_copy(bat->ibid, 1);
			if (bat->ibid == BID_NIL) 
				return LOG_ERR;
		} else if (oc_isnew && !bat->bid) { 
			/* move the bat to the new col, fixup the old col*/
			b = bat_new(TYPE_void, type, sz, PERSISTENT);
			bat_set_access(b, BAT_READ);
			obat->ibid = temp_create(b);
			obat->ibase = bat->ibase = (oid) obat->cnt;
			BATseqbase(b, obat->ibase);
			bat_destroy(b);
			if (c_isnew && tr->parent == gtrans) { 
				/* new cols are moved to gtrans and bat.bid */
				temp_dup(bat->ibid);
				obat->bid = bat->ibid;
			} else if (!c_isnew) {  
				bat->bid = bat->ibid;

				b = bat_new(TYPE_void, type, sz, PERSISTENT);
				bat_set_access(b, BAT_READ);
				BATseqbase(b, bat->ibase);
				bat->ibid = temp_create(b);
			}
		} else { /* old column */
			bat->ibid = ebat_copy(bat->ibid, bat->ibase, 0); 
			if (bat->ibid == BID_NIL) 
				return LOG_ERR;
		}
	}
	if (!temp && bat->ibid) { 
		if (bat->uibid && bat->uvbid) {
			if (c_isnew && tr->parent == gtrans) { 
				obat->uibid = ebat_copy(bat->uibid, 0, 0);
				obat->uvbid = ebat_copy(bat->uvbid, 0, 0);
			} else {
				bat->uibid = ebat_copy(bat->uibid, 0, 0); 
				bat->uvbid = ebat_copy(bat->uvbid, 0, 0); 
			}
			if (bat->uibid == BID_NIL || bat->uvbid == BID_NIL) 
				return LOG_ERR;
		} else {
			bat->uibid = e_bat(TYPE_oid);
			obat->uvbid = e_bat(type);
		}
	}
	if (bat->bid)
		temp_dup(bat->bid);
	return LOG_OK;
}

int 
dup_bat(sql_trans *tr, sql_table *t, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew)
{
	return dup_delta( tr, obat, bat, type, oc_isnew, c_isnew, isTempTable(t), t->sz);
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, int tpe)
{
	BAT *b = tids;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b)) 
		return LOG_OK;

	if (!c->data || !c->base.allocated) {
		int type = c->type.type->localtype;
		sql_column *oc = tr_find_column(tr->parent, c);
		sql_delta *bat = c->data = ZNEW(sql_delta), *obat = timestamp_delta(oc->data, tr->stime);
		(void)dup_bat(tr, c->t, obat, bat, type, isNew(oc), c->base.flag == TR_NEW); 
		c->base.allocated = 1;
	}
       	bat = c->data;
	bat->wtime = c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->wstime;
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		return delta_update_bat(bat, tids, upd, isNew(c));
	else 
		return delta_update_val(bat, *(oid*)tids, upd);
}

static int 
update_idx(sql_trans *tr, sql_idx * i, void *tids, void *upd, int tpe)
{
	BAT *b = tids;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b)) 
		return LOG_OK;

	if (!i->data || !i->base.allocated) {
		int type = (oid_index(i->type))?TYPE_oid:TYPE_wrd;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta *bat = i->data = ZNEW(sql_delta), *obat = timestamp_delta(oi->data, tr->stime);
		(void)dup_bat(tr, i->t, obat, bat, type, isNew(i), i->base.flag == TR_NEW); 
		i->base.allocated = 1;
	}
       	bat = i->data;
	bat->wtime = i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->wstime;
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	if (tpe == TYPE_bat)
		return delta_update_bat(bat, tids, upd, isNew(i));
	else
		assert(0);
	return LOG_OK;
}

static int
delta_append_bat( sql_delta *bat, BAT *i ) 
{
	int id = i->batCacheid;
	BAT *b;
#ifndef NDEBUG
	BAT *c = BBPquickdesc(bat->bid, 0); 
	assert(!c || c->htype == TYPE_void);
#endif

	if (!BATcount(i))
		return LOG_OK;
	b = temp_descriptor(bat->ibid);
	assert(b->htype == TYPE_void);

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	assert(!c || BATcount(c) == bat->ibase);
	if (BATcount(b) == 0 && BBP_refs(id) == 1 && BBP_lrefs(id) == 1 && !isVIEW(i) && i->ttype && i->S->role == PERSISTENT){
		temp_destroy(bat->ibid);
		bat->ibid = id;
		temp_dup(id);
		bat_destroy(b);
		i->hseqbase = bat->ibase;
	} else {
		if (!isEbat(b)){
			assert(b->T->heap.storage != STORE_PRIV);
		} else {
			temp_destroy(bat->ibid);
			bat->ibid = ebat2real(b->batCacheid, bat->ibase);
			bat_destroy(b);
			b = temp_descriptor(bat->ibid);
		}
		if (isVIEW(i) && b->batCacheid == abs(VIEWtparent(i))) {
			BAT *ic = COLcopy(i, i->ttype, TRUE, TRANSIENT);
			BATappend(b, ic, TRUE);
			bat_destroy(ic);
		} else 
			BATappend(b, i, TRUE);
		assert(BUNlast(b) > b->batInserted);
		bat_destroy(b);
	}
	bat->cnt += BATcount(i);
	return LOG_OK;
}

static int
delta_append_val( sql_delta *bat, void *i ) 
{
	BAT *b = temp_descriptor(bat->ibid);
#ifndef NDEBUG
	BAT *c = BBPquickdesc(bat->bid, 0);
	assert(!c || c->htype == TYPE_void);
#endif

	assert(b->htype == TYPE_void);
	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	assert(!c || BATcount(c) == bat->ibase);
	if (isEbat(b)) {
		bat_destroy(b);
		temp_destroy(bat->ibid);
		bat->ibid = ebat2real(bat->ibid, bat->ibase);
		b = temp_descriptor(bat->ibid);
	}
	BUNappend(b, i, TRUE);
	assert(BUNlast(b) > b->batInserted);
	bat->cnt ++;
	bat_destroy(b);
	return LOG_OK;
}

static int 
dup_col(sql_trans *tr, sql_column *oc, sql_column *c )
{
	int ok = LOG_OK;

	if (oc->data) {
		int type = c->type.type->localtype;
		sql_delta *bat = c->data = ZNEW(sql_delta), *obat = oc->data;
		ok = dup_bat(tr, c->t, obat, bat, type, isNew(oc), c->base.flag == TR_NEW);
		c->base.allocated = 1;
	}
	return ok;
}

static int 
dup_idx(sql_trans *tr, sql_idx *i, sql_idx *ni )
{
	int ok = LOG_OK;

	if (i->data) {
		int type = (oid_index(ni->type))?TYPE_oid:TYPE_wrd;
		sql_delta *bat = ni->data = ZNEW(sql_delta), *obat = i->data;
		ok = dup_bat(tr, ni->t, obat, bat, type, isNew(i), ni->base.flag == TR_NEW);
		ni->base.allocated = 1;
	}
	return ok;
}

static int
dup_dbat( sql_trans *tr, sql_dbat *obat, sql_dbat *bat, int is_new, int temp)
{
	bat->dbid = obat->dbid;
	bat->cnt = obat->cnt;
	bat->dname = _STRDUP(obat->dname);
	bat->wtime = obat->wtime;
	if (bat->dbid) {
		if (is_new) {
			obat->dbid = temp_copy(bat->dbid, temp);
		} else {
			bat->dbid = ebat_copy(bat->dbid, 0, temp);
		}
		if (bat->dbid == BID_NIL) 
			return LOG_ERR;
	}
	(void)tr;
	return LOG_OK;
}

static int
dup_del(sql_trans *tr, sql_table *ot, sql_table *t)
{
	sql_dbat *bat = t->data = ZNEW(sql_dbat), *obat = ot->data;
	int ok = dup_dbat( tr, obat, bat, isNew(t), isTempTable(t));
	assert(t->base.allocated == 0);
	t->base.allocated = 1;
	return ok;
}

static int 
append_col(sql_trans *tr, sql_column *c, void *i, int tpe)
{
	int ok = LOG_OK;
	BAT *b = i;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b)) 
		return ok;

	if (!c->data || !c->base.allocated) {
		int type = c->type.type->localtype;
		sql_column *oc = tr_find_column(tr->parent, c);
		sql_delta *bat = c->data = ZNEW(sql_delta), *obat = timestamp_delta(oc->data, tr->stime);
		(void)dup_bat(tr, c->t, obat, bat, type, isNew(oc), c->base.flag == TR_NEW); 
		c->base.allocated = 1;
	}
       	bat = c->data;
	/* appends only write */
	bat->wtime = c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->wstime;
	/* inserts are ordered with the current delta implementation */
	/* therefor mark appends as reads */
	c->t->s->base.rtime = c->t->base.rtime = tr->stime;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(bat, i);
	else
		ok = delta_append_val(bat, i);
	if (!c->t->data || !c->t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, c->t);
		sql_dbat *bat = c->t->data = ZNEW(sql_dbat), *obat = timestamp_dbat(ot->data, tr->stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(c->t)); 
		c->t->base.allocated = 1;
	}
	if (c->t && c->t->data && ((sql_dbat*)c->t->data)->cached) {
		sql_dbat *bat = c->t->data;

		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	return ok;
}

static int
append_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	BAT *b = ib;
	sql_delta *bat;
	int ok = LOG_OK;

	if (tpe == TYPE_bat && !BATcount(b)) 
		return ok;

	if (!i->data || !i->base.allocated) {
		int type = (oid_index(i->type))?TYPE_oid:TYPE_wrd;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta *bat = i->data = ZNEW(sql_delta), *obat = timestamp_delta(oi->data, tr->stime);
		(void)dup_bat(tr, i->t, obat, bat, type, isNew(i), i->base.flag == TR_NEW); 
		i->base.allocated = 1;
	}
       	bat = i->data;
	/* appends only write */
	bat->wtime = i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->wstime;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(bat, ib);
	else
		ok = delta_append_val(bat, ib);
	if (!i->t->data || !i->t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, i->t);
		sql_dbat *bat = i->t->data = ZNEW(sql_dbat), *obat = timestamp_dbat(ot->data, tr->stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(i->t)); 
		i->t->base.allocated = 1;
	}
	if (i->t && i->t->data && ((sql_dbat*)i->t->data)->cached) {
		sql_dbat *bat = i->t->data;

		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	return ok;
}

static int
delta_delete_bat( sql_dbat *bat, BAT *i ) 
{
	BAT *b = temp_descriptor(bat->dbid);

	if (isEbat(b)) {
		temp_destroy(bat->dbid);
		bat->dbid = temp_copy(b->batCacheid, FALSE);
		if (bat->dbid == BID_NIL) 
			return LOG_ERR;
		bat_destroy(b);
		b = temp_descriptor(bat->dbid);
	}
	assert(b->T->heap.storage != STORE_PRIV);
	BATappend(b, i, TRUE);
	bat_destroy(b);

	bat->cnt += BATcount(i);
	return LOG_OK;
}

static int
delta_delete_val( sql_dbat *bat, oid rid ) 
{
	BAT *b = temp_descriptor(bat->dbid);

	if (isEbat(b)) {
		temp_destroy(bat->dbid);
		bat->dbid = temp_copy(b->batCacheid, FALSE);
		if (bat->dbid == BID_NIL) 
			return LOG_ERR;
		bat_destroy(b);
		b = temp_descriptor(bat->dbid);
	}
	assert(b->T->heap.storage != STORE_PRIV);
	BUNappend(b, (ptr)&rid, TRUE);
	bat_destroy(b);

	bat->cnt ++;
	return LOG_OK;
}

static int
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	BAT *b = ib;
	sql_dbat *bat;
	node *n;
	int ok = LOG_OK;

	if (tpe == TYPE_bat && !BATcount(b)) 
		return ok;

	if (!t->data || !t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, t);
		sql_dbat *bat = t->data = ZNEW(sql_dbat), *obat = timestamp_dbat(ot->data, tr->stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(t)); 
		t->base.allocated = 1;
	}
       	bat = t->data;
	/* delete all cached copies */

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_delta *bat;

		if (!c->data) {
			sql_column *oc = tr_find_column(tr->parent, c);
			c->data = timestamp_delta(oc->data, tr->stime);
		}
       		bat = c->data;
		if (bat->cached) {
			bat_destroy(bat->cached);
			bat->cached = NULL;
		}
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			sql_delta *bat;

			if (!i->data) {
				sql_idx *oi = tr_find_idx(tr->parent, i);
				i->data = timestamp_delta(oi->data, tr->stime);
			}
       			bat = i->data;
			if (bat && bat->cached) {
				bat_destroy(bat->cached);
				bat->cached = NULL;
			}
		}
	}

	/* deletes only write */
	bat->wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (tpe == TYPE_bat)
		ok = delta_delete_bat(bat, ib);
	else
		ok = delta_delete_val(bat, *(oid*)ib);
	return ok;
}

static size_t
count_col(sql_trans *tr, sql_column *c, int all)
{
	sql_delta *b;

	if (!isTable(c->t)) 
		return 0;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->stime);
	}
        b = c->data;
	if (!b)
		return 1;
	if (all) 
		return b->cnt;
	else
		return b->cnt - b->ibase;
}

static size_t
dcount_col(sql_trans *tr, sql_column *c)
{
	sql_delta *b;

	if (!isTable(c->t)) 
		return 0;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->stime);
	}
        b = c->data;
	if (!b)
		return 1;
	if (b->cnt > 1024) {
		size_t dcnt = 0;
		dbl f = 1.0;
		BAT *v = delta_bind_bat(b, RDONLY, 0), *o = v, *u;

		if ((dcnt = (size_t) BATcount(v)) > 1024*1024) {
			v = BATsample(v, 1024);
			f = dcnt/1024.0;
		}
		u = BATunique(v, NULL);
		bat_destroy(o);
		if (v!=o)
			bat_destroy(v);
		dcnt = (size_t) (BATcount(u) * f);
		bat_destroy(u);
		return dcnt;
	} else {
		return 64;
	}
}

static size_t
count_idx(sql_trans *tr, sql_idx *i, int all)
{
	sql_delta *b;

	if (!isTable(i->t)) 
		return 0;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, tr->stime);
	}
	b = i->data;
	if (!b)
		return 0;
	if (all) 
		return b->cnt;
	else
		return b->cnt - b->ibase;
}

static size_t
count_del(sql_trans *tr, sql_table *t)
{
	sql_dbat *d;

	if (!isTable(t)) 
		return 0;
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, tr->stime);
	}
       	d = t->data;
	if (!d)
		return 0;
	return d->cnt;
}

static int
sorted_col(sql_trans *tr, sql_column *col)
{
	int sorted = 0;

	if (!isTable(col->t) || !col->t->s)
		return 0;
	/* fallback to central bat */
	if (tr && tr->parent && !col->data && col->po) 
		col = col->po; 

	if (col && col->data) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b)
			sorted = BATtordered(b);
	}
	return sorted;
}

static int
double_elim_col(sql_trans *tr, sql_column *col)
{
	int de = 0;

	if (!isTable(col->t) || !col->t->s)
		return 0;
	/* fallback to central bat */
	if (tr && tr->parent && !col->data && col->po) 
		col = col->po;

	if (col && col->data) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b && b->tvarsized) /* check double elimination */
			de = GDK_ELIMDOUBLES(b->T->vheap);
		if (de)
			de = b->T->width;
	}
	return de;
}

static int
load_delta(sql_delta *bat, int bid, int type)
{
	BAT *b;

	b = quick_descriptor(bid);
	if (!b)
		return LOG_ERR;
	bat->bid = temp_create(b);
	bat->ibase = BATcount(b);
	bat->cnt = bat->ibase; 
	bat->ucnt = 0; 
	bat->uibid = e_bat(TYPE_oid);
	bat->uvbid = e_bat(type);
	bat->ibid = e_bat(type);
	return LOG_OK;
}

static int 
load_bat(sql_delta *bat, int type) 
{
	int bid = logger_find_bat(bat_logger, bat->name);

	return load_delta(bat, bid, type);
}

static int
log_create_delta(sql_delta *bat) 
{
	int ok = LOG_OK;
	BAT *b = (bat->bid)?
			temp_descriptor(bat->bid):
			temp_descriptor(bat->ibid);

	if (!bat->uibid) 
		bat->uibid = e_bat(TYPE_oid);
	if (!bat->uvbid) 
		bat->uvbid = e_bat(b->ttype);

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
	bat_destroy(b);
	return ok;
}

static int
new_persistent_delta( sql_delta *bat, int sz )
{
	if (bat->bid) { /* result of alter ! */
		BAT *b = temp_descriptor(bat->bid);
		BAT *i = temp_descriptor(bat->ibid);

		bat->ibase = BATcount(b);
		bat->cnt = BATcount(b) + BATcount(i);
		bat->ucnt = 0;
		bat->ibid = temp_copy(i->batCacheid, FALSE);
		if (bat->ibid == BID_NIL) 
			return LOG_ERR;
		bat_destroy(i);
		i = temp_descriptor(bat->ibid);
		bat_set_access(i, BAT_READ);
		BATseqbase(i, bat->ibase);
		bat_destroy(i);
	} else {
		BAT *i, *b = temp_descriptor(bat->ibid);
		int type = b->ttype;

		bat->bid = bat->ibid;
		bat->cnt = bat->ibase = BATcount(b);
		bat->ucnt = 0;
		bat_destroy(b);

		i = bat_new(TYPE_void, type, sz, PERSISTENT);
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

static void
create_delta( sql_delta *d, BAT *b, BAT *i)
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
	d->uibid = d->uvbid = 0;
	d->ucnt = 0;
}

static bat
copyBat (bat i, int type, oid seq)
{
	BAT *b, *tb;
	bat res;

	if (!i)
		return i;
	tb = temp_descriptor(i);
	b = BATconstant(tb->hseqbase, type, ATOMnilptr(type), BATcount(tb), PERSISTENT);
	bat_destroy(tb);
	if (isVIEW(b)) {
		tb = COLcopy(b, b->ttype, TRUE, PERSISTENT);
		BATseqbase(b, 0); 
		b->H->dense = 1;
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

	if (!bat) {
		c->data = bat = ZNEW(sql_delta);
		bat->wtime = c->base.wtime = tr->wstime;
		c->base.allocated = 1;
	}
	if (!bat->name) 
		bat->name = sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name);

	if (c->base.flag == TR_OLD && !isTempTable(c->t)){
		c->base.wtime = 0;
		return load_bat(bat, type);
	} else if (bat && bat->ibid && !isTempTable(c->t)) {
		return new_persistent_bat(tr, c->data, c->t->sz);
	} else if (!bat->ibid) {
		sql_column *fc = NULL;
		size_t cnt = 0;

		/* alter ? */
		if (c->t->columns.set && (fc = c->t->columns.set->h->data) != NULL) 
			cnt = count_col(tr, fc, 1);
		if (cnt && fc != c) {
			sql_delta *d = fc->data;

			bat->bid = copyBat(d->bid, type, 0);
			if (d->ibid)
				bat->ibid = copyBat(d->ibid, type, d->ibase);
			bat->ibase = d->ibase;
			bat->cnt = d->cnt;
			if (d->uibid)
				bat->uibid = e_bat(TYPE_oid);
			if (d->uvbid)
				bat->uvbid = e_bat(type);
		} else {
			BAT *b = bat_new(TYPE_void, type, c->t->sz, PERSISTENT);
			if (!b) 
				return LOG_ERR;
			create_delta(c->data, NULL, b);
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

/* will be called for new idx's and when new index columns are created */
static int
create_idx(sql_trans *tr, sql_idx *ni)
{
	int ok = LOG_OK;
	sql_delta *bat = ni->data;
	int type = TYPE_wrd;

	if (oid_index(ni->type))
		type = TYPE_oid;

	if (!bat) {
		ni->data = bat = ZNEW(sql_delta);
		bat->wtime = ni->base.wtime = tr->wstime;
		ni->base.allocated = 1;
	}
	if (!bat->name) 
		bat->name = sql_message("%s_%s@%s", ni->t->s->base.name, ni->t->base.name, ni->base.name);

	if (ni->base.flag == TR_OLD && !isTempTable(ni->t)){
		ni->base.wtime = 0;
		return load_bat(bat, type);
	} else if (bat && bat->ibid && !isTempTable(ni->t)) {
		return new_persistent_bat( tr, ni->data, ni->t->sz);
	} else if (!bat->ibid) {
		sql_column *c = ni->t->columns.set->h->data;
		sql_delta *d;
	       
		if (!c->data) {
			sql_column *oc = tr_find_column(tr->parent, c);
			c->data = timestamp_delta(oc->data, tr->stime);
		}
		d = c->data;
		/* Here we also handle indices created through alter stmts */
		/* These need to be created aligned to the existing data */

		bat->bid = copyBat(d->bid, type, 0);
		bat->ibid = copyBat(d->ibid, type, d->ibase);
		bat->ibase = d->ibase;
		bat->cnt = d->cnt;
		bat->ucnt = 0;

		if (d->uibid) 
			bat->uibid = e_bat(TYPE_oid);
		if (d->uvbid) 
			bat->uvbid = e_bat(type);
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

static int
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

	if (!bat) {
		t->data = bat = ZNEW(sql_dbat);
		bat->wtime = t->base.wtime = t->s->base.wtime = tr->wstime;
		t->base.allocated = 1;
	}
	if (!bat->dname)
		bat->dname = sql_message("D_%s_%s", t->s->base.name, t->base.name);
	(void)tr;
	if (t->base.flag == TR_OLD && !isTempTable(t)) {
		log_bid bid = logger_find_bat(bat_logger, bat->dname);

		if (bid) {
			t->base.wtime = 0;
			return load_dbat(bat, bid);
		}
		ok = LOG_ERR;
	} else if (bat->dbid && !isTempTable(t)) {
		return ok;
	} else if (!bat->dbid) {
		b = bat_new(TYPE_void, TYPE_oid, t->sz, PERSISTENT);
		bat_set_access(b, BAT_READ);
		bat->dbid = temp_create(b);
		bat_destroy(b);
	}
	return ok;
}

static int
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
	bat_destroy(b);
	return LOG_OK;
}

static int
log_destroy_delta(sql_trans *tr, sql_delta *b)
{
	log_bid bid;
	int ok = LOG_OK;

	(void)tr;
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
destroy_delta(sql_delta *b)
{
	if (b->name)
		_DELETE(b->name);
	if (b->ibid)
		temp_destroy(b->ibid);
	if (b->uibid)
		temp_destroy(b->uibid);
	if (b->uvbid)
		temp_destroy(b->uvbid);
	if (b->bid) 
		temp_destroy(b->bid);
	if (b->cached)
		bat_destroy(b->cached);
	b->bid = b->ibid = b->uibid = b->uvbid = 0;
	b->name = NULL;
	b->cached = NULL;
	return LOG_OK;
}

static int
destroy_bat(sql_trans *tr, sql_delta *b)
{
	sql_delta *n;

	(void)tr;
	if (!b)
		return LOG_OK;
	n = b->next;
	destroy_delta(b);
	_DELETE(b);
	if (n)
		return destroy_bat(tr, n);
	return LOG_OK;
}

static int
destroy_col(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
       
	if (c->data && c->base.allocated) {
		c->base.allocated = 0;
		ok = destroy_bat(tr, c->data);
	}
	c->data = NULL;
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_column *c)
{
	if (c->data && c->base.allocated) 
		return log_destroy_delta(tr, c->data);
	return LOG_OK;
}

static int
destroy_idx(sql_trans *tr, sql_idx *i)
{
	int ok = LOG_OK;

	if (i->data && i->base.allocated) {
		i->base.allocated = 0;
       		ok = destroy_bat(tr, i->data);
	}
	i->data = NULL;
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_idx *i)
{
	if (i->data && i->base.allocated) 
		return log_destroy_delta(tr, i->data);
	return LOG_OK;
}

static int
destroy_dbat(sql_trans *tr, sql_dbat *bat)
{
	sql_dbat *n;
       
	if (!bat)
		return LOG_OK;
	n = bat->next;
	if (bat->dname)
		_DELETE(bat->dname);
	if (bat->dbid)
		temp_destroy(bat->dbid);
	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	bat->dbid = 0;
	bat->dname = NULL;
	_DELETE(bat);
	if (n)
		return destroy_dbat(tr, n);
	return LOG_OK;
}

static int
destroy_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;

	if (t->data && t->base.allocated) {
		t->base.allocated = 0;
		ok = destroy_dbat(tr, t->data);
	}
	t->data = NULL;
	return ok;
}

static int 
log_destroy_dbat(sql_trans *tr, sql_dbat *bat)
{
	int ok = LOG_OK;

	(void)tr;
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
	if (t->data && t->base.allocated) 
		return log_destroy_dbat(tr, t->data);
	return LOG_OK;
}

static BUN
clear_delta(sql_trans *tr, sql_delta *bat)
{
	BAT *b;
	BUN sz = 0;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
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
	if (bat->uibid) { 
		b = temp_descriptor(bat->uibid);
		bat_clear(b);
		BATcommit(b);
		bat_destroy(b);
	}
	if (bat->uvbid) { 
		b = temp_descriptor(bat->uvbid);
		bat_clear(b);
		BATcommit(b);
		bat_destroy(b);
	}
	bat->cnt = 0;
	bat->ucnt = 0;
	bat->wtime = tr->wstime;
	return sz;
}

static BUN 
clear_col(sql_trans *tr, sql_column *c)
{
	if (!c->data || !c->base.allocated) {
		int type = c->type.type->localtype;
		sql_column *oc = tr_find_column(tr->parent, c);
		sql_delta *bat = c->data = ZNEW(sql_delta), *obat = timestamp_delta(oc->data, tr->stime);
		assert(tr != gtrans);
		(void)dup_bat(tr, c->t, obat, bat, type, isNew(oc), c->base.flag == TR_NEW); 
		c->base.allocated = 1;
	}
	if (c->data)
		return clear_delta(tr, c->data);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	if (!i->data || !i->base.allocated) {
		int type = (oid_index(i->type))?TYPE_oid:TYPE_wrd;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta *bat = i->data = ZNEW(sql_delta), *obat = timestamp_delta(oi->data, tr->stime);
		(void)dup_bat(tr, i->t, obat, bat, type, isNew(i), i->base.flag == TR_NEW); 
		i->base.allocated = 1;
	}
	if (i->data)
		return clear_delta(tr, i->data);
	return 0;
}

static void 
empty_col(sql_column *c)
{
	int type = c->type.type->localtype;
	sql_delta *bat = c->data;

	assert(c->data && c->base.allocated && bat->bid == 0);
	bat->bid = bat->ibid;
	bat->ibid = e_bat(type);
	bat->ibase = 0;
	bat->cnt = BATcount(BBPquickdesc(bat->bid, 0));
	bat->ucnt = 0;

	if (bat->bid == bat->ibid)
		bat->bid = copyBat(bat->ibid, type, 0);

	/* make new bat persistent */
	{
		BAT *b = temp_descriptor(bat->bid);

		assert(b->batRole == PERSISTENT);
		if (b->batRole != PERSISTENT) {
			bat->bid = copyBat(b->batCacheid, type, 0);
			temp_destroy(b->batCacheid);
			bat_destroy(b);
			b = temp_descriptor(bat->bid);
		}
		bat_set_access(b, BAT_READ);
		BATmode(b, PERSISTENT);
		logger_add_bat(bat_logger, b, bat->name);
		bat_destroy(b);

	}
}

static void 
empty_idx(sql_idx *i)
{
	int type = (oid_index(i->type))?TYPE_oid:TYPE_wrd;
	sql_delta *bat = i->data;

	assert(i->data && i->base.allocated && bat->bid == 0);
	bat->bid = bat->ibid;
	bat->ibid = e_bat(type);
	bat->ibase = 0;
	bat->cnt = BATcount(BBPquickdesc(bat->bid, 0));
	bat->ucnt = 0;

	if (bat->bid == bat->ibid) 
		bat->bid = copyBat(bat->ibid, type, 0);

	/* make new bat persistent */
	{
		BAT *b = temp_descriptor(bat->bid);

		if (b->batRole != PERSISTENT) {
			bat->bid = copyBat(b->batCacheid, type, 0);
			temp_destroy(b->batCacheid);
			bat_destroy(b);
			b = temp_descriptor(bat->bid);
		}
		bat_set_access(b, BAT_READ);
		BATmode(b, PERSISTENT);
		logger_add_bat(bat_logger, b, bat->name);
		bat_destroy(b);
	}
}

static BUN
clear_dbat(sql_trans *tr, sql_dbat *bat)
{
	BUN sz = 0;

	(void)tr;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (bat->dbid) {
		BAT *b = temp_descriptor(bat->dbid);

		sz += BATcount(b);
		bat_clear(b);
		BATcommit(b);
		bat_destroy(b);
	}
	bat->cnt = 0;
	bat->wtime = tr->wstime;
	return sz;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	if (!t->data || !t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, t);
		sql_dbat *bat = t->data = ZNEW(sql_dbat), *obat = timestamp_dbat(ot->data, tr->stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(t)); 
		t->base.allocated = 1;
	}
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
gtr_update_delta( sql_trans *tr, sql_delta *cbat, int *changes)
{
	int ok = LOG_OK;
	BAT *ins, *cur;

	(void)tr;
	assert(store_nr_active==0);
	
	cur = temp_descriptor(cbat->bid);
	ins = temp_descriptor(cbat->ibid);
	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins)) {
		(*changes)++;
		assert(cur->T->heap.storage != STORE_PRIV);
		BATappend(cur,ins,TRUE);
		cbat->cnt = cbat->ibase = BATcount(cur);
		BATcleanProps(cur);
		temp_destroy(cbat->ibid);
		cbat->ibid = e_bat(cur->ttype);
	}
	bat_destroy(ins);

	if (cbat->ucnt) {
		BAT *ui = temp_descriptor(cbat->uibid);
		BAT *uv = temp_descriptor(cbat->uvbid);
		/* any updates */
		if (BUNlast(ui) > BUNfirst(ui)) {
			(*changes)++;
			void_replace_bat(cur, ui, uv, TRUE);
			temp_destroy(cbat->uibid);
			temp_destroy(cbat->uvbid);
			cbat->uibid = e_bat(TYPE_oid);
			cbat->uvbid = e_bat(cur->ttype);
			cbat->ucnt = 0;
		}
		bat_destroy(ui);
		bat_destroy(uv);
	}
	bat_destroy(cur);
	if (cbat->next) { 
		ok = destroy_bat(tr, cbat->next);
		cbat->next = NULL;
	}
	return ok;
}

static int
gtr_update_dbat(sql_dbat *d, int *changes)
{
	int ok = LOG_OK;
	BAT *idb;
	int dbid = logger_find_bat(bat_logger, d->dname);

	assert(store_nr_active==0);
	if (d->dbid == dbid)
		return ok;
	idb = temp_descriptor(d->dbid);
	if (BUNlast(idb) > idb->batInserted) {
		BAT *cdb = temp_descriptor(dbid);

		(*changes)++;
		append_inserted(cdb, idb);
		bat_destroy(cdb);
	}
	bat_destroy(idb);
	return ok;
}


static int
gtr_update_table(sql_trans *tr, sql_table *t, int *tchanges)
{
	int ok = LOG_OK;
	node *n;

	if (t->base.wtime <= t->base.allocated)
		return ok;
	gtr_update_dbat(t->data, tchanges);
	for (n = t->columns.set->h; ok == LOG_OK && n; n = n->next) {
		int changes = 0;
		sql_column *c = n->data;

		if (!c->base.wtime || c->base.wtime <= c->base.allocated) 
			continue;
		ok = gtr_update_delta(tr, c->data, &changes);
		if (changes)
			c->base.allocated = c->base.wtime = tr->wstime;
		(*tchanges) |= changes;
	}
	if (ok == LOG_OK && t->idxs.set) {
		for (n = t->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			int changes = 0;
			sql_idx *ci = n->data;

			/* some indices have no bats */
			if (!ci->base.wtime || ci->base.wtime <= ci->base.allocated) 
				continue;

			ok = gtr_update_delta(tr, ci->data, &changes);
			if (changes)
				ci->base.allocated = ci->base.wtime = tr->wstime;
			(*tchanges) |= changes;
		}
	}
	if (*tchanges)
		t->base.allocated = t->base.wtime = tr->wstime;
	return ok;
}

typedef int (*gtr_update_table_fptr)( sql_trans *tr, sql_table *t, int *changes);

static int
_gtr_update( sql_trans *tr, gtr_update_table_fptr gtr_update_table_f)
{
	int ok = LOG_OK, tchanges = 0;
	node *sn;

	for(sn = tr->schemas.set->h; sn && ok == LOG_OK; sn = sn->next) {
		int schanges = 0;
		sql_schema *s = sn->data;
		
		if (s->base.wtime <= s->base.allocated && 
			gtr_update_table_f == gtr_update_table)
			continue;
		if (!s->base.wtime)
			continue;
		if (!isTempSchema(s) && s->tables.set) {
			node *n;
			for (n = s->tables.set->h; n && ok == LOG_OK; n = n->next) {
				int changes = 0;
				sql_table *t = n->data;

				if (isTable(t) && isGlobal(t))
					ok = gtr_update_table_f(tr, t, &changes);
				schanges |= changes;
			}
		}
		if (schanges && gtr_update_table_f == gtr_update_table){
			s->base.allocated = s->base.wtime = tr->wstime;
			tchanges ++;
		}
	}
	if (tchanges && gtr_update_table_f == gtr_update_table)
		tr->wtime = tr->wstime;
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
	/* already set */
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
gtr_minmax_table(sql_trans *tr, sql_table *t, int *changes)
{
	int ok = LOG_OK;
	node *n;

	(void)changes;
	if (t->access > TABLE_WRITABLE) {
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

static int 
tr_update_delta( sql_trans *tr, sql_delta *obat, sql_delta *cbat, int unique)
{
	int ok = LOG_OK;
	BAT *ins, *cur = NULL;
	int cleared = 0;

	(void)tr;
	assert(store_nr_active==1);
	assert (obat->bid != 0 || tr != gtrans);

	/* for cleared tables the bid is reset */
	if (cbat->bid == 0) {
		cleared = 1;
		cbat->bid = obat->bid;
		temp_dup(cbat->bid);
	}

	if (obat->cached) {
		bat_destroy(obat->cached);
		obat->cached = NULL;
	}
	if (cbat->cached) {
		bat_destroy(cbat->cached);
		cbat->cached = NULL;
	}
	if (obat->bid)
		cur = temp_descriptor(obat->bid);
	if (!obat->bid && tr != gtrans) {
		*obat = *cbat;
		cbat->bid = 0;
		cbat->ibid = 0;
		cbat->uibid = 0;
		cbat->uvbid = 0;
		cbat->name = NULL;
		cbat->cached = NULL;
		return ok;
	}
	ins = temp_descriptor(cbat->ibid);
	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins) || cleared) {
		if ((!obat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE)){
			/* swap cur and ins */
			BAT *newcur = ins;

			if (unique)
				BATkey(BATmirror(newcur), TRUE);
			temp_destroy(cbat->bid);
			temp_destroy(obat->bid);
			obat->bid = cbat->ibid;
			cbat->bid = cbat->ibid = 0;

			BATmsync(ins);
			ins = cur;
			cur = newcur;
		} else {
			assert(cur->T->heap.storage != STORE_PRIV);
			assert((BATcount(cur) + BATcount(ins)) == cbat->cnt);
			//assert((BATcount(cur) + BATcount(ins)) == (obat->cnt + (BUNlast(ins) - ins->batInserted)));
			assert(!BATcount(ins) || !isEbat(ins));
			BATappend(cur,ins,TRUE);
			BATcleanProps(cur);
			temp_destroy(cbat->bid);
			temp_destroy(cbat->ibid);
			cbat->bid = cbat->ibid = 0;
			if (cur->batPersistence == PERSISTENT)
				BATmsync(cur);
		}
		obat->cnt = cbat->cnt = obat->ibase = cbat->ibase = BATcount(cur);
		temp_destroy(obat->ibid);
		obat->ibid = e_bat(cur->ttype);
	}
	if (obat->cnt != cbat->cnt) { /* locked */
		obat->cnt = cbat->cnt;
		obat->ibase = cbat->ibase;
	}
	bat_destroy(ins);

	if (cbat->ucnt || cleared) {
		BAT *ui = temp_descriptor(cbat->uibid);
		BAT *uv = temp_descriptor(cbat->uvbid);

		/* any updates */
		if (BUNlast(ui) > BUNfirst(ui)) {
			void_replace_bat(cur, ui, uv, TRUE);
			/* cleanup the old deltas */
			temp_destroy(obat->uibid);
			temp_destroy(obat->uvbid);
			obat->uibid = e_bat(TYPE_oid);
			obat->uvbid = e_bat(cur->ttype);
			temp_destroy(cbat->uibid);
			temp_destroy(cbat->uvbid);
			cbat->uibid = cbat->uvbid = 0;
			cbat->ucnt = obat->ucnt = 0;
		}
		bat_destroy(ui);
		bat_destroy(uv);
	}
	bat_destroy(cur);
	if (obat->next) { 
		ok = destroy_bat(tr, obat->next);
		obat->next = NULL;
	}
	return ok;
}

static int 
tr_merge_delta( sql_trans *tr, sql_delta *obat, int unique)
{
	int ok = LOG_OK;
	BAT *ins, *cur = NULL;
	int cleared = 0;

	(void)tr;
	assert(store_nr_active==1);
	assert (obat->bid != 0 || tr != gtrans);

	if (obat->cached) {
		bat_destroy(obat->cached);
		obat->cached = NULL;
	}
	if (obat->bid)
		cur = temp_descriptor(obat->bid);
	ins = temp_descriptor(obat->ibid);
	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins) || cleared) {
		if ((!obat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE)){
			/* swap cur and ins */
			BAT *newcur = ins;
			bat id = obat->bid;

			if (unique)
				BATkey(BATmirror(newcur), TRUE);
			obat->bid = obat->ibid;
			obat->ibid = id;

			BATmsync(ins);
			ins = cur;
			cur = newcur;
		} else {
			BATappend(cur,ins,TRUE);
			BATcleanProps(cur);
			if (cur->batPersistence == PERSISTENT)
				BATmsync(cur);
		}
		obat->cnt = obat->ibase = BATcount(cur);
		temp_destroy(obat->ibid);
		obat->ibid = e_bat(cur->ttype);
	}
	bat_destroy(ins);

	if (obat->ucnt || cleared) {
		BAT *ui = temp_descriptor(obat->uibid);
		BAT *uv = temp_descriptor(obat->uvbid);

		/* any updates */
		if (BUNlast(ui) > BUNfirst(ui)) {
			void_replace_bat(cur, ui, uv, TRUE);
			/* cleanup the old deltas */
			temp_destroy(obat->uibid);
			temp_destroy(obat->uvbid);
			obat->uibid = e_bat(TYPE_oid);
			obat->uvbid = e_bat(cur->ttype);
			obat->ucnt = 0;
		}
		bat_destroy(ui);
		bat_destroy(uv);
	}
	bat_destroy(cur);
	if (obat->next) { 
		ok = destroy_bat(tr, obat->next);
		obat->next = NULL;
	}
	return ok;
}

static int
tr_update_dbat(sql_trans *tr, sql_dbat *tdb, sql_dbat *fdb, int cleared)
{
	int ok = LOG_OK;
	BAT *db = NULL;

	if (!fdb)
		return ok;

	if (fdb->cached) {
		bat_destroy(fdb->cached);
		fdb->cached = NULL;
	}
	if (tdb->cached) {
		bat_destroy(tdb->cached);
		tdb->cached = NULL;
	}
	assert(store_nr_active==1);
	db = temp_descriptor(fdb->dbid);
	if (BUNlast(db) > db->batInserted || cleared) {
		BAT *odb = temp_descriptor(tdb->dbid);

		append_inserted(odb, db);
		BATcommit(odb);
		temp_destroy(fdb->dbid);

		fdb->dbid = 0;
		tdb->cnt = fdb->cnt;
		bat_destroy(odb);
	}
	bat_destroy(db);
	if (tdb->next) {
		ok = destroy_dbat(tr, tdb->next);
		tdb->next = NULL;
	}
	return ok;
}

static int
tr_merge_dbat(sql_trans *tr, sql_dbat *tdb)
{
	int ok = LOG_OK;

	if (tdb->cached) {
		bat_destroy(tdb->cached);
		tdb->cached = NULL;
	}
	assert(store_nr_active==1);
	if (tdb->next) {
		ok = destroy_dbat(tr, tdb->next);
		tdb->next = NULL;
	}
	return ok;
}

static int
update_table(sql_trans *tr, sql_table *ft, sql_table *tt)
{
	sql_trans *oldest = oldest_active_transaction();
	int ok = LOG_OK;
	node *n, *m;

	if (ft->cleared){
		if (store_nr_active == 1) {
			(void)store_funcs.clear_del(tr->parent, tt);
			for (n = tt->columns.set->h; n; n = n->next) 
				(void)store_funcs.clear_col(tr->parent, n->data);
			if (tt->idxs.set) 
				for (n = tt->idxs.set->h; n; n = n->next) 
					(void)store_funcs.clear_idx(tr->parent, n->data);
		} else {
			for (n = ft->columns.set->h; n; n = n->next) 
				empty_col(n->data);
			if (ft->idxs.set) 
				for (n = ft->idxs.set->h; n; n = n->next) 
					empty_idx(n->data);
		}
	}

	if (store_nr_active == 1 || ft->base.allocated) {
		if (store_nr_active > 1 && ft->data) { /* move delta */
			sql_dbat *b = ft->data;

			ft->data = NULL;
			b->next = tt->data;
			tt->data = b;

			if (b->cached) {
				bat_destroy(b->cached);
				b->cached = NULL;
			}
			while (b && b->wtime >= oldest->stime)
				b = b->next;
			if (0 && b && b->wtime < oldest->stime) {
				/* anything older can go */
				destroy_dbat(tr, b->next);
				b->next = NULL;
			}
		} else if (tt->data && ft->base.allocated) {
			tr_update_dbat(tr, tt->data, ft->data, ft->cleared);
		} else if (store_nr_active == 1 && !ft->base.allocated) {
			tr_merge_dbat(tr, tt->data);
			ft->data = NULL;
		} else if (ft->data) {
			tt->data = ft->data;
			tt->base.allocated = 1;
			ft->data = NULL;
		}
	}
	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
		sql_column *cc = n->data;
		sql_column *oc = m->data;

		if (store_nr_active == 1 || (cc->base.wtime && cc->base.allocated)) {
			assert(!cc->base.wtime || oc->base.wtime < cc->base.wtime);
			if (store_nr_active > 1 && cc->data) { /* move delta */
				sql_delta *b = cc->data;

				cc->data = NULL;
				b->next = oc->data;
				oc->data = b;

				if (b->cached) {
					bat_destroy(b->cached);
					b->cached = NULL;
				}
				while (b && b->wtime >= oldest->stime) 
					b = b->next;
				if (0 && b && b->wtime < oldest->stime) {
					/* anything older can go */
					destroy_bat(tr, b->next);
					b->next = NULL;
				}
			} else if (oc->data && cc->base.allocated) {
				tr_update_delta(tr, oc->data, cc->data, cc->unique == 1);
			} else if (store_nr_active == 1 && !cc->base.allocated) {
				tr_merge_delta(tr, oc->data, oc->unique == 1);
				cc->data = NULL;
			} else if (cc->data) {
				oc->data = cc->data; 
				oc->base.allocated = 1;
				cc->data = NULL;
			}
		}

		oc->null = cc->null;
		oc->unique = cc->unique;
		if (cc->storage_type && (!oc->storage_type || strcmp(cc->storage_type, oc->storage_type) != 0))
			oc->storage_type = sa_strdup(tr->sa, cc->storage_type);
		if (!cc->storage_type)
			oc->storage_type = NULL;
		if (cc->def && (!oc->def || strcmp(cc->def, oc->def) != 0))
			oc->def = sa_strdup(tr->sa, cc->def);
		if (!cc->def)
			oc->def = NULL;

		if (oc->base.rtime < cc->base.rtime)
			oc->base.rtime = cc->base.rtime;
		if (oc->base.wtime < cc->base.wtime)
			oc->base.wtime = cc->base.wtime;
		if (cc->data) 
			destroy_col(tr, cc);
		cc->base.allocated = cc->base.rtime = cc->base.wtime = 0;
	}
	if (ok == LOG_OK && tt->idxs.set) {
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;

			/* some indices have no bats */
			if (!oi->data) {
				ci->data = NULL;
				ci->base.allocated = ci->base.rtime = ci->base.wtime = 0;
				continue;
			}
			if (store_nr_active == 1 || (ci->base.wtime && ci->base.allocated)) {
				if (store_nr_active > 1 && ci->data) { /* move delta */
					sql_delta *b = ci->data;

					ci->data = NULL;
					b->next = oi->data;
					oi->data = b;
					if (b->cached) {
						bat_destroy(b->cached);
						b->cached = NULL;
					}
					while (b && b->wtime >= oldest->stime) 
						b = b->next;
					if (0 && b && b->wtime < oldest->stime) {
						/* anything older can go */
						destroy_bat(tr, b->next);
						b->next = NULL;
					}
				} else if (oi->data && ci->base.allocated) {
					tr_update_delta(tr, oi->data, ci->data, 0);
				} else if (store_nr_active == 1 && !ci->base.allocated) {
					tr_merge_delta(tr, oi->data, 0);
					ci->data = NULL;
				} else if (ci->data) {
					oi->data = ci->data;
					oi->base.allocated = 1;
					ci->data = NULL;
				}
			}

			if (oi->base.rtime < ci->base.rtime)
				oi->base.rtime = ci->base.rtime;
			if (oi->base.wtime < ci->base.wtime)
				oi->base.wtime = ci->base.wtime;
			if (ci->data)
				destroy_idx(tr, ci);
			ci->base.allocated = ci->base.rtime = ci->base.wtime = 0;
		}
	}
	if (tt->base.rtime < ft->base.rtime)
		tt->base.rtime = ft->base.rtime;
	if (tt->base.wtime < ft->base.wtime)
		tt->base.wtime = ft->base.wtime;
	if (ft->data)
		destroy_del(tr, ft);
	ft->base.allocated = ft->base.rtime = ft->base.wtime = 0;
	return ok;
}

static int 
tr_log_delta( sql_trans *tr, sql_delta *cbat, int cleared)
{
	int ok = LOG_OK;
	BAT *ins;

	(void)tr;
	assert(tr->parent == gtrans);
	ins = temp_descriptor(cbat->ibid);

	if (cleared) 
		log_bat_clear(bat_logger, cbat->name);

	/* any inserts */
	if (BUNlast(ins) > BUNfirst(ins)) {
		assert(store_nr_active>0);
		if (BUNlast(ins) > ins->batInserted && (store_nr_active != 1 || cbat->ibase || BATcount(ins) <= SNAPSHOT_MINSIZE)) 
			ok = log_bat(bat_logger, ins, cbat->name);
		if (store_nr_active == 1 &&
		    !cbat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE) {
			/* log new snapshot */
			logger_add_bat(bat_logger, ins, cbat->name);
			ok = log_bat_persists(bat_logger, ins, cbat->name);
		}
	}
	bat_destroy(ins);

	if (cbat->ucnt) {
		BAT *ui = temp_descriptor(cbat->uibid);
		BAT *uv = temp_descriptor(cbat->uvbid);
		/* any updates */
		if (ok == LOG_OK && (BUNlast(uv) > uv->batInserted || BATdirty(uv))) 
			ok = log_delta(bat_logger, ui, uv, cbat->name);
		bat_destroy(ui);
		bat_destroy(uv);
	}
	return ok;
}

static int
tr_log_dbat(sql_trans *tr, sql_dbat *fdb, int cleared)
{
	int ok = LOG_OK;
	BAT *db = NULL;

	if (!fdb)
		return ok;

	(void)tr;
	assert (fdb->dname);
	if (cleared) 
		log_bat_clear(bat_logger, fdb->dname);

	db = temp_descriptor(fdb->dbid);
	if (BUNlast(db) > BUNfirst(db)) {
		assert(store_nr_active>0);
		if (BUNlast(db) > db->batInserted) 
			ok = log_bat(bat_logger, db, fdb->dname);
	}
	bat_destroy(db);
	return ok;
}

static int
log_table(sql_trans *tr, sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	assert(tr->parent == gtrans);
	if (ft->base.wtime && ft->base.allocated)
		ok = tr_log_dbat(tr, ft->data, ft->cleared);
	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if (!cc->base.wtime || !cc->base.allocated) 
			continue;
		ok = tr_log_delta(tr, cc->data, ft->cleared);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime || !ci->base.allocated)
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
	if (store_nr_active == 1 && !cbat->ibase && cbat->cnt > SNAPSHOT_MINSIZE) {
		BAT *ins = temp_descriptor(cbat->ibid);

		/* any inserts */
		if (BUNlast(ins) > BUNfirst(ins)) {
			bat_set_access(ins, BAT_READ);
			BATmode(ins, PERSISTENT);
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

		if (!cc->base.wtime || !cc->base.allocated) 
			continue;
		tr_snapshot_bat(tr, cc->data);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime || !ci->base.allocated)
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

	sf->count_del = (count_del_fptr)&count_del;
	sf->count_col = (count_col_fptr)&count_col;
	sf->count_idx = (count_idx_fptr)&count_idx;
	sf->dcount_col = (dcount_col_fptr)&dcount_col;
	sf->sorted_col = (prop_col_fptr)&sorted_col;
	sf->double_elim_col = (prop_col_fptr)&double_elim_col;

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

