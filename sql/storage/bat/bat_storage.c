/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_storage.h"
#include "bat_utils.h"
#include "sql_string.h"
#include "gdk_atoms.h"

#define SNAPSHOT_MINSIZE ((BUN) 1024*128)

#define inTransaction(tr, obj) (((sql_base*)obj)->ts == tr->tid)

static int log_update_col( sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_update_idx( sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_update_del( sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_create_col(sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_create_idx(sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_create_del(sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_destroy_col(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest);
static int log_destroy_idx(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest);
static int log_destroy_del(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest);

static int tr_merge_delta( sql_trans *tr, sql_delta *obat);

static list *
sa_list_append(sql_allocator *sa, list *l, void *data)
{
	if (!l)
		l = sa_list(sa);
	return list_append(l, data);
}

static MT_Lock destroy_lock = MT_LOCK_INITIALIZER(destroy_lock);
sql_dbat *tobe_destroyed_dbat = NULL;
sql_delta *tobe_destroyed_delta = NULL;

sql_delta *
timestamp_delta( sql_delta *d, ulng tid, ulng ts)
{
	while (d->next && d->ts != tid && d->ts > ts)
		d = d->next;
	return d;
}

sql_dbat *
timestamp_dbat( sql_dbat *d, ulng tid, ulng ts)
{
	while (d->next && d->ts != tid && d->ts > ts)
		d = d->next;
	return d;
}

static BAT *
delta_bind_del(sql_dbat *bat, int access)
{
	BAT *b;

	(void) access;
	assert(access == RDONLY || access == RD_INS);
	assert(access != RD_UPD_ID && access != RD_UPD_VAL);

	b = temp_descriptor(bat->dbid);
	assert(BATcount(b) == bat->cnt);
	return b;
}

static BAT *
bind_del(sql_trans *tr, sql_table *t, int access)
{
	assert(access == QUICK || tr->active);
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, tr->tid, tr->ts);
	}
	return delta_bind_del(t->data, access);
}

static BAT *
delta_bind_ubat(sql_delta *bat, int access, int type)
{
	BAT *b;
	log_bid bb;

	(void) access;
	assert(access == RD_UPD_ID || access == RD_UPD_VAL);
	if (bat->uibid && bat->uvbid) {
		if (access == RD_UPD_ID)
			b = temp_descriptor(bat->uibid);
		else
			b = temp_descriptor(bat->uvbid);
	} else {
		if (access == RD_UPD_ID) {
			bb = e_bat(TYPE_oid);
			if(bb == BID_NIL)
				return NULL;
			b = temp_descriptor(bb);
		} else {
			bb = e_bat(type);
			if(bb == BID_NIL)
				return NULL;
			b = temp_descriptor(bb);
		}
	}
	return b;
}

static BAT *
bind_ucol(sql_trans *tr, sql_column *c, int access)
{
	BAT *u = NULL;

	assert(tr->active);
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
	}
	if (!c->t->data) {
		sql_table *ot = tr_find_table(tr->parent, c->t);
		c->t->data = timestamp_dbat(ot->data, tr->tid, tr->ts);
	}
	u = delta_bind_ubat(c->data, access, c->type.type->localtype);
	return u;
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	BAT *u = NULL;

	assert(tr->active);
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, tr->tid, tr->ts);
	}
	if (!i->t->data) {
		sql_table *ot = tr_find_table(tr->parent, i->t);
		i->t->data = timestamp_dbat(ot->data, tr->tid, tr->ts);
	}
	u = delta_bind_ubat(i->data, access, (oid_index(i->type))?TYPE_oid:TYPE_lng);
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
		if (b == NULL)
			return NULL;
		if (BATcount(b) && bat->uibid && bat->uvbid) {
			/* apply updates to the inserted */
			BAT *ui = temp_descriptor(bat->uibid), *uv = temp_descriptor(bat->uvbid), *nui = ui, *nuv = uv, *o;

			if (ui == NULL || uv == NULL) {
				bat_destroy(ui);
				bat_destroy(uv);
				bat_destroy(b);
				return NULL;
			}
			if (!isEbat(nui) && BATcount(nui)) {
				o = BATselect(ui, NULL, &b->hseqbase, ATOMnilptr(ui->ttype), true, false, false);
				if (o == NULL) {
					bat_destroy(ui);
					bat_destroy(uv);
					bat_destroy(b);
					return NULL;
				}
				nui = BATproject(o, ui);
				bat_destroy(ui);
				nuv = BATproject(o, uv);
				bat_destroy(uv);
				bat_destroy(o);
				if (nui == NULL ||
				    nuv == NULL ||
				    BATreplace(b, nui, nuv, true) != GDK_SUCCEED) {
					bat_destroy(nui);
					bat_destroy(nuv);
					bat_destroy(b);
					return NULL;
				}
			}
			bat_destroy(nui);
			bat_destroy(nuv);
		}
	} else if (!bat->bid) {
		int tt = 0;
		b = temp_descriptor(bat->ibid);
		if (b == NULL)
			return NULL;
		tt = b->ttype;
		bat_destroy(b);
		b = e_BAT(tt);
		if (b == NULL)
			return NULL;
	} else {
		b = temp_descriptor(bat->bid);
		if (b == NULL)
			return NULL;
		bat_set_access(b, BAT_READ);
	}
	assert(b);
	return b;
}

static BAT *
bind_col(sql_trans *tr, sql_column *c, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(c->t))
		return NULL;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
	}
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_ucol(tr, c, access);
	return delta_bind_bat( c->data, access, isTempTable(c->t));
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(i->t))
		return NULL;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, tr->tid, tr->ts);
	}
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_uidx(tr, i, access);
	return delta_bind_bat( i->data, access, isTempTable(i->t));
}

static int
delta_update_bat( sql_delta *bat, BAT *tids, BAT *updates, int is_new)
{
	BAT *b, *ui = NULL, *uv = NULL;
	gdk_return ret;

	if (!BATcount(tids))
		return LOG_OK;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (!is_new && bat->uibid && bat->uvbid) {
		BAT *ib = temp_descriptor(bat->ibid);
		BAT *o = NULL;

		if (ib == NULL)
			return LOG_ERR;

		if (BATcount(ib)) {
			BAT *nui = tids, *nuv = updates;

			o = BATselect(tids, NULL, &ib->hseqbase, ATOMnilptr(tids->ttype), true, false, false);
			if (o == NULL) {
				bat_destroy(ib);
				return LOG_ERR;
			}
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			bat_destroy(o);
			if (nui == NULL || nuv == NULL) {
				bat_destroy(ib);
				bat_destroy(nui);
				bat_destroy(nuv);
				return LOG_ERR;
			}
			assert(BATcount(nui) == BATcount(nuv));
			ret = BATreplace(ib, nui, nuv, true);
			bat_destroy(nui);
			bat_destroy(nuv);
			if (ret != GDK_SUCCEED) {
				bat_destroy(ib);
				return LOG_ERR;
			}

			o = BATselect(tids, NULL, ATOMnilptr(tids->ttype), &ib->hseqbase, false, false, false);
			if (o == NULL) {
				bat_destroy(ib);
				return LOG_ERR;
			}
		}
		bat_destroy(ib);

		ui = temp_descriptor(bat->uibid);
		uv = temp_descriptor(bat->uvbid);
		if (ui == NULL || uv == NULL) {
			bat_destroy(ui);
			bat_destroy(uv);
			return LOG_ERR;
		}
		assert(ui && uv);
		if (isEbat(ui)){
			temp_destroy(bat->uibid);
			bat->uibid = temp_copy(ui->batCacheid, false);
			bat_destroy(ui);
			if (bat->uibid == BID_NIL ||
			    (ui = temp_descriptor(bat->uibid)) == NULL) {
				bat_destroy(uv);
				return LOG_ERR;
			}
		}
		if (isEbat(uv)){
			temp_destroy(bat->uvbid);
			bat->uvbid = temp_copy(uv->batCacheid, false);
			bat_destroy(uv);
			if (bat->uvbid == BID_NIL ||
			    (uv = temp_descriptor(bat->uvbid)) == NULL) {
				bat_destroy(ui);
				return LOG_ERR;
			}
		}
		if (BATappend(ui, tids, o, true) != GDK_SUCCEED ||
		    BATappend(uv, updates, o, true) != GDK_SUCCEED) {
			bat_destroy(o);
			bat_destroy(ui);
			bat_destroy(uv);
			return LOG_ERR;
		}
		assert(BATcount(tids) == BATcount(updates));
		bat_destroy(o);
		bat_destroy(ui);
		bat_destroy(uv);
	} else if (is_new && bat->bid) {
		BAT *ib = temp_descriptor(bat->ibid);
		b = temp_descriptor(bat->bid);

		if (b == NULL || ib == NULL) {
			bat_destroy(b);
			bat_destroy(ib);
			return LOG_ERR;
		}
		if (BATcount(ib)) {
			BAT *nui = tids, *nuv = updates, *o;

			o = BATselect(tids, NULL, &ib->hseqbase, ATOMnilptr(tids->ttype), true, false, false);
			if (o == NULL) {
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			bat_destroy(o);
			if (nui == NULL || nuv == NULL) {
				bat_destroy(nui);
				bat_destroy(nuv);
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}
			assert(BATcount(nui) == BATcount(nuv));
			ret = BATreplace(ib, nui, nuv, true);
			bat_destroy(nui);
			bat_destroy(nuv);
			if (ret != GDK_SUCCEED) {
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}

			o = BATselect(tids, NULL, ATOMnilptr(tids->ttype), &ib->hseqbase, false, false, false);
			if (o == NULL) {
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}
			nui = BATproject(o, tids);
			nuv = BATproject(o, updates);
			bat_destroy(o);
			if (nui == NULL || nuv == NULL) {
				bat_destroy(nui);
				bat_destroy(nuv);
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}
			assert(BATcount(nui) == BATcount(nuv));
			ret = BATreplace(b, nui, nuv, true);
			bat_destroy(nui);
			bat_destroy(nuv);
			if (ret != GDK_SUCCEED) {
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}
		} else {
			if (BATreplace(b, tids, updates, true) != GDK_SUCCEED) {
				bat_destroy(b);
				bat_destroy(ib);
				return LOG_ERR;
			}
		}
		bat_destroy(ib);
		bat_destroy(b);
	} else {
		b = temp_descriptor(bat->ibid);
		if (b == NULL)
			return LOG_ERR;
		ret = BATreplace(b, tids, updates, true);
		bat_destroy(b);
		if (ret != GDK_SUCCEED) {
			return LOG_ERR;
		}
	}
	bat->ucnt += BATcount(tids);
	return LOG_OK;
}

static int
delta_update_val( sql_delta *bat, oid rid, void *upd)
{
	BAT *b = NULL;

	assert(!is_oid_nil(rid));

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (bat->uibid && bat->uvbid) {
		BAT *ib = temp_descriptor(bat->ibid);
		if(ib == NULL)
			return LOG_ERR;

		if (BATcount(ib) && ib->hseqbase <= rid) {
			/* ToDo what if rid updates 'old inserts' */
			if (void_inplace(ib, rid, upd, true) != GDK_SUCCEED) {
				bat_destroy(ib);
				return LOG_ERR;
			}
		} else {
			BAT *ui = temp_descriptor(bat->uibid);
			BAT *uv = temp_descriptor(bat->uvbid);
			if(ui == NULL || uv == NULL) {
				bat_destroy(ui);
				bat_destroy(uv);
				bat_destroy(ib);
				return LOG_ERR;
			}

			if (isEbat(ui)){
				temp_destroy(bat->uibid);
				bat->uibid = temp_copy(ui->batCacheid, false);
				if (bat->uibid == BID_NIL)
					return LOG_ERR;
				bat_destroy(ui);
				ui = temp_descriptor(bat->uibid);
				if(ui == NULL) {
					bat_destroy(uv);
					bat_destroy(ib);
					return LOG_ERR;
				}
			}
			if (isEbat(uv)){
				temp_destroy(bat->uvbid);
				bat->uvbid = temp_copy(uv->batCacheid, false);
				if (bat->uvbid == BID_NIL)
					return LOG_ERR;
				bat_destroy(uv);
				uv = temp_descriptor(bat->uvbid);
				if(uv == NULL) {
					bat_destroy(ui);
					bat_destroy(ib);
					return LOG_ERR;
				}
			}
			if (BUNappend(ui, (ptr) &rid, true) != GDK_SUCCEED ||
			    BUNappend(uv, (ptr) upd, true) != GDK_SUCCEED) {
				bat_destroy(ui);
				bat_destroy(uv);
				bat_destroy(ib);
				return LOG_ERR;
			}
			bat->ucnt++;
			bat_destroy(ui);
			bat_destroy(uv);
		}
		bat_destroy(ib);
	} else {
		if((b = temp_descriptor(bat->ibid)) == NULL)
			return LOG_ERR;
		if (void_inplace(b, rid, upd, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
		bat_destroy(b);
	}
	return LOG_OK;
}

static int
dup_delta(sql_trans *tr, sql_delta *obat, sql_delta *bat, int type, int oc_isnew, int c_isnew, int temp, int sz)
{
	(void)tr;
	if (!obat)
		return LOG_OK;
	bat->ibid = obat->ibid;
	bat->bid = obat->bid;
	bat->uibid = obat->uibid;
	bat->uvbid = obat->uvbid;
	bat->ibase = obat->ibase;
	bat->cnt = obat->cnt;
	bat->ucnt = obat->ucnt;
	bat->cleared = obat->cleared;

	bat->name = _STRDUP(obat->name);
	if(!bat->name)
		return LOG_ERR;

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
			b = COLnew((oid) obat->cnt, type, sz, PERSISTENT);
			if (b == NULL)
				return LOG_ERR;
			bat_set_access(b, BAT_READ);
			obat->ibid = temp_create(b);
			obat->ibase = bat->ibase = (oid) obat->cnt;
			bat_destroy(b);
			if (c_isnew) {
				/* new cols are moved to gtrans and bat.bid */
				temp_dup(bat->ibid);
				obat->bid = bat->ibid;
			} else if (!c_isnew) {
				bat->bid = bat->ibid;

				b = COLnew(bat->ibase, type, sz, PERSISTENT);
				if (b == NULL)
					return LOG_ERR;
				bat_set_access(b, BAT_READ);
				bat->ibid = temp_create(b);
				bat_destroy(b);
			}
		} else { /* old column */
			bat->ibid = ebat_copy(bat->ibid, bat->ibase, 0);
			if (bat->ibid == BID_NIL)
				return LOG_ERR;
		}
	}
	if (!temp && bat->ibid) {
		if (bat->uibid && bat->uvbid) {
			if (c_isnew) {
				obat->uibid = ebat_copy(bat->uibid, 0, 0);
				obat->uvbid = ebat_copy(bat->uvbid, 0, 0);
				if (obat->uibid == BID_NIL ||
				    obat->uvbid == BID_NIL)
					return LOG_ERR;
			} else {
				bat->uibid = ebat_copy(bat->uibid, 0, 0);
				bat->uvbid = ebat_copy(bat->uvbid, 0, 0);
				if (bat->uibid == BID_NIL ||
				    bat->uvbid == BID_NIL)
					return LOG_ERR;
			}
		} else {
			bat->uibid = e_bat(TYPE_oid);
			bat->uvbid = e_bat(type);
			if (bat->uibid == BID_NIL || bat->uvbid == BID_NIL)
				return LOG_ERR;
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

static sql_delta *
bind_col_data(sql_trans *tr, sql_column *c)
{
	sql_delta *obat = c->data;

	int type = c->type.type->localtype;
	sql_column *oc = tr_find_column(tr->parent, c);
	assert(c == oc);
	if (obat->ts == tr->tid)
		return obat;
	if (obat->ts >= TRANSACTION_ID_BASE)
		/* abort */
		return NULL;
	obat = timestamp_delta(oc->data, tr->tid, tr->ts);
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	if(dup_bat(tr, c->t, obat, bat, type, isNew(oc), isNew(c)) == LOG_ERR)
		return NULL;
	bat->ts = tr->tid;
	bat->next = obat;
	c->data = bat;
	return bat;
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, int tpe)
{
	int ok = LOG_OK;
	BAT *b = tids;
	sql_delta *delta, *odelta = c->data;

	if (tpe == TYPE_bat && !BATcount(b))
		return LOG_OK;

	if ((delta = bind_col_data(tr, c)) == NULL)
		return LOG_ERR;

	assert(delta && delta->ts == tr->tid);
	ulng cnt = delta->cnt;
	if (tpe == TYPE_bat)
		ok = delta_update_bat(delta, tids, upd, isNew(c));
	else
		ok = delta_update_val(delta, *(oid*)tids, upd);
	if (!inTransaction(tr, c->t) && !isTempTable(c->t) && odelta != delta) {
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &c->base;
		change->type = column_obj;
		change->kind = kind_append;
		change->data = delta;
		change->cnt = delta->cnt - cnt;
		change->log = &log_update_col;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
	}
	return ok;
}

static sql_delta *
bind_idx_data(sql_trans *tr, sql_idx *i)
{
	sql_delta *obat = i->data;

	int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;
	sql_idx *oi = tr_find_idx(tr->parent, i);

	if (obat->ts == tr->tid)
		return obat;
	if (obat->ts >= TRANSACTION_ID_BASE)
		/* abort */
		return NULL;

	obat = timestamp_delta(oi->data, tr->tid, tr->ts);
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	if(dup_bat(tr, i->t, obat, bat, type, isNew(i), isNew(i)) == LOG_ERR)
		return NULL;
	bat->ts = tr->tid;
	bat->next = obat;
	i->data = bat;
	return bat;
}

static int
update_idx(sql_trans *tr, sql_idx * i, void *tids, void *upd, int tpe)
{
	int ok = LOG_OK;
	BAT *b = tids;
	sql_delta *delta, *odelta = i->data;

	if (tpe == TYPE_bat && !BATcount(b))
		return LOG_OK;

	if ((delta = bind_idx_data(tr, i)) == NULL)
		return LOG_ERR;

	assert(delta && delta->ts == tr->tid);
	ulng cnt = delta->cnt;
	if (tpe == TYPE_bat)
		ok = delta_update_bat(delta, tids, upd, isNew(i));
	else
		assert(0);
	if (!inTransaction(tr, i->t) && !isTempTable(i->t) && odelta != delta) {
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &i->base;
		change->type = column_obj;
		change->kind = kind_append;
		change->data = delta;
		change->cnt = delta->cnt - cnt;
		change->log = &log_update_idx;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
	}
	return ok;
}

static int
delta_append_bat( sql_delta *bat, BAT *i )
{
	int id = i->batCacheid;
	BAT *b;
#ifndef NDEBUG
	BAT *c = BBPquickdesc(bat->bid, false);
#endif

	if (!BATcount(i))
		return LOG_OK;
	b = temp_descriptor(bat->ibid);
	if (b == NULL)
		return LOG_ERR;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	assert(!c || BATcount(c) == bat->ibase);
	if (BATcount(b) == 0 && BBP_refs(id) == 1 && BBP_lrefs(id) == 1 && !isVIEW(i) && i->ttype && i->batRole == PERSISTENT){
		temp_destroy(bat->ibid);
		bat->ibid = id;
		temp_dup(id);
		BAThseqbase(i, bat->ibase);
	} else {
		if (!isEbat(b)){
			assert(b->theap.storage != STORE_PRIV);
		} else {
			temp_destroy(bat->ibid);
			bat->ibid = ebat2real(b->batCacheid, bat->ibase);
			bat_destroy(b);
			if(bat->ibid != BID_NIL) {
				b = temp_descriptor(bat->ibid);
				if (b == NULL)
					return LOG_ERR;
			} else {
				return LOG_ERR;
			}
		}
		if (isVIEW(i) && b->batCacheid == VIEWtparent(i)) {
			BAT *ic = COLcopy(i, i->ttype, true, TRANSIENT);
			if (ic == NULL || BATappend(b, ic, NULL, true) != GDK_SUCCEED) {
				bat_destroy(ic);
				bat_destroy(b);
				return LOG_ERR;
			}
			bat_destroy(ic);
		} else if (BATappend(b, i, NULL, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
		assert(BUNlast(b) > b->batInserted);
	}
	bat_destroy(b);
	bat->cnt += BATcount(i);
	return LOG_OK;
}

static int
delta_append_val( sql_delta *bat, void *i )
{
	BAT *b = temp_descriptor(bat->ibid);
#ifndef NDEBUG
	BAT *c = BBPquickdesc(bat->bid, false);
#endif
	if(b == NULL)
		return LOG_ERR;

	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	assert(!c || BATcount(c) == bat->ibase);
	if (isEbat(b)) {
		bat_destroy(b);
		temp_destroy(bat->ibid);
		bat->ibid = ebat2real(bat->ibid, bat->ibase);
		if(bat->ibid != BID_NIL) {
			b = temp_descriptor(bat->ibid);
			if (b == NULL)
				return LOG_ERR;
		} else {
			return LOG_ERR;
		}
	}
	if (BUNappend(b, i, true) != GDK_SUCCEED) {
		bat_destroy(b);
		return LOG_ERR;
	}
	assert(BUNlast(b) > b->batInserted);
	bat->cnt ++;
	bat_destroy(b);
	return LOG_OK;
}

static int
dup_dbat( sql_trans *tr, sql_dbat *obat, sql_dbat *bat, int is_new, int temp)
{
	bat->dbid = obat->dbid;
	bat->cnt = obat->cnt;
	bat->dname = _STRDUP(obat->dname);
	bat->cleared = obat->cleared;
	if(!bat->dname)
		return LOG_ERR;
	if (bat->dbid) {
		if (is_new) {
			obat->dbid = temp_copy(bat->dbid, temp);
		} else {
			bat->dbid = ebat_copy(bat->dbid, 0, temp);
		}
		assert(BATcount(quick_descriptor(bat->dbid)) == bat->cnt);
		if (bat->dbid == BID_NIL)
			return LOG_ERR;
	}
	(void)tr;
	return LOG_OK;
}

static int
append_col(sql_trans *tr, sql_column *c, void *i, int tpe)
{
	int ok = LOG_OK;
	BAT *b = i;
	sql_delta *delta, *odelta = c->data;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if ((delta = bind_col_data(tr, c)) == NULL)
		return LOG_ERR;

	assert(delta && delta->ts == tr->tid);
	ulng cnt = delta->cnt;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(delta, i);
	else
		ok = delta_append_val(delta, i);
	if (!inTransaction(tr, c->t) && !isTempTable(c->t) && odelta != delta) {
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &c->base;
		change->type = column_obj;
		change->kind = kind_append;
		change->data = delta;
		change->cnt = delta->cnt - cnt;
		change->log = &log_update_col;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
	}
	return ok;
}

static int
append_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	int ok = LOG_OK;
	BAT *b = ib;
	sql_delta *delta, *odelta = i->data;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if ((delta = bind_idx_data(tr, i)) == NULL)
		return LOG_ERR;

	assert(delta && delta->ts == tr->tid);
	ulng cnt = delta->cnt;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(delta, ib);
	else
		ok = delta_append_val(delta, ib);
	if (!inTransaction(tr, i->t) && !isTempTable(i->t) && odelta != delta) {
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &i->base;
		change->type = idx_obj;
		change->kind = kind_append;
		change->data = delta;
		change->cnt = delta->cnt - cnt;
		change->log = &log_update_idx;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
	}
	return ok;
}

static int
delta_delete_bat( sql_dbat *bat, BAT *i )
{
	BAT *b = temp_descriptor(bat->dbid);

	if(!b)
		return LOG_ERR;

	if (isEbat(b)) {
		assert(ATOMtype(b->ttype) == TYPE_oid);
		temp_destroy(bat->dbid);
		bat->dbid = temp_copy(b->batCacheid, FALSE);
		if (bat->dbid == BID_NIL)
			return LOG_ERR;
		bat_destroy(b);
		b = temp_descriptor(bat->dbid);
		if (b == NULL)
			return LOG_ERR;
	}
	assert(b->theap.storage != STORE_PRIV);
	assert(BATcount(b) == bat->cnt);
	if (BATappend(b, i, NULL, true) != GDK_SUCCEED) {
		bat_destroy(b);
		return LOG_ERR;
	}
	BATkey(b, true);
	assert(BATcount(b) == bat->cnt+ BATcount(i));
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
		assert(ATOMtype(b->ttype) == TYPE_oid);
		bat->dbid = temp_copy(b->batCacheid, FALSE);
		if (bat->dbid == BID_NIL)
			return LOG_ERR;
		bat_destroy(b);
		b = temp_descriptor(bat->dbid);
		if (b == NULL)
			return LOG_ERR;
	}
	assert(b->theap.storage != STORE_PRIV);
	assert(BATcount(b) == bat->cnt);
	if (BUNappend(b, (ptr)&rid, true) != GDK_SUCCEED) {
		bat_destroy(b);
		return LOG_ERR;
	}
	BATkey(b, true);
	bat_destroy(b);

	bat->cnt ++;
	return LOG_OK;
}

static void
_destroy_dbat(sql_dbat *bat)
{
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
}

static int
destroy_dbat(sql_trans *tr, sql_dbat *bat)
{
	sql_dbat *n;

	(void)tr;
	while(bat) {
		n = bat->next;
		_destroy_dbat(bat);
		bat = n;
	}
	return LOG_OK;
}

static sql_dbat *
bind_del_data(sql_trans *tr, sql_table *t)
{
	sql_dbat *obat = t->data;

	sql_table *ot = tr_find_table(tr->parent, t);
	assert(t == ot);
	if (obat->ts == tr->tid)
		return obat;
	if (obat->ts >= TRANSACTION_ID_BASE)
		/* abort */
		return NULL;
	obat = timestamp_dbat(ot->data, tr->tid, tr->ts);
	sql_dbat *bat = ZNEW(sql_dbat);
	if(!bat)
		return NULL;
	dup_dbat(tr, obat, bat, isNew(ot), isTempTable(t));
	bat->ts = tr->tid;
	bat->next = obat;
	t->data = bat;
	return LOG_OK;
}

static int
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	int ok = LOG_OK;
	BAT *b = ib;
	sql_dbat *bat, *obat = t->data;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if ((bat = bind_del_data(tr, t)) == NULL)
		return LOG_ERR;

	/* delete all cached copies */
	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	/*
	//node *n;
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_delta *bat;

		if (!c->data) {
			sql_column *oc = tr_find_column(tr->parent, c);
			c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
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

			if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;
			if (!i->data) {
				sql_idx *oi = tr_find_idx(tr->parent, i);
				i->data = timestamp_delta(oi->data, tr->tid, tr->ts);
			}
			bat = i->data;
			if (bat && bat->cached) {
				bat_destroy(bat->cached);
				bat->cached = NULL;
			}
		}
	}
	*/

	assert(bat && bat->ts == tr->tid);
	/* deletes only write */
	if (tpe == TYPE_bat)
		ok = delta_delete_bat(bat, ib);
	else
		ok = delta_delete_val(bat, *(oid*)ib);
	if (!inTransaction(tr, t) && !isTempTable(t) && obat != bat) {
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &t->base;
		change->type = table_obj;
		change->kind = kind_delete;
		change->data = bat;
		change->log = &log_update_del;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
	}
	return ok;
}

static size_t
count_col(sql_trans *tr, sql_column *c, int all)
{
	sql_delta *b;

	assert(tr->active);
	if (!isTable(c->t))
		return 0;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
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

	assert(tr->active);
	if (!isTable(c->t))
		return 0;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
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

	assert(tr->active);
	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, tr->tid, tr->ts);
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

	assert(tr->active);
	if (!isTable(t))
		return 0;
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, tr->tid, tr->ts);
	}
	d = t->data;
	if (!d)
		return 0;
	return d->cnt;
}

static size_t
count_col_upd(sql_trans *tr, sql_column *c)
{
	sql_delta *b;

	assert(tr->active);
	assert (isTable(c->t)) ;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
	}
	b = c->data;
	if (!b)
		return 1;
	return b->ucnt;
}

static size_t
count_idx_upd(sql_trans *tr, sql_idx *i)
{
	sql_delta *b;

	assert(tr->active);
	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		if (oi)
			i->data = timestamp_delta(oi->data, tr->tid, tr->ts);
	}
	b = i->data;
	if (!b)
		return 0;
	return b->ucnt;
}

static size_t
count_upd(sql_trans *tr, sql_table *t)
{
	node *n;

	assert(tr->active);
	if (!isTable(t))
		return 0;

	for( n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (count_col_upd(tr, c))
			return 1;
	}
	if (t->idxs.set)
	for( n = t->idxs.set->h; n; n = n->next) {
		sql_idx *i = n->data;

		if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
			continue;
		if (count_idx_upd(tr, i))
			return 1;
	}
	return 0;
}

static int
sorted_col(sql_trans *tr, sql_column *col)
{
	int sorted = 0;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;
	/* fallback to central bat */
	if (tr && tr->parent && !col->data && col->po)
		col = col->po;

	if (col && col->data) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b)
			sorted = BATtordered(b) || BATtrevordered(b);
	}
	return sorted;
}

static int
unique_col(sql_trans *tr, sql_column *col)
{
	int distinct = 0;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;
	/* fallback to central bat */
	if (tr && tr->parent && !col->data && col->po)
		col = col->po;

	if (col && col->data) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b)
			distinct = b->tkey;
	}
	return distinct;
}

static int
double_elim_col(sql_trans *tr, sql_column *col)
{
	int de = 0;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;
	/* fallback to central bat */
	if (tr && tr->parent && !col->data && col->po)
		col = col->po;

	if (col && col->data) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b && b->tvarsized) /* check double elimination */
			de = GDK_ELIMDOUBLES(b->tvheap);
		if (de)
			de = b->twidth;
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
	if(bat->uibid == BID_NIL || bat->uvbid == BID_NIL || bat->ibid == BID_NIL) {
		return LOG_ERR;
	}
	return LOG_OK;
}

static int
load_bat(sql_trans *tr, sql_delta *bat, int type, char tpe, oid id)
{
	sqlstore *store = tr->store;
	int bid = logger_find_bat(store->logger, bat->name, tpe, id);

	return load_delta(bat, bid, type);
}

static int
log_create_delta(sql_trans *tr, sql_delta *bat, char tpe, oid id)
{
	int res = LOG_OK;
	gdk_return ok;
	BAT *b = (bat->bid)?
			temp_descriptor(bat->bid):
			temp_descriptor(bat->ibid);

	if (b == NULL)
		return LOG_ERR;

	if (!bat->uibid)
		bat->uibid = e_bat(TYPE_oid);
	if (!bat->uvbid)
		bat->uvbid = e_bat(b->ttype);
	if (bat->uibid == BID_NIL || bat->uvbid == BID_NIL)
		res = LOG_ERR;
	if (GDKinmemory(0)) {
		bat_destroy(b);
		return res;
	}

	sqlstore *store = tr->store;
	ok = logger_add_bat(store->logger, b, bat->name, tpe, id);
	if (ok == GDK_SUCCEED)
		ok = log_bat_persists(store->logger, b, bat->name, tpe, id);
	bat_destroy(b);
	if(res != LOG_OK)
		return res;
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
new_persistent_delta( sql_delta *bat, int sz )
{
	if (bat->bid) { /* result of alter ! */
		BAT *b = temp_descriptor(bat->bid);
		BAT *i = temp_descriptor(bat->ibid);

		if (b == NULL || i == NULL) {
			bat_destroy(b);
			bat_destroy(i);
			return LOG_ERR;
		}
		bat->ibase = BATcount(b);
		bat->cnt = BATcount(b) + BATcount(i);
		bat->ucnt = 0;
		bat->ibid = temp_copy(i->batCacheid, FALSE);
		bat_destroy(i);
		bat_destroy(b);
		if (bat->ibid == BID_NIL)
			return LOG_ERR;
		i = temp_descriptor(bat->ibid);
		if (i == NULL)
			return LOG_ERR;
		bat_set_access(i, BAT_READ);
		BAThseqbase(i, bat->ibase);
		bat_destroy(i);
	} else {
		BAT *i, *b = temp_descriptor(bat->ibid);
		int type;

		if (b == NULL)
			return LOG_ERR;
		type = b->ttype;
		bat->bid = bat->ibid;
		bat->cnt = bat->ibase = BATcount(b);
		bat->ucnt = 0;
		bat_destroy(b);

		i = COLnew(bat->ibase, type, sz, PERSISTENT);
		if (i == NULL)
			return LOG_ERR;
		bat_set_access(i, BAT_READ);
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
	d->ibase = i->hseqbase;
	d->ibid = temp_create(i);
	if (b) {
		d->cnt += BATcount(b);
		bat_set_access(b, BAT_READ);
		d->bid = temp_create(b);
	}
	d->uibid = d->uvbid = 0;
	d->ucnt = 0;
}

static int
upgrade_delta(sql_trans *tr, sql_delta *d, char tpe, oid id)
{
	sqlstore *store = tr->store;
	return logger_upgrade_bat(store->logger, d->name, tpe, id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static bat
copyBat (bat i, int type, oid seq)
{
	BAT *b, *tb;
	bat res;

	if (!i)
		return i;
	tb = temp_descriptor(i);
	if (tb == NULL)
		return 0;
	b = BATconstant(seq, type, ATOMnilptr(type), BATcount(tb), PERSISTENT);
	bat_destroy(tb);
	if (b == NULL)
		return 0;

	bat_set_access(b, BAT_READ);

	res = temp_create(b);
	bat_destroy(b);
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
		if(!bat)
			return LOG_ERR;
	}
	if (!bat->name) {
		bat->name = sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name);
		if(!bat->name)
			ok = LOG_ERR;
	}

	if (inTransaction(tr, c) && !isTempTable(c->t)){
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &c->base;
		change->type = column_obj;
		change->kind = kind_create;
		change->log = &log_create_col;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
		bat->ts = tr->tid;
	}
	if (!inTransaction(tr, c) && !isTempTable(c->t)){
		return load_bat(tr, bat, type, c->t->bootstrap?0:LOG_COL, c->base.id);
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

			if (d->bid) {
				bat->bid = copyBat(d->bid, type, 0);
				if(bat->bid == BID_NIL)
					ok = LOG_ERR;
			}
			if (d->ibid) {
				bat->ibid = copyBat(d->ibid, type, d->ibase);
				if(bat->ibid == BID_NIL)
					ok = LOG_ERR;
			}
			bat->ibase = d->ibase;
			bat->cnt = d->cnt;
			if (d->uibid) {
				bat->uibid = e_bat(TYPE_oid);
				if (bat->uibid == BID_NIL)
					ok = LOG_ERR;
			}
			if (d->uvbid) {
				bat->uvbid = e_bat(type);
				if(bat->uvbid == BID_NIL)
					ok = LOG_ERR;
			}
		} else {
			BAT *b = bat_new(type, c->t->sz, PERSISTENT);
			if (!b) {
				ok = LOG_ERR;
			} else {
				create_delta(c->data, NULL, b);
				bat_destroy(b);
			}
		}
	}
	return ok;
}

static int
upgrade_col(sql_trans *tr, sql_column *c)
{
	sql_delta *bat = c->data;

	if (!c->t->bootstrap)
		return upgrade_delta(tr, bat, LOG_COL, c->base.id);
	return LOG_OK;
}

static int
log_create_col(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_column *c = (sql_column*)change->obj;
	assert(!isTempTable(c->t));
	sql_delta *delta = c->data;
	assert(delta->ts == tr->tid);
	delta->ts = commit_ts;
	(void)oldest;
	ok = log_create_delta(tr,  c->data, c->t->bootstrap?0:LOG_COL, c->base.id);
	if (ok == LOG_OK) {
		assert(delta->next == NULL);
		ok = tr_merge_delta(tr, delta);
		c->t->base.flags = 0;
	}
	return ok;
}

/* will be called for new idx's and when new index columns are created */
static int
create_idx(sql_trans *tr, sql_idx *ni)
{
	int ok = LOG_OK;
	sql_delta *bat = ni->data;
	int type = TYPE_lng;

	if (oid_index(ni->type))
		type = TYPE_oid;

	if (!bat) {
		ni->data = bat = ZNEW(sql_delta);
		if(!bat)
			return LOG_ERR;
	}
	if (!bat->name) {
		bat->name = sql_message("%s_%s@%s", ni->t->s->base.name, ni->t->base.name, ni->base.name);
		if(!bat->name)
			ok = LOG_ERR;
	}

	if (inTransaction(tr, ni) && !isTempTable(ni->t)){
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &ni->base;
		change->type = idx_obj;
		change->kind = kind_create;
		change->log = &log_create_idx;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
		bat->ts = tr->tid;
	}
	if (!inTransaction(tr, ni) && !isTempTable(ni->t)){
		return load_bat(tr, bat, type, ni->t->bootstrap?0:LOG_IDX, ni->base.id);
	} else if (bat && bat->ibid && !isTempTable(ni->t)) {
		return new_persistent_bat( tr, ni->data, ni->t->sz);
	} else if (!bat->ibid) {
		sql_column *c = ni->t->columns.set->h->data;
		sql_delta *d;

		if (!c->data) {
			sql_column *oc = tr_find_column(tr->parent, c);
			c->data = timestamp_delta(oc->data, tr->tid, tr->ts);
		}
		d = c->data;
		/* Here we also handle indices created through alter stmts */
		/* These need to be created aligned to the existing data */


		if (d->bid) {
			bat->bid = copyBat(d->bid, type, 0);
			if(bat->bid == BID_NIL)
				ok = LOG_ERR;
		}
		if (d->ibid) {
			bat->ibid = copyBat(d->ibid, type, d->ibase);
			if(bat->ibid == BID_NIL)
				ok = LOG_ERR;
		}
		bat->ibase = d->ibase;
		bat->cnt = d->cnt;
		bat->ucnt = 0;

		if (d->uibid) {
			bat->uibid = e_bat(TYPE_oid);
			if (bat->uibid == BID_NIL)
				ok = LOG_ERR;
		}
		if (d->uvbid) {
			bat->uvbid = e_bat(type);
			if(bat->uvbid == BID_NIL)
				ok = LOG_ERR;
		}
	}
	return ok;
}

static int
upgrade_idx(sql_trans *tr, sql_idx *i)
{
	sql_delta *bat = i->data;

	if (!i->t->bootstrap && bat != NULL)
		return upgrade_delta(tr, bat, LOG_IDX, i->base.id);
	return LOG_OK;
}

static int
log_create_idx(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_idx *i = (sql_idx*)change->obj;
	assert(!isTempTable(i->t));
	sql_delta *delta = i->data;
	assert(delta->ts == tr->tid);
	delta->ts = commit_ts;
	(void)oldest;
	ok = log_create_delta(tr, i->data, i->t->bootstrap?0:LOG_IDX, i->base.id);
	if (ok == LOG_OK) {
		assert(delta->next == NULL);
		ok = tr_merge_delta(tr, delta);
		i->t->base.flags = 0;
	}
	return ok;
}

static int
load_dbat(sql_dbat *bat, int bid)
{
	BAT *b = quick_descriptor(bid);
	if(b) {
		assert(ATOMtype(b->ttype) == TYPE_oid);
		bat->dbid = temp_create(b);
		bat->cnt = BATcount(b);
		return LOG_OK;
	} else {
		return LOG_ERR;
	}
}

static int
create_del(sql_trans *tr, sql_table *t)
{
	sqlstore *store = tr->store;

	int ok = LOG_OK;
	BAT *b;
	sql_dbat *bat = t->data;

	if (!bat) {
		t->data = bat = ZNEW(sql_dbat);
		if(!bat)
			return LOG_ERR;
	}
	if (!bat->dname) {
		bat->dname = sql_message("D_%s_%s", t->s->base.name, t->base.name);
		if(!bat->dname)
			ok = LOG_ERR;
	}
	if (inTransaction(tr, t) && !isTempTable(t)){
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &t->base;
		change->type = table_obj;
		change->kind = kind_create;
		change->log = &log_create_del;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
		bat->ts = tr->tid;
	}
	if (!inTransaction(tr, t) && !isTempTable(t)) {
		log_bid bid = logger_find_bat(store->logger, bat->dname, t->bootstrap?0:LOG_TAB, t->base.id);

		if (bid) {
			return load_dbat(bat, bid);
		}
		ok = LOG_ERR;
	} else if (bat->dbid && !isTempTable(t)) {
		return ok;
	} else if (!bat->dbid) {
		b = bat_new(TYPE_oid, t->sz, PERSISTENT);
		if(b != NULL) {
			bat_set_access(b, BAT_READ);
			bat->dbid = temp_create(b);
			bat_destroy(b);
		} else {
			ok = LOG_ERR;
		}
	}
	return ok;
}

static int
upgrade_del(sql_trans *tr, sql_table *t)
{
	sql_dbat *bat = t->data;

	if (!t->bootstrap) {
		sqlstore *store = tr->store;
		return logger_upgrade_bat(store->logger, bat->dname, LOG_TAB, t->base.id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	}
	return LOG_OK;
}

static int
log_create_dbat(sql_trans *tr, sql_dbat *bat, char tpe, oid id)
{
	BAT *b;
	gdk_return ok;

	if (GDKinmemory(0))
		return LOG_OK;

	b = temp_descriptor(bat->dbid);
	if (b == NULL)
		return LOG_ERR;

	sqlstore *store = tr->store;
	ok = logger_add_bat(store->logger, b, bat->dname, tpe, id);
	if (ok == GDK_SUCCEED)
		ok = log_bat_persists(store->logger, b, bat->dname, tpe, id);
	bat_destroy(b);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_create_del(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sql_table *t = (sql_table*)change->obj;
	assert(!isTempTable(t));
	sql_dbat *delta = t->data;
	assert(delta->ts == tr->tid);
	delta->ts = commit_ts;
	(void)oldest;
	return log_create_dbat(tr, t->data, t->bootstrap?0:LOG_TAB, t->base.id);
}

static int
log_destroy_delta(sql_trans *tr, sql_delta *b, char tpe, oid id)
{
	log_bid bid;
	gdk_return ok = GDK_SUCCEED;

	(void)tr;
	sqlstore *store = tr->store;
	if (!GDKinmemory(0) &&
	    b &&
	    b->bid &&
	    b->name &&
	    (ok = log_bat_transient(store->logger, b->name, tpe, id)) == GDK_SUCCEED &&
	    (bid = logger_find_bat(store->logger, b->name, tpe, id)) != 0) {
		ok = logger_del_bat(store->logger, bid);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
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
destroy_bat(sql_delta *b)
{
	sql_delta *n;

	while(b) {
		n = b->next;
		destroy_delta(b);
		_DELETE(b);
		b = n;
	}
	return LOG_OK;
}

static int
destroy_col(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	sql_delta *delta = c->data;

	assert(delta);
	if (inTransaction(tr, c) && !isTempTable(c->t)){
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &c->base;
		change->type = column_obj;
		change->kind = kind_destroy;
		change->log = &log_destroy_col;
		change->data = delta;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
		delta->ts = tr->tid;
	}
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_column *c = (sql_column*)change->obj;
	assert(!isTempTable(c->t));
	sql_delta *delta = c->data;
	assert(delta->ts == tr->tid);
	delta->ts = commit_ts;
	ok = log_destroy_delta(tr, c->data, c->t->bootstrap?0:LOG_COL, c->base.id);
	if (ok == LOG_OK) {
		(void)oldest;
		assert(delta->next == NULL);
		ok = destroy_bat(delta);
	}
	return ok;
}

static int
destroy_idx(sql_trans *tr, sql_idx *i)
{
	int ok = LOG_OK;
	sql_delta *delta = i->data;

	assert(delta);
	if (inTransaction(tr, i) && !isTempTable(i->t)){
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &i->base;
		change->type = idx_obj;
		change->kind = kind_destroy;
		change->log = &log_destroy_idx;
		change->data = delta;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
		delta->ts = tr->tid;
	}
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_idx *i = (sql_idx*)change->obj;
	assert(!isTempTable(i->t));
	sql_delta *delta = i->data;
	assert(delta->ts == tr->tid);
	delta->ts = commit_ts;
	ok = log_destroy_delta(tr, i->data, i->t->bootstrap?0:LOG_IDX, i->base.id);
	if (ok == LOG_OK) {
		(void)oldest;
		assert(delta->next == NULL);
		ok = destroy_bat(delta);
	}
	return ok;
}

static int
cleanup(void)
{
	int ok = LOG_OK;

	MT_lock_set(&destroy_lock);
	if (tobe_destroyed_delta) {
		ok = destroy_bat(tobe_destroyed_delta);
		tobe_destroyed_delta = NULL;
	}
	if (ok == LOG_OK && tobe_destroyed_dbat) {
		ok = destroy_dbat(NULL, tobe_destroyed_dbat);
		tobe_destroyed_dbat = NULL;
	}
	MT_lock_unset(&destroy_lock);
	return ok;
}

static int
destroy_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	sql_dbat *dbat = t->data;

	if (inTransaction(tr, t) && !isTempTable(t)){
		sql_change *change = SA_ZNEW(tr->sa, sql_change);
		change->obj = &t->base;
		change->type = table_obj;
		change->kind = kind_destroy;
		change->log = &log_destroy_del;
		change->data = dbat;
		tr->changes = sa_list_append(tr->sa, tr->changes, change);
		dbat->ts = tr->tid;
	}
	return ok;
}

static int
log_destroy_dbat(sql_trans *tr, sql_dbat *bat, char tpe, oid id)
{
	log_bid bid;
	gdk_return ok = GDK_SUCCEED;

	(void)tr;
	sqlstore *store = tr->store;
	if (!GDKinmemory(0) &&
	    bat &&
	    bat->dbid &&
	    bat->dname &&
	    (ok = log_bat_transient(store->logger, bat->dname, tpe, id)) == GDK_SUCCEED &&
	    (bid = logger_find_bat(store->logger, bat->dname, tpe, id)) != 0) {
		ok = logger_del_bat(store->logger, bid);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_destroy_del(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;
	assert(!isTempTable(t));
	sql_dbat *dbat = t->data;
	assert(dbat->ts == tr->tid);
	dbat->ts = commit_ts;
	ok = log_destroy_dbat(tr, t->data, t->bootstrap?0:LOG_TAB, t->base.id);
	if (ok == LOG_OK) {
		(void)oldest;
		assert(dbat->next == NULL);
		ok = destroy_dbat(tr, dbat);
	}
	return ok;
}

static BUN
clear_delta(sql_trans *tr, sql_delta *bat)
{
	BAT *b;
	BUN sz = 0;
	int isnew = 0;

	(void)tr;
	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	if (bat->ibid) {
		b = temp_descriptor(bat->ibid);
		if (b && !isEbat(b)) {
			sz += BATcount(b);
			bat_clear(b);
			BATcommit(b);
		}
		bat_destroy(b);
	}
	if (bat->bid) {
		b = temp_descriptor(bat->bid);
		if (b) {
			assert(!isEbat(b));
			sz += BATcount(b);
			/* for transactions we simple switch to ibid only */
			temp_destroy(bat->bid);
			bat->bid = 0;
			bat_destroy(b);
		}
	} else {
		isnew = 1;
	}
	if (bat->uibid) {
		b = temp_descriptor(bat->uibid);
		if (b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b);
		}
		bat_destroy(b);
	}
	if (bat->uvbid) {
		b = temp_descriptor(bat->uvbid);
		if(b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b);
		}
		bat_destroy(b);
	}
	if (!isnew)
		bat->cleared = 1;
	bat->ibase = 0;
	bat->cnt = 0;
	bat->ucnt = 0;
	return sz;
}

static BUN
clear_col(sql_trans *tr, sql_column *c)
{
	sql_delta *delta;

	if ((delta = bind_col_data(tr, c)) == NULL)
		return BUN_NONE;
	if (delta)
		return clear_delta(tr, delta);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	sql_delta *delta;

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if ((delta = bind_idx_data(tr, i)) == NULL)
		return BUN_NONE;
	if (delta)
		return clear_delta(tr, delta);
	return 0;
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

		if (b && !isEbat(b)) {
			sz += BATcount(b);
			bat_clear(b);
			BATcommit(b);
			bat_destroy(b);
		}
	}
	bat->cleared = 1;
	bat->cnt = 0;
	return sz;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	sql_dbat *bat;

	if ((bat = bind_del_data(tr, t)) == NULL)
		return BUN_NONE;
	return clear_dbat(tr, bat);
}

static int
tr_log_delta( sql_trans *tr, sql_delta *cbat, int cleared, char tpe, oid id)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;
	BAT *ins;

	(void)tr;
	if (GDKinmemory(0))
		return LOG_OK;

	ins = temp_descriptor(cbat->ibid);
	if (ins == NULL)
		return LOG_ERR;

	if (cleared && log_bat_clear(store->logger, cbat->name, tpe, id) != GDK_SUCCEED) {
		bat_destroy(ins);
		return LOG_ERR;
	}

	/* any inserts */
	if (BUNlast(ins) > 0) {
		assert(ATOMIC_GET(&store->nr_active)>0);
		if (BUNlast(ins) > ins->batInserted &&
		    (cbat->ibase || BATcount(ins) <= SNAPSHOT_MINSIZE))
			ok = log_bat(store->logger, ins, cbat->name, tpe, id);
		if (ok == GDK_SUCCEED &&
		    !cbat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE) {
			/* log new snapshot */
			if ((ok = logger_add_bat(store->logger, ins, cbat->name, tpe, id)) == GDK_SUCCEED)
				ok = log_bat_persists(store->logger, ins, cbat->name, tpe, id);
		}
	}
	bat_destroy(ins);

	if (ok == GDK_SUCCEED && cbat->ucnt && cbat->uibid) {
		BAT *ui = temp_descriptor(cbat->uibid);
		BAT *uv = temp_descriptor(cbat->uvbid);
		/* any updates */
		if (ui == NULL || uv == NULL) {
			ok = GDK_FAIL;
		} else if (BUNlast(uv) > uv->batInserted || BATdirty(uv))
			ok = log_delta(store->logger, ui, uv, cbat->name, tpe, id);
		bat_destroy(ui);
		bat_destroy(uv);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
tr_log_dbat(sql_trans *tr, sql_dbat *fdb, int cleared, char tpe, oid id)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;
	BAT *db = NULL;

	if (!fdb || GDKinmemory(0))
		return LOG_OK;

	(void)tr;
	assert (fdb->dname);
	if (cleared && log_bat_clear(store->logger, fdb->dname, tpe, id) != GDK_SUCCEED)
		return LOG_ERR;

	db = temp_descriptor(fdb->dbid);
	if(!db)
		return LOG_ERR;
	if (BUNlast(db) > 0) {
		assert(ATOMIC_GET(&store->nr_active)>0);
		if (BUNlast(db) > db->batInserted)
			ok = log_bat(store->logger, db, fdb->dname, tpe, id);
	}
	bat_destroy(db);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
tr_merge_delta( sql_trans *tr, sql_delta *obat)
{
	sqlstore *store = tr->store;
	int ok = LOG_OK;
	BAT *ins, *cur = NULL;

	(void)store;
	assert(!store->initialized || ATOMIC_GET(&store->nr_active)==1);
	assert(!obat->cleared);

	if (obat->bid == 0) {
		cur = temp_descriptor(obat->ibid);
		obat->bid = obat->ibid;
		obat->ibase = obat->cnt;
		obat->ibid = e_bat(cur->ttype);
		obat->cleared = 0;
		assert(BATcount(cur) == obat->cnt);
		bat_destroy(cur);
		return ok;
	}

	if (obat->cached) {
		bat_destroy(obat->cached);
		obat->cached = NULL;
	}
	if (obat->bid) {
		cur = temp_descriptor(obat->bid);
		if(!cur)
			return LOG_ERR;
	}
	ins = temp_descriptor(obat->ibid);
	if(!ins) {
		bat_destroy(cur);
		return LOG_ERR;
	}
	if (BATcount(ins) > 0) {
		if (BATappend(cur, ins, NULL, true) != GDK_SUCCEED) {
			bat_destroy(cur);
			bat_destroy(ins);
			return LOG_ERR;
		}
		obat->cnt = obat->ibase = BATcount(cur);
		temp_destroy(obat->ibid);
		obat->ibid = e_bat(cur->ttype);
		if (obat->ibid == BID_NIL)
			ok = LOG_ERR;
	}
	bat_destroy(ins);

	if (obat->ucnt) {
		BAT *ui = temp_descriptor(obat->uibid);
		BAT *uv = temp_descriptor(obat->uvbid);

		if(!ui || !uv) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			return LOG_ERR;
		}

		/* any updates */
		assert(!isEbat(cur));
		if (BUNlast(ui) > 0) {
			if (BATreplace(cur, ui, uv, true) != GDK_SUCCEED) {
				bat_destroy(ui);
				bat_destroy(uv);
				bat_destroy(cur);
				return LOG_ERR;
			}
			/* cleanup the old deltas */
			temp_destroy(obat->uibid);
			temp_destroy(obat->uvbid);
			obat->uibid = e_bat(TYPE_oid);
			obat->uvbid = e_bat(cur->ttype);
			if(obat->uibid == BID_NIL || obat->uvbid == BID_NIL)
				ok = LOG_ERR;
			obat->ucnt = 0;
		}
		bat_destroy(ui);
		bat_destroy(uv);
	}
	assert(obat->cnt == BATcount(quick_descriptor(obat->bid)));
	obat->cleared = 0;
	bat_destroy(cur);
	return ok;
}

static int
tr_merge_dbat(sql_trans *tr, sql_dbat *tdb)
{
	sqlstore *store = tr->store;
	int ok = LOG_OK;

	if (tdb->cached) {
		bat_destroy(tdb->cached);
		tdb->cached = NULL;
	}
	assert(ATOMIC_GET(&store->nr_active)==1);
	if (tdb->next) {
		ok = destroy_dbat(tr, tdb->next);
		tdb->next = NULL;
	}
	return ok;
}

static int
log_update_col( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_column *c = (sql_column*)change->obj;
	sql_delta *delta = c->data;
	delta->ts = commit_ts;
	ok = tr_log_delta(tr, c->data, c->t->cleared, c->t->bootstrap?0:LOG_COL, c->base.id);
	if (ok == LOG_OK) {
		sql_delta *d = delta;
		/* clean up and merge deltas */
		while (delta && delta->ts > oldest) {
			delta = delta->next;
		}
		if (delta && delta != d) {
			if (delta->next) {
				ok = destroy_bat(delta->next);
				delta->next = NULL;
			}
		}
		if (ok == LOG_OK && delta == d && oldest == commit_ts)
			ok = tr_merge_delta(tr, delta);
	}
	return ok;
}

static int
log_update_idx( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_idx *i = (sql_idx*)change->obj;
	sql_delta *delta = i->data;
	delta->ts = commit_ts;
	ok = tr_log_delta(tr, i->data, i->t->cleared, i->t->bootstrap?0:LOG_COL, i->base.id);
	if (ok == LOG_OK) {
		sql_delta *d = delta;
		/* clean up and merge deltas */
		while (delta && delta->ts > oldest) {
			delta = delta->next;
		}
		if (delta && delta != d) {
			if (delta->next) {
				ok = destroy_bat(delta->next);
				delta->next = NULL;
			}
		}
		if (ok == LOG_OK && delta == d && oldest == commit_ts)
			ok = tr_merge_delta(tr, delta);
	}
	return ok;
}

static int
log_update_del( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;
	sql_dbat *dbat = t->data;
	dbat->ts = commit_ts;
	ok = tr_log_dbat(tr, t->data, t->cleared, t->bootstrap?0:LOG_TAB, t->base.id);
	if (ok == LOG_OK) {
		sql_dbat *d = dbat;
		/* clean up and merge deltas */
		while (dbat && dbat->ts > oldest) {
			dbat = dbat->next;
		}
		if (dbat && dbat != d) {
			if (dbat->next) {
				ok = destroy_dbat(tr, dbat->next);
				dbat->next = NULL;
			}
		}
		if (ok == LOG_OK && dbat == d && oldest == commit_ts)
			ok = tr_merge_dbat(tr, dbat);
	}
	return ok;
}


void
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
	sf->count_upd = (count_upd_fptr)&count_upd;
	sf->count_col = (count_col_fptr)&count_col;
	sf->count_col_upd = (count_col_upd_fptr)&count_col_upd;
	sf->count_idx = (count_idx_fptr)&count_idx;
	sf->dcount_col = (dcount_col_fptr)&dcount_col;
	sf->sorted_col = (prop_col_fptr)&sorted_col;
	sf->unique_col = (prop_col_fptr)&unique_col;
	sf->double_elim_col = (prop_col_fptr)&double_elim_col;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->upgrade_col = (upgrade_col_fptr)&upgrade_col;
	sf->upgrade_idx = (upgrade_idx_fptr)&upgrade_idx;
	sf->upgrade_del = (upgrade_del_fptr)&upgrade_del;

	sf->destroy_col = (destroy_col_fptr)&destroy_col;
	sf->destroy_idx = (destroy_idx_fptr)&destroy_idx;
	sf->destroy_del = (destroy_del_fptr)&destroy_del;

	sf->clear_col = (clear_col_fptr)&clear_col;
	sf->clear_idx = (clear_idx_fptr)&clear_idx;
	sf->clear_del = (clear_del_fptr)&clear_del;

	sf->cleanup = (cleanup_fptr)&cleanup;
}
