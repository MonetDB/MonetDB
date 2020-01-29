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
#include "algebra.h"
#include "gdk_atoms.h"

#define SNAPSHOT_MINSIZE ((BUN) 1024*128)

static MT_Lock destroy_lock = MT_LOCK_INITIALIZER("destroy_lock");
sql_dbat *tobe_destroyed_dbat = NULL;
sql_delta *tobe_destroyed_delta = NULL;

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
	return d;
}

sql_dbat *
timestamp_dbat( sql_dbat *d, int ts)
{
	while (d->next && d->wtime > ts) 
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
	assert(tr == gtrans || access == QUICK || tr->active);
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, t->base.stime);
	}
	assert(!store_initialized || tr != gtrans);
	t->s->base.rtime = t->base.rtime = tr->stime;
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

	assert(tr == gtrans || tr->active);
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, c->base.stime);
	}
	if (!c->t->data) {
		sql_table *ot = tr_find_table(tr->parent, c->t);
		c->t->data = timestamp_dbat(ot->data, c->t->base.stime);
	}
	assert(tr != gtrans);
	c->t->s->base.rtime = c->t->base.rtime = c->base.rtime = tr->stime;
	u = delta_bind_ubat(c->data, access, c->type.type->localtype);
	return u;
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	BAT *u = NULL;

	assert(tr == gtrans || tr->active);
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, i->base.stime);
	}
	if (!i->t->data) {
		sql_table *ot = tr_find_table(tr->parent, i->t);
		i->t->data = timestamp_dbat(ot->data, i->t->base.stime);
	}
	assert(tr != gtrans);
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
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
	assert(tr == gtrans || access == QUICK || tr->active);
	if (!isTable(c->t)) 
		return NULL;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, c->base.stime);
	}
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_ucol(tr, c, access);
	assert(access == QUICK || tr != gtrans);
	if (tr && access != QUICK)
		c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( c->data, access, isTemp(c));
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	assert(tr == gtrans || access == QUICK || tr->active);
	if (!isTable(i->t)) 
		return NULL;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, i->base.stime);
	}
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_uidx(tr, i, access);
	assert(access == QUICK || tr != gtrans);
	if (tr && access != QUICK)
		i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->rtime = tr->stime;
	return delta_bind_bat( i->data, access, isTemp(i));
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
			if (c_isnew && tr->parent == gtrans) { 
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
			obat->uvbid = e_bat(type);
			if (bat->uibid == BID_NIL || obat->uvbid == BID_NIL)
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
		sql_delta* bat = ZNEW(sql_delta),*obat;
		if(!bat)
			return LOG_ERR;
		c->data = bat;
		obat = timestamp_delta(oc->data, c->base.stime);
		if(dup_bat(tr, c->t, obat, bat, type, isNew(oc), isNew(c)) == LOG_ERR)
			return LOG_ERR;
		c->base.allocated = 1;
	}
	bat = c->data;
	bat->wtime = c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->wstime;
	assert(tr != gtrans);
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
		int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta* bat = ZNEW(sql_delta), *obat;
		if(!bat)
			return LOG_ERR;
		i->data = bat;
		obat = timestamp_delta(oi->data, i->base.stime);
		if(dup_bat(tr, i->t, obat, bat, type, isNew(i), isNew(i)) == LOG_ERR)
			return LOG_ERR;
		i->base.allocated = 1;
	}
	bat = i->data;
	bat->wtime = i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->wstime;
	assert(tr != gtrans);
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
				if(ic)
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
dup_col(sql_trans *tr, sql_column *oc, sql_column *c )
{
	int ok = LOG_OK;

	if (oc->data) {
		int type = c->type.type->localtype;
		sql_delta *bat = ZNEW(sql_delta), *obat = oc->data;
		if (!bat)
			ok = LOG_ERR;
		else {
			c->data = bat;
			ok = dup_bat(tr, c->t, obat, bat, type, isNew(oc), isNew(c));
			c->base.allocated = 1;
		}
	}
	return ok;
}

static int 
dup_idx(sql_trans *tr, sql_idx *i, sql_idx *ni )
{
	int ok = LOG_OK;

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return ok;
	if (i->data) {
		int type = (oid_index(ni->type))?TYPE_oid:TYPE_lng;
		sql_delta *bat = ZNEW(sql_delta), *obat = i->data;
		if (!bat)
			ok = LOG_ERR;
		else {
			ni->data = bat;
			ok = dup_bat(tr, ni->t, obat, bat, type, isNew(i), isNew(ni));
			ni->base.allocated = 1;
		}
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
dup_del(sql_trans *tr, sql_table *ot, sql_table *t)
{
	int ok;
	sql_dbat *bat = ZNEW(sql_dbat), *obat = ot->data;
	if (!bat)
		return LOG_ERR;
	t->data = bat;
	ok = dup_dbat( tr, obat, bat, isNew(t), isTempTable(t));
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
		sql_delta *bat = ZNEW(sql_delta), *obat;
		if (!bat)
			ok = LOG_ERR;
		else {
			c->data = bat;
			obat = timestamp_delta(oc->data, c->base.stime);
			ok = dup_bat(tr, c->t, obat, bat, type, isNew(oc), isNew(c));
			if(ok == LOG_OK)
				c->base.allocated = 1;
		}
	}

	if(ok == LOG_ERR)
		return ok;

	bat = c->data;
	/* appends only write */
	bat->wtime = c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->wstime;
	/* inserts are ordered with the current delta implementation */
	/* therefor mark appends as reads */
	assert(tr != gtrans);
	c->t->s->base.rtime = c->t->base.rtime = tr->stime;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(bat, i);
	else
		ok = delta_append_val(bat, i);
	/*
	if (!c->t->data || !c->t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, c->t);
		sql_dbat *bat = ZNEW(sql_dbat), *obat;
		if (!bat)
			return LOG_ERR;
		c->t->data = bat;
		obat = timestamp_dbat(ot->data, c->t->base.stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(c->t));
		c->t->base.allocated = 1;
	}
	if (c->t && c->t->data && ((sql_dbat*)c->t->data)->cached) {
		sql_dbat *bat = c->t->data;

		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	*/
	return ok;
}

static int
append_idx(sql_trans *tr, sql_idx * i, void *ib, int tpe)
{
	int ok = LOG_OK;
	BAT *b = ib;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b)) 
		return ok;

	if (!i->data || !i->base.allocated) {
		int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta *bat = ZNEW(sql_delta), *obat;
		if(!bat)
			ok = LOG_ERR;
		else {
			i->data = bat;
			obat = timestamp_delta(oi->data, i->base.stime);
			ok = dup_bat(tr, i->t, obat, bat, type, isNew(oi), isNew(i));
			if(ok == LOG_OK)
				i->base.allocated = 1;
		}
	}

	if(ok == LOG_ERR)
		return ok;

	bat = i->data;
	/* appends only write */
	bat->wtime = i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->wstime;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(bat, ib);
	else
		ok = delta_append_val(bat, ib);
	/*
	if (!i->t->data || !i->t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, i->t);
		sql_dbat *bat = ZNEW(sql_dbat), *obat;
		if(!bat)
			return LOG_ERR;
		i->t->data = bat;
		obat = timestamp_dbat(ot->data, i->t->base.stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(i->t));
		i->t->base.allocated = 1;
	}

	if (i->t && i->t->data && ((sql_dbat*)i->t->data)->cached) {
		sql_dbat *bat = i->t->data;

		bat_destroy(bat->cached);
		bat->cached = NULL;
	}
	*/
	return ok;
}

static int
delta_delete_bat( sql_dbat *bat, BAT *i ) 
{
	BAT *b = temp_descriptor(bat->dbid);

	if(!b)
		return LOG_ERR;

	if (isEbat(b)) {
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
		sql_dbat *bat = ZNEW(sql_dbat), *obat;
		if(!bat)
			return LOG_ERR;
		t->data = bat;
		obat = timestamp_dbat(ot->data, t->base.stime);
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
			c->data = timestamp_delta(oc->data, c->base.stime);
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
				i->data = timestamp_delta(oi->data, i->base.stime);
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
		c->data = timestamp_delta(oc->data, c->base.stime);
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
		c->data = timestamp_delta(oc->data, c->base.stime);
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

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type)) 
		return 0;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, i->base.stime);
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
		t->data = timestamp_dbat(ot->data, t->base.stime);
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

	assert (isTable(c->t)) ;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, c->base.stime);
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

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type)) 
		return 0;
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		if (oi)
			i->data = timestamp_delta(oi->data, i->base.stime);
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
load_bat(sql_delta *bat, int type, char tpe, oid id) 
{
	int bid = logger_find_bat(bat_logger, bat->name, tpe, id);

	return load_delta(bat, bid, type);
}

static int
log_create_delta(sql_delta *bat, char tpe, oid id) 
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
	if (GDKinmemory())
		return res;

	ok = logger_add_bat(bat_logger, b, bat->name, tpe, id);
	if (ok == GDK_SUCCEED)
		ok = log_bat_persists(bat_logger, b, bat->name, tpe, id);
	bat_destroy(b);
	if(res != LOG_OK)
		return res;
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
snapshot_new_persistent_bat(sql_trans *tr, sql_delta *bat) 
{
	int ok = LOG_OK;
	BAT *b = (bat->bid)?
			temp_descriptor(bat->bid):
			temp_descriptor(bat->ibid);

	if (b == NULL)
		return LOG_ERR;

	(void)tr;
	/* snapshot large bats */
	bat_set_access(b, BAT_READ);
	if (BATcount(b) > SNAPSHOT_MINSIZE)
		BATmode(b, false);
	bat_destroy(b);
	return ok;
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
upgrade_delta( sql_delta *d, char tpe, oid id)
{
	return logger_upgrade_bat(bat_logger, d->name, tpe, id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
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

	if (!bat || !c->base.allocated) {
		c->data = bat = ZNEW(sql_delta);
		if(!bat)
			return LOG_ERR;
		bat->wtime = c->base.wtime = tr->wstime;
		c->base.allocated = 1;
	}
	if (!bat->name) {
		bat->name = sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name);
		if(!bat->name)
			ok = LOG_ERR;
	}

	if (!isNew(c) && !isTempTable(c->t)){
		c->base.wtime = 0;
		return load_bat(bat, type, c->t->bootstrap?0:LOG_COL, c->base.id);
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
upgrade_col(sql_column *c)
{
	sql_delta *bat = c->data;

	if (!c->t->bootstrap)
		return upgrade_delta(bat, LOG_COL, c->base.id);
	return LOG_OK;
}

static int
log_create_col(sql_trans *tr, sql_column *c)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(c->t));
	return log_create_delta( c->data, c->t->bootstrap?0:LOG_COL, c->base.id);
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
	int type = TYPE_lng;

	if (oid_index(ni->type))
		type = TYPE_oid;

	if (!bat || !ni->base.allocated) {
		ni->data = bat = ZNEW(sql_delta);
		if(!bat)
			return LOG_ERR;
		bat->wtime = ni->base.wtime = tr->wstime;
		ni->base.allocated = 1;
	}
	if (!bat->name) {
		bat->name = sql_message("%s_%s@%s", ni->t->s->base.name, ni->t->base.name, ni->base.name);
		if(!bat->name)
			ok = LOG_ERR;
	}

	if (!isNew(ni) && !isTempTable(ni->t)){
		ni->base.wtime = 0;
		return load_bat(bat, type, ni->t->bootstrap?0:LOG_IDX, ni->base.id);
	} else if (bat && bat->ibid && !isTempTable(ni->t)) {
		return new_persistent_bat( tr, ni->data, ni->t->sz);
	} else if (!bat->ibid) {
		sql_column *c = ni->t->columns.set->h->data;
		sql_delta *d;
	       
		if (!c->data) {
			sql_column *oc = tr_find_column(tr->parent, c);
			c->data = timestamp_delta(oc->data, c->base.stime);
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
upgrade_idx(sql_idx *i)
{
	sql_delta *bat = i->data;

	if (!i->t->bootstrap && bat != NULL)
		return upgrade_delta(bat, LOG_IDX, i->base.id);
	return LOG_OK;
}

static int
log_create_idx(sql_trans *tr, sql_idx *ni)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return log_create_delta( ni->data, ni->t->bootstrap?0:LOG_IDX, ni->base.id);
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
	if(b) {
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
	int ok = LOG_OK;
	BAT *b;
	sql_dbat *bat = t->data;

	if (!bat) {
		t->data = bat = ZNEW(sql_dbat);
		if(!bat)
			return LOG_ERR;
		bat->wtime = t->base.wtime = t->s->base.wtime = tr->wstime;
		t->base.allocated = 1;
	}
	if (!bat->dname) {
		bat->dname = sql_message("D_%s_%s", t->s->base.name, t->base.name);
		if(!bat->dname)
			ok = LOG_ERR;
	}
	(void)tr;
	if (!isNew(t) && !isTempTable(t)) {
		log_bid bid = logger_find_bat(bat_logger, bat->dname, t->bootstrap?0:LOG_TAB, t->base.id);

		if (bid) {
			t->base.wtime = 0;
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
upgrade_del(sql_table *t)
{
	sql_dbat *bat = t->data;

	if (!t->bootstrap)
		return logger_upgrade_bat(bat_logger, bat->dname, LOG_TAB, t->base.id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static int
log_create_dbat( sql_dbat *bat, char tpe, oid id)
{
	BAT *b;
	gdk_return ok;

	if (GDKinmemory())
		return LOG_OK;

	b = temp_descriptor(bat->dbid);
	if (b == NULL)
		return LOG_ERR;

	ok = logger_add_bat(bat_logger, b, bat->dname, tpe, id);
	if (ok == GDK_SUCCEED)
		ok = log_bat_persists(bat_logger, b, bat->dname, tpe, id);
	bat_destroy(b);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_create_del(sql_trans *tr, sql_table *t)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(t));
	return log_create_dbat(t->data, t->bootstrap?0:LOG_TAB, t->base.id);
}

static int
snapshot_create_del(sql_trans *tr, sql_table *t)
{
	sql_dbat *bat = t->data;
	BAT *b = temp_descriptor(bat->dbid);

	if (b == NULL)
		return LOG_ERR;

	(void)tr;
	/* snapshot large bats */
	bat_set_access(b, BAT_READ);
	if (BATcount(b) > SNAPSHOT_MINSIZE) 
		BATmode(b, false);
	bat_destroy(b);
	return LOG_OK;
}

static int
log_destroy_delta(sql_trans *tr, sql_delta *b, char tpe, oid id)
{
	log_bid bid;
	gdk_return ok = GDK_SUCCEED;

	(void)tr;
	if (!GDKinmemory() &&
	    b &&
	    b->bid &&
	    b->name &&
	    (ok = log_bat_transient(bat_logger, b->name, tpe, id)) == GDK_SUCCEED &&
	    (bid = logger_find_bat(bat_logger, b->name, tpe, id)) != 0) {
		ok = logger_del_bat(bat_logger, bid);
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
       
	(void)tr;
	if (c->data && c->base.allocated) {
		c->base.allocated = 0;
		ok = destroy_bat(c->data);
	}
	c->data = NULL;
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_column *c)
{
	if (c->data && c->base.allocated)
		return log_destroy_delta(tr, c->data, c->t->bootstrap?0:LOG_COL, c->base.id);
	return LOG_OK;
}

static int
destroy_idx(sql_trans *tr, sql_idx *i)
{
	int ok = LOG_OK;

	(void)tr;
	if (i->data && i->base.allocated) {
		i->base.allocated = 0;
       		ok = destroy_bat(i->data);
	}
	i->data = NULL;
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_idx *i)
{
	if (i->data && i->base.allocated)
		return log_destroy_delta(tr, i->data, i->t->bootstrap?0:LOG_IDX, i->base.id);
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
delayed_destroy_bat(sql_delta *b)
{
	sql_delta *n = b;

	if (!n)
		return LOG_OK;
	while(n->next) 
		n = n->next;
	MT_lock_set(&destroy_lock);
	n->next = tobe_destroyed_delta;
	tobe_destroyed_delta = b;
	MT_lock_unset(&destroy_lock);
	return LOG_OK;
}

static int
delayed_destroy_dbat(sql_dbat *b)
{
	sql_dbat *n = b;

	if (!n)
		return LOG_OK;
	while(n->next) 
		n = n->next;
	MT_lock_set(&destroy_lock);
	n->next = tobe_destroyed_dbat;
	tobe_destroyed_dbat = b;
	MT_lock_unset(&destroy_lock);
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
log_destroy_dbat(sql_trans *tr, sql_dbat *bat, char tpe, oid id)
{
	log_bid bid;
	gdk_return ok = GDK_SUCCEED;

	(void)tr;
	if (!GDKinmemory() &&
	    bat &&
	    bat->dbid &&
	    bat->dname &&
	    (ok = log_bat_transient(bat_logger, bat->dname, tpe, id)) == GDK_SUCCEED &&
	    (bid = logger_find_bat(bat_logger, bat->dname, tpe, id)) != 0) {
		ok = logger_del_bat(bat_logger, bid);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_destroy_del(sql_trans *tr, sql_table *t)
{
	if (t->data && t->base.allocated)
		return log_destroy_dbat(tr, t->data, t->bootstrap?0:LOG_TAB, t->base.id);
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
		if (b && !isEbat(b)) {
			sz += BATcount(b);
			bat_clear(b);
			BATcommit(b);
		}
		if (b)
			bat_destroy(b);
	}
	if (bat->bid) {
		b = temp_descriptor(bat->bid);
		if (b) {
			assert(!isEbat(b));
			sz += BATcount(b);
			/* for transactions we simple switch to ibid only */
			if (tr != gtrans) {
				temp_destroy(bat->bid);
				bat->bid = 0;
			} else {
				bat_clear(b);
				BATcommit(b);
			}
			bat_destroy(b);
		}
	}
	if (bat->uibid) { 
		b = temp_descriptor(bat->uibid);
		if (b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b);
		}
		if (b)
			bat_destroy(b);
	}
	if (bat->uvbid) { 
		b = temp_descriptor(bat->uvbid);
		if(b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b);
		}
		if (b)
			bat_destroy(b);
	}
	bat->cleared = 1;
	bat->ibase = 0;
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
		sql_delta *bat = c->data = ZNEW(sql_delta), *obat;
		if(!bat)
			return 0;
		obat = timestamp_delta(oc->data, c->base.stime);
		assert(tr != gtrans);
		if(dup_bat(tr, c->t, obat, bat, type, isNew(oc), isNew(c)) == LOG_ERR)
			return 0;
		c->base.allocated = 1;
	}
	c->t->s->base.wtime = c->t->base.wtime = c->base.wtime = tr->wstime;
	if (c->data)
		return clear_delta(tr, c->data);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if (!i->data || !i->base.allocated) {
		int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta *bat = i->data = ZNEW(sql_delta), *obat;
		if(!bat)
			return 0;
		obat = timestamp_delta(oi->data, i->base.stime);
		if(dup_bat(tr, i->t, obat, bat, type, isNew(oi), isNew(i)))
			return 0;
		i->base.allocated = 1;
	}
	i->t->s->base.wtime = i->t->base.wtime = i->base.wtime = tr->wstime;
	if (i->data)
		return clear_delta(tr, i->data);
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
	bat->wtime = tr->wstime;
	return sz;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	if (!t->data || !t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, t);
		sql_dbat *bat = t->data = ZNEW(sql_dbat), *obat;
		if(!bat)
			return 0;
		obat = timestamp_dbat(ot->data, t->base.stime);
		dup_dbat(tr, obat, bat, isNew(ot), isTempTable(t)); 
		t->base.allocated = 1;
	}
	t->s->base.wtime = t->base.wtime = tr->wstime;
	return clear_dbat(tr, t->data);
}

static int 
gtr_update_delta( sql_trans *tr, sql_delta *cbat, int *changes, int id, int tpe)
{
	int ok = LOG_OK, cleared = 0;
	BAT *ins, *cur;

	(void)tr;
	assert(ATOMIC_GET(&store_nr_active)==0);
	
	if (!cbat->bid) {
		cleared = 1;
		cbat->bid = logger_find_bat(bat_logger, cbat->name, tpe, id);
		temp_dup(cbat->bid);
	}
	assert(cbat->bid == logger_find_bat(bat_logger, cbat->name, tpe, id));
	cur = temp_descriptor(cbat->bid);
	ins = temp_descriptor(cbat->ibid);

	if(cur == NULL || ins == NULL) {
		bat_destroy(ins);
		bat_destroy(cur);
		return LOG_ERR;
	}
	assert(!isEbat(cur));
	/* A snapshot column after being cleared */
	if (cbat->bid == cbat->ibid && cleared) {
		cbat->cnt = cbat->ibase = BATcount(cur);
		temp_destroy(cbat->ibid);
		cbat->ibid = e_bat(cur->ttype);
		if(cbat->ibid == BID_NIL)
			ok = LOG_ERR;
	} else
	/* any inserts */
	if (BUNlast(ins) > 0 || cbat->cleared) {
		(*changes)++;
		assert(cur->theap.storage != STORE_PRIV);
		if (cbat->cleared)
			bat_clear(cur);
		if (BATappend(cur, ins, NULL, true) != GDK_SUCCEED) {
			bat_destroy(ins);
			bat_destroy(cur);
			return LOG_ERR;
		}
		cbat->cnt = cbat->ibase = BATcount(cur);
		temp_destroy(cbat->ibid);
		cbat->ibid = e_bat(cur->ttype);
		if(cbat->ibid == BID_NIL)
			ok = LOG_ERR;
	}
	bat_destroy(ins);

	if (cbat->ucnt) {
		BAT *ui = temp_descriptor(cbat->uibid);
		BAT *uv = temp_descriptor(cbat->uvbid);
		if(ui == NULL || uv == NULL) {
			bat_destroy(cur);
			return LOG_ERR;
		}
		/* any updates */
		if (BUNlast(ui) > 0) {
			(*changes)++;
			if (BATreplace(cur, ui, uv, true) != GDK_SUCCEED) {
				bat_destroy(ui);
				bat_destroy(uv);
				bat_destroy(cur);
				return LOG_ERR;
			}
			temp_destroy(cbat->uibid);
			temp_destroy(cbat->uvbid);
			cbat->uibid = e_bat(TYPE_oid);
			cbat->uvbid = e_bat(cur->ttype);
			if(cbat->uibid == BID_NIL || cbat->uvbid == BID_NIL)
				ok = LOG_ERR;
			cbat->ucnt = 0;
		}
		bat_destroy(ui);
		bat_destroy(uv);
	}
	bat_destroy(cur);
	cbat->cleared = 0;
	if (cbat->next) { 
		ok = destroy_bat(cbat->next);
		cbat->next = NULL;
	}
	return ok;
}

static int
gtr_update_dbat(sql_trans *tr, sql_dbat *d, int *changes, char tpe, oid id)
{
	int ok = LOG_OK;
	BAT *idb, *cdb;
	int dbid = logger_find_bat(bat_logger, d->dname, tpe, id);

	assert(ATOMIC_GET(&store_nr_active)==0);
	if (d->dbid == dbid) {
		/* if set its handled by the bat clear of the dbid */
		d->cleared = 0;
		if (d->next) { 
			ok = destroy_dbat(tr, d->next);
			d->next = NULL;
		}
		return ok;
	}
	idb = temp_descriptor(d->dbid);
	if(!idb)
		return LOG_ERR;
	cdb = temp_descriptor(dbid);
	if(cdb) {
		(*changes)++;
		assert(!isEbat(cdb));
		if (d->cleared) {
			bat_clear(cdb);
			d->cnt = 0;
		}
		d->cnt = BATcount(cdb);
		idb->batInserted = d->cnt;
		if (append_inserted(cdb, idb) == BUN_NONE)
			ok = LOG_ERR;
		else
			BATcommit(cdb);
		d->cnt = BATcount(cdb);
		bat_destroy(cdb);
	} else {
		ok = LOG_ERR;
	}
	assert(BATcount(quick_descriptor(dbid)) == d->cnt);
	d->cleared = 0;
	temp_destroy(d->dbid);
	d->dbid = dbid;
	temp_dup(d->dbid);
	bat_destroy(idb);
	assert(BATcount(quick_descriptor(d->dbid)) == d->cnt);
	return ok;
}

static int
gtr_update_table(sql_trans *tr, sql_table *t, int *tchanges)
{
	int ok = LOG_OK;
	node *n;

	if (t->base.wtime <= t->base.allocated)
		return ok;
	gtr_update_dbat(tr, t->data, tchanges, t->bootstrap?0:LOG_TAB, t->base.id);
	for (n = t->columns.set->h; ok == LOG_OK && n; n = n->next) {
		int changes = 0;
		sql_column *c = n->data;

		if (!c->base.wtime || c->base.wtime <= c->base.allocated) 
			continue;
		ok = gtr_update_delta(tr, c->data, &changes, c->base.id, c->t->bootstrap?0:LOG_COL);
		if (changes)
			c->base.allocated = c->base.wtime = tr->wstime;
		(*tchanges) |= changes;
	}
	if (ok == LOG_OK && t->idxs.set) {
		for (n = t->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			int changes = 0;
			sql_idx *ci = n->data;

			/* some indices have no bats */
			if (!isTable(ci->t) || (hash_index(ci->type) && list_length(ci->columns) <= 1) || !idx_has_column(ci->type)) 
				continue;
			if (!ci->base.wtime || ci->base.wtime <= ci->base.allocated) 
				continue;

			ok = gtr_update_delta(tr, ci->data, &changes, ci->base.id, ci->t->bootstrap?0:LOG_IDX);
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
	if (cur == NULL)
		return LOG_ERR;
	/* make sure min and max values are stored in the BAT
	 * properties (BATmin and BATmax store them there if they're
	 * not already there, and if they are, they're quick) */
	BATmin(cur, &val);
	BATmax(cur, &val);
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

/* when existing delta's get cleared and again filled with large amouts of data, the ibid has
 * become the persistent bat. So we need to move this too the persistent (bid).
 */
static int 
tr_handle_snapshot( sql_trans *tr, sql_delta *bat)
{
	if (bat->ibase || tr->parent != gtrans)
		return LOG_OK;

	BAT *ins = temp_descriptor(bat->ibid);
	if (!ins)
		return LOG_ERR;
	if (BATcount(ins) > SNAPSHOT_MINSIZE){
		temp_destroy(bat->bid);
		bat->bid = bat->ibid;
		bat->cnt = bat->ibase = BATcount(ins);
		bat->ibid = e_bat(ins->ttype);
		BATmsync(ins);
	}
	bat_destroy(ins);
	return LOG_OK;
}

static int 
tr_update_delta( sql_trans *tr, sql_delta *obat, sql_delta *cbat, int unique)
{
	int ok = LOG_OK;
	BAT *ins, *cur = NULL;

	(void)tr;
	assert(ATOMIC_GET(&store_nr_active)==1);
	assert (obat->bid != 0 || tr != gtrans);

	/* for cleared tables the bid is reset */
	if (cbat->bid == 0) {
		cbat->bid = obat->bid;
		if (cbat->bid)
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
	if (obat->bid) {
		cur = temp_descriptor(obat->bid);
		if(!cur)
			return LOG_ERR;
	}
	if (!obat->bid && tr != gtrans) {
		if (obat->next)
			destroy_bat(obat->next);
		destroy_delta(obat);
		*obat = *cbat;
		cbat->bid = 0;
		cbat->ibid = 0;
		cbat->uibid = 0;
		cbat->uvbid = 0;
		cbat->cleared = 0;
		cbat->name = NULL;
		cbat->cached = NULL;
		return ok;
	}
	ins = temp_descriptor(cbat->ibid);
	if(!ins) {
		bat_destroy(cur);
		return LOG_ERR;
	}
	/* any inserts */
	if (BUNlast(ins) > 0 || cbat->cleared) {
		if ((!cbat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE)){
			/* swap cur and ins */
			BAT *newcur = ins;

			if (unique)
				BATkey(newcur, true);
			temp_destroy(cbat->bid);
			temp_destroy(obat->bid);
			obat->bid = cbat->ibid;
			cbat->bid = cbat->ibid = 0;

			BATmsync(ins);
			ins = cur;
			cur = newcur;
		} else {
			assert(cur->theap.storage != STORE_PRIV);
			assert(!BATcount(ins) || !isEbat(ins));
			assert(!isEbat(cur));
			if (cbat->cleared)
				bat_clear(cur);
			if (BATappend(cur, ins, NULL, true) != GDK_SUCCEED) {
				bat_destroy(cur);
				bat_destroy(ins);
				return LOG_ERR;
			}
			temp_destroy(cbat->bid);
			temp_destroy(cbat->ibid);
			cbat->bid = cbat->ibid = 0;
		}
		obat->cnt = cbat->cnt = obat->ibase = cbat->ibase = BATcount(cur);
		temp_destroy(obat->ibid);
		obat->ibid = e_bat(cur->ttype);
		if (obat->ibid == BID_NIL)
			ok = LOG_ERR;
	}
	if (obat->cnt != cbat->cnt) { /* locked */
		obat->cnt = cbat->cnt;
		obat->ibase = cbat->ibase;
	}
	bat_destroy(ins);

	if (cbat->ucnt && cbat->uibid) {
		BAT *ui = temp_descriptor(cbat->uibid);
		BAT *uv = temp_descriptor(cbat->uvbid);
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
		ok = destroy_bat(obat->next);
		obat->next = NULL;
	}
	return ok;
}

static int 
tr_merge_delta( sql_trans *tr, sql_delta *obat, int unique)
{
	int ok = LOG_OK;
	BAT *ins, *cur = NULL;

	(void)tr;
	assert(ATOMIC_GET(&store_nr_active)==1);
	assert (obat->bid != 0 || tr != gtrans);

	assert(!obat->cleared);
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
	/* any inserts */
	if (BUNlast(ins) > 0) {
		if ((!obat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE)){
			/* swap cur and ins */
			BAT *newcur = ins;
			bat id = obat->bid;

			if (unique)
				BATkey(newcur, true);
			obat->bid = obat->ibid;
			obat->ibid = id;

			BATmsync(ins);
			ins = cur;
			cur = newcur;
		} else {
			assert(!isEbat(cur));
			if (BATappend(cur, ins, NULL, true) != GDK_SUCCEED) {
				bat_destroy(cur);
				bat_destroy(ins);
				return LOG_ERR;
			}
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
	if (obat->next) { 
		ok = destroy_bat(obat->next);
		obat->next = NULL;
	}
	return ok;
}

static int
tr_update_dbat(sql_trans *tr, sql_dbat *tdb, sql_dbat *fdb)
{
	int ok = LOG_OK;
	BAT *db = NULL;

	if (fdb->cached) {
		bat_destroy(fdb->cached);
		fdb->cached = NULL;
	}
	if (tdb->cached) {
		bat_destroy(tdb->cached);
		tdb->cached = NULL;
	}
	assert(ATOMIC_GET(&store_nr_active)==1);
	db = temp_descriptor(fdb->dbid);
	if(!db)
		return LOG_ERR;
	if (tdb->cnt < BATcount(db) || fdb->cleared) {
		BAT *odb = temp_descriptor(tdb->dbid);
		if(odb) {
			if (isEbat(odb)){
				temp_destroy(tdb->dbid);
				tdb->dbid = temp_copy(odb->batCacheid, false);
				bat_destroy(odb);
				if (tdb->dbid == BID_NIL || (odb = temp_descriptor(tdb->dbid)) == NULL)
					return LOG_ERR;
			}
			if (fdb->cleared) {
				tdb->cleared = 1;
				tdb->cnt = 0;
				bat_clear(odb);
			}
			db->batInserted = tdb->cnt;
			if (append_inserted(odb, db) == BUN_NONE)
				ok = LOG_ERR;
			else
				BATcommit(odb);
			assert(BATcount(odb) == fdb->cnt);
			temp_destroy(fdb->dbid);

			if (ok == LOG_OK) {
				fdb->dbid = 0;
				assert(BATcount(db) == fdb->cnt);
				tdb->cnt = fdb->cnt;
			}
			bat_destroy(odb);
		} else {
			ok = LOG_ERR;
		}
	}
	bat_destroy(db);
	if (ok == LOG_OK && tdb->next) {
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
	assert(ATOMIC_GET(&store_nr_active)==1);
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
	sql_table *ot = NULL;
	int ok = LOG_OK;
	node *n, *m, *o = NULL;

	if (ATOMIC_GET(&store_nr_active) == 1 || ft->base.allocated) {
		if (ATOMIC_GET(&store_nr_active) > 1 && ft->data) { /* move delta */
			sql_dbat *b = ft->data;

			if (!tt->data)
				tt->base.allocated = ft->base.allocated;
			ft->data = NULL;
			b->next = tt->data;
			tt->data = b;

			if (b->cached) {
				bat_destroy(b->cached);
				b->cached = NULL;
			}
			while (b && b->wtime >= oldest->stime)
				b = b->next;
			/* find table t->base.stime */
			ot = tr_find_table(oldest, tt);
			if (b && ot && b->wtime < ot->base.stime) {
				/* anything older can go */
				delayed_destroy_dbat(b->next);
				b->next = NULL;
			}
		} else if (tt->data && ft->base.allocated) {
			if (tr_update_dbat(tr, tt->data, ft->data) != LOG_OK)
				ok = LOG_ERR;
		} else if (ATOMIC_GET(&store_nr_active) == 1 && !ft->base.allocated) {
			/* only insert/updates, merge earlier deletes */
			if (!tt->data) {
				sql_table *ot = tr_find_table(tr->parent, tt);
				tt->data = timestamp_dbat(ot->data, tt->base.stime);
			}
			assert(tt->data);
			if (tr_merge_dbat(tr, tt->data) != LOG_OK)
				ok = LOG_ERR;
			ft->data = NULL;
		} else if (ft->data) {
			tt->data = ft->data;
			tt->base.allocated = 1;
			ft->data = NULL;
		}
	}
	if (ot)
		o = ot->columns.set->h;
	for (n = ft->columns.set->h, m = tt->columns.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next, o=(o?o->next:NULL)) {
		sql_column *cc = n->data; // TODO: either stick to to/from terminology or old/current terminology
		sql_column *oc = m->data;

		if (ATOMIC_GET(&store_nr_active) == 1 || (cc->base.wtime && cc->base.allocated)) {
			assert(!cc->base.wtime || oc->base.wtime < cc->base.wtime);
			if (ATOMIC_GET(&store_nr_active) > 1 && cc->data) { /* move delta */
				sql_delta *b = cc->data;
				sql_column *oldc = NULL;

				if (!oc->data)
					oc->base.allocated = cc->base.allocated;
				cc->data = NULL;
				b->next = oc->data;
				oc->data = b;
				tr_handle_snapshot(tr, b);

				if (b->cached) {
					bat_destroy(b->cached);
					b->cached = NULL;
				}
				while (b && b->wtime >= oldest->stime) 
					b = b->next;
				/* find column c->base.stime */
				if (o)
					oldc = o->data;
				if (oldc && b && oldc->base.id == cc->base.id && b->wtime < oldc->base.stime) {
					/* anything older can go */
					delayed_destroy_bat(b->next);
					b->next = NULL;
				}
			} else if (oc->data && cc->base.allocated) {
				if (tr_update_delta(tr, oc->data, cc->data, cc->unique == 1) != LOG_OK) 
					ok = LOG_ERR;
			} else if (ATOMIC_GET(&store_nr_active) == 1 && !cc->base.allocated) {
				/* only deletes, merge earlier changes */
				if (!oc->data) {
					sql_column *o = tr_find_column(tr->parent, oc);
					oc->data = timestamp_delta(o->data, oc->base.stime);
				}
				assert(oc->data);
				if (tr_merge_delta(tr, oc->data, oc->unique == 1) != LOG_OK)
					ok = LOG_ERR;
				cc->data = NULL;
			} else if (cc->data) {
				tr_handle_snapshot(tr, cc->data);
				oc->data = cc->data; 
				oc->base.allocated = 1;
				cc->data = NULL;
			}
		}

		oc->colnr = cc->colnr;
		oc->null = cc->null;
		oc->unique = cc->unique;
		if (cc->storage_type && (!oc->storage_type || strcmp(cc->storage_type, oc->storage_type) != 0))
			oc->storage_type = sa_strdup(tr->parent->sa, cc->storage_type);
		if (!cc->storage_type)
			oc->storage_type = NULL;
		if (cc->def && (!oc->def || strcmp(cc->def, oc->def) != 0))
			oc->def = sa_strdup(tr->parent->sa, cc->def);
		if (!cc->def)
			oc->def = NULL;

		if (strcmp(cc->base.name, oc->base.name) != 0) { /* apply possible renaming */
			list_hash_delete(oc->t->columns.set, oc, NULL);
			oc->base.name = sa_strdup(tr->parent->sa, cc->base.name);
			if (!list_hash_add(oc->t->columns.set, oc, NULL))
				ok = LOG_ERR;
		}

		if (oc->base.rtime < cc->base.rtime)
			oc->base.rtime = cc->base.rtime;
		if (oc->base.wtime < cc->base.wtime)
			oc->base.wtime = cc->base.wtime;
		if (cc->data) 
			destroy_col(tr, cc);
		cc->base.allocated = 0;
	}
	if (ok == LOG_OK && tt->idxs.set) {
		o = NULL;
		if (ot)
			o = ot->idxs.set->h;
		for (n = ft->idxs.set->h, m = tt->idxs.set->h; ok == LOG_OK && n && m; n = n->next, m = m->next, o=(o?o->next:NULL)) {
			sql_idx *ci = n->data;
			sql_idx *oi = m->data;

			/* some indices have no bats */
			if (!oi->data || (hash_index(oi->type) && list_length(oi->columns) <= 1) || !idx_has_column(oi->type)) {
				ci->data = NULL;
				ci->base.allocated = 0;
				continue;
			}
			if (ATOMIC_GET(&store_nr_active) == 1 || (ci->base.wtime && ci->base.allocated)) {
				if (ATOMIC_GET(&store_nr_active) > 1 && ci->data) { /* move delta */
					sql_delta *b = ci->data;
					sql_idx *oldi = NULL;

					if (!oi->data)
						oi->base.allocated = ci->base.allocated;
					ci->data = NULL;
					b->next = oi->data;
					oi->data = b;
					tr_handle_snapshot(tr, b);

					if (b->cached) {
						bat_destroy(b->cached);
						b->cached = NULL;
					}
					while (b && b->wtime >= oldest->stime) 
						b = b->next;
					/* find idx i->base.stime */
					if (o)
						oldi = o->data;
					if (oldi && b && oldi->base.id == ci->base.id && b->wtime < oldi->base.stime) {
						/* anything older can go */
						delayed_destroy_bat(b->next);
						b->next = NULL;
					}
				} else if (oi->data && ci->base.allocated) {
					if (tr_update_delta(tr, oi->data, ci->data, 0) != LOG_OK)
						ok = LOG_ERR;
				} else if (ATOMIC_GET(&store_nr_active) == 1 && !ci->base.allocated) {
					if (!oi->data) {
						sql_idx *o = tr_find_idx(tr->parent, oi);
						oi->data = timestamp_delta(o->data, oi->base.stime);
					}
					assert(oi->data);
					if (tr_merge_delta(tr, oi->data, 0) != LOG_OK)
						ok = LOG_ERR;
					ci->data = NULL;
				} else if (ci->data) {
					tr_handle_snapshot(tr, ci->data);
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
			ci->base.allocated = 0;
		}
	}
	if (tt->base.rtime < ft->base.rtime)
		tt->base.rtime = ft->base.rtime;
	if (tt->base.wtime < ft->base.wtime)
		tt->base.wtime = ft->base.wtime;
	if (ft->data)
		destroy_del(tr, ft);
	ft->base.allocated = 0;
	return ok;
}

static int 
tr_log_delta( sql_trans *tr, sql_delta *cbat, int cleared, char tpe, oid id)
{
	gdk_return ok = GDK_SUCCEED;
	BAT *ins;

	(void)tr;
	if (GDKinmemory())
		return LOG_OK;

	assert(tr->parent == gtrans);
	ins = temp_descriptor(cbat->ibid);
	if (ins == NULL)
		return LOG_ERR;

	if (cleared && log_bat_clear(bat_logger, cbat->name, tpe, id) != GDK_SUCCEED) {
		bat_destroy(ins);
		return LOG_ERR;
	}

	/* any inserts */
	if (BUNlast(ins) > 0) {
		assert(ATOMIC_GET(&store_nr_active)>0);
		if (BUNlast(ins) > ins->batInserted &&
		    (cbat->ibase || BATcount(ins) <= SNAPSHOT_MINSIZE))
			ok = log_bat(bat_logger, ins, cbat->name, tpe, id);
		if (ok == GDK_SUCCEED &&
		    !cbat->ibase && BATcount(ins) > SNAPSHOT_MINSIZE) {
			/* log new snapshot */
			if ((ok = logger_add_bat(bat_logger, ins, cbat->name, tpe, id)) == GDK_SUCCEED)
				ok = log_bat_persists(bat_logger, ins, cbat->name, tpe, id);
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
			ok = log_delta(bat_logger, ui, uv, cbat->name, tpe, id);
		bat_destroy(ui);
		bat_destroy(uv);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
tr_log_dbat(sql_trans *tr, sql_dbat *fdb, int cleared, char tpe, oid id)
{
	gdk_return ok = GDK_SUCCEED;
	BAT *db = NULL;

	if (!fdb || GDKinmemory())
		return LOG_OK;

	(void)tr;
	assert (fdb->dname);
	if (cleared && log_bat_clear(bat_logger, fdb->dname, tpe, id) != GDK_SUCCEED)
		return LOG_ERR;

	db = temp_descriptor(fdb->dbid);
	if(!db)
		return LOG_ERR;
	if (BUNlast(db) > 0) {
		assert(ATOMIC_GET(&store_nr_active)>0);
		if (BUNlast(db) > db->batInserted) 
			ok = log_bat(bat_logger, db, fdb->dname, tpe, id);
	}
	bat_destroy(db);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_table(sql_trans *tr, sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	assert(tr->parent == gtrans);
	if (ft->base.wtime && ft->base.allocated)
		ok = tr_log_dbat(tr, ft->data, ft->cleared, ft->bootstrap?0:LOG_TAB, ft->base.id);
	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if (!cc->base.wtime || !cc->base.allocated) 
			continue;
		ok = tr_log_delta(tr, cc->data, ft->cleared, ft->bootstrap?0:LOG_COL, cc->base.id);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime || !ci->base.allocated)
				continue;

			ok = tr_log_delta(tr, ci->data, ft->cleared, ft->bootstrap?0:LOG_IDX, ci->base.id);
		}
	}
	return ok;
}

static int 
snapshot_bat(sql_delta *cbat)
{
	if (!cbat->ibase && cbat->cnt > SNAPSHOT_MINSIZE) {
		BAT *ins = temp_descriptor(cbat->ibid);
		if(ins) {
			if (BATsave(ins) != GDK_SUCCEED) {
				bat_destroy(ins);
				return LOG_ERR;
			}
			bat_destroy(ins);
		}
	}
	return LOG_OK;
}

static int
save_snapshot(sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if (!cc->base.wtime || !cc->base.allocated) 
			continue;
		if (snapshot_bat(cc->data) != LOG_OK)
			return LOG_ERR;
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || !ci->base.wtime || !ci->base.allocated)
				continue;

			if (snapshot_bat(ci->data) != LOG_OK)
				return LOG_ERR;
		}
	}
	return LOG_OK;
}

static int 
tr_snapshot_bat( sql_trans *tr, sql_delta *cbat)
{
	int ok = LOG_OK;

	assert(tr->parent == gtrans);
	assert(ATOMIC_GET(&store_nr_active)>0);

	(void)tr;
	if (!cbat->ibase && cbat->cnt > SNAPSHOT_MINSIZE) {
		BAT *ins = temp_descriptor(cbat->ibid);
		if(ins) {
			/* any inserts */
			if (BUNlast(ins) > 0) {
				bat_set_access(ins, BAT_READ);
				BATmode(ins, false);
			}
			bat_destroy(ins);
		} else {
			ok = LOG_ERR;
		}
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
	sf->double_elim_col = (prop_col_fptr)&double_elim_col;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->upgrade_col = (upgrade_col_fptr)&upgrade_col;
	sf->upgrade_idx = (upgrade_idx_fptr)&upgrade_idx;
	sf->upgrade_del = (upgrade_del_fptr)&upgrade_del;

	sf->log_create_col = (create_col_fptr)&log_create_col;
	sf->log_create_idx = (create_idx_fptr)&log_create_idx;
	sf->log_create_del = (create_del_fptr)&log_create_del;

	sf->snapshot_create_col = (create_col_fptr)&snapshot_create_col;
	sf->snapshot_create_idx = (create_idx_fptr)&snapshot_create_idx;
	sf->snapshot_create_del = (create_del_fptr)&snapshot_create_del;

	sf->save_snapshot = (snapshot_fptr)&save_snapshot;

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

	sf->cleanup = (cleanup_fptr)&cleanup;
}
