/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_storage.h"
#include "bat_utils.h"
#include "sql_string.h"
#include "gdk_atoms.h"
#include "gdk_atoms.h"
#include "matomic.h"

/*
 * The sql data is stored using 2 structure
 * sql_delta and sql_dbat. The dbat keeps the deleted rows and
 * the delta structure the column content consisting of a stable bat,
 * inserts and updates.
 *
 * The monetdb sql part uses MVCC, where the multiple versions of a these
 * structures keep timestamps (ts).
 *
 * The code needs too handle 3 cases of tables/transactions,
 * global persistent tables in normal transactions (ie those a chain of versions).
 * temporary tables, the chain holds a version per transaction.
 * save points these need too look at the parent transaction (and are private too one transaction).
 */

static int log_update_col( sql_trans *tr, sql_change *c);
static int log_update_idx( sql_trans *tr, sql_change *c);
static int log_update_del( sql_trans *tr, sql_change *c);
static int commit_update_col( sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int commit_update_idx( sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int commit_update_del( sql_trans *tr, sql_change *c, ulng commit_ts, ulng oldest);
static int log_create_col(sql_trans *tr, sql_change *change);
static int log_create_idx(sql_trans *tr, sql_change *change);
static int log_create_del(sql_trans *tr, sql_change *change);
static int commit_create_col(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest);
static int commit_create_idx(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest);
static int commit_create_del(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest);
static int log_destroy_col(sql_trans *tr, sql_change *change);
static int log_destroy_idx(sql_trans *tr, sql_change *change);
static int log_destroy_del(sql_trans *tr, sql_change *change);
static int tc_gc_col( sql_store Store, sql_change *c, ulng commit_ts, ulng oldest);
static int tc_gc_idx( sql_store Store, sql_change *c, ulng commit_ts, ulng oldest);
static int tc_gc_del( sql_store Store, sql_change *c, ulng commit_ts, ulng oldest);

static int tr_merge_delta( sql_trans *tr, sql_delta *obat);

/* used for communication between {append,update}_prepare and {append,update}_execute */
struct prep_exec_cookie {
	sql_delta *delta;
	bool is_new; // only used for updates
};

/* creates a new cookie, backed by sql allocator memory so it is
 * automatically freed even if errors occur
 */
static struct prep_exec_cookie *
make_cookie(sql_allocator *sa, sql_delta *delta, bool is_new)
{
	struct prep_exec_cookie *cookie;
	cookie = SA_NEW(sa, struct prep_exec_cookie);
	if (!cookie)
		return NULL;
	cookie->delta = delta;
	cookie->is_new = is_new;
	return cookie;
}

static sql_delta *
temp_dup_delta(ulng tid, int type)
{
	sql_delta *bat = ZNEW(sql_delta);
	BAT *b = bat_new(type, 1024, TRANSIENT);
	bat->bid = temp_create(b);
	bat_destroy(b);
	bat->ibid = e_bat(type);
	bat->uibid = e_bat(TYPE_oid);
	bat->uvbid = e_bat(type);
	bat->ibase = 0;
	bat->ucnt = bat->cnt = 0;
	bat->cleared = 0;
	bat->ts = tid;
	bat->refcnt = 1;
	bat->name = NULL;
	return bat;
}

static sql_delta *
temp_delta(sql_delta *d, ulng tid)
{
	while (d && d->ts != tid)
		d = d->next;
	return d;
}

static sql_dbat *
temp_dup_dbat(ulng tid)
{
	sql_dbat *bat = ZNEW(sql_dbat);
	BAT *b = bat_new(TYPE_oid, 1024, TRANSIENT);
	bat->dbid = temp_create(b);
	bat_destroy(b);
	bat->cnt = 0;
	bat->ts = tid;
	bat->refcnt = 1;
	bat->dname = NULL;
	return bat;
}

static sql_dbat *
temp_dbat(sql_dbat *d, ulng tid)
{
	while (d && d->ts != tid)
		d = d->next;
	return d;
}

static sql_delta *
timestamp_delta( sql_trans *tr, sql_delta *d)
{
	while (d->next && d->ts != tr->tid && (!tr->parent || !tr_version_of_parent(tr, d->ts)) && d->ts > tr->ts)
		d = d->next;
	return d;
}

static sql_delta *
temp_col_timestamp_delta( sql_trans *tr, sql_column *c)
{
	assert(isTempTable(c->t));
	sql_delta *d = temp_delta(ATOMIC_PTR_GET(&c->data), tr->tid);
	if (!d) {
		d = temp_dup_delta(tr->tid, c->type.type->localtype);
		do {
			d->next = ATOMIC_PTR_GET(&c->data);
		} while(!ATOMIC_PTR_CAS(&c->data, (void**)&d->next, d)); /* set c->data = d, when c->data == d->next else d->next = c->data */
	}
	return d;
}

sql_delta *
col_timestamp_delta( sql_trans *tr, sql_column *c)
{
	if (isTempTable(c->t))
		return temp_col_timestamp_delta(tr, c);
	return timestamp_delta( tr, ATOMIC_PTR_GET(&c->data));
}

static sql_delta *
temp_idx_timestamp_delta( sql_trans *tr, sql_idx *i)
{
	assert(isTempTable(i->t));
	sql_delta *d = temp_delta(ATOMIC_PTR_GET(&i->data), tr->tid);
	if (!d) {
		int type = oid_index(i->type)?TYPE_oid:TYPE_lng;
		d = temp_dup_delta(tr->tid, type);
		do {
			d->next = ATOMIC_PTR_GET(&i->data);
		} while(!ATOMIC_PTR_CAS(&i->data, (void**)&d->next, d)); /* set i->data = d, when i->data == d->next else d->next = i->data */
	}
	return d;
}

static sql_delta *
idx_timestamp_delta( sql_trans *tr, sql_idx *i)
{
	if (isTempTable(i->t))
		return temp_idx_timestamp_delta(tr, i);
	return timestamp_delta( tr, ATOMIC_PTR_GET(&i->data));
}

static sql_dbat *
timestamp_dbat( sql_trans *tr, sql_dbat *d)
{
	while (d->next && d->ts != tr->tid && (!tr->parent || !tr_version_of_parent(tr, d->ts)) && d->ts > tr->ts)
		d = d->next;
	return d;
}

static sql_dbat *
temp_tab_timestamp_dbat( sql_trans *tr, sql_table *t)
{
	assert(isTempTable(t));
	sql_dbat *d = temp_dbat(ATOMIC_PTR_GET(&t->data), tr->tid);
	if (!d) {
		d = temp_dup_dbat(tr->tid);
		do {
			d->next = ATOMIC_PTR_GET(&t->data);
		} while(!ATOMIC_PTR_CAS(&t->data, (void**)&d->next, d)); /* set t->data = d, when t->data == d->next else d->next = t->data */
	}
	return d;
}

static sql_dbat *
tab_timestamp_dbat( sql_trans *tr, sql_table *t)
{
	if (isTempTable(t))
		return temp_tab_timestamp_dbat(tr, t);
	return timestamp_dbat( tr, ATOMIC_PTR_GET(&t->data));
}

static sql_delta*
delta_dup(sql_delta *d)
{
	d->refcnt++;
	return d;
}

static void *
col_dup(sql_column *c)
{
	return delta_dup(ATOMIC_PTR_GET(&c->data));
}

static void *
idx_dup(sql_idx *i)
{
	if (!ATOMIC_PTR_GET(&i->data))
		return NULL;
	return delta_dup(ATOMIC_PTR_GET(&i->data));
}

static sql_dbat*
dbat_dup(sql_dbat *d)
{
	d->refcnt++;
	return d;
}

static void *
del_dup(sql_table *t)
{
	return dbat_dup(ATOMIC_PTR_GET(&t->data));
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
	sql_dbat *d = tab_timestamp_dbat(tr, t);
	return delta_bind_del(d, access);
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
	assert(tr->active);
	sql_delta *d = col_timestamp_delta(tr, c);
	return delta_bind_ubat(d, access, c->type.type->localtype);
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	int type = oid_index(i->type)?TYPE_oid:TYPE_lng;
	assert(tr->active);
	sql_delta *d = idx_timestamp_delta(tr, i);
	return delta_bind_ubat(d, access, type);
}

static BAT *
delta_bind_bat( sql_delta *bat, int access, int is_new)
{
	BAT *b;

	assert(access == RDONLY || access == RD_INS || access == QUICK);
	assert(bat != NULL);
	if (access == QUICK)
		return quick_descriptor(bat->bid);
	if (is_new || access == RD_INS) {
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
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_ucol(tr, c, access);
	sql_delta *d = col_timestamp_delta(tr, c);
	return delta_bind_bat( d, access, isNew(c->t));
}

static BAT *
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(i->t))
		return NULL;
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_uidx(tr, i, access);
	sql_delta *d = idx_timestamp_delta(tr, i);
	return delta_bind_bat( d, access, isNew(i->t));
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
dup_delta(sql_trans *tr, sql_delta *obat, sql_delta *bat, int type, int c_isnew, int temp, int sz)
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
		} else if (c_isnew && !bat->bid && !tr->parent) {
			/* tr->parent (ie savepoint) keep the same structure as the parent */
			/* move the bat to the new col, fixup the old col */
			b = COLnew((oid) obat->cnt, type, sz, PERSISTENT);
			if (b == NULL)
				return LOG_ERR;
			bat_set_access(b, BAT_READ);
			obat->ibid = temp_create(b);
			obat->ibase = bat->ibase = (oid) obat->cnt;
			bat_destroy(b);
			temp_dup(bat->ibid);
			obat->bid = bat->ibid;
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

static int
dup_bat(sql_trans *tr, sql_table *t, sql_delta *obat, sql_delta *bat, int type, int c_isnew)
{
	return dup_delta( tr, obat, bat, type, c_isnew, isTempTable(t), t->sz);
}

static int
destroy_delta(sql_delta *b)
{
	int ok = LOG_OK;

	if (--b->refcnt > 0)
		return LOG_OK;
	if (b->next)
		ok = destroy_delta(b->next);
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
	_DELETE(b);
	return ok;
}

static sql_delta *
bind_col_data(sql_trans *tr, sql_column *c)
{
	sql_delta *obat = ATOMIC_PTR_GET(&c->data);

	if (isTempTable(c->t))
		obat = temp_col_timestamp_delta(tr, c);

	if (obat->ts == tr->tid)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->ts)) && obat->ts >= TRANSACTION_ID_BASE && !isTempTable(c->t))
		/* abort */
		return NULL;
	assert(!isTempTable(c->t));
	obat = timestamp_delta(tr, ATOMIC_PTR_GET(&c->data));
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	bat->refcnt = 1;
	if(dup_bat(tr, c->t, obat, bat, c->type.type->localtype, isNew(c)) == LOG_ERR)
		return NULL;
	bat->ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&c->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_delta(bat);
		return NULL;
	}
	return bat;
}

static void*
update_col_prepare(sql_trans *tr, sql_allocator *sa, sql_column *c)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c)) == NULL)
		return NULL;

	assert(delta && delta->ts == tr->tid);
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);
	return make_cookie(sa, delta, isNew(c));
}

static int
update_col_execute(void *incoming_cookie, void *incoming_tids, void *incoming_values, bool is_bat)
{
	struct prep_exec_cookie *cookie = incoming_cookie;

	if (is_bat) {
		BAT *tids = incoming_tids;
		BAT *values = incoming_values;
		if (BATcount(tids) == 0)
			return LOG_OK;
		return delta_update_bat(cookie->delta, tids, values, cookie->is_new);
	}
	else
		return delta_update_val(cookie->delta, *(oid*)incoming_tids, incoming_values);
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, int tpe)
{
	void *cookie = update_col_prepare(tr, NULL, c);
	if (cookie == NULL)
		return LOG_ERR;

	int ok = update_col_execute(cookie, tids, upd, tpe == TYPE_bat);
	_DELETE(cookie);
	return ok;
}

static sql_delta *
bind_idx_data(sql_trans *tr, sql_idx *i)
{
	sql_delta *obat = ATOMIC_PTR_GET(&i->data);

	if (isTempTable(i->t))
		obat = temp_idx_timestamp_delta(tr, i);

	if (obat->ts == tr->tid)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->ts)) && obat->ts >= TRANSACTION_ID_BASE && !isTempTable(i->t))
		/* abort */
		return NULL;
	assert(!isTempTable(i->t));
	obat = timestamp_delta(tr, ATOMIC_PTR_GET(&i->data));
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	bat->refcnt = 1;
	if(dup_bat(tr, i->t, obat, bat, (oid_index(i->type))?TYPE_oid:TYPE_lng, isNew(i)) == LOG_ERR)
		return NULL;
	bat->ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&i->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_delta(bat);
		return NULL;
	}
	return bat;
}

static void*
update_idx_prepare(sql_trans *tr, sql_allocator *sa, sql_idx *i)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if ((delta = bind_idx_data(tr, i)) == NULL)
		return NULL;

	assert(delta && delta->ts == tr->tid);
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);
	return make_cookie(sa, delta, isNew(i));
}

static int
update_idx(sql_trans *tr, sql_idx * i, void *tids, void *upd, int tpe)
{
	void *cookie = update_idx_prepare(tr, NULL, i);
	if (cookie == NULL)
		return LOG_ERR;

	int ok = update_col_execute(cookie, tids, upd, tpe == TYPE_bat);
	_DELETE(cookie);
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
		bat_set_access(i, BAT_READ);
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
	bat->cnt = temp?0:obat->cnt;
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

static void*
append_col_prepare(sql_trans *tr, sql_allocator *sa, sql_column *c)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c)) == NULL)
		return NULL;

	assert(delta && delta->ts == tr->tid);
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);
	return make_cookie(sa, delta, false);
}

static int
append_col_execute(void *incoming_cookie, void *incoming_data, bool is_bat)
{
	struct prep_exec_cookie *cookie = incoming_cookie;
	int ok;

	if (is_bat) {
		BAT *bat = incoming_data;

		if (!BATcount(bat))
			return LOG_OK;
		ok = delta_append_bat(cookie->delta, bat);
	} else {
		ok = delta_append_val(cookie->delta, incoming_data);
	}

	return ok;
}

static int
append_col(sql_trans *tr, sql_column *c, void *i, int tpe)
{
	void *cookie = append_col_prepare(tr, NULL, c);
	if (cookie == NULL)
		return LOG_ERR;

	int ok = append_col_execute(cookie, i, tpe == TYPE_bat);
	_DELETE(cookie);
	return ok;
}

static void*
append_idx_prepare(sql_trans *tr, sql_allocator *sa, sql_idx *i)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if ((delta = bind_idx_data(tr, i)) == NULL)
		return NULL;

	assert(delta && delta->ts == tr->tid);
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);
	return make_cookie(sa, delta, false);
}

static int
append_idx(sql_trans *tr, sql_idx * i, void *data, int tpe)
{
	void *cookie = append_idx_prepare(tr, NULL, i);
	if (cookie == NULL)
		return LOG_ERR;

	int ok = append_col_execute(cookie, data, tpe == TYPE_bat);
	_DELETE(cookie);
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

static int
destroy_dbat(sql_dbat *bat)
{
	int ok = LOG_OK;

	if (--bat->refcnt > 0)
		return LOG_OK;
	if (bat->next)
		ok = destroy_dbat(bat->next);
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
	return ok;
}

static sql_dbat *
bind_del_data(sql_trans *tr, sql_table *t)
{
	sql_dbat *obat = ATOMIC_PTR_GET(&t->data);

	if (isTempTable(t))
		obat = temp_tab_timestamp_dbat(tr, t);

	if (obat->ts == tr->tid)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->ts)) && obat->ts >= TRANSACTION_ID_BASE && !isTempTable(t))
		/* abort */
		return NULL;
	assert(!isTempTable(t));
	obat = timestamp_dbat(tr, ATOMIC_PTR_GET(&t->data));
	sql_dbat *bat = ZNEW(sql_dbat);
	if(!bat)
		return NULL;
	bat->refcnt = 1;
	dup_dbat(tr, obat, bat, isNew(t), isTempTable(t));
	bat->ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&t->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_dbat(bat);
		return NULL;
	}
	return bat;
}

static int
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	int ok = LOG_OK;
	BAT *b = ib;
	sql_dbat *bat, *obat = ATOMIC_PTR_GET(&t->data);

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if ((bat = bind_del_data(tr, t)) == NULL)
		return LOG_ERR;

	/* delete all cached copies */
	if (bat->cached) {
		bat_destroy(bat->cached);
		bat->cached = NULL;
	}

	assert(bat && bat->ts == tr->tid);
	/* deletes only write */
	if (tpe == TYPE_bat)
		ok = delta_delete_bat(bat, ib);
	else
		ok = delta_delete_val(bat, *(oid*)ib);
	if ((!inTransaction(tr, t) && (obat != bat || isTempTable(t)) && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, bat, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	return ok;
}

static size_t
count_col(sql_trans *tr, sql_column *c, int all)
{
	sql_delta *b;

	assert(tr->active);
	if (!isTable(c->t))
		return 0;
	b = col_timestamp_delta(tr, c);
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
	b = col_timestamp_delta(tr, c);
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
	b = idx_timestamp_delta(tr, i);
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
	d = tab_timestamp_dbat(tr, t);
	assert(d);
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
	b = col_timestamp_delta(tr, c);
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
	b = idx_timestamp_delta(tr, i);
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

	if (col && ATOMIC_PTR_GET(&col->data)) {
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

	if (col && ATOMIC_PTR_GET(&col->data)) {
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

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b && b->tvarsized) /* check double elimination */
			de = GDK_ELIMDOUBLES(b->tvheap);
		if (de)
			de = b->twidth;
	}
	return de;
}

static int
nonil_col(sql_trans *tr, sql_column *col)
{
	int nonil = 0;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK), *c;

		if (b) {
			nonil = b->tnonil && !b->tnil; /* check deltas as well */

			if ((c = bind_col(tr, col, RD_INS))) {
				nonil &= c->tnonil && !c->tnil;
				BBPunfix(c->batCacheid);
			}
			if ((c = bind_col(tr, col, RD_UPD_VAL))) {
				nonil &= c->tnonil && !c->tnil;
				BBPunfix(c->batCacheid);
			}
		}
	}
	return nonil;
}

static ValPtr
col_min_value(sql_trans *tr, sql_column *col)
{
	ValPtr res = NULL;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK), *c;
		PROPrec *prop1 = NULL, *prop2 = NULL;

		if (b && (prop1 = BATgetprop(b, GDK_MIN_VALUE))) {
			ValRecord cp;
			res = &(prop1->v);

			if ((c = bind_col(tr, col, RD_INS))) {
				if ((prop2 = BATgetprop(c, GDK_MIN_VALUE))) {
					if (VARcalcgt(&cp, res, &(prop2->v)) != GDK_SUCCEED) {
						BBPunfix(c->batCacheid);
						return NULL;
					}
					res = cp.val.btval == 1 ? &(prop2->v) : res;
				} else if (BATcount(c) > 0) {
					res = NULL;
				}
				BBPunfix(c->batCacheid);
			}
			if (res && (c = bind_col(tr, col, RD_UPD_VAL))) {
				if ((prop2 = BATgetprop(c, GDK_MIN_VALUE))) {
					if (VARcalcgt(&cp, res, &(prop2->v)) != GDK_SUCCEED) {
						BBPunfix(c->batCacheid);
						return NULL;
					}
					res = cp.val.btval == 1 ? &(prop2->v) : res;
				} else if (BATcount(c) > 0) {
					res = NULL;
				}
				BBPunfix(c->batCacheid);
			}
		}
	}
	return res;
}

static ValPtr
col_max_value(sql_trans *tr, sql_column *col)
{
	ValPtr res = NULL;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK), *c;
		PROPrec *prop1 = NULL, *prop2 = NULL;

		if (b && (prop1 = BATgetprop(b, GDK_MAX_VALUE))) {
			ValRecord cp;
			res = &(prop1->v);

			if ((c = bind_col(tr, col, RD_INS))) {
				if ((prop2 = BATgetprop(c, GDK_MAX_VALUE))) {
					if (VARcalclt(&cp, res, &(prop2->v)) != GDK_SUCCEED) {
						BBPunfix(c->batCacheid);
						return NULL;
					}
					res = cp.val.btval == 1 ? &(prop2->v) : res;
				} else if (BATcount(c) > 0) {
					res = NULL;
				}
				BBPunfix(c->batCacheid);
			}
			if (res && (c = bind_col(tr, col, RD_UPD_VAL))) {
				if ((prop2 = BATgetprop(c, GDK_MAX_VALUE))) {
					if (VARcalclt(&cp, res, &(prop2->v)) != GDK_SUCCEED) {
						BBPunfix(c->batCacheid);
						return NULL;
					}
					res = cp.val.btval == 1 ? &(prop2->v) : res;
				} else if (BATcount(c) > 0) {
					res = NULL;
				}
				BBPunfix(c->batCacheid);
			}
		}
	}
	return res;
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
	int ok = LOG_OK, new = 0;
	int type = c->type.type->localtype;
	sql_delta *bat = ATOMIC_PTR_GET(&c->data);

	if (!bat) {
		new = 1;
		bat = ZNEW(sql_delta);
		ATOMIC_PTR_SET(&c->data, bat);
		if(!bat)
			return LOG_ERR;
		bat->refcnt = 1;
	}
	if (!bat->name) {
		bat->name = sql_message("%s_%s_%s", c->t->s->base.name, c->t->base.name, c->base.name);
		if(!bat->name)
			ok = LOG_ERR;
	}

	if (new)
		bat->ts = tr->tid;

	if (!isNew(c) && !isTempTable(c->t)){
		bat->ts = tr->ts;
		return load_bat(tr, bat, type, c->t->bootstrap?0:LOG_COL, c->base.id);
	} else if (bat && bat->ibid && !isTempTable(c->t)) {
		return new_persistent_bat(tr, ATOMIC_PTR_GET(&c->data), c->t->sz);
	} else if (!bat->ibid) {
		sql_column *fc = NULL;
		size_t cnt = 0;

		/* alter ? */
		if (c->t->columns.set && (fc = c->t->columns.set->h->data) != NULL)
			cnt = count_col(tr, fc, 1);
		if (cnt && fc != c) {
			sql_delta *d = ATOMIC_PTR_GET(&fc->data);

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
			bat->alter = 1;
		} else {
			BAT *b = bat_new(type, c->t->sz, PERSISTENT);
			if (!b) {
				ok = LOG_ERR;
			} else {
				create_delta(ATOMIC_PTR_GET(&c->data), NULL, b);
				bat_destroy(b);
			}
		}
		if (new /*&& !isTempTable(c->t)*/ && !isNew(c->t) /* alter */)
			trans_add(tr, &c->base, bat, &tc_gc_col, &commit_create_col, isTempTable(c->t)?NULL:&log_create_col);
	}
	return ok;
}

static int
upgrade_col(sql_trans *tr, sql_column *c)
{
	sql_delta *bat = ATOMIC_PTR_GET(&c->data);

	if (!c->t->bootstrap)
		return upgrade_delta(tr, bat, LOG_COL, c->base.id);
	return LOG_OK;
}

static int
log_create_col_(sql_trans *tr, sql_column *c)
{
	assert(!isTempTable(c->t));
	return log_create_delta(tr,  ATOMIC_PTR_GET(&c->data), c->t->bootstrap?0:LOG_COL, c->base.id);
}

static int
log_create_col(sql_trans *tr, sql_change *change)
{
	return log_create_col_(tr, (sql_column*)change->obj);
}

static int
commit_create_col_( sql_trans *tr, sql_column *c, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	(void)oldest;

	if(!isTempTable(c->t)) {
		sql_delta *delta = ATOMIC_PTR_GET(&c->data);
		assert(delta->ts == tr->tid);
		delta->ts = commit_ts;

		assert(delta->next == NULL);
		if (!delta->alter)
			ok = tr_merge_delta(tr, delta);
		delta->alter = 0;
		c->base.flags = 0;
	}
	return ok;
}

static int
commit_create_col( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sql_column *c = (sql_column*)change->obj;
	c->base.flags = 0;
	return commit_create_col_( tr, c, commit_ts, oldest);
}

/* will be called for new idx's and when new index columns are created */
static int
create_idx(sql_trans *tr, sql_idx *ni)
{
	int ok = LOG_OK, new = 0;
	sql_delta *bat = ATOMIC_PTR_GET(&ni->data);
	int type = TYPE_lng;

	if (oid_index(ni->type))
		type = TYPE_oid;

	if (!bat) {
		new = 1;
		bat = ZNEW(sql_delta);
		ATOMIC_PTR_SET(&ni->data, bat);
		if(!bat)
			return LOG_ERR;
		bat->refcnt = 1;
	}
	if (!bat->name) {
		bat->name = sql_message("%s_%s@%s", ni->t->s->base.name, ni->t->base.name, ni->base.name);
		if(!bat->name)
			ok = LOG_ERR;
	}

	if (new)
		bat->ts = tr->tid;

	if (!isNew(ni) && !isTempTable(ni->t)){
		bat->ts = tr->ts;
		return load_bat(tr, bat, type, ni->t->bootstrap?0:LOG_IDX, ni->base.id);
	} else if (bat && bat->ibid && !isTempTable(ni->t)) {
		return new_persistent_bat( tr, ATOMIC_PTR_GET(&ni->data), ni->t->sz);
	} else if (!bat->ibid) {
		sql_column *c = ni->t->columns.set->h->data;
		sql_delta *d;

		d = col_timestamp_delta(tr, c);
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
		bat->alter = 1;

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
		if (new /*&& !isTempTable(ni->t)*/ && !isNew(ni->t) /* alter */)
			trans_add(tr, &ni->base, bat, &tc_gc_idx, &commit_create_idx, isTempTable(ni->t)?NULL:&log_create_idx);
	}
	return ok;
}

static int
upgrade_idx(sql_trans *tr, sql_idx *i)
{
	sql_delta *bat = ATOMIC_PTR_GET(&i->data);

	if (!i->t->bootstrap && bat != NULL)
		return upgrade_delta(tr, bat, LOG_IDX, i->base.id);
	return LOG_OK;
}

static int
log_create_idx_(sql_trans *tr, sql_idx *i)
{
	assert(!isTempTable(i->t));
	return log_create_delta(tr, ATOMIC_PTR_GET(&i->data), i->t->bootstrap?0:LOG_IDX, i->base.id);
}

static int
log_create_idx(sql_trans *tr, sql_change *change)
{
	return log_create_idx_(tr, (sql_idx*)change->obj);
}

static int
commit_create_idx_( sql_trans *tr, sql_idx *i, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	(void)oldest;

	if(!isTempTable(i->t)) {
		sql_delta *delta = ATOMIC_PTR_GET(&i->data);
		assert(delta->ts == tr->tid);
		delta->ts = commit_ts;

		assert(delta->next == NULL);
		ok = tr_merge_delta(tr, delta);
		i->base.flags = 0;
	}
	return ok;
}

static int
commit_create_idx( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sql_idx *i = (sql_idx*)change->obj;
	i->base.flags = 0;
	return commit_create_idx_(tr, i, commit_ts, oldest);
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

	int ok = LOG_OK, new = 0;
	BAT *b;
	sql_dbat *bat = ATOMIC_PTR_GET(&t->data);

	if (!bat) {
		new = 1;
		bat = ZNEW(sql_dbat);
		ATOMIC_PTR_SET(&t->data, bat);
		if(!bat)
			return LOG_ERR;
		bat->refcnt = 1;
	}
	if (!bat->dname) {
		bat->dname = sql_message("D_%s_%s", t->s->base.name, t->base.name);
		if(!bat->dname)
			ok = LOG_ERR;
	}
	if (new)
		bat->ts = tr->tid;

	if (!isNew(t) && !isTempTable(t)) {
		log_bid bid = logger_find_bat(store->logger, bat->dname, t->bootstrap?0:LOG_TAB, t->base.id);

		if (bid) {
			bat->ts = tr->ts;
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
		if (new /*&& !isTempTable(t)*/)
			trans_add(tr, &t->base, bat, &tc_gc_del, &commit_create_del, isTempTable(t)?NULL:&log_create_del);
	}
	return ok;
}

static int
upgrade_del(sql_trans *tr, sql_table *t)
{
	sql_dbat *bat = ATOMIC_PTR_GET(&t->data);

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
log_create_del(sql_trans *tr, sql_change *change)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;

	assert(!isTempTable(t));
	ok = log_create_dbat(tr, ATOMIC_PTR_GET(&t->data), t->bootstrap?0:LOG_TAB, t->base.id);
	if (ok == LOG_OK) {
		for(node *n = t->columns.set->h; n && ok == LOG_OK; n = n->next) {
			sql_column *c = n->data;

			ok = log_create_col_(tr, c);
		}
		if (t->idxs.set) {
			for(node *n = t->idxs.set->h; n && ok == LOG_OK; n = n->next) {
				sql_idx *i = n->data;

				if (ATOMIC_PTR_GET(&i->data))
					ok = log_create_idx_(tr, i);
			}
		}
	}
	return ok;
}

static int
commit_create_del( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;

	if (!commit_ts) /* rollback handled by ? */
		return ok;
	if(!isTempTable(t)) {
		sql_dbat *dbat = ATOMIC_PTR_GET(&t->data);
		assert(dbat->ts == tr->tid);
		dbat->ts = commit_ts;
		if (ok == LOG_OK) {
			for(node *n = t->columns.set->h; n && ok == LOG_OK; n = n->next) {
				sql_column *c = n->data;

				ok = commit_create_col_(tr, c, commit_ts, oldest);
			}
			if (t->idxs.set) {
				for(node *n = t->idxs.set->h; n && ok == LOG_OK; n = n->next) {
					sql_idx *i = n->data;

					if (ATOMIC_PTR_GET(&i->data))
						ok = commit_create_idx_(tr, i, commit_ts, oldest);
				}
			}
			t->base.flags = 0;
		}
	}
			t->base.flags = 0;
	return ok;
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
destroy_col(sqlstore *store, sql_column *c)
{
	(void)store;
	int ok = LOG_OK;
	if (ATOMIC_PTR_GET(&c->data))
		ok = destroy_delta(ATOMIC_PTR_GET(&c->data));
	ATOMIC_PTR_SET(&c->data, NULL);
	return ok;
}

static int
log_destroy_col_(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	assert(!isTempTable(c->t));
	//delta->ts = commit_ts;
	if (!tr->parent) /* don't write save point commits */
		ok = log_destroy_delta(tr, ATOMIC_PTR_GET(&c->data), c->t->bootstrap?0:LOG_COL, c->base.id);
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_change *change)
{
	return log_destroy_col_(tr, (sql_column*)change->obj);
}

static int
destroy_idx(sqlstore *store, sql_idx *i)
{
	(void)store;
	int ok = LOG_OK;
	if (ATOMIC_PTR_GET(&i->data))
		ok = destroy_delta(ATOMIC_PTR_GET(&i->data));
	ATOMIC_PTR_SET(&i->data, NULL);
	return ok;
}

static int
log_destroy_idx_(sql_trans *tr, sql_idx *i)
{
	int ok = LOG_OK;
	assert(!isTempTable(i->t));
	if (ATOMIC_PTR_GET(&i->data)) {
		//delta->ts = commit_ts;
		if (!tr->parent) /* don't write save point commits */
			ok = log_destroy_delta(tr, ATOMIC_PTR_GET(&i->data), i->t->bootstrap?0:LOG_IDX, i->base.id);
	}
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_change *change)
{
	return log_destroy_idx_(tr, (sql_idx*)change->obj);
}


static int
cleanup(void)
{
	int ok = LOG_OK;
	return ok;
}

static int
destroy_del(sqlstore *store, sql_table *t)
{
	(void)store;
	int ok = LOG_OK;
	if (ATOMIC_PTR_GET(&t->data))
		ok = destroy_dbat(ATOMIC_PTR_GET(&t->data));
	ATOMIC_PTR_SET(&t->data, NULL);
	return ok;
}

static int
log_destroy_dbat(sql_trans *tr, sql_dbat *bat, char tpe, oid id)
{
	log_bid bid;
	gdk_return ok = GDK_SUCCEED;

	sqlstore *store = tr->store;
	if (!GDKinmemory(0) && !tr->parent && /* don't write save point commits */
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
log_destroy_del(sql_trans *tr, sql_change *change)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;
	assert(!isTempTable(t));
	sql_dbat *dbat = ATOMIC_PTR_GET(&t->data);
	if (dbat->ts < tr->ts) /* no changes ? */
		return ok;
	//dbat->ts = commit_ts;
	ok = log_destroy_dbat(tr, ATOMIC_PTR_GET(&t->data), t->bootstrap?0:LOG_TAB, t->base.id);

	if (ok == LOG_OK) {
		for(node *n = t->columns.set->h; n && ok == LOG_OK; n = n->next) {
			sql_column *c = n->data;

			ok = log_destroy_col_(tr, c);
		}
		if (t->idxs.set) {
			for(node *n = t->idxs.set->h; n && ok == LOG_OK; n = n->next) {
				sql_idx *i = n->data;

				ok = log_destroy_idx_(tr, i);
			}
		}
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
			sz += BATcount(b);
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
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c)) == NULL)
		return BUN_NONE;
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);
	if (delta)
		return clear_delta(tr, delta);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if ((delta = bind_idx_data(tr, i)) == NULL)
		return BUN_NONE;
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);
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
	sql_dbat *bat, *obat = ATOMIC_PTR_GET(&t->data);

	if ((bat = bind_del_data(tr, t)) == NULL)
		return BUN_NONE;
	if ((!inTransaction(tr, t) && (obat != bat || isTempTable(t)) && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, bat, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	return clear_dbat(tr, bat);
}

static BUN
clear_table(sql_trans *tr, sql_table *t)
{
	node *n = t->columns.set->h;
	sql_column *c = n->data;
	BUN sz = 0, nsz = 0;

	if ((nsz = clear_col(tr, c)) == BUN_NONE)
		return BUN_NONE;
	sz += nsz;
	if ((nsz = clear_del(tr, t)) == BUN_NONE)
		return BUN_NONE;
	sz -= nsz;

	for (n = n->next; n; n = n->next) {
		c = n->data;

		if (clear_col(tr, c) == BUN_NONE)
			return BUN_NONE;
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *ci = n->data;

			if (isTable(ci->t) && idx_has_column(ci->type) &&
				clear_idx(tr, ci) == BUN_NONE)
				return BUN_NONE;
		}
	}
	return sz;
}

static int
tr_log_delta( sql_trans *tr, sql_delta *cbat, char tpe, oid id)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;
	BAT *ins;

	(void)tr;
	if (GDKinmemory(0) || tr->parent)
		return LOG_OK;

	ins = temp_descriptor(cbat->ibid);
	if (ins == NULL)
		return LOG_ERR;

	if (cbat->cleared && log_bat_clear(store->logger, cbat->name, tpe, id) != GDK_SUCCEED) {
		bat_destroy(ins);
		return LOG_ERR;
	}

	/* any inserts */
	if (BUNlast(ins) > 0) {
		if (BUNlast(ins) > ins->batInserted)
			ok = log_bat(store->logger, ins, cbat->name, tpe, id);
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
tr_log_dbat(sql_trans *tr, sql_dbat *fdb, char tpe, oid id)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;
	BAT *db = NULL;

	if (!fdb || GDKinmemory(0) || tr->parent)
		return LOG_OK;

	(void)tr;
	assert (fdb->dname);
	if (fdb->cleared && log_bat_clear(store->logger, fdb->dname, tpe, id) != GDK_SUCCEED)
		return LOG_ERR;

	db = temp_descriptor(fdb->dbid);
	if(!db)
		return LOG_ERR;
	if (BUNlast(db) > 0) {
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
	//assert(!store->initialized || ATOMIC_GET(&store->nr_active)==1);
	//assert(!obat->cleared);

	if (obat->bid == 0) {
		cur = temp_descriptor(obat->ibid);
		obat->bid = obat->ibid;
		obat->ibase = obat->cnt;
		obat->ibid = e_bat(cur->ttype);
		obat->cleared = 0;
		if (!obat->uibid)
			obat->uibid = e_bat(TYPE_oid);
		if (!obat->uvbid)
			obat->uvbid = e_bat(cur->ttype);
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
		if (isEbat(cur)) {
			temp_destroy(obat->bid);
			obat->bid = ebat2real(cur->batCacheid, 0);
			bat_destroy(cur);
			if(obat->bid != BID_NIL) {
				cur = temp_descriptor(obat->bid);
				if (cur == NULL)
					return LOG_ERR;
			} else {
				return LOG_ERR;
			}
		}
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
tr_merge_dbat(sql_dbat *tdb)
{
	int ok = LOG_OK;

	if (tdb->cached) {
		bat_destroy(tdb->cached);
		tdb->cached = NULL;
	}
	if (tdb->next) {
		ok = destroy_dbat(tdb->next);
		tdb->next = NULL;
	}
	return ok;
}

static sql_delta *
savepoint_commit_delta( sql_delta *delta, ulng commit_ts)
{
	/* commit ie copy back to the parent transaction */
	if (delta && delta->ts == commit_ts && delta->next) {
		sql_delta *od = delta->next;
		if (od->ts == commit_ts) {
			sql_delta t = *od, *n = od->next;
			*od = *delta;
			od->next = n;
			*delta = t;
			delta->next = NULL;
			destroy_delta(delta);
			return od;
		}
	}
	return delta;
}

static int
rollback_delta(sql_trans *tr, sql_delta *delta, int type)
{
	(void)tr;
	temp_destroy(delta->ibid);
	delta->ibid = e_bat(type);
	if (delta->ucnt) {
		delta->ucnt = 0;
		temp_destroy(delta->uibid);
		temp_destroy(delta->uvbid);
		delta->uibid = e_bat(TYPE_oid);
		delta->uvbid = e_bat(type);
	}
	return LOG_OK;
}

static int
commit_delta(sql_trans *tr, sql_delta *delta)
{
	return tr_merge_delta(tr, delta);
}

static int
log_update_col( sql_trans *tr, sql_change *change)
{
	sql_column *c = (sql_column*)change->obj;

	if (!isTempTable(c->t) && !tr->parent) /* don't write save point commits */
		return tr_log_delta(tr, ATOMIC_PTR_GET(&c->data), c->t->bootstrap?0:LOG_COL, c->base.id);
	return LOG_OK;
}

static int
commit_update_col_( sql_trans *tr, sql_column *c, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_delta *delta = ATOMIC_PTR_GET(&c->data);

	(void)oldest;
	if (isTempTable(c->t)) {
		if (commit_ts) { /* commit */
			if (c->t->commit_action == CA_COMMIT || c->t->commit_action == CA_PRESERVE)
				commit_delta(tr, delta);
			else /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
				clear_delta(tr, delta);
		} else { /* rollback */
			if (c->t->commit_action == CA_COMMIT/* || c->t->commit_action == CA_PRESERVE*/)
				rollback_delta(tr, delta, c->type.type->localtype);
			else /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
				clear_delta(tr, delta);
		}
		c->t->base.flags = c->base.flags = 0;
	}
	return ok;
}

static int
commit_update_col( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_column *c = (sql_column*)change->obj;
	sql_delta *delta = ATOMIC_PTR_GET(&c->data);

	if (isTempTable(c->t))
		return commit_update_col_(tr, c, commit_ts, oldest);
	if (commit_ts)
		delta->ts = commit_ts;
	if (!commit_ts) { /* rollback */
		sql_delta *d = change->data, *o = ATOMIC_PTR_GET(&c->data);

		if (o != d) {
			while(o && o->next != d)
				o = o->next;
		}
		if (o == ATOMIC_PTR_GET(&c->data))
			ATOMIC_PTR_SET(&c->data, d->next);
		else
			o->next = d->next;
		d->next = NULL;
		destroy_delta(d);
	} else if (ok == LOG_OK && !tr->parent) {
		sql_delta *d = delta;
		/* clean up and merge deltas */
		while (delta && delta->ts > oldest) {
			delta = delta->next;
		}
		if (delta && delta != d) {
			if (delta->next) {
				ok = destroy_delta(delta->next);
				delta->next = NULL;
			}
		}
		if (ok == LOG_OK && delta == d && oldest == commit_ts)
			ok = tr_merge_delta(tr, delta);
	} else if (ok == LOG_OK && tr->parent) /* move delta into older and cleanup current save points */
		ATOMIC_PTR_SET(&c->data, savepoint_commit_delta(delta, commit_ts));
	return ok;
}

static int
log_update_idx( sql_trans *tr, sql_change *change)
{
	sql_idx *i = (sql_idx*)change->obj;

	if (!isTempTable(i->t) && !tr->parent) /* don't write save point commits */
		return tr_log_delta(tr, ATOMIC_PTR_GET(&i->data), i->t->bootstrap?0:LOG_IDX, i->base.id);
	return LOG_OK;
}

static int
commit_update_idx_( sql_trans *tr, sql_idx *i, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_delta *delta = ATOMIC_PTR_GET(&i->data);
	int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;

	(void)oldest;
	if (isTempTable(i->t)) {
		if (commit_ts) { /* commit */
			if (i->t->commit_action == CA_COMMIT || i->t->commit_action == CA_PRESERVE)
				commit_delta(tr, delta);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_delta(tr, delta);
		} else { /* rollback */
			if (i->t->commit_action == CA_COMMIT/* || i->t->commit_action == CA_PRESERVE*/)
				rollback_delta(tr, delta, type);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_delta(tr, delta);
		}
		i->t->base.flags = i->base.flags = 0;
	}
	return ok;
}

static int
commit_update_idx( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_idx *i = (sql_idx*)change->obj;
	sql_delta *delta = ATOMIC_PTR_GET(&i->data);

	if (isTempTable(i->t))
		return commit_update_idx_( tr, i, commit_ts, oldest);
	if (commit_ts)
		delta->ts = commit_ts;
	if (!commit_ts) { /* rollback */
		sql_delta *d = change->data, *o = ATOMIC_PTR_GET(&i->data);

		if (o != d) {
			while(o && o->next != d)
				o = o->next;
		}
		if (o == ATOMIC_PTR_GET(&i->data))
			ATOMIC_PTR_SET(&i->data, d->next);
		else
			o->next = d->next;
		d->next = NULL;
		destroy_delta(d);
	} else if (ok == LOG_OK && !tr->parent) {
		sql_delta *d = delta;
		/* clean up and merge deltas */
		while (delta && delta->ts > oldest) {
			delta = delta->next;
		}
		if (delta && delta != d) {
			if (delta->next) {
				ok = destroy_delta(delta->next);
				delta->next = NULL;
			}
		}
		if (ok == LOG_OK && delta == d && oldest == commit_ts)
			ok = tr_merge_delta(tr, delta);
	} else if (ok == LOG_OK && tr->parent) /* cleanup older save points */
		ATOMIC_PTR_SET(&i->data, savepoint_commit_delta(delta, commit_ts));
	return ok;
}

static sql_dbat *
savepoint_commit_dbat( sql_dbat *dbat, ulng commit_ts)
{
	if (dbat && dbat->ts == commit_ts && dbat->next) {
		sql_dbat *od = dbat->next;
		if (od->ts == commit_ts) {
			sql_dbat t = *od, *n = od->next;
			*od = *dbat;
			od->next = n;
			*dbat = t;
			dbat->next = NULL;
			destroy_dbat(dbat);
			return od;
		}
	}
	return dbat;
}

static int
log_update_del( sql_trans *tr, sql_change *change)
{
	sql_table *t = (sql_table*)change->obj;

	if (!isTempTable(t) && !tr->parent) /* don't write save point commits */
		return tr_log_dbat(tr, ATOMIC_PTR_GET(&t->data), t->bootstrap?0:LOG_TAB, t->base.id);
	return LOG_OK;
}

static int
rollback_dbat(sql_trans *tr, sql_dbat *dbat)
{
	(void)tr;
	(void)dbat;
	return LOG_OK;
}

static int
commit_dbat(sql_trans *tr, sql_dbat *dbat)
{
	(void)tr;
	(void)dbat;
	return LOG_OK;
}

static int
commit_update_del( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;
	sql_dbat *dbat = ATOMIC_PTR_GET(&t->data);

	if (isTempTable(t)) {
		if (commit_ts) { /* commit */
			if (t->commit_action == CA_COMMIT || t->commit_action == CA_PRESERVE)
				commit_dbat(tr, dbat);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_dbat(tr, dbat);
		} else { /* rollback */
			if (t->commit_action == CA_COMMIT/* || t->commit_action == CA_PRESERVE*/)
				rollback_dbat(tr, dbat);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_dbat(tr, dbat);
		}
		t->base.flags = 0;
		return ok;
	}
	if (!isTempTable(t))
		dbat->ts = commit_ts;
	if (!commit_ts) { /* rollback */
		sql_dbat *d = change->data, *o = ATOMIC_PTR_GET(&t->data);

		if (o != d) {
			while(o && o->next != d)
				o = o->next;
		}
		if (o == ATOMIC_PTR_GET(&t->data))
			ATOMIC_PTR_SET(&t->data, d->next);
		else
			o->next = d->next;
		d->next = NULL;
		destroy_dbat(d);
	} else if (ok == LOG_OK && !tr->parent) {
		sql_dbat *d = dbat;
		/* clean up and merge deltas */
		while (dbat && dbat->ts > oldest) {
			dbat = dbat->next;
		}
		if (dbat && dbat != d) {
			if (dbat->next) {
				ok = destroy_dbat(dbat->next);
				dbat->next = NULL;
			}
		}
		if (ok == LOG_OK && dbat == d && oldest == commit_ts)
			ok = tr_merge_dbat(dbat);
	} else if (ok == LOG_OK && tr->parent) {/* cleanup older save points */
		ATOMIC_PTR_SET(&t->data, savepoint_commit_dbat(dbat, commit_ts));
	}
	return ok;
}

/* only rollback (content version) case for now */
static int
tc_gc_col( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	sqlstore *store = Store;
	sql_column *c = (sql_column*)change->obj;

	(void)store;
	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&c->data) != change->data || isTempTable(c->t)) /* data is freed by commit */
		return 1;
	if (commit_ts && commit_ts >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data;
	if (d->next) {
		if (d->ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_delta(d->next);
		d->next = NULL;
	}
	return 1;
}

static int
tc_gc_idx( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	sqlstore *store = Store;
	sql_idx *i = (sql_idx*)change->obj;

	(void)store;
	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&i->data) != change->data || isTempTable(i->t)) /* data is freed by commit */
		return 1;
	if (commit_ts && commit_ts >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data;
	if (d->next) {
		if (d->ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_delta(d->next);
		d->next = NULL;
	}
	return 1;
}

static int
tc_gc_del( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	sqlstore *store = Store;
	sql_table *t = (sql_table*)change->obj;

	(void)store;
	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&t->data) != change->data || isTempTable(t)) /* data is freed by commit */
		return 1;
	if (commit_ts && commit_ts >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_dbat *d = (sql_dbat*)change->data;
	if (d->next) {
		if (d->ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_dbat(d->next);
		d->next = NULL;
	}
	return 1;
}

void
bat_storage_init( store_functions *sf)
{
	sf->bind_col = (bind_col_fptr)&bind_col;
	sf->bind_idx = (bind_idx_fptr)&bind_idx;
	sf->bind_del = (bind_del_fptr)&bind_del;

	sf->append_col = (append_col_fptr)&append_col;
	sf->append_idx = (append_idx_fptr)&append_idx;

	sf->append_col_prep = (modify_col_prep_fptr)&append_col_prepare;
	sf->append_idx_prep = (modify_idx_prep_fptr)&append_idx_prepare;
	sf->append_col_exec = (append_col_exec_fptr)&append_col_execute;
	sf->update_col = (update_col_fptr)&update_col;
	sf->update_idx = (update_idx_fptr)&update_idx;

	sf->update_col_prep = (modify_col_prep_fptr)&update_col_prepare;
	sf->update_idx_prep = (modify_idx_prep_fptr)&update_idx_prepare;
	sf->update_col_exec = (update_col_exec_fptr)&update_col_execute;

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
	sf->nonil_col = (prop_col_fptr)&nonil_col;
	sf->col_min_value = (proprec_col_fptr)&col_min_value;
	sf->col_max_value = (proprec_col_fptr)&col_max_value;

	sf->col_dup = (col_dup_fptr)&col_dup;
	sf->idx_dup = (idx_dup_fptr)&idx_dup;
	sf->del_dup = (del_dup_fptr)&del_dup;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->destroy_col = (destroy_col_fptr)&destroy_col;
	sf->destroy_idx = (destroy_idx_fptr)&destroy_idx;
	sf->destroy_del = (destroy_del_fptr)&destroy_del;

	/* change into drop_* */
	sf->log_destroy_col = (log_destroy_col_fptr)&log_destroy_col;
	sf->log_destroy_idx = (log_destroy_idx_fptr)&log_destroy_idx;
	sf->log_destroy_del = (log_destroy_del_fptr)&log_destroy_del;

	sf->clear_table = (clear_table_fptr)&clear_table;

	sf->upgrade_col = (upgrade_col_fptr)&upgrade_col;
	sf->upgrade_idx = (upgrade_idx_fptr)&upgrade_idx;
	sf->upgrade_del = (upgrade_del_fptr)&upgrade_del;
	sf->cleanup = (cleanup_fptr)&cleanup;
}
