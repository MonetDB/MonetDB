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

static MT_Lock segs_lock = MT_LOCK_INITIALIZER("segs_lock");
#define NR_TABLE_LOCKS 64
static MT_Lock table_locks[NR_TABLE_LOCKS]; /* set of locks to protect table changes (claim) */

static MT_Lock destroy_lock = MT_LOCK_INITIALIZER("destroy_lock");
storage *tobe_destroyed_dbat = NULL;
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
	while (d->next && d->cs.wtime > ts)
		d = d->next;
	return d;
}

storage *
timestamp_dbat( storage *d, int ts)
{
	while (d->next && d->cs.wtime > ts)
		d = d->next;
	return d;
}

static size_t
count_inserts( segment *s, sql_trans *tr)
{
	size_t cnt = 0;

	while(s) {
		if (s->owner == tr)
			cnt += s->end - s->start;
		s = s->next;
	}
	return cnt;
}

static size_t
count_col(sql_trans *tr, sql_column *c, int access)
{
	storage *d;
	sql_delta *ds;
	sql_table *t = c->t;

	if (!isTable(c->t))
		return 0;
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, t->base.stime);
	}
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, c->base.stime);
	}
	d = t->data;
	ds = c->data;
	if (!d)
		return 0;
	if (access == 2)
		return ds?ds->cs.ucnt:0;
	if (access == 1)
		return count_inserts(d->segs->head, tr);
	return d->end;
}

static size_t
dcount_col(sql_trans *tr, sql_column *c)
{
	sql_delta *b;
	assert(0);

	if (!isTable(c->t))
		return 0;
	if (!c->data) {
		sql_column *oc = tr_find_column(tr->parent, c);
		c->data = timestamp_delta(oc->data, c->base.stime);
	}
	b = c->data;
	if (!b)
		return 1;
	assert(0);
	/* TDOO */
	return 0;
}

static size_t
count_idx(sql_trans *tr, sql_idx *i, int access)
{
	storage *d;
	sql_delta *ds;
	sql_table *t = i->t;

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, t->base.stime);
	}
	if (!i->data) {
		sql_idx *oi = tr_find_idx(tr->parent, i);
		i->data = timestamp_delta(oi->data, i->base.stime);
	}
	d = t->data;
	ds = i->data;
	if (!d)
		return 0;
	if (access == 2)
		return ds?ds->cs.ucnt:0;
	if (access == 1)
		return count_inserts(d->segs->head, tr);
	return d->end;
}

static int
cs_real_update_bats( column_storage *cs, BAT **Ui, BAT **Uv)
{
	BAT *ui = temp_descriptor(cs->uibid);
	BAT *uv = temp_descriptor(cs->uvbid);

	if (ui == NULL || uv == NULL) {
		bat_destroy(ui);
		bat_destroy(uv);
		return LOG_ERR;
	}
	assert(ui && uv);
	if (isEbat(ui)){
		temp_destroy(cs->uibid);
		cs->uibid = temp_copy(ui->batCacheid, false);
		bat_destroy(ui);
		if (cs->uibid == BID_NIL ||
		    (ui = temp_descriptor(cs->uibid)) == NULL) {
			bat_destroy(uv);
			return LOG_ERR;
		}
	}
	if (isEbat(uv)){
		temp_destroy(cs->uvbid);
		cs->uvbid = temp_copy(uv->batCacheid, false);
		bat_destroy(uv);
		if (cs->uvbid == BID_NIL ||
		    (uv = temp_descriptor(cs->uvbid)) == NULL) {
			bat_destroy(ui);
			return LOG_ERR;
		}
	}
	*Ui = ui;
	*Uv = uv;
	return LOG_OK;
}

static size_t
count_deletes(storage *d)
{
	/* needs to be optimized */
	BAT *b = temp_descriptor(d->cs.bid);
	lng cnt = 0;

	if (!d->cs.ucnt) {
		if (BATsum(&cnt, TYPE_lng, b, NULL, true, false, false) != GDK_SUCCEED) {
			bat_destroy(b);
			return 0;
		}
	} else {
		BAT *ui, *uv, *c;

		if (cs_real_update_bats(&d->cs, &ui, &uv) == LOG_ERR) {
			assert(0);
			bat_destroy(b);
			return 0;
		}
		c = COLcopy(b, b->ttype, true, TRANSIENT);
		bat_destroy(b);
		if (BATreplace(c, ui, uv, true) != GDK_SUCCEED) {
			BBPunfix(c->batCacheid);
			return 0;
		}
		BBPunfix(ui->batCacheid);
		BBPunfix(uv->batCacheid);
		if (BATsum(&cnt, TYPE_lng, c, NULL, true, false, false) != GDK_SUCCEED) {
			BBPunfix(c->batCacheid);
			return 0;
		}
	}
	return cnt;
}

static size_t
count_del(sql_trans *tr, sql_table *t, int access)
{
	storage *d;

	if (!isTable(t))
		return 0;
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, t->base.stime);
	}
	d = t->data;
	if (!d)
		return 0;
	if (access == 2)
		return d->cs.ucnt;
	if (access == 1)
		return count_inserts(d->segs->head, tr);
	return count_deletes(d);
}

static BAT *
cs_bind_ubat( column_storage *cs, int access, int type)
{
	BAT *b;

	/* TODO should deduplicate oids (only last update) */
	assert(access == RD_UPD_ID || access == RD_UPD_VAL);
	if (cs->uibid && cs->uvbid) {
		if (access == RD_UPD_ID)
			b = temp_descriptor(cs->uibid);
		else
			b = temp_descriptor(cs->uvbid);
	} else {
		log_bid bb;

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
cs_bind_bat( column_storage *cs, int access, size_t cnt)
{
	BAT *b;

	assert(access == RDONLY || access == QUICK);
	assert(cs != NULL);
	if (access == QUICK)
		return quick_descriptor(cs->bid);
	assert(cs->bid);
	b = temp_descriptor(cs->bid);
	if (b == NULL)
		return NULL;
	bat_set_access(b, BAT_READ);
	/* return slice */
	return BATslice(b, 0, cnt);
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
	if (tr && access != QUICK)
		c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->stime;
	sql_delta *s = c->data;
	if (access == RD_UPD_ID || access == RD_UPD_VAL) {
		return cs_bind_ubat( &s->cs, access, c->type.type->localtype);
	} else {
		size_t cnt = count_col(tr, c, 0);
		return cs_bind_bat( &s->cs, access, cnt);
	}
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
	if (tr && access != QUICK)
		i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->stime;
	sql_delta *s = i->data;
	if (access == RD_UPD_ID || access == RD_UPD_VAL) {
		return cs_bind_ubat( &s->cs, access, (oid_index(i->type))?TYPE_oid:TYPE_lng);
	} else {
		size_t cnt = count_idx(tr, i, 0);
		return cs_bind_bat( &s->cs, access, cnt);
	}
}

static BAT *
bind_del(sql_trans *tr, sql_table *t, int access)
{
	assert(!store_initialized || tr != gtrans);
	assert(tr == gtrans || access == QUICK || tr->active);
	if (!isTable(t))
		return NULL;
	if (!t->data) {
		sql_table *ot = tr_find_table(tr->parent, t);
		t->data = timestamp_dbat(ot->data, t->base.stime);
	}
	if (tr && access != QUICK)
		t->base.rtime = t->s->base.rtime = tr->stime;
	storage *s = t->data;
	if (access == RD_UPD_ID || access == RD_UPD_VAL) {
		return cs_bind_ubat( &s->cs, access, TYPE_msk);
	} else {
		return cs_bind_bat( &s->cs, access, s->end);
	}
}

static int
cs_update_bat( column_storage *cs, BAT *tids, BAT *updates, int is_new)
{
	if (!BATcount(tids))
		return LOG_OK;

	/* handle cleared and updates on just inserted bits */
	if (!is_new && !cs->cleared && cs->uibid && cs->uvbid) {
		BAT *ui, *uv;

		if (cs_real_update_bats(cs, &ui, &uv) == LOG_ERR)
			return LOG_ERR;

		assert(BATcount(tids) == BATcount(updates));
		if (BATappend(ui, tids, NULL, true) != GDK_SUCCEED ||
		    BATappend(uv, updates, NULL, true) != GDK_SUCCEED) {
			bat_destroy(ui);
			bat_destroy(uv);
			return LOG_ERR;
		}
		assert(BATcount(tids) == BATcount(updates));
		assert(BATcount(ui) == BATcount(uv));
		bat_destroy(ui);
		bat_destroy(uv);
		cs->ucnt += BATcount(tids);
	} else if (is_new || cs->cleared) {
		BAT *b = temp_descriptor(cs->bid);

		if (b == NULL)
			return LOG_ERR;
		if (BATcount(b)==0) { /* alter add column */
			if (BATappend(b, updates, NULL, true) != GDK_SUCCEED) {
				bat_destroy(b);
				return LOG_ERR;
			}
		} else if (BATreplace(b, tids, updates, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
		bat_destroy(b);
	}
	return LOG_OK;
}

static int
delta_update_bat( sql_delta *bat, BAT *tids, BAT *updates, int is_new)
{
	return cs_update_bat(&bat->cs, tids, updates, is_new);
}

static int
cs_update_val( column_storage *cs, oid rid, void *upd, int is_new)
{
	assert(!is_oid_nil(rid));

	if (!is_new && !cs->cleared && cs->uibid && cs->uvbid) {
		BAT *ui, *uv;

		if (cs_real_update_bats(cs, &ui, &uv) == LOG_ERR)
			return LOG_ERR;

		assert(BATcount(ui) == BATcount(uv));
		if (BUNappend(ui, (ptr) &rid, true) != GDK_SUCCEED ||
		    BUNappend(uv, (ptr) upd, true) != GDK_SUCCEED) {
			assert(0);
			bat_destroy(ui);
			bat_destroy(uv);
			return LOG_ERR;
		}
		assert(BATcount(ui) == BATcount(uv));
		bat_destroy(ui);
		bat_destroy(uv);
		cs->ucnt++;
	} else if (is_new || cs->cleared) {
		BAT *b = NULL;

		if((b = temp_descriptor(cs->bid)) == NULL)
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
delta_update_val( sql_delta *bat, oid rid, void *upd, int is_new)
{
	return cs_update_val(&bat->cs, rid, upd, is_new);
}

static int
dup_cs(column_storage *ocs, column_storage *cs, int type, int c_isnew, int temp)
{
	(void)type;
	cs->bid = ocs->bid;
	cs->uibid = ocs->uibid;
	cs->uvbid = ocs->uvbid;
	cs->ucnt = ocs->ucnt;
	cs->wtime = ocs->wtime;
	//cs->cleared = ocs->cleared;

	if (temp) {
		cs->bid = temp_copy(cs->bid, 1);
		if (cs->bid == BID_NIL)
			return LOG_ERR;
	} else {
		(void)c_isnew;
		/* move the bat to the new col, fixup the old col*/
		//if (oc_isnew) {
		//	BAT *b = ...
		//	bat_set_access(b, BAT_READ);
		//	bat_destroy(b);
		//}
		temp_dup(cs->bid);
	}
	if (!temp) {
		if (cs->uibid && cs->uvbid) {
			ocs->uibid = ebat_copy(cs->uibid);
			ocs->uvbid = ebat_copy(cs->uvbid);
			if (ocs->uibid == BID_NIL ||
			    ocs->uvbid == BID_NIL)
				return LOG_ERR;
		} else {
			cs->uibid = e_bat(TYPE_oid);
			cs->uvbid = e_bat(type);
			if (cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
				return LOG_ERR;
		}
	}
	return LOG_OK;
}

static int
dup_bat(sql_table *t, sql_delta *obat, sql_delta *bat, int type, int c_isnew)
{
	int temp = isTempTable(t);
	if (!obat)
		return LOG_OK;
	return dup_cs(&obat->cs, &bat->cs, type, c_isnew, temp);

}

static int
bind_col_data(sql_trans *tr, sql_column *c)
{
	if (!c->data || !c->base.allocated) {
		int type = c->type.type->localtype;
		sql_column *oc = tr_find_column(tr->parent, c);
		sql_delta* bat = ZNEW(sql_delta),*obat;
		if(!bat)
			return LOG_ERR;
		c->data = bat;
		obat = timestamp_delta(oc->data, c->base.stime);
		if(dup_bat(c->t, obat, bat, type, isNew(c)) == LOG_ERR)
			return LOG_ERR;
		c->base.allocated = 1;
	}
	return LOG_OK;
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, int tpe)
{
	BAT *b = tids;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b))
		return LOG_OK;

	if (bind_col_data(tr, c) == LOG_ERR)
		return LOG_ERR;

	bat = c->data;
	bat->cs.wtime = c->base.wtime = c->t->base.wtime = c->t->s->base.wtime = tr->wtime = tr->wstime;
	assert(tr != gtrans);
	c->base.rtime = c->t->base.rtime = c->t->s->base.rtime = tr->stime;
	if (tpe == TYPE_bat)
		return delta_update_bat(bat, tids, upd, isNew(c));
	else
		return delta_update_val(bat, *(oid*)tids, upd, isNew(c));
}

static int
bind_idx_data(sql_trans *tr, sql_idx *i)
{
	if (!i->data || !i->base.allocated) {
		int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;
		sql_idx *oi = tr_find_idx(tr->parent, i);
		sql_delta* bat = ZNEW(sql_delta), *obat;
		if(!bat)
			return LOG_ERR;
		i->data = bat;
		obat = timestamp_delta(oi->data, i->base.stime);
		if(dup_bat(i->t, obat, bat, type, isNew(i)) == LOG_ERR)
			return LOG_ERR;
		i->base.allocated = 1;
	}
	return LOG_OK;
}

static int
update_idx(sql_trans *tr, sql_idx * i, void *tids, void *upd, int tpe)
{
	BAT *b = tids;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b))
		return LOG_OK;

	if (bind_idx_data(tr, i) == LOG_ERR)
		return LOG_ERR;

	bat = i->data;
	bat->cs.wtime = i->base.wtime = i->t->base.wtime = i->t->s->base.wtime = tr->wtime = tr->wstime;
	assert(tr != gtrans);
	i->base.rtime = i->t->base.rtime = i->t->s->base.rtime = tr->stime;
	if (tpe == TYPE_bat)
		return delta_update_bat(bat, tids, upd, isNew(i));
	else
		assert(0);
	return LOG_OK;
}

static int
delta_append_bat( sql_delta *bat, size_t offset, BAT *i, sql_table *t )
{
	BAT *b;

	(void)t;
	if (!BATcount(i))
		return LOG_OK;
	b = temp_descriptor(bat->cs.bid); /* TODO VIEW get parent */
	if (b == NULL)
		return LOG_ERR;

	if (BATcount(b) >= offset+BATcount(i)){
		BAT *ui = BATdense(0, offset, BATcount(i));
		if (BATreplace(b, ui, i, true) != GDK_SUCCEED) {
			bat_destroy(b);
			bat_destroy(ui);
			return LOG_ERR;
		}
		bat_destroy(ui);
	} else {
		//assert (isNew(t) || isTempTable(t) || bat->cs.cleared);
		if (BATcount(b) < offset) { /* add space */
			const void *tv = ATOMnilptr(b->ttype);
			lng i, d = offset - BATcount(b);
			for(i=0;i<d;i++) {
				if (BUNappend(b, tv, true) != GDK_SUCCEED) {
					bat_destroy(b);
					return LOG_ERR;
				}
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
	}
	bat_destroy(b);
	return LOG_OK;
}

static int
delta_append_val( sql_delta *bat, size_t offset, void *i, sql_table *t )
{
	BAT *b;

	(void)t;
	b = temp_descriptor(bat->cs.bid); /* TODO VIEW get parent */
	if(b == NULL)
		return LOG_ERR;

	if (BATcount(b) > offset){
		if (BUNreplace(b, offset, i, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
	} else {
		//assert (isNew(t) || isTempTable(t) || bat->cs.cleared);
		if (BATcount(b) < offset) { /* add space */
			const void *tv = ATOMnilptr(b->ttype);
			lng i, d = offset - BATcount(b);
			for(i=0;i<d;i++) {
				if (BUNappend(b, tv, true) != GDK_SUCCEED) {
					bat_destroy(b);
					return LOG_ERR;
				}
			}
		}
		if (BUNappend(b, i, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
	}
	bat_destroy(b);
	return LOG_OK;
}

static int
dup_col(sql_trans *tr, sql_column *oc, sql_column *c )
{
	(void)tr;
	int ok = LOG_OK;

	if (oc->data) {
		int type = c->type.type->localtype;
		sql_delta *bat = ZNEW(sql_delta), *obat = oc->data;
		if (!bat)
			ok = LOG_ERR;
		else {
			c->data = bat;
			ok = dup_bat(c->t, obat, bat, type, isNew(c));
			c->base.allocated = 1;
		}
	}
	return ok;
}

static int
dup_idx(sql_trans *tr, sql_idx *i, sql_idx *ni )
{
	(void)tr;
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
			ok = dup_bat(ni->t, obat, bat, type, isNew(ni));
			ni->base.allocated = 1;
		}
	}
	return ok;
}

static segments*
dup_segments(segments *s)
{
	sql_ref_inc(&s->r);
	return s;
}

static segment *
new_segment(segment *o, sql_trans *tr, size_t cnt)
{
	segment *n = (segment*)GDKmalloc(sizeof(segment));

	if (n) {
		n->owner = tr?tr:0;
		n->start = o?o->end:0;
		n->end = n->start + cnt;
		n->next = o;
	}
	return n;
}

static segments *
new_segments(size_t cnt)
{
	segments *n = (segments*)GDKmalloc(sizeof(segments));

	if (n) {
		sql_ref_init(&n->r);
		n->head = new_segment(NULL, NULL, cnt);
	       	n->end = n->head->end;
	}
	return n;
}


static int
dup_dbat(storage *obat, storage *bat, int is_new, int temp)
{
	if (!obat)
		return LOG_OK;
	if (temp) {
		bat->segs = new_segments(0);
		bat->end = bat->segs->end;
	} else {
		bat->end = obat->end = obat->segs->end;
		MT_lock_set(&segs_lock);
		bat->segs = dup_segments(obat->segs);
		MT_lock_unset(&segs_lock);
		assert(bat->end <= bat->segs->end);
	}
	return dup_cs(&obat->cs, &bat->cs, TYPE_msk, is_new, temp);
}

static int
dup_del(sql_trans *tr, sql_table *ot, sql_table *t)
{
	(void)tr;
	int ok;
	storage *bat = ZNEW(storage), *obat = ot->data;
	if (!bat)
		return LOG_ERR;
	t->data = bat;
	ok = dup_dbat(obat, bat, isNew(t), isTempTable(t));
	assert(t->base.allocated == 0);
	t->base.allocated = 1;
	return ok;
}

static int
append_col(sql_trans *tr, sql_column *c, size_t offset, void *i, int tpe)
{
	int ok = LOG_OK;
	BAT *b = i;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if (bind_col_data(tr, c) == LOG_ERR)
		return LOG_ERR;

	bat = c->data;
	/* appends only write */
	bat->cs.wtime = c->base.atime = c->t->base.atime = c->t->s->base.atime = tr->atime = tr->wstime;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(bat, offset, i, c->t);
	else
		ok = delta_append_val(bat, offset, i, c->t);
	return ok;
}

static int
append_idx(sql_trans *tr, sql_idx * i, size_t offset, void *ib, int tpe)
{
	int ok = LOG_OK;
	BAT *b = ib;
	sql_delta *bat;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if (bind_idx_data(tr, i) == LOG_ERR)
		return LOG_ERR;

	bat = i->data;
	/* appends only write */
	bat->cs.wtime = i->base.atime = i->t->base.atime = i->t->s->base.atime = tr->atime = tr->wstime;
	if (tpe == TYPE_bat)
		ok = delta_append_bat(bat, offset, ib, i->t);
	else
		ok = delta_append_val(bat, offset, ib, i->t);
	return ok;
}

static int
delta_delete_bat( storage *bat, BAT *i, int is_new)
{
	/* update ids */
	msk T = TRUE;
	BAT *t = BATconstant(i->hseqbase, TYPE_msk, &T, BATcount(i), TRANSIENT);
	int ok = LOG_OK;

	if (t) {
		ok = cs_update_bat( &bat->cs, i, t, is_new);
	}
	BBPunfix(t->batCacheid);
	return ok;
}

static int
delta_delete_val( storage *bat, oid rid, int is_new)
{
	/* update pos */
	msk T = TRUE;
	return cs_update_val(&bat->cs, rid, &T, is_new);
}

static int
bind_del_data(sql_trans *tr, sql_table *t)
{
	if (!t->data || !t->base.allocated) {
		sql_table *ot = tr_find_table(tr->parent, t);
		storage *bat = ZNEW(storage), *obat;
		if(!bat)
			return LOG_ERR;
		t->data = bat;
		obat = timestamp_dbat(ot->data, t->base.stime);
		dup_dbat(obat, bat, isNew(ot), isTempTable(t));
		t->base.allocated = 1;
	}
	return LOG_OK;
}

static int
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	BAT *b = ib;
	storage *bat;
	int ok = LOG_OK;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if (bind_del_data(tr, t) == LOG_ERR)
		return LOG_ERR;

	bat = t->data;

	/* deletes only write */
	bat->cs.wtime = t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	if (tpe == TYPE_bat)
		ok = delta_delete_bat(bat, ib, isNew(t));
	else
		ok = delta_delete_val(bat, *(oid*)ib, isNew(t));
	return ok;
}

static int
claim_cs(column_storage *cs, size_t cnt)
{
	BAT *b = temp_descriptor(cs->bid);

	if (!b)
		return LOG_ERR;
	const void *nilptr = ATOMnilptr(b->ttype);

	for(size_t i=0; i<cnt; i++){
		if (BUNappend(b, nilptr, TRUE) != GDK_SUCCEED)
			return LOG_ERR;
	}
	return LOG_OK;
}

static int
table_claim_space(sql_trans *tr, sql_table *t, size_t cnt)
{
	node *n = t->columns.set->h;
	sql_column *c = n->data;

	t->base.atime = t->s->base.atime = tr->atime = tr->wstime;
	c->base.atime = tr->wstime;

	for (n = t->columns.set->h; n; n = n->next) {
		c = n->data;
		c->base.atime = tr->wstime;

		if (bind_col_data(tr, c) == LOG_ERR)
			return LOG_ERR;
		if (claim_cs(c->data, cnt) == LOG_ERR)
			return LOG_ERR;
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *ci = n->data;

			ci->base.atime = tr->wstime;
			if (isTable(ci->t) && idx_has_column(ci->type)) {
				if (bind_idx_data(tr, ci) == LOG_ERR)
					return LOG_ERR;
				if (claim_cs(ci->data, cnt) == LOG_ERR)
					return LOG_ERR;
			}
		}
	}
	return LOG_OK;
}

static void
lock_table(sqlid id)
{
	MT_lock_set(&table_locks[id&(NR_TABLE_LOCKS-1)]);
}

static void
unlock_table(sqlid id)
{
	MT_lock_unset(&table_locks[id&(NR_TABLE_LOCKS-1)]);
}

static BAT *
mask_bat(size_t cnt, msk val)
{
	BAT *b = COLnew(0, TYPE_msk, cnt, TRANSIENT);

	if (b) {
		size_t nr = (cnt+31)/32;
		int *p = (int*)b->T.heap->base;
		for(size_t i = 0; i<nr; i++) {
			p[i] = (val)?0xffffffff:0;
		}
		BATsetcount(b, cnt);
	}
	return b;
}
/*
 * Claim cnt slots to store the tuples. The claim_tab should claim storage on the level
 * of the global transaction and mark the newly added storage slots unused on the global
 * level but used on the local transaction level. Besides this the local transaction needs
 * to update (and mark unused) any slot inbetween the old end and new slots.
 * */
static size_t
claim_tab(sql_trans *tr, sql_table *t, size_t cnt)
{
	storage *s, *ps = NULL;
	BUN slot = 0;

	if (bind_del_data(tr, t) == LOG_ERR)
		return 0;

	/* use (resizeable) array of locks like BBP */
	lock_table(t->base.id);

	s = t->data;
	if (isNew(t) || isTempTable(t) || s->cs.cleared) {
		/* a new table ie no competition */
		ps = s;
	} else {
		/* find parent which knows about the slots to use */
		sql_table *ot = tr_find_base_table(tr->parent, t);
		ps = timestamp_dbat(ot->data, t->base.stime);
	}
	if (!ps)
		return LOG_ERR;

	slot = ps->end;
	if (isNew(t) || isTempTable(t) || s->cs.cleared) {
		ps->end += cnt;
		if (ps->segs->head)
			ps->segs->end = ps->segs->head->end = ps->end;
	} else {
		assert(ps->end <= ps->segs->end);
		ps->segs->head = new_segment(ps->segs->head, tr, cnt);
		s->end = ps->end = ps->segs->end = ps->segs->head->end;
	}

	BAT *b = temp_descriptor(s->cs.bid); /* use s->cs.bid, as its equal ps->cs.bid or for cleared tables its a private bid */

	assert(isNew(t) || isTempTable(t) || s->cs.cleared || BATcount(b) == slot);

	msk deleted = FALSE;

	/* general case, write deleted in the central bat (ie others don't see these values) and
	 * insert rows into the update bats */
	if (!s->cs.cleared && ps != s && !isTempTable(t)) {
		/* add updates */
		BAT *ui, *uv;

		if (/* DISABLES CODE */ (0) && table_claim_space(tr, t, cnt) == LOG_ERR) {
			unlock_table(t->base.id);
			return LOG_ERR;
		}
		if (cs_real_update_bats(&s->cs, &ui, &uv) == LOG_ERR) {
			unlock_table(t->base.id);
			return LOG_ERR;
		}

		oid id = slot;
		BAT *uin = BATdense(0, id, cnt);
		BAT *uvn = mask_bat(cnt, deleted);
		if (!uin || !uvn ||
				BATappend(ui, uin, NULL, true) != GDK_SUCCEED ||
				BATappend(uv, uvn, NULL, true) != GDK_SUCCEED) {
			if (uin) bat_destroy(uin);
			if (uvn) bat_destroy(uvn);
			bat_destroy(ui);
			bat_destroy(uv);
			unlock_table(t->base.id);
			return LOG_ERR;
		}
		bat_destroy(uin);
		bat_destroy(uvn);
#if 0
		for(lng i=0; i<(lng)cnt; i++, id++) {
			/* create void-bat ui (id,cnt), msk-0's (write in chunks of 32bit */
			if (BUNappend(ui, &id, true) != GDK_SUCCEED ||
			    BUNappend(uv, &deleted, true) != GDK_SUCCEED) {
				bat_destroy(ui);
				bat_destroy(uv);
				unlock_table(t->base.id);
				return LOG_ERR;
			}
		}
#endif
		s->cs.ucnt += cnt;
	}

	assert(isNew(t) || isTempTable(t) || s->cs.cleared || BATcount(b) == slot);
	if (isNew(t) || isTempTable(t) || s->cs.cleared)
		deleted = FALSE;
	else /* persistent central copy needs space marked deleted (such that other transactions don't see these rows) */
		deleted = TRUE;
	/* TODO first up to 32 boundary, then int writes */
	for(lng i=0; i<(lng)cnt; i++) {
		if (BUNappend(b, &deleted, true) != GDK_SUCCEED) {
			bat_destroy(b);
			unlock_table(t->base.id);
			return LOG_ERR;
		}
	}
	bat_destroy(b);
	assert(isTempTable(t) || s->cs.cleared || ps->cs.bid == s->cs.bid);

	/* inserts only write */
	s->cs.wtime = t->base.atime = t->s->base.atime = tr->atime = tr->wstime;
	unlock_table(t->base.id);
	return (size_t)slot;
}

static int
sorted_col(sql_trans *tr, sql_column *col)
{
	int sorted = 0;

	assert(tr->active || tr == gtrans);
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

	assert(tr->active || tr == gtrans);
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

	assert(tr->active || tr == gtrans);
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
load_cs(column_storage *cs, int type, sqlid id)
{
	int bid = logger_find_bat(bat_logger, id);
	BAT *b = temp_descriptor(bid);
	if (!b)
		return LOG_ERR;
	cs->bid = temp_create(b);
	cs->ucnt = 0;
	cs->uibid = e_bat(TYPE_oid);
	cs->uvbid = e_bat(type);
	assert(cs->uibid && cs->uvbid);
	if(cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
		return LOG_ERR;
	return LOG_OK;
}

static int
log_create_delta(sql_delta *bat, sqlid id)
{
	int res = LOG_OK;
	gdk_return ok;
	BAT *b = temp_descriptor(bat->cs.bid);
	if (b == NULL)
		return LOG_ERR;
	if (!bat->cs.uibid)
		bat->cs.uibid = e_bat(TYPE_oid);
	if (!bat->cs.uvbid)
		bat->cs.uvbid = e_bat(b->ttype);
	assert(bat->cs.uibid && bat->cs.uvbid);
	if (bat->cs.uibid == BID_NIL || bat->cs.uvbid == BID_NIL)
		res = LOG_ERR;
	if (GDKinmemory())
		return res;

	bat_set_access(b, BAT_READ);
	ok = log_bat_persists(bat_logger, b, id);
	bat_destroy(b);
	if(res != LOG_OK)
		return res;
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
new_persistent_delta( sql_delta *bat)
{
	BAT *b = temp_descriptor(bat->cs.bid);

	if (b == NULL) {
		bat_destroy(b);
		return LOG_ERR;
	}
	bat->cs.ucnt = 0;
	bat_destroy(b);
	return LOG_OK;
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
		bat->cs.wtime = c->base.wtime = tr->wstime;
		c->base.allocated = 1;
	}

	if (!isNew(c) && !isTempTable(c->t)){
		c->base.wtime = 0;
		return load_cs(&bat->cs, type, c->base.id);
	} else if (bat && bat->cs.bid && !isTempTable(c->t)) {
		return new_persistent_delta(c->data);
	} else {
		BAT *b = bat_new(type, c->t->sz, PERSISTENT);
		if (!b) {
			ok = LOG_ERR;
		} else {
			sql_delta *d = c->data;

			bat_set_access(b, BAT_READ);
			d->cs.bid = temp_create(b);

			if (!isTempTable(c->t)) {
				bat->cs.uibid = e_bat(TYPE_oid);
				if (bat->cs.uibid == BID_NIL)
					ok = LOG_ERR;
				bat->cs.uvbid = e_bat(type);
				if(bat->cs.uvbid == BID_NIL)
					ok = LOG_ERR;
			}
			d->cs.ucnt = 0;
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
	return log_create_delta( c->data, c->base.id);
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
		bat->cs.wtime = ni->base.wtime = tr->wstime;
		ni->base.allocated = 1;
	}

	if (!isNew(ni) && !isTempTable(ni->t)){
		ni->base.wtime = 0;
		return load_cs(&bat->cs, type, ni->base.id);
	} else if (bat && bat->cs.bid && !isTempTable(ni->t)) {
		return new_persistent_delta(ni->data);
	} else {
		sql_column *c = ni->t->columns.set->h->data;
		sql_delta *d;

		if (!c->data) {
			sql_column *oc = tr_find_column(tr->parent, c);
			c->data = timestamp_delta(oc->data, c->base.stime);
		}
		d = c->data;
		/* Here we also handle indices created through alter stmts */
		/* These need to be created aligned to the existing data */


		if (d->cs.bid) {
			bat->cs.bid = copyBat(d->cs.bid, type, 0);
			if(bat->cs.bid == BID_NIL)
				ok = LOG_ERR;
		}
		if (!isTempTable(ni->t)) {
			bat->cs.uibid = e_bat(TYPE_oid);
			if (bat->cs.uibid == BID_NIL)
				ok = LOG_ERR;
			bat->cs.uvbid = e_bat(type);
			if(bat->cs.uvbid == BID_NIL)
				ok = LOG_ERR;
		}
		bat->cs.ucnt = 0;

	}
	return ok;
}

static int
log_create_idx(sql_trans *tr, sql_idx *ni)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(ni->t));
	return log_create_delta( ni->data, ni->base.id);
}

static int
load_storage(storage *bat, sqlid id)
{
	int ok = load_cs(&bat->cs, TYPE_msk, id);

	if (ok == LOG_OK) {
		bat->segs = new_segments(BATcount(quick_descriptor(bat->cs.bid)));
		bat->end = bat->segs->end;
	}
	return ok;
}

static int
create_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;
	BAT *b;
	storage *bat = t->data;

	if (!bat) {
		t->data = bat = ZNEW(storage);
		if(!bat)
			return LOG_ERR;
		bat->cs.wtime = t->base.wtime = t->s->base.wtime = tr->wstime;
		t->base.allocated = 1;
	}
	(void)tr;
	if (!isNew(t) && !isTempTable(t)) {
		t->base.wtime = 0;
		return load_storage(bat, t->base.id);
	} else if (bat->cs.bid && !isTempTable(t)) {
		return ok;
	} else if (!bat->cs.bid) {
		assert(!bat->segs && !bat->end);
		bat->segs = new_segments(0);
		bat->end = 0;

		b = bat_new(TYPE_msk, t->sz, PERSISTENT);
		if(b != NULL) {
			bat_set_access(b, BAT_READ);
			bat->cs.bid = temp_create(b);
			bat_destroy(b);
		} else {
			ok = LOG_ERR;
		}
	}
	return ok;
}

static int
log_create_storage( storage *bat, sqlid id)
{
	BAT *b;
	gdk_return ok;

	if (GDKinmemory())
		return LOG_OK;

	b = temp_descriptor(bat->cs.bid);
	if (b == NULL)
		return LOG_ERR;

	bat_set_access(b, BAT_READ);
	ok = log_bat_persists(bat_logger, b, id);
	bat_destroy(b);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_create_del(sql_trans *tr, sql_table *t)
{
	(void)tr;
	assert(tr->parent == gtrans && !isTempTable(t));
	return log_create_storage(t->data, t->base.id);
}

static int
log_destroy_delta(sql_trans *tr, sql_delta *b, sqlid id)
{
	gdk_return ok = GDK_SUCCEED;

	(void)tr;
	if (!GDKinmemory() && b && b->cs.bid)
		ok = log_bat_transient(bat_logger, id);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
destroy_cs(column_storage *cs)
{
	if (cs->uibid)
		temp_destroy(cs->uibid);
	if (cs->uvbid)
		temp_destroy(cs->uvbid);
	if (cs->bid)
		temp_destroy(cs->bid);
	cs->bid = cs->uibid = cs->uvbid = 0;
	return LOG_OK;
}

static int
destroy_bat(sql_delta *b)
{
	sql_delta *n;

	while(b) {
		n = b->next;
		destroy_cs(&b->cs);
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
		return log_destroy_delta(tr, c->data, c->base.id);
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
		return log_destroy_delta(tr, i->data, i->base.id);
	return LOG_OK;
}

static void
destroy_segs(segment *s)
{
	if (!s)
		return;
	while(s) {
		segment *n = s->next;
		_DELETE(s);
		s = n;
	}
}

static void
destroy_segments(segments *s)
{
	MT_lock_set(&segs_lock);
	if (sql_ref_dec(&s->r) > 0) {
		MT_lock_unset(&segs_lock);
		return;
	}
	MT_lock_unset(&segs_lock);
	destroy_segs(s->head);
	_DELETE(s);
}

static int
destroy_dbat(sql_trans *tr, storage *bat)
{
	storage *n;

	(void)tr;
	while(bat) {
		n = bat->next;
		destroy_cs(&bat->cs);
		destroy_segments(bat->segs);
		_DELETE(bat);
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
	MT_lock_set(&destroy_lock);
	sql_delta *n = b;

	if (n) {
		while(n->next)
			n = n->next;
		n->next = tobe_destroyed_delta;
		tobe_destroyed_delta = b;
	}
	MT_lock_unset(&destroy_lock);
	return LOG_OK;
}

static int
delayed_destroy_dbat(storage *b)
{
	MT_lock_set(&destroy_lock);
	storage *n = b;

	if (n) {
		while(n->next)
			n = n->next;
		n->next = tobe_destroyed_dbat;
		tobe_destroyed_dbat = b;
	}
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
log_destroy_dbat(sql_trans *tr, storage *bat, sqlid id)
{
	gdk_return ok = GDK_SUCCEED;

	(void)tr;
	if (!GDKinmemory() && bat && bat->cs.bid)
		ok = log_bat_transient(bat_logger, id);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_destroy_del(sql_trans *tr, sql_table *t)
{
	if (t->data && t->base.allocated)
		return log_destroy_dbat(tr, t->data, t->base.id);
	return LOG_OK;
}

static void
clear_cs(sql_trans *tr, column_storage *cs)
{
	BAT *b;

	if (cs->bid) {
		b = temp_descriptor(cs->bid);
		if (b) {
			if (tr != gtrans) {
				bat bid = cs->bid;
				cs->bid = temp_copy(bid, 1); /* create empty copy */
				temp_destroy(bid);
			} else {
				bat_clear(b);
				BATcommit(b, BUN_NONE);
			}
			bat_destroy(b);
		}
	}
	if (cs->uibid) {
		b = temp_descriptor(cs->uibid);
		if (b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b, BUN_NONE);
		}
		if (b)
			bat_destroy(b);
	}
	if (cs->uvbid) {
		b = temp_descriptor(cs->uvbid);
		if(b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b, BUN_NONE);
		}
		if (b)
			bat_destroy(b);
	}
	cs->cleared = 1;
	cs->ucnt = 0;
	cs->wtime = tr->wstime;
}

static void
clear_col(sql_trans *tr, sql_column *c)
{
	if (bind_col_data(tr, c) == LOG_ERR)
		return ;
	c->t->s->base.wtime = c->t->base.wtime = c->base.wtime = tr->wstime;
	if (c->data) {
		sql_delta *bat = c->data;
		clear_cs(tr, &bat->cs);
	}
}

static void
clear_idx(sql_trans *tr, sql_idx *i)
{
	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return ;
	if (bind_idx_data(tr, i) == LOG_ERR)
		return ;
	i->t->s->base.wtime = i->t->base.wtime = i->base.wtime = tr->wstime;
	if (i->data) {
		sql_delta *bat = i->data;
		clear_cs(tr, &bat->cs);
	}
}

static void
clear_del(sql_trans *tr, sql_table *t)
{
	if (bind_del_data(tr, t) == LOG_ERR)
		return;
	t->s->base.wtime = t->base.wtime = tr->wstime;
	storage *s = t->data;
	clear_cs(tr, &s->cs);

	if (s->segs)
		destroy_segments(s->segs);
	s->segs = new_segments(0);
	s->end = 0;
}

static BUN
clear_table(sql_trans *tr, sql_table *t)
{
	node *n = t->columns.set->h;
	sql_column *c = n->data;
	BUN sz = count_col(tr, c, 0);

	t->cleared = 1;
	t->base.wtime = t->s->base.wtime = tr->wtime = tr->wstime;
	c->base.wtime = tr->wstime;

	sz -= count_del(tr, t, 0);
	clear_del(tr, t);
	for (; n; n = n->next) {
		c = n->data;
		c->base.wtime = tr->wstime;

		clear_col(tr, c);
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *ci = n->data;

			ci->base.wtime = tr->wstime;
			if (isTable(ci->t) && idx_has_column(ci->type))
				clear_idx(tr, ci);
		}
	}
	return sz;
}

static int
minmax_col(sql_column *c)
{
	int ok = LOG_OK;
	sql_delta *cbat = c->data;
	BAT *cur;
	lng val;

	/* already set */
	if (!cbat || c->type.type->localtype >= TYPE_str || c->t->system)
		return ok;

	cur = temp_descriptor(cbat->cs.bid);
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
minmax_table(sql_table *t)
{
	int ok = LOG_OK;

	if (t->access > TABLE_WRITABLE) {
		for (node *n = t->columns.set->h; ok == LOG_OK && n; n = n->next) {
			sql_column *c = n->data;

			ok = minmax_col(c);
		}
	}
	return ok;
}

static int
minmax( sql_trans *tr )
{
	int ok = LOG_OK;
	node *sn;

	for(sn = tr->schemas.set->h; sn && ok == LOG_OK; sn = sn->next) {
		sql_schema *s = sn->data;

		if (!s->base.wtime && !s->base.atime)
			continue;
		if (!isTempSchema(s) && s->tables.set) {
			node *n;
			for (n = s->tables.set->h; n && ok == LOG_OK; n = n->next) {
				sql_table *t = n->data;

				if (isTable(t) && isGlobal(t))
					ok = minmax_table(t);
			}
		}
	}
	return LOG_OK;
}

static int
tr_update_cs( sql_trans *tr, column_storage *ocs, column_storage *ccs)
{
	int ok = LOG_OK;
	BAT *cur = NULL;

	(void)tr;
	assert(ATOMIC_GET(&store_nr_active)==1);
	assert (ocs->bid != 0 || tr != gtrans);

	int cleared = ccs->cleared;
	(void)cleared;
	if (ccs->cleared) {
		ccs->cleared = 0;
		assert(ccs->bid != ocs->bid);
		bat bid = ocs->bid;
		if (ccs->bid)
			ocs->bid = ccs->bid;
		else
			ocs->bid = temp_copy(ocs->bid, 1); /* create empty new bat */
		temp_destroy(bid);
		ccs->bid = ocs->bid;
		temp_dup(ocs->bid);
	}

	assert(ocs->bid == ccs->bid);
	cur = temp_descriptor(ocs->bid);
	if(!cur)
		return LOG_ERR;
	if (ccs->ucnt && ccs->uibid) {
		assert(!cleared);
		BAT *ui = temp_descriptor(ccs->uibid);
		BAT *uv = temp_descriptor(ccs->uvbid);
		if(!ui || !uv) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			return LOG_ERR;
		}

		assert(BATcount(ui) == BATcount(uv));
		/* any updates */
		assert(!isEbat(cur));
		if (BATreplace(cur, ui, uv, true) != GDK_SUCCEED) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			return LOG_ERR;
		}
		/* cleanup the old deltas */
		temp_destroy(ocs->uibid);
		temp_destroy(ocs->uvbid);
		ocs->uibid = e_bat(TYPE_oid);
		ocs->uvbid = e_bat(cur->ttype);
		if(ocs->uibid == BID_NIL || ocs->uvbid == BID_NIL)
			ok = LOG_ERR;
		temp_destroy(ccs->uibid);
		temp_destroy(ccs->uvbid);
		ccs->uibid = ccs->uvbid = 0;
		ccs->ucnt = ocs->ucnt = 0;
		bat_destroy(ui);
		bat_destroy(uv);
	}
	bat_destroy(cur);
	return ok;
}

static int
tr_update_delta( sql_trans *tr, sql_delta *obat, sql_delta *cbat)
{
	int ok = tr_update_cs( tr, &obat->cs, &cbat->cs);

	if (ok == LOG_OK && obat->next) {
		ok = destroy_bat(obat->next);
		obat->next = NULL;
	}
	return ok;
}

static int
tr_merge_cs( sql_trans *tr, column_storage *cs)
{
	int ok = LOG_OK;
	BAT *cur = NULL;

	(void)tr;
	assert(ATOMIC_GET(&store_nr_active)==1);
	assert (cs->bid != 0 || tr != gtrans);

	if (cs->bid) {
		cur = temp_descriptor(cs->bid);
		if(!cur)
			return LOG_ERR;
	}

	if (cs->ucnt) {
		BAT *ui = temp_descriptor(cs->uibid);
		BAT *uv = temp_descriptor(cs->uvbid);

		if(!ui || !uv) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			return LOG_ERR;
		}
		assert(BATcount(ui) == BATcount(uv));

		/* any updates */
		assert(!isEbat(cur));
		if (BATreplace(cur, ui, uv, true) != GDK_SUCCEED) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			return LOG_ERR;
		}
		/* cleanup the old deltas */
		temp_destroy(cs->uibid);
		temp_destroy(cs->uvbid);
		cs->uibid = e_bat(TYPE_oid);
		cs->uvbid = e_bat(cur->ttype);
		if(cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
			ok = LOG_ERR;
		cs->ucnt = 0;
		bat_destroy(ui);
		bat_destroy(uv);
	}
	cs->cleared = 0;
	bat_destroy(cur);
	return ok;
}

static int
tr_merge_delta( sql_trans *tr, sql_delta *obat)
{
	int ok = tr_merge_cs(tr, &obat->cs);
	if (obat->next) {
		ok = destroy_bat(obat->next);
		obat->next = NULL;
	}
	return ok;
}

static int
tr_update_dbat( sql_trans *tr, storage *ts, storage *fs)
{
	if (fs->cs.cleared) {
		destroy_segments(ts->segs);
		MT_lock_set(&segs_lock);
		ts->segs = dup_segments(fs->segs);
		MT_lock_unset(&segs_lock);
		ts->end = ts->segs->end;
		assert(ts->segs->head);
	} else {
		assert(ts->segs == fs->segs);
		/* merge segments or cleanup ? */
		segment *segs = ts->segs->head, *seg = segs;
		for (; segs; segs = segs->next) {
			if (segs->owner == tr || !segs->owner) {
				/* merge range */
				segs->owner = NULL;
				if (seg == segs) /* skip first */
					continue;
				if (seg->start == segs->end) {
					seg->start = segs->start;
					seg->next = segs->next;
					segs->next = NULL;
					destroy_segs(segs);
						segs = seg;
				} else {
					seg = segs; /* begin of new merge */
				}
			}
		}
		ts->end = ts->segs->end;
	}
	int ok = tr_update_cs( tr, &ts->cs, &fs->cs);
	if (ok == LOG_OK && ts->next) {
		ok = destroy_dbat(tr, ts->next);
		ts->next = NULL;
	}
	return ok;
}

static int
tr_merge_dbat(sql_trans *tr, storage *tdb)
{
	int ok = tr_merge_cs(tr, &tdb->cs);
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
			storage *b = ft->data;

			if (!tt->data)
				tt->base.allocated = ft->base.allocated;
			ft->data = NULL;
			b->next = tt->data;
			tt->data = b;

			while (b && b->cs.wtime >= oldest->stime)
				b = b->next;
			/* find table t->base.stime */
			ot = tr_find_table(oldest, tt);
			if (b && ot && b->cs.wtime < ot->base.stime) {
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

		if (ATOMIC_GET(&store_nr_active) == 1 || ((cc->base.wtime || cc->base.atime) && cc->base.allocated)) {
			if (ATOMIC_GET(&store_nr_active) > 1 && cc->data) { /* move delta */
				sql_delta *b = cc->data;
				sql_column *oldc = NULL;

				if (!oc->data)
					oc->base.allocated = cc->base.allocated;
				cc->data = NULL;
				b->next = oc->data;
				oc->data = b;

				while (b && b->cs.wtime >= oldest->stime)
					b = b->next;
				/* find column c->base.stime */
				if (o)
					oldc = o->data;
				if (oldc && b && oldc->base.id == cc->base.id && b->cs.wtime < oldc->base.stime) {
					/* anything older can go */
					delayed_destroy_bat(b->next);
					b->next = NULL;
				}
			} else if (oc->data && cc->base.allocated) {
				if (tr_update_delta(tr, oc->data, cc->data) != LOG_OK)
					ok = LOG_ERR;
			} else if (ATOMIC_GET(&store_nr_active) == 1 && !cc->base.allocated) {
				/* only deletes, merge earlier changes */
				if (!oc->data) {
					sql_column *o = tr_find_column(tr->parent, oc);
					oc->data = timestamp_delta(o->data, oc->base.stime);
				}
				assert(oc->data);
				if (tr_merge_delta(tr, oc->data) != LOG_OK)
					ok = LOG_ERR;
				cc->data = NULL;
			} else if (cc->data) {
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
		if (oc->base.atime < cc->base.atime)
			oc->base.atime = cc->base.atime;
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
			if (ATOMIC_GET(&store_nr_active) == 1 || ((ci->base.wtime || ci->base.atime) && ci->base.allocated)) {
				if (ATOMIC_GET(&store_nr_active) > 1 && ci->data) { /* move delta */
					sql_delta *b = ci->data;
					sql_idx *oldi = NULL;

					if (!oi->data)
						oi->base.allocated = ci->base.allocated;
					ci->data = NULL;
					b->next = oi->data;
					oi->data = b;

					while (b && b->cs.wtime >= oldest->stime)
						b = b->next;
					/* find idx i->base.stime */
					if (o)
						oldi = o->data;
					if (oldi && b && oldi->base.id == ci->base.id && b->cs.wtime < oldi->base.stime) {
						/* anything older can go */
						delayed_destroy_bat(b->next);
						b->next = NULL;
					}
				} else if (oi->data && ci->base.allocated) {
					if (tr_update_delta(tr, oi->data, ci->data) != LOG_OK)
						ok = LOG_ERR;
				} else if (ATOMIC_GET(&store_nr_active) == 1 && !ci->base.allocated) {
					if (!oi->data) {
						sql_idx *o = tr_find_idx(tr->parent, oi);
						oi->data = timestamp_delta(o->data, oi->base.stime);
					}
					assert(oi->data);
					if (tr_merge_delta(tr, oi->data) != LOG_OK)
						ok = LOG_ERR;
					ci->data = NULL;
				} else if (ci->data) {
					oi->data = ci->data;
					oi->base.allocated = 1;
					ci->data = NULL;
				}
			}

			if (oi->base.rtime < ci->base.rtime)
				oi->base.rtime = ci->base.rtime;
			if (oi->base.atime < ci->base.atime)
				oi->base.atime = ci->base.atime;
			if (oi->base.wtime < ci->base.wtime)
				oi->base.wtime = ci->base.wtime;
			if (ci->data)
				destroy_idx(tr, ci);
			ci->base.allocated = 0;
		}
	}
	if (tt->base.rtime < ft->base.rtime)
		tt->base.rtime = ft->base.rtime;
	if (tt->base.atime < ft->base.atime)
		tt->base.atime = ft->base.atime;
	if (tt->base.wtime < ft->base.wtime)
		tt->base.wtime = ft->base.wtime;
	if (ft->data)
		destroy_del(tr, ft);
	ft->base.allocated = 0;
	return ok;
}

static int
tr_log_cs( sql_trans *tr, column_storage *cs, segment *segs, int cleared, sqlid id)
{
	int ok = GDK_SUCCEED;

	if (GDKinmemory())
		return LOG_OK;

	assert(cs->cleared == cleared);
	assert(tr->parent == gtrans);
	if (cleared && log_bat_clear(bat_logger, id) != GDK_SUCCEED)
		return LOG_ERR;

	for (; segs; segs=segs->next) {
		if (segs->owner == tr) {
			BAT *ins = temp_descriptor(cs->bid);
			assert(ins);
			ok = log_bat(bat_logger, ins, id, segs->start, segs->end-segs->start);
			bat_destroy(ins);
		}
	}

	if (ok == GDK_SUCCEED && cs->ucnt && cs->uibid) {
		BAT *ui = temp_descriptor(cs->uibid);
		BAT *uv = temp_descriptor(cs->uvbid);
		/* any updates */
		if (ui == NULL || uv == NULL) {
			ok = GDK_FAIL;
		} else if (BUNlast(uv) > uv->batInserted || BATdirty(uv))
			ok = log_delta(bat_logger, ui, uv, id);
		bat_destroy(ui);
		bat_destroy(uv);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
tr_log_delta( sql_trans *tr, sql_delta *cbat, segment *segs, int cleared, sqlid id)
{
	return tr_log_cs( tr, &cbat->cs, segs, cleared, id);
}

static int
tr_log_dbat(sql_trans *tr, storage *fdb, segment *segs, int cleared, sqlid id)
{
	return tr_log_cs( tr, &fdb->cs, segs, cleared, id);
}

#if 0
static lng
log_get_nr_inserted(sql_column *fc, lng *offset)
{
	lng cnt = 0;

	if (!fc || GDKinmemory())
		return 0;

	if (fc->base.atime && fc->base.allocated) {
		sql_delta *fb = fc->data;
		BAT *ins = temp_descriptor(fb->cs.bid);

		if (ins && BUNlast(ins) > 0 && BUNlast(ins) > ins->batInserted) {
			cnt = BUNlast(ins) - ins->batInserted;
		}
		bat_destroy(ins);
	}
	return cnt;
}

static lng
log_get_nr_deleted(sql_table *ft, lng *offset)
{
	lng cnt = 0;

	if (!ft || GDKinmemory())
		return 0;

	if (ft->base.atime && ft->base.allocated) {
		storage *fdb = ft->data;
		BAT *db = temp_descriptor(fdb->cs.bid);

		if (db && BUNlast(db) > 0 && BUNlast(db) > db->batInserted) {
			cnt = BUNlast(db) - db->batInserted;
			*offset = db->batInserted;
		}
		bat_destroy(db);
	}
	return cnt;
}
#endif

static int
log_table(sql_trans *tr, sql_table *ft)
{
	int ok = LOG_OK;
	node *n;

	/*
	//sql_column *fc = ft->columns.set->h->data;
	if (log_batgroup(bat_logger, ft->bootstrap?0:LOG_TAB, ft->base.id, ft->cleared,
				log_get_nr_inserted(fc, &offset_inserted), offset_inserted,
				log_get_nr_deleted(ft, &offset_deleted), offset_deleted) != GDK_SUCCEED)
		ok = LOG_ERR;
		*/
	assert(tr->parent == gtrans);
	/* offset/end */

	storage *s = ft->data;
	if (ok == LOG_OK && (ft->base.wtime || ft->base.atime) && ft->base.allocated)
		ok = tr_log_dbat(tr, ft->data, s?s->segs->head:NULL, ft->cleared, ft->base.id);
	for (n = ft->columns.set->h; ok == LOG_OK && n; n = n->next) {
		sql_column *cc = n->data;

		if ((!cc->base.wtime && !cc->base.atime) || !cc->base.allocated)
			continue;
		ok = tr_log_delta(tr, cc->data, s?s->segs->head:NULL, ft->cleared, cc->base.id);
	}
	if (ok == LOG_OK && ft->idxs.set) {
		for (n = ft->idxs.set->h; ok == LOG_OK && n; n = n->next) {
			sql_idx *ci = n->data;

			/* some indices have no bats or changes */
			if (!ci->data || (!ci->base.wtime && !ci->base.atime) || !ci->base.allocated)
				continue;

			if (!isTable(ci->t) || (hash_index(ci->type) && list_length(ci->columns) <= 1) || !idx_has_column(ci->type))
				continue;

			ok = tr_log_delta(tr, ci->data, s?s->segs->head:NULL, ft->cleared, ci->base.id);
		}
	}
	if (s)
		for (segment *segs = s->segs->head; segs; segs=segs->next)
			if (segs->owner == tr)
				segs->owner = NULL;
	/*
	if (log_batgroup_end(bat_logger, ft->base.id) != GDK_SUCCEED)
		ok = LOG_ERR;
		*/
	return ok;
}

void
bat_storage_init( store_functions *sf)
{
	sf->bind_col = (bind_col_fptr)&bind_col;
	sf->bind_idx = (bind_idx_fptr)&bind_idx;
	sf->bind_del = (bind_del_fptr)&bind_del;

	sf->bind_col_data = (bind_col_data_fptr)&bind_col_data;
	sf->bind_idx_data = (bind_idx_data_fptr)&bind_idx_data;
	sf->bind_del_data = (bind_del_data_fptr)&bind_del_data;

	sf->append_col = (append_col_fptr)&append_col;
	sf->append_idx = (append_idx_fptr)&append_idx;
	sf->update_col = (update_col_fptr)&update_col;
	sf->update_idx = (update_idx_fptr)&update_idx;
	sf->delete_tab = (delete_tab_fptr)&delete_tab;
	sf->claim_tab = (claim_tab_fptr)&claim_tab;

	sf->count_del = (count_del_fptr)&count_del;
	sf->count_col = (count_col_fptr)&count_col;
	sf->count_idx = (count_idx_fptr)&count_idx;
	sf->dcount_col = (dcount_col_fptr)&dcount_col;
	sf->sorted_col = (prop_col_fptr)&sorted_col;
	sf->unique_col = (prop_col_fptr)&unique_col;
	sf->double_elim_col = (prop_col_fptr)&double_elim_col;

	sf->create_col = (create_col_fptr)&create_col;
	sf->create_idx = (create_idx_fptr)&create_idx;
	sf->create_del = (create_del_fptr)&create_del;

	sf->log_create_col = (create_col_fptr)&log_create_col;
	sf->log_create_idx = (create_idx_fptr)&log_create_idx;
	sf->log_create_del = (create_del_fptr)&log_create_del;

	sf->dup_col = (dup_col_fptr)&dup_col;
	sf->dup_idx = (dup_idx_fptr)&dup_idx;
	sf->dup_del = (dup_del_fptr)&dup_del;

	sf->destroy_col = (destroy_col_fptr)&destroy_col;
	sf->destroy_idx = (destroy_idx_fptr)&destroy_idx;
	sf->destroy_del = (destroy_del_fptr)&destroy_del;

	sf->log_destroy_col = (destroy_col_fptr)&log_destroy_col;
	sf->log_destroy_idx = (destroy_idx_fptr)&log_destroy_idx;
	sf->log_destroy_del = (destroy_del_fptr)&log_destroy_del;

	sf->clear_table = (clear_table_fptr)&clear_table;
	sf->update_table = (update_table_fptr)&update_table;
	sf->log_table = (update_table_fptr)&log_table;
	sf->gtrans_minmax = (gtrans_update_fptr)&minmax;

	sf->cleanup = (cleanup_fptr)&cleanup;

	for(int i=0;i<NR_TABLE_LOCKS;i++)
		MT_lock_init(&table_locks[i], "table_lock");
}
