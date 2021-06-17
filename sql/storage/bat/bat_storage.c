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

#define inTransaction(tr,t) (isLocalTemp(t))

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
static int tc_gc_col( sql_store Store, sql_change *c, ulng oldest);
static int tc_gc_idx( sql_store Store, sql_change *c, ulng oldest);
static int tc_gc_del( sql_store Store, sql_change *c, ulng oldest);

static int merge_delta( sql_delta *obat);

/* valid
 * !deleted && VALID_4_READ(TS, tr)				existing or newly created segment
 *  deleted && TS > tr->ts && OLDTS < tr->ts		deleted after current transaction
 */

#define VALID_4_READ(TS,tr) \
	(TS == tr->tid || (tr->parent && tr_version_of_parent(tr, TS)) || TS < tr->ts)

/* when changed, check if the old status is still valid */
#define OLD_VALID_4_READ(TS,OLDTS,tr) \
		(OLDTS && TS != tr->tid && TS > tr->ts && OLDTS < tr->ts)

#define SEG_VALID_4_DELETE(seg,tr) \
	(!seg->deleted && VALID_4_READ(seg->ts, tr))

/* Delete (in current trans or by some other finised transaction, or re-used segment which used to be deleted */
#define SEG_IS_DELETED(seg,tr) \
	((seg->deleted && (VALID_4_READ(seg->ts, tr) || !OLD_VALID_4_READ(seg->ts, seg->oldts, tr))) || \
	 (!seg->deleted && !VALID_4_READ(seg->ts, tr)))

/* A segment is part of the current transaction is someway or is deleted by some other transaction but use to be valid */
#define SEG_IS_VALID(seg, tr) \
		((!seg->deleted && VALID_4_READ(seg->ts, tr)) || \
		 (seg->deleted && OLD_VALID_4_READ(seg->ts, seg->oldts, tr)))

static void
lock_table(sqlstore *store, sqlid id)
{
	MT_lock_set(&store->table_locks[id&(NR_TABLE_LOCKS-1)]);
}

static void
unlock_table(sqlstore *store, sqlid id)
{
	MT_lock_unset(&store->table_locks[id&(NR_TABLE_LOCKS-1)]);
}

static void
lock_column(sqlstore *store, sqlid id)
{
	MT_lock_set(&store->column_locks[id&(NR_TABLE_LOCKS-1)]);
}

static void
unlock_column(sqlstore *store, sqlid id)
{
	MT_lock_unset(&store->column_locks[id&(NR_TABLE_LOCKS-1)]);
}


static int
tc_gc_seg( sql_store Store, sql_change *change, ulng oldest)
{
	(void)Store;
	segment *s = change->data;

	if (s->ts <= oldest) {
		while(s) {
			segment *n = s->prev;
			_DELETE(s);
			s = n;
		}
		return 1;
	}
	return LOG_OK;
}

static void
mark4destroy(segment *s, sql_change *c, ulng commit_ts)
{
	/* we can only be accessed by anything older then commit_ts */
	if (c->cleanup == &tc_gc_seg)
		s->prev = c->data;
	else
		c->cleanup = &tc_gc_seg;
	c->data = s;
	s->ts = commit_ts;
}

static segment *
new_segment(segment *o, sql_trans *tr, size_t cnt)
{
	segment *n = (segment*)GDKmalloc(sizeof(segment));

	assert(tr);
	if (n) {
		n->ts = tr->tid;
		n->oldts = 0;
		n->deleted = false;
		n->start = 0;
		n->next = NULL;
		n->prev = NULL;
		if (o) {
			n->start = o->end;
			o->next = n;
		}
		n->end = n->start + cnt;
	}
	return n;
}

static segment *
split_segment(segments *segs, segment *o, segment *p, sql_trans *tr, size_t start, size_t cnt, bool deleted)
{
	if (o->start == start && o->end == start+cnt) {
		assert(o->deleted != deleted || o->ts < TRANSACTION_ID_BASE);
		o->oldts = o->ts;
		o->ts = tr->tid;
		o->deleted = deleted;
		return o;
	}
	segment *n = (segment*)GDKmalloc(sizeof(segment));

	assert(tr);
	if (!n)
		return NULL;
	n->prev = NULL;

	n->oldts = 0;
	if (o->ts == tr->tid) {
		n->ts = 1;
		n->deleted = true;
	} else {
		n->oldts = o->ts;
		n->ts = tr->tid;
		n->deleted = deleted;
	}
	if (start == o->start) {
		n->start = o->start;
		n->end = n->start + cnt;
		n->next = o;
		if (segs->h == o)
			segs->h = n;
		if (p)
			p->next = n;
		o->start = n->end;
		return n;
	} else if (start+cnt == o->end) {
		n->start = o->end - cnt;
		n->end = o->end;
		n->next = o->next;
		o->next = n;
		if (segs->t == o)
			segs->t = n;
		o->end = n->start;
		return n;
	}
	/* 3 way split */
	n->start = start;
	n->end = o->end;
	n->next = o->next;
	o->next = n;
	if (segs->t == o)
		segs->t = n;
	o->end = n->start;

	segment *oo = o;
	o = n;
	n = (segment*)GDKmalloc(sizeof(segment));
	if (!n)
		return NULL;
	n->prev = NULL;
	n->ts = oo->ts;
	n->oldts = oo->oldts;
	n->deleted = oo->deleted;
	n->start = start+cnt;
	n->end = o->end;
	n->next = o->next;
	o->next = n;
	if (segs->t == o)
		segs->t = n;
	o->end = n->start;
	return o;
}

static void
rollback_segments(segments *segs, sql_trans *tr, sql_change *change, ulng oldest)
{
	segment *cur = segs->h, *seg = NULL;
	for (; cur; cur = cur->next) {
		if (cur->ts == tr->tid) { /* revert */
			cur->deleted = !cur->deleted || (cur->ts == cur->oldts);
			cur->ts = cur->oldts==tr->tid?0:cur->oldts; /* need old ts */
			cur->oldts = 0;
		}
		if (cur->ts <= oldest) { /* possibly merge range */
			if (!seg) { /* skip first */
				seg = cur;
			} else if (seg->end == cur->start && seg->deleted == cur->deleted) {
				/* merge with previous */
				seg->end = cur->end;
				seg->next = cur->next;
				if (cur == segs->t)
					segs->t = seg;
				mark4destroy(cur, change, store_get_timestamp(tr->store));
				cur = seg;
			} else {
				seg = cur; /* begin of new merge */
			}
		}
	}
}

static void
merge_segments(segments *segs, sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	segment *cur = segs->h, *seg = NULL;
	for (; cur; cur = cur->next) {
		if (cur->ts == tr->tid) {
			if (!cur->deleted)
				cur->oldts = 0;
			cur->ts = commit_ts;
		}
		if (cur->ts <= oldest && cur->ts < TRANSACTION_ID_BASE) { /* possibly merge range */
			if (!seg) { /* skip first */
				seg = cur;
			} else if (seg->end == cur->start && seg->deleted == cur->deleted) {
				/* merge with previous */
				seg->end = cur->end;
				seg->next = cur->next;
				if (cur == segs->t)
					segs->t = seg;
				if (commit_ts == oldest)
					_DELETE(cur);
				else
					mark4destroy(cur, change, commit_ts);
				cur = seg;
			} else {
				seg = cur; /* begin of new merge */
			}
		}
	}
}

static int
segments_in_transaction(sql_trans *tr, sql_table *t)
{
	storage *s = ATOMIC_PTR_GET(&t->data);
	segment *seg = s->segs->h;

	if (seg && s->segs->t->ts == tr->tid)
		return 1;
	for (; seg ; seg=seg->next) {
		if (seg->ts == tr->tid)
			return 1;
	}
	return 0;
}

static size_t
segs_end( segments *segs, sql_trans *tr, sql_table *table)
{
	size_t cnt = 0;

	lock_table(tr->store, table->base.id);
	segment *s = segs->h, *l = NULL;

	for(;s; s = s->next) {
		if (SEG_IS_VALID(s, tr))
				l = s;
	}
	if (l)
		cnt = l->end;
	unlock_table(tr->store, table->base.id);
	return cnt;
}

static int
segments2cs(sql_trans *tr, segments *segs, column_storage *cs, sql_table *t)
{
	/* set bits correctly */
	BAT *b = temp_descriptor(cs->bid);

	if (!b)
		return LOG_ERR;
	segment *s = segs->h;

	size_t nr = segs_end(segs, tr, t);
	if (nr >= BATcapacity(b) && BATextend(b, nr) != GDK_SUCCEED)
		return LOG_ERR;

	BATsetcount(b, nr);
	uint32_t *restrict dst;
	for (; s ; s=s->next) {
		if (s->ts == tr->tid && s->end != s->start) {
			size_t lnr = s->end-s->start;
			size_t pos = s->start;
			dst = ((uint32_t*)Tloc(b, 0)) + (s->start/32);
			uint32_t cur = 0;
			if (s->deleted) {
				size_t used = pos&31, end = 32;
				if (used) {
					if (lnr < (32-used))
						end = used + lnr;
					for(size_t j=used; j < end; j++, lnr--)
						cur |= 1U<<j;
					*dst++ |= cur;
					cur = 0;
				}
				size_t full = lnr/32;
				size_t rest = lnr%32;
				for(size_t i = 0; i<full; i++, lnr-=32)
					*dst++ = ~0;
				if (rest) {
					for(size_t j=0; j < rest; j++, lnr--)
						cur |= 1U<<j;
					*dst |= cur;
				}
				assert(lnr==0);
			} else {
				size_t used = pos&31, end = 32;
				if (used) {
					if (lnr < (32-used))
						end = used + lnr;
					for(size_t j=used; j < end; j++, lnr--)
						cur |= 1U<<j;
					*dst++ &= ~cur;
					cur = 0;
				}
				size_t full = lnr/32;
				size_t rest = lnr%32;
				for(size_t i = 0; i<full; i++, lnr-=32)
					*dst++ = 0;
				if (rest) {
					for(size_t j=0; j < rest; j++, lnr--)
						cur |= 1U<<j;
					*dst &= ~cur;
				}
				assert(lnr==0);
			}
		}
	}
	bat_destroy(b);
	return LOG_OK;
}

static segments *
new_segments(sql_trans *tr, size_t cnt)
{
	segments *n = (segments*)GDKmalloc(sizeof(segments));

	if (n) {
		sql_ref_init(&n->r);
		n->h = n->t = new_segment(NULL, tr, cnt);
	}
	return n;
}

static segments*
dup_segments(segments *s)
{
	sql_ref_inc(&s->r);
	return s;
}

static int
temp_dup_cs(column_storage *cs, ulng tid, int type)
{
	BAT *b = bat_new(type, 1024, TRANSIENT);
	if (!b)
		return LOG_ERR;
	cs->bid = temp_create(b);
	bat_destroy(b);
	cs->uibid = e_bat(TYPE_oid);
	cs->uvbid = e_bat(type);
	cs->ucnt = 0;
	cs->cleared = 0;
	cs->ts = tid;
	cs->refcnt = 1;
	return LOG_OK;
}

static sql_delta *
temp_dup_delta(ulng tid, int type)
{
	sql_delta *bat = ZNEW(sql_delta);
	if (temp_dup_cs(&bat->cs, tid, type)) {
		_DELETE(bat);
		return NULL;
	}
	return bat;
}

static sql_delta *
temp_delta(sql_delta *d, ulng tid)
{
	while (d && d->cs.ts != tid)
		d = d->next;
	return d;
}

static storage *
temp_dup_storage(sql_trans *tr)
{
	storage *bat = ZNEW(storage);
	if (temp_dup_cs(&bat->cs, tr->tid, TYPE_msk)) {
		_DELETE(bat);
		return NULL;
	}
	bat->segs = new_segments(tr, 0);
	return bat;
}

static storage *
temp_storage(storage *d, ulng tid)
{
	while (d && d->cs.ts != tid)
		d = d->next;
	return d;
}

static sql_delta *
timestamp_delta( sql_trans *tr, sql_delta *d)
{
	while (d->next && !VALID_4_READ(d->cs.ts, tr))
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

static storage *
timestamp_storage( sql_trans *tr, storage *d)
{
	if (!d)
		return NULL;
	while (d->next && !VALID_4_READ(d->cs.ts, tr))
		d = d->next;
	return d;
}

static storage *
temp_tab_timestamp_storage( sql_trans *tr, sql_table *t)
{
	assert(isTempTable(t));
	storage *d = temp_storage(ATOMIC_PTR_GET(&t->data), tr->tid);
	if (!d) {
		d = temp_dup_storage(tr);
		do {
			d->next = ATOMIC_PTR_GET(&t->data);
		} while(!ATOMIC_PTR_CAS(&t->data, (void**)&d->next, d)); /* set t->data = d, when t->data == d->next else d->next = t->data */
	}
	return d;
}

static storage *
tab_timestamp_storage( sql_trans *tr, sql_table *t)
{
	if (isTempTable(t))
		return temp_tab_timestamp_storage(tr, t);
	return timestamp_storage( tr, ATOMIC_PTR_GET(&t->data));
}

static sql_delta*
delta_dup(sql_delta *d)
{
	d->cs.refcnt++;
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

static storage*
storage_dup(storage *d)
{
	d->cs.refcnt++;
	return d;
}

static void *
del_dup(sql_table *t)
{
	return storage_dup(ATOMIC_PTR_GET(&t->data));
}

static size_t
count_inserts( segment *s, sql_trans *tr)
{
	size_t cnt = 0;

	for(;s; s = s->next) {
		if (!s->deleted && s->ts == tr->tid)
			cnt += s->end - s->start;
	}
	return cnt;
}

static size_t
count_deletes_in_range( segment *s, sql_trans *tr, BUN start, BUN end)
{
	size_t cnt = 0;

	for(;s && s->end <= start; s = s->next)
		;

	for(;s && s->start < end; s = s->next) {
		if (SEG_IS_DELETED(s, tr)) /* assume aligned s->end and end */
			cnt += s->end - s->start;
	}
	return cnt;
}

static size_t
count_deletes( segment *s, sql_trans *tr)
{
	size_t cnt = 0;

	for(;s; s = s->next) {
		if (SEG_IS_DELETED(s, tr))
			cnt += s->end - s->start;
	}
	return cnt;
}

static size_t
count_col(sql_trans *tr, sql_column *c, int access)
{
	storage *d;
	sql_delta *ds;

	if (!isTable(c->t))
		return 0;
	d = tab_timestamp_storage(tr, c->t);
	ds = col_timestamp_delta(tr, c);
	if (!d)
		return 0;
	if (access == 2)
		return ds?ds->cs.ucnt:0;
	if (access == 1)
		return count_inserts(d->segs->h, tr);
	if (access == QUICK || isTempTable(c->t))
		return d->segs->t?d->segs->t->end:0;
	if (access == 10) {
		size_t cnt = segs_end(d->segs, tr, c->t);
		lock_table(tr->store, c->t->base.id);
		cnt -= count_deletes_in_range(d->segs->h, tr, 0, cnt);
		unlock_table(tr->store, c->t->base.id);
		return cnt;
	}
	return segs_end(d->segs, tr, c->t);
}

static size_t
count_idx(sql_trans *tr, sql_idx *i, int access)
{
	storage *d;
	sql_delta *ds;

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	d = tab_timestamp_storage(tr, i->t);
	ds = idx_timestamp_delta(tr, i);
	if (!d)
		return 0;
	if (access == 2)
		return ds?ds->cs.ucnt:0;
	if (access == 1)
		return count_inserts(d->segs->h, tr);
	if (access == QUICK || isTempTable(i->t))
		return d->segs->t?d->segs->t->end:0;
	return segs_end(d->segs, tr, i->t);
}

static BAT *
cs_bind_ubat( column_storage *cs, int access, int type)
{
	BAT *b;

	assert(access == RD_UPD_ID || access == RD_UPD_VAL);
	/* returns the updates for cs */
	if (cs->uibid && cs->uvbid) {
		if (access == RD_UPD_ID)
			b = temp_descriptor(cs->uibid);
		else
			b = temp_descriptor(cs->uvbid);
	} else {
		b = e_BAT(access == RD_UPD_ID?TYPE_oid:type);
	}
	return b;
}

static BAT *
merge_updates( BAT *ui, BAT **UV, BAT *oi, BAT *ov)
{
	int err = 0;
	BAT *uv = *UV;
	BUN cnt = BATcount(ui)+BATcount(oi);
	BAT *ni = bat_new(TYPE_oid, cnt, PERSISTENT);
	BAT *nv = uv?bat_new(uv->ttype, cnt, PERSISTENT):NULL;

	if (!ni || (uv && !nv)) {
		bat_destroy(ni);
		bat_destroy(nv);
		bat_destroy(ui);
		bat_destroy(uv);
		bat_destroy(oi);
		bat_destroy(ov);
		return NULL;
	}
	BATiter uvi;
	BATiter ovi;

	if (uv) {
		uvi = bat_iterator(uv);
		ovi = bat_iterator(ov);
	}

	/* handle dense (void) cases together as we need too merge updates (which is slower anyway) */
	BUN uip = 0, uie = BATcount(ui);
	BUN oip = 0, oie = BATcount(oi);

	oid uiseqb = ui->tseqbase;
	oid oiseqb = oi->tseqbase;
	oid *uipt = NULL, *oipt = NULL;
	if (!BATtdense(ui))
		uipt = Tloc(ui, 0);
	if (!BATtdense(oi))
		oipt = Tloc(oi, 0);
	while (uip < uie && oip < oie && !err) {
		oid uiid = (uipt)?uipt[uip]: uiseqb+uip;
		oid oiid = (oipt)?oipt[oip]: oiseqb+oip;

		if (uiid <= oiid) {
			if (BUNappend(ni, (ptr) &uiid, true) != GDK_SUCCEED ||
		    	    (ov && BUNappend(nv, (ptr) BUNtail(uvi, uip), true) != GDK_SUCCEED))
				err = 1;
			uip++;
			if (uiid == oiid)
				oip++;
		} else { /* uiid > oiid */
			if (BUNappend(ni, (ptr) &oiid, true) != GDK_SUCCEED ||
		    	    (ov && BUNappend(nv, (ptr) BUNtail(ovi, oip), true) != GDK_SUCCEED) )
				err = 1;
			oip++;
		}
	}
	while (uip < uie && !err) {
		oid uiid = (uipt)?uipt[uip]: uiseqb+uip;
		if (BUNappend(ni, (ptr) &uiid, true) != GDK_SUCCEED ||
	    	    (ov && BUNappend(nv, (ptr) BUNtail(uvi, uip), true) != GDK_SUCCEED))
			err = 1;
		uip++;
	}
	while (oip < oie && !err) {
		oid oiid = (oipt)?oipt[oip]: oiseqb+oip;
		if (BUNappend(ni, (ptr) &oiid, true) != GDK_SUCCEED ||
	    	    (ov && BUNappend(nv, (ptr) BUNtail(ovi, oip), true) != GDK_SUCCEED) )
			err = 1;
		oip++;
	}
	bat_destroy(ui);
	bat_destroy(uv);
	bat_destroy(oi);
	bat_destroy(ov);
	if (!err) {
		if (nv)
			*UV = nv;
		return ni;
	}
	*UV = NULL;
	bat_destroy(ni);
	bat_destroy(nv);
	return NULL;
}

static sql_delta *
older_delta( sql_delta *d, sql_trans *tr)
{
	sql_delta *o = d->next;

	while (o) {
	       	if (o->cs.ucnt && VALID_4_READ(o->cs.ts, tr))
			break;
		else
			o = o->next;
	}
	if (o && o->cs.ucnt && VALID_4_READ(o->cs.ts, tr))
		return o;
	return NULL;
}

static BAT *
bind_ubat(sql_trans *tr, sql_delta *d, int access, int type)
{
	assert(tr->active);
	sql_delta *o = NULL;
	BAT *ui = NULL, *uv = NULL;

	ui = cs_bind_ubat(&d->cs, RD_UPD_ID, type);
	if (access == RD_UPD_VAL)
		uv = cs_bind_ubat(&d->cs, RD_UPD_VAL, type);
	while ((o = older_delta(d, tr)) != NULL) {
		BAT *oui = NULL, *ouv = NULL;
		if (!oui)
			oui = cs_bind_ubat(&o->cs, RD_UPD_ID, type);
		if (access == RD_UPD_VAL)
			ouv = cs_bind_ubat(&o->cs, RD_UPD_VAL, type);
		if (!ui || !oui || (access == RD_UPD_VAL && (!uv || !ouv)))
			return NULL;
		if ((ui = merge_updates(ui, &uv, oui, ouv)) == NULL)
			return NULL;
		d = o;
	}
	if (uv) {
		bat_destroy(ui);
		return uv;
	}
	return ui;
}

static BAT *
bind_ucol(sql_trans *tr, sql_column *c, int access)
{
	return bind_ubat(tr, col_timestamp_delta(tr, c), access, c->type.type->localtype);
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	int type = oid_index(i->type)?TYPE_oid:TYPE_lng;
	return bind_ubat(tr, idx_timestamp_delta(tr, i), access, type);
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
	BAT *s = BATslice(b, 0, cnt);
	bat_destroy(b);
	return s;
}

static void *					/* BAT * */
bind_col(sql_trans *tr, sql_column *c, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(c->t))
		return NULL;
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_ucol(tr, c, access);
	sql_delta *d = col_timestamp_delta(tr, c);
	size_t cnt = count_col(tr, c, 0);
	return cs_bind_bat( &d->cs, access, cnt);
}

static void *					/* BAT * */
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(i->t))
		return NULL;
	if (access == RD_UPD_ID || access == RD_UPD_VAL)
		return bind_uidx(tr, i, access);
	sql_delta *d = idx_timestamp_delta(tr, i);
	size_t cnt = count_idx(tr, i, 0);
	return cs_bind_bat( &d->cs, access, cnt);
}

static int
cs_real_update_bats( column_storage *cs, BAT **Ui, BAT **Uv)
{
	if (!cs->uibid) {
		cs->uibid = e_bat(TYPE_oid);
		if (cs->uibid == BID_NIL)
			return LOG_ERR;
	}
	if (!cs->uvbid) {
		BAT *cur = temp_descriptor(cs->bid);
		int type = cur->ttype;
		bat_destroy(cur);
		cs->uvbid = e_bat(type);
		if(cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
			return LOG_ERR;
	}
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

static int
segments_is_append(segment *s, sql_trans *tr, oid rid)
{
	for(; s; s=s->next) {
		if (s->start <= rid && s->end > rid) {
			if (s->ts == tr->tid && !s->deleted) {
				return 1;
			}
			break;
		}
	}
	return 0;
}

static int
segments_is_deleted(segment *s, sql_trans *tr, oid rid)
{
	for(; s; s=s->next) {
		if (s->start <= rid && s->end > rid) {
			if (s->ts >= tr->ts && s->deleted) {
				return 1;
			}
			break;
		}
	}
	return 0;
}

/*
 * Returns LOG_OK, LOG_ERR or LOG_CONFLICT
 */
static int
cs_update_bat( sql_trans *tr, column_storage *cs, sql_table *t, BAT *tids, BAT *updates, int is_new)
{
	storage *s = ATOMIC_PTR_GET(&t->data);
	int res = LOG_OK;
	BAT *otids = tids, *oupdates = updates;

	if (!BATcount(tids))
		return LOG_OK;

	if (tids && (tids->ttype == TYPE_msk || mask_cand(tids))) {
		otids = BATunmask(tids);
		if (!otids)
			return LOG_ERR;
	}
	/* When we go to smaller grained update structures we should check for concurrent updates on this column ! */
	/* currently only one update delta is possible */
	if (!is_new && !cs->cleared) {
		if (!otids->tsorted || complex_cand(otids) /* make sure we have simple dense or oids */) {
			BAT *sorted, *order;
			if (BATsort(&sorted, &order, NULL, otids, NULL, NULL, false, false, false) != GDK_SUCCEED) {
				if (otids != tids)
					bat_destroy(otids);
				return LOG_ERR;
			}
			if (otids != tids)
				bat_destroy(otids);
			otids = sorted;
			oupdates = BATproject(order, oupdates);
			bat_destroy(order);
			if (!oupdates) {
				bat_destroy(otids);
				return LOG_ERR;
			}
		}
		assert(otids->tsorted);
		BAT *ui = NULL, *uv = NULL;

		/* handle updates on just inserted bits */
		/* handle updates on updates (within one transaction) */
		BATiter upi = bat_iterator(oupdates);
		BUN cnt = 0, ucnt = BATcount(otids);
		BAT *b, *ins = NULL;
		int *msk = NULL;

		if((b = temp_descriptor(cs->bid)) == NULL)
			res = LOG_ERR;

		if (BATtdense(otids)) {
			oid start = otids->tseqbase, offset = start;
			oid end = start + ucnt;

			for(segment *seg = s->segs->h; seg && res == LOG_OK ; seg=seg->next) {
				if (seg->start <= start && seg->end > start) {
					/* check for delete conflicts */
					if (seg->ts >= tr->ts && seg->deleted) {
						res = LOG_CONFLICT;
						continue;
					}

					/* check for inplace updates */
					BUN lend = end < seg->end?end:seg->end;
					if (seg->ts == tr->tid && !seg->deleted) {
						if (!ins) {
							ins = COLnew(0, TYPE_msk, ucnt, TRANSIENT);
							if (!ins)
								res = LOG_ERR;
							else {
								BATsetcount(ins, ucnt); /* all full updates  */
								msk = (int*)Tloc(ins, 0);
							}
						}
						for (oid i = 0, rid = start; rid < lend && res == LOG_OK; rid++, i++) {
							ptr upd = BUNtail(upi, rid-offset);
							if (void_inplace(b, rid, upd, true) != GDK_SUCCEED)
								res = LOG_ERR;

							oid word = i/32;
							int pos = i%32;
							msk[word] |= 1U<<pos;
							cnt++;
						}
					}
				}
				if (end < seg->end)
					break;
			}
		} else {
			BUN i = 0;
			oid *rid = Tloc(otids,0);
			segment *seg = s->segs->h;
			while ( seg && res == LOG_OK && i < ucnt) {
				if (seg->end <= rid[i])
					seg = seg->next;
				else if (seg->start <= rid[i] && seg->end > rid[i]) {
					/* check for delete conflicts */
					if (seg->ts >= tr->ts && seg->deleted) {
						res = LOG_CONFLICT;
						continue;
					}

					/* check for inplace updates */
					if (seg->ts == tr->tid && !seg->deleted) {
						if (!ins) {
							ins = COLnew(0, TYPE_msk, ucnt, TRANSIENT);
							if (!ins) {
								res = LOG_ERR;
								break;
							} else {
								BATsetcount(ins, ucnt); /* all full updates  */
								msk = (int*)Tloc(ins, 0);
							}
						}
						ptr upd = BUNtail(upi, i);
						if (void_inplace(b, rid[i], upd, true) != GDK_SUCCEED)
							res = LOG_ERR;

						oid word = i/32;
						int pos = i%32;
						msk[word] |= 1U<<pos;
						cnt++;
					}
					i++;
				}
			}
		}

		if (cnt < ucnt) { 	/* now handle real updates */
			if (cs->ucnt == 0) {
				if (cnt) {
					BAT *nins = BATmaskedcands(0, ucnt, ins, false);
					if (nins) {
						ui = BATproject(nins, otids);
						uv = BATproject(nins, oupdates);
						bat_destroy(nins);
					}
				} else {
					ui = temp_descriptor(otids->batCacheid);
					uv = temp_descriptor(oupdates->batCacheid);
				}
				if (!ui || !uv) {
					res = LOG_ERR;
				} else {
					temp_destroy(cs->uibid);
					temp_destroy(cs->uvbid);
					cs->uibid = temp_create(ui);
					cs->uvbid = temp_create(uv);
					cs->ucnt = BATcount(ui);
				}
			} else {
				BAT *nui = NULL, *nuv = NULL;

				/* merge taking msk of inserted into account */
				if (res == LOG_OK && cs_real_update_bats(cs, &ui, &uv) != LOG_OK)
					res = LOG_ERR;

				if (res == LOG_OK) {
					ptr upd = NULL;
					nui = bat_new(TYPE_oid, cs->ucnt + ucnt - cnt, PERSISTENT);
					nuv = bat_new(uv->ttype, cs->ucnt + ucnt - cnt, PERSISTENT);

					if (!nui || !nuv) {
						res = LOG_ERR;
					} else {
						BATiter ovi = bat_iterator(uv);

						/* handle dense (void) cases together as we need too merge updates (which is slower anyway) */
						BUN uip = 0, uie = BATcount(ui);
						BUN nip = 0, nie = BATcount(otids);
						oid uiseqb = ui->tseqbase;
						oid niseqb = otids->tseqbase;
						oid *uipt = NULL, *nipt = NULL;
						if (!BATtdense(ui))
							uipt = Tloc(ui, 0);
						if (!BATtdense(otids))
							nipt = Tloc(otids, 0);
						while (uip < uie && nip < nie && res == LOG_OK) {
							oid uiv = (uipt)?uipt[uip]: uiseqb+uip;
							oid niv = (nipt)?nipt[nip]: niseqb+nip;

							if (uiv < niv) {
								upd = BUNtail(ovi, uip);
								if (BUNappend(nui, (ptr) &uiv, true) != GDK_SUCCEED ||
											BUNappend(nuv, (ptr) upd, true) != GDK_SUCCEED)
									res = LOG_ERR;
								uip++;
							} else if (uiv == niv) {
								/* handle == */
								if (!msk || (msk[nip/32] & (1U<<(nip%32))) == 0) {
									upd = BUNtail(upi, nip);
									if (BUNappend(nui, (ptr) &niv, true) != GDK_SUCCEED ||
													BUNappend(nuv, (ptr) upd, true) != GDK_SUCCEED)
										res = LOG_ERR;
								} else {
									upd = BUNtail(ovi, uip);
									if (BUNappend(nui, (ptr) &uiv, true) != GDK_SUCCEED ||
													BUNappend(nuv, (ptr) upd, true) != GDK_SUCCEED)
										res = LOG_ERR;
								}
								uip++;
								nip++;
							} else { /* uiv > niv */
								if (!msk || (msk[nip/32] & (1U<<(nip%32))) == 0) {
									upd = BUNtail(upi, nip);
									if (BUNappend(nui, (ptr) &niv, true) != GDK_SUCCEED ||
													BUNappend(nuv, (ptr) upd, true) != GDK_SUCCEED)
										res = LOG_ERR;
								}
								nip++;
							}
						}
						while (uip < uie && res == LOG_OK) {
							oid uiv = (uipt)?uipt[uip]: uiseqb+uip;
							upd = BUNtail(ovi, uip);
							if (BUNappend(nui, (ptr) &uiv, true) != GDK_SUCCEED ||
									BUNappend(nuv, (ptr) upd, true) != GDK_SUCCEED)
								res = LOG_ERR;
							uip++;
						}
						while (nip < nie && res == LOG_OK) {
							oid niv = (nipt)?nipt[nip]: niseqb+nip;
							if (!msk || (msk[nip/32] & (1U<<(nip%32))) == 0) {
								upd = BUNtail(upi, nip);
								if (BUNappend(nui, (ptr) &niv, true) != GDK_SUCCEED ||
											BUNappend(nuv, (ptr) upd, true) != GDK_SUCCEED)
									res = LOG_ERR;
							}
							nip++;
						}
						if (res == LOG_OK) {
							temp_destroy(cs->uibid);
							temp_destroy(cs->uvbid);
							cs->uibid = temp_create(nui);
							cs->uvbid = temp_create(nuv);
							cs->ucnt = BATcount(nui);
						}
					}
					bat_destroy(nui);
					bat_destroy(nuv);
				}
			}
		}
		bat_destroy(b);
		bat_destroy(ins);
		bat_destroy(ui);
		bat_destroy(uv);
		if (otids != tids)
			bat_destroy(otids);
		if (oupdates != updates)
			bat_destroy(oupdates);
		return res;
	} else if (is_new || cs->cleared) {
		BAT *b = temp_descriptor(cs->bid);

		if (b == NULL)
			res = LOG_ERR;
		else if (BATcount(b)==0 && BATappend(b, updates, NULL, true) != GDK_SUCCEED) /* alter add column */
			res = LOG_ERR;
		else if (BATreplace(b, otids, updates, true) != GDK_SUCCEED)
			res = LOG_ERR;
		BBPcold(b->batCacheid);
		bat_destroy(b);
	}
	if (otids != tids)
		bat_destroy(otids);
	if (oupdates != updates)
		bat_destroy(oupdates);
	return res;
}

static int
delta_update_bat( sql_trans *tr, sql_delta *bat, sql_table *t, BAT *tids, BAT *updates, int is_new)
{
	return cs_update_bat(tr, &bat->cs, t, tids, updates, is_new);
}

static int
cs_update_val( sql_trans *tr, column_storage *cs, sql_table *t, oid rid, void *upd, int is_new)
{
	storage *s = ATOMIC_PTR_GET(&t->data);
	assert(!is_oid_nil(rid));
	int inplace = is_new || cs->cleared || segments_is_append (s->segs->h, tr, rid);

	/* check if rid is insert ? */
	if (!inplace) {
		/* check conflict */
		if (segments_is_deleted(s->segs->h, tr, rid))
			return LOG_CONFLICT;
		BAT *ui, *uv;

		/* When we go to smaller grained update structures we should check for concurrent updates on this column ! */
		/* currently only one update delta is possible */
		if (cs_real_update_bats(cs, &ui, &uv) != LOG_OK)
			return LOG_ERR;

		assert(BATcount(ui) == BATcount(uv));
		if (BUNappend(ui, (ptr) &rid, true) != GDK_SUCCEED ||
		    BUNappend(uv, (ptr) upd, true) != GDK_SUCCEED) {
			bat_destroy(ui);
			bat_destroy(uv);
			return LOG_ERR;
		}
		assert(BATcount(ui) == BATcount(uv));
		bat_destroy(ui);
		bat_destroy(uv);
		cs->ucnt++;
	} else {
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
delta_update_val( sql_trans *tr, sql_delta *bat, sql_table *t, oid rid, void *upd, int is_new)
{
	return cs_update_val(tr, &bat->cs, t, rid, upd, is_new);
}

static int
dup_cs(sql_trans *tr, column_storage *ocs, column_storage *cs, int type, int temp)
{
	(void)tr;
	if (!ocs)
		return LOG_OK;
	(void)type;
	cs->bid = ocs->bid;
	cs->uibid = ocs->uibid;
	cs->uvbid = ocs->uvbid;
	cs->ucnt = ocs->ucnt;

	if (temp) {
		cs->bid = temp_copy(cs->bid, 1);
		if (cs->bid == BID_NIL)
			return LOG_ERR;
	} else {
		temp_dup(cs->bid);
	}
	cs->ucnt = 0;
	cs->uibid = e_bat(TYPE_oid);
	cs->uvbid = e_bat(type);
	if (cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
		return LOG_ERR;
	return LOG_OK;
}

static int
dup_bat(sql_trans *tr, sql_table *t, sql_delta *obat, sql_delta *bat, int type)
{
	return dup_cs(tr, &obat->cs, &bat->cs, type, isTempTable(t));
}

static int
destroy_delta(sql_delta *b, bool recursive)
{
	int ok = LOG_OK;

	if (--b->cs.refcnt > 0)
		return LOG_OK;
	if (recursive && b->next)
		ok = destroy_delta(b->next, true);
	if (b->cs.uibid)
		temp_destroy(b->cs.uibid);
	if (b->cs.uvbid)
		temp_destroy(b->cs.uvbid);
	if (b->cs.bid)
		temp_destroy(b->cs.bid);
	b->cs.bid = b->cs.uibid = b->cs.uvbid = 0;
	_DELETE(b);
	return ok;
}

static sql_delta *
bind_col_data(sql_trans *tr, sql_column *c, bool *update_conflict)
{
	sql_delta *obat = ATOMIC_PTR_GET(&c->data);

	if (isTempTable(c->t))
		obat = temp_col_timestamp_delta(tr, c);

	if (obat->cs.ts == tr->tid || ((obat->cs.ts < TRANSACTION_ID_BASE || tr_version_of_parent(tr, obat->cs.ts)) && !update_conflict)) /* on append there are no conflicts */
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE && !isTempTable(c->t)) {
		/* abort */
		if (update_conflict)
			*update_conflict = true;
		return NULL;
	}
	assert(!isTempTable(c->t));
	obat = timestamp_delta(tr, ATOMIC_PTR_GET(&c->data));
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	bat->cs.refcnt = 1;
	if(dup_bat(tr, c->t, obat, bat, c->type.type->localtype) != LOG_OK)
		return NULL;
	bat->cs.ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&c->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_delta(bat, false);
		return NULL;
	}
	return bat;
}

static int
update_col_execute(sql_trans *tr, sql_delta *delta, sql_table *table, bool is_new, void *incoming_tids, void *incoming_values, bool is_bat)
{
	int ok = LOG_OK;

	if (is_bat) {
		BAT *tids = incoming_tids;
		BAT *values = incoming_values;
		if (BATcount(tids) == 0)
			return LOG_OK;
		lock_table(tr->store, table->base.id);
		ok = delta_update_bat(tr, delta, table, tids, values, is_new);
	} else {
		lock_table(tr->store, table->base.id);
		ok = delta_update_val(tr, delta, table, *(oid*)incoming_tids, incoming_values, is_new);
	}
	unlock_table(tr->store, table->base.id);
	return ok;
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, int tpe)
{
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c, &update_conflict)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;

	assert(delta && delta->cs.ts == tr->tid);
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);

	return update_col_execute(tr, delta, c->t, isNew(c), tids, upd, tpe == TYPE_bat);
}

static sql_delta *
bind_idx_data(sql_trans *tr, sql_idx *i, bool *update_conflict)
{
	sql_delta *obat = ATOMIC_PTR_GET(&i->data);

	if (isTempTable(i->t))
		obat = temp_idx_timestamp_delta(tr, i);

	if (obat->cs.ts == tr->tid || ((obat->cs.ts < TRANSACTION_ID_BASE || tr_version_of_parent(tr, obat->cs.ts)) && !update_conflict)) /* on append there are no conflicts */
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE && !isTempTable(i->t)) {
		/* abort */
		if (update_conflict)
			*update_conflict = true;
		return NULL;
	}
	assert(!isTempTable(i->t));
	obat = timestamp_delta(tr, ATOMIC_PTR_GET(&i->data));
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	bat->cs.refcnt = 1;
	if(dup_bat(tr, i->t, obat, bat, (oid_index(i->type))?TYPE_oid:TYPE_lng) != LOG_OK)
		return NULL;
	bat->cs.ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&i->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_delta(bat, false);
		return NULL;
	}
	return bat;
}

static int
update_idx(sql_trans *tr, sql_idx * i, void *tids, void *upd, int tpe)
{
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if ((delta = bind_idx_data(tr, i, &update_conflict)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;

	assert(delta && delta->cs.ts == tr->tid);
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);

	return update_col_execute(tr, delta, i->t, isNew(i), tids, upd, tpe == TYPE_bat);
}

static int
delta_append_bat( sql_delta *bat, BAT *offsets, BAT *i )
{
	BAT *b, *oi = i;

	assert(BATcount(offsets) == BATcount(i));
	if (!BATcount(i))
		return LOG_OK;
	b = temp_descriptor(bat->cs.bid);
	if (b == NULL)
		return LOG_ERR;
	if (i && (i->ttype == TYPE_msk || mask_cand(i))) {
		oi = BATunmask(i);
	}
	if (BATupdate(b, offsets, oi, true) != GDK_SUCCEED) {
		if (oi != i)
			bat_destroy(oi);
		bat_destroy(b);
		return LOG_ERR;
	}
	if (oi != i)
		bat_destroy(oi);
	bat_destroy(b);
	return LOG_OK;
}

static int
delta_append_val( sql_delta *bat, BAT *offsets, void *i)
{
	BAT *b = temp_descriptor(bat->cs.bid);

	if(b == NULL)
		return LOG_ERR;

	size_t offset = 0;
	if (BATtdense(offsets)) {
		offset = offsets->tseqbase;
	} else {
		/* TODO handle multiple offsets */
		assert(0);
		offset = *(oid*)Tloc(offsets, 0);
	}
	BUN cnt = BATcount(offsets);

	BUN bcnt = BATcount(b);
	if (bcnt > offset){
		size_t ccnt = ((offset+cnt) > bcnt)? (bcnt - offset):cnt;
		if (BUNreplacemultiincr(b, offset, i, ccnt, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
		cnt -= ccnt;
		offset += ccnt;
	}
	if (cnt) {
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
		if (BUNappendmulti(b, i, cnt, true) != GDK_SUCCEED) {
			bat_destroy(b);
			return LOG_ERR;
		}
	}
	bat_destroy(b);
	return LOG_OK;
}

static int
dup_storage( sql_trans *tr, storage *obat, storage *bat, int temp)
{
	if (temp) {
		bat->segs = new_segments(tr, 0);
	} else {
		bat->segs = dup_segments(obat->segs);
	}
	return dup_cs(tr, &obat->cs, &bat->cs, TYPE_msk, temp);
}

static int
append_col_execute(sql_trans *tr, sql_delta *delta, sqlid id, void *offset, void *incoming_data, bool is_bat)
{
	int ok = LOG_OK;

	lock_column(tr->store, id);
	if (is_bat) {
		BAT *bat = incoming_data;

		if (BATcount(bat))
			ok = delta_append_bat(delta, offset, bat);
	} else {
		ok = delta_append_val(delta, offset, incoming_data);
	}
	unlock_column(tr->store, id);
	return ok;
}

static int
append_col(sql_trans *tr, sql_column *c, void *offset, void *i, int tpe)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c, NULL)) == NULL)
		return LOG_ERR;

	assert(delta && (!isTempTable(c->t) || delta->cs.ts == tr->tid));
	if (isTempTable(c->t))
	if ((!inTransaction(tr, c->t) && (odelta != delta || !segments_in_transaction(tr, c->t) || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);

	return append_col_execute(tr, delta, c->base.id, offset, i, tpe == TYPE_bat);
}

static int
append_idx(sql_trans *tr, sql_idx * i, void *offset, void *data, int tpe)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if ((delta = bind_idx_data(tr, i, NULL)) == NULL)
		return LOG_ERR;

	assert(delta && (!isTempTable(i->t) || delta->cs.ts == tr->tid));
	if (isTempTable(i->t))
	if ((!inTransaction(tr, i->t) && (odelta != delta || !segments_in_transaction(tr, i->t) || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);

	return append_col_execute(tr, delta, i->base.id, offset, data, tpe == TYPE_bat);
}

static int
deletes_conflict_updates(sql_trans *tr, sql_table *t, oid rid, size_t cnt)
{
	int err = 0;

	/* TODO check for conflicting updates */
	(void)rid;
	(void)cnt;
	for(node *n = ol_first_node(t->columns); n && !err; n = n->next) {
		sql_column *c = n->data;
		sql_delta *d = ATOMIC_PTR_GET(&c->data);

		/* check for active updates */
		if (!VALID_4_READ(d->cs.ts, tr) && d->cs.ucnt)
			return 1;
	}
	return 0;
}

static int
storage_delete_val(sql_trans *tr, sql_table *t, storage *s, oid rid)
{
	int in_transaction = segments_in_transaction(tr, t);

	/* find segment of rid, split, mark new segment deleted (for tr->tid) */
	segment *seg = s->segs->h, *p = NULL;
	for (; seg; p = seg, seg = seg->next) {
		if (seg->start <= rid && seg->end > rid) {
			if (!SEG_VALID_4_DELETE(seg,tr))
				return LOG_CONFLICT;
			if (deletes_conflict_updates( tr, t, rid, 1))
				return LOG_CONFLICT;
			(void)split_segment(s->segs, seg, p, tr, rid, 1, true);
			break;
		}
	}
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, s, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	return LOG_OK;
}

static int
seg_delete_range(sql_trans *tr, sql_table *t, storage *s, segment **Seg, size_t start, size_t cnt)
{
	segment *seg = *Seg, *p = NULL;
	for (; seg; p = seg, seg = seg->next) {
		if (seg->start <= start && seg->end > start) {
			size_t lcnt = cnt;
			if (start+lcnt > seg->end)
				lcnt = seg->end-start;
			if (SEG_IS_DELETED(seg, tr)) {
				start += lcnt;
				cnt -= lcnt;
				continue;
			} else if (!SEG_VALID_4_DELETE(seg, tr))
				return LOG_CONFLICT;
			if (deletes_conflict_updates( tr, t, start, lcnt))
				return LOG_CONFLICT;
			*Seg = seg = split_segment(s->segs, seg, p, tr, start, lcnt, true);
			start += lcnt;
			cnt -= lcnt;
		}
		if (start+cnt <= seg->end)
			break;
	}
	return LOG_OK;
}

static int
delete_range(sql_trans *tr, sql_table *t, storage *s, size_t start, size_t cnt)
{
	segment *seg = s->segs->h;
	return seg_delete_range(tr, t, s, &seg, start, cnt);
}

static int
storage_delete_bat(sql_trans *tr, sql_table *t, storage *s, BAT *i)
{
	int in_transaction = segments_in_transaction(tr, t);
	BAT *oi = i;	/* update ids */
	int ok = LOG_OK;

	if (i->ttype == TYPE_msk || mask_cand(i))
		i = BATunmask(i);
	if (BATcount(i)) {
		segment *seg = s->segs->h;
		if (BATtdense(i)) {
			size_t start = i->tseqbase;
			size_t cnt = BATcount(i);
			ok = delete_range(tr, t, s, start, cnt);
		} else if (complex_cand(i)) {
			struct canditer ci;
			oid f = 0, l = 0, cur = 0;

			canditer_init(&ci, NULL, i);
			cur = f = canditer_next(&ci);
			if (!is_oid_nil(f)) {
				for(l = canditer_next(&ci); !is_oid_nil(l) && ok == LOG_OK; l = canditer_next(&ci)) {
					if (cur+1 == l) {
						cur++;
						continue;
					}
					ok = seg_delete_range(tr, t, s, &seg, f, cur-f);
					f = cur = l;
				}
				if (ok == LOG_OK)
					ok = seg_delete_range(tr, t, s, &seg, f, cur-f);
			}
		} else {
			if (!BATtordered(i)) {
				assert(oi == i);
				BAT *ni = NULL;
				if (BATsort(&ni, NULL, NULL, i, NULL, NULL, false, false, false) != GDK_SUCCEED)
					ok = LOG_ERR;
				if (ni)
					i = ni;
			}
			assert(BATtordered(i));
			BUN icnt = BATcount(i);
			oid *o = Tloc(i,0), n = o[0]+1;
			size_t lcnt = 1;
			for (size_t i=1; i<icnt && ok == LOG_OK; i++) {
				if (o[i] == n) {
					lcnt++;
					n++;
				} else {
					ok = seg_delete_range(tr, t, s, &seg, n-lcnt, lcnt);
					lcnt = 0;
				}
				if (!lcnt) {
					n = o[i]+1;
					lcnt = 1;
				}
			}
			if (lcnt && ok == LOG_OK)
				ok = seg_delete_range(tr, t, s, &seg, n-lcnt, lcnt);
		}
	}
	if (i != oi)
		bat_destroy(i);
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, s, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	return ok;
}

static void
destroy_segments(segments *s)
{
	if (!s || sql_ref_dec(&s->r) > 0)
		return;
	segment *seg = s->h;
	while(seg) {
		segment *n = seg->next;
		_DELETE(seg);
		seg = n;
	}
	_DELETE(s);
}

static int
destroy_storage(storage *bat)
{
	int ok = LOG_OK;

	if (--bat->cs.refcnt > 0)
		return LOG_OK;
	if (bat->next)
		ok = destroy_storage(bat->next);
	destroy_segments(bat->segs);
	if (bat->cs.uibid)
		temp_destroy(bat->cs.uibid);
	if (bat->cs.uvbid)
		temp_destroy(bat->cs.uvbid);
	if (bat->cs.bid)
		temp_destroy(bat->cs.bid);
	bat->cs.bid = bat->cs.uibid = bat->cs.uvbid = 0;
	_DELETE(bat);
	return ok;
}

static int
segments_conflict(sql_trans *tr, segments *segs)
{
	for (segment *s = segs->h; s; s = s->next)
		if (!VALID_4_READ(s->ts,tr))
			return 1;
	return 0;
}

static storage *
bind_del_data(sql_trans *tr, sql_table *t, bool *clear)
{
	storage *obat = ATOMIC_PTR_GET(&t->data);

	if (isTempTable(t))
		obat = temp_tab_timestamp_storage(tr, t);

	if (obat->cs.ts == tr->tid)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE && !isTempTable(t)) {
		/* abort */
		if (clear)
			*clear = true;
		return NULL;
	}
	if (!isTempTable(t) && !clear)
		return obat;
	if (!isTempTable(t) && clear && segments_conflict(tr, obat->segs)) {
		*clear = true;
		return NULL;
	}

	assert(!isTempTable(t));
	obat = timestamp_storage(tr, ATOMIC_PTR_GET(&t->data));
	storage *bat = ZNEW(storage);
	if(!bat)
		return NULL;
	bat->cs.refcnt = 1;
	dup_storage(tr, obat, bat, clear || isTempTable(t) /* for clear and temp create empty storage */);
	bat->cs.ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&t->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_storage(bat);
		return NULL;
	}
	return bat;
}

static int
delete_tab(sql_trans *tr, sql_table * t, void *ib, int tpe)
{
	int ok = LOG_OK;
	BAT *b = ib;
	storage *bat;

	if (tpe == TYPE_bat && !BATcount(b))
		return ok;

	if ((bat = bind_del_data(tr, t, NULL)) == NULL)
		return LOG_ERR;

	lock_table(tr->store, t->base.id);
	if (tpe == TYPE_bat)
		ok = storage_delete_bat(tr, t, bat, ib);
	else
		ok = storage_delete_val(tr, t, bat, *(oid*)ib);
	unlock_table(tr->store, t->base.id);
	return ok;
}

static size_t
dcount_col(sql_trans *tr, sql_column *c)
{
	sql_delta *b;

	if (!isTable(c->t))
		return 0;
	b = col_timestamp_delta(tr, c);
	if (!b)
		return 1;

	storage *s = ATOMIC_PTR_GET(&c->t->data);
	if (!s || !s->segs->t)
		return 1;
	size_t cnt = s->segs->t->end;
	if (cnt) {
		BAT *v = cs_bind_bat( &b->cs, QUICK, cnt);
		size_t dcnt = 0;

		if (v)
			dcnt = BATguess_uniques(v, NULL);
		return dcnt;
	}
	return cnt;
}

static size_t
count_segs(segment *s)
{
	size_t nr = 0;

	for( ; s; s = s->next)
		nr++;
	return nr;
}

static size_t
count_del(sql_trans *tr, sql_table *t, int access)
{
	storage *d;

	if (!isTable(t))
		return 0;
	d = tab_timestamp_storage(tr, t);
	if (!d)
		return 0;
	if (access == 2)
		return d->cs.ucnt;
	if (access == 1)
		return count_inserts(d->segs->h, tr);
	if (access == 10) /* special case for counting the number of segments */
		return count_segs(d->segs->h);
	return count_deletes(d->segs->h, tr);
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
			de = (int) ceil(b->tvheap->free / (double) GDK_VAROFFSET);
		assert(de >= 0 && de <= 16);
	}
	return de;
}

static int
load_cs(sql_trans *tr, column_storage *cs, int type, sqlid id)
{
	sqlstore *store = tr->store;
	int bid = logger_find_bat(store->logger, id);
	if (!bid)
		return LOG_ERR;
	cs->bid = temp_dup(bid);
	cs->ucnt = 0;
	cs->uibid = e_bat(TYPE_oid);
	cs->uvbid = e_bat(type);
	assert(cs->uibid && cs->uvbid);
	if(cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
		return LOG_ERR;
	return LOG_OK;
}

static int
log_create_delta(sql_trans *tr, sql_delta *bat, sqlid id)
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
	if (bat->cs.uibid == BID_NIL || bat->cs.uvbid == BID_NIL)
		res = LOG_ERR;
	if (GDKinmemory(0)) {
		bat_destroy(b);
		return res;
	}

	bat_set_access(b, BAT_READ);
	sqlstore *store = tr->store;
	ok = log_bat_persists(store->logger, b, id);
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

static void
create_delta( sql_delta *d, BAT *b)
{
	bat_set_access(b, BAT_READ);
	d->cs.bid = temp_create(b);
	d->cs.uibid = d->cs.uvbid = 0;
	d->cs.ucnt = 0;
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
		bat->cs.refcnt = 1;
	}

	if (new)
		bat->cs.ts = tr->tid;

	if (!isNew(c) && !isTempTable(c->t)){
		bat->cs.ts = tr->ts;
		return load_cs(tr, &bat->cs, type, c->base.id);
	} else if (bat && bat->cs.bid && !isTempTable(c->t)) {
		return new_persistent_delta(ATOMIC_PTR_GET(&c->data));
	} else {
		sql_column *fc = NULL;
		size_t cnt = 0;

		/* alter ? */
		if (ol_first_node(c->t->columns) && (fc = ol_first_node(c->t->columns)->data) != NULL) {
			storage *s = ATOMIC_PTR_GET(&fc->t->data);
			cnt = segs_end(s->segs, tr, c->t);
		}
		if (cnt && fc != c) {
			sql_delta *d = ATOMIC_PTR_GET(&fc->data);

			if (d->cs.bid) {
				bat->cs.bid = copyBat(d->cs.bid, type, 0);
				if(bat->cs.bid == BID_NIL)
					ok = LOG_ERR;
			}
			if (d->cs.uibid) {
				bat->cs.uibid = e_bat(TYPE_oid);
				if (bat->cs.uibid == BID_NIL)
					ok = LOG_ERR;
			}
			if (d->cs.uvbid) {
				bat->cs.uvbid = e_bat(type);
				if(bat->cs.uvbid == BID_NIL)
					ok = LOG_ERR;
			}
			bat->cs.alter = 1;
		} else {
			BAT *b = bat_new(type, c->t->sz, PERSISTENT);
			if (!b) {
				ok = LOG_ERR;
			} else {
				create_delta(ATOMIC_PTR_GET(&c->data), b);
				bat_destroy(b);
			}

			if (!new) {
				bat->cs.uibid = e_bat(TYPE_oid);
				if (bat->cs.uibid == BID_NIL)
					ok = LOG_ERR;
				bat->cs.uvbid = e_bat(type);
				if(bat->cs.uvbid == BID_NIL)
					ok = LOG_ERR;
			}
		}
		bat->cs.ucnt = 0;

		if (new /*&& !isTempTable(c->t)*/ && !isNew(c->t) /* alter */)
			trans_add(tr, &c->base, bat, &tc_gc_col, &commit_create_col, isTempTable(c->t)?NULL:&log_create_col);
	}
	return ok;
}

static int
log_create_col_(sql_trans *tr, sql_column *c)
{
	assert(!isTempTable(c->t));
	return log_create_delta(tr,  ATOMIC_PTR_GET(&c->data), c->base.id);
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
	(void)tr; (void)oldest;

	if(!isTempTable(c->t)) {
		sql_delta *delta = ATOMIC_PTR_GET(&c->data);
		assert(delta->cs.ts == tr->tid);
		delta->cs.ts = commit_ts;

		assert(delta->next == NULL);
		if (!delta->cs.alter)
			ok = merge_delta(delta);
		delta->cs.alter = 0;
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
		bat->cs.refcnt = 1;
	}

	if (new)
		bat->cs.ts = tr->tid;

	if (!isNew(ni) && !isTempTable(ni->t)){
		bat->cs.ts = 1;
		return load_cs(tr, &bat->cs, type, ni->base.id);
	} else if (bat && bat->cs.bid && !isTempTable(ni->t)) {
		return new_persistent_delta(ATOMIC_PTR_GET(&ni->data));
	} else {
		sql_column *c = ol_first_node(ni->t->columns)->data;
		sql_delta *d;

		d = col_timestamp_delta(tr, c);
		/* Here we also handle indices created through alter stmts */
		/* These need to be created aligned to the existing data */
		if (d->cs.bid) {
			bat->cs.bid = copyBat(d->cs.bid, type, 0);
			if(bat->cs.bid == BID_NIL)
				ok = LOG_ERR;
		}
		bat->cs.ucnt = 0;
		if (!isNew(c))
			bat->cs.alter = 1;

		if (!new) {
			bat->cs.uibid = e_bat(TYPE_oid);
			if (bat->cs.uibid == BID_NIL)
				ok = LOG_ERR;
			bat->cs.uvbid = e_bat(type);
			if(bat->cs.uvbid == BID_NIL)
				ok = LOG_ERR;
		}
		bat->cs.ucnt = 0;
		if (new && !isNew(ni->t) /* alter */)
			trans_add(tr, &ni->base, bat, &tc_gc_idx, &commit_create_idx, isTempTable(ni->t)?NULL:&log_create_idx);
	}
	return ok;
}

static int
log_create_idx_(sql_trans *tr, sql_idx *i)
{
	assert(!isTempTable(i->t));
	return log_create_delta(tr, ATOMIC_PTR_GET(&i->data), i->base.id);
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
	(void)tr; (void)oldest;

	if(!isTempTable(i->t)) {
		sql_delta *delta = ATOMIC_PTR_GET(&i->data);
		assert(delta->cs.ts == tr->tid);
		delta->cs.ts = commit_ts;

		assert(delta->next == NULL);
		ok = merge_delta(delta);
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
load_storage(sql_trans *tr, sql_table *t, storage *s, sqlid id)
{
	int ok = load_cs(tr, &s->cs, TYPE_msk, id);
	BAT *b = NULL, *ib = NULL;

	if (ok != LOG_OK)
		return ok;
	if (!(b = temp_descriptor(s->cs.bid)))
		return LOG_ERR;
	ib = b;

	if (b->ttype == TYPE_msk || mask_cand(b))
		b = BATunmask(b);

	if (BATcount(b)) {
		if (ok == LOG_OK)
			s->segs = new_segments(tr, BATcount(ib));
		if (BATtdense(b)) {
			size_t start = b->tseqbase;
			size_t cnt = BATcount(b);
			ok = delete_range(tr, t, s, start, cnt);
		} else {
			assert(BATtordered(b));
			BUN icnt = BATcount(b);
			oid *o = Tloc(b,0), n = o[0]+1;
			size_t lcnt = 1;
			for (size_t i=1; i<icnt; i++) {
				if (o[i] == n) {
					lcnt++;
					n++;
				} else {
					if ((ok = delete_range(tr, t, s, n-lcnt, lcnt)) != LOG_OK)
						break;
					lcnt = 0;
				}
				if (!lcnt) {
					n = o[i]+1;
					lcnt = 1;
				}
			}
			if (lcnt && ok == LOG_OK)
				ok = delete_range(tr, t, s, n-lcnt, lcnt);
		}
		if (ok == LOG_OK)
			for (segment *seg = s->segs->h; seg; seg = seg->next)
				if (seg->ts == tr->tid)
					seg->ts = 1;
	} else {
		if (ok == LOG_OK) {
			s->segs = new_segments(tr, BATcount(quick_descriptor(s->cs.bid)));
			segment *seg = s->segs->h;
			if (seg->ts == tr->tid)
				seg->ts = 1;
		}
	}
	if (b != ib)
		bat_destroy(b);
	bat_destroy(ib);

	return ok;
}

static int
create_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK, new = 0;
	BAT *b;
	storage *bat = ATOMIC_PTR_GET(&t->data);

	if (!bat) {
		new = 1;
		bat = ZNEW(storage);
		ATOMIC_PTR_SET(&t->data, bat);
		if(!bat)
			return LOG_ERR;
		bat->cs.refcnt = 1;
	}
	if (new)
		bat->cs.ts = tr->tid;

	if (!isNew(t) && !isTempTable(t)) {
		bat->cs.ts = tr->ts;
		return load_storage(tr, t, bat, t->base.id);
	} else if (bat->cs.bid && !isTempTable(t)) {
		return ok;
	} else if (!bat->cs.bid) {
		assert(!bat->segs);
		bat->segs = new_segments(tr, 0);

		b = bat_new(TYPE_msk, t->sz, PERSISTENT);
		if(b != NULL) {
			bat_set_access(b, BAT_READ);
			bat->cs.bid = temp_create(b);
			bat_destroy(b);
		} else {
			ok = LOG_ERR;
		}
		if (new)
			trans_add(tr, &t->base, bat, &tc_gc_del, &commit_create_del, isTempTable(t)?NULL:&log_create_del);
	}
	return ok;
}

static int
log_segment(sql_trans *tr, segment *s, sqlid id)
{
	sqlstore *store = tr->store;
	msk m = s->deleted;
	return log_constant(store->logger, TYPE_msk, &m, id, s->start, s->end-s->start)==GDK_SUCCEED?LOG_OK:LOG_ERR;
}

static int
log_segments(sql_trans *tr, segments *segs, sqlid id)
{
	/* log segments */
	for (segment *seg = segs->h; seg; seg=seg->next) {
		if (seg->ts == tr->tid) {
			if (log_segment(tr, seg, id) != LOG_OK)
				return LOG_ERR;
		}
	}
	return LOG_OK;
}

static int
log_create_storage(sql_trans *tr, storage *bat, sql_table *t)
{
	BAT *b;
	int ok;

	if (GDKinmemory(0))
		return LOG_OK;

	b = temp_descriptor(bat->cs.bid);
	if (b == NULL)
		return LOG_ERR;

	sqlstore *store = tr->store;
	bat_set_access(b, BAT_READ);
	/* set bits correctly */
	ok = segments2cs(tr, bat->segs, &bat->cs, t);
	if (ok == LOG_OK)
		ok = (log_bat_persists(store->logger, b, t->base.id) == GDK_SUCCEED)?LOG_OK:LOG_ERR;
	if (ok == LOG_OK)
		ok = log_segments(tr, bat->segs, t->base.id);
	bat_destroy(b);
	return ok;
}

static int
log_create_del(sql_trans *tr, sql_change *change)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;

	if (t->base.deleted)
		return ok;
	assert(!isTempTable(t));
	ok = log_create_storage(tr, ATOMIC_PTR_GET(&t->data), t);
	if (ok == LOG_OK) {
		for(node *n = ol_first_node(t->columns); n && ok == LOG_OK; n = n->next) {
			sql_column *c = n->data;

			ok = log_create_col_(tr, c);
		}
		if (t->idxs) {
			for(node *n = ol_first_node(t->idxs); n && ok == LOG_OK; n = n->next) {
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
		storage *dbat = ATOMIC_PTR_GET(&t->data);
		merge_segments(dbat->segs, tr, change, commit_ts, commit_ts/* create is we are alone */ /*oldest*/);
		assert(dbat->cs.ts == tr->tid);
		dbat->cs.ts = commit_ts;
		if (ok == LOG_OK) {
			for(node *n = ol_first_node(t->columns); n && ok == LOG_OK; n = n->next) {
				sql_column *c = n->data;

				ok = commit_create_col_(tr, c, commit_ts, oldest);
			}
			if (t->idxs) {
				for(node *n = ol_first_node(t->idxs); n && ok == LOG_OK; n = n->next) {
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
log_destroy_delta(sql_trans *tr, sql_delta *b, sqlid id)
{
	gdk_return ok = GDK_SUCCEED;

	sqlstore *store = tr->store;
	if (!GDKinmemory(0) && b && b->cs.bid)
		ok = log_bat_transient(store->logger, id);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
destroy_col(sqlstore *store, sql_column *c)
{
	(void)store;
	int ok = LOG_OK;
	if (ATOMIC_PTR_GET(&c->data))
		ok = destroy_delta(ATOMIC_PTR_GET(&c->data), true);
	ATOMIC_PTR_SET(&c->data, NULL);
	return ok;
}

static int
log_destroy_col_(sql_trans *tr, sql_column *c)
{
	int ok = LOG_OK;
	assert(!isTempTable(c->t));
	if (!tr->parent) /* don't write save point commits */
		ok = log_destroy_delta(tr, ATOMIC_PTR_GET(&c->data), c->base.id);
	return ok;
}

static int
log_destroy_col(sql_trans *tr, sql_change *change)
{
	sql_column *c = (sql_column*)change->obj;
	int res = log_destroy_col_(tr, c);
	change->obj = NULL;
	column_destroy(tr->store, c);
	return res;
}

static int
destroy_idx(sqlstore *store, sql_idx *i)
{
	(void)store;
	int ok = LOG_OK;
	if (ATOMIC_PTR_GET(&i->data))
		ok = destroy_delta(ATOMIC_PTR_GET(&i->data), true);
	ATOMIC_PTR_SET(&i->data, NULL);
	return ok;
}

static int
log_destroy_idx_(sql_trans *tr, sql_idx *i)
{
	int ok = LOG_OK;
	assert(!isTempTable(i->t));
	if (ATOMIC_PTR_GET(&i->data)) {
		if (!tr->parent) /* don't write save point commits */
			ok = log_destroy_delta(tr, ATOMIC_PTR_GET(&i->data), i->base.id);
	}
	return ok;
}

static int
log_destroy_idx(sql_trans *tr, sql_change *change)
{
	sql_idx *i = (sql_idx*)change->obj;
	int res = log_destroy_idx_(tr, i);
	change->obj = NULL;
	idx_destroy(tr->store, i);
	return res;
}

static int
destroy_del(sqlstore *store, sql_table *t)
{
	(void)store;
	int ok = LOG_OK;
	if (ATOMIC_PTR_GET(&t->data))
		ok = destroy_storage(ATOMIC_PTR_GET(&t->data));
	ATOMIC_PTR_SET(&t->data, NULL);
	return ok;
}

static int
log_destroy_storage(sql_trans *tr, storage *bat, sqlid id)
{
	gdk_return ok = GDK_SUCCEED;

	sqlstore *store = tr->store;
	if (!GDKinmemory(0) && !tr->parent && /* don't write save point commits */
	    bat && bat->cs.bid)
		ok = log_bat_transient(store->logger, id);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_destroy_del(sql_trans *tr, sql_change *change)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;

	assert(!isTempTable(t));
	ok = log_destroy_storage(tr, ATOMIC_PTR_GET(&t->data), t->base.id);
	if (ok == LOG_OK) {
		for(node *n = ol_first_node(t->columns); n && ok == LOG_OK; n = n->next) {
			sql_column *c = n->data;

			ok = log_destroy_col_(tr, c);
		}
		if (t->idxs) {
			for(node *n = ol_first_node(t->idxs); n && ok == LOG_OK; n = n->next) {
				sql_idx *i = n->data;

				ok = log_destroy_idx_(tr, i);
			}
		}
	}
	return ok;
}

static int
commit_destroy_del( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void)tr;
	(void)change;
	(void)commit_ts;
	(void)oldest;
	return 0;
}

static int
drop_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;

	if (!isNew(t) && !isTempTable(t)) {
		storage *bat = ATOMIC_PTR_GET(&t->data);
		trans_add(tr, &t->base, bat, &tc_gc_del, &commit_destroy_del, &log_destroy_del);
	}
	return ok;
}

static int
drop_col(sql_trans *tr, sql_column *c)
{
	assert(!isNew(c) && !isTempTable(c->t));
	sql_delta *d = ATOMIC_PTR_GET(&c->data);
	trans_add(tr, &c->base, d, &tc_gc_col, &commit_destroy_del, &log_destroy_col);
	return LOG_OK;
}

static int
drop_idx(sql_trans *tr, sql_idx *i)
{
	assert(!isNew(i) && !isTempTable(i->t));
	sql_delta *d = ATOMIC_PTR_GET(&i->data);
	trans_add(tr, &i->base, d, &tc_gc_idx, &commit_destroy_del, &log_destroy_idx);
	return LOG_OK;
}


static BUN
clear_cs(sql_trans *tr, column_storage *cs, bool renew)
{
	BAT *b;
	BUN sz = 0;

	(void)tr;
	if (cs->bid && renew) {
		b = temp_descriptor(cs->bid);
		if (b) {
			sz += BATcount(b);
			bat bid = cs->bid;
			cs->bid = temp_copy(bid, 1); /* create empty copy */
			temp_destroy(bid);
			bat_destroy(b);
		}
	}
	if (cs->uibid) {
		b = temp_descriptor(cs->uibid);
		if (b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b, BUN_NONE);
		}
		bat_destroy(b);
	}
	if (cs->uvbid) {
		b = temp_descriptor(cs->uvbid);
		if(b && !isEbat(b)) {
			bat_clear(b);
			BATcommit(b, BUN_NONE);
		}
		bat_destroy(b);
	}
	cs->cleared = 1;
	cs->ucnt = 0;
	return sz;
}

static BUN
clear_col(sql_trans *tr, sql_column *c, bool renew)
{
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c, renew?&update_conflict:NULL)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);
	if (delta)
		return clear_cs(tr, &delta->cs, renew);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i, bool renew)
{
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if ((delta = bind_idx_data(tr, i, renew?&update_conflict:NULL)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);
	if (delta)
		return clear_cs(tr, &delta->cs, renew);
	return 0;
}

static BUN
clear_storage(sql_trans *tr, storage *s)
{
	BUN sz = count_deletes(s->segs->h, tr);

	clear_cs(tr, &s->cs, true);
	s->cs.cleared = 1;
	if (s->segs)
		destroy_segments(s->segs);
	s->segs = new_segments(tr, 0);
	return sz;
}


/*
 * Clear the table, in general this means replacing the storage,
 * but in case of earlier deletes (or inserts by this transaction), we only mark
 * all segments as deleted.
 * this function returns BUN_NONE on LOG_ERR and BUN_NONE - 1 on LOG_CONFLICT
 */
static BUN
clear_del(sql_trans *tr, sql_table *t, int in_transaction)
{
	int clear = !in_transaction || isTempTable(t), ok = LOG_OK;
	bool conflict = false;
	storage *bat;

	if ((bat = bind_del_data(tr, t, clear?&conflict:NULL)) == NULL)
		return conflict?BUN_NONE-1:BUN_NONE;

	if (!clear) {
		lock_table(tr->store, t->base.id);
		ok = delete_range(tr, t, bat, 0, bat->segs->t->end);
		unlock_table(tr->store, t->base.id);
	}
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, bat, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	if (clear && ok == LOG_OK)
		return clear_storage(tr, bat);
	if (ok == LOG_ERR)
		return BUN_NONE;
	if (ok == LOG_CONFLICT)
		return BUN_NONE - 1;
	return LOG_OK;
}

/* this function returns BUN_NONE on LOG_ERR and BUN_NONE - 1 on LOG_CONFLICT */
static BUN
clear_table(sql_trans *tr, sql_table *t)
{
	int in_transaction = segments_in_transaction(tr, t);
	int clear = !in_transaction || isTempTable(t);

	node *n = ol_first_node(t->columns);
	sql_column *c = n->data;
	BUN sz = count_col(tr, c, 0), clear_ok;

	storage *d = tab_timestamp_storage(tr, t);
	lock_table(tr->store, t->base.id);
	sz -= count_deletes_in_range(d->segs->h, tr, 0, sz);
	unlock_table(tr->store, t->base.id);
	if ((clear_ok = clear_del(tr, t, in_transaction)) >= BUN_NONE - 1)
		return clear_ok;

	for (; n; n = n->next) {
		c = n->data;

		if ((clear_ok = clear_col(tr, c, clear)) >= BUN_NONE - 1)
			return clear_ok;
	}
	if (t->idxs) {
		for (n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *ci = n->data;

			if (isTable(ci->t) && idx_has_column(ci->type) &&
				(clear_ok = clear_idx(tr, ci, clear)) >= BUN_NONE - 1)
				return clear_ok;
		}
	}
	return sz;
}

static int
tr_log_cs( sql_trans *tr, sql_table *t, column_storage *cs, segment *segs, sqlid id)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;

	if (GDKinmemory(0))
		return LOG_OK;

	/*
	if (cs->cleared && log_bat_clear(store->logger, id) != GDK_SUCCEED)
		return LOG_ERR;
		*/

	if (cs->cleared) {
		assert(cs->ucnt == 0);
		BAT *ins = temp_descriptor(cs->bid);
		if (isEbat(ins)) {
			assert(0);
			temp_destroy(cs->bid);
			cs->bid = temp_copy(ins->batCacheid, false);
			bat_destroy(ins);
			ins = temp_descriptor(cs->bid);
		}
		bat_set_access(ins, BAT_READ);
		ok = log_bat_persists(store->logger, ins, id);
		bat_destroy(ins);
		return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	}

	if (isTempTable(t)) {
		assert(0);
	for (; segs; segs=segs->next) {
		if (segs->ts == tr->tid) {
			BAT *ins = temp_descriptor(cs->bid);
			assert(ins);
			assert(BATcount(ins) >= segs->end);
			ok = log_bat(store->logger, ins, id, segs->start, segs->end-segs->start);
			bat_destroy(ins);
		}
	}
	}

	if (ok == GDK_SUCCEED && cs->ucnt && cs->uibid) {
		BAT *ui = temp_descriptor(cs->uibid);
		BAT *uv = temp_descriptor(cs->uvbid);
		/* any updates */
		if (ui == NULL || uv == NULL) {
			ok = GDK_FAIL;
		} else if (BUNlast(uv) > uv->batInserted || BATdirty(uv))
			ok = log_delta(store->logger, ui, uv, id);
		bat_destroy(ui);
		bat_destroy(uv);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
tr_log_delta( sql_trans *tr, sql_table *t, sql_delta *cbat, segment *segs, sqlid id)
{
	return tr_log_cs( tr, t, &cbat->cs, segs, id);
}

static int
log_table_append(sql_trans *tr, sql_table *t, segments *segs)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;

	if (isTempTable(t))
		return LOG_OK;
	size_t end = segs_end(segs, tr, t);
	for (segment *cur = segs->h; cur && ok; cur = cur->next) {
		if (cur->ts == tr->tid && !cur->deleted && cur->start < end) {
			for (node *n = ol_first_node(t->columns); n && ok; n = n->next) {
				sql_column *c = n->data;
				column_storage *cs = ATOMIC_PTR_GET(&c->data);

				/* append col*/
				BAT *ins = temp_descriptor(cs->bid);
				assert(ins);
				assert(BATcount(ins) >= cur->end);
				ok = log_bat(store->logger, ins, c->base.id, cur->start, cur->end-cur->start);
				bat_destroy(ins);
			}
			if (t->idxs) {
				for (node *n = ol_first_node(t->idxs); n && ok; n = n->next) {
					sql_idx *i = n->data;

					if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
						continue;
					column_storage *cs = ATOMIC_PTR_GET(&i->data);

					if (cs) {
						/* append idx */
						BAT *ins = temp_descriptor(cs->bid);
						assert(ins);
						assert(BATcount(ins) >= cur->end);
						ok = log_bat(store->logger, ins, i->base.id, cur->start, cur->end-cur->start);
						bat_destroy(ins);
					}
				}
			}
		}
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
log_storage(sql_trans *tr, sql_table *t, storage *s, sqlid id)
{
	int ok = segments2cs(tr, s->segs, &s->cs, t);
	if (ok == LOG_OK && s->cs.cleared)
		return tr_log_cs(tr, t, &s->cs, s->segs->h, t->base.id);
	if (ok == LOG_OK)
		ok = log_table_append(tr, t, s->segs);
	if (ok == LOG_OK)
		return log_segments(tr, s->segs, id);
	return ok;
}

static int
merge_cs( column_storage *cs)
{
	int ok = LOG_OK;
	BAT *cur = NULL;

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
merge_delta( sql_delta *obat)
{
	return merge_cs(&obat->cs);
}

static int
merge_storage(storage *tdb)
{
	int ok = merge_cs(&tdb->cs);

	if (tdb->next) {
		ok = destroy_storage(tdb->next);
		tdb->next = NULL;
	}
	return ok;
}

static sql_delta *
savepoint_commit_delta( sql_delta *delta, ulng commit_ts)
{
	/* commit ie copy back to the parent transaction */
	if (delta && delta->cs.ts == commit_ts && delta->next) {
		sql_delta *od = delta->next;
		if (od->cs.ts == commit_ts) {
			sql_delta t = *od, *n = od->next;
			*od = *delta;
			od->next = n;
			*delta = t;
			delta->next = NULL;
			destroy_delta(delta, true);
			return od;
		}
	}
	return delta;
}

static int
rollback_delta(sql_trans *tr, sql_delta *delta, int type)
{
	(void)tr;
	if (delta->cs.ucnt) {
		delta->cs.ucnt = 0;
		temp_destroy(delta->cs.uibid);
		temp_destroy(delta->cs.uvbid);
		delta->cs.uibid = e_bat(TYPE_oid);
		delta->cs.uvbid = e_bat(type);
	}
	return LOG_OK;
}

static int
log_update_col( sql_trans *tr, sql_change *change)
{
	sql_column *c = (sql_column*)change->obj;

	if (!isTempTable(c->t) && !tr->parent) {/* don't write save point commits */
		storage *s = ATOMIC_PTR_GET(&c->t->data);
		return tr_log_delta(tr, c->t, ATOMIC_PTR_GET(&c->data), s->segs->h, c->base.id);
	}
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
				merge_delta(delta);
			else /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
				clear_cs(tr, &delta->cs, true);
		} else { /* rollback */
			if (c->t->commit_action == CA_COMMIT/* || c->t->commit_action == CA_PRESERVE*/)
				rollback_delta(tr, delta, c->type.type->localtype);
			else /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
				clear_cs(tr, &delta->cs, true);
		}
		c->t->base.flags = c->base.flags = 0;
	}
	return ok;
}

static int
tc_gc_rollbacked( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;

	sql_delta *d = (sql_delta*)change->data;
	if (d->cs.ts < oldest) {
		destroy_delta(d, false);
		return 1;
	}
	if (d->cs.ts > TRANSACTION_ID_BASE)
		d->cs.ts = store_get_timestamp(store) + 1;
	return 0;
}

static int
tc_gc_rollbacked_storage( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;

	storage *d = (storage*)change->data;
	if (d->cs.ts < oldest) {
		destroy_storage(d);
		return 1;
	}
	if (d->cs.ts > TRANSACTION_ID_BASE)
		d->cs.ts = store_get_timestamp(store) + 1;
	return 0;
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
		delta->cs.ts = commit_ts;
	if (!commit_ts) { /* rollback */
		sql_delta *d = change->data, *o = ATOMIC_PTR_GET(&c->data);

		if (change->ts && c->t->base.new) /* handled by create col */
			return ok;
		if (o != d) {
			while(o && o->next != d)
				o = o->next;
		}
		if (o == ATOMIC_PTR_GET(&c->data))
			ATOMIC_PTR_SET(&c->data, d->next);
		else
			o->next = d->next;
		change->cleanup = &tc_gc_rollbacked;
	} else if (ok == LOG_OK && !tr->parent) {
		sql_delta *d = delta;
		/* clean up and merge deltas */
		while (delta && delta->cs.ts > oldest) {
			delta = delta->next;
		}
		if (delta && delta != d) {
			if (delta->next) {
				ok = destroy_delta(delta->next, true);
				delta->next = NULL;
			}
		}
		if (ok == LOG_OK && delta == d && oldest == commit_ts) {
			lock_column(tr->store, c->base.id);
			ok = merge_delta(delta);
			unlock_column(tr->store, c->base.id);
		}
	} else if (ok == LOG_OK && tr->parent) /* move delta into older and cleanup current save points */
		ATOMIC_PTR_SET(&c->data, savepoint_commit_delta(delta, commit_ts));
	return ok;
}

static int
log_update_idx( sql_trans *tr, sql_change *change)
{
	sql_idx *i = (sql_idx*)change->obj;

	if (!isTempTable(i->t) && !tr->parent) { /* don't write save point commits */
		storage *s = ATOMIC_PTR_GET(&i->t->data);
		return tr_log_delta(tr, i->t, ATOMIC_PTR_GET(&i->data), s->segs->h, i->base.id);
	}
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
				merge_delta(delta);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_cs(tr, &delta->cs, true);
		} else { /* rollback */
			if (i->t->commit_action == CA_COMMIT/* || i->t->commit_action == CA_PRESERVE*/)
				rollback_delta(tr, delta, type);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_cs(tr, &delta->cs, true);
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
		delta->cs.ts = commit_ts;
	if (!commit_ts) { /* rollback */
		sql_delta *d = change->data, *o = ATOMIC_PTR_GET(&i->data);

		if (change->ts && i->t->base.new) /* handled by create col */
			return ok;
		if (o != d) {
			while(o && o->next != d)
				o = o->next;
		}
		if (o == ATOMIC_PTR_GET(&i->data))
			ATOMIC_PTR_SET(&i->data, d->next);
		else
			o->next = d->next;
		change->cleanup = &tc_gc_rollbacked;
	} else if (ok == LOG_OK && !tr->parent) {
		sql_delta *d = delta;
		/* clean up and merge deltas */
		while (delta && delta->cs.ts > oldest) {
			delta = delta->next;
		}
		if (delta && delta != d) {
			if (delta->next) {
				ok = destroy_delta(delta->next, true);
				delta->next = NULL;
			}
		}
		if (ok == LOG_OK && delta == d && oldest == commit_ts) {
			lock_column(tr->store, i->base.id);
			ok = merge_delta(delta);
			unlock_column(tr->store, i->base.id);
		}
	} else if (ok == LOG_OK && tr->parent) /* cleanup older save points */
		ATOMIC_PTR_SET(&i->data, savepoint_commit_delta(delta, commit_ts));
	return ok;
}

static storage *
savepoint_commit_storage( storage *dbat, ulng commit_ts)
{
	if (dbat && dbat->cs.ts == commit_ts && dbat->next) {
		assert(0);
		storage *od = dbat->next;
		if (od->cs.ts == commit_ts) {
			storage t = *od, *n = od->next;
			*od = *dbat;
			od->next = n;
			*dbat = t;
			dbat->next = NULL;
			destroy_storage(dbat);
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
		return log_storage(tr, t, ATOMIC_PTR_GET(&t->data), t->base.id);
	return LOG_OK;
}

static int
rollback_storage(sql_trans *tr, storage *dbat)
{
	(void)tr;
	(void)dbat;
	return LOG_OK;
}

static int
commit_storage(sql_trans *tr, storage *dbat)
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
	storage *dbat = ATOMIC_PTR_GET(&t->data);

	if (isTempTable(t)) {
		if (commit_ts) { /* commit */
			if (t->commit_action == CA_COMMIT || t->commit_action == CA_PRESERVE)
				commit_storage(tr, dbat);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_storage(tr, dbat);
		} else { /* rollback */
			if (t->commit_action == CA_COMMIT/* || t->commit_action == CA_PRESERVE*/)
				rollback_storage(tr, dbat);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_storage(tr, dbat);
		}
		t->base.flags = 0;
		return ok;
	}
	lock_table(tr->store, t->base.id);
	if (!commit_ts) { /* rollback */
		if (dbat->cs.ts == tr->tid) {
			if (change->ts && t->base.new) { /* handled by the create table */
				unlock_table(tr->store, t->base.id);
				return ok;
			}
			storage *d = change->data, *o = ATOMIC_PTR_GET(&t->data);

			if (o != d) {
				while(o && o->next != d)
					o = o->next;
			}
			if (o == ATOMIC_PTR_GET(&t->data)) {
				assert(d->next);
				ATOMIC_PTR_SET(&t->data, d->next);
			} else
				o->next = d->next;
			d->next = NULL;
			change->cleanup = &tc_gc_rollbacked_storage;
		} else
			rollback_segments(dbat->segs, tr, change, oldest);
	} else if (ok == LOG_OK && !tr->parent) {
		storage *d = dbat;
		if (dbat->cs.ts == tr->tid) /* cleared table */
			dbat->cs.ts = commit_ts;
		merge_segments(dbat->segs, tr, change, commit_ts, oldest);
		if (ok == LOG_OK && dbat == d && oldest == commit_ts)
			ok = merge_storage(dbat);
	} else if (ok == LOG_OK && tr->parent) {/* cleanup older save points */
		merge_segments(dbat->segs, tr, change, commit_ts, oldest);
		ATOMIC_PTR_SET(&t->data, savepoint_commit_storage(dbat, commit_ts));
	}
	unlock_table(tr->store, t->base.id);
	return ok;
}

/* only rollback (content version) case for now */
static int
tc_gc_col( sql_store Store, sql_change *change, ulng oldest)
{
	(void)Store;
	sql_column *c = (sql_column*)change->obj;

	if (!c) /* cleaned earlier */
		return 1;

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&c->data) != change->data || isTempTable(c->t)) /* data is freed by commit */
		return 1;
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data;
	if (d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_delta(d->next, true);
		d->next = NULL;
	}
	return 1;
}

static int
tc_gc_idx( sql_store Store, sql_change *change, ulng oldest)
{
	(void)Store;
	sql_idx *i = (sql_idx*)change->obj;

	if (!i) /* cleaned earlier */
		return 1;

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&i->data) != change->data || isTempTable(i->t)) /* data is freed by commit */
		return 1;
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data;
	if (d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_delta(d->next, true);
		d->next = NULL;
	}
	return 1;
}

static int
tc_gc_del( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;
	sql_table *t = (sql_table*)change->obj;

	(void)store;
	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&t->data) != change->data || isTempTable(t)) /* data is freed by commit */
		return 1;
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	storage *d = (storage*)change->data;
	if (d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_storage(d->next);
		d->next = NULL;
	}
	return 1;
}

static BAT *
add_offsets(BAT *pos, BUN slot, size_t nr, size_t total)
{
	if (nr == 0)
		return pos;
	assert (nr > 0);
	if (!pos && nr == total)
		return BATdense(0, slot, nr);
	if (!pos) {
		pos = COLnew(0, TYPE_oid, total, TRANSIENT);
		if (!pos)
			return NULL;
	}
	for(size_t i = 0; i < nr; i++) {
		oid v = slot + i;
		if (BUNappend(pos, &v, true) != GDK_SUCCEED)
			return NULL;
	}
	return pos;
}

static BAT *
claim_segmentsV2(sql_trans *tr, sql_table *t, storage *s, size_t cnt)
{
	int in_transaction = segments_in_transaction(tr, t), ok = LOG_OK;
	assert(s->segs);
	ulng oldest = store_oldest(tr->store);
	BUN slot = 0;
	BAT *pos = NULL;
	size_t total = cnt;

	/* naive vacuum approach, iterator through segments, use deleted segments or create new segment at the end */
	for (segment *seg = s->segs->h, *p = NULL; seg && cnt && ok == LOG_OK; p = seg, seg = seg->next) {
		if (seg->deleted && seg->ts < oldest && seg->end > seg->start) { /* re-use old deleted or rolledback append */
			if ((seg->end - seg->start) >= cnt) {
				/* if previous is claimed before we could simply adjust the end/start */
				if (p && p->ts == tr->tid && !p->deleted) {
					slot = p->end;
					p->end += cnt;
					seg->start += cnt;
					pos = add_offsets(pos, slot, cnt, total);
					if (!pos) {
						ok = LOG_ERR;
						break;
					}
					cnt = 0;
					break;
				}
				/* we claimed part of the old segment, the split off part needs too stay deleted */
				size_t rcnt = seg->end - seg->start;
				if (rcnt > cnt)
					rcnt = cnt;
				if ((seg=split_segment(s->segs, seg, p, tr, seg->start, rcnt, false)) == NULL) {
					ok = LOG_ERR;
					break;
				}
			}
			seg->ts = tr->tid;
			seg->deleted = false;
			slot = seg->start;
			pos = add_offsets(pos, slot, (seg->end-seg->start), total);
			if (!pos) {
				ok = LOG_ERR;
				break;
			}
			cnt -= (seg->end - seg->start);
		}
	}
	if (ok == LOG_OK && cnt) {
		if (s->segs->t && s->segs->t->ts == tr->tid && !s->segs->t->deleted) {
			slot = s->segs->t->end;
			s->segs->t->end += cnt;
		} else {
			s->segs->t = new_segment(s->segs->t, tr, cnt);
			if (!s->segs->h)
				s->segs->h = s->segs->t;
			slot = s->segs->t->start;
		}
		pos = add_offsets(pos, slot, cnt, total);
		if (!pos)
			ok = LOG_ERR;
	}

	/* hard to only add this once per transaction (probably want to change to once per new segment) */
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t))) {
		trans_add(tr, &t->base, s, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
		if (!isLocalTemp(t))
			tr->logchanges += (int) total;
	}
	assert(BATcount(pos) == total);
	return pos;
}

static BAT *
claim_segments(sql_trans *tr, sql_table *t, storage *s, size_t cnt)
{
	if (cnt > 1)
		return claim_segmentsV2(tr, t, s, cnt);
	int in_transaction = segments_in_transaction(tr, t), ok = LOG_OK;
	assert(s->segs);
	ulng oldest = store_oldest(tr->store);
	BUN slot = 0;
	int reused = 0;

	/* naive vacuum approach, iterator through segments, check for large enough deleted segments
	 * or create new segment at the end */
	for (segment *seg = s->segs->h, *p = NULL; seg && ok == LOG_OK; p = seg, seg = seg->next) {
		if (seg->deleted && seg->ts < oldest && (seg->end-seg->start) >= cnt) { /* re-use old deleted or rolledback append */

			if ((seg->end - seg->start) >= cnt) {

				/* if previous is claimed before we could simply adjust the end/start */
				if (p && p->ts == tr->tid && !p->deleted) {
					slot = p->end;
					p->end += cnt;
					seg->start += cnt;
					reused = 1;
					break;
				}
				/* we claimed part of the old segment, the split off part needs too stay deleted */
				if ((seg=split_segment(s->segs, seg, p, tr, seg->start, cnt, false)) == NULL)
					ok = LOG_ERR;
			}
			seg->ts = tr->tid;
			seg->deleted = false;
			slot = seg->start;
			reused = 1;
			break;
		}
	}
	if (ok == LOG_OK && !reused) {
		if (s->segs->t && s->segs->t->ts == tr->tid && !s->segs->t->deleted) {
			slot = s->segs->t->end;
			s->segs->t->end += cnt;
		} else {
			s->segs->t = new_segment(s->segs->t, tr, cnt);
			if (!s->segs->h)
				s->segs->h = s->segs->t;
			slot = s->segs->t->start;
		}
	}

	/* hard to only add this once per transaction (probably want to change to once per new segment) */
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t))) {
		trans_add(tr, &t->base, s, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
		if (!isLocalTemp(t))
			tr->logchanges += (int) cnt;
	}
	if (ok == LOG_OK)
		return BATdense(0, slot, cnt);
	return NULL;
}

/*
 * Claim cnt slots to store the tuples. The claim_tab should claim storage on the level
 * of the global transaction and mark the newly added storage slots unused on the global
 * level but used on the local transaction level. Besides this the local transaction needs
 * to update (and mark unused) any slot inbetween the old end and new slots.
 * */
static void *
claim_tab(sql_trans *tr, sql_table *t, size_t cnt)
{
	storage *s;

	/* we have a single segment structure for each persistent table
	 * for temporary tables each has its own */
	if ((s = bind_del_data(tr, t, NULL)) == NULL)
		return NULL;

	lock_table(tr->store, t->base.id);
	BAT *slots = claim_segments(tr, t, s, cnt); /* find slot(s) */
	unlock_table(tr->store, t->base.id);
	return slots;
}

/* some tables cannot be updates at the concurrently (user/roles etc) */
static void *
key_claim_tab(sql_trans *tr, sql_table *t, size_t cnt)
{
	storage *s;

	/* we have a single segment structure for each persistent table
	 * for temporary tables each has its own */
	if ((s = bind_del_data(tr, t, NULL)) == NULL || segments_conflict(tr, s->segs))
		/* TODO check for other inserts ! */
		return NULL;

	lock_table(tr->store, t->base.id);
	BAT *slots = claim_segments(tr, t, s, cnt); /* find slot(s) */
	unlock_table(tr->store, t->base.id);
	return slots;
}

static BAT *
segments2cands(segment *s, sql_trans *tr, size_t start, size_t end)
{
	size_t nr = end - start, pos = 0;
	uint32_t cur = 0;
	BAT *b = COLnew(0, TYPE_msk, nr, TRANSIENT), *bn = NULL;
	if (!b)
		return NULL;

	uint32_t *restrict dst = Tloc(b, 0);
	for( ; s; s=s->next) {
		if (s->end < start)
			continue;
		if (s->start >= end)
			break;
		msk m = (SEG_IS_VALID(s, tr));
		size_t lnr = s->end-s->start;
		if (s->start < start)
			lnr -= (start - s->start);
		if (s->end > end)
			lnr -= s->end - end;

		if (m) {
			size_t used = pos&31, end = 32;
			if (used) {
				if (lnr < (32-used))
					end = used + lnr;
				for(size_t j=used; j < end; j++, pos++, lnr--)
					cur |= 1U<<j;
				if (end == 32) {
					*dst++ = cur;
					cur = 0;
				}
			}
			size_t full = lnr/32;
			size_t rest = lnr%32;
			for(size_t i = 0; i<full; i++, pos+=32, lnr-=32)
				*dst++ = ~0;
			for(size_t j=0; j < rest; j++, pos++, lnr--)
				cur |= 1U<<j;
			assert(lnr==0);
		} else {
			size_t used = pos&31, end = 32;
			if (used) {
				if (lnr < (32-used))
					end = used + lnr;

				pos+= (end-used);
				lnr-= (end-used);
				if (end == 32) {
					*dst++ = cur;
					cur = 0;
				}
			}
			size_t full = lnr/32;
			size_t rest = lnr%32;
			for(size_t i = 0; i<full; i++, pos+=32, lnr-=32)
				*dst++ = 0;
			pos+= rest;
			lnr-= rest;
			assert(lnr==0);
		}
	}
	if (pos%32)
		*dst=cur;
	BATsetcount(b, nr);
	bn = BATmaskedcands(start, nr, b, true);
	BBPreclaim(b);
	(void)pos;
	assert (pos == nr);
	return bn;
}

static void *					/* BAT * */
bind_cands(sql_trans *tr, sql_table *t, int nr_of_parts, int part_nr)
{
	/* with nr_of_parts - part_nr we can adjust parts */
	storage *s = tab_timestamp_storage(tr, t);

	if (!s)
		return NULL;
	size_t nr = segs_end(s->segs, tr, t);

	if (!nr)
		return BATdense(0, 0, 0);

	/* compute proper part */
	size_t part_size = nr/nr_of_parts;
	size_t start = part_size * part_nr;
	size_t end = start + part_size;
	if (part_nr == (nr_of_parts-1))
		end = nr;
	assert(end <= nr);
	/* step one no deletes -> dense range */
	size_t dnr = count_deletes(s->segs->h, tr);
	if (!dnr)
		return BATdense(start, start, end-start);
	lock_table(tr->store, t->base.id);
	BAT *r = segments2cands(s->segs->h, tr, start, end);
	unlock_table(tr->store, t->base.id);
	return r;
}

void
bat_storage_init( store_functions *sf)
{
	sf->bind_col = &bind_col;
	sf->bind_idx = &bind_idx;
	sf->bind_cands = &bind_cands;

	sf->claim_tab = &claim_tab;
	sf->key_claim_tab = &key_claim_tab;

	sf->append_col = &append_col;
	sf->append_idx = &append_idx;

	sf->update_col = &update_col;
	sf->update_idx = &update_idx;

	sf->delete_tab = &delete_tab;

	sf->count_del = &count_del;
	sf->count_col = &count_col;
	sf->count_idx = &count_idx;
	sf->dcount_col = &dcount_col;
	sf->sorted_col = &sorted_col;
	sf->unique_col = &unique_col;
	sf->double_elim_col = &double_elim_col;

	sf->col_dup = &col_dup;
	sf->idx_dup = &idx_dup;
	sf->del_dup = &del_dup;

	sf->create_col = &create_col;	/* create and add too change list */
	sf->create_idx = &create_idx;
	sf->create_del = &create_del;

	sf->destroy_col = &destroy_col;	/* free resources */
	sf->destroy_idx = &destroy_idx;
	sf->destroy_del = &destroy_del;

	sf->drop_col = &drop_col;		/* add drop too change list */
	sf->drop_idx = &drop_idx;
	sf->drop_del = &drop_del;

	sf->clear_table = &clear_table;
}

#if 0
static lng
log_get_nr_inserted(sql_column *fc, lng *offset)
{
	lng cnt = 0;

	if (!fc || GDKinmemory(0))
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

	if (!ft || GDKinmemory(0))
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
