/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "bat_storage.h"
#include "bat_utils.h"
#include "sql_string.h"
#include "matomic.h"

#define FATAL_MERGE_FAILURE "Out Of Memory during critical merge operation: %s"
#define NOT_TO_BE_LOGGED(t) (isUnloggedTable(t) || isTempTable(t))

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
static int tc_gc_upd_col( sql_store Store, sql_change *c, ulng oldest);
static int tc_gc_upd_idx( sql_store Store, sql_change *c, ulng oldest);

static lng merge_delta( sql_delta *obat);

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

/* Delete (in current trans or by some other finished transaction, or re-used segment which used to be deleted */
#define SEG_IS_DELETED(seg,tr) \
	((seg->deleted && (VALID_4_READ(seg->ts, tr) || !OLD_VALID_4_READ(seg->ts, seg->oldts, tr))) || \
	 (!seg->deleted && !VALID_4_READ(seg->ts, tr)))

/* A segment is part of the current transaction is someway or is deleted by some other transaction but use to be valid */
#define SEG_IS_VALID(seg, tr) \
		((!seg->deleted && VALID_4_READ(seg->ts, tr)) || \
		 (seg->deleted && OLD_VALID_4_READ(seg->ts, seg->oldts, tr)))

static inline BAT *
transfer_to_systrans(BAT *b)
{
	/* transfer a BAT from the TRANSIENT farm to the SYSTRANS farm */
	MT_lock_set(&b->theaplock);
	if (VIEWtparent(b) || VIEWvtparent(b)) {
		MT_lock_unset(&b->theaplock);
		BAT *bn = COLcopy(b, b->ttype, true, SYSTRANS);
		BBPreclaim(b);
		return bn;
	}
	if (b->theap->farmid == TRANSIENT ||
		(b->tvheap && b->tvheap->farmid == TRANSIENT)) {
		QryCtx *qc = MT_thread_get_qry_ctx();
		if (qc) {
			if (b->theap->farmid == TRANSIENT && b->theap->parentid == b->batCacheid) {
				ATOMIC_SUB(&qc->datasize, b->theap->size);
				b->theap->farmid = SYSTRANS;
				b->batRole = SYSTRANS;
			}
			if (b->tvheap && b->tvheap->farmid == TRANSIENT && b->tvheap->parentid == b->batCacheid) {
				ATOMIC_SUB(&qc->datasize, b->tvheap->size);
				b->tvheap->farmid = SYSTRANS;
			}
		}
	}
	MT_lock_unset(&b->theaplock);
	return b;
}

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
	MT_lock_set(&store->column_locks[id&(NR_COLUMN_LOCKS-1)]);
}

static void
unlock_column(sqlstore *store, sqlid id)
{
	MT_lock_unset(&store->column_locks[id&(NR_COLUMN_LOCKS-1)]);
}

static void
trans_add_obj_(sql_trans *tr, sql_base *b, void *data, tc_cleanup_fptr cleanup, tc_commit_fptr commit, tc_log_fptr log)
{
	bool found = false;
	MT_lock_set(&tr->lock);
	if (tr->changes) {
		for(node *n = tr->changes->h; n && !found; n = n->next) {
			sql_change *c = n->data;
			if (c->obj->id == b->id)
				found = true;
		}
	}
 	MT_lock_unset(&tr->lock);
	if (!found)
		trans_add(tr, dup_base(b), data, cleanup, commit, log);
}

static void
trans_add_obj(sql_trans *tr, sql_base *b, void *data, tc_cleanup_fptr cleanup, tc_commit_fptr commit, tc_log_fptr log)
{
	assert(cleanup);
	trans_add(tr, dup_base(b), data, cleanup, commit, log);
}

static void
trans_add_table(sql_trans *tr, sql_base *b, sql_table *t, void *data, tc_cleanup_fptr cleanup, tc_commit_fptr commit, tc_log_fptr log)
{
	assert(cleanup);
	dup_base(&t->base);
	trans_add(tr, b, data, cleanup, commit, log);
}

static int
tc_gc_seg( sql_store Store, sql_change *change, ulng oldest)
{
	segment *s = change->data;

	if (s->ts <= oldest) {
		while(s) {
			segment *n = s->prev;
			_DELETE(s);
			s = n;
		}
		sqlstore *store = Store;
		table_destroy(store, (sql_table*)change->obj);
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
		*n = (segment) {
			.ts = tr->tid,
			.oldts = 0,
			.deleted = false,
			.start = 0,
			.end = cnt,
			.next = ATOMIC_PTR_VAR_INIT(NULL),
			.prev = NULL,
		};
		if (o) {
			n->start += o->end;
			n->end += o->end;
			ATOMIC_PTR_SET(&o->next, n);
		}
	}
	return n;
}

static segment *
split_segment(segments *segs, segment *o, segment *p, sql_trans *tr, size_t start, size_t cnt, bool deleted)
{
	assert(tr);
	if (o->start == start && o->end == start+cnt) {
		assert(o->deleted != deleted || o->ts < TRANSACTION_ID_BASE);
		o->oldts = o->ts;
		o->ts = tr->tid;
		o->deleted = deleted;
		return o;
	}
	segment *n = (segment*)GDKmalloc(sizeof(segment));

	if (!n)
		return NULL;
	n->prev = NULL;

	if (o->ts == tr->tid) {
		n->oldts = 0;
		n->ts = 1;
		n->deleted = true;
	} else {
		n->oldts = o->ts;
		n->ts = tr->tid;
		n->deleted = deleted;
	}
	if (start == o->start) {
		/* 2-way split: o remains latter part of segment, new one is
		 * inserted before */
		n->start = o->start;
		n->end = n->start + cnt;
		ATOMIC_PTR_INIT(&n->next, o);
		if (segs->h == o)
			segs->h = n;
		if (p)
			ATOMIC_PTR_SET(&p->next, n);
		o->start = n->end;
	} else if (start+cnt == o->end) {
		/* 2-way split: o remains first part of segment, new one is
		 * added after */
		n->start = o->end - cnt;
		n->end = o->end;
		ATOMIC_PTR_INIT(&n->next, ATOMIC_PTR_GET(&o->next));
		ATOMIC_PTR_SET(&o->next, n);
		if (segs->t == o)
			segs->t = n;
		o->end = n->start;
	} else {
		/* 3-way split: o remains first part of segment, two new ones
		 * are added after */
		segment *n2 = GDKmalloc(sizeof(segment));
		if (n2 == NULL) {
			GDKfree(n);
			return NULL;
		}
		ATOMIC_PTR_INIT(&n->next, n2);
		n->start = start;
		n->end = start + cnt;
		*n2 = *o;
		ATOMIC_PTR_INIT(&n2->next, ATOMIC_PTR_GET(&o->next));
		n2->start = n->end;
		n2->prev = NULL;
		if (segs->t == o)
			segs->t = n2;
		ATOMIC_PTR_SET(&o->next, n);
		o->end = start;
	}
	return n;
}

static void
rollback_segments(segments *segs, sql_trans *tr, sql_change *change, ulng oldest)
{
	segment *cur = segs->h, *seg = NULL;
	for (; cur; cur = ATOMIC_PTR_GET(&cur->next)) {
		if (cur->ts == tr->tid) { /* revert */
			if (!cur->deleted || cur->ts == cur->oldts)
				ATOMIC_ADD(&segs->deleted, cur->end - cur->start);
			else
				ATOMIC_SUB(&segs->deleted, cur->end - cur->start);

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
				ATOMIC_PTR_SET(&seg->next, ATOMIC_PTR_GET(&cur->next));
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

static size_t
segs_end_include_deleted( segments *segs, sql_trans *tr)
{
	size_t cnt = 0;
	segment *s = segs->h, *l = NULL;

	for(;s; s = ATOMIC_PTR_GET(&s->next)) {
		if (s->ts == tr->tid || SEG_IS_VALID(s, tr))
				l = s;
	}
	if (l)
		cnt = l->end;
	return cnt;
}

static int
segments2cs(sql_trans *tr, segments *segs, column_storage *cs)
{
	/* set bits correctly */
	BAT *b = temp_descriptor(cs->bid);

	if (!b)
		return LOG_ERR;
	segment *s = segs->h;

	size_t nr = segs_end_include_deleted(segs, tr);
	size_t rounded_nr = ((nr+31)&~31);
	if (rounded_nr > BATcapacity(b) && BATextend(b, rounded_nr) != GDK_SUCCEED) {
		bat_destroy(b);
		return LOG_ERR;
	}

	/* disable all properties here */
	MT_lock_set(&b->theaplock);
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	BUN cnt = BATcount(b);

	uint32_t *restrict dst;
	for (; s ; s=ATOMIC_PTR_GET(&s->next)) {
		if (s->start >= nr)
			break;
		if (s->ts == tr->tid && s->end != s->start) {
			if (cnt < s->start) { /* first mark as deleted ! */
				size_t lnr = s->start-cnt;
				size_t pos = cnt;
				dst = (uint32_t *) Tloc(b, 0) + (pos/32);
				uint32_t cur = 0;

				size_t used = pos&31, end = 32;
				if (used) {
					if (lnr < (32-used))
						end = used + lnr;
					assert(end > used);
					cur |= ((1U << (end - used)) - 1) << used;
					lnr -= end - used;
					*dst++ |= cur;
					cur = 0;
				}
				size_t full = lnr/32;
				size_t rest = lnr%32;
				if (full > 0) {
					memset(dst, ~0, full * sizeof(*dst));
					dst += full;
					lnr -= full * 32;
				}
				if (rest > 0) {
					cur |= (1U << rest) - 1;
					lnr -= rest;
					*dst |= cur;
				}
				assert(lnr==0);
			}
			size_t lnr = s->end-s->start;
			size_t pos = s->start;
			dst = (uint32_t *) Tloc(b, 0) + (pos/32);
			uint32_t cur = 0;
			size_t used = pos&31, end = 32;
			if (used) {
				if (lnr < (32-used))
					end = used + lnr;
				assert(end > used);
				cur |= ((1U << (end - used)) - 1) << used;
				lnr -= end - used;
				*dst = s->deleted ? *dst | cur : *dst & ~cur;
				dst++;
				cur = 0;
			}
			size_t full = lnr/32;
			size_t rest = lnr%32;
			if (full > 0) {
				memset(dst, s->deleted?~0:0, full * sizeof(*dst));
				dst += full;
				lnr -= full * 32;
			}
			if (rest > 0) {
				cur |= (1U << rest) - 1;
				lnr -= rest;
				*dst = s->deleted ? *dst | cur : *dst & ~cur;
			}
			assert(lnr==0);
			if (cnt < s->end)
				cnt = s->end;
		}
	}
	if (nr > BATcount(b)) {
		BATsetcount(b, nr);
	}
	b->theap->dirty = true;
	MT_lock_unset(&b->theaplock);

	bat_destroy(b);
	return LOG_OK;
}

/* TODO return LOG_OK/ERR */
static void
merge_segments(storage *s, sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sqlstore* store = tr->store;
	segment *cur = s->segs->h, *seg = NULL;
	for (; cur; cur = ATOMIC_PTR_GET(&cur->next)) {
		if (cur->ts == tr->tid) {
			if (!cur->deleted)
				cur->oldts = 0;
			cur->ts = commit_ts;
		}
		if (!seg) {
			/* first segment */
			seg = cur;
		}
		else if (seg->ts < TRANSACTION_ID_BASE) {
			/* possible merge since both deleted flags are equal */
			if (seg->deleted == cur->deleted && cur->ts < TRANSACTION_ID_BASE) {
				int merge = 1;
				node *n = store->active->h;
				for (int i = 0; i < store->active->cnt; i++, n = n->next) {
					sql_trans* other = ((sql_trans*)n->data);
					ulng active = other->ts;
					if(other->active == 2)
						continue; /* pretend that another recently committed transaction is no longer active */
					if (active == tr->ts)
						continue; /* pretend that committing transaction has already committed and is no longer active */
					if (seg->ts < active && cur->ts < active)
						break;
					if (seg->ts > active && cur->ts > active)
						continue;

					assert((active > seg->ts && active < cur->ts) || (active < seg->ts && active > cur->ts));
					/* cannot safely merge since there is an active transaction between the segments */
					merge = false;
					break;
				}
				/* merge segments */
				if (merge) {
					seg->end = cur->end;
					ATOMIC_PTR_SET(&seg->next, ATOMIC_PTR_GET(&cur->next));
					if (cur == s->segs->t)
						s->segs->t = seg;
					if (commit_ts == oldest) {
						_DELETE(cur);
					} else
						mark4destroy(cur, change, commit_ts);
					cur = seg;
					continue;
				}
			}
		}
		seg = cur;
	}
}

static int
segments_in_transaction(sql_trans *tr, sql_table *t)
{
	storage *s = ATOMIC_PTR_GET(&t->data);
	segment *seg = s->segs->h;

	if (seg && s->segs->t->ts == tr->tid)
		return 1;
	for (; seg ; seg=ATOMIC_PTR_GET(&seg->next)) {
		if (seg->ts == tr->tid)
			return 1;
	}
	return 0;
}

static size_t
segs_end( segments *segs, sql_trans *tr, sql_table *table)
{
	size_t cnt = 0;

	/* because a table can grow rows over the time a transaction is running, we need to find the last valid segment, to
	 * keep all of the parts aligned */
	lock_table(tr->store, table->base.id);
	segment *s = segs->h, *l = NULL;

	if (segs->t && SEG_IS_VALID(segs->t, tr))
		l = s = segs->t;

	for(;s; s = ATOMIC_PTR_GET(&s->next)) {
		if (SEG_IS_VALID(s, tr))
				l = s;
	}
	if (l)
		cnt = l->end;
	unlock_table(tr->store, table->base.id);
	return cnt;
}

static segments *
new_segments(sql_trans *tr, size_t cnt)
{
	segments *n = (segments*)GDKmalloc(sizeof(segments));

	if (n) {
		n->nr_reused = 0;
		ATOMIC_INIT(&n->deleted, 0);
		n->h = n->t = new_segment(NULL, tr, cnt);
		if (!n->h) {
			GDKfree(n);
			return NULL;
		}
		sql_ref_init(&n->r);
	}
	return n;
}

static sql_delta *
timestamp_delta( sql_trans *tr, sql_delta *d)
{
	while (d->next && !VALID_4_READ(d->cs.ts, tr))
		d = d->next;
	return d;
}

static sql_delta *
col_timestamp_delta( sql_trans *tr, sql_column *c)
{
	return timestamp_delta( tr, ATOMIC_PTR_GET(&c->data));
}

static sql_delta *
idx_timestamp_delta( sql_trans *tr, sql_idx *i)
{
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
tab_timestamp_storage( sql_trans *tr, sql_table *t)
{
	return timestamp_storage( tr, ATOMIC_PTR_GET(&t->data));
}

static sql_delta*
delta_dup(sql_delta *d)
{
	ATOMIC_INC(&d->cs.refcnt);
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
	ATOMIC_INC(&d->cs.refcnt);
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

	for(;s; s = ATOMIC_PTR_GET(&s->next)) {
		if (!s->deleted && s->ts == tr->tid)
			cnt += s->end - s->start;
	}
	return cnt;
}

static size_t
count_deletes_in_range( segment *s, sql_trans *tr, BUN start, BUN end)
{
	size_t cnt = 0;

	for(;s && s->end <= start; s = ATOMIC_PTR_GET(&s->next))
		;

	for(;s && s->start < end; s = ATOMIC_PTR_GET(&s->next)) {
		if (SEG_IS_DELETED(s, tr)) /* assume aligned s->end and end */
			cnt += s->end - s->start;
	}
	return cnt;
}

static size_t
count_deletes( segment *s, sql_trans *tr)
{
	size_t cnt = 0;

	for(;s; s = ATOMIC_PTR_GET(&s->next)) {
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
	if (!d ||!ds)
		return 0;
	if (access == RD_UPD_ID)
		return ds?ds->cs.ucnt:0;
	if (access == RD_INS)
		return count_inserts(d->segs->h, tr);
	if (access == QUICK)
		return d->segs->t?d->segs->t->end:0;
	if (access == CNT_ACTIVE) {
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
	if (!d || !ds)
		return 0;
	if (access == RD_UPD_ID)
		return ds?ds->cs.ucnt:0;
	if (access == RD_INS)
		return count_inserts(d->segs->h, tr);
	if (access == QUICK)
		return d->segs->t?d->segs->t->end:0;
	return segs_end(d->segs, tr, i->t);
}

#define BATtdense2(b) (b->ttype == TYPE_void && b->tseqbase != oid_nil)
static BAT *
cs_bind_ubat( column_storage *cs, int access, int type, size_t cnt /* ie max position < cnt */)
{
	BAT *b;

	assert(access == RD_UPD_ID || access == RD_UPD_VAL);
	/* returns the updates for cs */
	if (cs->uibid && cs->uvbid && cs->ucnt) {
		if (access == RD_UPD_ID) {
			if (!(b = temp_descriptor(cs->uibid)))
				return NULL;
			if (!b->tsorted || ((BATtdense2(b) && (b->tseqbase + BATcount(b)) >= cnt) ||
			   (!BATtdense2(b) && BATcount(b) && ((oid*)b->theap->base)[BATcount(b)-1] >= cnt))) {
					oid nil = oid_nil;
					/* less then cnt */
					BAT *s = BATselect(b, NULL, &nil, &cnt, false, false, false, false);
					if (!s) {
						bat_destroy(b);
						return NULL;
					}

					BAT *nb = BATproject(s, b);
					bat_destroy(s);
					bat_destroy(b);
					b = nb;
			}
		} else {
			b = temp_descriptor(cs->uvbid);
		}
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
	BAT *ni = bat_new(TYPE_oid, cnt, SYSTRANS);
	BAT *nv = uv?bat_new(uv->ttype, cnt, SYSTRANS):NULL;

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

	/* handle dense (void) cases together as we need to merge updates (which is slower anyway) */
	BUN uip = 0, uie = BATcount(ui);
	BUN oip = 0, oie = BATcount(oi);

	oid uiseqb = ui->tseqbase;
	oid oiseqb = oi->tseqbase;
	oid *uipt = NULL, *oipt = NULL;
	BATiter uii = bat_iterator(ui);
	BATiter oii = bat_iterator(oi);
	if (!BATtdensebi(&uii))
		uipt = uii.base;
	if (!BATtdensebi(&oii))
		oipt = oii.base;
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
	if (uv) {
		bat_iterator_end(&uvi);
		bat_iterator_end(&ovi);
	}
	bat_iterator_end(&uii);
	bat_iterator_end(&oii);
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

	while (o && !o->cs.merged) {
		if (o->cs.ucnt && VALID_4_READ(o->cs.ts, tr))
			break;
		else
			o = o->next;
	}
	if (o && !o->cs.merged && o->cs.ucnt && VALID_4_READ(o->cs.ts, tr))
		return o;
	return NULL;
}

static BAT *
bind_ubat(sql_trans *tr, sql_delta *d, int access, int type, size_t cnt)
{
	assert(tr->active);
	sql_delta *o = NULL;
	BAT *ui = NULL, *uv = NULL;

	if (!(ui = cs_bind_ubat(&d->cs, RD_UPD_ID, type, cnt)))
		return NULL;
	if (access == RD_UPD_VAL) {
		if (!(uv = cs_bind_ubat(&d->cs, RD_UPD_VAL, type, cnt))) {
			bat_destroy(ui);
			return NULL;
		}
	}
	while ((o = older_delta(d, tr)) != NULL) {
		BAT *oui = NULL, *ouv = NULL;
		if (!oui)
			oui = cs_bind_ubat(&o->cs, RD_UPD_ID, type, cnt);
		if (access == RD_UPD_VAL)
			ouv = cs_bind_ubat(&o->cs, RD_UPD_VAL, type, cnt);
		if (!ui || !oui || (access == RD_UPD_VAL && (!uv || !ouv))) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(oui);
			bat_destroy(ouv);
			return NULL;
		}
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
cs_bind_bat( column_storage *cs, int access, size_t cnt)
{
	BAT *b;

	assert(access == RDONLY || access == QUICK || access == RD_EXT);
	assert(cs != NULL);
	if (access == QUICK)
		return quick_descriptor(cs->bid);
	if (access == RD_EXT)
		return temp_descriptor(cs->ebid);
	assert(cs->bid);
	b = temp_descriptor(cs->bid);
	if (b == NULL)
		return NULL;
	assert(b->batRestricted == BAT_READ);
	/* return slice */
	BAT *s = BATslice(b, 0, cnt);
	bat_destroy(b);
	return s;
}

static int
bind_updates(sql_trans *tr, sql_column *c, BAT **ui, BAT **uv)
{
	lock_column(tr->store, c->base.id);
	size_t cnt = count_col(tr, c, 0);
	sql_delta *d = col_timestamp_delta(tr, c);
	int type = c->type.type->localtype;

	if (!d) {
		unlock_column(tr->store, c->base.id);
		return LOG_ERR;
	}
	if (d->cs.st == ST_DICT) {
		BAT *b = quick_descriptor(d->cs.bid);

		type = b->ttype;
	}

	*ui = bind_ubat(tr, d, RD_UPD_ID, type, cnt);
	*uv = bind_ubat(tr, d, RD_UPD_VAL, type, cnt);

	unlock_column(tr->store, c->base.id);

	if (*ui == NULL || *uv == NULL) {
		bat_destroy(*ui);
		bat_destroy(*uv);
		return LOG_ERR;
	}
	return LOG_OK;
}

static int
bind_updates_idx(sql_trans *tr, sql_idx *i, BAT **ui, BAT **uv)
{
	lock_column(tr->store, i->base.id);
	size_t cnt = count_idx(tr, i, 0);
	sql_delta *d = idx_timestamp_delta(tr, i);
	int type = oid_index(i->type)?TYPE_oid:TYPE_lng;

	if (!d) {
		unlock_column(tr->store, i->base.id);
		return LOG_ERR;
	}

	*ui = bind_ubat(tr, d, RD_UPD_ID, type, cnt);
	*uv = bind_ubat(tr, d, RD_UPD_VAL, type, cnt);

	unlock_column(tr->store, i->base.id);

	if (*ui == NULL || *uv == NULL) {
		bat_destroy(*ui);
		bat_destroy(*uv);
		return LOG_ERR;
	}
	return LOG_OK;
}

static void *					/* BAT * */
bind_col(sql_trans *tr, sql_column *c, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(c->t))
		return NULL;
	sql_delta *d = col_timestamp_delta(tr, c);
	if (!d)
		return NULL;
	size_t cnt = count_col(tr, c, 0);
	assert (access != RD_UPD_ID && access != RD_UPD_VAL);
	BAT *b = cs_bind_bat( &d->cs, access, cnt);
	assert(!b || ((c->storage_type && access != RD_EXT) || b->ttype == c->type.type->localtype) || (access == QUICK && b->ttype < 0));
	return b;
}

static void *					/* BAT * */
bind_idx(sql_trans *tr, sql_idx * i, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(i->t))
		return NULL;
	sql_delta *d = idx_timestamp_delta(tr, i);
	if (!d)
		return NULL;
	size_t cnt = count_idx(tr, i, 0);
	assert (access != RD_UPD_ID && access != RD_UPD_VAL);
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
		BAT *cur = quick_descriptor(cs->bid);
		if (!cur)
			return LOG_ERR;
		int type = cur->ttype;
		cs->uvbid = e_bat(type);
		if (cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
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
		cs->uibid = temp_copy(ui->batCacheid, true, true);
		bat_destroy(ui);
		if (cs->uibid == BID_NIL ||
		    (ui = temp_descriptor(cs->uibid)) == NULL) {
			bat_destroy(uv);
			return LOG_ERR;
		}
	}
	if (isEbat(uv)){
		temp_destroy(cs->uvbid);
		cs->uvbid = temp_copy(uv->batCacheid, true, true);
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
	for(; s; s=ATOMIC_PTR_GET(&s->next)) {
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
	for(; s; s=ATOMIC_PTR_GET(&s->next)) {
		if (s->start <= rid && s->end > rid) {
			if (s->ts >= tr->ts && s->deleted) {
				return 1;
			}
			break;
		}
	}
	return 0;
}

static sql_delta *
tr_dup_delta(sql_trans *tr, sql_delta *bat)
{
	sql_delta *n = ZNEW(sql_delta);
	if (!n)
		return NULL;
	*n = *bat;
	n->next = NULL;
	n->cs.ts = tr->tid;
	return n;
}

static BAT *
dict_append_bat(sql_trans *tr, sql_delta **batp, BAT *i)
{
	BAT *newoffsets = NULL;
	sql_delta *bat = *batp;
	column_storage *cs = &bat->cs;
	BAT *u = temp_descriptor(cs->ebid), *b = NULL, *n = NULL;

	if (!u)
		return NULL;
	BUN max_cnt = (BATcount(u) < 256)?256:(BATcount(u)<65536)?65536:INT_MAX;
	if (DICTprepare4append(&newoffsets, i, u) < 0) {
		bat_destroy(u);
		return NULL;
	} else {
		int new = 0;
		/* returns new offset bat (ie to be appended), possibly with larger type ! */
		if (BATcount(u) >= max_cnt) {
			if (max_cnt == INT_MAX) { /* decompress */
				if (!(b = temp_descriptor(cs->bid))) {
					bat_destroy(u);
					return NULL;
				}
				if (cs->ucnt) {
					BAT *ui = NULL, *uv = NULL;
					BAT *nb = COLcopy(b, b->ttype, true, SYSTRANS);
					bat_destroy(b);
					if (!nb || cs_real_update_bats(cs, &ui, &uv) != LOG_OK) {
						bat_destroy(nb);
						bat_destroy(u);
						return NULL;
					}
					b = nb;
					if (BATupdate(b, ui, uv, true) != GDK_SUCCEED) {
						bat_destroy(ui);
						bat_destroy(uv);
						bat_destroy(b);
						bat_destroy(u);
					}
					bat_destroy(ui);
					bat_destroy(uv);
				}
				n = DICTdecompress_(b, u, PERSISTENT);
				bat_destroy(b);
				assert(newoffsets == NULL);
				if (!n) {
					bat_destroy(u);
					return NULL;
				}
				if (cs->ts != tr->tid) {
					if ((*batp = tr_dup_delta(tr, bat)) == NULL) {
						bat_destroy(n);
						return NULL;
					}
					cs = &(*batp)->cs;
					new = 1;
				}
				if (cs->bid && !new)
					temp_destroy(cs->bid);
				n = transfer_to_systrans(n);
				if (n == NULL)
					return NULL;
				bat_set_access(n, BAT_READ);
				cs->bid = temp_create(n);
				bat_destroy(n);
				if (cs->ebid && !new)
					temp_destroy(cs->ebid);
				cs->ebid = 0;
				cs->ucnt = 0;
				if (cs->uibid && !new)
					temp_destroy(cs->uibid);
				if (cs->uvbid && !new)
					temp_destroy(cs->uvbid);
				cs->uibid = cs->uvbid = 0;
				cs->st = ST_DEFAULT;
				cs->cleared = true;
			} else {
				if (!(b = temp_descriptor(cs->bid))) {
					bat_destroy(newoffsets);
					bat_destroy(u);
					return NULL;
				}
				n = DICTenlarge(b, BATcount(b), BATcount(b) + BATcount(i), (BATcount(u)>65536)?TYPE_int:TYPE_sht, PERSISTENT);
				bat_destroy(b);
				if (!n) {
					bat_destroy(newoffsets);
					bat_destroy(u);
					return NULL;
				}
				if (cs->ts != tr->tid) {
					if ((*batp = tr_dup_delta(tr, bat)) == NULL) {
						bat_destroy(n);
						return NULL;
					}
					cs = &(*batp)->cs;
					new = 1;
					temp_dup(cs->ebid);
					if (cs->uibid) {
						temp_dup(cs->uibid);
						temp_dup(cs->uvbid);
					}
				}
				if (cs->bid && !new)
					temp_destroy(cs->bid);
				n = transfer_to_systrans(n);
				if (n == NULL)
					return NULL;
				bat_set_access(n, BAT_READ);
				cs->bid = temp_create(n);
				bat_destroy(n);
				cs->cleared = true;
				i = newoffsets;
			}
		} else { /* append */
			i = newoffsets;
		}
	}
	bat_destroy(u);
	return i;
}

static BAT *
for_append_bat(column_storage *cs, BAT *i, char *storage_type)
{
	lng offsetval = strtoll(storage_type+4, NULL, 10);
	BAT *newoffsets = NULL;
	BAT *b = NULL, *n = NULL;

	if (!(b = temp_descriptor(cs->bid)))
		return NULL;

	if (FORprepare4append(&newoffsets, i, offsetval, b->ttype) < 0) {
		bat_destroy(b);
		return NULL;
	} else {
		/* returns new offset bat if values within min/max, else decompress */
		if (!newoffsets) { /* decompress */
			if (cs->ucnt) {
				BAT *ui = NULL, *uv = NULL;
				BAT *nb = COLcopy(b, b->ttype, true, SYSTRANS);
				bat_destroy(b);
				if (!nb || cs_real_update_bats(cs, &ui, &uv) != LOG_OK) {
					bat_destroy(nb);
					return NULL;
				}
				b = nb;
				if (BATupdate(b, ui, uv, true) != GDK_SUCCEED) {
					bat_destroy(ui);
					bat_destroy(uv);
					bat_destroy(b);
				}
				bat_destroy(ui);
				bat_destroy(uv);
			}
			n = FORdecompress_(b, offsetval, i->ttype, PERSISTENT);
			bat_destroy(b);
			if (!n)
				return NULL;
			if (cs->bid)
				temp_destroy(cs->bid);
			n = transfer_to_systrans(n);
			if (n == NULL)
				return NULL;
			bat_set_access(n, BAT_READ);
			cs->bid = temp_create(n);
			cs->ucnt = 0;
			if (cs->uibid)
				temp_destroy(cs->uibid);
			if (cs->uvbid)
				temp_destroy(cs->uvbid);
			cs->uibid = cs->uvbid = 0;
			cs->st = ST_DEFAULT;
			cs->cleared = true;
			b = n;
		} else { /* append */
			i = newoffsets;
		}
	}
	bat_destroy(b);
	return i;
}

/*
 * Returns LOG_OK, LOG_ERR or LOG_CONFLICT
 */
static int
cs_update_bat( sql_trans *tr, sql_delta **batp, sql_table *t, BAT *tids, BAT *updates, int is_new)
{
	int res = LOG_OK;
	sql_delta *bat = *batp;
	column_storage *cs = &bat->cs;
	BAT *otids = tids, *oupdates = updates;

	if (!BATcount(tids))
		return LOG_OK;

	if (tids && (tids->ttype == TYPE_msk || mask_cand(tids))) {
		tids = BATunmask(tids);
		if (!tids)
			return LOG_ERR;
	}
	if (updates && (updates->ttype == TYPE_msk || mask_cand(updates))) {
		updates = BATunmask(updates);
		if (!updates) {
			if (otids != tids)
				bat_destroy(tids);
			return LOG_ERR;
		}
	} else if (updates && updates->ttype == TYPE_void && !complex_cand(updates)) { /* dense later use optimized log structure */
		updates = COLcopy(updates, TYPE_oid, true /* make sure we get a oid col */, SYSTRANS);
		if (!updates) {
			if (otids != tids)
				bat_destroy(tids);
			return LOG_ERR;
		}
	}

	if (cs->st == ST_DICT) {
		/* possibly a new array is returned */
		BAT *nupdates = dict_append_bat(tr, batp, updates);
		bat = *batp;
		cs = &bat->cs;
		if (oupdates != updates)
			bat_destroy(updates);
		updates = nupdates;
		if (!updates) {
			if (otids != tids)
				bat_destroy(tids);
			return LOG_ERR;
		}
	}

	/* When we go to smaller grained update structures we should check for concurrent updates on this column ! */
	/* currently only one update delta is possible */
	lock_table(tr->store, t->base.id);
	storage *s = ATOMIC_PTR_GET(&t->data);
	if (!is_new && !cs->cleared) {
		if (!tids->tsorted /* make sure we have simple dense or oids */) {
			BAT *sorted, *order;
			if (BATsort(&sorted, &order, NULL, tids, NULL, NULL, false, false, false) != GDK_SUCCEED) {
				if (otids != tids)
					bat_destroy(tids);
				if (oupdates != updates)
					bat_destroy(updates);
				unlock_table(tr->store, t->base.id);
				return LOG_ERR;
			}
			if (otids != tids)
				bat_destroy(tids);
			tids = sorted;
			BAT *nupdates = BATproject(order, updates);
			bat_destroy(order);
			if (oupdates != updates)
				bat_destroy(updates);
			updates = nupdates;
			if (!updates) {
				bat_destroy(tids);
				unlock_table(tr->store, t->base.id);
				return LOG_ERR;
			}
		}
		assert(tids->tsorted);
		BAT *ui = NULL, *uv = NULL;

		/* handle updates on just inserted bits */
		/* handle updates on updates (within one transaction) */
		BATiter upi = bat_iterator(updates);
		BUN cnt = 0, ucnt = BATcount(tids);
		BAT *b, *ins = NULL;
		int *msk = NULL;

		if((b = temp_descriptor(cs->bid)) == NULL)
			res = LOG_ERR;

		if (res == LOG_OK && BATtdense(tids)) {
			oid start = tids->tseqbase, offset = start;
			oid end = start + ucnt;

			for(segment *seg = s->segs->h; seg && res == LOG_OK ; seg=ATOMIC_PTR_GET(&seg->next)) {
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
							ins = COLnew(0, TYPE_msk, ucnt, SYSTRANS);
							if (!ins)
								res = LOG_ERR;
							else {
								BATsetcount(ins, ucnt); /* all full updates  */
								msk = (int*)Tloc(ins, 0);
								BUN end = (ucnt+31)/32;
								memset(msk, 0, end * sizeof(int));
							}
						}
						for (oid i = 0, rid = start; rid < lend && res == LOG_OK; rid++, i++) {
							const void *upd = BUNtail(upi, rid-offset);
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
		} else if (res == LOG_OK && complex_cand(tids)) {
			struct canditer ci;
			segment *seg = s->segs->h;
			canditer_init(&ci, NULL, tids);
			BUN i = 0;
			while ( seg && res == LOG_OK && i < ucnt) {
				oid rid = canditer_next(&ci);
				if (seg->end <= rid)
					seg = ATOMIC_PTR_GET(&seg->next);
				else if (seg->start <= rid && seg->end > rid) {
					/* check for delete conflicts */
					if (seg->ts >= tr->ts && seg->deleted) {
						res = LOG_CONFLICT;
						continue;
					}

					/* check for inplace updates */
					if (seg->ts == tr->tid && !seg->deleted) {
						if (!ins) {
							ins = COLnew(0, TYPE_msk, ucnt, SYSTRANS);
							if (!ins) {
								res = LOG_ERR;
								break;
							} else {
								BATsetcount(ins, ucnt); /* all full updates  */
								msk = (int*)Tloc(ins, 0);
								BUN end = (ucnt+31)/32;
								memset(msk, 0, end * sizeof(int));
							}
						}
						ptr upd = BUNtail(upi, i);
						if (void_inplace(b, rid, upd, true) != GDK_SUCCEED)
							res = LOG_ERR;

						oid word = i/32;
						int pos = i%32;
						msk[word] |= 1U<<pos;
						cnt++;
					}
					i++;
				}
			}
		} else if (res == LOG_OK) {
			BUN i = 0;
			oid *rid = Tloc(tids,0);
			segment *seg = s->segs->h;
			while ( seg && res == LOG_OK && i < ucnt) {
				if (seg->end <= rid[i])
					seg = ATOMIC_PTR_GET(&seg->next);
				else if (seg->start <= rid[i] && seg->end > rid[i]) {
					/* check for delete conflicts */
					if (seg->ts >= tr->ts && seg->deleted) {
						res = LOG_CONFLICT;
						continue;
					}

					/* check for inplace updates */
					if (seg->ts == tr->tid && !seg->deleted) {
						if (!ins) {
							ins = COLnew(0, TYPE_msk, ucnt, SYSTRANS);
							if (!ins) {
								res = LOG_ERR;
								break;
							} else {
								BATsetcount(ins, ucnt); /* all full updates  */
								msk = (int*)Tloc(ins, 0);
								BUN end = (ucnt+31)/32;
								memset(msk, 0, end * sizeof(int));
							}
						}
						const void *upd = BUNtail(upi, i);
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

		if (res == LOG_OK && cnt < ucnt) { 	/* now handle real updates */
			if (cs->ucnt == 0) {
				if (cnt) {
					BAT *nins = BATmaskedcands(0, ucnt, ins, false);
					if (nins) {
						ui = BATproject(nins, tids);
						uv = BATproject(nins, updates);
						bat_destroy(nins);
					}
				} else {
					ui = temp_descriptor(tids->batCacheid);
					uv = temp_descriptor(updates->batCacheid);
				}
				if (!ui || !uv) {
					res = LOG_ERR;
				} else {
					temp_destroy(cs->uibid);
					temp_destroy(cs->uvbid);
					ui = transfer_to_systrans(ui);
					uv = transfer_to_systrans(uv);
					if (ui == NULL || uv == NULL) {
						BBPreclaim(ui);
						BBPreclaim(uv);
						res = LOG_ERR;
					} else {
						cs->uibid = temp_create(ui);
						cs->uvbid = temp_create(uv);
						cs->ucnt = BATcount(ui);
					}
				}
			} else {
				BAT *nui = NULL, *nuv = NULL;

				/* merge taking msk of inserted into account */
				if (res == LOG_OK && cs_real_update_bats(cs, &ui, &uv) != LOG_OK)
					res = LOG_ERR;

				if (res == LOG_OK) {
					const void *upd = NULL;
					nui = bat_new(TYPE_oid, cs->ucnt + ucnt - cnt, SYSTRANS);
					nuv = bat_new(uv->ttype, cs->ucnt + ucnt - cnt, SYSTRANS);

					if (!nui || !nuv) {
						res = LOG_ERR;
					} else {
						BATiter ovi = bat_iterator(uv);

						/* handle dense (void) cases together as we need to merge updates (which is slower anyway) */
						BUN uip = 0, uie = BATcount(ui);
						BUN nip = 0, nie = BATcount(tids);
						oid uiseqb = ui->tseqbase;
						oid niseqb = tids->tseqbase;
						oid *uipt = NULL, *nipt = NULL;
						BATiter uii = bat_iterator(ui);
						BATiter tidsi = bat_iterator(tids);
						if (!BATtdensebi(&uii))
							uipt = uii.base;
						if (!BATtdensebi(&tidsi))
							nipt = tidsi.base;
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
						bat_iterator_end(&uii);
						bat_iterator_end(&tidsi);
						bat_iterator_end(&ovi);
						if (res == LOG_OK) {
							temp_destroy(cs->uibid);
							temp_destroy(cs->uvbid);
							nui = transfer_to_systrans(nui);
							nuv = transfer_to_systrans(nuv);
							if (nui == NULL || nuv == NULL) {
								res = LOG_ERR;
							} else {
								cs->uibid = temp_create(nui);
								cs->uvbid = temp_create(nuv);
								cs->ucnt = BATcount(nui);
							}
						}
					}
					bat_destroy(nui);
					bat_destroy(nuv);
				}
			}
		}
		bat_iterator_end(&upi);
		bat_destroy(b);
		unlock_table(tr->store, t->base.id);
		bat_destroy(ins);
		bat_destroy(ui);
		bat_destroy(uv);
		if (otids != tids)
			bat_destroy(tids);
		if (oupdates != updates)
			bat_destroy(updates);
		return res;
	} else if (is_new || cs->cleared) {
		BAT *b = temp_descriptor(cs->bid);

		if (b == NULL) {
			res = LOG_ERR;
		} else {
			if (BATcount(b)==0) {
				if (BATappend(b, updates, NULL, true) != GDK_SUCCEED) /* alter add column */
					res = LOG_ERR;
			} else if (BATreplace(b, tids, updates, true) != GDK_SUCCEED)
				res = LOG_ERR;
			BBPcold(b->batCacheid);
			bat_destroy(b);
		}
	}
	unlock_table(tr->store, t->base.id);
	if (otids != tids)
		bat_destroy(tids);
	if (oupdates != updates)
		bat_destroy(updates);
	return res;
}

static int
delta_update_bat( sql_trans *tr, sql_delta **bat, sql_table *t, BAT *tids, BAT *updates, int is_new)
{
	return cs_update_bat(tr, bat, t, tids, updates, is_new);
}

static void *
dict_append_val(sql_trans *tr, sql_delta **batp, void *i, BUN cnt)
{
	void *newoffsets = NULL;
	sql_delta *bat = *batp;
	column_storage *cs = &bat->cs;
	BAT *u = temp_descriptor(cs->ebid), *b = NULL, *n = NULL;

	if (!u)
		return NULL;
	BUN max_cnt = (BATcount(u) < 256)?256:64*1024;
	if (DICTprepare4append_vals(&newoffsets, i, cnt, u) < 0) {
		bat_destroy(u);
		return NULL;
	} else {
		int new = 0;
		/* returns new offset bat (ie to be appended), possibly with larger type ! */
		if (BATcount(u) >= max_cnt) {
			if (max_cnt == INT_MAX) { /* decompress */
				if (!(b = temp_descriptor(cs->bid))) {
					bat_destroy(u);
					return NULL;
				}
				n = DICTdecompress_(b, u, PERSISTENT);
				/* TODO decompress updates if any */
				bat_destroy(b);
				assert(newoffsets == NULL);
				if (!n) {
					bat_destroy(u);
					return NULL;
				}
				if (cs->ts != tr->tid) {
					if ((*batp = tr_dup_delta(tr, bat)) == NULL) {
						bat_destroy(n);
						bat_destroy(u);
						return NULL;
					}
					cs = &(*batp)->cs;
					new = 1;
					cs->uibid = cs->uvbid = 0;
				}
				if (cs->bid && !new)
					temp_destroy(cs->bid);
				n = transfer_to_systrans(n);
				if (n == NULL) {
					bat_destroy(u);
					return NULL;
				}
				bat_set_access(n, BAT_READ);
				cs->bid = temp_create(n);
				bat_destroy(n);
				if (cs->ebid && !new)
					temp_destroy(cs->ebid);
				cs->ebid = 0;
				cs->st = ST_DEFAULT;
				/* at append_col the column's storage type is cleared */
				cs->cleared = true;
			} else {
				if (!(b = temp_descriptor(cs->bid))) {
					GDKfree(newoffsets);
					bat_destroy(u);
					return NULL;
				}
				n = DICTenlarge(b, BATcount(b), BATcount(b) + cnt, (BATcount(u)>65536)?TYPE_int:TYPE_sht, PERSISTENT);
				bat_destroy(b);
				if (!n) {
					GDKfree(newoffsets);
					bat_destroy(u);
					return NULL;
				}
				if (cs->ts != tr->tid) {
					if ((*batp = tr_dup_delta(tr, bat)) == NULL) {
						bat_destroy(u);
						bat_destroy(n);
						return NULL;
					}
					cs = &(*batp)->cs;
					new = 1;
					temp_dup(cs->ebid);
					if (cs->uibid) {
						temp_dup(cs->uibid);
						temp_dup(cs->uvbid);
					}
				}
				if (cs->bid)
					temp_destroy(cs->bid);
				n = transfer_to_systrans(n);
				if (n == NULL) {
					bat_destroy(u);
					return NULL;
				}
				bat_set_access(n, BAT_READ);
				cs->bid = temp_create(n);
				bat_destroy(n);
				cs->cleared = true;
				i = newoffsets;
			}
		} else { /* append */
			i = newoffsets;
		}
	}
	bat_destroy(u);
	return i;
}

static void *
for_append_val(column_storage *cs, void *i, BUN cnt, char *storage_type, int tt)
{
	lng offsetval = strtoll(storage_type+4, NULL, 10);
	void *newoffsets = NULL;
	BAT *b = NULL, *n = NULL;

	if (!(b = temp_descriptor(cs->bid)))
		return NULL;

	if (FORprepare4append_vals(&newoffsets, i, cnt, offsetval, tt, b->ttype) < 0) {
		bat_destroy(b);
		return NULL;
	} else {
		/* returns new offset bat if values within min/max, else decompress */
		if (!newoffsets) {
			n = FORdecompress_(b, offsetval, tt, PERSISTENT);
			bat_destroy(b);
			if (!n)
				return NULL;
			/* TODO decompress updates if any */
			if (cs->bid)
				temp_destroy(cs->bid);
			n = transfer_to_systrans(n);
			if (n == NULL)
				return NULL;
			bat_set_access(n, BAT_READ);
			cs->bid = temp_create(n);
			cs->st = ST_DEFAULT;
			/* at append_col the column's storage type is cleared */
			cs->cleared = true;
			b = n;
		} else { /* append */
			i = newoffsets;
		}
	}
	bat_destroy(b);
	return i;
}

static int
cs_update_val( sql_trans *tr, sql_delta **batp, sql_table *t, oid rid, void *upd, int is_new)
{
	void *oupd = upd;
	sql_delta *bat = *batp;
	column_storage *cs = &bat->cs;
	storage *s = ATOMIC_PTR_GET(&t->data);
	assert(!is_oid_nil(rid));
	int inplace = is_new || cs->cleared || segments_is_append (s->segs->h, tr, rid);

	if (cs->st == ST_DICT) {
		/* possibly a new array is returned */
		upd = dict_append_val(tr, batp, upd, 1);
		bat = *batp;
		cs = &bat->cs;
		if (!upd)
			return LOG_ERR;
	}

	/* check if rid is insert ? */
	if (!inplace) {
		/* check conflict */
		if (segments_is_deleted(s->segs->h, tr, rid)) {
			if (oupd != upd)
				GDKfree(upd);
			return LOG_CONFLICT;
		}
		BAT *ui, *uv;

		/* When we go to smaller grained update structures we should check for concurrent updates on this column ! */
		/* currently only one update delta is possible */
		if (cs_real_update_bats(cs, &ui, &uv) != LOG_OK) {
			if (oupd != upd)
				GDKfree(upd);
			return LOG_ERR;
		}

		assert(uv->ttype);
		assert(BATcount(ui) == BATcount(uv));
		if (BUNappend(ui, (ptr) &rid, true) != GDK_SUCCEED ||
		    BUNappend(uv, (ptr) upd, true) != GDK_SUCCEED) {
			if (oupd != upd)
				GDKfree(upd);
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

		if((b = temp_descriptor(cs->bid)) == NULL) {
			if (oupd != upd)
				GDKfree(upd);
			return LOG_ERR;
		}
		if (void_inplace(b, rid, upd, true) != GDK_SUCCEED) {
			if (oupd != upd)
				GDKfree(upd);
			bat_destroy(b);
			return LOG_ERR;
		}
		bat_destroy(b);
	}
	if (oupd != upd)
		GDKfree(upd);
	return LOG_OK;
}

static int
delta_update_val( sql_trans *tr, sql_delta **bat, sql_table *t, oid rid, void *upd, int is_new)
{
	int res = LOG_OK;
	lock_table(tr->store, t->base.id);
	res = cs_update_val(tr, bat, t, rid, upd, is_new);
	unlock_table(tr->store, t->base.id);
	return res;
}

static int
dup_cs(sql_trans *tr, column_storage *ocs, column_storage *cs, int type, int temp)
{
	(void)tr;
	if (!ocs)
		return LOG_OK;
	cs->bid = ocs->bid;
	cs->ebid = ocs->ebid;
	cs->uibid = ocs->uibid;
	cs->uvbid = ocs->uvbid;
	cs->ucnt = ocs->ucnt;

	if (temp) {
		cs->bid = temp_copy(cs->bid, true, false);
		if (cs->bid == BID_NIL)
			return LOG_ERR;
	} else {
		temp_dup(cs->bid);
	}
	if (cs->ebid)
		temp_dup(cs->ebid);
	cs->ucnt = 0;
	cs->uibid = e_bat(TYPE_oid);
	cs->uvbid = e_bat(type);
	if (cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
		return LOG_ERR;
	cs->st = ocs->st;
	return LOG_OK;
}

static void
destroy_delta(sql_delta *b, bool recursive)
{
	if (ATOMIC_DEC(&b->cs.refcnt) > 0)
		return;
	if (recursive && b->next)
		destroy_delta(b->next, true);
	if (b->cs.uibid)
		temp_destroy(b->cs.uibid);
	if (b->cs.uvbid)
		temp_destroy(b->cs.uvbid);
	if (b->cs.bid)
		temp_destroy(b->cs.bid);
	if (b->cs.ebid)
		temp_destroy(b->cs.ebid);
	b->cs.bid = b->cs.ebid = b->cs.uibid = b->cs.uvbid = 0;
	_DELETE(b);
}

static sql_delta *
bind_col_data(sql_trans *tr, sql_column *c, bool *update_conflict)
{
	sql_delta *obat = ATOMIC_PTR_GET(&c->data);

	if (obat->cs.ts == tr->tid || ((obat->cs.ts < TRANSACTION_ID_BASE || tr_version_of_parent(tr, obat->cs.ts)) && !update_conflict)) /* on append there are no conflicts */
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE) {
		/* abort */
		if (update_conflict)
			*update_conflict = true;
		else if (!obat->cs.cleared) /* concurrent appends are only allowed on concurrent updates */
			return timestamp_delta(tr, ATOMIC_PTR_GET(&c->data));
		return NULL;
	}
	if (!(obat = timestamp_delta(tr, ATOMIC_PTR_GET(&c->data))))
		return NULL;
	sql_delta* bat = ZNEW(sql_delta);
	if (!bat)
		return NULL;
	ATOMIC_INIT(&bat->cs.refcnt, 1);
	if (dup_cs(tr, &obat->cs, &bat->cs, c->type.type->localtype, 0) != LOG_OK) {
		destroy_delta(bat, false);
		return NULL;
	}
	bat->cs.ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (obat)
		bat->nr_updates = obat->nr_updates;
	if (!ATOMIC_PTR_CAS(&c->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_delta(bat, false);
		if (update_conflict)
			*update_conflict = true;
		return NULL;
	}
	return bat;
}

static int
update_col_execute(sql_trans *tr, sql_delta **delta, sql_table *table, bool is_new, void *incoming_tids, void *incoming_values, bool is_bat)
{
	int ok = LOG_OK;

	if (is_bat) {
		BAT *tids = incoming_tids;
		BAT *values = incoming_values;
		if (BATcount(tids) == 0)
			return LOG_OK;
		ok = delta_update_bat(tr, delta, table, tids, values, is_new);
	} else {
		ok = delta_update_val(tr, delta, table, *(oid*)incoming_tids, incoming_values, is_new);
	}
	return ok;
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, bool isbat)
{
	int res = LOG_OK;
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if (isbat) {
		BAT *t = tids;
		if (!BATcount(t))
			return LOG_OK;
	}

	if (c == NULL)
		return LOG_ERR;

	if ((delta = bind_col_data(tr, c, &update_conflict)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;

	assert(delta && delta->cs.ts == tr->tid);
	assert(c->t->persistence != SQL_DECLARED_TABLE);
	if (odelta != delta)
		trans_add_table(tr, &c->base, c->t, delta, &tc_gc_upd_col, &commit_update_col, NOT_TO_BE_LOGGED(c->t) ? NULL : &log_update_col);

	odelta = delta;
	if ((res = update_col_execute(tr, &delta, c->t, isNew(c), tids, upd, isbat)) != LOG_OK)
		return res;
	assert(delta == odelta);
	if (delta->cs.st == ST_DEFAULT && c->storage_type)
		res = sql_trans_alter_storage(tr, c, NULL);
	return res;
}

static sql_delta *
bind_idx_data(sql_trans *tr, sql_idx *i, bool *update_conflict)
{
	sql_delta *obat = ATOMIC_PTR_GET(&i->data);

	if (obat->cs.ts == tr->tid || ((obat->cs.ts < TRANSACTION_ID_BASE || tr_version_of_parent(tr, obat->cs.ts)) && !update_conflict)) /* on append there are no conflicts */
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE) {
		/* abort */
		if (update_conflict)
			*update_conflict = true;
		return NULL;
	}
	if (!(obat = timestamp_delta(tr, ATOMIC_PTR_GET(&i->data))))
		return NULL;
	sql_delta* bat = ZNEW(sql_delta);
	if (!bat)
		return NULL;
	ATOMIC_INIT(&bat->cs.refcnt, 1);
	if (dup_cs(tr, &obat->cs, &bat->cs, (oid_index(i->type))?TYPE_oid:TYPE_lng, 0) != LOG_OK) {
		destroy_delta(bat, false);
		return NULL;
	}
	bat->cs.ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&i->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_delta(bat, false);
		if (update_conflict)
			*update_conflict = true;
		return NULL;
	}
	return bat;
}

static int
update_idx(sql_trans *tr, sql_idx * i, void *tids, void *upd, bool isbat)
{
	int res = LOG_OK;
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if (isbat) {
		BAT *t = tids;
		if (!BATcount(t))
			return LOG_OK;
	}

	if (i == NULL)
		return LOG_ERR;

	if ((delta = bind_idx_data(tr, i, &update_conflict)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;

	assert(delta && delta->cs.ts == tr->tid);
	if (odelta != delta)
		trans_add_table(tr, &i->base, i->t, delta, &tc_gc_upd_idx, &commit_update_idx, NOT_TO_BE_LOGGED(i->t) ? NULL : &log_update_idx);

	odelta = delta;
	res = update_col_execute(tr, &delta, i->t, isNew(i), tids, upd, isbat);
	assert(delta == odelta);
	return res;
}

static int
delta_append_bat(sql_trans *tr, sql_delta **batp, sqlid id, BUN offset, BAT *offsets, BAT *i, char *storage_type)
{
	BAT *b, *oi = i;
	int err = 0;
	sql_delta *bat = *batp;

	assert(!offsets || BATcount(offsets) == BATcount(i));
	if (!BATcount(i))
		return LOG_OK;
	if ((i->ttype == TYPE_msk || mask_cand(i)) && !(oi = BATunmask(i)))
		return LOG_ERR;

	lock_column(tr->store, id);
	if (bat->cs.st == ST_DICT) {
		BAT *ni = dict_append_bat(tr, batp, oi);
		bat = *batp;
		if (oi != i) /* oi will be replaced, so destroy possible unmask reference */
			bat_destroy(oi);
		oi = ni;
		if (!oi) {
			unlock_column(tr->store, id);
			return LOG_ERR;
		}
	}
	if (bat->cs.st == ST_FOR) {
		BAT *ni = for_append_bat(&bat->cs, oi, storage_type);
		bat = *batp;
		if (oi != i) /* oi will be replaced, so destroy possible unmask reference */
			bat_destroy(oi);
		oi = ni;
		if (!oi) {
			unlock_column(tr->store, id);
			return LOG_ERR;
		}
	}

	b = temp_descriptor(bat->cs.bid);
	if (b == NULL) {
		unlock_column(tr->store, id);
		if (oi != i)
			bat_destroy(oi);
		return LOG_ERR;
	}
	if (!offsets && offset == b->hseqbase+BATcount(b)) {
		if (BATappend(b, oi, NULL, true) != GDK_SUCCEED)
			err = 1;
	} else if (!offsets) {
		if (BATupdatepos(b, &offset, oi, true, true) != GDK_SUCCEED)
			err = 1;
	} else if ((BATtdense(offsets) && offsets->tseqbase == (b->hseqbase+BATcount(b)))) {
		if (BATappend(b, oi, NULL, true) != GDK_SUCCEED)
			err = 1;
	} else if (BATupdate(b, offsets, oi, true) != GDK_SUCCEED) {
			err = 1;
	}
	bat_destroy(b);
	unlock_column(tr->store, id);

	if (oi != i)
		bat_destroy(oi);
	return (err)?LOG_ERR:LOG_OK;
}

// Look at the offsets and find where the replacements end and the appends begin.
static BUN
start_of_appends(BAT *offsets, BUN bcnt)
{
	BUN ocnt = BATcount(offsets);
	if (ocnt == 0)
		return 0;

	BUN highest = *(oid*)Tloc(offsets, ocnt - 1);
	if (highest < bcnt)
		// all are replacements
		return ocnt;

	// reason backward to find the first append.
	// Suppose offsets has 15 entries, bcnt == 100
	// and the highest offset in offsets is 109.
	BUN new_bcnt = highest + 1; // 110
	BUN nappends = new_bcnt - bcnt; // 10
	BUN nreplacements = ocnt - nappends; // 5

	// The first append should be to position bcnt
	assert(bcnt == *(oid*)Tloc(offsets, nreplacements));

	return nreplacements;
}


static int
delta_append_val(sql_trans *tr, sql_delta **batp, sqlid id, BUN offset, BAT *offsets, void *i, BUN cnt, char *storage_type, int tt)
{
	void *oi = i;
	BAT *b;
	lock_column(tr->store, id);
	sql_delta *bat = *batp;

	if (bat->cs.st == ST_DICT) {
		/* possibly a new array is returned */
		i = dict_append_val(tr, batp, i, cnt);
		bat = *batp;
		if (!i) {
			unlock_column(tr->store, id);
			return LOG_ERR;
		}
	}
	if (bat->cs.st == ST_FOR) {
		/* possibly a new array is returned */
		i = for_append_val(&bat->cs, i, cnt, storage_type, tt);
		bat = *batp;
		if (!i) {
			unlock_column(tr->store, id);
			return LOG_ERR;
		}
	}

	b = temp_descriptor(bat->cs.bid);
	if (b == NULL) {
		if (i != oi)
			GDKfree(i);
		unlock_column(tr->store, id);
		return LOG_ERR;
	}
	BUN bcnt = BATcount(b);

	if (offsets) {
		// The first few might be replacements while later items might be appends.
		// Handle the replacements here while leaving the appends to the code below.
		BUN nreplacements = start_of_appends(offsets, bcnt);

		oid *start = Tloc(offsets, 0);
		if (BUNreplacemulti(b, start, i, nreplacements, true) != GDK_SUCCEED) {
			bat_destroy(b);
			if (i != oi)
				GDKfree(i);
			unlock_column(tr->store, id);
			return LOG_ERR;
		}

		// Replacements have been handled. The rest are appends.
		assert(offset == oid_nil);
		offset = bcnt;
		cnt -= nreplacements;
	}

	if (bcnt > offset){
		size_t ccnt = ((offset+cnt) > bcnt)? (bcnt - offset):cnt;
		if (BUNreplacemultiincr(b, offset, i, ccnt, true) != GDK_SUCCEED) {
			bat_destroy(b);
			if (i != oi)
				GDKfree(i);
			unlock_column(tr->store, id);
			return LOG_ERR;
		}
		cnt -= ccnt;
		offset += ccnt;
	}
	if (cnt) {
		if (BATcount(b) < offset) { /* add space */
			BUN d = offset - BATcount(b);
			if (BUNappendmulti(b, NULL, d, true) != GDK_SUCCEED) {
				bat_destroy(b);
				if (i != oi)
					GDKfree(i);
				unlock_column(tr->store, id);
				return LOG_ERR;
			}
		}
		if (BUNappendmulti(b, i, cnt, true) != GDK_SUCCEED) {
			bat_destroy(b);
			if (i != oi)
				GDKfree(i);
			unlock_column(tr->store, id);
			return LOG_ERR;
		}
	}
	bat_destroy(b);
	if (i != oi)
		GDKfree(i);
	unlock_column(tr->store, id);
	return LOG_OK;
}

static int
dup_storage( sql_trans *tr, storage *obat, storage *bat)
{
	if (!(bat->segs = new_segments(tr, 0)))
		return LOG_ERR;
	return dup_cs(tr, &obat->cs, &bat->cs, TYPE_msk, 1);
}

static int
append_col_execute(sql_trans *tr, sql_delta **delta, sqlid id, BUN offset, BAT *offsets, void *incoming_data, BUN cnt, bool isbat, int tt, char *storage_type)
{
	int ok = LOG_OK;

	if ((*delta)->cs.merged)
		(*delta)->cs.merged = false; /* TODO needs to move */
	if (isbat) {
		BAT *bat = incoming_data;

		if (BATcount(bat))
			ok = delta_append_bat(tr, delta, id, offset, offsets, bat, storage_type);
	} else {
		ok = delta_append_val(tr, delta, id, offset, offsets, incoming_data, cnt, storage_type, tt);
	}
	return ok;
}

static int
append_col(sql_trans *tr, sql_column *c, BUN offset, BAT *offsets, void *data, BUN cnt, bool isbat, int tpe)
{
	int res = LOG_OK;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if (isbat) {
		BAT *t = data;
		if (!BATcount(t))
			return LOG_OK;
	}

	if ((delta = bind_col_data(tr, c, NULL)) == NULL)
		return LOG_ERR;

	assert(delta->cs.st == ST_DEFAULT || delta->cs.st == ST_DICT || delta->cs.st == ST_FOR);

	odelta = delta;
	if ((res = append_col_execute(tr, &delta, c->base.id, offset, offsets, data, cnt, isbat, tpe, c->storage_type)) != LOG_OK)
		return res;
	if (odelta != delta) {
		delta->next = odelta;
		if (!ATOMIC_PTR_CAS(&c->data, (void**)&delta->next, delta)) {
			delta->next = NULL;
			destroy_delta(delta, false);
			return LOG_CONFLICT;
		}
	}
	if (delta->cs.st == ST_DEFAULT && c->storage_type)
		res = sql_trans_alter_storage(tr, c, NULL);
	return res;
}

static int
append_idx(sql_trans *tr, sql_idx *i, BUN offset, BAT *offsets, void *data, BUN cnt, bool isbat, int tpe)
{
	int res = LOG_OK;
	sql_delta *delta;

	if (isbat) {
		BAT *t = data;
		if (!BATcount(t))
			return LOG_OK;
	}

	if ((delta = bind_idx_data(tr, i, NULL)) == NULL)
		return LOG_ERR;

	assert(delta->cs.st == ST_DEFAULT);

	res = append_col_execute(tr, &delta, i->base.id, offset, offsets, data, cnt, isbat, tpe, NULL);
	return res;
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
	lock_table(tr->store, t->base.id);

	int in_transaction = segments_in_transaction(tr, t);

	/* find segment of rid, split, mark new segment deleted (for tr->tid) */
	segment *seg = s->segs->h, *p = NULL;
	for (; seg; p = seg, seg = ATOMIC_PTR_GET(&seg->next)) {
		if (seg->start <= rid && seg->end > rid) {
			if (!SEG_VALID_4_DELETE(seg,tr)) {
				unlock_table(tr->store, t->base.id);
				return LOG_CONFLICT;
			}
			if (deletes_conflict_updates( tr, t, rid, 1)) {
				unlock_table(tr->store, t->base.id);
				return LOG_CONFLICT;
			}
			if (!split_segment(s->segs, seg, p, tr, rid, 1, true)) {
				unlock_table(tr->store, t->base.id);
				return LOG_ERR;
			}
			ATOMIC_ADD(&s->segs->deleted, 1);
			break;
		}
	}
	unlock_table(tr->store, t->base.id);
	if (!in_transaction)
		trans_add_obj(tr, &t->base, s, &tc_gc_del, &commit_update_del, NOT_TO_BE_LOGGED(t) ? NULL : &log_update_del);
	return LOG_OK;
}

static int
seg_delete_range(sql_trans *tr, sql_table *t, storage *s, segment **Seg, size_t start, size_t cnt)
{
	segment *seg = *Seg, *p = NULL;
	for (; seg; p = seg, seg = ATOMIC_PTR_GET(&seg->next)) {
		if (seg->start <= start && seg->end > start) {
			size_t lcnt = cnt;
			if (start+lcnt > seg->end)
				lcnt = seg->end-start;
			if (SEG_IS_DELETED(seg, tr)) {
				start += lcnt;
				cnt -= lcnt;
				ATOMIC_ADD(&s->segs->deleted, lcnt);
				continue;
			} else if (!SEG_VALID_4_DELETE(seg, tr))
				return LOG_CONFLICT;
			if (deletes_conflict_updates( tr, t, start, lcnt))
				return LOG_CONFLICT;
			*Seg = seg = split_segment(s->segs, seg, p, tr, start, lcnt, true);
			if (!seg)
				return LOG_ERR;
			start += lcnt;
			cnt -= lcnt;
			ATOMIC_ADD(&s->segs->deleted, lcnt);
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

	if ((i->ttype == TYPE_msk || mask_cand(i)) && !(i = BATunmask(i)))
		return LOG_ERR;
	if (BATcount(i)) {
		if (BATtdense(i)) {
			size_t start = i->tseqbase;
			size_t cnt = BATcount(i);

			lock_table(tr->store, t->base.id);
			ok = delete_range(tr, t, s, start, cnt);
			unlock_table(tr->store, t->base.id);
		} else if (complex_cand(i)) {
			struct canditer ci;
			oid f = 0, l = 0, cur = 0;

			canditer_init(&ci, NULL, i);
			cur = f = canditer_next(&ci);

			lock_table(tr->store, t->base.id);
			if (!is_oid_nil(f)) {
				segment *seg = s->segs->h;
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
			unlock_table(tr->store, t->base.id);
		} else {
			if (!i->tsorted) {
				assert(oi == i);
				BAT *ni = NULL;
				if (BATsort(&ni, NULL, NULL, i, NULL, NULL, false, false, false) != GDK_SUCCEED)
					ok = LOG_ERR;
				if (ni)
					i = ni;
			}
			assert(i->tsorted);
			BUN icnt = BATcount(i);
			BATiter ii = bat_iterator(i);
			oid *o = ii.base, n = o[0]+1;
			size_t lcnt = 1;

			lock_table(tr->store, t->base.id);
			segment *seg = s->segs->h;
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
			bat_iterator_end(&ii);
			if (lcnt && ok == LOG_OK)
				ok = seg_delete_range(tr, t, s, &seg, n-lcnt, lcnt);
			unlock_table(tr->store, t->base.id);
		}
	}
	if (i != oi)
		bat_destroy(i);
	// assert
	if (!in_transaction)
		trans_add_obj(tr, &t->base, s, &tc_gc_del, &commit_update_del, NOT_TO_BE_LOGGED(t) ? NULL : &log_update_del);
	return ok;
}

static void
destroy_segments(segments *s)
{
	if (!s || sql_ref_dec(&s->r) > 0)
		return;
	segment *seg = s->h;
	while(seg) {
		segment *n = ATOMIC_PTR_GET(&seg->next);
		_DELETE(seg);
		seg = n;
	}
	_DELETE(s);
}

static void
destroy_storage(storage *bat)
{
	if (ATOMIC_DEC(&bat->cs.refcnt) > 0)
		return;
	if (bat->next)
		destroy_storage(bat->next);
	destroy_segments(bat->segs);
	if (bat->cs.uibid)
		temp_destroy(bat->cs.uibid);
	if (bat->cs.uvbid)
		temp_destroy(bat->cs.uvbid);
	if (bat->cs.bid)
		temp_destroy(bat->cs.bid);
	bat->cs.bid = bat->cs.uibid = bat->cs.uvbid = 0;
	_DELETE(bat);
}

static int
segments_conflict(sql_trans *tr, segments *segs, int uncommitted)
{
	if (uncommitted) {
		for (segment *s = segs->h; s; s = ATOMIC_PTR_GET(&s->next))
			if (!VALID_4_READ(s->ts,tr))
				return 1;
	} else {
		for (segment *s = segs->h; s; s = ATOMIC_PTR_GET(&s->next))
			if (s->ts < TRANSACTION_ID_BASE && !VALID_4_READ(s->ts,tr))
				return 1;
	}

	return 0;
}

static int clear_storage(sql_trans *tr, sql_table *t, storage *s);

storage *
bind_del_data(sql_trans *tr, sql_table *t, bool *clear)
{
	storage *obat;

	obat = ATOMIC_PTR_GET(&t->data);

	if (obat->cs.ts != tr->tid)
		if (!tr->parent || !tr_version_of_parent(tr, obat->cs.ts))
			if (obat->cs.ts >= TRANSACTION_ID_BASE) {
				/* abort */
				if (clear)
					*clear = true;
				return NULL;
			}

	if (!clear)
		return obat;

	/* remainder is only to handle clear */
	if (segments_conflict(tr, obat->segs, 1)) {
		*clear = true;
		return NULL;
	}
	if (!(obat = timestamp_storage(tr, ATOMIC_PTR_GET(&t->data))))
		return NULL;
	storage *bat = ZNEW(storage);
	if (!bat)
		return NULL;
	ATOMIC_INIT(&bat->cs.refcnt, 1);
	if (dup_storage(tr, obat, bat) != LOG_OK) {
		destroy_storage(bat);
		return NULL;
	}
	bat->cs.cleared = true;
	bat->cs.ts = tr->tid;
	/* only one writer else abort */
	bat->next = obat;
	if (!ATOMIC_PTR_CAS(&t->data, (void**)&bat->next, bat)) {
		bat->next = NULL;
		destroy_storage(bat);
		if (clear)
			*clear = true;
		return NULL;
	}
	return bat;
}

static int
delete_tab(sql_trans *tr, sql_table * t, void *ib, bool isbat)
{
	int ok = LOG_OK;
	BAT *b = ib;
	storage *bat;

	if (isbat && !BATcount(b))
		return ok;

	if (t == NULL)
		return LOG_ERR;

	if ((bat = bind_del_data(tr, t, NULL)) == NULL)
		return LOG_ERR;

	if (isbat)
		ok = storage_delete_bat(tr, t, bat, ib);
	else
		ok = storage_delete_val(tr, t, bat, *(oid*)ib);
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

static BAT *
bind_no_view(BAT *b, bool quick)
{
	if (VIEWtparent(b)) { /* If it is a view get the parent BAT */
		BAT *nb = BBP_desc(VIEWtparent(b));
		bat_destroy(b);
		if (!(b = quick ? quick_descriptor(nb->batCacheid) : temp_descriptor(nb->batCacheid)))
			return NULL;
	}
	return b;
}

static int
set_stats_col(sql_trans *tr, sql_column *c, double *unique_est, char *min, char *max)
{
	int ok = 0;
	assert(tr->active);
	if (!c || !ATOMIC_PTR_GET(&c->data) || !isTable(c->t) || !c->t->s)
		return 0;
	lock_column(tr->store, c->base.id);
	if (unique_est) {
		sql_delta *d;
		if ((d = ATOMIC_PTR_GET(&c->data)) && d->cs.st == ST_DEFAULT) {
			BAT *b;
			if ((b = bind_col(tr, c, RDONLY)) && (b = bind_no_view(b, false))) {
				MT_lock_set(&b->theaplock);
				b->tunique_est = *unique_est;
				MT_lock_unset(&b->theaplock);
				bat_destroy(b);
			}
		}
	}
	if (min) {
		_DELETE(c->min);
		size_t minlen = ATOMlen(c->type.type->localtype, min);
		if ((c->min = GDKmalloc(minlen)) != NULL) {
			memcpy(c->min, min, minlen);
			ok = 1;
		}
	}
	if (max) {
		_DELETE(c->max);
		size_t maxlen = ATOMlen(c->type.type->localtype, max);
		if ((c->max = GDKmalloc(maxlen)) != NULL) {
			memcpy(c->max, max, maxlen);
			ok = 1;
		}
	}
	unlock_column(tr->store, c->base.id);
	return ok;
}

static int
min_max_col(sql_trans *tr, sql_column *c)
{
	int ok = 0;
	BAT *b = NULL;
	sql_delta *d = NULL;

	assert(tr->active);
	if (!c || !ATOMIC_PTR_GET(&c->data) || !isTable(c->t) || !c->t->s)
		return 0;
	if (c->min && c->max)
		return 1;
	if ((d = ATOMIC_PTR_GET(&c->data))) {
		if (d->cs.st == ST_FOR)
			return 0;
		int access = d->cs.st == ST_DICT ? RD_EXT : RDONLY;
		lock_column(tr->store, c->base.id);
		if (c->min && c->max) {
			unlock_column(tr->store, c->base.id);
			return 1;
		}
		_DELETE(c->min);
		_DELETE(c->max);
		if ((b = bind_col(tr, c, access))) {
			if (!(b = bind_no_view(b, false))) {
				unlock_column(tr->store, c->base.id);
				return 0;
			}
			BATiter bi = bat_iterator(b);
			if (bi.minpos != BUN_NONE && bi.maxpos != BUN_NONE) {
				const void *nmin = BUNtail(bi, bi.minpos), *nmax = BUNtail(bi, bi.maxpos);
				size_t minlen = ATOMlen(bi.type, nmin), maxlen = ATOMlen(bi.type, nmax);

				if (!(c->min = GDKmalloc(minlen)) || !(c->max = GDKmalloc(maxlen))) {
					_DELETE(c->min);
					_DELETE(c->max);
				} else {
					memcpy(c->min, nmin, minlen);
					memcpy(c->max, nmax, maxlen);
					ok = 1;
				}
			}
			bat_iterator_end(&bi);
			bat_destroy(b);
		}
		unlock_column(tr->store, c->base.id);
	}
	return ok;
}

static size_t
count_segs(segment *s)
{
	size_t nr = 0;

	for( ; s; s = ATOMIC_PTR_GET(&s->next))
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
	if (access == RD_UPD_ID)
		return d->cs.ucnt;
	if (access == RD_INS)
		return count_inserts(d->segs->h, tr);
	if (access == CNT_ACTIVE) /* special case for counting the number of segments */
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

	if (col && ATOMIC_PTR_GET(&col->data) && !col->storage_type /* no order on dict compressed tables */) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b)
			sorted = b->tsorted || b->trevsorted;
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
	sql_delta *d;

	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return 0;

	if (col && (d=ATOMIC_PTR_GET(&col->data))!=NULL && col->storage_type) {
		if (d->cs.st == ST_DICT) {
			BAT *b = bind_col(tr, col, QUICK);
			if (b && b->ttype == TYPE_bte)
				de = 1;
			else if (b && b->ttype == TYPE_sht)
				de = 2;
		}
	} else if (col && ATOMstorage(col->type.type->localtype) == TYPE_str && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b && ATOMstorage(b->ttype) == TYPE_str) { /* check double elimination */
			de = GDK_ELIMDOUBLES(b->tvheap);
			if (de)
				de = (int) ceil(b->tvheap->free / (double) GDK_VAROFFSET);
		}
		assert(de >= 0 && de <= 16);
	}
	return de;
}

static int
col_stats(sql_trans *tr, sql_column *c, bool *nonil, bool *unique, double *unique_est, ValPtr min, ValPtr max)
{
	int ok = 0;
	BAT *b = NULL, *off = NULL;
	sql_delta *d = NULL;

	(void) tr;
	assert(tr->active);
	*nonil = false;
	*unique = false;
	*unique_est = 0.0;
	if (!c || !isTable(c->t) || !c->t->s)
		return ok;

	if ((d = ATOMIC_PTR_GET(&c->data))) {
		if (d->cs.st == ST_FOR) {
			*nonil = true; /* TODO for min/max. I will do it later */
			return ok;
		}
		int eclass = c->type.type->eclass;
		int access = d->cs.st == ST_DICT ? RD_EXT : RDONLY;
		if ((b = bind_col(tr, c, access))) {
			if (!(b = bind_no_view(b, false)))
				return ok;
			BATiter bi = bat_iterator(b);
			*nonil = bi.nonil && !bi.nil;

			if ((EC_NUMBER(eclass) || EC_VARCHAR(eclass) || EC_TEMP_NOFRAC(eclass) || eclass == EC_DATE) &&
				d->cs.ucnt == 0 && (bi.minpos != BUN_NONE || bi.maxpos != BUN_NONE)) {
				if (c->min && VALinit(min, bi.type, c->min))
					ok |= 1;
				else if (bi.minpos != BUN_NONE && VALinit(min, bi.type, BUNtail(bi, bi.minpos)))
					ok |= 1;
				if (c->max && VALinit(max, bi.type, c->max))
					ok |= 2;
				else if (bi.maxpos != BUN_NONE && VALinit(max, bi.type, BUNtail(bi, bi.maxpos)))
					ok |= 2;
			}
			if (d->cs.ucnt == 0) {
				if (d->cs.st == ST_DEFAULT) {
					*unique = bi.key;
					*unique_est = bi.unique_est;
					if (*unique_est == 0)
						*unique_est = (double)BATguess_uniques(b,NULL);
				} else if (d->cs.st == ST_DICT && (off = bind_col(tr, c, QUICK)) && (off = bind_no_view(off, true))) {
					/* for dict, check the offsets bat for uniqueness */
					MT_lock_set(&off->theaplock);
					*unique = off->tkey;
					*unique_est = off->tunique_est;
					MT_lock_unset(&off->theaplock);
				}
			}
			bat_iterator_end(&bi);
			bat_destroy(b);
			if (*nonil && d->cs.ucnt > 0)
				*nonil = false;
		}
	}
	return ok;
}

static int
col_set_range(sql_trans *tr, sql_column *col, sql_part *pt, bool add_range)
{
	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return LOG_OK;

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b) { /* add props for ranges [min, max> */
			MT_lock_set(&b->theaplock);
			if (add_range) {
				BATsetprop_nolock(b, GDK_MIN_BOUND, b->ttype, pt->part.range.minvalue);
				if (ATOMcmp(b->ttype, pt->part.range.maxvalue, ATOMnilptr(b->ttype)) != 0)
					BATsetprop_nolock(b, GDK_MAX_BOUND, b->ttype, pt->part.range.maxvalue);
				else
					BATrmprop_nolock(b, GDK_MAX_BOUND);
				if (!pt->with_nills || !col->null)
					BATsetprop_nolock(b, GDK_NOT_NULL, b->ttype, ATOMnilptr(b->ttype));
			} else {
				BATrmprop_nolock(b, GDK_MIN_BOUND);
				BATrmprop_nolock(b, GDK_MAX_BOUND);
				BATrmprop_nolock(b, GDK_NOT_NULL);
			}
			MT_lock_unset(&b->theaplock);
		}
	}
	return LOG_OK;
}

static int
col_not_null(sql_trans *tr, sql_column *col, bool not_null)
{
	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return LOG_OK;

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, QUICK);

		if (b) { /* add props for ranges [min, max> */
			if (not_null) {
				BATsetprop(b, GDK_NOT_NULL, b->ttype, ATOMnilptr(b->ttype));
			} else {
				BATrmprop(b, GDK_NOT_NULL);
			}
		}
	}
	return LOG_OK;
}

static int
swap_bats(sql_trans *tr, sql_column *col, BAT *bn)
{
	bool update_conflict = false;

	if (segments_in_transaction(tr, col->t))
		return LOG_CONFLICT;

	sql_delta *d = NULL, *odelta = ATOMIC_PTR_GET(&col->data);

	if ((d = bind_col_data(tr, col, &update_conflict)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;
	assert(d && d->cs.ts == tr->tid);
	if (odelta != d)
		trans_add_obj(tr, &col->base, d, &tc_gc_col, &commit_update_col, NOT_TO_BE_LOGGED(col->t)?NULL:&log_update_col);
	if (d->cs.bid)
		temp_destroy(d->cs.bid);
	if (d->cs.uibid)
		temp_destroy(d->cs.uibid);
	if (d->cs.uvbid)
		temp_destroy(d->cs.uvbid);
	bat_set_access(bn, BAT_READ);
	d->cs.bid = temp_create(bn);
	d->cs.uibid = 0;
	d->cs.uvbid = 0;
	d->cs.ucnt = 0;
	d->cs.cleared = true;
	d->cs.ts = tr->tid;
	ATOMIC_INIT(&d->cs.refcnt, 1);
	return LOG_OK;
}

static int
col_subtype(sql_trans *tr, sql_column *col, sql_subtype *t)
{
	int res = LOG_ERR;
	assert(tr->active);
	if (!isTable(col->t) || !col->t->s)
		return res;

	if (col && ATOMIC_PTR_GET(&col->data)) {
		BAT *b = bind_col(tr, col, RDONLY);

		if (!b)
			return res;

		BAT *bn = BATconvert(b, NULL /* could use tids, but need NILS */, t->type->localtype, col->type.scale, t->scale, t->type->eclass == EC_NUM ? 0 : t->digits);
		if (!bn)
			return res;
		BBPreclaim(b);
		b = COLcopy(bn, bn->ttype, true, PERSISTENT);
		BBPreclaim(bn);
		if ((bn = b) == NULL)
			return res;

		res = swap_bats(tr, col, bn);
		BBPreclaim(bn);
	}
	return res;
}

static int
load_cs(sql_trans *tr, column_storage *cs, int type, sqlid id)
{
	sqlstore *store = tr->store;
	int bid = log_find_bat(store->logger, id);
	if (bid <= 0)
		return LOG_ERR;
	cs->bid = temp_dup(bid);
	cs->ucnt = 0;
	cs->uibid = e_bat(TYPE_oid);
	cs->uvbid = e_bat(type);
	if (cs->uibid == BID_NIL || cs->uvbid == BID_NIL)
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
	bat->cs.ucnt = 0;
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
	tb = quick_descriptor(i);
	if (tb == NULL)
		return 0;
	b = BATconstant(seq, type, ATOMnilptr(type), BATcount(tb), PERSISTENT);
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
		if (!bat)
			return LOG_ERR;
		ATOMIC_PTR_SET(&c->data, bat);
		ATOMIC_INIT(&bat->cs.refcnt, 1);
	}

	if (new)
		bat->cs.ts = tr->tid;

	if (!isNew(c)&& !isTempTable(c->t)){
		bat->cs.ts = tr->ts;
		ok = load_cs(tr, &bat->cs, type, c->base.id);
		if (ok == LOG_OK && c->storage_type) {
			if (strcmp(c->storage_type, "DICT") == 0) {
				sqlstore *store = tr->store;
				int bid = log_find_bat(store->logger, -c->base.id);
				if (bid <= 0)
					return LOG_ERR;
				bat->cs.ebid = temp_dup(bid);
				bat->cs.st = ST_DICT;
			} else if (strncmp(c->storage_type, "FOR", 3) == 0) {
				bat->cs.st = ST_FOR;
			}
		}
		return ok;
	} else if (bat && bat->cs.bid) {
		return new_persistent_delta(ATOMIC_PTR_GET(&c->data));
	} else {
		sql_column *fc = NULL;
		size_t cnt = 0;

		/* alter ? */
		if (!isTempTable(c->t) && ol_first_node(c->t->columns) && (fc = ol_first_node(c->t->columns)->data) != NULL) {
			storage *s = tab_timestamp_storage(tr, fc->t);
			if (s == NULL)
				return LOG_ERR;
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

		if (new && !isTempTable(c->t) && !isNew(c->t) /* alter */)
			trans_add_obj(tr, &c->base, bat, &tc_gc_col, &commit_create_col, &log_create_col);
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
commit_create_delta( sql_trans *tr, sql_table *t, sql_base *base, sql_delta *delta, ulng commit_ts, ulng oldest)
{
	(void) t; // TODO transaction_layer_revamp: remove if unnecessary
	(void)oldest;
	assert(delta->cs.ts == tr->tid);
	delta->cs.ts = commit_ts;

	assert(delta->next == NULL);
	if (!delta->cs.merged)
		delta->nr_updates += merge_delta(delta);
	if (!tr->parent)
		base->new = 0;
	return LOG_OK;
}

static int
commit_create_col( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sql_column *c = (sql_column*)change->obj;
	sql_delta *delta = ATOMIC_PTR_GET(&c->data);
	if (!tr->parent)
		c->base.new = 0;
	return commit_create_delta( tr, c->t, &c->base, delta, commit_ts, oldest);
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
		if (!bat)
			return LOG_ERR;
		ATOMIC_PTR_INIT(&ni->data, bat);
		ATOMIC_INIT(&bat->cs.refcnt, 1);
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
		sql_delta *d = col_timestamp_delta(tr, c);

		if (d) {
			/* Here we also handle indices created through alter stmts */
			/* These need to be created aligned to the existing data */
			if (d->cs.bid) {
				bat->cs.bid = copyBat(d->cs.bid, type, 0);
				if(bat->cs.bid == BID_NIL)
					ok = LOG_ERR;
			}
		} else {
			return LOG_ERR;
		}

		bat->cs.ucnt = 0;

		if (!new) {
			bat->cs.uibid = e_bat(TYPE_oid);
			if (bat->cs.uibid == BID_NIL)
				ok = LOG_ERR;
			bat->cs.uvbid = e_bat(type);
			if(bat->cs.uvbid == BID_NIL)
				ok = LOG_ERR;
		}
		bat->cs.ucnt = 0;
		if (new && !isTempTable(ni->t) && !isNew(ni->t) /* alter */)
			trans_add_obj(tr, &ni->base, bat, &tc_gc_idx, &commit_create_idx, &log_create_idx);
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
commit_create_idx( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sql_idx *i = (sql_idx*)change->obj;
	sql_delta *delta = ATOMIC_PTR_GET(&i->data);
	if (!tr->parent)
		i->base.new = 0;
	return commit_create_delta( tr, i->t, &i->base, delta, commit_ts, oldest);
	return LOG_OK;
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

	if ((b->ttype == TYPE_msk || mask_cand(b)) && !(b = BATunmask(b))) {
		bat_destroy(ib);
		return LOG_ERR;
	}

	if (BATcount(b)) {
		if (ok == LOG_OK && !(s->segs = new_segments(tr, BATcount(ib)))) {
			bat_destroy(ib);
			return LOG_ERR;
		}
		if (BATtdense(b)) {
			size_t start = b->tseqbase;
			size_t cnt = BATcount(b);
			ok = delete_range(tr, t, s, start, cnt);
		} else {
			assert(b->tsorted);
			BUN icnt = BATcount(b);
			BATiter bi = bat_iterator(b);
			size_t lcnt = 1;
			oid n;
			segment *seg = s->segs->h;
			if (complex_cand(b)) {
				oid o = * (oid *) Tpos(&bi, 0);
				n = o + 1;
				for (BUN i = 1; i < icnt; i++) {
					o = * (oid *) Tpos(&bi, i);
					if (o == n) {
						lcnt++;
						n++;
					} else {
						if ((ok = seg_delete_range(tr, t, s, &seg, n-lcnt, lcnt)) != LOG_OK)
							break;
						lcnt = 0;
					}
					if (!lcnt) {
						n = o + 1;
						lcnt = 1;
					}
				}
			} else {
				oid *o = bi.base;
				n = o[0]+1;
				for (size_t i=1; i<icnt; i++) {
					if (o[i] == n) {
						lcnt++;
						n++;
					} else {
						if ((ok = seg_delete_range(tr, t, s, &seg, n-lcnt, lcnt)) != LOG_OK)
							break;
						lcnt = 0;
					}
					if (!lcnt) {
						n = o[i]+1;
						lcnt = 1;
					}
				}
			}
			if (lcnt && ok == LOG_OK)
				ok = delete_range(tr, t, s, n-lcnt, lcnt);
			bat_iterator_end(&bi);
		}
		if (ok == LOG_OK)
			for (segment *seg = s->segs->h; seg; seg = ATOMIC_PTR_GET(&seg->next))
				if (seg->ts == tr->tid)
					seg->ts = 1;
	} else {
		if (ok == LOG_OK) {
			BAT *bb = quick_descriptor(s->cs.bid);

			if (!bb || !(s->segs = new_segments(tr, BATcount(bb)))) {
				ok = LOG_ERR;
			} else {
				segment *seg = s->segs->h;
				if (seg->ts == tr->tid)
					seg->ts = 1;
			}
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
		if(!bat)
			return LOG_ERR;
		ATOMIC_PTR_INIT(&t->data, bat);
		ATOMIC_INIT(&bat->cs.refcnt, 1);
		bat->cs.ts = tr->tid;
	}

	if (!isNew(t) && !isTempTable(t)) {
		bat->cs.ts = tr->ts;
		return load_storage(tr, t, bat, t->base.id);
	} else if (bat->cs.bid) {
		return ok;
	} else {
		assert(!bat->segs);
		if (!(bat->segs = new_segments(tr, 0)))
			return LOG_ERR;

		b = bat_new(TYPE_msk, t->sz, PERSISTENT);
		if(b != NULL) {
			bat_set_access(b, BAT_READ);
			bat->cs.bid = temp_create(b);
			bat_destroy(b);
		} else {
			return LOG_ERR;
		}
		if (new)
			trans_add_obj(tr, &t->base, bat, &tc_gc_del, &commit_create_del, isTempTable(t) ? NULL : &log_create_del);
	}
	return ok;
}

static int
log_segment(sql_trans *tr, segment *s, sqlid id, size_t total)
{
	sqlstore *store = tr->store;
	msk m = s->deleted;
	return log_constant(store->logger, TYPE_msk, &m, id, s->start, s->end-s->start, total)==GDK_SUCCEED?LOG_OK:LOG_ERR;
}

static int
log_segments(sql_trans *tr, segments *segs, sqlid id)
{
	size_t total = 0;
	/* log segments */
	lock_table(tr->store, id);
	for (segment *seg = segs->h; seg; seg=ATOMIC_PTR_GET(&seg->next)) {
		if (seg->ts == tr->tid && seg->end-seg->start &&
			(ATOMIC_PTR_GET(&seg->next) || !seg->deleted || seg->ts != seg->oldts))
			total += seg->end-seg->start;
	}
	for (segment *seg = segs->h; seg; seg=ATOMIC_PTR_GET(&seg->next)) {
		unlock_table(tr->store, id);
		if (seg->ts == tr->tid && seg->end-seg->start &&
			(ATOMIC_PTR_GET(&seg->next) || !seg->deleted || seg->ts != seg->oldts)) {
			if (log_segment(tr, seg, id, total) != LOG_OK) {
				return LOG_ERR;
			}
		}
		lock_table(tr->store, id);
	}
	unlock_table(tr->store, id);
	return LOG_OK;
}

static int
log_create_storage(sql_trans *tr, storage *bat, sql_table *t)
{
	BAT *b;
	int ok = LOG_OK;

	if (GDKinmemory(0))
		return LOG_OK;

	b = temp_descriptor(bat->cs.bid);
	if (b == NULL)
		return LOG_ERR;

	sqlstore *store = tr->store;
	bat_set_access(b, BAT_READ);
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
	storage *dbat = ATOMIC_PTR_GET(&t->data);

	if (t->commit_action == CA_DELETE || t->commit_action == CA_DROP) {
		assert(isTempTable(t));
		if ((ok = clear_storage(tr, t, dbat)) == LOG_OK)
			if (commit_ts) dbat->segs->h->ts = commit_ts;
		return ok;
	}

	if (!commit_ts) /* rollback handled by ? */
		return ok;
	ok = segments2cs(tr, dbat->segs, &dbat->cs);
	assert(ok == LOG_OK);
	if (ok != LOG_OK)
		return ok;
	merge_segments(dbat, tr, change, commit_ts, commit_ts/* create is we are alone */ /*oldest*/);
	assert(dbat->cs.ts == tr->tid);
	dbat->cs.ts = commit_ts;
	if (ok == LOG_OK) {
		for(node *n = ol_first_node(t->columns); n && ok == LOG_OK; n = n->next) {
			sql_column *c = n->data;
			sql_delta *delta = ATOMIC_PTR_GET(&c->data);

			ok = commit_create_delta(tr, c->t, &c->base, delta, commit_ts, oldest);
		}
		if (t->idxs) {
			for(node *n = ol_first_node(t->idxs); n && ok == LOG_OK; n = n->next) {
				sql_idx *i = n->data;
				sql_delta *delta = ATOMIC_PTR_GET(&i->data);

				if (delta)
					ok = commit_create_delta(tr, i->t, &i->base, delta, commit_ts, oldest);
			}
		}
		if (!tr->parent)
			t->base.new = 0;
	}
	if (!tr->parent)
		t->base.new = 0;
	return ok;
}

static int
log_destroy_delta(sql_trans *tr, sql_delta *b, sqlid id)
{
	gdk_return ok = GDK_SUCCEED;

	sqlstore *store = tr->store;
	if (!GDKinmemory(0) && b && b->cs.bid)
		ok = log_bat_transient(store->logger, id);
	if (ok == GDK_SUCCEED && !GDKinmemory(0) && b && b->cs.ebid)
		ok = log_bat_transient(store->logger, -id);
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
destroy_col(sqlstore *store, sql_column *c)
{
	(void)store;
	if (ATOMIC_PTR_GET(&c->data))
		destroy_delta(ATOMIC_PTR_GET(&c->data), true);
	ATOMIC_PTR_SET(&c->data, NULL);
	return LOG_OK;
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
	if (ATOMIC_PTR_GET(&i->data))
		destroy_delta(ATOMIC_PTR_GET(&i->data), true);
	ATOMIC_PTR_SET(&i->data, NULL);
	return LOG_OK;
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
	if (ATOMIC_PTR_GET(&t->data))
		destroy_storage(ATOMIC_PTR_GET(&t->data));
	ATOMIC_PTR_SET(&t->data, NULL);
	return LOG_OK;
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
	return ok;
}

static int
commit_destroy_del( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void)tr;
	(void)change;
	(void)commit_ts;
	(void)oldest;
	if (commit_ts)
		change->handled = true;
	return 0;
}

static int
drop_del(sql_trans *tr, sql_table *t)
{
	int ok = LOG_OK;

	if (!isNew(t)) {
		storage *bat = ATOMIC_PTR_GET(&t->data);
		trans_add_obj(tr, &t->base, bat, &tc_gc_del, &commit_destroy_del, isTempTable(t) ? NULL : &log_destroy_del);
	}
	return ok;
}

static int
drop_col(sql_trans *tr, sql_column *c)
{
	assert(!isNew(c));
	sql_delta *d = ATOMIC_PTR_GET(&c->data);
	trans_add(tr, &c->base, d, &tc_gc_col, &commit_destroy_del, isTempTable(c->t) ? NULL : &log_destroy_col);
	return LOG_OK;
}

static int
drop_idx(sql_trans *tr, sql_idx *i)
{
	assert(!isNew(i));
	sql_delta *d = ATOMIC_PTR_GET(&i->data);
	trans_add(tr, &i->base, d, &tc_gc_idx, &commit_destroy_del, isTempTable(i->t) ? NULL : &log_destroy_idx);
	return LOG_OK;
}


static BUN
clear_cs(sql_trans *tr, column_storage *cs, bool renew, bool temp)
{
	BAT *b;
	BUN sz = 0;

	(void)tr;
	assert(cs->st == ST_DEFAULT || cs->st == ST_DICT || cs->st == ST_FOR);
	if (cs->bid && renew) {
		b = quick_descriptor(cs->bid);
		if (b) {
			sz += BATcount(b);
			if (cs->st == ST_DICT) {
				bat nebid = temp_copy(cs->ebid, true, temp); /* create empty copy */
				BAT *n = COLnew(0, TYPE_bte, 0, PERSISTENT);

				if (nebid == BID_NIL || !n) {
					temp_destroy(nebid);
					bat_destroy(n);
					return BUN_NONE;
				}
				temp_destroy(cs->ebid);
				cs->ebid = nebid;
				if (!temp)
					bat_set_access(n, BAT_READ);
				temp_destroy(cs->bid);
				cs->bid = temp_create(n); /* create empty copy */
				bat_destroy(n);
			} else {
				bat nbid = temp_copy(cs->bid, true, false); /* create empty copy */

				if (nbid == BID_NIL)
					return BUN_NONE;
				temp_destroy(cs->bid);
				cs->bid = nbid;
			}
		} else {
			return BUN_NONE;
		}
	}
	if (cs->uibid) {
		temp_destroy(cs->uibid);
		cs->uibid = 0;
	}
	if (cs->uvbid) {
		temp_destroy(cs->uvbid);
		cs->uvbid = 0;
	}
	cs->cleared = true;
	cs->ucnt = 0;
	return sz;
}

static BUN
clear_col(sql_trans *tr, sql_column *c, bool renew)
{
	bool update_conflict = false;
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c, renew?&update_conflict:NULL)) == NULL)
		return update_conflict ? BUN_NONE - 1 : BUN_NONE;
	assert(c->t->persistence != SQL_DECLARED_TABLE);
	if (odelta != delta)
		trans_add_table(tr, &c->base, c->t, delta, &tc_gc_upd_col, &commit_update_col, NOT_TO_BE_LOGGED(c->t) ? NULL : &log_update_col);
	if (delta)
		return clear_cs(tr, &delta->cs, renew, isTempTable(c->t));
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
		return update_conflict ? BUN_NONE - 1 : BUN_NONE;
	assert(i->t->persistence != SQL_DECLARED_TABLE);
	if (odelta != delta)
		trans_add_table(tr, &i->base, i->t, delta, &tc_gc_upd_idx, &commit_update_idx, NOT_TO_BE_LOGGED(i->t) ? NULL : &log_update_idx);
	if (delta)
		return clear_cs(tr, &delta->cs, renew, isTempTable(i->t));
	return 0;
}

static int
clear_storage(sql_trans *tr, sql_table *t, storage *s)
{
	if (clear_cs(tr, &s->cs, true, isTempTable(t)) == BUN_NONE)
		return LOG_ERR;
	if (s->segs)
		destroy_segments(s->segs);
	if (!(s->segs = new_segments(tr, 0)))
		return LOG_ERR;
	return LOG_OK;
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
	int clear = !in_transaction, ok = LOG_OK;
	bool conflict = false;
	storage *bat;

	if ((bat = bind_del_data(tr, t, clear?&conflict:NULL)) == NULL)
		return conflict?BUN_NONE-1:BUN_NONE;

	if (!clear) {
		lock_table(tr->store, t->base.id);
		ok = delete_range(tr, t, bat, 0, bat->segs->t->end);
		unlock_table(tr->store, t->base.id);
	}
	assert(t->persistence != SQL_DECLARED_TABLE);
	if (!in_transaction)
		trans_add_obj(tr, &t->base, bat, &tc_gc_del, &commit_update_del, NOT_TO_BE_LOGGED(t) ? NULL : &log_update_del);
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
	node *n = ol_first_node(t->columns);
	sql_column *c = n->data;
	storage *d = tab_timestamp_storage(tr, t);
	int in_transaction, clear;
	BUN sz, clear_ok;

	if (!d)
		return BUN_NONE;
	lock_table(tr->store, t->base.id);
	in_transaction = segments_in_transaction(tr, t);
	unlock_table(tr->store, t->base.id);
	clear = !in_transaction;
	sz = count_col(tr, c, CNT_ACTIVE);
	if ((clear_ok = clear_del(tr, t, in_transaction)) >= BUN_NONE - 1)
		return clear_ok;

	if (in_transaction)
		return sz;

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
	if (clear) {
		d->segs->nr_reused = 0;
		ATOMIC_SET(&d->segs->deleted, 0);
	}
	return sz;
}

static int
tr_log_cs( sql_trans *tr, sql_table *t, column_storage *cs, segment *segs, sqlid id)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;

	(void) t;
	(void) segs;
	if (GDKinmemory(0))
		return LOG_OK;

	if (cs->cleared) {
		assert(cs->ucnt == 0);
		BAT *ins = temp_descriptor(cs->bid);
		if (!ins)
			return LOG_ERR;
		assert(!isEbat(ins));
		bat_set_access(ins, BAT_READ);
		ok = log_bat_persists(store->logger, ins, id);
		bat_destroy(ins);
		if (ok == GDK_SUCCEED && cs->ebid) {
			BAT *ins = temp_descriptor(cs->ebid);
			if (!ins)
				return LOG_ERR;
			assert(!isEbat(ins));
			bat_set_access(ins, BAT_READ);
			ok = log_bat_persists(store->logger, ins, -id);
			bat_destroy(ins);
		}
		return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	}

	assert(!isTempTable(t));

	if (ok == GDK_SUCCEED && cs->ucnt && cs->uibid) {
		BAT *ui = temp_descriptor(cs->uibid);
		BAT *uv = temp_descriptor(cs->uvbid);
		/* any updates */
		if (ui == NULL || uv == NULL) {
			ok = GDK_FAIL;
		} else if (BATcount(uv) > uv->batInserted || BATdirty(uv))
			ok = log_delta(store->logger, ui, uv, id);
		bat_destroy(ui);
		bat_destroy(uv);
	}
	return ok == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static inline int
tr_log_table_start(sql_trans *tr, sql_table *t) {
	sqlstore *store = tr->store;
	return log_bat_group_start(store->logger, t->base.id) == GDK_SUCCEED? LOG_OK: LOG_ERR;
}

static inline int
tr_log_table_end(sql_trans *tr, sql_table *t) {
	sqlstore *store = tr->store;
	return log_bat_group_end(store->logger, t->base.id) == GDK_SUCCEED? LOG_OK: LOG_ERR;
}

static int
log_table_append(sql_trans *tr, sql_table *t, segments *segs)
{
	sqlstore *store = tr->store;
	gdk_return ok = GDK_SUCCEED;

	size_t end = segs_end(segs, tr, t);

	if (tr_log_table_start(tr, t) != LOG_OK)
		return LOG_ERR;

	size_t nr_appends = 0;

	lock_table(tr->store, t->base.id);
	for (segment *seg = segs->h; seg; seg=ATOMIC_PTR_GET(&seg->next)) {
		if (seg->ts == tr->tid && seg->end-seg->start && !seg->deleted)
			nr_appends += (seg->end - seg->start);
	}
	for (segment *seg = segs->h; seg; seg=ATOMIC_PTR_GET(&seg->next)) {
		unlock_table(tr->store, t->base.id);

		if (seg->ts == tr->tid && seg->end-seg->start && !seg->deleted) {
			if (log_segment(tr, seg, t->base.id, nr_appends) != LOG_OK)
				return LOG_ERR;
		}
		lock_table(tr->store, t->base.id);
	}
	unlock_table(tr->store, t->base.id);

	for (node *n = ol_first_node(t->columns); n && ok == GDK_SUCCEED; n = n->next) {
		sql_column *c = n->data;
		column_storage *cs = ATOMIC_PTR_GET(&c->data);

		if (cs->cleared) {
			ok = (tr_log_cs(tr, t, cs, NULL, c->base.id) == LOG_OK)? GDK_SUCCEED : GDK_FAIL;
			continue;
		}

		lock_table(tr->store, t->base.id);
		if (!cs->cleared) {
			for (segment *cur = segs->h; cur && ok == GDK_SUCCEED; cur = ATOMIC_PTR_GET(&cur->next)) {
				unlock_table(tr->store, t->base.id);
				if (cur->ts == tr->tid && !cur->deleted && cur->start < end) {
					/* append col*/
					BAT *ins = temp_descriptor(cs->bid);
					if (ins == NULL)
						return LOG_ERR;
					assert(BATcount(ins) >= cur->end);
					ok = log_bat(store->logger, ins, c->base.id, cur->start, cur->end-cur->start, nr_appends);
					bat_destroy(ins);
				}
				lock_table(tr->store, t->base.id);
			}
		}
		unlock_table(tr->store, t->base.id);

		if (ok == GDK_SUCCEED && cs->ebid) {
			BAT *ins = temp_descriptor(cs->ebid);
			if (ins == NULL)
				return LOG_ERR;
			if (BATcount(ins) > ins->batInserted)
				ok = log_bat(store->logger, ins, -c->base.id, ins->batInserted, BATcount(ins)-ins->batInserted, 0);
			BATcommit(ins, BATcount(ins));
			bat_destroy(ins);
		}
	}

	if (t->idxs) {
		for (node *n = ol_first_node(t->idxs); n && ok == GDK_SUCCEED; n = n->next) {
			sql_idx *i = n->data;

			if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;
			column_storage *cs = ATOMIC_PTR_GET(&i->data);

			if (cs) {
				if (cs->cleared) {
					ok = (tr_log_cs(tr, t, cs, NULL, i->base.id) == LOG_OK)? GDK_SUCCEED : GDK_FAIL;
					continue;
				}

				lock_table(tr->store, t->base.id);
				for (segment *cur = segs->h; cur && ok == GDK_SUCCEED; cur = ATOMIC_PTR_GET(&cur->next)) {
					unlock_table(tr->store, t->base.id);
					if (cur->ts == tr->tid && !cur->deleted && cur->start < end) {
						/* append idx */
						BAT *ins = temp_descriptor(cs->bid);
						if (ins == NULL)
							return LOG_ERR;
						assert(BATcount(ins) >= cur->end);
						ok = log_bat(store->logger, ins, i->base.id, cur->start, cur->end-cur->start, nr_appends);
						bat_destroy(ins);
					}
					lock_table(tr->store, t->base.id);
				}
				unlock_table(tr->store, t->base.id);
			}
		}
	}

	if (ok != GDK_SUCCEED || tr_log_table_end(tr, t) != LOG_OK)
		return LOG_ERR;

	return LOG_OK;
}

static int
log_storage(sql_trans *tr, sql_table *t, storage *s)
{
	int ok = LOG_OK;
	bool cleared = s->cs.cleared;
	if (ok == LOG_OK && cleared)
		ok =  tr_log_cs(tr, t, &s->cs, s->segs->h, t->base.id);
	if (ok == LOG_OK)
		ok = log_segments(tr, s->segs, t->base.id);
	if (ok == LOG_OK && !cleared)
		ok = log_table_append(tr, t, s->segs);
	return ok;
}

static void
merge_cs( column_storage *cs, const char* caller)
{
	if (cs->bid && cs->ucnt) {
		BAT *cur = temp_descriptor(cs->bid);
		BAT *ui = temp_descriptor(cs->uibid);
		BAT *uv = temp_descriptor(cs->uvbid);

		if (!cur || !ui || !uv) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			GDKfatal(FATAL_MERGE_FAILURE, caller);
			return;
		}
		assert(BATcount(ui) == BATcount(uv));

		/* any updates */
		assert(!isEbat(cur));
		if (BATreplace(cur, ui, uv, true) != GDK_SUCCEED) {
			bat_destroy(ui);
			bat_destroy(uv);
			bat_destroy(cur);
			GDKfatal(FATAL_MERGE_FAILURE, caller);
			return;
		}
		/* cleanup the old deltas */
		temp_destroy(cs->uibid);
		temp_destroy(cs->uvbid);
		cs->uibid = e_bat(TYPE_oid);
		cs->uvbid = e_bat(cur->ttype);
		assert(cs->uibid != BID_NIL && cs->uvbid != BID_NIL); // Should be pre-allocated.
		cs->ucnt = 0;
		bat_destroy(ui);
		bat_destroy(uv);
		bat_destroy(cur);
	}
	cs->cleared = false;
	cs->merged = true;
	return;
}

static lng
merge_delta( sql_delta *obat)
{
	lng res = 0;
	if (obat && obat->next && !obat->cs.merged)
		res += merge_delta(obat->next);
	res += obat->cs.ucnt;
	merge_cs(&obat->cs, __func__);
	return res;
}

static void
merge_storage(storage *tdb)
{
	merge_cs(&tdb->cs, __func__);

	if (tdb->next) {
		destroy_storage(tdb->next);
		tdb->next = NULL;
	}
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
log_update_col( sql_trans *tr, sql_change *change)
{
	sql_column *c = (sql_column*)change->obj;
	assert(!isTempTable(c->t));

	if (isDeleted(c->t)) {
		change->handled = true;
		return LOG_OK;
	}

	if (!isDeleted(c->t) && !tr->parent) {/* don't write save point commits */
		storage *s = ATOMIC_PTR_GET(&c->t->data);
		sql_delta *d = ATOMIC_PTR_GET(&c->data);
		return tr_log_cs(tr, c->t, &d->cs, s->segs->h, c->base.id);
	}
	return LOG_OK;
}

static int
tc_gc_rollbacked( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;

	sql_delta *d = (sql_delta*)change->data;
	if (d->cs.ts < oldest) {
		destroy_delta(d, false);
		if (change->commit == &commit_update_idx)
			table_destroy(store, ((sql_idx*)change->obj)->t);
		else
			table_destroy(store, ((sql_column*)change->obj)->t);
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
		table_destroy(store, (sql_table*)change->obj);
		return 1;
	}
	if (d->cs.ts > TRANSACTION_ID_BASE)
		d->cs.ts = store_get_timestamp(store) + 1;
	return 0;
}

static int
commit_update_delta( sql_trans *tr, sql_change *change, sql_table* t, sql_base* base, ATOMIC_PTR_TYPE* data, int type, ulng commit_ts, ulng oldest)
{
	(void) type; // TODO transaction_layer_revamp remove if remains unused

	sql_delta *delta = ATOMIC_PTR_GET(data), *idelta = delta;

	if (t->commit_action == CA_DELETE || t->commit_action == CA_DROP) {
		int ok = LOG_OK;
		assert(isTempTable(t));
		if (clear_cs(tr, &delta->cs, true, isTempTable(t)) == BUN_NONE)
			ok = LOG_ERR; /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
		if (!tr->parent)
			t->base.new = base->new = 0;
		change->handled = true;
		return ok;
	}

	if (commit_ts)
		delta->cs.ts = commit_ts;
	if (!commit_ts) { /* rollback */
		sql_delta *d = change->data, *o = ATOMIC_PTR_GET(data);

		if (change->ts && t->base.new) /* handled by create col */
			return LOG_OK;
		if (o != d) {
			while(o && o->next != d)
				o = o->next;
		}
		if (o == ATOMIC_PTR_GET(data))
			ATOMIC_PTR_SET(data, d->next);
		else
			o->next = d->next;
		d->next = NULL;
		change->cleanup = &tc_gc_rollbacked;
	} else if (!tr->parent) {
		/* merge deltas */
		while (delta && delta->cs.ts > oldest)
			delta = delta->next;
		if (delta && !delta->cs.merged && delta->cs.ts <= oldest) {
			lock_column(tr->store, base->id); /* lock for concurrent updates (appends) */
			idelta->nr_updates += merge_delta(delta);
			unlock_column(tr->store, base->id);
		}
	} else if (tr->parent) /* move delta into older and cleanup current save points */
		ATOMIC_PTR_SET(data, savepoint_commit_delta(delta, commit_ts));
	return LOG_OK;
}

static int
commit_update_col( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{

	sql_column *c = (sql_column*)change->obj;
	sql_base* base = &c->base;
	sql_table* t = c->t;
	ATOMIC_PTR_TYPE* data = &c->data;
	int type = c->type.type->localtype;

	if (change->handled || isDeleted(c->t))
		return LOG_OK;

	return commit_update_delta(tr, change, t, base, data, type, commit_ts, oldest);
}

static int
log_update_idx( sql_trans *tr, sql_change *change)
{
	sql_idx *i = (sql_idx*)change->obj;
	assert(!isTempTable(i->t));

	if (isDeleted(i->t)) {
		change->handled = true;
		return LOG_OK;
	}

	if (!isDeleted(i->t) && !tr->parent) { /* don't write save point commits */
		storage *s = ATOMIC_PTR_GET(&i->t->data);
		sql_delta *d = ATOMIC_PTR_GET(&i->data);
		return tr_log_cs(tr, i->t, &d->cs, s->segs->h, i->base.id);
	}
	return LOG_OK;
}

static int
commit_update_idx( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	sql_idx *i = (sql_idx*)change->obj;
	sql_base* base = &i->base;
	sql_table* t = i->t;
	ATOMIC_PTR_TYPE* data = &i->data;
	int type = (oid_index(i->type))?TYPE_oid:TYPE_lng;

	if (change->handled || isDeleted(i->t))
		return LOG_OK;

	return commit_update_delta(tr, change, t, base, data, type, commit_ts, oldest);
}

static storage *
savepoint_commit_storage( storage *dbat, ulng commit_ts)
{
	if (dbat && dbat->cs.ts == commit_ts && dbat->next) {
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
	assert(!isTempTable(t));

	if (isDeleted(t)) {
		change->handled = true;
		return LOG_OK;
	}

	if (!isDeleted(t) && !tr->parent) /* don't write save point commits */
		return log_storage(tr, t, ATOMIC_PTR_GET(&t->data));
	return LOG_OK;
}

static int
commit_update_del( sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;
	storage *dbat = ATOMIC_PTR_GET(&t->data);

	if (change->handled || isDeleted(t))
		return ok;

	if (t->commit_action == CA_DELETE || t->commit_action == CA_DROP) {
		assert(isTempTable(t));
		if ((ok = clear_storage(tr, t, dbat)) == LOG_OK)
			if (commit_ts) dbat->segs->h->ts = commit_ts;
		change->handled = true;
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
		if (dbat->cs.ts == tr->tid) /* cleared table */
			dbat->cs.ts = commit_ts;

		ok = segments2cs(tr, dbat->segs, &dbat->cs);
		if (ok == LOG_OK) {
			merge_segments(dbat, tr, change, commit_ts, oldest);
			if (oldest == commit_ts)
				merge_storage(dbat);
		}
		if (dbat)
			dbat->cs.cleared = false;
	} else if (ok == LOG_OK && tr->parent) {/* cleanup older save points */
		merge_segments(dbat, tr, change, commit_ts, oldest);
		ATOMIC_PTR_SET(&t->data, savepoint_commit_storage(dbat, commit_ts));
		storage *s = change->data;
		if (s->cs.ts == tr->tid)
			s->cs.ts = commit_ts;
	}
	unlock_table(tr->store, t->base.id);
	return ok;
}

/* only rollback (content version) case for now */
static int
tc_gc_col( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;
	sql_column *c = (sql_column*)change->obj;

	if (!c) /* cleaned earlier */
		return 1;

	if (change->handled || isDeleted(c->t)) {
		column_destroy(store, c);
		return 1;
	}

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&c->data) != change->data) { /* data is freed by commit */
		column_destroy(store, c);
		return 1;
	}
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data, *id = d;
	if (d && d->next) {

		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		// d is oldest reachable delta
		if (d->cs.merged && d->next) { // Unreachable can immediately be destroyed.
			destroy_delta(d->next, true);
			d->next = NULL;
		}
		lock_column(store, c->base.id); /* lock for concurrent updates (appends) */
		id->nr_updates += merge_delta(d);
		unlock_column(store, c->base.id);
	}
	column_destroy(store, c);
	return 1;
}

static int
tc_gc_upd_col( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;
	sql_column *c = (sql_column*)change->obj;

	if (!c) /* cleaned earlier */
		return 1;

	if (change->handled || isDeleted(c->t)) {
		table_destroy(store, c->t);
		return 1;
	}

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&c->data) != change->data) { /* data is freed by commit */
		table_destroy(store, c->t);
		return 1;
	}
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data, *id = d;
	if (d && d->next) {

		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		// d is oldest reachable delta
		if (d->cs.merged && d->next) { // Unreachable can immediately be destroyed.
			destroy_delta(d->next, true);
			d->next = NULL;
		}
		lock_column(store, c->base.id); /* lock for concurrent updates (appends) */
		id->nr_updates += merge_delta(d);
		unlock_column(store, c->base.id);
	}
	table_destroy(store, c->t);
	return 1;
}

static int
tc_gc_idx( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;
	sql_idx *i = (sql_idx*)change->obj;

	if (!i) /* cleaned earlier */
		return 1;

	if (change->handled || isDeleted(i->t)) {
		idx_destroy(store, i);
		return 1;
	}

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&i->data) != change->data) { /* data is freed by commit */
		idx_destroy(store, i);
		return 1;
	}
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data, *id = d;
	if (d && d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		// d is oldest reachable delta
		if (d->cs.merged && d->next) { // Unreachable can immediately be destroyed.
			destroy_delta(d->next, true);
			d->next = NULL;
		}
		lock_column(store, i->base.id); /* lock for concurrent updates (appends) */
		id->nr_updates += merge_delta(d);
		unlock_column(store, i->base.id);
	}
	idx_destroy(store, i);
	return 1;
}

static int
tc_gc_upd_idx( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;
	sql_idx *i = (sql_idx*)change->obj;

	if (!i) /* cleaned earlier */
		return 1;

	if (change->handled || isDeleted(i->t)) {
		table_destroy(store, i->t);
		return 1;
	}

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&i->data) != change->data) { /* data is freed by commit */
		table_destroy(store, i->t);
		return 1;
	}
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	sql_delta *d = (sql_delta*)change->data, *id = d;
	if (d && d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		// d is oldest reachable delta
		if (d->cs.merged && d->next) { // Unreachable can immediately be destroyed.
			destroy_delta(d->next, true);
			d->next = NULL;
		}
		lock_column(store, i->base.id); /* lock for concurrent updates (appends) */
		id->nr_updates += merge_delta(d);
		unlock_column(store, i->base.id);
	}
	table_destroy(store, i->t);
	return 1;
}

static int
tc_gc_del( sql_store Store, sql_change *change, ulng oldest)
{
	sqlstore *store = Store;
	sql_table *t = (sql_table*)change->obj;

	if (change->handled || isDeleted(t)) {
		table_destroy(store, t);
		return 1;
	}
	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&t->data) != change->data) { /* data is freed by commit */
		table_destroy(store, t);
		return 1;
	}
	if (oldest && oldest >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
		return 0;
	storage *d = (storage*)change->data;
	if (d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_storage(d->next);
		d->next = NULL;
	}
	table_destroy(store, t);
	return 1;
}

static int
add_offsets(BUN slot, size_t nr, size_t total, BUN *offset, BAT **offsets)
{
	if (nr == 0)
		return LOG_OK;
	assert (nr > 0);
	if ((!offsets || !*offsets) && nr == total) {
		*offset = slot;
		return LOG_OK;
	}
	if (!*offsets) {
		*offsets = COLnew(0, TYPE_oid, total, SYSTRANS);
		if (!*offsets)
			return LOG_ERR;
	}
	oid *restrict dst = Tloc(*offsets, BATcount(*offsets));
	for(size_t i = 0; i < nr; i++)
		dst[i] = slot + i;
	(*offsets)->batCount += nr;
	(*offsets)->theap->dirty = true;
	return LOG_OK;
}

static int
claim_segmentsV2(sql_trans *tr, sql_table *t, storage *s, size_t cnt, BUN *offset, BAT **offsets, bool locked)
{
	assert(s->segs);
	ulng oldest = store_oldest(tr->store, NULL);
	BUN slot = 0;
	size_t total = cnt;

	if (!locked)
		lock_table(tr->store, t->base.id);
	int ok = LOG_OK;
	/* naive vacuum approach, iterator through segments, use deleted segments or create new segment at the end */
	if (ATOMIC_GET(&s->segs->deleted) != 0)
	for (segment *seg = s->segs->h, *p = NULL; seg && cnt && ok == LOG_OK; p = seg, seg = ATOMIC_PTR_GET(&seg->next)) {
		if (seg->deleted && seg->ts < oldest && seg->end > seg->start) { /* reuse old deleted or rolled back append */
			if ((seg->end - seg->start) >= cnt) {
				/* if previous is claimed before we could simply adjust the end/start */
				if (p && p->ts == tr->tid && !p->deleted) {
					slot = p->end;
					p->end += cnt;
					seg->start += cnt;
					if (add_offsets(slot, cnt, total, offset, offsets) != LOG_OK) {
						ok = LOG_ERR;
						break;
					}
					s->segs->nr_reused += cnt;
					ATOMIC_SUB(&s->segs->deleted, cnt);
					cnt = 0;
					break;
				}
				/* we claimed part of the old segment, the split off part needs to stay deleted */
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
			if (add_offsets(slot, (seg->end-seg->start), total, offset, offsets) != LOG_OK) {
				ok = LOG_ERR;
				break;
			}
			size_t rcnt = seg->end - seg->start;
			s->segs->nr_reused += rcnt;
			cnt -= rcnt;
			ATOMIC_SUB(&s->segs->deleted, rcnt);
		}
	}
	if (ok == LOG_OK && cnt) {
		if (s->segs->t && s->segs->t->ts == tr->tid && !s->segs->t->deleted) {
			slot = s->segs->t->end;
			s->segs->t->end += cnt;
		} else {
			if (!(s->segs->t = new_segment(s->segs->t, tr, cnt))) {
				ok = LOG_ERR;
			} else {
				if (!s->segs->h)
					s->segs->h = s->segs->t;
				slot = s->segs->t->start;
			}
		}
		if (ok == LOG_OK)
			ok = add_offsets(slot, cnt, total, offset, offsets);
	}
	if (!locked)
		unlock_table(tr->store, t->base.id);

	if (ok == LOG_OK) {
		/* hard to only add this once per transaction (probably want to change to once per new segment) */
		trans_add_obj_(tr, &t->base, s, &tc_gc_del, &commit_update_del, NOT_TO_BE_LOGGED(t) ? NULL : &log_update_del);
		if (!NOT_TO_BE_LOGGED(t))
			tr->logchanges += (lng) total;
		if (*offsets) {
			BAT *pos = *offsets;
			assert(BATcount(pos) == total);
			BATsetcount(pos, total); /* set other properties */
			pos->tnil = false;
			pos->tnonil = true;
			pos->tkey = true;
			pos->tsorted = true;
			pos->trevsorted = false;
		}
	}
	return ok;
}

static int
claim_segments(sql_trans *tr, sql_table *t, storage *s, size_t cnt, BUN *offset, BAT **offsets, bool locked)
{
	if (cnt > 1 && offsets)
		return claim_segmentsV2(tr, t, s, cnt, offset, offsets, locked);
	assert(s->segs);
	ulng oldest = store_oldest(tr->store, NULL);
	BUN slot = 0;
	int reused = 0;

	if (!locked)
		lock_table(tr->store, t->base.id);
	int ok = LOG_OK;
	/* naive vacuum approach, iterator through segments, check for large enough deleted segments
	 * or create new segment at the end */
	if (ATOMIC_GET(&s->segs->deleted) != 0)
	for (segment *seg = s->segs->h, *p = NULL; seg && ok == LOG_OK; p = seg, seg = ATOMIC_PTR_GET(&seg->next)) {
		if (seg->deleted && seg->ts < oldest && (seg->end-seg->start) >= cnt) { /* reuse old deleted or rolled back append */

			if ((seg->end - seg->start) >= cnt) {

				/* if previous is claimed before we could simply adjust the end/start */
				if (p && p->ts == tr->tid && !p->deleted) {
					slot = p->end;
					p->end += cnt;
					seg->start += cnt;
					s->segs->nr_reused += cnt;
					ATOMIC_SUB(&s->segs->deleted, cnt);
					reused = 1;
					break;
				}
				/* we claimed part of the old segment, the split off part needs to stay deleted */
				if ((seg=split_segment(s->segs, seg, p, tr, seg->start, cnt, false)) == NULL) {
					ok = LOG_ERR;
					break;
				}
			}
			seg->ts = tr->tid;
			seg->deleted = false;
			slot = seg->start;
			s->segs->nr_reused += cnt;
			ATOMIC_SUB(&s->segs->deleted, cnt);
			reused = 1;
			break;
		}
	}
	if (ok == LOG_OK && !reused) {
		if (s->segs->t && s->segs->t->ts == tr->tid && !s->segs->t->deleted) {
			slot = s->segs->t->end;
			s->segs->t->end += cnt;
		} else {
			if (!(s->segs->t = new_segment(s->segs->t, tr, cnt))) {
				ok = LOG_ERR;
			} else {
				if (!s->segs->h)
					s->segs->h = s->segs->t;
				slot = s->segs->t->start;
			}
		}
	}
	if (!locked)
		unlock_table(tr->store, t->base.id);

	if (ok == LOG_OK) {
		/* hard to only add this once per transaction (probably want to change to once per new segment) */
		trans_add_obj_(tr, &t->base, s, &tc_gc_del, &commit_update_del, NOT_TO_BE_LOGGED(t) ? NULL : &log_update_del);
		if (!NOT_TO_BE_LOGGED(t))
			tr->logchanges += (lng) cnt;
		*offset = slot;
	}
	return ok;
}

/*
 * Claim cnt slots to store the tuples. The claim_tab should claim storage on the level
 * of the global transaction and mark the newly added storage slots unused on the global
 * level but used on the local transaction level. Besides this the local transaction needs
 * to update (and mark unused) any slot in between the old end and new slots.
 * */
static int
claim_tab(sql_trans *tr, sql_table *t, size_t cnt, BUN *offset, BAT **offsets)
{
	storage *s;

	/* we have a single segment structure for each persistent table
	 * for temporary tables each has its own */
	if ((s = bind_del_data(tr, t, NULL)) == NULL)
		return LOG_ERR;

	return claim_segments(tr, t, s, cnt, offset, offsets, false); /* find slot(s) */
}

/* some tables cannot be updated concurrently (user/roles etc) */
static int
key_claim_tab(sql_trans *tr, sql_table *t, size_t cnt, BUN *offset, BAT **offsets)
{
	storage *s;
	int res = 0;

	/* we have a single segment structure for each persistent table
	 * for temporary tables each has its own */
	if ((s = bind_del_data(tr, t, NULL)) == NULL)
		/* TODO check for other inserts ! */
		return LOG_ERR;

	lock_table(tr->store, t->base.id);
	if ((res = segments_conflict(tr, s->segs, 1))) {
		unlock_table(tr->store, t->base.id);
		return LOG_CONFLICT;
	}
	res = claim_segments(tr, t, s, cnt, offset, offsets, true); /* find slot(s) */
	unlock_table(tr->store, t->base.id);
	return res;
}

static int
tab_validate(sql_trans *tr, sql_table *t, int uncommitted)
{
	storage *s;
	int res = 0;

	if ((s = bind_del_data(tr, t, NULL)) == NULL)
		return LOG_ERR;

	lock_table(tr->store, t->base.id);
	res = segments_conflict(tr, s->segs, uncommitted);
	unlock_table(tr->store, t->base.id);
	return res ? LOG_CONFLICT : LOG_OK;
}

static size_t
has_deletes_in_range( segment *s, sql_trans *tr, BUN start, BUN end)
{
	size_t cnt = 0;

	for(;s && s->end <= start; s = ATOMIC_PTR_GET(&s->next))
		;

	for(;s && s->start < end && !cnt; s = ATOMIC_PTR_GET(&s->next)) {
		if (SEG_IS_DELETED(s, tr)) /* assume aligned s->end and end */
			cnt += s->end - s->start;
	}
	return cnt;
}

static BAT *
segments2cands(storage *S, sql_trans *tr, sql_table *t, size_t start, size_t end)
{
	lock_table(tr->store, t->base.id);
	segment *s = S->segs->h;
	/* step one no deletes -> dense range */
	uint32_t cur = 0;
	size_t dnr = has_deletes_in_range(s, tr, start, end), nr = end - start, pos = 0;
	if (!dnr) {
		unlock_table(tr->store, t->base.id);
		return BATdense(start, start, end-start);
	}

	BAT *b = COLnew(0, TYPE_msk, nr, SYSTRANS), *bn = NULL;
	if (!b) {
		unlock_table(tr->store, t->base.id);
		return NULL;
	}

	uint32_t *restrict dst = Tloc(b, 0);
	for( ; s; s=ATOMIC_PTR_GET(&s->next)) {
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
				assert(end > used);
				cur |= ((1U << (end - used)) - 1) << used;
				lnr -= end - used;
				pos += end - used;
				if (end == 32) {
					*dst++ = cur;
					cur = 0;
				}
			}
			size_t full = lnr/32;
			size_t rest = lnr%32;
			if (full > 0) {
				memset(dst, ~0, full * sizeof(*dst));
				dst += full;
				lnr -= full * 32;
				pos += full * 32;
			}
			if (rest > 0) {
				cur |= (1U << rest) - 1;
				lnr -= rest;
				pos += rest;
			}
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
			memset(dst, 0, full * sizeof(*dst));
			dst += full;
			lnr -= full * 32;
			pos += full * 32;
			pos+= rest;
			lnr-= rest;
			assert(lnr==0);
		}
	}

	unlock_table(tr->store, t->base.id);
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
	return segments2cands(s, tr, t, start, end);
}

static int
vacuum_col(sql_trans *tr, sql_column *c, bool force)
{
	if (segments_in_transaction(tr, c->t))
		return LOG_CONFLICT;

	sql_delta *d = NULL;

	/* do we have enough to clean */
	if ((d = bind_col_data(tr, c, NULL)) == NULL)
		return LOG_CONFLICT;

	/* do we have enough to clean */
	if (!force && (d->nr_updates) < 1024)
		return LOG_OK;

	BAT *b = NULL, *bn = NULL;;
	if ((b = bind_col(tr, c, 0)) == NULL)
		return LOG_ERR;
	if ((bn = COLcopy(b, b->ttype, true, PERSISTENT)) == NULL) {
		BBPreclaim(b);
		return LOG_ERR;
	}
	int res = swap_bats(tr, c, bn);
	d->nr_updates = 0;
	BBPreclaim(b);
	BBPreclaim(bn);
	return res;
}

static int
vacuum_tab(sql_trans *tr, sql_table *t, bool force)
{
	if (segments_in_transaction(tr, t))
		return LOG_CONFLICT;

	storage *s;
	if ((s = bind_del_data(tr, t, NULL)) == NULL)
		return LOG_ERR;

	for( node *n = ol_first_node(t->columns); n; n = n->next) {
		sql_column *c = n->data;

		if (!ATOMvarsized(c->type.type->localtype))
			continue;
		sql_delta *d = NULL;

		/* do we have enough to clean */
		if ((d = bind_col_data(tr, c, NULL)) == NULL)
			return LOG_CONFLICT;

		/* do we have enough to clean */
		if (!force && (d->nr_updates + s->segs->nr_reused) < 1024)
			continue;

		BAT *b = NULL, *bn = NULL;;
		if ((b = bind_col(tr, c, 0)) == NULL)
			return LOG_ERR;
		if ((bn = COLcopy(b, b->ttype, true, PERSISTENT)) == NULL) {
			BBPreclaim(b);
			return LOG_ERR;
		}
		int res = swap_bats(tr, c, bn);
		d->nr_updates = 0;
		BBPreclaim(b);
		BBPreclaim(bn);
		if (res != LOG_OK)
			return res;
	}
	s->segs->nr_reused = 0;
	ATOMIC_SET(&s->segs->deleted, 0);
	return LOG_OK;
}


static int
col_compress(sql_trans *tr, sql_column *col, storage_type st, BAT *o, BAT *u)
{
	bool update_conflict = false;

	if (segments_in_transaction(tr, col->t))
		return LOG_CONFLICT;

	sql_delta *d = NULL, *odelta = ATOMIC_PTR_GET(&col->data);

	if ((d = bind_col_data(tr, col, &update_conflict)) == NULL)
		return update_conflict ? LOG_CONFLICT : LOG_ERR;
	assert(d && d->cs.ts == tr->tid);
	assert(col->t->persistence != SQL_DECLARED_TABLE);
	if (odelta != d)
		trans_add_obj(tr, &col->base, d, &tc_gc_col, &commit_update_col, NOT_TO_BE_LOGGED(col->t) ? NULL : &log_update_col);

	d->cs.st = st;
	d->cs.cleared = true;
	if (d->cs.bid)
		temp_destroy(d->cs.bid);
	o = transfer_to_systrans(o);
	if (o == NULL)
		return LOG_ERR;
	bat_set_access(o, BAT_READ);
	d->cs.bid = temp_create(o);
	if (u) {
		if (d->cs.ebid)
			temp_destroy(d->cs.ebid);
		u = transfer_to_systrans(u);
		if (u == NULL)
			return LOG_ERR;
		d->cs.ebid = temp_create(u);
	}
	return LOG_OK;
}

void
bat_storage_init( store_functions *sf)
{
	sf->bind_col = &bind_col;
	sf->bind_updates = &bind_updates;
	sf->bind_updates_idx = &bind_updates_idx;
	sf->bind_idx = &bind_idx;
	sf->bind_cands = &bind_cands;

	sf->claim_tab = &claim_tab;
	sf->key_claim_tab = &key_claim_tab;
	sf->tab_validate = &tab_validate;

	sf->append_col = &append_col;
	sf->append_idx = &append_idx;

	sf->update_col = &update_col;
	sf->update_idx = &update_idx;

	sf->delete_tab = &delete_tab;

	sf->count_del = &count_del;
	sf->count_col = &count_col;
	sf->count_idx = &count_idx;
	sf->dcount_col = &dcount_col;
	sf->min_max_col = &min_max_col;
	sf->set_stats_col = &set_stats_col;
	sf->sorted_col = &sorted_col;
	sf->unique_col = &unique_col;
	sf->double_elim_col = &double_elim_col;
	sf->col_stats = &col_stats;
	sf->col_set_range = &col_set_range;
	sf->col_not_null = &col_not_null;
	sf->col_subtype = &col_subtype;

	sf->col_dup = &col_dup;
	sf->idx_dup = &idx_dup;
	sf->del_dup = &del_dup;

	sf->create_col = &create_col;	/* create and add to change list */
	sf->create_idx = &create_idx;
	sf->create_del = &create_del;

	sf->destroy_col = &destroy_col;	/* free resources */
	sf->destroy_idx = &destroy_idx;
	sf->destroy_del = &destroy_del;

	sf->drop_col = &drop_col;		/* add drop to change list */
	sf->drop_idx = &drop_idx;
	sf->drop_del = &drop_del;

	sf->clear_table = &clear_table;

	sf->vacuum_col = &vacuum_col;
	sf->vacuum_tab = &vacuum_tab;
	sf->col_compress = &col_compress;
}
