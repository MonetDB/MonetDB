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
static int tc_gc_col( sql_store Store, sql_change *c, ulng commit_ts, ulng oldest);
static int tc_gc_idx( sql_store Store, sql_change *c, ulng commit_ts, ulng oldest);
static int tc_gc_del( sql_store Store, sql_change *c, ulng commit_ts, ulng oldest);

static int tr_merge_delta( sql_trans *tr, sql_delta *obat);

#define VALID_4_READ(TS,tr) \
	(TS == tr->tid || (tr->parent && tr_version_of_parent(tr, TS)) || TS < tr->ts)

#define SEG_IS_DELETED(seg,tr) \
	((seg->deleted && VALID_4_READ(seg->ts, tr)) || \
	 (!seg->deleted && seg->oldts && seg->ts != tr->tid && seg->ts > TRANSACTION_ID_BASE && seg->oldts < tr->ts))
#define SEG_VALID(seg,tr) \
	((!seg->deleted && VALID_4_READ(seg->ts, tr)) || (seg->deleted != d && seg->oldts < tr->ts))

#define SEG_VALID_4_WRITE(seg,tr,d) \
	(seg->deleted != d && VALID_4_READ(seg->ts, tr))

#define SEG_VALID_4_DELETE(seg,tr) SEG_VALID_4_WRITE(seg,tr,true)

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

static int
tc_gc_seg( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void)Store; (void)commit_ts;
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
		n->ts = 0;
		n->deleted = true;
	} else {
		n->oldts = o->ts;
		n->ts = tr->tid;
		n->deleted = deleted;
	}
	if (start == o->start) {
		n->start = o->start;
		n->end = n->start + cnt;
		o->start = n->end;
		n->next = o;
		if (segs->h == o)
			segs->h = n;
		if (p)
			p->next = n;
		return n;
	} else if (start+cnt == o->end) {
		n->start = o->end - cnt;
		n->end = o->end;
		o->end = n->start;
		n->next = o->next;
		o->next = n;
		if (segs->t == o)
			segs->t = n;
		return n;
	}
	/* 3 way split */
	n->start = start;
	n->end = o->end;
	o->end = n->start;
	n->next = o->next;
	o->next = n;
	if (segs->t == o)
		segs->t = n;

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
	o->end = n->start;
	n->next = o->next;
	o->next = n;
	if (segs->t == o)
		segs->t = n;
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
				mark4destroy(cur, change, oldest/* TODO somehow get current timestamp*/);
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

	for (; seg ; seg=seg->next) {
		if (seg->ts == tr->tid)
			return 1;
	}
	return 0;
}

static int
segments2cs(sql_trans *tr, segments *segs, column_storage *cs)
{
	/* set bits correctly */
	BAT *b = temp_descriptor(cs->bid);

	if (!b)
		return LOG_ERR;
	segment *s = segs->h;

	for (; s ; s=s->next) {
		if (s->ts == tr->tid) {
			msk m = s->deleted;
			if (BATcount(b) < s->start) {
				msk nil = bit_nil;
				for(BUN i=BATcount(b); i<s->start; i++){
					if (BUNappend(b, &nil, true) != GDK_SUCCEED) {
						bat_destroy(b);
						return LOG_ERR;
					}
				}
			}
			for(BUN p = s->start; p < s->end; p++) {
				if (p >= BATcount(b)) {
					if (BUNappend(b, (ptr) &m, true) != GDK_SUCCEED) {
						bat_destroy(b);
						return LOG_ERR;
					}
				} else {
					if (BUNreplace(b, p, (ptr) &m, true) != GDK_SUCCEED) {
						bat_destroy(b);
						return LOG_ERR;
					}
				}
			}
		}
	}
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
	cs->ucnt = cs->cnt = 0;
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
segs_end( segments *segs, sql_trans *tr)
{
	segment *s = segs->h, *l = NULL;

	for(;s; s = s->next) {
		if (VALID_4_READ(s->ts, tr))
				l = s;
	}
	if (!l)
		return 0;
	return l->end;
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
	return segs_end(d->segs, tr);
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
	return segs_end(d->segs, tr);
}

static BAT *
cs_bind_ubat( column_storage *cs, int access, int type)
{
	BAT *b;

	assert(access == RD_UPD_ID || access == RD_UPD_VAL);
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
bind_ucol(sql_trans *tr, sql_column *c, int access)
{
	assert(tr->active);
	sql_delta *d = col_timestamp_delta(tr, c);
	return cs_bind_ubat(&d->cs, access, c->type.type->localtype);
}

static BAT *
bind_uidx(sql_trans *tr, sql_idx * i, int access)
{
	int type = oid_index(i->type)?TYPE_oid:TYPE_lng;
	assert(tr->active);
	sql_delta *d = idx_timestamp_delta(tr, i);
	return cs_bind_ubat(&d->cs, access, type);
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

static void *					/* BAT * */
bind_del(sql_trans *tr, sql_table *t, int access)
{
	assert(access == QUICK || tr->active);
	if (!isTable(t))
		return NULL;
	storage *d = tab_timestamp_storage(tr, t);
	if (access == RD_UPD_ID || access == RD_UPD_VAL) {
		return cs_bind_ubat( &d->cs, access, TYPE_msk);
	} else {
		return cs_bind_bat( &d->cs, access, d->segs->t?d->segs->t->end:0);
	}
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
cs_update_bat( sql_trans *tr, column_storage *cs, sql_table *t, BAT *tids, BAT *updates, int is_new)
{
	storage *s = ATOMIC_PTR_GET(&t->data);
	int res = LOG_OK;
	BAT *otids = tids;
	if (!BATcount(tids))
		return LOG_OK;

	if (tids && (tids->ttype == TYPE_msk || mask_cand(tids))) {
		otids = BATunmask(tids);
		if (!otids)
			return LOG_ERR;
	}
	if (!is_new && !cs->cleared) {
		BAT *ui, *uv;

		if (cs_real_update_bats(cs, &ui, &uv) == LOG_ERR)
			return LOG_ERR;

		/* handle updates on just inserted bits */
		if (count_inserts(s->segs->h, tr)) {
			segment *seg = s->segs->h;
			BUN ucnt = BATcount(otids);
			BATiter upi = bat_iterator(updates);
			BAT *b;

			if((b = temp_descriptor(cs->bid)) == NULL) {
				bat_destroy(ui);
				bat_destroy(uv);
				if (otids != tids)
					bat_destroy(otids);
				return LOG_ERR;
			}

			if (BATtdense(otids)) {
				oid start = otids->tseqbase, offset = start;
				oid end = start + ucnt;
				for(; seg; seg=seg->next) {
					if (seg->start <= start && seg->end > start) {
						BUN lend = end < seg->end?end:seg->end;
						if (seg->ts == tr->tid && !seg->deleted) {
							for (oid rid = start; rid < lend; rid++) {
								ptr upd = BUNtail(upi, rid-offset);
								if (void_inplace(b, rid, upd, true) != GDK_SUCCEED) {
									bat_destroy(b);
									bat_destroy(ui);
									bat_destroy(uv);
									if (otids != tids)
										bat_destroy(otids);
									return LOG_ERR;
								}
							}
						} else {
							for (oid rid = start; rid < lend; rid++) {
								ptr upd = BUNtail(upi, rid-offset);
								/* handle as updates */
								if (BUNappend(ui, (ptr) &rid, true) != GDK_SUCCEED ||
									BUNappend(uv, (ptr) upd, true) != GDK_SUCCEED) {
										bat_destroy(b);
										bat_destroy(ui);
										bat_destroy(uv);
										if (otids != tids)
											bat_destroy(otids);
									return LOG_ERR;
								}
							}
							cs->ucnt+=(lend-start);
						}
						if (end < seg->end)
							break;
						start = seg->end;
					}
				}
			} else {
				oid *rid = Tloc(otids,0);
				for (BUN i = 0; i < ucnt; i++) {
					ptr upd = BUNtail(upi, i);
					int is_append = segments_is_append(s->segs->h, tr, rid[i]);

					if ((is_append && void_inplace(b, rid[i], upd, true) != GDK_SUCCEED) ||
						(!is_append &&
							(BUNappend(ui, (ptr)(rid+i), true) != GDK_SUCCEED ||
						 	 BUNappend(uv, (ptr) upd, true) != GDK_SUCCEED))) {
						bat_destroy(b);
						bat_destroy(ui);
						bat_destroy(uv);
						if (otids != tids)
							bat_destroy(otids);
						return LOG_ERR;
					}
				}
			}
			bat_destroy(b);
			bat_destroy(ui);
			bat_destroy(uv);
			if (otids != tids)
				bat_destroy(otids);
			return LOG_OK;
		}

		assert(BATcount(otids) == BATcount(updates));
		if (BATappend(ui, otids, NULL, true) != GDK_SUCCEED ||
		    BATappend(uv, updates, NULL, true) != GDK_SUCCEED) {
			if (otids != tids)
				bat_destroy(otids);
			bat_destroy(ui);
			bat_destroy(uv);
			return LOG_ERR;
		}
		assert(BATcount(otids) == BATcount(updates));
		assert(BATcount(ui) == BATcount(uv));
		bat_destroy(ui);
		bat_destroy(uv);
		cs->ucnt += BATcount(otids);
	} else if (is_new || cs->cleared) {
		BAT *b = temp_descriptor(cs->bid);

		if (b == NULL)
			res = LOG_ERR;
		else if (BATcount(b)==0 && BATappend(b, updates, NULL, true) != GDK_SUCCEED) /* alter add column */
			res = LOG_ERR;
		else if (BATreplace(b, otids, updates, true) != GDK_SUCCEED)
			res = LOG_ERR;
		bat_destroy(b);
	}
	if (otids != tids)
		bat_destroy(otids);
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
bind_col_data(sql_trans *tr, sql_column *c, bool update)
{
	sql_delta *obat = ATOMIC_PTR_GET(&c->data);

	if (isTempTable(c->t))
		obat = temp_col_timestamp_delta(tr, c);

	if (obat->cs.ts == tr->tid || !update)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE && !isTempTable(c->t))
		/* abort */
		return NULL;
	assert(!isTempTable(c->t));
	obat = timestamp_delta(tr, ATOMIC_PTR_GET(&c->data));
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	bat->cs.refcnt = 1;
	if(dup_bat(tr, c->t, obat, bat, c->type.type->localtype) == LOG_ERR)
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
	if (is_bat) {
		BAT *tids = incoming_tids;
		BAT *values = incoming_values;
		if (BATcount(tids) == 0)
			return LOG_OK;
		return delta_update_bat(tr, delta, table, tids, values, is_new);
	}
	else
		return delta_update_val(tr, delta, table, *(oid*)incoming_tids, incoming_values, is_new);
}

static int
update_col(sql_trans *tr, sql_column *c, void *tids, void *upd, int tpe)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c, true)) == NULL)
		return LOG_ERR;

	assert(delta && delta->cs.ts == tr->tid);
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);

	return update_col_execute(tr, delta, c->t, isNew(c), tids, upd, tpe == TYPE_bat);
}

static sql_delta *
bind_idx_data(sql_trans *tr, sql_idx *i, bool update)
{
	sql_delta *obat = ATOMIC_PTR_GET(&i->data);

	if (isTempTable(i->t))
		obat = temp_idx_timestamp_delta(tr, i);

	if (obat->cs.ts == tr->tid || !update)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE && !isTempTable(i->t))
		/* abort */
		return NULL;
	assert(!isTempTable(i->t));
	obat = timestamp_delta(tr, ATOMIC_PTR_GET(&i->data));
	sql_delta* bat = ZNEW(sql_delta);
	if(!bat)
		return NULL;
	bat->cs.refcnt = 1;
	if(dup_bat(tr, i->t, obat, bat, (oid_index(i->type))?TYPE_oid:TYPE_lng) == LOG_ERR)
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
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if ((delta = bind_idx_data(tr, i, true)) == NULL)
		return LOG_ERR;

	assert(delta && delta->cs.ts == tr->tid);
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);

	return update_col_execute(tr, delta, i->t, isNew(i), tids, upd, tpe == TYPE_bat);
}

static int
delta_append_bat( sql_delta *bat, size_t offset, BAT *i )
{
	BAT *b, *oi = i;

	if (!BATcount(i))
		return LOG_OK;
	b = temp_descriptor(bat->cs.bid);
	if (b == NULL)
		return LOG_ERR;
	if (i && (i->ttype == TYPE_msk || mask_cand(i))) {
		oi = BATunmask(i);
	}
	if (BATcount(b) >= offset+BATcount(oi)){
		BAT *ui = BATdense(0, offset, BATcount(oi));
		if (BATreplace(b, ui, oi, true) != GDK_SUCCEED) {
			if (oi != i)
				bat_destroy(oi);
			bat_destroy(b);
			bat_destroy(ui);
			return LOG_ERR;
		}
		assert(!isVIEW(b));
		bat_destroy(ui);
	} else {
		if (BATcount(b) < offset) { /* add space */
			const void *tv = ATOMnilptr(b->ttype);
			lng d = offset - BATcount(b);
			for(lng j=0;j<d;j++) {
				if (BUNappend(b, tv, true) != GDK_SUCCEED) {
					if (oi != i)
						bat_destroy(oi);
					bat_destroy(b);
					return LOG_ERR;
				}
			}
		}
		if (isVIEW(oi) && b->batCacheid == VIEWtparent(oi)) {
			BAT *ic = COLcopy(oi, oi->ttype, true, TRANSIENT);

			if (ic == NULL || BATappend(b, ic, NULL, true) != GDK_SUCCEED) {
				if (oi != i)
					bat_destroy(oi);
				bat_destroy(ic);
                		bat_destroy(b);
                		return LOG_ERR;
            		}
            		bat_destroy(ic);
		} else if (BATappend(b, oi, NULL, true) != GDK_SUCCEED) {
			if (oi != i)
				bat_destroy(oi);
			bat_destroy(b);
			return LOG_ERR;
		}
	}
	if (oi != i)
		bat_destroy(oi);
	assert(!isVIEW(b));
	bat_destroy(b);
	return LOG_OK;
}

static int
delta_append_val( sql_delta *bat, size_t offset, void *i, size_t cnt )
{
	BAT *b = temp_descriptor(bat->cs.bid);

	if(b == NULL)
		return LOG_ERR;

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
append_col_execute(sql_trans *tr, sql_delta *delta, sql_table *table, size_t offset, void *incoming_data, bool is_bat, size_t cnt)
{
	int ok = LOG_OK;

	lock_table(tr->store, table->base.id);
	if (is_bat) {
		BAT *bat = incoming_data;

		if (BATcount(bat))
			ok = delta_append_bat(delta, offset, bat);
	} else {
		ok = delta_append_val(delta, offset, incoming_data, cnt);
	}
	unlock_table(tr->store, table->base.id);
	return ok;
}

static int
append_col(sql_trans *tr, sql_column *c, size_t offset, void *i, int tpe, size_t cnt)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);
	int in_transaction = segments_in_transaction(tr, c->t);

	if ((delta = bind_col_data(tr, c, false)) == NULL)
		return LOG_ERR;

	assert(delta && (!isTempTable(c->t) || delta->cs.ts == tr->tid));
	if (isTempTable(c->t))
	if ((!inTransaction(tr, c->t) && (odelta != delta || !in_transaction || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);

	return append_col_execute(tr, delta, c->t, offset, i, tpe == TYPE_bat, cnt);
}

static int
append_idx(sql_trans *tr, sql_idx * i, size_t offset, void *data, int tpe, size_t cnt)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);
	int in_transaction = segments_in_transaction(tr, i->t);

	if ((delta = bind_idx_data(tr, i, false)) == NULL)
		return LOG_ERR;

	assert(delta && (!isTempTable(i->t) || delta->cs.ts == tr->tid));
	if (isTempTable(i->t))
	if ((!inTransaction(tr, i->t) && (odelta != delta || !in_transaction || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);

	return append_col_execute(tr, delta, i->t, offset, data, tpe == TYPE_bat, cnt);
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
					return LOG_ERR;
			(void)split_segment(s->segs, seg, p, tr, rid, 1, true);
			break;
		}
	}
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, s, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	return LOG_OK;
}

static int
delete_range(sql_trans *tr, storage *s, size_t start, size_t cnt)
{
	segment *seg = s->segs->h, *p = NULL;
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
				return LOG_ERR;
			seg = split_segment(s->segs, seg, p, tr, start, lcnt, true);
			start += lcnt;
			cnt -= lcnt;
		}
		if (start+cnt <= seg->end)
			break;
	}
	return LOG_OK;;
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
		if (BATtdense(i)) {
			size_t start = i->tseqbase;
			size_t cnt = BATcount(i);
			ok = delete_range(tr, s, start, cnt);
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
					if (delete_range(tr, s, f, cur-f) == LOG_ERR)
						ok = LOG_ERR;
					f = cur = l;
				}
				if (ok == LOG_OK && delete_range(tr, s, f, cur-f) == LOG_ERR)
					ok = LOG_ERR;
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
					if (delete_range(tr, s, n-lcnt, lcnt) == LOG_ERR)
						ok = LOG_ERR;
					lcnt = 0;
				}
				if (!lcnt) {
					n = o[i]+1;
					lcnt = 1;
				}
			}
			if (lcnt && ok == LOG_OK) {
				if (delete_range(tr, s, n-lcnt, lcnt) == LOG_ERR)
					ok = LOG_ERR;
			}
		}
	}
	if (i != oi)
		bat_destroy(i);
	if (ok == LOG_ERR)
		return LOG_ERR;
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

static storage *
bind_del_data(sql_trans *tr, sql_table *t)
{
	storage *obat = ATOMIC_PTR_GET(&t->data);

	if (isTempTable(t))
		obat = temp_tab_timestamp_storage(tr, t);

	if (obat->cs.ts == tr->tid)
		return obat;
	if ((!tr->parent || !tr_version_of_parent(tr, obat->cs.ts)) && obat->cs.ts >= TRANSACTION_ID_BASE && !isTempTable(t))
		/* abort */
		return NULL;
	if (!isTempTable(t))
		return obat;
	assert(!isTempTable(t));
	obat = timestamp_storage(tr, ATOMIC_PTR_GET(&t->data));
	storage *bat = ZNEW(storage);
	if(!bat)
		return NULL;
	bat->cs.refcnt = 1;
	dup_storage(tr, obat, bat, isTempTable(t));
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

	if ((bat = bind_del_data(tr, t)) == NULL)
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
	d->cs.cnt = BATcount(b);
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
			cnt = segs_end(s->segs, tr);
		}
		if (cnt && fc != c) {
			sql_delta *d = ATOMIC_PTR_GET(&fc->data);

			if (d->cs.bid) {
				bat->cs.bid = copyBat(d->cs.bid, type, 0);
				if(bat->cs.bid == BID_NIL)
					ok = LOG_ERR;
			}
			bat->cs.cnt = d->cs.cnt;
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
	(void)oldest;

	if(!isTempTable(c->t)) {
		sql_delta *delta = ATOMIC_PTR_GET(&c->data);
		assert(delta->cs.ts == tr->tid);
		delta->cs.ts = commit_ts;

		assert(delta->next == NULL);
		if (!delta->cs.alter)
			ok = tr_merge_delta(tr, delta);
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
		bat->cs.cnt = d->cs.cnt;
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
	(void)oldest;

	if(!isTempTable(i->t)) {
		sql_delta *delta = ATOMIC_PTR_GET(&i->data);
		assert(delta->cs.ts == tr->tid);
		delta->cs.ts = commit_ts;

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
load_storage(sql_trans *tr, storage *s, sqlid id)
{
	int ok = load_cs(tr, &s->cs, TYPE_msk, id);
	BAT *b = temp_descriptor(s->cs.bid), *ib = b;

	if (!b)
		return LOG_ERR;

	if (b->ttype == TYPE_msk || mask_cand(b))
		b = BATunmask(b);

	if (BATcount(b)) {
		if (ok == LOG_OK)
			s->segs = new_segments(tr, BATcount(ib));
		if (BATtdense(b)) {
			size_t start = b->tseqbase;
			size_t cnt = BATcount(b);
			if (delete_range(tr, s, start, cnt) == LOG_ERR)
				return LOG_ERR;
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
					if (delete_range(tr, s, n-lcnt, lcnt) == LOG_ERR)
						return LOG_ERR;
					lcnt = 0;
				}
				if (!lcnt) {
					n = o[i]+1;
					lcnt = 1;
				}
			}
			if (lcnt) {
				if (delete_range(tr, s, n-lcnt, lcnt) == LOG_ERR)
					return LOG_ERR;
			}
		}
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
		return load_storage(tr, bat, t->base.id);
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
log_create_storage(sql_trans *tr, storage *bat, sqlid id)
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
	ok = segments2cs(tr, bat->segs, &bat->cs);
	if (ok == LOG_OK)
		ok = (log_bat_persists(store->logger, b, id) == GDK_SUCCEED)?LOG_OK:LOG_ERR;
	if (ok == LOG_OK)
		ok = log_segments(tr, bat->segs, id);
	bat_destroy(b);
	return ok;
}

static int
log_create_del(sql_trans *tr, sql_change *change)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;

	assert(!isTempTable(t));
	ok = log_create_storage(tr, ATOMIC_PTR_GET(&t->data), t->base.id);
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
		merge_segments(dbat->segs, tr, change, commit_ts, oldest);
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
log_destroy_col(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void) commit_ts;
	(void) oldest;
	return log_destroy_col_(tr, (sql_column*)change->obj);
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
log_destroy_idx(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void) commit_ts;
	(void) oldest;
	return log_destroy_idx_(tr, (sql_idx*)change->obj);
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
log_destroy_del(sql_trans *tr, sql_change *change, ulng commit_ts, ulng oldest)
{
	int ok = LOG_OK;
	sql_table *t = (sql_table*)change->obj;
	assert(!isTempTable(t));
	storage *dbat = ATOMIC_PTR_GET(&t->data);
	(void) commit_ts;
	(void) oldest;
	if (dbat->cs.ts < tr->ts) /* no changes ? */
		return ok;
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

static BUN
clear_cs(sql_trans *tr, column_storage *cs)
{
	BAT *b;
	BUN sz = 0;

	(void)tr;
	if (cs->bid) {
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
clear_col(sql_trans *tr, sql_column *c)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&c->data);

	if ((delta = bind_col_data(tr, c, false)) == NULL)
		return BUN_NONE;
	if ((!inTransaction(tr, c->t) && (odelta != delta || isTempTable(c->t)) && isGlobal(c->t)) || (!isNew(c->t) && isLocalTemp(c->t)))
		trans_add(tr, &c->base, delta, &tc_gc_col, &commit_update_col, isLocalTemp(c->t)?NULL:&log_update_col);
	if (delta)
		return clear_cs(tr, &delta->cs);
	return 0;
}

static BUN
clear_idx(sql_trans *tr, sql_idx *i)
{
	sql_delta *delta, *odelta = ATOMIC_PTR_GET(&i->data);

	if (!isTable(i->t) || (hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
		return 0;
	if ((delta = bind_idx_data(tr, i, false)) == NULL)
		return BUN_NONE;
	if ((!inTransaction(tr, i->t) && (odelta != delta || isTempTable(i->t)) && isGlobal(i->t)) || (!isNew(i->t) && isLocalTemp(i->t)))
		trans_add(tr, &i->base, delta, &tc_gc_idx, &commit_update_idx, isLocalTemp(i->t)?NULL:&log_update_idx);
	if (delta)
		return clear_cs(tr, &delta->cs);
	return 0;
}

static BUN
clear_storage(sql_trans *tr, storage *s)
{
	BUN sz = count_deletes(s->segs->h, tr);

	clear_cs(tr, &s->cs);
	s->cs.cleared = 1;
	s->cs.cnt = 0;
	if (s->segs)
		destroy_segments(s->segs);
	s->segs = new_segments(tr, 0);
	return sz;
}

static BUN
clear_del(sql_trans *tr, sql_table *t)
{
	int in_transaction = segments_in_transaction(tr, t);
	storage *bat;

	if ((bat = bind_del_data(tr, t)) == NULL)
		return BUN_NONE;
	if (!isTempTable(t)) {
		if (delete_range(tr, bat, 0, bat->segs->t->end) == LOG_ERR)
			return LOG_ERR;
	}
	if ((!inTransaction(tr, t) && !in_transaction && isGlobal(t)) || (!isNew(t) && isLocalTemp(t)))
		trans_add(tr, &t->base, bat, &tc_gc_del, &commit_update_del, isLocalTemp(t)?NULL:&log_update_del);
	if (isTempTable(t))
		return clear_storage(tr, bat);
	return LOG_OK;
}

static BUN
clear_table(sql_trans *tr, sql_table *t)
{

	node *n = ol_first_node(t->columns);
	sql_column *c = n->data;
	BUN sz = count_col(tr, c, 0);

	sz -= count_del(tr, t, 0);
	if ((clear_del(tr, t)) == BUN_NONE)
		return BUN_NONE;

	if (isTempTable(t)) { /* temp tables switch too new bats */
		for (; n; n = n->next) {
			c = n->data;

			if (clear_col(tr, c) == BUN_NONE)
				return BUN_NONE;
		}
		if (t->idxs) {
			for (n = ol_first_node(t->idxs); n; n = n->next) {
				sql_idx *ci = n->data;

				if (isTable(ci->t) && idx_has_column(ci->type) &&
					clear_idx(tr, ci) == BUN_NONE)
					return BUN_NONE;
			}
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

	if (cs->cleared && log_bat_clear(store->logger, id) != GDK_SUCCEED)
		return LOG_ERR;

	if (cs->cleared) {
		assert(cs->ucnt == 0);
		BAT *ins = temp_descriptor(cs->bid);
		if (isEbat(ins)) {
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

	if (isTempTable(t))
	for (; segs; segs=segs->next) {
		if (segs->ts == tr->tid) {
			BAT *ins = temp_descriptor(cs->bid);
			assert(ins);
			assert(BATcount(ins) >= segs->end);
			ok = log_bat(store->logger, ins, id, segs->start, segs->end-segs->start);
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
	size_t end = segs_end(segs, tr);
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
	int ok = segments2cs(tr, s->segs, &s->cs);
	if (ok == LOG_OK)
		ok = log_table_append(tr, t, s->segs);
	if (ok == LOG_OK)
		return log_segments(tr, s->segs, id);
	return ok;
}

static int
tr_merge_cs( sql_trans *tr, column_storage *cs)
{
	int ok = LOG_OK;
	BAT *cur = NULL;

	(void)tr;
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
	return tr_merge_cs(tr, &obat->cs);
}

static int
tr_merge_storage(sql_trans *tr, storage *tdb)
{
	int ok = tr_merge_cs(tr, &tdb->cs);

	if (tdb->next) {
		assert(0);
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
commit_delta(sql_trans *tr, sql_delta *delta)
{
	return tr_merge_delta(tr, delta);
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
				commit_delta(tr, delta);
			else /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
				clear_cs(tr, &delta->cs);
		} else { /* rollback */
			if (c->t->commit_action == CA_COMMIT/* || c->t->commit_action == CA_PRESERVE*/)
				rollback_delta(tr, delta, c->type.type->localtype);
			else /* CA_DELETE as CA_DROP's are gone already (or for globals are equal to a CA_DELETE) */
				clear_cs(tr, &delta->cs);
		}
		c->t->base.flags = c->base.flags = 0;
	}
	return ok;
}

static int
tc_gc_rollbacked( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void)commit_ts;
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
				commit_delta(tr, delta);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_cs(tr, &delta->cs);
		} else { /* rollback */
			if (i->t->commit_action == CA_COMMIT/* || i->t->commit_action == CA_PRESERVE*/)
				rollback_delta(tr, delta, type);
			else /* CA_DELETE as CA_DROP's are gone already */
				clear_cs(tr, &delta->cs);
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
		if (ok == LOG_OK && delta == d && oldest == commit_ts)
			ok = tr_merge_delta(tr, delta);
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
		rollback_segments(dbat->segs, tr, change, oldest);
	} else if (ok == LOG_OK && !tr->parent) {
		storage *d = dbat;
		merge_segments(dbat->segs, tr, change, commit_ts, oldest);
		if (ok == LOG_OK && dbat == d && oldest == commit_ts)
			ok = tr_merge_storage(tr, dbat);
	} else if (ok == LOG_OK && tr->parent) {/* cleanup older save points */
		merge_segments(dbat->segs, tr, change, commit_ts, oldest);
		ATOMIC_PTR_SET(&t->data, savepoint_commit_storage(dbat, commit_ts));
	}
	unlock_table(tr->store, t->base.id);
	return ok;
}

/* only rollback (content version) case for now */
static int
tc_gc_col( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void)Store;
	sql_column *c = (sql_column*)change->obj;

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&c->data) != change->data || isTempTable(c->t)) /* data is freed by commit */
		return 1;
	if (commit_ts && commit_ts >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
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
tc_gc_idx( sql_store Store, sql_change *change, ulng commit_ts, ulng oldest)
{
	(void)Store;
	sql_idx *i = (sql_idx*)change->obj;

	/* savepoint commit (did it merge ?) */
	if (ATOMIC_PTR_GET(&i->data) != change->data || isTempTable(i->t)) /* data is freed by commit */
		return 1;
	if (commit_ts && commit_ts >= TRANSACTION_ID_BASE) /* cannot cleanup older stuff on savepoint commits */
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
	storage *d = (storage*)change->data;
	if (d->next) {
		if (d->cs.ts > oldest)
			return LOG_OK; /* cannot cleanup yet */

		destroy_storage(d->next);
		d->next = NULL;
	}
	return 1;
}

static BUN
claim_segment(sql_trans *tr, sql_table *t, storage *s, size_t cnt)
{
	int in_transaction = segments_in_transaction(tr, t);
	assert(s->segs);
	ulng oldest = store_oldest(tr->store);
	BUN slot = 0;
	int reused = 0;

	/* naive vacuum approach, iterator through segments, check for large enough deleted segments
	 * or create new segment at the end */
	/* when claiming an segment use atomic CAS */
	for (segment *seg = s->segs->h, *p = NULL; seg; p = seg, seg = seg->next) {
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
				if ((seg=split_segment(s->segs, seg, p, tr, seg->start, cnt, false)) == NULL) {
					return BUN_NONE;
				}
			}
			seg->ts = tr->tid;
			seg->deleted = false;
			slot = seg->start;
			reused = 1;
			break;
		}
	}
	if (!reused) {
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
			tr->logchanges += cnt;
	}
	return slot;
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
	storage *s;

	/* we have a single segment structure for each persistent table
	 * for temporary tables each has its own */
	if ((s = bind_del_data(tr, t)) == NULL)
		return BUN_NONE;

	lock_table(tr->store, t->base.id);
	BUN slot = claim_segment(tr, t, s, cnt); /* find slot */
	unlock_table(tr->store, t->base.id);
	if (slot == BUN_NONE)
		return BUN_NONE;
	return (size_t)slot;
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
		msk m = !(SEG_IS_DELETED(s, tr));
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
	if (!(bn = BATmaskedcands(start, nr, b, true))) {
		BBPreclaim(b);
		return NULL;
	}
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
	size_t nr = segs_end(s->segs, tr);

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
	sf->bind_del = &bind_del;
	sf->bind_cands = &bind_cands;

	sf->claim_tab = &claim_tab;

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

	sf->create_col = &create_col;
	sf->create_idx = &create_idx;
	sf->create_del = &create_del;

	sf->destroy_col = &destroy_col;
	sf->destroy_idx = &destroy_idx;
	sf->destroy_del = &destroy_del;

	/* change into drop_* */
	sf->log_destroy_col = &log_destroy_col;
	sf->log_destroy_idx = &log_destroy_idx;
	sf->log_destroy_del = &log_destroy_del;

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
