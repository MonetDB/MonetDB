/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_atoms.h"
#include "gdk_time.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "pp_hash.h"
#include "pipeline.h"

// TODO

int
BATupgrade(BAT *r, BAT *b, bool locked)
{
	/* if locked is true, both r and b's theaplock are locked, else
	 * neither of them are */
	int err = 0;
	//TODO add upgradevarheap variant which only widens, no resize!
	if (!locked)
		MT_lock_set(&b->theaplock);
	if (ATOMvarsized(r->ttype) &&
		BATcount(r) == 0 &&
		r->tvheap->parentid == r->batCacheid &&
		r->twidth < b->twidth) {
		if (locked)
			MT_lock_unset(&r->theaplock);
		if (r->twidth < b->twidth)
			err = GDKupgradevarheap(r, (1L << (8 << (b->tshift - 1))) + GDK_VAROFFSET, 0, 0) != GDK_SUCCEED;
		if (locked)
			MT_lock_set(&r->theaplock);
		assert (r->twidth == b->twidth);
	}
	/*
	if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
		   	if (r->twidth < b->twidth) {
				int m = b->twidth / r->twidth;
				r->twidth = b->twidth;
				r->tshift = b->tshift;
				r->batCapacity /= m;
				printf("%s %d\n", b->T.heap->filename, b->twidth);
			}
	}
	*/
	if (!locked)
		MT_lock_unset(&b->theaplock);
	return err;
}

/* For varsized BATs, when `u` is still empty, we let `u` share the tvheap of
 * `b`, so as to avoid copying strings during processing
 */
void
BATswap_heaps(BAT *u, BAT *b, Pipeline *p)
{
	if (p)
		pipeline_lock(p);
	MT_lock_set(&b->theaplock);
	Heap *h = b->tvheap;
	HEAPincref(h);
	MT_lock_unset(&b->theaplock);
	MT_lock_set(&u->theaplock);
	bat old = 0, new = 0;
	if (ATOMvarsized(u->ttype) && BATcount(u) == 0 && u->tvheap->parentid == u->batCacheid) {
		if (u->tvheap->parentid != u->batCacheid)
			old = u->tvheap->parentid; // FIXME: this code is now never executed
		HEAPdecref(u->tvheap, true);
		u->tvheap = h;
		new = h->parentid;
		h = NULL;
	}
	MT_lock_unset(&u->theaplock);
	if (h)
		HEAPdecref(h, false);
	if (p)
		pipeline_unlock(p);
	if (new)
		BBPretain(new);
	if (old)
		BBPrelease(old);
}

static void
sleep_ns( int ns)
{
#ifdef HAVE_NANOSLEEP
        struct timespec ts;

        ts.tv_sec = (time_t) 0;
        ts.tv_nsec = ns;
        while (nanosleep(&ts, &ts) == -1 && errno == EINTR)
                ;
#else
        struct timeval tv;

        tv.tv_sec = 0;
        tv.tv_usec = ((ns+999)/1000);
        (void) select(0, NULL, NULL, NULL, &tv);
#endif
}

#define COUNTER_SINK 98
typedef struct pp_counter_t {
	Sink s;

	MT_Lock l;
	int nr;
	int current;
	bool sync;
	int scnt;
	MT_Cond c;
	int *cur; /* nr per worker */
} pp_counter;

static void
counter_free(pp_counter *c)
{
	if (c->cur)
		GDKfree(c->cur);
	MT_lock_destroy(&c->l);
	GDKfree(c);
}

static int
sync_counter_done(pp_counter *c, int wid, int nr_workers, int redo)
{
	(void)redo;
	int res = 0, cur;
	MT_lock_set(&c->l);
	if (!c->cur)
		c->cur = (int*)GDKzalloc(sizeof(int) * nr_workers);
	cur = c->current++;
	if (cur >= c->nr)
		res = 1;
	if (res && c->scnt != nr_workers) {
		c->scnt++;
		if (c->scnt != nr_workers)
			MT_cond_wait(&c->c, &c->l);
		else
			MT_cond_broadcast(&c->c);
		assert(c->scnt == nr_workers);
	}
	MT_lock_unset(&c->l);
	assert(c->cur);
	c->cur[wid] = cur;
	return res;
}

static int
counter_done(pp_counter *c, int wid, int nr_workers, int redo)
{
	(void)redo;
	int res = 0, cur;
	MT_lock_set(&c->l);
	if (!c->cur)
		c->cur = (int*)GDKzalloc(sizeof(int) * nr_workers);
	cur = c->current++;
	if (cur >= c->nr)
		res = 1;
	MT_lock_unset(&c->l);
	assert(c->cur);
	c->cur[wid] = cur;
	return res;
}

static str
PPcounter_get(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	int *cur = getArgReference_int(stk, pci, 0);
	bat cb = *getArgReference_bat(stk, pci, 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 2);

	BAT *b = BATdescriptor(cb);
	if (!b)
		throw(MAL, "pipeline.counter_get", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	pp_counter *c = (pp_counter*)b->tsink;
	if (c->s.type != COUNTER_SINK) {
		BBPreclaim(b);
		throw(MAL, "pipeline.counter_get", SQLSTATE(HY002) "Invalid source %d", c->s.type);
	}
	if (c->sync && c->scnt != (int)p->p->nr_workers) {
		MT_lock_set(&c->l);
		if (c->scnt != (int)p->p->nr_workers) {
			c->scnt++;
			if (c->scnt != p->p->nr_workers)
				MT_cond_wait(&c->c, &c->l);
			else
				MT_cond_broadcast(&c->c);
		}
		assert(c->scnt == (int)p->p->nr_workers);
		MT_lock_unset(&c->l);
	}
	if (!c->cur) {
		c->s.done(c, p->wid, p->p->nr_workers, false);
		//counter_done(c, p->wid, p->p->nr_workers, false);
	}
	*cur = c->cur[p->wid];
	BBPreclaim(b);
	return MAL_SUCCEED;
}

static str
PPcounter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *rb = getArgReference_bat(stk, pci, 0);
	int nr = *getArgReference_int(stk, pci, 1);
	bool sync = false;
	if (pci->argc == 3)
		sync = *getArgReference_bit(stk, pci, 2);

	pp_counter *c = (pp_counter*)GDKzalloc(sizeof(pp_counter));
	if (!c) {
		throw(SQL, "pipeline.counter",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BAT *b = COLnew(0, TYPE_bte, 0, TRANSIENT);
	if (!b) {
		GDKfree(c);
		throw(SQL, "pipeline.counter",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	b->tsink = (Sink*)c;
	c->s.type = COUNTER_SINK;
	c->s.destroy = (sink_destroy)&counter_free;
	c->s.done = (sink_done)&counter_done;
	c->current = 0;
	c->cur = NULL;
	c->nr = nr;
	MT_lock_init(&c->l, "counter");
	if (sync) {
		c->sync = true;
		c->scnt = 0;
		MT_cond_init(&c->c, "sync_counter");
		c->s.done = (sink_done)&sync_counter_done;
	}
	*rb = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

void
counter_wait(Sink *s, int nr, Pipeline *p)
{
	pp_counter *c = (pp_counter*)s;
	assert(s->type == COUNTER_SINK);

	while (c->current < nr && !ATOMIC_PTR_GET(&p->p->error))
		sleep_ns(10);
}

void
counter_next(Sink *s)
{
	pp_counter *c = (pp_counter*)s;
	assert(s->type == COUNTER_SINK);

	MT_lock_set(&c->l);
	c->current++;
	MT_lock_unset(&c->l);
}

static str
PPdone(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = getArgReference_bit(stk, pci, 0);
	bat B = *getArgReference_bat(stk, pci, 1);
	bit redo = *getArgReference_bit(stk, pci, 2);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3);

	(void)cntxt; (void)mb;
	BAT *b = BATdescriptor(B);
	if (b) {
		*res = b->tsink->done(b->tsink, p->wid, p->p->nr_workers, redo);
		BBPreclaim(b);
	}
	return MAL_SUCCEED;
}

// 	 (mailbox:T, metadata:int) := pipeline.channel(initial_value:T)
static str
PPchannel(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	void *mailbox = getArgReference(stk, pci, 0);
	int *metadata = getArgReference_int(stk, pci, 1);
	void *value = getArgReference(stk, pci, 2);
	int tpe = getArgType(mb, pci, 2);

	if (!isaBatType(tpe) && ATOMvarsized(tpe))
		throw(MAL, "pipeline.chanel", SQLSTATE(42000)"cannot make channel for varsized items");

	if (isaBatType(tpe)) {
		*(bat*)mailbox = *(bat*)value;
		BBPretain(*(bat*)mailbox);
	} else if (ATOMputFIX(tpe, mailbox, value) != GDK_SUCCEED) {
		throw(MAL, "pipeline.send", GDK_EXCEPTION);
	}
	*metadata = 0;

	(void)cntxt;
	return MAL_SUCCEED;
}

// 	 dummy := pipeline.send(handle:ptr, mailbox:T, metadata:int, value:T)
static str
PPsend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;

	ptr handle = *getArgReference_ptr(stk, pci, 1);
	Pipeline *p = (Pipeline*)handle;
	Pipelines *pp = p->p;

	// Note: these point into the master stack frame not the worker stack frame
	void *mailbox = getArgReference(pp->stk, pci, 2);
	int *metadata = getArgReference_int(pp->stk, pci, 3);

	void *value = getArgReference(stk, pci, 4);
	int tpe = getArgGDKType(mb, pci, 4);

	MT_lock_set(&pp->l);

	// const char *ch_name = mb->var[getArg(pci, 2)].name;
	// char *formatted = ATOMformat(tpe, value);
	// fprintf(stderr, "Iteration %d sending value %s on channel %s\n", pp->counters[p->wid], formatted, ch_name);
	// GDKfree(formatted);

	if (!is_int_nil(*metadata)) {
		msg = createException(MAL, "pipeline.send", SQLSTATE(42000)"causality violation detected in iteration %d: %d has sent already",
			pp->counters[p->wid], *metadata);
		goto bailout;
	}

	if (isaBatType(tpe)) {
		*(bat*)mailbox = *(bat*)value;
		BBPretain(*(bat*)mailbox);
	} else if (ATOMputFIX(tpe, mailbox, value) != GDK_SUCCEED) {
		msg = createException(MAL, "pipeline.send", GDK_EXCEPTION);
		goto bailout;
	}
	*metadata = pp->counters[p->wid] + 1;

	MT_cond_broadcast(&pp->cond);
	(void)cntxt;
bailout:
	MT_lock_unset(&pp->l);
	return msg;
}

// 	 value:T := pipeline.recv(handle, mailbox:T, metadata:int)
static str
PPrecv(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	bool locked = false;

	void *ret = getArgReference(stk, pci, 0);

	ptr handle = *getArgReference_ptr(stk, pci, 1);
	Pipeline *p = (Pipeline*)handle;
	Pipelines *pp = p->p;

	// Note: these point into the master stack frame not the worker stack frame
	void *mailbox = getArgReference(pp->stk, pci, 2);
	int *metadata = getArgReference_int(pp->stk, pci, 3);
	int prev = int_nil; (void)prev; // for debug logging

	int tpe = getArgGDKType(mb, pci, 2);
	// const char *ch_name = mb->var[getArg(pci, 2)].name;

	MT_lock_set(&pp->l);
	locked = true;
	prev = *metadata ^1; // different but no overflow
	while (true) {
		int myself = pp->counters[p->wid];
		if (*metadata == myself) {
			// found it, drop out
			if (isaBatType(tpe)) {
				*(bat*)ret = *(bat*)mailbox;
				BBPretain(*(bat*)ret);
			} else if (ATOMputFIX(tpe, ret, mailbox) != GDK_SUCCEED) {
				msg = createException(MAL, "pipeline.recv", GDK_EXCEPTION);
				goto bailout;
			}
			if (isaBatType(tpe)) {
				BBPrelease(*(bat*)mailbox);
				*(bat*)mailbox = bat_nil;
			} else if (ATOMputFIX(tpe, mailbox, ATOMnilptr(tpe)) != GDK_SUCCEED) {
				msg = createException(MAL, "pipeline.recv", GDK_EXCEPTION);
				goto bailout;
			}
			*metadata = int_nil;
			//
			// char *formatted = ATOMformat(tpe, ret);
			// fprintf(stderr, "Iteration %d recv'd %s from channel %s\n", pp->counters[p->wid], formatted, ch_name);
			// GDKfree(formatted);
			break;
		}
		// value is not in yet, is there still hope?
		bool sender_still_running = false;
		for (int i = 0; i < p->p->nr_workers; i++) {
			if (pp->counters[i] == myself - 1) {
				sender_still_running = true;
				break;
			}
		}
		if (!sender_still_running) {
			// fprintf(stderr, "Iteration %d failed to recv from channel %s because no message was sent\n", pp->counters[p->wid], ch_name);
			msg = createException(MAL, "pipeline.recv", SQLSTATE(42000)"iteration %d neglected to send a message to %d", myself - 1, myself);
			break;
		}

		// if (*metadata != prev) {
		// 	prev = *metadata;
		// 	fprintf(stderr, "Iteration %d waiting to recv from channel %s [currently ", pp->counters[p->wid], ch_name);
		// 	if (is_int_nil(*metadata))
		// 		fprintf(stderr, "empty]\n");
		// 	else
		// 		fprintf(stderr, "for %d]\n", *metadata);
		// }

		// Wait until something changes
		MT_cond_wait(&pp->cond, &pp->l);
	}
	(void)cntxt;

bailout:
	if (locked)
		MT_lock_unset(&p->p->l);
	return msg;
}

#define CONCAT_SINK 99
#define SUBCONCAT_SINK 100
typedef struct pp_concat_t {
	Sink s;

	MT_Lock l;
	/* TODO per worker ? */
	int current;
	int max;
	bool started;
	int *cur;
	Sink *srcs[];
} pp_concat;

static void
concat_free( pp_concat *pcat )
{
	MT_lock_destroy(&pcat->l);
	GDKfree(pcat->cur);
	GDKfree(pcat);
}

static int
concat_done( pp_concat *c, int wid, int nr_workers, bool redo )
{
	int res = 1;
	assert(c->s.type == CONCAT_SINK || c->s.type == SUBCONCAT_SINK);
	MT_lock_set(&c->l);
	if (!c->started) {
		c->cur = (int*)GDKzalloc(sizeof(int) * nr_workers);
		c->started = true;
	}
	Sink *s = c->srcs[c->cur[wid]];
	if (s) {
		MT_lock_unset(&c->l);
		if (s->type == SUBCONCAT_SINK)
			res = concat_done( (pp_concat*)s, wid, nr_workers, redo);
		else {
			res = s->done(s, wid, nr_workers, redo);
		}
		MT_lock_set(&c->l);
		while(res && ++c->cur[wid] < c->max) {
			s = c->srcs[c->cur[wid]];
			if (!s)
				break;
			MT_lock_unset(&c->l);
			if (s->type == SUBCONCAT_SINK)
				res = concat_done( (pp_concat*)s, wid, nr_workers, false);
			else {
				res = s->done(s, wid, nr_workers, false);
			}
			MT_lock_set(&c->l);
		}
	}
	MT_lock_unset(&c->l);
	return res;
}

static str
PPconcat_block(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bit *r = getArgReference_bit(stk, pci, 0);
	bat cb = *getArgReference_bat(stk, pci, 1);
	int blockid = *getArgReference_int(stk, pci, 2);
	bit prefcnd = *getArgReference_bit(stk, pci, 3);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 4);

	if (prefcnd) {
		*r = false;
		return MAL_SUCCEED;
	}

	BAT *b = BATdescriptor(cb);
	if (!b)
		throw(MAL, "pipeline.concat_block", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	pp_concat *pcat = (pp_concat*)b->tsink;
	if (pcat->s.type != CONCAT_SINK && pcat->s.type != SUBCONCAT_SINK) {
		BBPreclaim(b);
		throw(MAL, "pipeline.concat_block", SQLSTATE(HY002) "Invalid source %d", pcat->s.type);
	}
	MT_lock_set(&pcat->l);
	assert(pcat->cur);

	*r = (pcat->cur[p->wid] == blockid);
	MT_lock_unset(&pcat->l);
	BBPreclaim(b);
	return MAL_SUCCEED;
}

static str
PPconcat_add(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *rb = getArgReference_bat(stk, pci, 0);
	bat cb = *getArgReference_bat(stk, pci, 1);
	bat ib = *getArgReference_bat(stk, pci, 2);

	BAT *b = BATdescriptor(cb), *i = BATdescriptor(ib);
	if (!b || !i) {
		BBPreclaim(b);
		throw(MAL, "pipeline.concat_add", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	pp_concat *pcat = (pp_concat*)b->tsink;
	if (pcat->s.type != CONCAT_SINK && pcat->s.type != SUBCONCAT_SINK) {
		BBPreclaim(b);
		BBPreclaim(i);
		throw(MAL, "pipeline.concat_add", SQLSTATE(HY002) "Invalid source %d", pcat->s.type);
	}
	if (pcat->current >= pcat->max) {
		BBPreclaim(b);
		BBPreclaim(i);
		throw(MAL, "pipeline.concat_add", SQLSTATE(HY002) "Concat too many sources (%d)", pcat->current);
	}
	pcat->srcs[pcat->current++] = i->tsink;
	if (i->tsink->type == CONCAT_SINK)
		i->tsink->type = SUBCONCAT_SINK;
	BBPreclaim(i);
	*rb = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

static str
PPconcat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *rb = getArgReference_bat(stk, pci, 0);
	int nr = *getArgReference_int(stk, pci, 1);
	pp_concat *pcat = (pp_concat*)GDKzalloc(sizeof(pp_concat) + (nr+1) * sizeof(Sink*) );

	if (!pcat)
		throw(SQL, "pipeline.concat",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BAT *b = COLnew(0, TYPE_bte, 0, TRANSIENT);
	if (!b) {
		GDKfree(pcat);
		throw(SQL, "pipeline.concat",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b->tsink = (Sink*)pcat;
	pcat->s.type = CONCAT_SINK;
	pcat->s.destroy = (sink_destroy)&concat_free;
	pcat->s.done = (sink_done)&concat_done;
	pcat->current = 0;
	pcat->max = nr;
	pcat->started = false;
	MT_lock_init(&pcat->l, "concat");
	*rb = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

typedef struct pp_resultset_t {
	Sink s;
	ATOMIC_TYPE claimed;
	MT_Lock l;
} pp_resultset;

static str
PPresultset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	bat *rb = getArgReference_bat(stk, pci, 0);
	pp_resultset *prs = (pp_resultset*)GDKzalloc(sizeof(pp_resultset));

	if (!prs)
		throw(SQL, "pipeline.resultset",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BAT *b = COLnew(0, TYPE_bte, 0, TRANSIENT);
	if (!b) {
		GDKfree(prs);
		throw(SQL, "pipeline.resultset",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b->tsink = (Sink*)prs;
	prs->s.destroy = (sink_destroy)&GDKfree;
	MT_lock_init(&prs->l, "resultset");
	*rb = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

static str
PPclaim(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb;
	lng *res = getArgReference_lng(stk, pci, 0);
	bat rb = *getArgReference_bat(stk, pci, 1);
	lng cnt = *getArgReference_lng(stk, pci, 2);

	BAT *b = BATdescriptor(rb);
	if (b) {
		pp_resultset *rs = (pp_resultset*)b->tsink;
		*res = ATOMIC_ADD(&rs->claimed, cnt);
		BBPreclaim(b);
	}
	return MAL_SUCCEED;
}

static str
PPidentity(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat rb = *getArgReference_bat(stk, pci, 1);
	bat ib = *getArgReference_bat(stk, pci, 2);

	BAT *b = BATdescriptor(ib), *bn = NULL;

	if (!b)
		throw(MAL, "pipeline.identity", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	BUN cnt = BATcount(b), offset = 0;
	BUN seq = b->hseqbase;
	BBPreclaim(b);
	b = BATdescriptor(rb);
	if (b) {
		pp_resultset *rs = (pp_resultset*)b->tsink;
		offset = ATOMIC_ADD(&rs->claimed, cnt);
		BBPreclaim(b);

		if (!(bn = BATdense(seq, offset, cnt)))
			throw(MAL, "pipeline.identity", GDK_EXCEPTION);
		*res = bn->batCacheid;
		BBPkeepref(bn);
	}
	return MAL_SUCCEED;
}

static str
PPappend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb;
	//bat *res = getArgReference_bat(stk, pci, 0);
	int *resdummy = getArgReference_int(stk, pci, 0);
	bat rb = *getArgReference_bat(stk, pci, 1);
	lng offset = *getArgReference_lng(stk, pci, 2);
	bat ib = *getArgReference_bat(stk, pci, 3);
	bit force = *getArgReference_bit(stk, pci, 4);
	bat rs = *getArgReference_bat(stk, pci, 5);

	*resdummy = 0;
	BAT *b = BATdescriptor(rb);
	BAT *i = BATdescriptor(ib), *oi = i;
	BAT *r = BATdescriptor(rs);

	if (!b || !i || !rs) {
		BBPreclaim(b);
		BBPreclaim(i);
		BBPreclaim(r);
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	pp_resultset *pp_rs = (pp_resultset*)r->tsink;
	(void)pp_rs;

	if (i && (i->ttype == TYPE_msk || mask_cand(i))) {
		i = BATunmask(i);
		BBPreclaim(oi);
		if (!i) {
			BBPreclaim(b);
			BBPreclaim(i);
			BBPreclaim(r);
			throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	}
	if (BATcount(i)) {
		while(BATcount(b) != (BUN)offset) {
			/* TODO if error or other issue return */
			sleep_ns(10);
		}
		//MT_lock_set(&pp_rs->l);
		if (BATappend(b, i, NULL, /*offset,*/ force) != GDK_SUCCEED) {
			//MT_lock_unset(&pp_rs->l);
			BBPreclaim(b);
			BBPreclaim(i);
			BBPreclaim(r);
			throw(MAL, "bat.append", SQLSTATE(HY002) "failed to append");
		}
		//MT_lock_unset(&pp_rs->l);
	}
	//*res = b->batCacheid;
	/* no propchecks */
	//BBPretain(*res);
	BBPreclaim(b);
	BBPreclaim(i);
	BBPreclaim(r);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pipeline_init_funcs[] = {
 pattern("pipeline", "counter", PPcounter, true, "return counter source", args(1,2,
	 batarg("sink", bte),
	 arg("nr", int)
 )),
 pattern("pipeline", "counter", PPcounter, true, "return counter source", args(1,3,
	 batarg("sink", bte),
	 arg("nr", int),
	 arg("sync", bool)	/* sync (ie all workers need to call this counter once, before any can continue) */
 )),
 pattern("pipeline", "counter_get", PPcounter_get, true, "return current number from the counter", args(1,3,
	 arg("", int),
	 batarg("sink", bte),
	 arg("pipeline", ptr)
 )),
 pattern("pipeline", "done", PPdone, true, "return counter based on source, != 0 means done", args(1,4,
	 arg("res", bit),
	 batargany("b", 1),
	 arg("redo", bit),
	 arg("pipeline", ptr)
 )),
 pattern("pipeline", "concat", PPconcat, false, "This source iterates sequentialy through the input sources", args(1,2,
	 batarg("sink", bte),
	 arg("nr", int)
 )),
 pattern("pipeline", "concat_add", PPconcat_add, false, "Add source to concat", args(1,3,
	 batarg("res", bte),
	 batarg("concat", bte),
	 batargany("input", 1)
 )),
 pattern("pipeline", "concat_block", PPconcat_block, false, "Add source to concat", args(1,5,
	 arg("res", bit),
	 batarg("concat", bte),
	 arg("blockid", int),
	 arg("prefcond", bit),
	 arg("pipeline", ptr)
 )),
 pattern("pipeline", "resultset", PPresultset, false, "return sink for synchronizing appends into resultset", args(1,1,
	 batarg("sink", bte)
 )),
 pattern("pipeline", "claim", PPclaim, false, "Claim next set of rows", args(1,3,
	 arg("", lng),
	 batarg("sink", bte),
	 arg("cnt", lng)
 )),
 pattern("pipeline", "identity", PPidentity, false, "Return next set of unique object identifiers", args(1,3,
	 batarg("res", oid),
	 batarg("sink", bte),
	 batargany("b", 1)
 )),
 pattern("bat", "append", PPappend, true, "Append bat at offset", args(1,6,
	 //batargany("res", 1),
	 arg("dummy", int),
	 batargany("b", 1),
	 arg("offset", lng),
	 batargany("input", 1),
	 arg("force", bit),
	 batarg("sink", bte)
 )),

 pattern("pipeline", "channel", PPchannel, true, "create a new channel", args(2,3,
	 argany("mailbox", 1), arg("channel", int),
	 argany("initial", 1)
 )),
 pattern("pipeline", "recv", PPrecv, true, "receive from channel", args(1,4,
	 argany("", 1),
	 arg("handle", ptr), argany("mailbox", 1), arg("channel", int)
 )),
 pattern("pipeline", "send", PPsend, true, "send through channel", args(1,5,
	 arg("", bit),
	 arg("handle", ptr), argany("mailbox",1), arg("channel",int), argany("value", 1)
 )),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pipeline", NULL, pipeline_init_funcs); }
