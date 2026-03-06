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
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "pipeline.h"

typedef struct topn_t {
	Sink s;
	lng start;
	lng end;
} topn_t;

static void
topn_destroy(topn_t *t)
{
	GDKfree(t);
}

static topn_t *
topn_create(void)
{
	topn_t *t = (topn_t*)GDKzalloc(sizeof(topn_t));
	if (!t)
		return NULL;

	t->s.destroy = (sink_destroy)&topn_destroy;
	t->s.type = TOPN_SINK;
	t->start = 0;
	t->end = 0;
	return t;
}

static str
LALGsubslice(Client ctx, bat *gid, bat *rid, bat *tid, bat *bid, /*bat *sid,*/ lng *start, lng *end, const ptr *H)
{
	(void)ctx;
	str msg = MAL_SUCCEED;
	Pipeline *p = (Pipeline*)*H;
	BAT *g = NULL, *r = NULL, *t = NULL, *b = NULL;
 	BUN s = *start, e = *end;
	int fb = 1, locked = 0;
	bool private = (!tid || is_bat_nil(*tid));
	topn_t *n = NULL;

	if (*start < 0 || (*end < 0 && !is_lng_nil(*end)))
		throw(MAL, "algebra.subslice", ILLEGAL_ARGUMENT);
	if ((b = BATdescriptor(*bid)) == NULL)
		return createException(SQL, "algebra.subslice",	SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if (private && *tid && is_bat_nil(*tid)) {
		t = COLnew(b->hseqbase, /*b->ttype?b->ttype:*/TYPE_oid, 0, TRANSIENT);
		n = topn_create();
		if (!t || !n) {
			msg = createException(SQL, "algebra.subslice", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		t->tsink = (Sink*)n;
		t->tprivate_bat = 1;
	} else {
		if ((t = BATdescriptor(*tid)) == NULL) {
			msg = createException(SQL, "algebra.subslice", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto error;
		}
	}

	if (!private) {
		pipeline_lock1(t);
		locked = 1;
	}
	n = (topn_t*)t->tsink;
	if (!n) {
		if ((n = topn_create()) == NULL) {
			msg = createException(SQL, "algebra.subslice", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		t->tsink = (Sink*)n;
	}
	assert(n && n->s.type == TOPN_SINK);

	(void)p;
	BUN cnt = BATcount(b);

	BUN rs = 0, re = 0;
	rs = n->start;
	re = n->end;
	BUN off = b->hseqbase;
	e += 1; /* make range exclusive */
	if (re < e) {
		BUN ls = 0, lnr = cnt;
		if (rs < s) {
			ls = s-rs;
			if (ls > cnt)
				ls = cnt;

			rs += ls;
			lnr -= ls;
		}
		e -= s;
		if (re < e && lnr) {
			BUN rest = e - re;
			if (rest > lnr)
				rest = lnr;
			g = BATdense(0, re, rest);
			r = BATdense(0, ls+off, rest);
			if (!g || !r) {
				msg = createException(SQL, "algebra.subslice", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto error;
			}
			re += rest;
			g->tmaxval = re;
			fb = 0;
		}
		n->start = rs;
		n->end = re;
	}
	if (fb) {
		assert(!g && !r);
		g = COLnew(b->hseqbase, TYPE_oid, 0, TRANSIENT);
		r = COLnew(b->hseqbase, TYPE_oid, 0, TRANSIENT);
		if (!g || !r) {
			msg = createException(SQL, "algebra.subslice", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			goto error;
		}
		g->tmaxval = re;
	}
	BBPunfix(b->batCacheid);
	if (!private) {
		pipeline_unlock1(t);
		locked = 0;
	}
	*gid = g->batCacheid;
	*rid = r->batCacheid;
	*tid = t->batCacheid;
	BBPkeepref(g);
	BBPkeepref(r);
	BBPkeepref(t);
	return msg;
error:
	if (locked)
		pipeline_unlock1(t);
	BBPreclaim(t);
	BBPreclaim(g);
	BBPreclaim(r);
	BBPreclaim(b);
	return msg;
}

#define SLICE_SIZE 100000
static str
SLICERnth_slice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* return the nth slice of SLICE_SIZE rows from the input bat */
	(void)cntxt;
	(void)mb;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *bid = getArgReference_bat(stk, pci, 1);
	int nr  = *getArgReference_int(stk, pci, 2);

	BUN s = SLICE_SIZE*nr;
	BAT *b = BATdescriptor(*bid);
	if (!b)
		return createException(SQL, "slicer.nth_slice",	SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	BAT *r = NULL;
	if (BATcount(b) < s) {
		r = COLnew(b->hseqbase, b->ttype, 0, TRANSIENT);
	} else {
		r = BATslice(b, s, s+SLICE_SIZE);
	}
	if (!r) {
		BBPunfix(b->batCacheid);
		return createException(SQL, "slicer.nth_slice",	SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	*bid = b->batCacheid;
	*res = r->batCacheid;
	BBPkeepref(b);
	BBPkeepref(r);
	return MAL_SUCCEED;
}

static str
SLICERno_slices(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* return nr of slices */
	(void)cntxt;
	(void)mb;
	int *res = getArgReference_int(stk, pci, 0);
	bat *bid = getArgReference_bat(stk, pci, 1);
	BAT *b = BATdescriptor(*bid);
	if (!b)
		return createException(SQL, "algebra.no_slices",	SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	BUN cnt = BATcount(b);

	if (cnt < SLICE_SIZE)
		*res = 1;
	else
		*res = (int)((cnt+SLICE_SIZE-1)/SLICE_SIZE);
	FORCEMITODEBUG
	if (*res < GDKnr_threads)
		*res = MIN((int)GDKnr_threads,(int)cnt);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pp_slicer_init_funcs[] = {
 command("algebra", "subslice", LALGsubslice, false, "Returns the slice of a pipelined result", args(3,7, batarg("gid", oid), batarg("rid", oid), batarg("tid", oid), batargany("b", 1), arg("start", lng), arg("end", lng), arg("pipeline", ptr))),
 pattern("slicer", "nth_slice", SLICERnth_slice, false, "Return the n-th slice, of SLICE_SIZE rrows, from the input BAT", args(2,3, batargany("slice",1), batargany("b",1), arg("nr",int))),
 pattern("slicer", "no_slices", SLICERno_slices, false, "Returns the number of slices into which the input BAT is to be sliced", args(1,2, arg("slices", int), batargany("b",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pp_slicer", NULL, pp_slicer_init_funcs); }
