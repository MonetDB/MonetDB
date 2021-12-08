/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "algebra.h"

static str
PPcounter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);

    *res = ATOMIC_INC(&p->counter);
	(void)cntxt; (void)mb;
	return MAL_SUCCEED;
}

#define sum(a,b) a+b
#define min(a,b) a<b?a:b
#define max(a,b) a>b?a:b

#define aggr(T,f)  \
	if (type == TYPE_##T) {								\
		T val = *getArgReference_##T(stk, pci, 2);		\
		if (val != T##_nil && BATcount(b)) {			\
			T *t = Tloc(b, 0);							\
			if (t[0] == T##_nil) {						\
				t[0] = val;								\
			} else										\
				t[0] = f(t[0], val);					\
			b->tnil = false;							\
			b->tnonil = true;							\
		} else if (BATcount(b) == 0) {					\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)\
				err = createException(SQL, "aggr.sum",	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
	}

static str
LOCKEDAGGRsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte)
			return createException(SQL, "aggr.sum",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(hge,sum);
		aggr(lng,sum);
		aggr(int,sum);
		aggr(sht,sum);
		aggr(bte,sum);
		if (!err)
			BBPkeepref(b->batCacheid);
		else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.sum",	"Result is not initialized");
	}
	MT_lock_unset(&p->l);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte)
			return createException(SQL, "aggr.min",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(hge,min);
		aggr(lng,min);
		aggr(int,min);
		aggr(sht,min);
		aggr(bte,min);
		if (!err)
			BBPkeepref(b->batCacheid);
		else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.min",	"Result is not initialized");
	}
	MT_lock_unset(&p->l);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte)
			return createException(SQL, "aggr.max",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(hge,max);
		aggr(lng,max);
		aggr(int,max);
		aggr(sht,max);
		aggr(bte,max);
		if (!err)
			BBPkeepref(b->batCacheid);
		else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.max",	"Result is not initialized");
	}
	MT_lock_unset(&p->l);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LALGprojection(bat *result, const ptr *h, const bat *lid, const bat *rid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->l);
	res = ALGprojection(result, lid, rid);
	MT_lock_unset(&p->l);
	return res;
}

#define unique(Type) \
		if (tt == TYPE_##Type) { \
            Type *bp = Tloc(b, 0);        \
			for(BUN i = 0; i<cnt; i++) { \
				BUN p;					 \
				bool fnd = 0;			 \
                HASHloop_##Type(ui, ui.b->thash, p, bp+i) { \
					fnd = 1;			 \
					break;				 \
				}						 \
				if (!fnd) {				 \
					if (BUNappend(u, bp+i, true) != GDK_SUCCEED) { \
						err = 1;							\
						break;								\
					}									    \
					ui.base = u->T.heap->base;				\
					gp[r++] = b->hseqbase + i;			    \
				}											\
			}												\
		}

static str
LALGunique(bat *gid, bat *uid, const ptr *h, bat *bid, bat *sid)
{
	Pipeline *p = (Pipeline*)*h;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *b = BATdescriptor(*bid);
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;

	if (u) {
		MT_lock_set(&p->l);
		BUN cnt = BATcount(b);
		BATiter ui = bat_iterator_nolock(u);

		BAT *g = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int err = 0, tt = b->ttype;
		oid *gp = Tloc(g, 0);
		BUN r = 0;

        if (BAThash(u) == GDK_SUCCEED) {
			unique(bte)
			unique(sht)
			unique(int)
			unique(lng)
			unique(hge)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, r);
			/* props */
			BBPkeepref(*uid = u->batCacheid);
			BBPkeepref(*gid = g->batCacheid);
		}
		MT_lock_unset(&p->l);
	}
	return MAL_SUCCEED;
}


#include "mel.h"
static mel_func pipeline_init_funcs[] = {
 pattern("pipeline", "counter", PPcounter, true, "return next atomic number [0..n>", args(1,2, arg("", int), arg("pipeline", ptr))),
 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "sum values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "min", LOCKEDAGGRmin, true, "min values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "max", LOCKEDAGGRmax, true, "max values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 command("lockedalgebra", "projection", LALGprojection, false, "Project left input onto right input.", args(1,4, batargany("",3), arg("pipeline", ptr), batarg("left",oid),batargany("right",3))),
 command("algebra", "unique", LALGunique, false, "Project left input onto right input.", args(2,5, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pipeline", NULL, pipeline_init_funcs); }
