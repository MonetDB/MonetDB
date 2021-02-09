/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * (c) M.L.Kersten, P. Boncz
 * BAT Buffer Pool
 * It is primarilly meant to ease inspection of the BAT collection managed
 * by the server.
 */
#include "monetdb_config.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_module.h"
#include "mal_session.h"
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_profiler.h"
#include "bat5.h"
#include "mutils.h"

static int
pseudo(bat *ret, BAT *b, str X1,str X2) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s", X1,X2);
	if ((BBPindex(buf) <= 0 && BBPrename(b->batCacheid, buf) != 0) || BATroles(b,X2) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		return -1;
	}
	*ret = b->batCacheid;
	BBPkeepref(*ret);
	return -0;
}

static str
CMDbbpbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	ValPtr lhs;
	bat i;
	int tt;
	BAT *b;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	lhs = &stk->stk[pci->argv[0]];
	name = *getArgReference_str(stk, pci, 1);
	if (name == NULL || isIdentifier(name) < 0)
		throw(MAL, "bbp.bind", IDENTIFIER_EXPECTED);
	i = BBPindex(name);
	if (i == 0)
		throw(MAL, "bbp.bind", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	/* make sure you load the descriptors and heaps */
	b = (BAT *) BATdescriptor(i);
	if (b == 0)
		/* Simple ignore the binding if you can't find the bat */
		throw(MAL, "bbp.bind", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	/* check conformity of the actual type and the one requested */
	tt= getBatType(getArgType(mb,pci,0));
	if( b->ttype == TYPE_void && tt== TYPE_oid) tt= TYPE_void;

	if( tt != b->ttype){
		BBPunfix(i);
		throw(MAL, "bbp.bind", SEMANTIC_TYPE_MISMATCH );
	}
	/* make sure we are not dealing with an about to be deleted bat */
	if( BBP_refs(b->batCacheid) == 1 &&
		BBP_lrefs(b->batCacheid) == 0){
		BBPunfix(i);
		throw(MAL, "bbp.bind", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	BBPkeepref(b->batCacheid);
	lhs->vtype = TYPE_bat;
	lhs->val.bval = i;
	return MAL_SUCCEED;
}
/*
 * BBP status
 * The BAT buffer pool datastructures describe the memory resident information
 * on the whereabouts of the BATs. The three predominant tables are made accessible
 * for inspection.
 *
 * The most interesting system bat for end-users is the BID-> NAME mapping,
 * because it provides access to the system guaranteed persistent BAT identifier.
 * It may be the case that the user already introduced a BAT with this name,
 * it is simply removed first
 */

static str
CMDbbpNames(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpNames", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) {
				if (BUNappend(b, BBP_logical(i), false) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpNames", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","name"))
		throw(MAL, "catalog.bbpNames", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
static str
CMDbbpDiskSpace(lng *ret)
{
	*ret=  getDiskSpace();
	return MAL_SUCCEED;
}
static str
CMDgetPageSize(int *ret)
{
	*ret= (int)  MT_pagesize();
	return MAL_SUCCEED;
}

static str
CMDbbpName(str *ret, bat *bid)
{
	*ret = (str) GDKstrdup(BBP_logical(*bid));
	if (*ret == NULL)
		throw(MAL, "catalog.bbpName", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CMDbbpCount(bat *ret)
{
	BAT *b, *bn;
	int i;
	lng l;

	b = COLnew(0, TYPE_lng, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpCount", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				bn = BATdescriptor(i);
				if (bn) {
					l = BATcount(bn);
					BBPunfix(bn->batCacheid);
					if (BUNappend(b,  &l, false) != GDK_SUCCEED) {
						BBPreclaim(b);
						throw(MAL, "catalog.bbpCount", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					}
				}
			}
		}
	if (pseudo(ret,b,"bbp","count"))
		throw(MAL, "catalog.bbpCount", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/*
 * The BAT status is redundantly stored in CMDbat_info.
 */
static str
CMDbbpLocation(bat *ret)
{
	BAT *b;
	int i;
	char buf[FILENAME_MAX];
	char cwd[FILENAME_MAX];

	if (MT_getcwd(cwd, FILENAME_MAX) == NULL)
		throw(MAL, "catalog.bbpLocation", RUNTIME_DIR_ERROR);

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpLocation", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				int len = snprintf(buf,FILENAME_MAX,"%s/bat/%s",cwd,BBP_physical(i));
				if (len == -1 || len >= FILENAME_MAX) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpLocation", SQLSTATE(HY013) "Could not write bpp filename path is too large");
				}
				if (BUNappend(b, buf, false) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpLocation", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","location"))
		throw(MAL, "catalog.bbpLocation", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/*
 * The BAT dirty status:dirty => (mem != disk); diffs = not-committed
 */
static str
CMDbbpDirty(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpDirty", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				BAT *bn = BBP_cache(i);

				if (BUNappend(b, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", false) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpDirty", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
	BBPunlock();
	if (pseudo(ret,b,"bbp","status"))
		throw(MAL, "catalog.bbpDirty", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/*
 * The BAT status is redundantly stored in CMDbat_info.
 */
static str
CMDbbpStatus(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpStatus", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				char *loc = BBP_cache(i) ? "load" : "disk";

				if (BUNappend(b, loc, false) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpStatus", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
	BBPunlock();
	if (pseudo(ret,b,"bbp","status"))
		throw(MAL, "catalog.bbpStatus", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
CMDbbpKind(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpKind", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			const char *mode;

			if ((BBP_status(i) & BBPDELETED) || !(BBP_status(i) & BBPPERSISTENT))
				mode = "transient";
			else
				mode = "persistent";
			if (BUNappend(b, mode, false) != GDK_SUCCEED) {
				BBPunlock();
				BBPreclaim(b);
					throw(MAL, "catalog.bbpKind", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","kind"))
		throw(MAL, "catalog.bbpKind", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
CMDbbpRefCount(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_int, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpRefCount", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			int refs = BBP_refs(i);

			if (BUNappend(b, &refs, false) != GDK_SUCCEED) {
				BBPunlock();
				BBPreclaim(b);
				throw(MAL, "catalog.bbpRefCount", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","refcnt"))
		throw(MAL, "catalog.bbpRefCount", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
CMDbbpLRefCount(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_int, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpLRefCount", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			int refs = BBP_lrefs(i);

			if (BUNappend(b, &refs, false) != GDK_SUCCEED) {
				BBPunlock();
				BBPreclaim(b);
				throw(MAL, "catalog.bbpLRefCount", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","lrefcnt"))
		throw(MAL, "catalog.bbpLRefCount", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
CMDbbpgetIndex(int *res, bat *bid)
{
	*res= *bid;
	return MAL_SUCCEED;
}

static str
CMDgetBATrefcnt(int *res, bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bbp.getRefCount", INTERNAL_BAT_ACCESS);
	}
	*res = BBP_refs(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDgetBATlrefcnt(int *res, bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bbp.getLRefCount", INTERNAL_BAT_ACCESS);
	}
	*res = BBP_lrefs(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDbbp(bat *ID, bat *NS, bat *TT, bat *CNT, bat *REFCNT, bat *LREFCNT, bat *LOCATION, bat *HEAT, bat *DIRTY, bat *STATUS, bat *KIND)
{
	BAT *id, *ns, *tt, *cnt, *refcnt, *lrefcnt, *location, *heat, *dirty, *status, *kind, *bn;
	bat	i;
	char buf[FILENAME_MAX];
	bat sz = getBBPsize();
	str msg = MAL_SUCCEED;

	id = COLnew(0, TYPE_int, (BUN) sz, TRANSIENT);
	ns = COLnew(0, TYPE_str, (BUN) sz, TRANSIENT);
	tt = COLnew(0, TYPE_str, (BUN) sz, TRANSIENT);
	cnt = COLnew(0, TYPE_lng, (BUN) sz, TRANSIENT);
	refcnt = COLnew(0, TYPE_int, (BUN) sz, TRANSIENT);
	lrefcnt = COLnew(0, TYPE_int, (BUN) sz, TRANSIENT);
	location = COLnew(0, TYPE_str, (BUN) sz, TRANSIENT);
	heat = COLnew(0, TYPE_int, (BUN) sz, TRANSIENT);
	dirty = COLnew(0, TYPE_str, (BUN) sz, TRANSIENT);
	status = COLnew(0, TYPE_str, (BUN) sz, TRANSIENT);
	kind = COLnew(0, TYPE_str, (BUN) sz, TRANSIENT);

	if (!id || !ns || !tt || !cnt || !refcnt || !lrefcnt || !location || !heat || !dirty || !status || !kind) {
		goto bailout;
	}
	for (i = 1; i < sz; i++) {
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			bn = BATdescriptor(i);
			if (bn) {
				lng l = BATcount(bn);
				int heat_ = 0, len;
				char *loc = BBP_cache(i) ? "load" : "disk";
				char *mode = "persistent";
				int refs = BBP_refs(i);
				int lrefs = BBP_lrefs(i);

				if ((BBP_status(i) & BBPDELETED) || !(BBP_status(i) & BBPPERSISTENT))
					mode = "transient";
				len = snprintf(buf, FILENAME_MAX, "%s", BBP_physical(i));
				if (len == -1 || len >= FILENAME_MAX) {
					msg = createException(MAL, "catalog.bbp", SQLSTATE(HY013) "Could not bpp filename path is too large");
					goto bailout;
				}
				if (BUNappend(id, &i, false) != GDK_SUCCEED ||
					BUNappend(ns, BBP_logical(i), false) != GDK_SUCCEED ||
					BUNappend(tt, BATatoms[BATttype(bn)].name, false) != GDK_SUCCEED ||
					BUNappend(cnt, &l, false) != GDK_SUCCEED ||
					BUNappend(refcnt, &refs, false) != GDK_SUCCEED ||
					BUNappend(lrefcnt, &lrefs, false) != GDK_SUCCEED ||
					BUNappend(location, buf, false) != GDK_SUCCEED ||
					BUNappend(heat, &heat_, false) != GDK_SUCCEED ||
					BUNappend(dirty, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", false) != GDK_SUCCEED ||
					BUNappend(status, loc, false) != GDK_SUCCEED ||
					BUNappend(kind, mode, false) != GDK_SUCCEED) {
					BBPunfix(bn->batCacheid);
					msg = createException(MAL, "catalog.bbp", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				BBPunfix(bn->batCacheid);
			}
		}
	}
	BBPkeepref(*ID = id->batCacheid);
	BBPkeepref(*NS = ns->batCacheid);
	BBPkeepref(*TT = tt->batCacheid);
	BBPkeepref(*CNT = cnt->batCacheid);
	BBPkeepref(*REFCNT = refcnt->batCacheid);
	BBPkeepref(*LREFCNT = lrefcnt->batCacheid);
	BBPkeepref(*LOCATION = location->batCacheid);
	BBPkeepref(*HEAT = heat->batCacheid);
	BBPkeepref(*DIRTY = dirty->batCacheid);
	BBPkeepref(*STATUS = status->batCacheid);
	BBPkeepref(*KIND = kind->batCacheid);
	return MAL_SUCCEED;

  bailout:
	BBPreclaim(id);
	BBPreclaim(ns);
	BBPreclaim(tt);
	BBPreclaim(cnt);
	BBPreclaim(refcnt);
	BBPreclaim(lrefcnt);
	BBPreclaim(location);
	BBPreclaim(heat);
	BBPreclaim(dirty);
	BBPreclaim(status);
	BBPreclaim(kind);
	return msg;
}

static str
CMDsetName(str *rname, const bat *bid, str *name)
{
	BAT *b;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bbp.setName", INTERNAL_BAT_ACCESS);
	}
	if (BBPrename(b->batCacheid, *name) != 0) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bbp.setName", GDK_EXCEPTION);
	}
	*rname = GDKstrdup(*name);
	BBPunfix(b->batCacheid);
	if (*rname == NULL)
		throw(MAL, "bbp.setName", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func bbp_init_funcs[] = {
 pattern("bbp", "bind", CMDbbpbind, false, "Locate the BAT using its logical name", args(1,2, batargany("",2),arg("name",str))),
 command("bbp", "getIndex", CMDbbpgetIndex, false, "Retrieve the index in the BBP", args(1,2, arg("",int),batargany("b",2))),
 command("bbp", "getNames", CMDbbpNames, false, "Map BAT into its bbp name", args(1,1, batarg("",str))),
 command("bbp", "get", CMDbbp, false, "bpp", args(11,11, batarg("id",int),batarg("ns",str),batarg("tt",str),batarg("cnt",lng),batarg("refcnt",int),batarg("lrefcnt",int),batarg("location",str),batarg("heat",int),batarg("dirty",str),batarg("status",str),batarg("kind",str))),
 command("bbp", "getName", CMDbbpName, false, "Map a BAT into its internal name", args(1,2, arg("",str),batargany("b",1))),
 command("bbp", "setName", CMDsetName, false, "Rename a BAT", args(1,3, arg("",str),batargany("b",1),arg("n",str))),
 command("bbp", "getCount", CMDbbpCount, false, "Create a BAT with the cardinalities of all known BATs", args(1,1, batarg("",lng))),
 command("bbp", "getRefCount", CMDbbpRefCount, false, "Create a BAT with the (hard) reference counts", args(1,1, batarg("",int))),
 command("bbp", "getLRefCount", CMDbbpLRefCount, false, "Create a BAT with the logical reference counts", args(1,1, batarg("",int))),
 command("bbp", "getLocation", CMDbbpLocation, false, "Create a BAT with their disk locations", args(1,1, batarg("",str))),
 command("bbp", "getDirty", CMDbbpDirty, false, "Create a BAT with the dirty/ diffs/clean status", args(1,1, batarg("",str))),
 command("bbp", "getStatus", CMDbbpStatus, false, "Create a BAT with the disk/load status", args(1,1, batarg("",str))),
 command("bbp", "getKind", CMDbbpKind, false, "Create a BAT with the persistency status", args(1,1, batarg("",str))),
 command("bbp", "getRefCount", CMDgetBATrefcnt, false, "Utility for debugging MAL interpreter", args(1,2, arg("",int),batargany("b",1))),
 command("bbp", "getLRefCount", CMDgetBATlrefcnt, false, "Utility for debugging MAL interpreter", args(1,2, arg("",int),batargany("b",1))),
 command("bbp", "getDiskSpace", CMDbbpDiskSpace, false, "Estimate the amount of disk space occupied by dbpath", args(1,1, arg("",lng))),
 command("bbp", "getPageSize", CMDgetPageSize, false, "Obtain the memory page size", args(1,1, arg("",int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_bbp_mal)
{ mal_module("bbp", NULL, bbp_init_funcs); }
