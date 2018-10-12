/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * (c) M.L.Kersten, P. Boncz
 * BAT Buffer Pool
 * It is primarilly meant to ease inspection of the BAT collection managed
 * by the server.
 */
#include "monetdb_config.h"
#include "bbp.h"

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

str
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

str
CMDbbpNames(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpNames", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) {
				if (BUNappend(b, BBP_logical(i), FALSE) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpNames", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","name"))
		throw(MAL, "catalog.bbpNames", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
str
CMDbbpDiskSpace(lng *ret)
{
	*ret=  getDiskSpace();
	return MAL_SUCCEED;
}
str
CMDgetPageSize(int *ret)
{
	*ret= (int)  MT_pagesize();
	return MAL_SUCCEED;
}

str
CMDbbpName(str *ret, bat *bid)
{
	*ret = (str) GDKstrdup(BBP_logical(*bid));
	if (*ret == NULL)
		throw(MAL, "catalog.bbpName", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
CMDbbpCount(bat *ret)
{
	BAT *b, *bn;
	int i;
	lng l;

	b = COLnew(0, TYPE_lng, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpCount", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				bn = BATdescriptor(i);
				if (bn) {
					l = BATcount(bn);
					BBPunfix(bn->batCacheid);
					if (BUNappend(b,  &l, FALSE) != GDK_SUCCEED) {
						BBPreclaim(b);
						throw(MAL, "catalog.bbpCount", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
str
CMDbbpLocation(bat *ret)
{
	BAT *b;
	int i;
	char buf[FILENAME_MAX];
	char cwd[FILENAME_MAX];

	if (getcwd(cwd, FILENAME_MAX) == NULL)
		throw(MAL, "catalog.bbpLocation", RUNTIME_DIR_ERROR);

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpLocation", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				snprintf(buf,FILENAME_MAX,"%s/bat/%s",cwd,BBP_physical(i));
				if (BUNappend(b, buf, FALSE) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpLocation", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
str
CMDbbpDirty(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpDirty", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				BAT *bn = BBP_cache(i);

				if (BUNappend(b, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", FALSE) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpDirty", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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
str
CMDbbpStatus(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpStatus", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				char *loc = BBP_cache(i) ? "load" : "disk";

				if (BUNappend(b, loc, FALSE) != GDK_SUCCEED) {
					BBPunlock();
					BBPreclaim(b);
					throw(MAL, "catalog.bbpStatus", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
			}
	BBPunlock();
	if (pseudo(ret,b,"bbp","status"))
		throw(MAL, "catalog.bbpStatus", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
CMDbbpKind(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_str, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpKind", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			const char *mode;

			if ((BBP_status(i) & BBPDELETED) || !(BBP_status(i) & BBPPERSISTENT))
				mode = "transient";
			else
				mode = "persistent";
			if (BUNappend(b, mode, FALSE) != GDK_SUCCEED) {
				BBPunlock();
				BBPreclaim(b);
					throw(MAL, "catalog.bbpKind", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","kind"))
		throw(MAL, "catalog.bbpKind", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
CMDbbpRefCount(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_int, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpRefCount", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			int refs = BBP_refs(i);

			if (BUNappend(b, &refs, FALSE) != GDK_SUCCEED) {
				BBPunlock();
				BBPreclaim(b);
				throw(MAL, "catalog.bbpRefCount", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","refcnt"))
		throw(MAL, "catalog.bbpRefCount", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
CMDbbpLRefCount(bat *ret)
{
	BAT *b;
	int i;

	b = COLnew(0, TYPE_int, getBBPsize(), TRANSIENT);
	if (b == 0)
		throw(MAL, "catalog.bbpLRefCount", SQLSTATE(HY001) MAL_MALLOC_FAIL);

	BBPlock();
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			int refs = BBP_lrefs(i);

			if (BUNappend(b, &refs, FALSE) != GDK_SUCCEED) {
				BBPunlock();
				BBPreclaim(b);
				throw(MAL, "catalog.bbpLRefCount", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
	BBPunlock();
	if (pseudo(ret,b,"bbp","lrefcnt"))
		throw(MAL, "catalog.bbpLRefCount", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
CMDbbpgetIndex(int *res, bat *bid)
{
	*res= *bid;
	return MAL_SUCCEED;
}

str
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

str
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

str CMDbbp(bat *ID, bat *NS, bat *TT, bat *CNT, bat *REFCNT, bat *LREFCNT, bat *LOCATION, bat *HEAT, bat *DIRTY, bat *STATUS, bat *KIND)
{
	BAT *id, *ns, *tt, *cnt, *refcnt, *lrefcnt, *location, *heat, *dirty, *status, *kind, *bn;
	bat	i;
	char buf[FILENAME_MAX];
	bat sz = getBBPsize();

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
				int heat_ = 0;
				char *loc = BBP_cache(i) ? "load" : "disk";
				char *mode = "persistent";
				int refs = BBP_refs(i);
				int lrefs = BBP_lrefs(i);

				if ((BBP_status(i) & BBPDELETED) || !(BBP_status(i) & BBPPERSISTENT))
					mode = "transient";
				snprintf(buf, FILENAME_MAX, "%s", BBP_physical(i));
				if (BUNappend(id, &i, FALSE) != GDK_SUCCEED ||
					BUNappend(ns, BBP_logical(i), FALSE) != GDK_SUCCEED ||
					BUNappend(tt, BATatoms[BATttype(bn)].name, FALSE) != GDK_SUCCEED ||
					BUNappend(cnt, &l, FALSE) != GDK_SUCCEED ||
					BUNappend(refcnt, &refs, FALSE) != GDK_SUCCEED ||
					BUNappend(lrefcnt, &lrefs, FALSE) != GDK_SUCCEED ||
					BUNappend(location, buf, FALSE) != GDK_SUCCEED ||
					BUNappend(heat, &heat_, FALSE) != GDK_SUCCEED ||
					BUNappend(dirty, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", FALSE) != GDK_SUCCEED ||
					BUNappend(status, loc, FALSE) != GDK_SUCCEED ||
					BUNappend(kind, mode, FALSE) != GDK_SUCCEED) {
					BBPunfix(bn->batCacheid);
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
	throw(MAL, "catalog.bbp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
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
		throw(MAL, "bbp.setName", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}
