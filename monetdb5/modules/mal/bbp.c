/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) M.L.Kersten, P. Boncz
 * BAT Buffer Pool
 * It is primarilly meant to ease inspection of the BAT collection managed
 * by the server.
 */
#include "monetdb_config.h"
#include "bbp.h"

static void
pseudo(int *ret, BAT *b, str X1,str X2) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s", X1,X2);
	if (BBPindex(buf) <= 0)
		BATname(b,buf);
	BATroles(b,X1,X2);
	BATmode(b,TRANSIENT);
	BATfakeCommit(b);
	*ret = b->batCacheid;
	BBPkeepref(*ret);
}

str
CMDbbpbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	ValPtr lhs;
	int i = -1;
	int ht,tt;
	BAT *b;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	lhs = getArgReference(stk,pci,0); 
	name = *(str*) getArgReference(stk, pci, 1);
	if (isIdentifier(name) < 0)
		throw(MAL, "bbp.bind", IDENTIFIER_EXPECTED);
	i = BBPindex(name);
	if (i == 0)
		throw(MAL, "bbp.bind", RUNTIME_OBJECT_MISSING);
	/* make sure you load the descriptors and heaps */
	b = (BAT *) BATdescriptor(i);
	if (b == 0)
		/* Simple ignore the binding if you can;t find the bat */
		throw(MAL, "bbp.bind", RUNTIME_OBJECT_MISSING);

	/* check conformity of the actual type and the one requested */
	ht= getHeadType(getArgType(mb,pci,0));
	tt= getColumnType(getArgType(mb,pci,0));
	if( b->htype == TYPE_void && ht== TYPE_oid) ht= TYPE_void;
	if( b->ttype == TYPE_void && tt== TYPE_oid) tt= TYPE_void;

	if( ht != b->htype || tt != b->ttype){
		BBPunfix(i);
		throw(MAL, "bbp.bind", SEMANTIC_TYPE_MISMATCH );
	}
	/* make sure we are not dealing with an about to be deleted bat */
	if( BBP_refs(b->batCacheid) == 1 &&
		BBP_lrefs(b->batCacheid) == 0){
		BBPunfix(i);
		throw(MAL, "bbp.bind", RUNTIME_OBJECT_MISSING);
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
CMDbbpNames(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_str, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpNames", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpNames");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) {
				BUNappend(b, BBP_logical(i), FALSE);
				if (BBP_logical(-i) && (BBP_refs(-i) || BBP_lrefs(-i)) && !BBPtmpcheck(BBP_logical(-i)))
					BUNappend(b,  BBP_logical(-i), FALSE);
			}
		}
	BBPunlock("CMDbbpNames");
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"bbp","name");
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
CMDbbpName(str *ret, int *bid)
{
	*ret = (str) GDKstrdup(BBP_logical(*bid));
	return MAL_SUCCEED;
}

str
CMDbbpCount(int *ret)
{
	BAT *b, *bn;
	int i;
	lng l;

	b = BATnew(TYPE_void, TYPE_lng, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpCount", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				bn = BATdescriptor(i);
				if (bn) {
					l = BATcount(bn);
					BUNappend(b,  &l, FALSE);
					BBPunfix(bn->batCacheid);
				}
			}
		}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"bbp","count");
	return MAL_SUCCEED;
}

/*
 * The BAT status is redundantly stored in CMDbat_info.
 */
str
CMDbbpLocation(int *ret)
{
	BAT *b;
	int i;
	char buf[MAXPATHLEN];
	char cwd[MAXPATHLEN];

	if (getcwd(cwd, MAXPATHLEN) == NULL)
		throw(MAL, "catalog.bbpLocation", RUNTIME_DIR_ERROR);

	b = BATnew(TYPE_void, TYPE_str, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpLocation", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpLocation");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				snprintf(buf,MAXPATHLEN,"%s/bat/%s",cwd,BBP_physical(i));
				BUNappend(b, buf, FALSE);
			}
		}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpLocation");
	pseudo(ret,b,"bbp","location");
	return MAL_SUCCEED;
}

#define monet_modulesilent (GDKdebug&PERFMASK)

str
CMDbbpHeat(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_int, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpHeat", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpHeat");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid) {
			if (BBP_cache(i) && !monet_modulesilent) {
				int heat = BBP_lastused(i);

				BUNins(b, &i, &heat, FALSE);
			} else if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				int zero = 0;

				BUNins(b, &i, &zero, FALSE);
			}
		}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpHeat");
	pseudo(ret,b,"bbp","heat");
	return MAL_SUCCEED;
}

/*
 * The BAT dirty status:dirty => (mem != disk); diffs = not-committed
 */
str
CMDbbpDirty(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_str, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpDirty", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpDirty");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				BAT *bn = BBP_cache(i);

				BUNappend(b, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", FALSE);
			}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpDirty");
	pseudo(ret,b,"bbp","status");
	return MAL_SUCCEED;
}

/*
 * The BAT status is redundantly stored in CMDbat_info.
 */
str
CMDbbpStatus(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_str, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpStatus", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpStatus");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				char *loc = BBP_cache(i) ? "load" : "disk";

				BUNappend(b, loc, FALSE);
			}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpStatus");
	pseudo(ret,b,"bbp","status");
	return MAL_SUCCEED;
}

str
CMDbbpKind(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_str, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpKind", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpKind");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid)
			if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
				char *mode = NULL;

				if ((BBP_status(i) & BBPDELETED) || !(BBP_status(i) & BBPPERSISTENT))
					mode = "transient";
				else
					mode = "persistent";
				if (mode)
					BUNappend(b, mode, FALSE);
			}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpKind");
	pseudo(ret,b,"bbp","kind");
	return MAL_SUCCEED;
}

str
CMDbbpRefCount(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_int, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpRefCount", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpRefCount");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			int refs = BBP_refs(i);

			BUNappend(b, &refs, FALSE);
		}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpRefCount");
	pseudo(ret,b,"bbp","refcnt");
	return MAL_SUCCEED;
}

str
CMDbbpLRefCount(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_int, getBBPsize());
	if (b == 0)
		throw(MAL, "catalog.bbpLRefCount", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpLRefCount");
	for (i = 1; i < getBBPsize(); i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			int refs = BBP_lrefs(i);

			BUNappend(b, &refs, FALSE);
		}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	BBPunlock("CMDbbpLRefCount");
	pseudo(ret,b,"bbp","lrefcnt");
	return MAL_SUCCEED;
}

str
CMDbbpgetIndex(int *res, int *bid)
{
	*res= *bid;
	return MAL_SUCCEED;
}

str
CMDgetBATrefcnt(int *res, int *bid)
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
CMDgetBATlrefcnt(int *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bbp.getLRefCount", INTERNAL_BAT_ACCESS);
	}
	*res = BBP_lrefs(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str CMDbbp(bat *ID, bat *NS, bat *HT, bat *TT, bat *CNT, bat *REFCNT, bat *LREFCNT, bat *LOCATION, bat *HEAT, bat *DIRTY, bat *STATUS, bat *KIND)
{
	BAT *id, *ns, *ht, *tt, *cnt, *refcnt, *lrefcnt, *location, *heat, *dirty, *status, *kind, *bn;
	int	i;
	char buf[MAXPATHLEN];

	id = BATnew(TYPE_void, TYPE_int, getBBPsize());
	ns = BATnew(TYPE_void, TYPE_str, getBBPsize());
	ht = BATnew(TYPE_void, TYPE_str, getBBPsize());
	tt = BATnew(TYPE_void, TYPE_str, getBBPsize());
	cnt = BATnew(TYPE_void, TYPE_lng, getBBPsize());
	refcnt = BATnew(TYPE_void, TYPE_int, getBBPsize());
	lrefcnt = BATnew(TYPE_void, TYPE_int, getBBPsize());
	location = BATnew(TYPE_void, TYPE_str, getBBPsize());
	heat = BATnew(TYPE_void, TYPE_int, getBBPsize());
	dirty = BATnew(TYPE_void, TYPE_str, getBBPsize());
	status = BATnew(TYPE_void, TYPE_str, getBBPsize());
	kind = BATnew(TYPE_void, TYPE_str, getBBPsize());

	if (!id || !ns || !ht || !tt || !cnt || !refcnt || !lrefcnt || !location || !heat || !dirty || !status || !kind) {
		if (id)
			BBPreclaim(id);
		if (ns)
			BBPreclaim(ns);
		if (ht)
			BBPreclaim(ht);
		if (tt)
			BBPreclaim(tt);
		if (cnt)
			BBPreclaim(cnt);
		if (refcnt)
			BBPreclaim(refcnt);
		if (lrefcnt)
			BBPreclaim(lrefcnt);
		if (location)
			BBPreclaim(location);
		if (heat)
			BBPreclaim(heat);
		if (dirty)
			BBPreclaim(dirty);
		if (status)
			BBPreclaim(status);
		if (kind)
			BBPreclaim(kind);
		throw(MAL, "catalog.bbp", MAL_MALLOC_FAIL);
	}
	BATseqbase(id, 0);
	BATseqbase(ns, 0);
	BATseqbase(ht, 0);
	BATseqbase(tt, 0);
	BATseqbase(cnt, 0);
	BATseqbase(refcnt, 0);
	BATseqbase(lrefcnt, 0);
	BATseqbase(location, 0);
	BATseqbase(heat, 0);
	BATseqbase(dirty, 0);
	BATseqbase(status, 0);
	BATseqbase(kind, 0);
	for (i = 1; i < getBBPsize(); i++) {
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			bn = BATdescriptor(i);
			if (bn) {
				lng l = BATcount(bn);
				int heat_ = BBP_lastused(i);
				char *loc = BBP_cache(i) ? "load" : "disk";
				char *mode = "persistent";
				int refs = BBP_refs(i);
				int lrefs = BBP_lrefs(i);

				if ((BBP_status(i) & BBPDELETED) || !(BBP_status(i) & BBPPERSISTENT))
					mode = "transient";
				snprintf(buf, MAXPATHLEN, "%s", BBP_physical(i));
				BUNappend(id, &i, FALSE);
				BUNappend(ns, BBP_logical(i), FALSE);
				BUNappend(ht, BATatoms[BAThtype(bn)].name, FALSE);
				BUNappend(tt, BATatoms[BATttype(bn)].name, FALSE);
				BUNappend(cnt, &l, FALSE);
				BUNappend(refcnt, &refs, FALSE);
				BUNappend(lrefcnt, &lrefs, FALSE);
				BUNappend(location, buf, FALSE);
				BUNappend(heat, &heat_, FALSE);
				BUNappend(dirty, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", FALSE);
				BUNappend(status, loc, FALSE);
				BUNappend(kind, mode, FALSE);
				BBPunfix(bn->batCacheid);
			}
		}
	}
	BBPkeepref(*ID = id->batCacheid);
	BBPkeepref(*NS = ns->batCacheid);
	BBPkeepref(*HT = ht->batCacheid);
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
}

