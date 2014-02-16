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

/*
 * Access to a box calls for resolving the first parameter
 * to a named box. The bbp box is automatically opened.
 */
#include "monetdb_config.h"
#include "bbp.h"

#define OpenBox(X) \
	box= findBox("bbp");\
	if(box == 0 )\
		box= openBox("bbp");\
	if( box ==0) \
		throw(MAL, "bbp." X, BOX_CLOSED);


str
CMDbbpprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	(void) cntxt;
	if (openBox("bbp"))
		return MAL_SUCCEED;
	throw(MAL, "bbp.prelude", BOX_CLOSED);
}

str
CMDbbpbind(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;
	ValPtr lhs, rhs;
	int i = -1;
	int ht,tt;
	BAT *b;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	lhs = getArgReference(stk,pci,0); 
	name = *(str*) getArgReference(stk, pci, 1);
	if (isIdentifier(name) < 0)
		throw(MAL, "bbp.bind", IDENTIFIER_EXPECTED);
	box = findBox("bbp");
	if (box && (i = findVariable(box->sym, name)) >= 0) {
		rhs = &box->val->stk[i];
		VALcopy(lhs, rhs);
		if (lhs->vtype == TYPE_bat) {
			BAT *b;

			b = BBPquickdesc(lhs->val.bval, 0);
			if (b == NULL)
				throw(MAL, "bbp.bind", INTERNAL_BAT_ACCESS);
			BBPincref(b->batCacheid, TRUE);
		}
		return MAL_SUCCEED;
	}
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
	tt= getTailType(getArgType(mb,pci,0));
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
 * Compression and decompression are handled explicity. A compressed BAT is
 * recognized by its file extension on the heap name.
 * Compression of heaps can be useful to reduce the diskspace and as preparation
 * for shipping data around. We keep the truncated file around for consistency
 * of the database structures.
 */

static void
CMDtruncateheap(Heap *h,str fnme)
{
	FILE *f;
	char buf[PATHLENGTH];
	if (h == 0 || h->filename == 0 || h->storage != STORE_MMAP)
		return;
	/* reduce disk footprint */
	snprintf(buf, PATHLENGTH, "%s/%s.%s", BATDIR, fnme, (strrchr(h->filename,'.')? strrchr(h->filename,'.')+1:""));
	if ((f = fopen(buf, "w")) == NULL) {
		IODEBUG mnstr_printf(GDKstdout, "#failed to truncate: %s\n",
				strerror(errno));
	} else {
		fclose(f);
	}
	IODEBUG mnstr_printf(GDKstdout, "#truncate %s file\n", fnme);
}

static str
CMDcompressheap(Heap *h, str fnme)
{
#ifdef HAVE_LIBZ
	stream *fp;
	char buf[PATHLENGTH];
	char buf2[PATHLENGTH];

	if ( h && h->filename && h->base && h->storage == STORE_MMAP){
		snprintf(buf2,PATHLENGTH,"%s.%s",fnme,(strrchr(h->filename,'.')? strrchr(h->filename,'.')+1:""));
		GDKfilepath(buf, BATDIR, buf2, "gz");
		fp = open_gzwstream(buf);
		if ( fp ) {
			if ( (ssize_t) h->size != mnstr_write(fp,(void*)h->base,1,h->size) ){
				close_stream(fp);
				throw(MAL,"bbp.compress","compress write error");
			}
			IODEBUG mnstr_printf(GDKstdout,"#compress %s size " SZFMT" is now compressed\n", buf,h->size);
			close_stream(fp);
		}
	}
#else
	(void) h;
	(void) fnme;
#endif
	return MAL_SUCCEED;
}

static int
CMDdecompressheap(Heap *h, Heap *hn, str fnme)
{
#ifdef HAVE_LIBZ
	stream *fp;
	char buf[PATHLENGTH];
	char buf2[PATHLENGTH];

	if ( !h || !hn)
		return 0;

	if ( h->storage != STORE_MMAP || h->filename == 0)
		return -9;

	snprintf(buf2,PATHLENGTH,"%s.%s",fnme,(strrchr(h->filename,'.')? strrchr(h->filename,'.')+1:""));
	GDKfilepath(buf, BATDIR, buf2, "gz");
	fp = open_gzrstream(buf);
	if ( fp && !mnstr_errnr(fp)){
		if ( HEAPextend(hn,h->size,0) < 0)
			return -999;
		/* skip header */
		if ((ssize_t) h->size != mnstr_read(fp, (void*) hn->base,1, h->size))
			return -999;
		hn->free = h->free;
		if (h->parentid)
			BBPkeepref( h->parentid);
		hn->parentid = h->parentid;
		hn->newstorage = h->newstorage;
		hn->hashash = h->hashash;
		assert(h->copied == 0);
		IODEBUG mnstr_printf(GDKstdout,"#decompress %s size " SZFMT" \n", buf, h->size);
		close_stream(fp);
		return 1;
	}
#else
	(void) h;
	(void) hn;
	(void) fnme;
#endif
	return 0;
}

str
CMDbbpcompress(int *ret, int *bid, str *fnme)
{
	BAT *b;
	str msg;

	b = (BAT *) BATdescriptor(*bid);
	if (b == 0)
		throw(MAL, "bbp.compress", INTERNAL_BAT_ACCESS);
	if ( BATcount(b) ){
		IODEBUG mnstr_printf(GDKstdout,"#compress BAT %d %s %s \n", *bid, BBP_physical(*bid), *fnme);
		if ( (msg = CMDcompressheap(&b->H->heap, *fnme))  )
			return msg;
		if ( (msg = CMDcompressheap(b->H->vheap, *fnme))  )
			return msg;
		if ( (msg = CMDcompressheap(&b->T->heap, *fnme))  )
			return msg;
		if ( (msg = CMDcompressheap(b->T->vheap, *fnme))  )
			return msg;
	}
	BBPkeepref(*ret = b->batCacheid);
	return MAL_SUCCEED;
}
str
CMDbbpdecompress(int *ret, int *bid, str *fnme)
{
	BAT *b, *bn;
	int decomp=0;

	b = (BAT *) BATdescriptor(*bid);
	if (b == 0)
		throw(MAL, "bbp.decompress", INTERNAL_BAT_ACCESS);
	if (BATcount(b) ){
		assert(b->htype == TYPE_void);
		bn = BATnew(TYPE_void,b->ttype,0);
		/* copy the properties of the source */
		bn->hsorted = b->hsorted;
		bn->hrevsorted = b->hrevsorted;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->hkey = b->hkey;
		bn->tkey = b->tkey;
		bn->H->nonil = b->H->nonil;
		bn->T->nonil = b->T->nonil;
		bn->H->dense = b->H->dense;
		bn->T->dense = b->T->dense;
		bn->hseqbase = b->hseqbase;
		bn->tseqbase = b->tseqbase;
	
		/* use the stored width for varsized atoms */
		bn->H->width = b->H->width;
		bn->H->shift = b->H->shift;
		bn->T->width = b->T->width;
		bn->T->shift = b->T->shift;

		if (b->H->type)
			decomp += CMDdecompressheap(&b->H->heap,&bn->H->heap,*fnme);
		if (b->H->type)
			decomp += CMDdecompressheap(b->H->vheap,bn->H->vheap,*fnme);
		if (b->T->type)
			decomp += CMDdecompressheap(&b->T->heap,&bn->T->heap,*fnme);
		if (b->T->type)
			decomp += CMDdecompressheap(b->T->vheap,bn->T->vheap,*fnme);
		IODEBUG	mnstr_printf(GDKstdout,"#decompressed BAT %d %s decomp %d \n", *bid, BBP_physical(*bid),decomp);
		if (decomp < -800) {
			BBPreleaseref(bn->batCacheid);
			BBPreleaseref(b->batCacheid);
			throw(MAL,"bbp.decompress",MAL_MALLOC_FAIL);
		}
		if ( decomp <= 0){
			BBPreleaseref(bn->batCacheid);
			BBPkeepref(*ret = b->batCacheid);
			return MAL_SUCCEED;
		}
		BATsetcapacity(bn, BATcount(b));
		BATsetcount(bn, BATcount(b));
		BATsetaccess(bn, b->batRestricted);
		BBPreleaseref(b->batCacheid);
		BBPkeepref(*ret = bn->batCacheid);
		return MAL_SUCCEED;
	}
	BBPkeepref(*ret = b->batCacheid);
	return MAL_SUCCEED;
}

str
CMDbbptruncate(int *ret, int *bid, str *fnme)
{
	BAT *b;

	b = (BAT *) BATdescriptor(*bid);
	if (b == 0)
		throw(MAL, "bbp.truncate", INTERNAL_BAT_ACCESS);
	IODEBUG mnstr_printf(GDKstdout,"#truncate BAT %d %s \n", *bid, BBP_physical(*bid));
	CMDtruncateheap(&b->H->heap,*fnme);
	CMDtruncateheap(b->H->vheap,*fnme);
	CMDtruncateheap(&b->T->heap,*fnme);
	CMDtruncateheap(b->T->vheap,*fnme);
	HASHdestroy(b);
	BBPkeepref(*ret = *bid);
	return MAL_SUCCEED;
}

str
CMDbbpexpand(int *ret, int *bid, str *fnme)
{
	BAT *b;
	int decomp = 0;

	b = (BAT *) BATdescriptor(*bid);
	if (b == 0)
		throw(MAL, "bbp.expand", INTERNAL_BAT_ACCESS);
	decomp += CMDdecompressheap(&b->H->heap,&b->H->heap,*fnme);
	decomp += CMDdecompressheap(b->H->vheap,b->H->vheap,*fnme);
	decomp += CMDdecompressheap(&b->T->heap,&b->T->heap,*fnme);
	decomp += CMDdecompressheap(b->T->vheap,b->T->vheap,*fnme);
	if (decomp < 0)
		throw(MAL,"bbp.expand",MAL_MALLOC_FAIL);
	BBPkeepref(*ret = *bid);
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

	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpNames", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpNames");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_lng, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpCount", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpLocation", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpLocation");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_int, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpHeat", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpHeat");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpDirty", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpDirty");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpStatus", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpStatus");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpKind", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpKind");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_int, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpRefCount", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpRefCount");
	for (i = 1; i < BBPsize; i++)
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

	b = BATnew(TYPE_void, TYPE_int, BBPsize);
	if (b == 0)
		throw(MAL, "catalog.bbpLRefCount", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpLRefCount");
	for (i = 1; i < BBPsize; i++)
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

	id = BATnew(TYPE_void, TYPE_int, BBPsize);
	ns = BATnew(TYPE_void, TYPE_str, BBPsize);
	ht = BATnew(TYPE_void, TYPE_str, BBPsize);
	tt = BATnew(TYPE_void, TYPE_str, BBPsize);
	cnt = BATnew(TYPE_void, TYPE_lng, BBPsize);
	refcnt = BATnew(TYPE_void, TYPE_int, BBPsize);
	lrefcnt = BATnew(TYPE_void, TYPE_int, BBPsize);
	location = BATnew(TYPE_void, TYPE_str, BBPsize);
	heat = BATnew(TYPE_void, TYPE_int, BBPsize);
	dirty = BATnew(TYPE_void, TYPE_str, BBPsize);
	status = BATnew(TYPE_void, TYPE_str, BBPsize);
	kind = BATnew(TYPE_void, TYPE_str, BBPsize);

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
	for (i = 1; i < BBPsize; i++) {
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
