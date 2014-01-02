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
 * author M.L.Kersten, P. Boncz
 * BAT Buffer Pool
 * The BBP module implements a box interface over the BAT buffer pool.
 * It is primarilly meant to ease inspection of the BAT collection managed
 * by the server.
 *
 * The two predominant approaches to use bbp is to access the BBP
 * with either bind() or take(). The former merely maps the BAT name
 * to the object in the bat buffer pool.
 * A more controlled scheme is to deposit(), take(), release()
 * and discard() elements.
 * Any BAT B created can be brought under this scheme with the name N.
 * The association N->B is only maintained in the box administration
 * and not reflected in the BAT descriptor.
 * In particular, taking a  BAT object out of the box leads to a private
 * copy to isolate the user from concurrent updates on the underlying store.
 * Upon releasing it, the updates are merged with the master copy [todo].
 *
 * The remainder of this module contains operations that rely
 * on the MAL runtime setting, but logically belong to the kernel/bat
 * module.
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

/*
 * Operator implementation
 */
str
CMDbbpopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	if (openBox("bbp") != 0)
		return MAL_SUCCEED;
	throw(MAL, "bbp.open", BOX_CLOSED);
}

str
CMDbbpclose(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	OpenBox("close");
	closeBox("bbp", TRUE);
	return MAL_SUCCEED;
}

str
CMDbbpdestroy(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	OpenBox("destroy");
	destroyBox("bbp");
	return MAL_SUCCEED;
}

/*
 * Beware that once you deposit a BAT into a box, it should
 * increment the reference count to assure it is not
 * garbage collected. Moreover, it should be done so only once.
 */
str
CMDbbpdeposit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	ValPtr v;
	Box box;
	int flg;

	(void) cntxt;
	(void) mb;		/* fool compiler */

	OpenBox("deposit");
	name = *(str*) getArgReference(stk, pci, 1);
	if (isIdentifier(name) < 0 )
		throw(MAL, "bbp.deposit", IDENTIFIER_EXPECTED);
	v = getArgReference(stk,pci,2); 
	flg = findVariable(box->sym, name) >= 0;
	if (depositBox(box, name, getArgType(mb,pci,1), v))
		throw(MAL, "bbp.deposit", OPERATION_FAILED);
	if (!flg)
		BBPincref(v->val.bval, TRUE);
	return MAL_SUCCEED;
}

str
CMDbbpbindDefinition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name, loc;
	Box box;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("bind");
	name = *(str*) getArgReference(stk, pci, 1);
	loc = *(str*) getArgReference(stk, pci, 2);
	if (isIdentifier(name) < 0)
		throw(MAL, "bbp.bind", IDENTIFIER_EXPECTED);
	if (bindBAT(box, name, loc))
		throw(MAL, "bbp.bind", RUNTIME_OBJECT_MISSING);
	return MAL_SUCCEED;
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

			b = (BAT *) BBPgetdesc(lhs->val.bval);
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

str
CMDbbpbind2(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	int *ret= (int*) getArgReference(stk,pci,0);
	str hnme = *(str*) getArgReference(stk, pci, 1);
	str tnme = *(str*) getArgReference(stk, pci, 2);
	int i,ht,tt;

	(void) cntxt;
	/* check conformity of the actual type and the one requested */
	ht= getHeadType(getArgType(mb,pci,0));
	tt= getTailType(getArgType(mb,pci,0));
	/* find a specific BAT in the buffer pool */
	BBPlock("CMDbbpbind2");
	for (i = 1; i < BBPsize; i++)
	if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
		BAT *b = (BAT *) BATdescriptor(i);
		if( b == 0 ) 
			continue;
		if( strcmp(b->hident,hnme)==0 &&
			strcmp(b->tident,tnme)==0 ){

			if( b->htype == TYPE_void && ht== TYPE_oid) ht= TYPE_void;
			if( b->ttype == TYPE_void && tt== TYPE_oid) tt= TYPE_void;

			if( ht != b->htype || tt != b->ttype){
				BBPunfix(i);
				throw(MAL, "bbp.bind", SEMANTIC_TYPE_MISMATCH);
			}

			BBPkeepref(i);
			*ret = i;
			BBPunlock("CMDbbpbind2");
			return MAL_SUCCEED;
		}
		BBPreleaseref(i);
	}
	BBPunlock("CMDbbpbind2");
	throw(MAL, "bbp.find", RUNTIME_OBJECT_MISSING);
}
str
CMDbbpbindindex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	BAT *b;
	int ht,tt;
	int *ret= (int*) getArgReference(stk,pci,0);
	int *bid= (int*) getArgReference(stk,pci,1);

	(void) cntxt;
	/* check conformity of the actual type and the one requested */
	ht= getHeadType(getArgType(mb,pci,0));
	tt= getTailType(getArgType(mb,pci,0));

	if ( *bid  == bat_nil)
		throw(MAL, "bbp.bind", INTERNAL_BAT_ACCESS "Integer expected");
	b = (BAT *) BATdescriptor(*bid);
	if (b == 0)
		throw(MAL, "bbp.bind", INTERNAL_BAT_ACCESS);

	if( b->htype == TYPE_void && ht== TYPE_oid) ht= TYPE_void;
	if( b->ttype == TYPE_void && tt== TYPE_oid) tt= TYPE_void;

	if( ht != b->htype || tt != b->ttype){
		BBPunfix(b->batCacheid);
		throw(MAL, "bbp.bind", SEMANTIC_TYPE_MISMATCH);
	}
	*ret = b->batCacheid;
	BBPkeepref(*ret);
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

		bn = BATnew(b->htype,b->ttype,0);
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
		BATsetaccess(bn, b->P->restricted);
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
 * Moving BATs in/out of the box also involves
 * checking the type already known for possible misfits.
 * Therefore, we need access to the runtime context.
 * If the bat is not known in the box, we go to the bbp pool
 * and make an attempt to load it directly.
 */
str
CMDbbptake(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;
	ValPtr v;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("take");
	name = *(str*) getArgReference(stk, pci, 1);
	if (isIdentifier(name)< 0)
		throw(MAL, "bbp.take", IDENTIFIER_EXPECTED);
	if (strstr(name, "M5system_auth") == name)
		throw(MAL, "bbp.take", INVCRED_ACCESS_DENIED);
	v = getArgReference(stk,pci,0); 
	if (takeBox(box, name, v, (int) getArgType(mb, pci, 0))) {
		int bid = BBPindex(name);

		if (bid > 0 && (v->vtype == TYPE_any || v->vtype == TYPE_bat)) {
			/* adjust the types as well */
			v->vtype = TYPE_bat;
			v->val.ival = bid;
			BBPincref(bid, TRUE);
		} else
			throw(MAL, "bbp.take", RUNTIME_OBJECT_MISSING);
	}
	/* make a private copy for this client session */
	/* use the cheapest copy method */
	/* printf("bbp.take not yet fully implemented\n"); */
	return MAL_SUCCEED;
}

str
CMDbbprelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;		/* fool compiler */
	(void) mb;		/* fool compiler */
	OpenBox("release");
	name = *(str*) getArgReference(stk, pci, 1);
	releaseBox(box, name);
	/* merge the updates of this BAT with the master copy */
	/* printf("bbp.release not yet fully implemented\n"); */
	return MAL_SUCCEED;
}

/*
 * A BAT can be released to make room for others.
 * We decrease the reference count with one, but should not
 * immediately release it, because there may be aliases.
 */
str
CMDbbpreleaseBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *bid;
	Box box;

	(void) cntxt;
	OpenBox("release");
	bid = (int *) getArgReference(stk, pci, 1);
	BBPdecref(*bid, TRUE);
	releaseBAT(mb, stk, *bid);
	*bid = 0;
	return MAL_SUCCEED;
}

#if 0
/*
 * A BAT designated as garbage can be removed, provided we
 * do not keep additional references in the stack frame
 * Be careful here not to remove persistent BATs.
 * Note that we clear the dirty bit to ensure that
 * the BAT is not written back to store before being freed.
 */
str
CMDbbpgarbage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *bid;
	Box box;
	BAT *b;

	(void) cntxt;
	(void) mb;
	OpenBox("release");
	bid = (int *) getArgReference(stk, pci, 1);
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bbp.garbage", INTERNAL_BAT_ACCESS);
	}

	b->batDirty= FALSE;
	if (BBP_lrefs(*bid) == 0) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bbp.garbage", INTERNAL_BAT_ACCESS);
	}
	if (BBP_lrefs(*bid) == 2 && b->batPersistence == PERSISTENT) {
		/* release BAT from pool altogether */
		BBPunfix(b->batCacheid);
		BBPdecref(*bid, TRUE);
		*bid = 0;
		return MAL_SUCCEED;
	}
	BBPunfix(b->batCacheid);
	if (*bid)
		/* while( BBP_lrefs(*bid) > 1 ) */
		BBPdecref(*bid, TRUE);
	*bid = 0;
	return MAL_SUCCEED;
}
#endif

/*
 * A BAT can be removed forever immediately or at the end of
 * a session. The references within the current frame
 * should also be zapped.
 */
str
CMDbbpdestroyBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit ret = 0;
	int *bid;
	bit *immediate;
	str msg;

	(void) cntxt;
	bid = (int *) getArgReference(stk, pci, 1);
	immediate = (bit *) getArgReference(stk, pci, 2);
	msg = CMDbbpreleaseBAT(cntxt,mb, stk, pci);
	if( *immediate) 
		msg = BKCdestroyImmediate(&ret, bid);
	else 
		msg = BKCdestroy(&ret, bid);
	*(int *) getArgReference(stk, pci, 1)  = 0;
	return msg;
}

str
CMDbbpdestroyBAT1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit ret = 0;
	int *bid;
	(void) cntxt;
	(void) mb;

	bid = (int *) getArgReference(stk, pci, 1);
	*(int *) getArgReference(stk, pci, 1)  = 0;
	return BKCdestroyImmediate(&ret, bid);
}

str
CMDbbpSubCommit(int *ret, int *bid)
{
	bat list[2];
	list[0] = 0;	/* dummy */
	list[1] = *bid;
	BBPdir(2,list);
	(void) ret;
	return MAL_SUCCEED;
}

str
CMDbbpReleaseAll(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;

	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;		/* fool compiler */
	OpenBox("releaseAll");
	releaseAllBox(box);
	/* merge the updates with the master copies */
	/* printf("bbp.releaseAll not yet fully implemented\n"); */
	throw(MAL, "bbp.commit", PROGRAM_NYI);
}

str
CMDbbpdiscard(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str name;
	Box box;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("discard");
	name = *(str*) getArgReference(stk, pci, 1);
	if (discardBox(box, name))
		throw(MAL, "bbp.discard", OPERATION_FAILED);
	/*printf("bbp.discard not yet fully implemented\n"); */
	return MAL_SUCCEED;
}

str
CMDbbptoStr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	int i;
	ValPtr v;
	str nme;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("toString");
	nme = *(str*) getArgReference(stk, pci, 1);
	i = findVariable(box->sym, nme);
	if (i < 0)
		throw(MAL, "bbp.toString", OPERATION_FAILED);
	v = &box->val->stk[i];
	garbageElement(cntxt, getArgReference(stk,pci,0));
	if (VALconvert(TYPE_str, v) == ILLEGALVALUE)
			throw(MAL, "bbp.toString", OPERATION_FAILED);
	VALcopy( getArgReference(stk,pci,0), v);
	return MAL_SUCCEED;
}

str
CMDbbpiterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	Box box;
	oid *cursor;
	ValPtr v;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("iterator");
	cursor = (oid *) getArgReference(stk, pci, 0);
	v = getArgReference(stk,pci,1);
	if (nextBoxElement(box, cursor, v))
		throw(MAL, "bbp.iterator", OPERATION_FAILED);
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
CMDbbpGetObjects(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	int i;
	ValPtr v;
	Box box;
	int *ret;

	(void) cntxt;
	(void) mb;		/* fool compiler */
	OpenBox("getObjects");
	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "bbp.getObjects", INTERNAL_OBJ_CREATE);
	BATseqbase(b,0);
	for (i = 0; i < box->sym->vtop; i++) {
		v = &box->val->stk[i];
		BUNins(b, &v->val.bval, getVarName(box->sym, i), FALSE);
	}

	ret = (int *) getArgReference(stk, pci, 0);
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"bbp","objects");
	return MAL_SUCCEED;
}

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
CMDbbpRNames(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_void, TYPE_str, BBPsize);
	if (b == 0)
		throw(MAL, "bbp.getRNames", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	BBPlock("CMDbbpRNames");
	for (i = 1; i < BBPsize; i++)
		if (i != b->batCacheid && BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
			if (BBP_logical(-i)) {
				BUNappend(b, BBP_logical(-i), FALSE);
			} else
				BUNappend(b, BBP_logical(i), FALSE);
		}
	BBPunlock("CMDbbpRNames");
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"bbp","revname");
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


str CMDbbpType( int *ret){
	BAT	*b,*bn;
	int	i;

	b= BATnew(TYPE_void,TYPE_str,BBPsize);
	if (b == 0) 
		throw(MAL, "catalog.bbpTailType", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for(i=1; i < BBPsize; i++) if (i != b->batCacheid) 
	if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i))) {
		bn= BATdescriptor(i);
		if(bn) BUNappend(b, BATatoms[BATttype(bn)].name, FALSE);
		BBPunfix(bn->batCacheid);
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"bbp","TailType");
	return MAL_SUCCEED;
}

str CMDbbp( int *NS, int *HT, int *TT, int *CNT, int *REFCNT, int *LREFCNT, int *LOCATION, int *HEAT, int *DIRTY, int *STATUS, int *KIND)
{
	BAT	*ns, *ht, *tt, *cnt, *refcnt, *lrefcnt, *location, *heat, *dirty, *status, *kind, *bn;
	int	i;
	char buf[MAXPATHLEN];

	ns = BATnew(TYPE_int,TYPE_str,BBPsize);
	ht = BATnew(TYPE_int,TYPE_str,BBPsize);
	tt = BATnew(TYPE_int,TYPE_str,BBPsize);
	cnt = BATnew(TYPE_int,TYPE_lng,BBPsize);
	refcnt = BATnew(TYPE_int,TYPE_int,BBPsize);
	lrefcnt = BATnew(TYPE_int,TYPE_int,BBPsize);
	location = BATnew(TYPE_int,TYPE_str,BBPsize);
	heat = BATnew(TYPE_int,TYPE_int,BBPsize);
	dirty = BATnew(TYPE_int,TYPE_str,BBPsize);
	status = BATnew(TYPE_int,TYPE_str,BBPsize);
	kind = BATnew(TYPE_int,TYPE_str,BBPsize);

	if (!ns || !ht || !tt || !cnt || !refcnt || !lrefcnt || !location || !heat || !dirty || !status || !kind) 
		throw(MAL, "catalog.bbp", MAL_MALLOC_FAIL);

	for(i=1; i < BBPsize; i++) 
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
			snprintf(buf,MAXPATHLEN,"%s",BBP_physical(i));
			BUNins(ns, &i, BBP_logical(i), FALSE);
			BUNins(ht, &i, BATatoms[BAThtype(bn)].name, FALSE);
			BUNins(tt, &i, BATatoms[BATttype(bn)].name, FALSE);
			BUNins(cnt, &i, &l, FALSE);
			BUNins(refcnt, &i, &refs, FALSE);
			BUNins(lrefcnt, &i, &lrefs, FALSE);
			BUNins(location, &i, buf, FALSE);
			BUNins(heat, &i, &heat_, FALSE);
			BUNins(dirty, &i, bn ? BATdirty(bn) ? "dirty" : DELTAdirty(bn) ? "diffs" : "clean" : (BBP_status(i) & BBPSWAPPED) ? "diffs" : "clean", FALSE);
			BUNins(status, &i, loc, FALSE);
			BUNins(kind, &i, mode, FALSE);
			BBPunfix(bn->batCacheid);
		}
	}
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
