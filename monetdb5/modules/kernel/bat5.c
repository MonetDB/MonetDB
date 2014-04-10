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
 * Peter Boncz, M.L. Kersten
 * Binary Association Tables
 * This module contains the commands and patterns to manage Binary
 * Association Tables (BATs). The relational operations you can execute
 * on BATs have the form of a neat algebra, described in algebra.mx
 *
 * But a database system needs more that just this algebra, since often it
 * is crucial to do table-updates (this would not be permitted in a strict
 * algebra).
 *
 * All commands needed for BAT updates, property management, basic I/O,
 * persistence, and storage options can be found in this module.
 *
 * All parameters to the modules are passed by reference.
 * In particular, this means that string values are passed to the module
 * layer as (str *)
 * and we have to de-reference them before entering the gdk library.
 * (Actual a design error in gdk to differentiate passing int/str)
 * This calls for knowledge on the underlying BAT types`s
 */

#include "monetdb_config.h"
#include "bat5.h"
#include "mal_exception.h"

/*
 * The next collection of operators fill a hole in the MonetDB kernel libraries.
 * It provide handy operations on void-BATs.
 */
static BAT *
lock_desc(bat bid)
{
	BBPfix(bid);
	return &BBPgetdesc(bid)->B;
}

static void
unlock_desc(bat bid)
{
	BBPunfix(bid);
}

/* set access mode to bat, replacing input with output */
static BAT *
setaccess(BAT *b, int mode)
{
	BAT *bn;

	bn = BATsetaccess(b, mode);	/* can return NULL */
	if (b != bn)
		BBPunfix(b->batCacheid);
	return bn;
}

static char *
pre(str s1, str s2)
{
	static char buf[64];

	snprintf(buf, 64, "%s%s", s1, s2);
	return buf;
}
static char *
local_itoa(ssize_t i)
{
	static char buf[32];

	snprintf(buf, 32, SSZFMT, i);
	return buf;
}
static char *
local_utoa(size_t i)
{
	static char buf[32];

	snprintf(buf, 32, SZFMT, i);
	return buf;
}

#define COLLISION (8 * sizeof(size_t))

static void
HASHinfo(BAT *bk, BAT *bv, Hash *h, str s)
{
	BUN i;
	BUN j;
	BUN k;
	BUN cnt[COLLISION + 1];

	BUNappend(bk, pre(s, "type"), FALSE);
	BUNappend(bv, ATOMname(h->type),FALSE);
	BUNappend(bk, pre(s, "mask"), FALSE);
	BUNappend(bv, local_utoa(h->lim),FALSE);

	for (i = 0; i < COLLISION + 1; i++) {
		cnt[i] = 0;
	}
	for (i = 0; i <= h->mask; i++) {
		j = HASHlist(h, i);
		for (k = 0; j; k++)
			j >>= 1;
		cnt[k]++;
	}

	for (i = 0; i < COLLISION + 1; i++)
		if (cnt[i]) {
			BUNappend(bk, pre(s, local_utoa(i?(((size_t)1)<<(i-1)):0)), FALSE);
			BUNappend(bv, local_utoa((size_t) cnt[i]), FALSE);
		}
}

static void
infoHeap(BAT *bk, BAT*bv, Heap *hp, str nme)
{
	char buf[1024], *p = buf;

	if (!hp)
		return;
	while (*nme)
		*p++ = *nme++;
	strcpy(p, "free");
	BUNappend(bk, buf, FALSE);
	BUNappend(bv, local_utoa(hp->free),FALSE);
	strcpy(p, "size");
	BUNappend(bk, buf, FALSE);
	BUNappend(bv, local_utoa(hp->size),FALSE);
	strcpy(p, "storage");
	BUNappend(bk, buf, FALSE);
	BUNappend(bv, (hp->base == NULL || hp->base == (char*)1) ? "absent" : (hp->storage == STORE_MMAP) ? (hp->filename ? "memory mapped" : "anonymous vm") : (hp->storage == STORE_PRIV) ? "private map" : "malloced",FALSE);
	strcpy(p, "newstorage");
	BUNappend(bk, buf, FALSE);
	BUNappend(bv, (hp->newstorage == STORE_MEM) ? "malloced" : (hp->newstorage == STORE_PRIV) ? "private map" : "memory mapped",FALSE);
	strcpy(p, "filename");
	BUNappend(bk, buf, FALSE);
	BUNappend(bv, hp->filename ? hp->filename : "no file",FALSE);
}

static char *
oidtostr(oid i)
{
	int len = 48;
	static char bf[48];
	char *p = bf;

	(void) OIDtoStr(&p, &len, &i);
	return bf;
}

static int
CMDinfo(BAT **ret1, BAT **ret2, BAT *b)
{
	BAT *bk, *bv;
	const char *mode, *accessmode;

	if (!(bk = BATnew(TYPE_void, TYPE_str, 128)))
		return GDK_FAIL;
	if (!(bv = BATnew(TYPE_void, TYPE_str, 128)))
		return GDK_FAIL;
	BATseqbase(bk,0);
	BATseqbase(bv,0);
	*ret1 = bk;
	*ret2 = bv;

	if (b->batPersistence == PERSISTENT) {
		mode = "persistent";
	} else if (b->batPersistence == TRANSIENT) {
		mode = "transient";
	} else {
		mode ="unknown";
	}

	switch (b->batRestricted) {
	case BAT_READ:
		accessmode = "read-only";
		break;
	case BAT_WRITE:
		accessmode = "updatable";
		break;
	case BAT_APPEND:
		accessmode = "append-only";
		break;
	default:
		accessmode = "unknown";
	}

	BUNappend(bk, "batId", FALSE);
	BUNappend(bv, BATgetId(b),FALSE);
	BUNappend(bk, "batCacheid", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->batCacheid)),FALSE);
	BUNappend(bk, "hparentid", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->H->heap.parentid)),FALSE);
	BUNappend(bk, "tparentid", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->T->heap.parentid)),FALSE);
	BUNappend(bk, "batSharecnt", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->batSharecnt)),FALSE);
	BUNappend(bk, "batCount", FALSE);
	BUNappend(bv, local_utoa((size_t)b->batCount),FALSE);
	BUNappend(bk, "batCapacity", FALSE);
	BUNappend(bv, local_utoa((size_t)b->batCapacity),FALSE);
	BUNappend(bk, "head", FALSE);
	BUNappend(bv, ATOMname(b->htype),FALSE);
	BUNappend(bk, "tail", FALSE);
	BUNappend(bv, ATOMname(b->ttype),FALSE);
	BUNappend(bk, "batPersistence", FALSE);
	BUNappend(bv, mode,FALSE);
	BUNappend(bk, "batRestricted", FALSE);
	BUNappend(bv, accessmode,FALSE);
	BUNappend(bk, "batRefcnt", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(BBP_refs(b->batCacheid))),FALSE);
	BUNappend(bk, "batLRefcnt", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(BBP_lrefs(b->batCacheid))),FALSE);
	BUNappend(bk, "batDirty", FALSE);
	BUNappend(bv, BATdirty(b) ? "dirty" : "clean",FALSE);
	BUNappend(bk, "batSet", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->batSet)),FALSE);

	BUNappend(bk, "hsorted", FALSE);
	BUNappend(bv, local_itoa((ssize_t)BAThordered(b)),FALSE);
	BUNappend(bk, "hrevsorted", FALSE);
	BUNappend(bv, local_itoa((ssize_t)BAThrevordered(b)),FALSE);
	BUNappend(bk, "hident", FALSE);
	BUNappend(bv, b->hident,FALSE);
	BUNappend(bk, "hdense", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(BAThdense(b))),FALSE);
	BUNappend(bk, "hseqbase", FALSE);
	BUNappend(bv, oidtostr(b->hseqbase),FALSE);
	BUNappend(bk, "hkey", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->hkey)),FALSE);
	BUNappend(bk, "hvarsized", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->hvarsized)),FALSE);
	BUNappend(bk, "halign", FALSE);
	BUNappend(bv, local_utoa(b->halign),FALSE);
	BUNappend(bk, "hnosorted", FALSE);
	BUNappend(bv, local_utoa(b->H->nosorted),FALSE);
	BUNappend(bk, "hnorevsorted", FALSE);
	BUNappend(bv, local_utoa(b->H->norevsorted),FALSE);
	BUNappend(bk, "hnodense", FALSE);
	BUNappend(bv, local_utoa(b->H->nodense),FALSE);
	BUNappend(bk, "hnokey[0]", FALSE);
	BUNappend(bv, local_utoa(b->H->nokey[0]),FALSE);
	BUNappend(bk, "hnokey[1]", FALSE);
	BUNappend(bv, local_utoa(b->H->nokey[1]),FALSE);
	BUNappend(bk, "hnonil", FALSE);
	BUNappend(bv, local_utoa(b->H->nonil),FALSE);
	BUNappend(bk, "hnil", FALSE);
	BUNappend(bv, local_utoa(b->H->nil),FALSE);

	BUNappend(bk, "tident", FALSE);
	BUNappend(bv, b->tident,FALSE);
	BUNappend(bk, "tdense", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(BATtdense(b))), FALSE);
	BUNappend(bk, "tseqbase", FALSE);
	BUNappend(bv, oidtostr(b->tseqbase), FALSE);
	BUNappend(bk, "tsorted", FALSE);
	BUNappend(bv, local_itoa((ssize_t)BATtordered(b)), FALSE);
	BUNappend(bk, "trevsorted", FALSE);
	BUNappend(bv, local_itoa((ssize_t)BATtrevordered(b)), FALSE);
	BUNappend(bk, "tkey", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->tkey)), FALSE);
	BUNappend(bk, "tvarsized", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->tvarsized)), FALSE);
	BUNappend(bk, "talign", FALSE);
	BUNappend(bv, local_utoa(b->talign), FALSE);
	BUNappend(bk, "tnosorted", FALSE);
	BUNappend(bv, local_utoa(b->T->nosorted), FALSE);
	BUNappend(bk, "tnorevsorted", FALSE);
	BUNappend(bv, local_utoa(b->T->norevsorted), FALSE);
	BUNappend(bk, "tnodense", FALSE);
	BUNappend(bv, local_utoa(b->T->nodense), FALSE);
	BUNappend(bk, "tnokey[0]", FALSE);
	BUNappend(bv, local_utoa(b->T->nokey[0]), FALSE);
	BUNappend(bk, "tnokey[1]", FALSE);
	BUNappend(bv, local_utoa(b->T->nokey[1]), FALSE);
	BUNappend(bk, "tnonil", FALSE);
	BUNappend(bv, local_utoa(b->T->nonil), FALSE);
	BUNappend(bk, "tnil", FALSE);
	BUNappend(bv, local_utoa(b->T->nil), FALSE);

	BUNappend(bk, "batInserted", FALSE);
	BUNappend(bv, local_utoa(b->batInserted), FALSE);
	BUNappend(bk, "batDeleted", FALSE);
	BUNappend(bv, local_utoa(b->batDeleted), FALSE);
	BUNappend(bk, "batFirst", FALSE);
	BUNappend(bv, local_utoa(b->batFirst), FALSE);
	BUNappend(bk, "htop", FALSE);
	BUNappend(bv, local_utoa(b->H->heap.free), FALSE);
	BUNappend(bk, "ttop", FALSE);
	BUNappend(bv, local_utoa(b->T->heap.free), FALSE);
	BUNappend(bk, "batStamp", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->batStamp)), FALSE);
	BUNappend(bk, "lastUsed", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(BBP_lastused(b->batCacheid))), FALSE);
	BUNappend(bk, "curStamp", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(BBPcurstamp())), FALSE);
	BUNappend(bk, "batCopiedtodisk", FALSE);
	BUNappend(bv, local_itoa((ssize_t)(b->batCopiedtodisk)), FALSE);
	BUNappend(bk, "batDirtydesc", FALSE);
	BUNappend(bv, b->batDirtydesc ? "dirty" : "clean", FALSE);

	BUNappend(bk, "H->heap.dirty", FALSE);
	BUNappend(bv, b->H->heap.dirty ? "dirty" : "clean", FALSE);
	BUNappend(bk, "T->heap.dirty", FALSE);
	BUNappend(bv, b->T->heap.dirty ? "dirty" : "clean", FALSE);
	infoHeap(bk, bv, &b->H->heap, "head.");
	infoHeap(bk, bv, &b->T->heap, "tail.");

	BUNappend(bk, "H->vheap->dirty", FALSE);
	BUNappend(bv, (b->H->vheap && b->H->vheap->dirty) ? "dirty" : "clean", FALSE);
	infoHeap(bk, bv, b->H->vheap, "hheap.");

	BUNappend(bk, "T->vheap->dirty", FALSE);
	BUNappend(bv, (b->T->vheap && b->T->vheap->dirty) ? "dirty" : "clean", FALSE);
	infoHeap(bk, bv, b->T->vheap, "theap.");

	/* dump index information */
	if (b->H->hash) {
		HASHinfo(bk, bv, b->H->hash, "hhash->");
	}
	if (b->T->hash) {
		HASHinfo(bk, bv, b->T->hash, "thash->");
	}
	assert(BATcount(bk) == BATcount(bv));
	return GDK_SUCCEED;
}

#define ROUND_UP(x,y) ((y)*(((x)+(y)-1)/(y)))

static int
CMDbatdisksize(lng *tot, BAT *b)
{
	size_t blksize = 512;
	size_t size = 0;

	if (!isVIEW(b)) {
		size += ROUND_UP(b->H->heap.free, blksize);
		size += ROUND_UP(b->T->heap.free, blksize);
		if (b->H->vheap)
			size += ROUND_UP(b->H->vheap->free, blksize);
		if (b->T->vheap)
			size += ROUND_UP(b->T->vheap->free, blksize);
	}
	*tot = size;
	return GDK_SUCCEED;
}

static int
CMDbatvmsize(lng *tot, BAT *b)
{
	size_t blksize = MT_pagesize();
	size_t size = 0;

	if (!isVIEW(b)) {
		BUN cnt = BATcapacity(b);

		size += ROUND_UP(b->H->heap.size, blksize);
		size += ROUND_UP(b->T->heap.size, blksize);
		if (b->H->vheap)
			size += ROUND_UP(b->H->vheap->size, blksize);
		if (b->T->vheap)
			size += ROUND_UP(b->T->vheap->size, blksize);
		if (b->H->hash)
			size += ROUND_UP(sizeof(BUN) * cnt, blksize);
		if (b->T->hash)
			size += ROUND_UP(sizeof(BUN) * cnt, blksize);
	}
	*tot = size;
	return GDK_SUCCEED;
}

static int
CMDbatsize(lng *tot, BAT *b, int force)
{
	size_t size = 0;

	if ( force || !isVIEW(b)) {
		BUN cnt = BATcapacity(b);

		size += b->H->heap.size;
		size += b->T->heap.size;
		if (b->H->vheap)
			size += b->H->vheap->size;
		if (b->T->vheap)
			size += b->T->vheap->size;
		if (b->H->hash)
			size += sizeof(BUN) * cnt;
		if (b->T->hash)
			size += sizeof(BUN) * cnt;
	}
	*tot = size;
	return GDK_SUCCEED;
}

/*
 * BBP Management, IO
 */
static int
CMDrename(bit *retval, BAT *b, str s)
{
	int ret;
	int c;
	char *t = s;

	for ( ; (c = *t) != 0; t++) {
		if (c != '_' && !GDKisalnum(c)) {
			GDKerror("CMDrename: identifier expected: %s\n", s);
			return GDK_FAIL;
		}
	}

	ret = BATname(b, s);
	*retval = FALSE;
	if (ret == 1) {
		GDKerror("CMDrename: identifier expected: %s\n", s);
		return GDK_FAIL;
	} else if (ret == BBPRENAME_ILLEGAL) {
		GDKerror("CMDrename: illegal temporary name: '%s'\n", s);
		return GDK_FAIL;
	} else if (ret == BBPRENAME_LONG) {
		GDKerror("CMDrename: name too long: '%s'\n", s);
		return GDK_FAIL;
	} else if (ret != BBPRENAME_ALREADY) {
		*retval = TRUE;
	}
	return GDK_SUCCEED;
}

static int
CMDunload(bit *res, str input)
{
	bat bid = abs(BBPindex(input));

	*res = FALSE;
	if (bid > 0) {
		BAT *b;

		BBPfix(bid);
		b = BBP_cache(bid);
		if (b) {
			BBPcold(bid);	/* will trigger unload of also persistent bats */
		}
		*res = BBPunfix(bid) == 0;
	}
	return GDK_SUCCEED;
}

static int
CMDsave(bit *res, str input)
{
	bat bid = BBPindex(input);
	BAT *b;

	*res = FALSE;
	if (bid) {
		BBPfix(bid);
		b = BBP_cache(bid);
		if (b && BATdirty(b)) {
			if (BBPsave(b) == 0)
				*res = TRUE;
		}
		BBPunfix(bid);
	}
	return GDK_SUCCEED;
}


/*
 * Wrapping
 * The remainder contains the wrapper code over the version 4
 * InformationFunctions
 * In most cases we pass a BAT identifier, which should be unified
 * with a BAT descriptor. Upon failure we can simply abort the function.
 *
 * The logical head type :oid is mapped to a TYPE_void
 * with sequenceBase. It represents the old fashioned :vid
 */


str
BKCnewBAT(int *res, int *ht, int *tt, BUN *cap)
{
	BAT *bn;

	bn = BATnew(*ht == TYPE_oid ? TYPE_void : *ht, *tt, *cap);
	if (bn == NULL)
		throw(MAL, "bat.new", GDK_EXCEPTION);
	if (*ht == TYPE_oid)
		BATseqbase(bn, 0);
	*res = bn->batCacheid;
	BBPkeepref(*res);
	return MAL_SUCCEED;
}

str
BKCattach(int *ret, int *tt, str *heapfile)
{
	BAT *bn;

	bn = BATattach(*tt, *heapfile);
	if (bn == NULL)
		throw(MAL, "bat.attach", GDK_EXCEPTION);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
BKCdensebat(int *ret, wrd *size)
{
	BAT *bn;

	if (*size < 0)
		*size = 0;
	if (*size > (wrd) BUN_MAX)
		*size = (wrd) BUN_MAX;
	bn = BATnew(TYPE_void, TYPE_void, (BUN) *size);
	if (bn == NULL)
		throw(MAL, "bat.densebat", GDK_EXCEPTION);
	BATsetcount(bn, (BUN) *size);
	BATseqbase(bn, 0);
	BATseqbase(BATmirror(bn), 0);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
BKCreverse(int *ret, int *bid)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.reverse", RUNTIME_OBJECT_MISSING);
	}

	bn = BATmirror(b);			/* bn inherits ref from b */
	assert(bn != NULL);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCmirror(int *ret, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.mirror", RUNTIME_OBJECT_MISSING);
	}
	bn = VIEWcombine(b);
	if (bn != NULL) {
		if (b->batRestricted == BAT_WRITE) {
			BAT *bn1;
			bn1 = BATcopy(bn, bn->htype, bn->ttype, FALSE);
			BBPreclaim(bn);
			bn = bn1;
		}
		if (bn != NULL) {
			*ret = bn->batCacheid;
			BBPkeepref(*ret);
			BBPreleaseref(b->batCacheid);
			return MAL_SUCCEED;
		}
	}
	*ret = 0;
	BBPreleaseref(b->batCacheid);
	throw(MAL, "bat.mirror", GDK_EXCEPTION);
}

str
BKCrevert(int *r, int *bid)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.revert", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.revert", OPERATION_FAILED);
	bn = BATrevert(b);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.revert", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCorder(int *r, int *bid)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.order", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.order", OPERATION_FAILED);
	bn = BATorder(b);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.order", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCorder_rev(int *r, int *bid)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.order_rev", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.order_rev", OPERATION_FAILED);
	bn = BATorder_rev(b);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.order_rev", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCinsert_bun(int *r, int *bid, ptr h, ptr t)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.insert", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.insert", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (h == 0 || *(str*)h == 0)
			h = (ptr) str_nil;
		else
			h = (ptr) *(str *)h;
	}
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (t == 0 || *(str*)t == 0)
			t = (ptr) str_nil;
		else
			t = (ptr) *(str *)t;
	}
	bn = BUNins(b, h, t, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.insert", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCinsert_bun_force(int *r, int *bid, ptr h, ptr t, bit *force)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.insert", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.insert", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (h == 0 || *(str*)h == 0)
			h = (ptr) str_nil;
		else
			h = (ptr) *(str *)h;
	}
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (t == 0 || *(str*)t == 0)
			t = (ptr) str_nil;
		else
			t = (ptr) *(str *)t;
	}
	bn = BUNins(b, h, t, *force);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.insert", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCinsert_bat(int *r, int *bid, int *sid)
{
	BAT *bn, *b, *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.insert", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.insert", OPERATION_FAILED);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.insert", RUNTIME_OBJECT_MISSING);
	}
	bn = BATins(b, s, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.insert", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCinsert_bat_force(int *r, int *bid, int *sid, bit *force)
{
	BAT *bn, *b, *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.insert", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.insert", OPERATION_FAILED);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.insert", RUNTIME_OBJECT_MISSING);
	}
	bn = BATins(b, s, *force);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.insert", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}


str
BKCreplace_bun(int *r, int *bid, ptr h, ptr t)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.replace", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.replace", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (h == 0 || *(str*)h == 0)
			h = (ptr) str_nil;
		else
			h = (ptr) *(str *)h;
	}
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (t == 0 || *(str*)t == 0)
			t = (ptr) str_nil;
		else
			t = (ptr) *(str *)t;
	}
	bn = BUNreplace(b, h, t, 0);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.replace", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCreplace_bat(int *r, int *bid, int *sid)
{
	BAT *b, *bn, *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.replace", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.replace", OPERATION_FAILED);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.replace", RUNTIME_OBJECT_MISSING);
	}
	bn = BATreplace(b, s, 0);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.replace", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCreplace_bun_force(int *r, int *bid, ptr h, ptr t, bit *force)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.replace", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.replace", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (h == 0 || *(str*)h == 0)
			h = (ptr) str_nil;
		else
			h = (ptr) *(str *)h;
	}
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (t == 0 || *(str*)t == 0)
			t = (ptr) str_nil;
		else
			t = (ptr) *(str *)t;
	}
	bn = BUNreplace(b, h, t, *force);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.replace", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCreplace_bat_force(int *r, int *bid, int *sid, bit *force)
{
	BAT *b, *bn, *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.replace", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.replace", OPERATION_FAILED);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.replace", RUNTIME_OBJECT_MISSING);
	}
	bn = BATreplace(b, s, *force);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.replace", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCdelete_bun(int *r, int *bid, ptr h, ptr t)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete_bun", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.delete_bun", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (h == 0 || *(str*)h == 0)
			h = (ptr) str_nil;
		else
			h = (ptr) *(str *)h;
	}
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (t == 0 || *(str*)t == 0)
			t = (ptr) str_nil;
		else
			t = (ptr) *(str *)t;
	}
	bn = BUNdel(b, h, t, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.delete_bun", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCdelete(int *r, int *bid, const void *h)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.delete", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (h == 0 || *(str*)h == 0)
			h = str_nil;
		else
			h = *(str *)h;
	}
	bn = BUNdelHead(b, h, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCdelete_all(int *r, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	bn = BATclear(b, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.delete_all", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCdelete_bat_bun(int *r, int *bid, int *sid)
{
	BAT *b, *bn, *s;

	if (*bid == *sid)
		return BKCdelete_all(r, bid);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	}
	bn = BATdel(b, s, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.delete_bat_bun", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCdelete_bat(int *r, int *bid, int *sid)
{
	BAT *bn, *b, *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.delete", OPERATION_FAILED);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	}
	bn = BATdelHead(b, s, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

static str
CMDdestroy(bit *res, const char *input)
{
	int bid = BBPindex(input);

	*res = FALSE;
	if (bid) {
		BBPfix(bid);
		if (BBPindex(input) == bid) {
			BAT *b = &BBPgetdesc(abs(bid))->B;

			BATmode(b, TRANSIENT);
			*res = TRUE;
		}
		BBPunfix(bid);
	}
	return MAL_SUCCEED;
}

str
BKCdestroy_bat(bit *r, str *input)
{
	return CMDdestroy(r, *input);
}

str
BKCdestroyImmediate(bit *r, int *bid)
{
	BAT *b;
	char buf[512];

	if ((b = BATdescriptor(*bid)) == NULL)
		return MAL_SUCCEED;
	BBPlogical(b->batCacheid, buf);
	BBPreleaseref(b->batCacheid);
	return CMDdestroy(r, buf);
}

char *
BKCdestroy(bit *r, int *bid)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.destroy", RUNTIME_OBJECT_MISSING);
	*bid = 0;
	BATmode(b, TRANSIENT);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCappend_wrap(int *r, int *bid, int *uid)
{
	BAT *bn, *b, *u;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	}
	bn = BATappend(b, u, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(u->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.append", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCappend_val_wrap(int *r, int *bid, ptr u)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (u == 0 || *(str*)u == 0)
			u = (ptr) str_nil;
		else
			u = (ptr) *(str *)u;
	}
	bn = BUNappend(b, u, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.append", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCappend_reverse_val_wrap(int *r, int *bid, ptr u)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if (b->htype >= TYPE_str && ATOMstorage(b->htype) >= TYPE_str) {
		if (u == 0 || *(str*)u == 0)
			u = (ptr) str_nil;
		else
			u = (ptr) *(str *)u;
	}
	b = BATmirror(b);
	bn = BUNappend(b, u, FALSE);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.append", GDK_EXCEPTION);
	bn = BATmirror(bn);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCappend_force_wrap(int *r, int *bid, int *uid, bit *force)
{
	BAT *bn, *b, *u;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	}
	if (BATcount(u) == 0) {
		bn = b;
	} else {
		if ((b = setaccess(b, BAT_WRITE)) == NULL)
			throw(MAL, "bat.append", OPERATION_FAILED);
		bn = BATappend(b, u, *force);
	}
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	BBPreleaseref(u->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.append", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCappend_val_force_wrap(int *r, int *bid, ptr u, bit *force)
{
	BAT *bn, *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (u == 0 || *(str*)u == 0)
			u = (ptr) str_nil;
		else
			u = (ptr) *(str *)u;
	}
	bn = BUNappend(b, u, *force);
	if (b != bn)
		BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.append", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbun_inplace(int *r, int *bid, oid *id, ptr t)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	if (void_inplace(b, *id, t, FALSE) == GDK_FAIL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbun_inplace_force(int *r, int *bid, oid *id, ptr t, bit *force)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	if (void_inplace(b, *id, t, *force) == GDK_FAIL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbat_inplace(int *r, int *bid, int *rid)
{
	BAT *b, *d;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	if ((d = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	}
	if (void_replace_bat(b, d, FALSE) == BUN_NONE) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	BBPreleaseref(d->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbat_inplace_force(int *r, int *bid, int *rid, bit *force)
{
	BAT *b, *d;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*rid)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	}
	if (void_replace_bat(b, d, *force) == BUN_NONE) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	BBPreleaseref(d->batCacheid);
	return MAL_SUCCEED;
}

/*end of SQL enhancement */

char *
BKCgetAlpha(int *r, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getInserted", RUNTIME_OBJECT_MISSING);
	bn = BATalpha(b);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.getInserted", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCgetDelta(int *r, int *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getDeleted", RUNTIME_OBJECT_MISSING);
	bn = BATdelta(b);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.getDeleted", GDK_EXCEPTION);
	BBPkeepref(*r = bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCgetCapacity(lng *res, bat *bid)
{
	*res = lng_nil;
	if (BBPcheck(*bid, "bat.getCapacity")) {
		BAT *b = lock_desc(*bid);

		if (b != NULL)
			*res = (lng) BATcapacity(b);
		unlock_desc(*bid);
	}
	return MAL_SUCCEED;
}

str
BKCgetHeadType(str *res, bat *bid)
{
	const char *ret = str_nil;

	if (BBPcheck(*bid, "bat.getHeadType")) {
		BAT *b = lock_desc(*bid);

		if (b) {
			ret = *bid > 0 ? ATOMname(b->htype) : ATOMname(b->ttype);
		}
		unlock_desc(*bid);
	}
	*res = GDKstrdup(ret);
	return MAL_SUCCEED;
}

str
BKCgetTailType(str *res, int *bid)
{
	const char *ret = str_nil;

	if (BBPcheck(*bid, "bat.getTailType")) {
		BAT *b = lock_desc(*bid);

		if (b) {
			ret = *bid < 0 ? ATOMname(b->htype) : ATOMname(b->ttype);
		}
		unlock_desc(*bid);
	}
	*res = GDKstrdup(ret);
	return MAL_SUCCEED;
}

str
BKCgetRole(str *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getType", RUNTIME_OBJECT_MISSING);
	}
	*res = GDKstrdup((*bid > 0) ? b->hident : b->tident);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetkey(int *res, int *bid, bit *param)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setKey", RUNTIME_OBJECT_MISSING);
	}
	BATkey(b, *param ? BOUND2BTRUE :FALSE);
	*res = b->batCacheid;
	BBPkeepref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetSet(int *res, int *bid, bit *param)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setSet", RUNTIME_OBJECT_MISSING);
	}
	BATset(b, *param ? BOUND2BTRUE :FALSE);
	*res = b->batCacheid;
	BBPkeepref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCisaSet(bit *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isaSet", RUNTIME_OBJECT_MISSING);
	}
	*res = b->batSet;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCisSorted(bit *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", RUNTIME_OBJECT_MISSING);
	}
	*res = BATordered(b);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCisSortedReverse(bit *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", RUNTIME_OBJECT_MISSING);
	}
	*res = BATordered_rev(b);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * We must take care of the special case of a nil column (TYPE_void,seqbase=nil)
 * such nil columns never set hkey (and BUNins will never invalidate it if set) yet
 * a nil column of a BAT with <= 1 entries does not contain doubles => return TRUE.
 */

str
BKCgetKey(bit *ret, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", RUNTIME_OBJECT_MISSING);
	}
	/* we must take care of the special case of a nil column
	 * (TYPE_void,seqbase=nil): such nil columns never set hkey (and
	 * BUNins will never invalidate it if set) yet a nil column of a
	 * BAT with <= 1 entries does not contain doubles => return TRUE.
	 */
	if (BATcount(b) <= 1) {
		*ret = TRUE;
	} else {
		if (!b->hkey) {
			BATderiveHeadProps(b, 1);
		}
		*ret = b->hkey ? TRUE : FALSE;
	}
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCpersists(int *r, int *bid, bit *flg)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", RUNTIME_OBJECT_MISSING);
	}
	BATmode(b, (*flg == TRUE) ? PERSISTENT : TRANSIENT);
	BBPreleaseref(b->batCacheid);
	*r = 0;
	return MAL_SUCCEED;
}

str
BKCsetPersistent(int *r, int *bid)
{
	bit flag= TRUE;
	return BKCpersists(r, bid, &flag);
}

str
BKCisPersistent(bit *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", RUNTIME_OBJECT_MISSING);
	}
	*res = (b->batPersistence == PERSISTENT) ? TRUE :FALSE;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetTransient(int *r, int *bid)
{
	bit flag = FALSE;
	return BKCpersists(r, bid, &flag);
}

str
BKCisTransient(bit *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setTransient", RUNTIME_OBJECT_MISSING);
	}
	*res = b->batPersistence == TRANSIENT;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetWriteMode(int *res, int *bid)
{
	BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL)
        throw(MAL, "bat.setWriteMode", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.setWriteMode", OPERATION_FAILED);
	BBPkeepref(*res = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKChasWriteMode(bit *res, int *bid)
{
	BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL)
        throw(MAL, "bat.setWriteMode", RUNTIME_OBJECT_MISSING);
	*res = BATgetaccess(b) == BAT_WRITE;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetReadMode(int *res, int *bid)
{
	BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL)
        throw(MAL, "bat.setReadMode", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_READ)) == NULL)
		throw(MAL, "bat.setReadMode", OPERATION_FAILED);
	BBPkeepref(*res = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKChasReadMode(bit *res, int *bid)
{
	BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL)
        throw(MAL, "bat.setReadMode", RUNTIME_OBJECT_MISSING);
	*res = BATgetaccess(b) == BAT_READ;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetAppendMode(int *res, int *bid)
{
	BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL)
        throw(MAL, "bat.setAppendMode", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_APPEND)) == NULL)
		throw(MAL, "bat.setAppendMode", OPERATION_FAILED);
	BBPkeepref(*res = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKChasAppendMode(bit *res, int *bid)
{
	BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL)
        throw(MAL, "bat.setAppendMode", RUNTIME_OBJECT_MISSING);
	*res = BATgetaccess(b) == BAT_APPEND;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetAccess(int *res, int *bid, str *param)
{
	BAT *b;
	int m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.setAccess", RUNTIME_OBJECT_MISSING);
	switch (*param[0]) {
	case 'r':
		m = BAT_READ;
		break;
	case 'a':
		m = BAT_APPEND;
		break;
	case 'w':
		m = BAT_WRITE;
		break;
	default:
		*res = 0;
		throw(MAL, "bat.setAccess", ILLEGAL_ARGUMENT " Got %c" " expected 'r','a', or 'w'", *param[0]);
	}
	if ((b = setaccess(b, m)) == NULL)
		throw(MAL, "bat.setAccess", OPERATION_FAILED);
	BBPkeepref(*res = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCgetAccess(str *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getAccess", RUNTIME_OBJECT_MISSING);
	switch (BATgetaccess(b)) {
	case BAT_READ:
		*res = GDKstrdup("read");
		break;
	case BAT_APPEND:
		*res = GDKstrdup("append");
		break;
	case BAT_WRITE:
		*res = GDKstrdup("write");
		break;
	default:
		/* cannot happen, just here to help analysis tools */
		*res = GDKstrdup(str_nil);
		break;
	}
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Property management
 * All property operators should ensure exclusive access to the BAT
 * descriptor.
 * Where necessary use the primary view to access the properties
 */
str
BKCinfo(int *ret1, int *ret2, int *bid)
{
	BAT *bk = NULL, *bv= NULL, *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getInfo", RUNTIME_OBJECT_MISSING);
	}
	if (CMDinfo(&bk, &bv, b) == GDK_SUCCEED) {
		*ret1 = bk->batCacheid;
		*ret2 = bv->batCacheid;
		BBPkeepref(bk->batCacheid);
		BBPkeepref(bv->batCacheid);
		BBPreleaseref(*bid);
		return MAL_SUCCEED;
	}
	BBPreleaseref(*bid);
	throw(MAL, "BKCinfo", GDK_EXCEPTION);
}

str
BKCbatdisksize(lng *tot, int *bid){
	BAT *b;
	if ((b = BATdescriptor(abs(*bid))) == NULL)
		throw(MAL, "bat.getDiskSize", RUNTIME_OBJECT_MISSING);
	CMDbatdisksize(tot,b);
	BBPreleaseref(*bid);
	return MAL_SUCCEED;
}

str
BKCbatvmsize(lng *tot, int *bid){
	BAT *b;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getDiskSize", RUNTIME_OBJECT_MISSING);
	}
	CMDbatvmsize(tot,b);
	BBPreleaseref(*bid);
	return MAL_SUCCEED;
}

str
BKCbatsize(lng *tot, int *bid){
	BAT *b;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getDiskSize", RUNTIME_OBJECT_MISSING);
	}
	CMDbatsize(tot,b, FALSE);
	BBPreleaseref(*bid);
	return MAL_SUCCEED;
}

str
BKCgetStorageSize(lng *tot, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getStorageSize", RUNTIME_OBJECT_MISSING);
	CMDbatsize(tot,b,TRUE);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}
str
BKCgetSpaceUsed(lng *tot, int *bid)
{
	BAT *b;
	size_t size = sizeof(BATstore);

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getSpaceUsed", RUNTIME_OBJECT_MISSING);

	if (!isVIEW(b)) {
		BUN cnt = BATcount(b);

		size += headsize(b, cnt);
		size += tailsize(b, cnt);
		/* the upperbound is used for the heaps */
		if (b->H->vheap)
			size += b->H->vheap->size;
		if (b->T->vheap)
			size += b->T->vheap->size;
		if (b->H->hash)
			size += sizeof(BUN) * cnt;
		if (b->T->hash)
			size += sizeof(BUN) * cnt;
	}
	*tot = size;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCgetStorageSize_str(lng *tot, str batname)
{
	int bid = BBPindex(batname);

	if (bid == 0)
		throw(MAL, "bat.getStorageSize", RUNTIME_OBJECT_MISSING);
	return BKCgetStorageSize(tot, &bid);
}

/*
 * Synced BATs
 */
str
BKCisSynced(bit *ret, int *bid1, int *bid2)
{
	BAT *b1, *b2;

	if ((b1 = BATdescriptor(*bid1)) == NULL) {
		throw(MAL, "bat.isSynced", RUNTIME_OBJECT_MISSING);
	}
	if ((b2 = BATdescriptor(*bid2)) == NULL) {
		BBPreleaseref(b1->batCacheid);
		throw(MAL, "bat.isSynced", RUNTIME_OBJECT_MISSING);
	}
	*ret = ALIGNsynced(b1, b2) ? 1 : 0;
	BBPreleaseref(b1->batCacheid);
	BBPreleaseref(b2->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Role Management
 */
char *
BKCsetRole(int *r, int *bid, char **hname, char **tname)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setRole", RUNTIME_OBJECT_MISSING);
	}
	if (hname == 0 || *hname == 0 || **hname == 0){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.setRole", ILLEGAL_ARGUMENT " Head name missing");
	}
	if (tname == 0 || *tname == 0 || **tname == 0){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.setRole", ILLEGAL_ARGUMENT " Tail name missing");
	}
	BATroles(b, *hname, *tname);
	BBPreleaseref(b->batCacheid);
	*r = 0;
	return MAL_SUCCEED;
}

str
BKCsetColumn(int *r, int *bid, str *tname)
{
	BAT *b;
	str dummy;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setColumn", RUNTIME_OBJECT_MISSING);
	}
	if (tname == 0 || *tname == 0 || **tname == 0){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.setColumn", ILLEGAL_ARGUMENT " Column name missing");
	}
	/* watch out, hident is freed first */
	dummy= GDKstrdup(b->hident);
	BATroles(b, dummy, *tname);
	GDKfree(dummy);
	BBPreleaseref(b->batCacheid);
	*r =0;
	return MAL_SUCCEED;
}

str
BKCsetColumns(int *r, int *bid, str *hname, str *tname)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setColumns", RUNTIME_OBJECT_MISSING);
	}
	if (hname == 0 || *hname == 0 || **hname == 0){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.setRole", ILLEGAL_ARGUMENT " Head name missing");
	}
	if (tname == 0 || *tname == 0 || **tname == 0){
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.setRole", ILLEGAL_ARGUMENT " Tail name missing");
	}
	BATroles(b, *hname, *tname);
	BBPreleaseref(b->batCacheid);
	*r =0;
	return MAL_SUCCEED;
}


str
BKCsetName(int *r, int *bid, str *s)
{
	BAT *b;
	bit res, *rp = &res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setName", RUNTIME_OBJECT_MISSING);
	}
	CMDrename(rp, b, *s);
	BBPreleaseref(b->batCacheid);
	*r = 0;
	return MAL_SUCCEED;
}

str
BKCgetBBPname(str *ret, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getName", RUNTIME_OBJECT_MISSING);
	}
	*ret = GDKstrdup(BBPname(b->batCacheid));
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCunload(bit *res, str *input)
{
	CMDunload(res, *input);
	return MAL_SUCCEED;
}

str
BKCisCached(bit *res, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isCached", RUNTIME_OBJECT_MISSING);
	}
	*res = 0;
	BBPreleaseref(b->batCacheid);
	throw(MAL, "bat.isCached", PROGRAM_NYI);
}

str
BKCload(int *res, str *input)
{
	bat bid = BBPindex(*input);

	*res = bid;
	if (bid) {
		BBPincref(bid,TRUE);
		return MAL_SUCCEED;
	}
	throw(MAL, "bat.unload", ILLEGAL_ARGUMENT " File name missing");
}

str
BKChot(int *res, str *input)
{
	(void) res;		/* fool compiler */
	BBPhot(BBPindex(*input));
	return MAL_SUCCEED;
}

str
BKCcold(int *res, str *input)
{
	(void) res;		/* fool compiler */
	BBPcold(BBPindex(*input));
	return MAL_SUCCEED;
}

str
BKCcoldBAT(int *res, int *bid)
{
	BAT *b;

	(void) res;
	(void) bid;		/* fool compiler */
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isCached", RUNTIME_OBJECT_MISSING);
	}
	BBPcold(b->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCheat(int *res, str *input)
{
	int bid = BBPindex(*input);

	if (bid) {
		*res = BBPLASTUSED(BBP_lastused(bid));
	}
	throw(MAL, "bat", PROGRAM_NYI);
}

str
BKChotBAT(int *res, int *bid)
{
	BAT *b;

	(void) res;
	(void) bid;		/* fool compiler */
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isCached", RUNTIME_OBJECT_MISSING);
	}
	BBPhot(b->batCacheid);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsave(bit *res, str *input)
{
	CMDsave(res, *input);
	return MAL_SUCCEED;
}

str
BKCsave2(int *r, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.save", RUNTIME_OBJECT_MISSING);
	}

	if (b && BATdirty(b))
		BBPsave(b);
	BBPreleaseref(b->batCacheid);
	*r = 0;
	return MAL_SUCCEED;
}

str
BKCmmap(bit *res, int *bid, int *hbns, int *tbns, int *hhp, int *thp)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.mmap", RUNTIME_OBJECT_MISSING);
	}
	/* == int_nil means no change */
	if (*hbns == int_nil)
		*hbns = b->batMaphead;
	if (*tbns == int_nil)
		*tbns = b->batMaptail;
	if (b->H->vheap && *hhp == int_nil)
		*hhp = b->batMaphheap;
	if (b->T->vheap && *thp == int_nil)
		*thp = b->batMaptheap;
	if (BATmmap(b, *hbns, *tbns, *hhp, *thp, 0) == 0) {
		*res = TRUE;
		BBPreleaseref(b->batCacheid);
		return MAL_SUCCEED;
	}
	*res =FALSE;
	BBPreleaseref(b->batCacheid);
	throw(MAL, "bat.mmap", GDK_EXCEPTION);
}

str
BKCmmap2(bit *res, int *bid, int *mode)
{
	return BKCmmap(res, bid, mode, mode, mode, mode);
}

str
BKCmadvise(bit *res, int *bid, int *hbns, int *tbns, int *hhp, int *thp)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.madvice", RUNTIME_OBJECT_MISSING);
	}
	*res = BATmadvise(b, (*hbns == int_nil) ? -1 : *hbns, (*tbns == int_nil) ? -1 : *tbns, (*hhp == int_nil) ? -1 : *hhp, (*thp == int_nil) ? -1 : *thp);
	BBPreleaseref(b->batCacheid);
	if (*res)
		throw(MAL, "bat.madvise", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
BKCmadvise2(bit *res, int *bid, int *mode)
{
	return BKCmadvise(res, bid, mode, mode, mode, mode);
}

/*
 * Accelerator Control
 */
str
BKCaccbuild(int *ret, int *bid, str *acc, ptr *param)
{
	(void) bid;
	(void) acc;
	(void) param;
	*ret = TRUE;
	throw(MAL, "Accelerator", PROGRAM_NYI);
}

str
BKCaccbuild_std(int *ret, int *bid, int *acc)
{
	(void) bid;
	(void) acc;
	*ret = TRUE;
	throw(MAL, "Accelerator", PROGRAM_NYI);
}


str
BKCsetHash(bit *ret, int *bid, bit *prop)
{
	BAT *b;

	(void) ret;
	(void) prop;		/* fool compiler */
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setHash", RUNTIME_OBJECT_MISSING);
	}
	BAThash(b, 0);
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetSequenceBase(int *r, int *bid, oid *o)
{
    BAT *b;

    if ((b = BATdescriptor(*bid)) == NULL) {
        throw(MAL, "bat.setSequenceBase", RUNTIME_OBJECT_MISSING);
    }
    BATseqbase(b, *o);
    *r = b->batCacheid;
    BBPkeepref(b->batCacheid);
    return MAL_SUCCEED;
}

str
BKCsetSequenceBaseNil(int *r, int *bid, oid *o)
{
	oid ov = oid_nil;

	(void) o;
	return BKCsetSequenceBase(r, bid, &ov);
}

str
BKCgetSequenceBase(oid *r, int *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setSequenceBase", RUNTIME_OBJECT_MISSING);
	}
	*r = b->hseqbase;
	BBPreleaseref(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Shrinking a void-headed BAT using a list of oids to ignore.
 */
#define shrinkloop(Type)							\
	do {											\
		Type *p = (Type*)Tloc(b, BUNfirst(b));		\
		Type *q = (Type*)Tloc(b, BUNlast(b));		\
		Type *r = (Type*)Tloc(bn, BUNfirst(bn));	\
		cnt=0;										\
		for (;p<q; oidx++, p++) {					\
			if ( o < ol && *o == oidx ){			\
				o++;								\
			} else {								\
				cnt++;								\
				*r++ = *p;							\
			}										\
		}											\
	} while (0)

str
BKCshrinkBAT(int *ret, int *bid, int *did)
{
	BAT *b, *d, *bn, *bs;
	BUN cnt =0;
	oid oidx = 0, *o, *ol;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrink", RUNTIME_OBJECT_MISSING);
	}
	if ( b->htype != TYPE_void) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.shrink", SEMANTIC_TYPE_MISMATCH);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.shrink", RUNTIME_OBJECT_MISSING);
	}
	bn= BATnew(b->htype, b->ttype, BATcount(b) - BATcount(d) );
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.shrink", MAL_MALLOC_FAIL );
	}
	bs = BATmirror(BATsort(BATmirror(d)));
	BBPreleaseref(d->batCacheid);
	if (bs == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "bat.shrink", MAL_MALLOC_FAIL );
	}

	o = (oid*)Tloc(bs, BUNfirst(bs));
	ol= (oid*)Tloc(bs, BUNlast(bs));

	switch(ATOMstorage(b->ttype) ){
	case TYPE_bte: shrinkloop(bte); break;
	case TYPE_sht: shrinkloop(sht); break;
	case TYPE_int: shrinkloop(int); break;
	case TYPE_lng: shrinkloop(lng); break;
	case TYPE_flt: shrinkloop(flt); break;
	case TYPE_dbl: shrinkloop(dbl); break;
	case TYPE_oid: shrinkloop(oid); break;
	default:
		if (ATOMvarsized(bn->ttype)) {
			BUN p = BUNfirst(b);
			BUN q = BUNlast(b);
			BATiter bi = bat_iterator(b);

			cnt=0;
			for (;p<q; oidx++, p++) {
				if ( o < ol && *o == oidx ){
					o++;
				} else {
					BUNappend(bn, BUNtail(bi, p), FALSE);
					cnt++;
				}
			}
		} else {
			switch( b->T->width){
			case 1:shrinkloop(bte); break;
			case 2:shrinkloop(sht); break;
			case 4:shrinkloop(int); break;
			case 8:shrinkloop(lng); break;
			default:
				throw(MAL, "bat.shrink", "Illegal argument type");
			}
		}
	}

	BATsetcount(bn, cnt);
	BATseqbase(bn, 0);
	bn->tsorted = 0;
	bn->trevsorted = 0;
	bn->tdense = 0;
	bn->tkey = b->tkey;
	bn->T->nonil = b->T->nonil;
	bn->T->nil = b->T->nil;

	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);

	BBPreleaseref(b->batCacheid);
	BBPreleaseref(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCshrinkBATmap(int *ret, int *bid, int *did)
{
	BAT *b, *d, *bn, *bs;
	oid lim,oidx = 0, *o, *ol;
	oid *r;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	if ( b->htype != TYPE_void) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.shrinkMap", SEMANTIC_TYPE_MISMATCH);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	if ( d->htype != TYPE_void) {
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.shrinkMap", SEMANTIC_TYPE_MISMATCH);
	}

	bn= BATnew(TYPE_void, TYPE_oid, BATcount(b) );
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}
	bs = BATmirror(BATsort(BATmirror(d)));
	BBPreleaseref(d->batCacheid);
	if (bs == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}

	o = (oid*)Tloc(bs, BUNfirst(bs));
	ol= (oid*)Tloc(bs, BUNlast(bs));
	r = (oid*)Tloc(bn, BUNfirst(bn));

	lim = BATcount(b);

	for (;oidx<lim; oidx++) {
		if ( o < ol && *o == oidx ){
			o++;
		} else {
			*r++ = oidx;
		}
	}

    BATsetcount(bn, BATcount(b)-BATcount(bs));
	BATseqbase(bn, b->hseqbase);
    bn->tsorted = 0;
    bn->trevsorted = 0;
    bn->tdense = 0;

    if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);

	BBPreleaseref(b->batCacheid);
	BBPreleaseref(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}
/*
 * Shrinking a void-headed BAT using a list of oids to ignore.
 */
#define reuseloop(Type)								\
	do {											\
		Type *p = (Type*)Tloc(b, BUNfirst(b));		\
		Type *q = (Type*)Tloc(b, BUNlast(b));		\
		Type *r = (Type*)Tloc(bn, BUNfirst(bn));	\
		for (;p<q; oidx++, p++) {					\
			if ( *o == oidx ){						\
				while ( *ol == bidx && ol>o) {		\
					bidx--;							\
					ol--;q--;						\
				}									\
				*r++ = *(--q);						\
				o += (o < ol);						\
				bidx--;								\
			} else									\
				*r++ = *p;							\
		}											\
	} while (0)

str
BKCreuseBAT(int *ret, int *bid, int *did)
{
	BAT *b, *d, *bn, *bs;
	oid oidx = 0, bidx, *o, *ol;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.reuse", RUNTIME_OBJECT_MISSING);
	}
	if ( b->htype != TYPE_void) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.reuse", SEMANTIC_TYPE_MISMATCH);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.reuse", RUNTIME_OBJECT_MISSING);
	}
	bn= BATnew(b->htype, b->ttype, BATcount(b) - BATcount(d) );
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.reuse", MAL_MALLOC_FAIL );
	}
	bs = BATmirror(BATsort(BATmirror(d)));
	BBPreleaseref(d->batCacheid);
	if (bs == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "bat.reuse", MAL_MALLOC_FAIL );
	}

	bidx= BUNlast(b)-1;
	o = (oid*)Tloc(bs, BUNfirst(bs));
	ol= (oid*)Tloc(bs, BUNlast(bs))-1;

	switch(ATOMstorage(b->ttype) ){
	case TYPE_bte: reuseloop(bte); break;
	case TYPE_sht: reuseloop(sht); break;
	case TYPE_int: reuseloop(int); break;
	case TYPE_lng: reuseloop(lng); break;
	case TYPE_flt: reuseloop(flt); break;
	case TYPE_dbl: reuseloop(dbl); break;
	case TYPE_oid: reuseloop(oid); break;
	case TYPE_str: /* to be done based on its index width */
	default:
		if (ATOMvarsized(bn->ttype)) {
			BUN p = BUNfirst(b);
			BUN q = BUNlast(b);
			BATiter bi = bat_iterator(b);

			for (;p<q; oidx++, p++) {
				if ( *o == oidx ){
					while ( *ol == bidx && ol>o) {
						bidx--;
						ol--;q--;
					}
					BUNappend(bn, BUNtail(bi, --q), FALSE);
					o += (o < ol);
					bidx--;
				} else
					BUNappend(bn, BUNtail(bi, p), FALSE);
			}
		} else {
			switch( b->T->width){
			case 1:reuseloop(bte); break;
			case 2:reuseloop(sht); break;
			case 4:reuseloop(int); break;
			case 8:reuseloop(lng); break;
			default:
				throw(MAL, "bat.shrink", "Illegal argument type");
			}
		}
	}

    BATsetcount(bn, BATcount(b) - BATcount(bs));
	BATseqbase(bn, b->hseqbase);
    bn->tsorted = 0;
    bn->trevsorted = 0;
    bn->tdense = 0;
	bn->tkey = b->tkey;

    if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);

	BBPreleaseref(b->batCacheid);
	BBPreleaseref(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}
str
BKCreuseBATmap(int *ret, int *bid, int *did)
{
	BAT *b, *d, *bn, *bs;
	oid bidx, oidx = 0, *o, *ol;
	oid *r;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	if ( b->htype != TYPE_void) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.shrinkMap", SEMANTIC_TYPE_MISMATCH);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPreleaseref(b->batCacheid);
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	bn= BATnew(TYPE_void, TYPE_oid, BATcount(b) );
	if (bn == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(d->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}
	bs = BATmirror(BATsort(BATmirror(d)));
	BBPreleaseref(d->batCacheid);
	if (bs == NULL) {
		BBPreleaseref(b->batCacheid);
		BBPreleaseref(bn->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}

	bidx= BUNlast(b)-1;
    o = (oid*)Tloc(bs, BUNfirst(bs));
    ol= (oid*)Tloc(bs, BUNlast(bs));
    r = (oid*)Tloc(bn, BUNfirst(bn));

	for (;oidx<bidx; oidx++) {
		if ( *o == oidx ){
			while ( ol > o && *--ol == bidx) {
				bidx--;
			}
			*r++ = bidx;
			o += (o < ol);
			bidx--;
		} else
			*r++ = oidx;
	}

    BATsetcount(bn, BATcount(b)-BATcount(bs));
	BATseqbase(bn, b->hseqbase);
    bn->tsorted = 0;
    bn->trevsorted = 0;
    bn->tdense = 0;

    if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);

	BBPreleaseref(b->batCacheid);
	BBPreleaseref(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCmergecand(bat *ret, bat *aid, bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.mergecand", RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPreleaseref(a->batCacheid);
		throw(MAL, "bat.mergecand", RUNTIME_OBJECT_MISSING);
	}
	bn = BATmergecand(a, b);
	BBPreleaseref(a->batCacheid);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.mergecand", OPERATION_FAILED);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
BKCintersectcand(bat *ret, bat *aid, bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.intersectcand", RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPreleaseref(a->batCacheid);
		throw(MAL, "bat.intersectcand", RUNTIME_OBJECT_MISSING);
	}
	bn = BATintersectcand(a, b);
	BBPreleaseref(a->batCacheid);
	BBPreleaseref(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.intersectcand", OPERATION_FAILED);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}
