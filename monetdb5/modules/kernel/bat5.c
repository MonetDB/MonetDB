/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

/* set access mode to bat, replacing input with output */
static BAT *
setaccess(BAT *b, int mode)
{
	BAT *bn = b;

	if (BATsetaccess(b, mode) != GDK_SUCCEED) {
		if (b->batSharecnt && mode != BAT_READ) {
			bn = COLcopy(b, b->ttype, TRUE, TRANSIENT);
			if (bn != NULL)
				BATsetaccess(bn, mode);
		} else {
			bn = NULL;
		}
		BBPunfix(b->batCacheid);
	}
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

static gdk_return
HASHinfo(BAT *bk, BAT *bv, Hash *h, str s)
{
	BUN i;
	BUN j;
	BUN k;
	BUN cnt[COLLISION + 1];

	if (BUNappend(bk, pre(s, "type"), FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, ATOMname(h->type),FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, pre(s, "mask"), FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(h->lim),FALSE) != GDK_SUCCEED)
		return GDK_FAIL;

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
			if (BUNappend(bk, pre(s, local_utoa(i?(((size_t)1)<<(i-1)):0)), FALSE) != GDK_SUCCEED ||
			    BUNappend(bv, local_utoa((size_t) cnt[i]), FALSE) != GDK_SUCCEED)
				return GDK_FAIL;
		}
	return GDK_SUCCEED;
}

static gdk_return
infoHeap(BAT *bk, BAT*bv, Heap *hp, str nme)
{
	char buf[1024], *p = buf;

	if (!hp)
		return GDK_SUCCEED;
	while (*nme)
		*p++ = *nme++;
	strcpy(p, "free");
	if (BUNappend(bk, buf, FALSE) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(hp->free), FALSE) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "size");
	if (BUNappend(bk, buf, FALSE) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(hp->size), FALSE) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "storage");
	if (BUNappend(bk, buf, FALSE) != GDK_SUCCEED ||
		BUNappend(bv, (hp->base == NULL || hp->base == (char*)1) ? "absent" : (hp->storage == STORE_MMAP) ? (hp->filename ? "memory mapped" : "anonymous vm") : (hp->storage == STORE_PRIV) ? "private map" : "malloced", FALSE) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "newstorage");
	if (BUNappend(bk, buf, FALSE) != GDK_SUCCEED ||
		BUNappend(bv, (hp->newstorage == STORE_MEM) ? "malloced" : (hp->newstorage == STORE_PRIV) ? "private map" : "memory mapped", FALSE) != GDK_SUCCEED)
		return GDK_FAIL;
	strcpy(p, "filename");
	if (BUNappend(bk, buf, FALSE) != GDK_SUCCEED ||
		BUNappend(bv, hp->filename ? hp->filename : "no file", FALSE) != GDK_SUCCEED)
		return GDK_FAIL;
	return GDK_SUCCEED;
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

/*
 * The remainder contains the wrapper code over the mserver version 4
 * InformationFunctions
 * In most cases we pass a BAT identifier, which should be unified
 * with a BAT descriptor. Upon failure we can simply abort the function.
 *
 * The logical head type :oid is mapped to a TYPE_void
 * with sequenceBase. It represents the old fashioned :vid
 */


str
BKCnewBAT(bat *res, const int *tt, const BUN *cap, int role)
{
	BAT *bn;

	bn = COLnew(0, *tt, *cap, role);
	if (bn == NULL)
		throw(MAL, "bat.new", GDK_EXCEPTION);
	*res = bn->batCacheid;
	BBPkeepref(*res);
	return MAL_SUCCEED;
}

str
BKCattach(bat *ret, const int *tt, const char * const *heapfile)
{
	BAT *bn;

	bn = BATattach(*tt, *heapfile, TRANSIENT);
	if (bn == NULL)
		throw(MAL, "bat.attach", GDK_EXCEPTION);
	if( bn->batPersistence == PERSISTENT)
		BATmsync(bn);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
BKCdensebat(bat *ret, const lng *size)
{
	BAT *bn;
	lng sz = *size;

	if (sz < 0)
		sz = 0;
	if (sz > (lng) BUN_MAX)
		sz = (lng) BUN_MAX;
	bn = BATdense(0, 0, (BUN) sz);
	if (bn == NULL)
		throw(MAL, "bat.densebat", GDK_EXCEPTION);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
BKCmirror(bat *ret, const bat *bid)
{
	BAT *b, *bn;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.mirror", RUNTIME_OBJECT_MISSING);
	}
	bn = BATdense(b->hseqbase, b->hseqbase, BATcount(b));
	if (bn != NULL) {
		if (b->batRestricted == BAT_WRITE) {
			BAT *bn1;
			bn1 = COLcopy(bn, bn->ttype, FALSE, TRANSIENT);
			BBPreclaim(bn);
			bn = bn1;
		}
		if (bn != NULL) {
			*ret = bn->batCacheid;
			BBPkeepref(*ret);
			BBPunfix(b->batCacheid);
			return MAL_SUCCEED;
		}
	}
	*ret = 0;
	BBPunfix(b->batCacheid);
	throw(MAL, "bat.mirror", GDK_EXCEPTION);
}

char *
BKCdelete(bat *r, const bat *bid, const oid *h)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.delete", OPERATION_FAILED);
	if (BUNdelete(b, *h) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCdelete_multi(bat *r, const bat *bid, const bat *sid)
{
	BAT *b, *s;
	gdk_return ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	}
	ret = BATdel(b, s);
	BBPunfix(s->batCacheid);
	if (ret != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCdelete_all(bat *r, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", RUNTIME_OBJECT_MISSING);
	if (BATclear(b, FALSE) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete_all", GDK_EXCEPTION);
	}
	if( b->batPersistence == PERSISTENT)
		BATmsync(b);
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCappend_wrap(bat *r, const bat *bid, const bat *uid)
{
	BAT *b, *u;
	gdk_return ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	}
	ret = BATappend(b, u, FALSE);
	BBPunfix(u->batCacheid);
	if (ret != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	if( b->batPersistence == PERSISTENT)
		BATmsync(b);
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCappend_val_wrap(bat *r, const bat *bid, const void *u)
{
	BAT *b;

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
	if (BUNappend(b, u, FALSE) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

char *
BKCappend_force_wrap(bat *r, const bat *bid, const bat *uid, const bit *force)
{
	BAT *b, *u;
	gdk_return ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", RUNTIME_OBJECT_MISSING);
	}
	if (BATcount(u) == 0) {
		ret = GDK_SUCCEED;
	} else {
		if ((b = setaccess(b, BAT_WRITE)) == NULL)
			throw(MAL, "bat.append", OPERATION_FAILED);
		ret = BATappend(b, u, *force);
	}
	BBPunfix(u->batCacheid);
	if (ret != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	if( b->batPersistence == PERSISTENT)
		BATmsync(b);
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCappend_val_force_wrap(bat *r, const bat *bid, const void *u, const bit *force)
{
	BAT *b;

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
	if (BUNappend(b, u, *force) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbun_inplace(bat *r, const bat *bid, const oid *id, const void *t)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	if (void_inplace(b, *id, t, FALSE) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbun_inplace_force(bat *r, const bat *bid, const oid *id, const void *t, const bit *force)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	if (void_inplace(b, *id, t, *force) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	return MAL_SUCCEED;
}


str
BKCbat_inplace_force(bat *r, const bat *bid, const bat *rid, const bat *uid, const bit *force)
{
	BAT *b, *p, *u;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	if ((p = BATdescriptor(*rid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	}
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(p->batCacheid);
		throw(MAL, "bat.inplace", RUNTIME_OBJECT_MISSING);
	}
	if (void_replace_bat(b, p, u, *force) == BUN_NONE) {
		BBPunfix(b->batCacheid);
		BBPunfix(p->batCacheid);
		BBPunfix(u->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	BBPkeepref(*r = b->batCacheid);
	BBPunfix(p->batCacheid);
	BBPunfix(u->batCacheid);
	return MAL_SUCCEED;
}

str
BKCbat_inplace(bat *r, const bat *bid, const bat *rid, const bat *uid)
{
	bit F = FALSE;

	return BKCbat_inplace_force(r, bid, rid, uid, &F);
}

/*end of SQL enhancement */

str
BKCgetCapacity(lng *res, const bat *bid)
{
	*res = lng_nil;
	if (BBPcheck(*bid, "bat.getCapacity")) {
		BAT *b = BBPquickdesc(*bid, 0);

		if (b != NULL)
			*res = (lng) BATcapacity(b);
	}
	return MAL_SUCCEED;
}

str
BKCgetColumnType(str *res, const bat *bid)
{
	const char *ret = str_nil;

	if (BBPcheck(*bid, "bat.getColumnType")) {
		BAT *b = BBPquickdesc(*bid, 0);

		if (b) {
			ret = *bid < 0 ? ATOMname(TYPE_void) : ATOMname(b->ttype);
		}
	}
	*res = GDKstrdup(ret);
	return MAL_SUCCEED;
}

str
BKCgetRole(str *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getRole", RUNTIME_OBJECT_MISSING);
	}
	*res = GDKstrdup(b->tident);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetkey(bat *res, const bat *bid, const bit *param)
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
BKCisSorted(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", RUNTIME_OBJECT_MISSING);
	}
	*res = BATordered(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCisSortedReverse(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", RUNTIME_OBJECT_MISSING);
	}
	*res = BATordered_rev(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * We must take care of the special case of a nil column (TYPE_void,seqbase=nil)
 * such nil columns never set tkey
 * a nil column of a BAT with <= 1 entries does not contain doubles => return TRUE.
 */

str
BKCgetKey(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) 
		throw(MAL, "bat.setPersistence", RUNTIME_OBJECT_MISSING);
	if (BATcount(b) <= 1) {
		*ret = TRUE;
	} else {
		if (!b->tkey) {
			BATderiveTailProps(b, 1);
		}
		*ret = b->tkey ? TRUE : FALSE;
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCpersists(void *r, const bat *bid, const bit *flg)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", RUNTIME_OBJECT_MISSING);
	}
	if (BATmode(b, (*flg == TRUE) ? PERSISTENT : TRANSIENT) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.setPersistence", ILLEGAL_ARGUMENT);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetPersistent(void *r, const bat *bid)
{
	bit flag= TRUE;
	return BKCpersists(r, bid, &flag);
}

str
BKCisPersistent(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", RUNTIME_OBJECT_MISSING);
	}
	*res = (b->batPersistence == PERSISTENT) ? TRUE :FALSE;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetTransient(void *r, const bat *bid)
{
	bit flag = FALSE;
	return BKCpersists(r, bid, &flag);
}

str
BKCisTransient(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setTransient", RUNTIME_OBJECT_MISSING);
	}
	*res = b->batPersistence == TRANSIENT;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetAccess(bat *res, const bat *bid, const char * const *param)
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
BKCgetAccess(str *res, const bat *bid)
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
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Property management
 * All property operators should ensure exclusive access to the BAT
 * descriptor.
 * Where necessary use the primary view to access the properties
 */
str
BKCinfo(bat *ret1, bat *ret2, const bat *bid)
{
	const char *mode, *accessmode;
	BAT *bk = NULL, *bv= NULL, *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getInfo", RUNTIME_OBJECT_MISSING);
	}

	bk = COLnew(0, TYPE_str, 128, TRANSIENT);
	bv = COLnew(0, TYPE_str, 128, TRANSIENT);
	if (bk == NULL || bv == NULL) {
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getInfo", MAL_MALLOC_FAIL);
	}

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

	if (BUNappend(bk, "batId", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, BATgetId(b), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batCacheid", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->batCacheid)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tparentid", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->theap.parentid)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batSharecnt", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->batSharecnt)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batCount", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa((size_t)b->batCount), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batCapacity", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa((size_t)b->batCapacity), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "head", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, ATOMname(TYPE_void), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tail", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, ATOMname(b->ttype), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batPersistence", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, mode, FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batRestricted", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, accessmode, FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batRefcnt", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(BBP_refs(b->batCacheid))), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batLRefcnt", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(BBP_lrefs(b->batCacheid))), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batDirty", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, BATdirty(b) ? "dirty" : "clean", FALSE) != GDK_SUCCEED ||

	    BUNappend(bk, "hseqbase", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, oidtostr(b->hseqbase), FALSE) != GDK_SUCCEED ||

	    BUNappend(bk, "tident", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, b->tident, FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tdense", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(BATtdense(b))), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tseqbase", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, oidtostr(b->tseqbase), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tsorted", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)BATtordered(b)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "trevsorted", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)BATtrevordered(b)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tkey", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->tkey)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tvarsized", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->tvarsized)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "talign", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->talign), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnosorted", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnosorted), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnorevsorted", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnorevsorted), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnodense", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnodense), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnokey[0]", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnokey[0]), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnokey[1]", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnokey[1]), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnonil", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnonil), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "tnil", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->tnil), FALSE) != GDK_SUCCEED ||

	    BUNappend(bk, "batInserted", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->batInserted), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "ttop", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_utoa(b->theap.free), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batStamp", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->batStamp)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "lastUsed", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(BBP_lastused(b->batCacheid))), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "curStamp", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(BBPcurstamp())), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batCopiedtodisk", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, local_itoa((ssize_t)(b->batCopiedtodisk)), FALSE) != GDK_SUCCEED ||
	    BUNappend(bk, "batDirtydesc", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, b->batDirtydesc ? "dirty" : "clean", FALSE) != GDK_SUCCEED ||

	    BUNappend(bk, "theap.dirty", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, b->theap.dirty ? "dirty" : "clean", FALSE) != GDK_SUCCEED ||
		infoHeap(bk, bv, &b->theap, "tail.") != GDK_SUCCEED ||

	    BUNappend(bk, "tvheap->dirty", FALSE) != GDK_SUCCEED ||
	    BUNappend(bv, (b->tvheap && b->tvheap->dirty) ? "dirty" : "clean", FALSE) != GDK_SUCCEED ||
		infoHeap(bk, bv, b->tvheap, "theap.") != GDK_SUCCEED ||

		/* dump index information */
		(b->thash &&
		 HASHinfo(bk, bv, b->thash, "thash->") != GDK_SUCCEED)) {
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.getInfo", MAL_MALLOC_FAIL);
	}

	assert(BATcount(bk) == BATcount(bv));

	BBPunfix(*bid);
	BBPkeepref(*ret1 = bk->batCacheid);
	BBPkeepref(*ret2 = bv->batCacheid);
	return MAL_SUCCEED;
}

// get the actual size of all constituents, also for views
#define ROUND_UP(x,y) ((y)*(((x)+(y)-1)/(y)))

str
BKCgetSize(lng *tot, const bat *bid){
	BAT *b;
	lng size = 0;
	lng blksize = (lng) MT_pagesize();
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getDiskSize", RUNTIME_OBJECT_MISSING);
	}

	size = sizeof (bat);
	if ( !isVIEW(b)) {
		BUN cnt = BATcapacity(b);
		size += ROUND_UP(b->theap.free, blksize);
		if (b->tvheap)
			size += ROUND_UP(b->tvheap->free, blksize);
		if (b->thash)
			size += ROUND_UP(sizeof(BUN) * cnt, blksize);
		size += IMPSimprintsize(b);
	} 
	*tot = size;
	BBPunfix(*bid);
	return MAL_SUCCEED;
}

/*
 * Synced BATs
 */
str
BKCisSynced(bit *ret, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2;

	if ((b1 = BATdescriptor(*bid1)) == NULL) {
		throw(MAL, "bat.isSynced", RUNTIME_OBJECT_MISSING);
	}
	if ((b2 = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(b1->batCacheid);
		throw(MAL, "bat.isSynced", RUNTIME_OBJECT_MISSING);
	}
	*ret = ALIGNsynced(b1, b2) != 0;
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Role Management
 */
str
BKCsetColumn(void *r, const bat *bid, const char * const *tname)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setColumn", RUNTIME_OBJECT_MISSING);
	}
	if (tname == 0 || *tname == 0 || **tname == 0){
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.setColumn", ILLEGAL_ARGUMENT " Column name missing");
	}
	BATroles(b, *tname);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetName(void *r, const bat *bid, const char * const *s)
{
	BAT *b;
	int ret;
	int c;
	const char *t = *s;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.setName", RUNTIME_OBJECT_MISSING);

	for ( ; (c = *t) != 0; t++)
		if (c != '_' && !GDKisalnum(c))
			throw(MAL, "bat.setName", ILLEGAL_ARGUMENT ": identifier expected: %s", *s);

	t = *s;
	ret = BATname(b, t);
	BBPunfix(b->batCacheid);
	switch (ret) {
	case BBPRENAME_ILLEGAL:
		throw(MAL, "bat.setName", ILLEGAL_ARGUMENT ": illegal temporary name: '%s'", t);
	case BBPRENAME_LONG:
		throw(MAL, "bat.setName", ILLEGAL_ARGUMENT ": name too long: '%s'", t);
	case BBPRENAME_ALREADY:
	case 0:
		break;
	}
	return MAL_SUCCEED;
}

str
BKCgetBBPname(str *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getName", RUNTIME_OBJECT_MISSING);
	}
	*ret = GDKstrdup(BBPname(b->batCacheid));
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsave(bit *res, const char * const *input)
{
	bat bid = BBPindex(*input);
	BAT *b;

	*res = FALSE;
	if (bid) {
		BBPfix(bid);
		b = BBP_cache(bid);
		if (b && BATdirty(b)) {
			if (BBPsave(b) == GDK_SUCCEED)
				*res = TRUE;
		}
		BBPunfix(bid);
	}
	return MAL_SUCCEED;
}

str
BKCsave2(void *r, const bat *bid)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.save", RUNTIME_OBJECT_MISSING);
	}
	if ( b->batPersistence != TRANSIENT){
		throw(MAL, "bat.save", "Only save transient columns.");
	}

	if (b && BATdirty(b))
		BBPsave(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Accelerator Control
 */
str
BKCsetHash(bit *ret, const bat *bid)
{
	BAT *b;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setHash", RUNTIME_OBJECT_MISSING);
	}
	*ret = BAThash(b, 0) == GDK_SUCCEED;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCsetImprints(bit *ret, const bat *bid)
{
	BAT *b;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setImprints", RUNTIME_OBJECT_MISSING);
	}
	*ret = BATimprints(b) == GDK_SUCCEED;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
BKCgetSequenceBase(oid *r, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setSequenceBase", RUNTIME_OBJECT_MISSING);
	}
	*r = b->hseqbase;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Shrinking a void-headed BAT using a list of oids to ignore.
 */
#define shrinkloop(Type)							\
	do {											\
		Type *p = (Type*)Tloc(b, 0);				\
		Type *q = (Type*)Tloc(b, BUNlast(b));		\
		Type *r = (Type*)Tloc(bn, 0);				\
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
BKCshrinkBAT(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	BUN cnt =0;
	oid oidx = 0, *o, *ol;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrink", RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.shrink", RUNTIME_OBJECT_MISSING);
	}
	bn= COLnew(0, b->ttype, BATcount(b) - BATcount(d) , TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.shrink", MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, 0, 0);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.shrink", MAL_MALLOC_FAIL );
	}

	o = (oid*)Tloc(bs, 0);
	ol= (oid*)Tloc(bs, BUNlast(bs));

	switch(ATOMstorage(b->ttype) ){
	case TYPE_bte: shrinkloop(bte); break;
	case TYPE_sht: shrinkloop(sht); break;
	case TYPE_int: shrinkloop(int); break;
	case TYPE_lng: shrinkloop(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: shrinkloop(hge); break;
#endif
	case TYPE_flt: shrinkloop(flt); break;
	case TYPE_dbl: shrinkloop(dbl); break;
	case TYPE_oid: shrinkloop(oid); break;
	default:
		if (ATOMvarsized(bn->ttype)) {
			BUN p = 0;
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
			switch( b->twidth){
			case 1:shrinkloop(bte); break;
			case 2:shrinkloop(sht); break;
			case 4:shrinkloop(int); break;
			case 8:shrinkloop(lng); break;
#ifdef HAVE_HGE
			case 16:shrinkloop(hge); break;
#endif
			default:
				throw(MAL, "bat.shrink", "Illegal argument type");
			}
		}
	}

	BATsetcount(bn, cnt);
	bn->tsorted = 0;
	bn->trevsorted = 0;
	bn->tdense = 0;
	bn->tkey = b->tkey;
	bn->tnonil = b->tnonil;
	bn->tnil = b->tnil;

	if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCshrinkBATmap(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	oid lim,oidx = 0, *o, *ol;
	oid *r;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}

	bn= COLnew(b->hseqbase, TYPE_oid, BATcount(b) , TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, 0, 0);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}

	o = (oid*)Tloc(bs, 0);
	ol= (oid*)Tloc(bs, BUNlast(bs));
	r = (oid*)Tloc(bn, 0);

	lim = BATcount(b);

	for (;oidx<lim; oidx++) {
		if ( o < ol && *o == oidx ){
			o++;
		} else {
			*r++ = oidx;
		}
	}

    BATsetcount(bn, BATcount(b)-BATcount(bs));
    bn->tsorted = 0;
    bn->trevsorted = 0;
    bn->tdense = 0;

    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}
/*
 * Shrinking a void-headed BAT using a list of oids to ignore.
 */
#define reuseloop(Type)								\
	do {											\
		Type *p = (Type*)Tloc(b, 0);				\
		Type *q = (Type*)Tloc(b, BUNlast(b));		\
		Type *r = (Type*)Tloc(bn, 0);				\
		for (;p<q; oidx++, p++) {					\
			if ( *o == oidx ){						\
				while ( ol>o && ol[-1] == bidx) {	\
					bidx--;							\
					q--;							\
					ol--;							\
				}									\
				*r++ = *(--q);						\
				o += (o < ol);						\
				bidx--;								\
			} else									\
				*r++ = *p;							\
		}											\
	} while (0)

str
BKCreuseBAT(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	oid oidx = 0, bidx, *o, *ol;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.reuse", RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.reuse", RUNTIME_OBJECT_MISSING);
	}
	bn= COLnew(b->hseqbase, b->ttype, BATcount(b) - BATcount(d), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.reuse", MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, 0, 0);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.reuse", MAL_MALLOC_FAIL );
	}

	oidx = b->hseqbase;
	bidx = oidx + BATcount(b)-1;
	o = (oid*)Tloc(bs, 0);
	ol= (oid*)Tloc(bs, BUNlast(bs));

	switch(ATOMstorage(b->ttype) ){
	case TYPE_bte: reuseloop(bte); break;
	case TYPE_sht: reuseloop(sht); break;
	case TYPE_int: reuseloop(int); break;
	case TYPE_lng: reuseloop(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: reuseloop(hge); break;
#endif
	case TYPE_flt: reuseloop(flt); break;
	case TYPE_dbl: reuseloop(dbl); break;
	case TYPE_oid: reuseloop(oid); break;
	case TYPE_str: /* to be done based on its index width */
	default:
		if (ATOMvarsized(bn->ttype)) {
			BUN p = 0;
			BUN q = BUNlast(b);
			BATiter bi = bat_iterator(b);

			for (;p<q; oidx++, p++) {
				if ( *o == oidx ){
					while ( ol > o && ol[-1] == bidx) {
						bidx--;
						q--;
						ol--;
					}
					BUNappend(bn, BUNtail(bi, --q), FALSE);
					o += (o < ol);
					bidx--;
				} else {
					BUNappend(bn, BUNtail(bi, p), FALSE);
				}
			}
		} else {
			switch( b->twidth){
			case 1:reuseloop(bte); break;
			case 2:reuseloop(sht); break;
			case 4:reuseloop(int); break;
			case 8:reuseloop(lng); break;
#ifdef HAVE_HGE
			case 16:reuseloop(hge); break;
#endif
			default:
				throw(MAL, "bat.shrink", "Illegal argument type");
			}
		}
	}

    BATsetcount(bn, BATcount(b) - BATcount(bs));
    bn->tsorted = 0;
    bn->trevsorted = 0;
    bn->tdense = 0;
	bn->tkey = b->tkey;

    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCreuseBATmap(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	oid bidx, oidx = 0, *o, *ol;
	oid *r;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.shrinkMap", RUNTIME_OBJECT_MISSING);
	}
	bn= COLnew(b->hseqbase, TYPE_oid, BATcount(b) - BATcount(d), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, 0, 0);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.shrinkMap", MAL_MALLOC_FAIL );
	}

	oidx = b->hseqbase;
	bidx = oidx + BATcount(b)-1;
	o  = (oid*)Tloc(bs, 0);
	ol = (oid*)Tloc(bs, BUNlast(bs));
	r  = (oid*)Tloc(bn, 0);

	for (; oidx <= bidx; oidx++) {
		if ( *o == oidx ){
			while ( ol > o && ol[-1] == bidx) {
				bidx--;
				ol--;
			}
			*r++ = bidx;
			o += (o < ol);
			bidx--;
		} else {
			*r++ = oidx;
		}
	}

    BATsetcount(bn, BATcount(b)-BATcount(bs));
    bn->tsorted = 0;
    bn->trevsorted = 0;
    bn->tdense = 0;

    if (!(bn->batDirty&2)) BATsetaccess(bn, BAT_READ);

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

str
BKCmergecand(bat *ret, const bat *aid, const bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.mergecand", RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPunfix(a->batCacheid);
		throw(MAL, "bat.mergecand", RUNTIME_OBJECT_MISSING);
	}
	bn = BATmergecand(a, b);
	BBPunfix(a->batCacheid);
	BBPunfix(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.mergecand", OPERATION_FAILED);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
BKCintersectcand(bat *ret, const bat *aid, const bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.intersectcand", RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPunfix(a->batCacheid);
		throw(MAL, "bat.intersectcand", RUNTIME_OBJECT_MISSING);
	}
	bn = BATintersectcand(a, b);
	BBPunfix(a->batCacheid);
	BBPunfix(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.intersectcand", OPERATION_FAILED);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}
