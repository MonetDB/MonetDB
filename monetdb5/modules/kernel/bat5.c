/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * Peter Boncz, M.L. Kersten
 * Binary Association Tables
 * This module contains the commands and patterns to manage Binary
 * Association Tables (BATs). The relational operations you can execute
 * on BATs have the form of a neat algebra, described in algebra.c
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
#include "mal_debugger.h"

/* set access mode to bat, replacing input with output */
static BAT *
setaccess(BAT *b, restrict_t mode)
{
	BAT *bn = b;

	if (BATsetaccess(b, mode) != GDK_SUCCEED) {
		if (b->batSharecnt && mode != BAT_READ) {
			bn = COLcopy(b, b->ttype, true, TRANSIENT);
			if (bn != NULL &&
				BATsetaccess(bn, mode) != GDK_SUCCEED) {
				BBPreclaim(bn);
				bn = NULL;
			}
		} else {
			bn = NULL;
		}
		BBPunfix(b->batCacheid);
	}
	return bn;
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
BKCnewBAT(bat *res, const int *tt, const BUN *cap, role_t role)
{
	BAT *bn;

	bn = COLnew(0, *tt, *cap, role);
	if (bn == NULL)
		throw(MAL, "bat.new", GDK_EXCEPTION);
	*res = bn->batCacheid;
	BATsettrivprop(bn);
	BBPretain(bn->batCacheid);
	BBPunfix(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCattach(bat *ret, const int *tt, const char * const *heapfile)
{
	BAT *bn;

	bn = BATattach(*tt, *heapfile, TRANSIENT);
	if (bn == NULL)
		throw(MAL, "bat.attach", GDK_EXCEPTION);
	if( !bn->batTransient)
		BATmsync(bn);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

static str
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

	*ret = 0;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.mirror", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATdense(b->hseqbase, b->hseqbase, BATcount(b));
	BBPunfix(b->batCacheid);
	if (bn == NULL) {
		throw(MAL, "bat.mirror", GDK_EXCEPTION);
	}
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

static str
BKCdelete(bat *r, const bat *bid, const oid *h)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BUNdelete(b, *h) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCdelete_multi(bat *r, const bat *bid, const bat *sid)
{
	BAT *b, *s;
	gdk_return ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	ret = BATdel(b, s);
	BBPunfix(s->batCacheid);
	if (ret != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCdelete_all(bat *r, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BATclear(b, false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	if( !b->batTransient)
		BATmsync(b);
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCappend_cand_force_wrap(bat *r, const bat *bid, const bat *uid, const bat *sid, const bit *force)
{
	BAT *b, *u, *s = NULL;
	gdk_return ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(u->batCacheid);
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	ret = BATappend(b, u, s, force ? *force : false);
	BBPunfix(u->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (ret != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	if( !b->batTransient)
		BATmsync(b);
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCappend_cand_wrap(bat *r, const bat *bid, const bat *uid, const bat *sid)
{
	return BKCappend_cand_force_wrap(r, bid, uid, sid, NULL);
}

static str
BKCappend_wrap(bat *r, const bat *bid, const bat *uid)
{
	return BKCappend_cand_force_wrap(r, bid, uid, NULL, NULL);
}

static str
BKCappend_force_wrap(bat *r, const bat *bid, const bat *uid, const bit *force)
{
	return BKCappend_cand_force_wrap(r, bid, uid, NULL, force);
}

static str
BKCappend_val_force_wrap(bat *r, const bat *bid, const void *u, const bit *force)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((b = setaccess(b, BAT_WRITE)) == NULL)
		throw(MAL, "bat.append", OPERATION_FAILED);
	if (b->ttype >= TYPE_str && ATOMstorage(b->ttype) >= TYPE_str) {
		if (u == 0 || *(str*)u == 0)
			u = (ptr) str_nil;
		else
			u = (ptr) *(str *)u;
	}
	if (BUNappend(b, u, force ? *force : false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCappend_val_wrap(bat *r, const bat *bid, const void *u)
{
	return BKCappend_val_force_wrap(r, bid, u, NULL);
}

static str
BKCbun_inplace(bat *r, const bat *bid, const oid *id, const void *t)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (void_inplace(b, *id, t, false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCbun_inplace_force(bat *r, const bat *bid, const oid *id, const void *t, const bit *force)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (void_inplace(b, *id, t, *force) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}


static str
BKCbat_inplace_force(bat *r, const bat *bid, const bat *rid, const bat *uid, const bit *force)
{
	BAT *b, *p, *u;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((p = BATdescriptor(*rid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(p->batCacheid);
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (BATreplace(b, p, u, *force) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(p->batCacheid);
		BBPunfix(u->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(p->batCacheid);
	BBPunfix(u->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCbat_inplace(bat *r, const bat *bid, const bat *rid, const bat *uid)
{
	bit F = FALSE;

	return BKCbat_inplace_force(r, bid, rid, uid, &F);
}

/*end of SQL enhancement */

static str
BKCgetCapacity(lng *res, const bat *bid)
{
	*res = lng_nil;
	if (BBPcheck(*bid, "bat.getCapacity")) {
		BAT *b = BBPquickdesc(*bid, false);

		if (b != NULL)
			*res = (lng) BATcapacity(b);
	}
	return MAL_SUCCEED;
}

static str
BKCgetColumnType(str *res, const bat *bid)
{
	const char *ret = str_nil;

	if (BBPcheck(*bid, "bat.getColumnType")) {
		BAT *b = BBPquickdesc(*bid, false);

		if (b) {
			ret = *bid < 0 ? ATOMname(TYPE_void) : ATOMname(b->ttype);
		}
	}
	*res = GDKstrdup(ret);
	if(*res == NULL)
		throw(MAL,"bat.getColumnType", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
BKCgetRole(str *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getRole", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*res = GDKstrdup(b->tident);
	BBPunfix(b->batCacheid);
	if(*res == NULL)
		throw(MAL,"bat.getRole", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
BKCisSorted(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*res = BATordered(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCisSortedReverse(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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

static str
BKCgetKey(bit *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.setPersistence", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	*ret = BATkeyed(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCpersists(void *r, const bat *bid, const bit *flg)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (BATmode(b, (*flg != TRUE)) != GDK_SUCCEED) {
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

static str
BKCisPersistent(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*res = !b->batTransient;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCsetTransient(void *r, const bat *bid)
{
	bit flag = FALSE;
	return BKCpersists(r, bid, &flag);
}

static str
BKCisTransient(bit *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setTransient", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*res = b->batTransient;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCsetAccess(bat *res, const bat *bid, const char * const *param)
{
	BAT *b;
	restrict_t m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.setAccess", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.setAccess", ILLEGAL_ARGUMENT " Got %c" " expected 'r','a', or 'w'", *param[0]);
	}
	if ((b = setaccess(b, m)) == NULL)
		throw(MAL, "bat.setAccess", OPERATION_FAILED);
	*res = b->batCacheid;
	BATsettrivprop(b);
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCgetAccess(str *res, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getAccess", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
	}
	BBPunfix(b->batCacheid);
	if(*res == NULL)
		throw(MAL,"bat.getAccess", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/*
 * Property management
 * All property operators should ensure exclusive access to the BAT
 * descriptor.
 * Where necessary use the primary view to access the properties
 */
static str
BKCinfo(bat *ret1, bat *ret2, const bat *bid)
{
	BAT *bv, *bk;
	str msg;

	if ((msg = BATinfo(&bk, &bv, *bid)) != NULL)
		return msg;
	BBPkeepref(*ret1 = bk->batCacheid);
	BBPkeepref(*ret2 = bv->batCacheid);
	return MAL_SUCCEED;
}

// get the actual size of all constituents, also for views
#define ROUND_UP(x,y) ((y)*(((x)+(y)-1)/(y)))

static str
BKCgetSize(lng *tot, const bat *bid){
	BAT *b;
	lng size = 0;
	lng blksize = (lng) MT_pagesize();
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getDiskSize", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	size = sizeof (bat);
	if ( !isVIEW(b)) {
		BUN cnt = BATcapacity(b);
		size += ROUND_UP(b->theap->free, blksize);
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
static str
BKCisSynced(bit *ret, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2;

	if ((b1 = BATdescriptor(*bid1)) == NULL) {
		throw(MAL, "bat.isSynced", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((b2 = BATdescriptor(*bid2)) == NULL) {
		BBPunfix(b1->batCacheid);
		throw(MAL, "bat.isSynced", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*ret = ALIGNsynced(b1, b2) != 0;
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Role Management
 */
static str
BKCsetColumn(void *r, const bat *bid, const char * const *tname)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setColumn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (tname == 0 || *tname == 0 || **tname == 0){
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.setColumn", ILLEGAL_ARGUMENT " Column name missing");
	}
	if (BATroles(b, *tname) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.setColumn", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
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
		throw(MAL, "bat.setName", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	for ( ; (c = *t) != 0; t++)
		if (c != '_' && !GDKisalnum(c)) {
			BBPunfix(b->batCacheid);
			throw(MAL, "bat.setName", ILLEGAL_ARGUMENT ": identifier expected: %s", *s);
		}

	t = *s;
	ret = BBPrename(b->batCacheid, t);
	BBPunfix(b->batCacheid);
	switch (ret) {
	case BBPRENAME_ILLEGAL:
		GDKclrerr();
		throw(MAL, "bat.setName", ILLEGAL_ARGUMENT ": illegal temporary name: '%s'", t);
	case BBPRENAME_LONG:
		GDKclrerr();
		throw(MAL, "bat.setName", ILLEGAL_ARGUMENT ": name too long: '%s'", t);
	case BBPRENAME_MEMORY:
		GDKclrerr();
		throw(MAL, "bat.setName", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	case BBPRENAME_ALREADY:
		GDKclrerr();
		/* fall through */
	case 0:
		break;
	}
	return MAL_SUCCEED;
}

static str
BKCgetBBPname(str *ret, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getName", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*ret = GDKstrdup(BBPname(b->batCacheid));
	BBPunfix(b->batCacheid);
	return *ret ? MAL_SUCCEED : createException(MAL, "bat.getName", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
BKCsave(bit *res, const char * const *input)
{
	bat bid = BBPindex(*input);
	BAT *b;

	*res = FALSE;
	if (!is_bat_nil(bid)) {
		if (BBPfix(bid) > 0) {
			b = BBP_cache(bid);
			if (b && BATdirty(b)) {
				if (BBPsave(b) == GDK_SUCCEED)
					*res = TRUE;
			}
			BBPunfix(bid);
			return MAL_SUCCEED;
		}
		throw(MAL, "bat.save", "fix failed");
	}
	return MAL_SUCCEED;
}

static str
BKCsave2(void *r, const bat *bid)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.save", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ( !b->batTransient){
		BBPunfix(b->batCacheid);
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
static str
BKCsetHash(bit *ret, const bat *bid)
{
	BAT *b;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setHash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*ret = BAThash(b) == GDK_SUCCEED;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCsetImprints(bit *ret, const bat *bid)
{
	BAT *b;

	(void) ret;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setImprints", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*ret = BATimprints(b) == GDK_SUCCEED;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCgetSequenceBase(oid *r, const bat *bid)
{
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setSequenceBase", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		throw(MAL, "bat.shrink", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.shrink", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn= COLnew(0, b->ttype, BATcount(b) - BATcount(d), b->batRole);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.shrink", SQLSTATE(HY013) MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, false, false, false);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.shrink", SQLSTATE(HY013) MAL_MALLOC_FAIL );
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
					if (BUNappend(bn, BUNtail(bi, p), false) != GDK_SUCCEED) {
						BBPunfix(b->batCacheid);
						BBPunfix(bn->batCacheid);
						throw(MAL, "bat.shrink", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					}
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
				BBPunfix(b->batCacheid);
				BBPunfix(bn->batCacheid);
				throw(MAL, "bat.shrink", "Illegal argument type");
			}
		}
	}

	BATsetcount(bn, cnt);
	bn->tsorted = false;
	bn->trevsorted = false;
	bn->tseqbase = oid_nil;
	bn->tkey = b->tkey;
	bn->tnonil = b->tnonil;
	bn->tnil = false;		/* can't be sure if values deleted */

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

#if 0
str
BKCshrinkBATmap(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	oid lim,oidx = 0, *o, *ol;
	oid *r;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	bn= COLnew(b->hseqbase, TYPE_oid, BATcount(b) , TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY013) MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, false, false, false);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY013) MAL_MALLOC_FAIL );
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
    bn->tsorted = false;
    bn->trevsorted = false;
	bn->tseqbase = oid_nil;


	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}
#endif	/* unused */

/*
 * Shrinking a void-headed BAT using a list of oids to ignore.
 */
#define reuseloop(Type)										\
	do {													\
		Type *dst = (Type *) Tloc(bn, 0);					\
		const Type *src = (const Type *) Tloc(b, 0);		\
		for (BUN p = 0; p < b->batCount; p++, src++) {		\
			if (o < ol && b->hseqbase + p == *o) {			\
				do											\
					o++;									\
				while (o < ol && b->hseqbase + p == *o);	\
			} else {										\
				*dst++ = *src;								\
				n++;										\
			}												\
		}													\
	} while (0)

str
BKCreuseBAT(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.reuse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.reuse", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn= COLnew(b->hseqbase, b->ttype, BATcount(b) - BATcount(d), b->batRole);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.reuse", SQLSTATE(HY013) MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, false, false, false);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.reuse", SQLSTATE(HY013) MAL_MALLOC_FAIL );
	}

	const oid *o = (const oid *) Tloc(bs, 0);
	const oid *ol = o + bs->batCount;
	while (o < ol && *o < b->hseqbase)
		o++;
	if (b->tvarsized) {
		BATiter bi = bat_iterator(b);
		for (BUN p = 0; p < b->batCount; p++) {
			if (o < ol && b->hseqbase + p == *o) {
				do
					o++;
				while (o < ol && b->hseqbase + p == *o);
			} else if (BUNappend(bn, BUNtail(bi, p), false) != GDK_SUCCEED) {
				BBPunfix(bn->batCacheid);
				BBPunfix(b->batCacheid);
				BBPunfix(bs->batCacheid);
				throw(MAL, "bat.shrink", GDK_EXCEPTION);
			}
		}
	} else {
		BUN n = 0;
		switch (b->twidth) {
		case 1:
			reuseloop(bte);
			break;
		case 2:
			reuseloop(sht);
			break;
		case 4:
			reuseloop(int);
			break;
		case 8:
			reuseloop(lng);
			break;
#ifdef HAVE_HGE
		case 16:
			reuseloop(hge);
			break;
#endif
		default: {
			char *dst = (char *) Tloc(bn, 0);
			const char *src = (const char *) Tloc(b, 0);
			for (BUN p = 0; p < b->batCount; p++) {
				if (o < ol && b->hseqbase + p == *o) {
					do
						o++;
					while (o < ol && b->hseqbase + p == *o);
				} else {
					memcpy(dst, src, b->twidth);
					dst += b->twidth;
					n++;
				}
				src += b->twidth;
			}
			break;
		}
		}
		BATsetcount(bn, n);
		bn->tkey = b->tkey;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->tnonil = b->tnonil;
		bn->tnil = false;		/* can't be sure if values deleted */
	}

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCreuseBATmap(bat *ret, const bat *bid, const bat *did)
{
	BAT *b, *d, *bn, *bs;
	gdk_return res;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((d = BATdescriptor(*did)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn= COLnew(b->hseqbase, TYPE_oid, BATcount(b) - BATcount(d), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(d->batCacheid);
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY013) MAL_MALLOC_FAIL );
	}
	res = BATsort(&bs, NULL, NULL, d, NULL, NULL, false, false, false);
	BBPunfix(d->batCacheid);
	if (res != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "bat.shrinkMap", SQLSTATE(HY013) MAL_MALLOC_FAIL );
	}

	const oid *o = (const oid *) Tloc(bs, 0);
	const oid *ol = o + bs->batCount;
	while (o < ol && *o < b->hseqbase)
		o++;
	oid *dst = (oid *) Tloc(bn, 0);
	BUN n = 0;
	for (BUN p = 0; p < b->batCount; p++) {
		if (o < ol && b->hseqbase + p == *o) {
			do
				o++;
			while (o < ol && b->hseqbase + p == *o);
		} else {
			*dst++ = b->hseqbase + p;
			n++;
		}
	}
	BATsetcount(bn, n);
	bn->tkey = true;
	bn->tsorted = true;
	bn->trevsorted = n <= 1;
	bn->tnil = false;
	bn->tnonil = true;
	bn->tseqbase = oid_nil;

	BBPunfix(b->batCacheid);
	BBPunfix(bs->batCacheid);
	BBPkeepref(*ret= bn->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCmergecand(bat *ret, const bat *aid, const bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.mergecand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPunfix(a->batCacheid);
		throw(MAL, "bat.mergecand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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

static str
BKCintersectcand(bat *ret, const bat *aid, const bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.intersectcand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPunfix(a->batCacheid);
		throw(MAL, "bat.intersectcand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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

static str
BKCdiffcand(bat *ret, const bat *aid, const bat *bid)
{
	BAT *a, *b, *bn;

	if ((a = BATdescriptor(*aid)) == NULL) {
		throw(MAL, "bat.diffcand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		BBPunfix(a->batCacheid);
		throw(MAL, "bat.diffcand", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATdiffcand(a, b);
	BBPunfix(a->batCacheid);
	BBPunfix(b->batCacheid);
	if (bn == NULL)
		throw(MAL, "bat.diffcand", OPERATION_FAILED);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func bat5_init_funcs[] = {
 command("bat", "mirror", BKCmirror, false, "Returns the head-mirror image of a BAT (two head columns).", args(1,2, batarg("",oid),batargany("b",2))),
 command("bat", "delete", BKCdelete, false, "Delete BUN indicated by head value, exchanging with last BUN", args(1,3, batargany("",1),batargany("b",1),arg("h",oid))),
 command("bat", "delete", BKCdelete_multi, false, "Delete multiple BUN, shifting BUNs up", args(1,3, batargany("",1),batargany("b",1),batarg("d",oid))),
 command("bat", "delete", BKCdelete_all, false, "Delete all entries.", args(1,2, batargany("",1),batargany("b",1))),
 command("bat", "replace", BKCbun_inplace, false, "Replace the tail value of one BUN that has some head value.", args(1,4, batargany("",1),batargany("b",1),arg("h",oid),argany("t",1))),
 command("bat", "replace", BKCbun_inplace_force, false, "Replace the tail value of one BUN that has some head value.", args(1,5, batargany("",1),batargany("b",1),arg("h",oid),argany("t",1),arg("force",bit))),
 command("bat", "replace", BKCbat_inplace, false, "Perform replace for all BUNs of the second BAT into the first.", args(1,4, batargany("",1),batargany("b",1),batarg("rid",oid),batargany("val",1))),
 command("bat", "replace", BKCbat_inplace_force, false, "Perform replace for all BUNs of the second BAT into the first.", args(1,5, batargany("",1),batargany("b",1),batarg("rid",oid),batargany("val",1),arg("force",bit))),
 command("bat", "append", BKCappend_wrap, false, "append the content of u to i", args(1,3, batargany("",1),batargany("i",1),batargany("u",1))),
 command("bat", "append", BKCappend_force_wrap, false, "append the content of u to i", args(1,4, batargany("",1),batargany("i",1),batargany("u",1),arg("force",bit))),
 command("bat", "append", BKCappend_cand_wrap, false, "append the content of u with candidate list s to i", args(1,4, batargany("",1),batargany("i",1),batargany("u",1),batarg("s",oid))),
 command("bat", "append", BKCappend_cand_force_wrap, false, "append the content of u with candidate list s to i", args(1,5, batargany("",1),batargany("i",1),batargany("u",1),batarg("s",oid),arg("force",bit))),
 command("bat", "append", BKCappend_val_force_wrap, false, "append the value u to i", args(1,4, batargany("",1),batargany("i",1),argany("u",1),arg("force",bit))),
 command("bat", "attach", BKCattach, false, "Returns a new BAT with dense head and tail of the given type and uses\nthe given file to initialize the tail. The file will be owned by the\nserver.", args(1,3, batargany("",1),arg("tt",int),arg("heapfile",str))),
 command("bat", "densebat", BKCdensebat, false, "Creates a new [void,void] BAT of size 'sz'.", args(1,2, batarg("",oid),arg("sz",lng))),
 command("bat", "info", BKCinfo, false, "Produce a table containing information about a BAT in [attribute,value] format. \nIt contains all properties of the BAT record. ", args(2,3, batarg("",str),batarg("",str),batargany("b",1))),
 command("bat", "getSize", BKCgetSize, false, "Calculate the actual size of the BAT descriptor, heaps, hashes and imprint indices in bytes\nrounded to the memory page size (see bbp.getPageSize()).", args(1,2, arg("",lng),batargany("b",1))),
 command("bat", "getCapacity", BKCgetCapacity, false, "Returns the current allocation size (in max number of elements) of a BAT.", args(1,2, arg("",lng),batargany("b",1))),
 command("bat", "getColumnType", BKCgetColumnType, false, "Returns the type of the tail column of a BAT, as an integer type number.", args(1,2, arg("",str),batargany("b",1))),
 command("bat", "getRole", BKCgetRole, false, "Returns the rolename of the head column of a BAT.", args(1,2, arg("",str),batargany("bid",1))),
 command("bat", "isaKey", BKCgetKey, false, "Return whether the column tail values are unique (key).", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setAccess", BKCsetAccess, false, "Try to change the update access priviliges \nto this BAT. Mode:\nr[ead-only]      - allow only read access.\na[append-only]   - allow reads and update.\nw[riteable]      - allow all operations.\nBATs are updatable by default. On making a BAT read-only, \nall subsequent updates fail with an error message.\nReturns the BAT itself.", args(1,3, batargany("",1),batargany("b",1),arg("mode",str))),
 command("bat", "getAccess", BKCgetAccess, false, "Return the access mode attached to this BAT as a character.", args(1,2, arg("",str),batargany("b",1))),
 command("bat", "getSequenceBase", BKCgetSequenceBase, false, "Get the sequence base for the void column of a BAT.", args(1,2, arg("",oid),batargany("b",1))),
 command("bat", "isSorted", BKCisSorted, false, "Returns true if BAT values are ordered.", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "isSortedReverse", BKCisSortedReverse, false, "Returns true if BAT values are reversely ordered.", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "append", BKCappend_val_wrap, false, "append the value u to i", args(1,3, batargany("",1),batargany("i",1),argany("u",1))),
 command("bat", "setName", BKCsetName, false, "Give a logical name to a BAT. ", args(1,3, arg("",void),batargany("b",1),arg("s",str))),
 command("bat", "getName", BKCgetBBPname, false, "Gives back the logical name of a BAT.", args(1,2, arg("",str),batargany("b",1))),
 command("bat", "setColumn", BKCsetColumn, false, "Give a logical name to the tail column of a BAT.", args(1,3, arg("",void),batargany("b",1),arg("t",str))),
 command("bat", "isTransient", BKCisTransient, false, "", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setTransient", BKCsetTransient, false, "Make the BAT transient.  Returns \nboolean which indicates if the\nBAT administration has indeed changed.", args(1,2, arg("",void),batargany("b",1))),
 command("bat", "isPersistent", BKCisPersistent, false, "", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setPersistent", BKCsetPersistent, false, "Make the BAT persistent.", args(1,2, arg("",void),batargany("b",1))),
 command("bat", "save", BKCsave2, false, "", args(1,2, arg("",void),batargany("nme",1))),
 command("bat", "save", BKCsave, false, "Save a BAT to storage, if it was loaded and dirty.  \nReturns whether IO was necessary.  Please realize that \ncalling this function violates the atomic commit protocol!!", args(1,2, arg("",bit),arg("nme",str))),
 command("bat", "setHash", BKCsetHash, false, "Create a hash structure on the column", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setImprints", BKCsetImprints, false, "Create an imprints structure on the column", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "isSynced", BKCisSynced, false, "Tests whether two BATs are synced or not. ", args(1,3, arg("",bit),batargany("b1",1),batargany("b2",2))),
 command("bat", "reuse", BKCreuseBAT, false, "Shuffle the values around to restore a dense representation of buns.", args(1,3, batargany("",1),batargany("b",1),batarg("del",oid))),
 command("bat", "reuseMap", BKCreuseBATmap, false, "Derive the oid mapping for reuse BAT based on list of to-be-deleted", args(1,3, batarg("",oid),batargany("b",1),batarg("del",oid))),
 command("bat", "mergecand", BKCmergecand, false, "Merge two candidate lists into one", args(1,3, batarg("",oid),batarg("a",oid),batarg("b",oid))),
 command("bat", "intersectcand", BKCintersectcand, false, "Intersect two candidate lists into one", args(1,3, batarg("",oid),batarg("a",oid),batarg("b",oid))),
 command("bat", "diffcand", BKCdiffcand, false, "Calculate difference of two candidate lists", args(1,3, batarg("",oid),batarg("a",oid),batarg("b",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_bat5_mal)
{ mal_module("bat5", NULL, bat5_init_funcs); }
