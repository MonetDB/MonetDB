/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

/*
 * The remainder contains the wrapper code over the mserver version 4
 * InformationFunctions
 * In most cases we pass a BAT identifier, which should be unified
 * with a BAT descriptor. Upon failure we can simply abort the function.
 *
 * The logical head type :oid is mapped to a TYPE_void
 * with sequenceBase. It represents the old fashioned :vid
 */


#define derefStr(b, v)							\
	do {										\
		int _tpe= ATOMstorage((b)->ttype);		\
		if (_tpe >= TYPE_str) {					\
			if ((v) == 0 || *(str*) (v) == 0)	\
				(v) = (str) str_nil;			\
			else								\
				(v) = *(str *) (v);				\
		}										\
	} while (0)

str
BKCnewBAT(bat *res, const int *tt, const BUN *cap, role_t role)
{
	BAT *bn;

	bn = COLnew(0, *tt, *cap, role);
	if (bn == NULL)
		throw(MAL, "bat.new", GDK_EXCEPTION);
	*res = bn->batCacheid;
	BBPretain(bn->batCacheid);
	BBPunfix(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCdensebat(Client ctx, bat *ret, const lng *size)
{
	(void) ctx;
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
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

str
BKCmirror(Client ctx, bat *ret, const bat *bid)
{
	(void) ctx;
	BAT *b, *bn;

	*ret = 0;
	if (!(b = BBPquickdesc(*bid)))
		throw(MAL, "bat.mirror", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (!(bn = BATdense(b->hseqbase, b->hseqbase, BATcount(b))))
		throw(MAL, "bat.mirror", GDK_EXCEPTION);
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

static str
BKCdelete(Client ctx, bat *r, const bat *bid, const oid *h)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BUNdelete(b, *h) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCdelete_multi(Client ctx, bat *r, const bat *bid, const bat *sid)
{
	(void) ctx;
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
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCdelete_all(Client ctx, bat *r, const bat *bid)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.delete", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BATclear(b, false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.delete", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCappend_cand_force_wrap(Client ctx, bat *r, const bat *bid, const bat *uid,
						  const bat *sid, const bit *force)
{
	(void) ctx;
	BAT *b, *u, *s = NULL;
	gdk_return ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (isVIEW(b)) {
		BAT *bn = COLcopy(b, b->ttype, true, TRANSIENT);
		MT_lock_set(&b->theaplock);
		restrict_t mode = b->batRestricted;
		MT_lock_unset(&b->theaplock);
		BBPunfix(b->batCacheid);
		if (bn == NULL || (b = BATsetaccess(bn, mode)) == NULL)
			throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	if ((u = BATdescriptor(*uid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (mask_cand(u)) {
		BAT *ou = u;
		u = BATunmask(u);
		BBPunfix(ou->batCacheid);
		if (!u) {
			BBPunfix(b->batCacheid);
			throw(MAL, "bat.append", GDK_EXCEPTION);
		}
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(u->batCacheid);
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	ret = BATappend(b, u, s, force ? *force : false);
	BBPunfix(u->batCacheid);
	BBPreclaim(s);
	if (ret != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCappend_cand_wrap(Client ctx, bat *r, const bat *bid, const bat *uid, const bat *sid)
{
	(void) ctx;
	return BKCappend_cand_force_wrap(ctx, r, bid, uid, sid, NULL);
}

static str
BKCappend_wrap(Client ctx, bat *r, const bat *bid, const bat *uid)
{
	(void) ctx;
	return BKCappend_cand_force_wrap(ctx, r, bid, uid, NULL, NULL);
}

static str
BKCappend_force_wrap(Client ctx, bat *r, const bat *bid, const bat *uid, const bit *force)
{
	(void) ctx;
	return BKCappend_cand_force_wrap(ctx, r, bid, uid, NULL, force);
}

static str
BKCappend_val_force_wrap(Client ctx, bat *r, const bat *bid, const void *u,
						 const bit *force)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.append", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (isVIEW(b)) {
		BAT *bn = COLcopy(b, b->ttype, true, TRANSIENT);
		MT_lock_set(&b->theaplock);
		restrict_t mode = b->batRestricted;
		MT_lock_unset(&b->theaplock);
		BBPunfix(b->batCacheid);
		if (bn == NULL || (b = BATsetaccess(bn, mode)) == NULL)
			throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	derefStr(b, u);
	if (BUNappend(b, u, force ? *force : false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.append", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCappend_val_wrap(Client ctx, bat *r, const bat *bid, const void *u)
{
	(void) ctx;
	return BKCappend_val_force_wrap(ctx, r, bid, u, NULL);
}

static str
BKCbun_inplace(Client ctx, bat *r, const bat *bid, const oid *id, const void *t)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	derefStr(b, t);
	if (void_inplace(b, *id, t, false) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCbun_inplace_force(Client ctx, bat *r, const bat *bid, const oid *id, const void *t,
					 const bit *force)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.inplace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	derefStr(b, t);
	if (void_inplace(b, *id, t, *force) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.inplace", GDK_EXCEPTION);
	}
	*r = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}


static str
BKCbat_inplace_force(Client ctx, bat *r, const bat *bid, const bat *rid, const bat *uid,
					 const bit *force)
{
	(void) ctx;
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
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(p->batCacheid);
	BBPunfix(u->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCbat_inplace(Client ctx, bat *r, const bat *bid, const bat *rid, const bat *uid)
{
	(void) ctx;
	bit F = FALSE;

	return BKCbat_inplace_force(ctx, r, bid, rid, uid, &F);
}

/*end of SQL enhancement */

static str
BKCgetCapacity(Client ctx, lng *res, const bat *bid)
{
	(void) ctx;
	*res = lng_nil;
	BAT *b = BBPquickdesc(*bid);

	if (b == NULL)
		throw(MAL, "bat.getCapacity", ILLEGAL_ARGUMENT);
	*res = (lng) BATcapacity(b);
	return MAL_SUCCEED;
}

static str
BKCgetColumnType(Client ctx, str *res, const bat *bid)
{
	allocator *ma = ctx->curprg->def->ma;
	const char *ret = str_nil;
	BAT *b = BBPquickdesc(*bid);

	if (b == NULL)
		throw(MAL, "bat.getColumnType", ILLEGAL_ARGUMENT);
	ret = *bid < 0 ? ATOMname(TYPE_void) : ATOMname(b->ttype);
	*res = ma_strdup(ma, ret);
	if (*res == NULL)
		throw(MAL, "bat.getColumnType", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
BKCisSorted(Client ctx, bit *res, const bat *bid)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.isSorted", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*res = BATordered(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCisSortedReverse(Client ctx, bit *res, const bat *bid)
{
	(void) ctx;
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
BKCgetKey(Client ctx, bit *ret, const bat *bid)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.setPersistence",
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	MT_lock_set(&b->theaplock);
	*ret = b->tkey;
	MT_lock_unset(&b->theaplock);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCpersists(void *r, const bat *bid, const bit *flg)
{
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence",
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (BATmode(b, (*flg != TRUE)) != GDK_SUCCEED) {
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.setPersistence", ILLEGAL_ARGUMENT);
	}
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCsetPersistent(Client ctx, void *r, const bat *bid)
{
	(void) ctx;
	bit flag = TRUE;
	return BKCpersists(r, bid, &flag);
}

static str
BKCisPersistent(Client ctx, bit *res, const bat *bid)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setPersistence",
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	MT_lock_set(&b->theaplock);
	*res = !b->batTransient;
	MT_lock_unset(&b->theaplock);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCsetTransient(Client ctx, void *r, const bat *bid)
{
	(void) ctx;
	bit flag = FALSE;
	return BKCpersists(r, bid, &flag);
}

static str
BKCisTransient(Client ctx, bit *res, const bat *bid)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setTransient", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	MT_lock_set(&b->theaplock);
	*res = b->batTransient;
	MT_lock_unset(&b->theaplock);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCsetAccess(Client ctx, bat *res, const bat *bid, const char *const *param)
{
	(void) ctx;
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
		throw(MAL, "bat.setAccess",
			  ILLEGAL_ARGUMENT " Got %c" " expected 'r','a', or 'w'",
			  *param[0]);
	}
	if ((b = BATsetaccess(b, m)) == NULL)
		throw(MAL, "bat.setAccess", OPERATION_FAILED);
	*res = b->batCacheid;
	BBPretain(b->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCgetAccess(Client ctx, str *res, const bat *bid)
{
	BAT *b;

	(void) ctx;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.getAccess", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	switch (BATgetaccess(b)) {
	case BAT_READ:
		*res = "read";
		break;
	case BAT_APPEND:
		*res = "append";
		break;
	case BAT_WRITE:
		*res = "write";
		break;
	default:
		MT_UNREACHABLE();
	}
	BBPunfix(b->batCacheid);
	if (*res == NULL)
		throw(MAL, "bat.getAccess", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/*
 * Property management
 * All property operators should ensure exclusive access to the BAT
 * descriptor.
 * Where necessary use the primary view to access the properties
 */
static inline char *
local_itoa(ssize_t i, char *buf)
{
	snprintf(buf, 32, "%zd", i);
	return buf;
}

static inline char *
local_utoa(size_t i, char *buf)
{
	snprintf(buf, 32, "%zu", i);
	return buf;
}

static inline char *
oidtostr(allocator *ma, oid i, char *p, size_t len)
{
	if (OIDtoStr(ma, &p, &len, &i, false) < 0)
		return NULL;
	return p;
}

static gdk_return
infoHeap(BAT *bk, BAT *bv, Heap *hp, const char *nme)
{
	char kbuf[32], vbuf[32];

	if (!hp)
		return GDK_SUCCEED;
	strconcat_len(kbuf, sizeof(kbuf), nme, "free", NULL);
	if (BUNappend(bk, kbuf, false) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(hp->free, vbuf), false) != GDK_SUCCEED)
		return GDK_FAIL;
	strconcat_len(kbuf, sizeof(kbuf), nme, "size", NULL);
	if (BUNappend(bk, kbuf, false) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(hp->size, vbuf), false) != GDK_SUCCEED)
		return GDK_FAIL;
	strconcat_len(kbuf, sizeof(kbuf), nme, "storage", NULL);
	if (BUNappend(bk, kbuf, false) != GDK_SUCCEED ||
		BUNappend(bv, (hp->base == NULL || hp->base == (char *) 1) ? "absent" : (hp->storage == STORE_MMAP) ? (hp-> filename [0] ? "memory mapped" : "anonymous vm") : (hp->storage == STORE_PRIV) ? "private map" : "malloced", false) != GDK_SUCCEED)
		return GDK_FAIL;
	strconcat_len(kbuf, sizeof(kbuf), nme, "newstorage", NULL);
	if (BUNappend(bk, kbuf, false) != GDK_SUCCEED ||
		BUNappend(bv, (hp->newstorage == STORE_MEM) ? "malloced" : (hp->newstorage == STORE_PRIV) ? "private map" : "memory mapped", false) != GDK_SUCCEED)
		return GDK_FAIL;
	strconcat_len(kbuf, sizeof(kbuf), nme, "filename", NULL);
	if (BUNappend(bk, kbuf, false) != GDK_SUCCEED ||
		BUNappend(bv, hp->filename[0] ? hp->filename : "no file",
				  false) != GDK_SUCCEED)
		return GDK_FAIL;
	return GDK_SUCCEED;
}

static gdk_return
HASHinfo(BAT *bk, BAT *bv, Hash *h)
{
	if (BUNappend(bk, "thash->type", false) != GDK_SUCCEED ||
		BUNappend(bv, ATOMname(h->type), false) != GDK_SUCCEED ||
		BUNappend(bk, "thash->mask", false) != GDK_SUCCEED ||
		BUNappend(bv, local_utoa(h->nbucket, (char[32]){0}), false) != GDK_SUCCEED)
		return GDK_FAIL;

	return GDK_SUCCEED;
}

static str
BKCinfo(Client ctx, bat *ret1, bat *ret2, const bat *bid)
{
	allocator *ma = ctx->curprg->def->ma;
	const char *mode, *accessmode;
	BAT *bk = NULL, *bv = NULL, *b;
	char bf[oidStrlen];
	char buf[32];

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.info", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	bk = COLnew(0, TYPE_str, 128, TRANSIENT);
	bv = COLnew(0, TYPE_str, 128, TRANSIENT);
	if (bk == NULL || bv == NULL) {
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BATiter bi = bat_iterator(b);
	if (bi.transient) {
		mode = "transient";
	} else {
		mode = "persistent";
	}

	switch (bi.restricted) {
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

	if (BUNappend(bk, "batId", false) != GDK_SUCCEED
		|| BUNappend(bv, BATgetId(b), false) != GDK_SUCCEED
		|| BUNappend(bk, "batCacheid", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) b->batCacheid, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "tparentid", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) bi.h->parentid, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "batCount", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa((size_t) bi.count, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "batCapacity", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa((size_t) b->batCapacity, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "head", false) != GDK_SUCCEED
		|| BUNappend(bv, ATOMname(TYPE_void), false) != GDK_SUCCEED
		|| BUNappend(bk, "tail", false) != GDK_SUCCEED
		|| BUNappend(bv, ATOMname(bi.type), false) != GDK_SUCCEED
		|| BUNappend(bk, "batPersistence", false) != GDK_SUCCEED
		|| BUNappend(bv, mode, false) != GDK_SUCCEED
		|| BUNappend(bk, "batRestricted", false) != GDK_SUCCEED
		|| BUNappend(bv, accessmode, false) != GDK_SUCCEED
		|| BUNappend(bk, "batRefcnt", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) BBP_refs(b->batCacheid), buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "batLRefcnt", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) BBP_lrefs(b->batCacheid), buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "batDirty", false) != GDK_SUCCEED
		|| BUNappend(bv, BATdirtybi(bi) ? "dirty" : "clean",
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "hseqbase", false) != GDK_SUCCEED
		|| BUNappend(bv, oidtostr(ma, b->hseqbase, bf, sizeof(bf)),
					 FALSE) != GDK_SUCCEED
		|| BUNappend(bk, "tdense", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) BATtdensebi(&bi), buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "tseqbase", false) != GDK_SUCCEED
		|| BUNappend(bv, oidtostr(ma, bi.tseq, bf, sizeof(bf)),
					 FALSE) != GDK_SUCCEED
		|| BUNappend(bk, "tsorted", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) bi.sorted, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "trevsorted", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) bi.revsorted, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "tkey", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) bi.key, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "tvarsized", false) != GDK_SUCCEED
		|| BUNappend(bv,
					 local_itoa((ssize_t)
								(bi.type == TYPE_void
								 || bi.vh != NULL), buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "tnosorted", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.nosorted, buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "tnorevsorted", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.norevsorted, buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "tnokey[0]", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.nokey[0], buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "tnokey[1]", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.nokey[1], buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "tnonil", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.nonil, buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "tnil", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.nil, buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "batInserted", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(b->batInserted, buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "ttop", false) != GDK_SUCCEED
		|| BUNappend(bv, local_utoa(bi.hfree, buf), false) != GDK_SUCCEED
		|| BUNappend(bk, "batCopiedtodisk", false) != GDK_SUCCEED
		|| BUNappend(bv, local_itoa((ssize_t) bi.copiedtodisk, buf),
					 false) != GDK_SUCCEED
		|| BUNappend(bk, "tail.dirty", false) != GDK_SUCCEED
		|| BUNappend(bv, bi.hdirty ? "dirty" : "clean", false) != GDK_SUCCEED
		|| infoHeap(bk, bv, bi.h, "tail.") != GDK_SUCCEED
		|| (bi.vh
			&& (BUNappend(bk, "tvheap.dirty", false) != GDK_SUCCEED
				|| BUNappend(bv, bi.vhdirty ? "dirty" : "clean", false) != GDK_SUCCEED
				|| infoHeap(bk, bv, bi.vh, "tvheap.") != GDK_SUCCEED))) {
		bat_iterator_end(&bi);
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	/* dump index information */
	MT_rwlock_rdlock(&b->thashlock);
	if (b->thash && HASHinfo(bk, bv, b->thash) != GDK_SUCCEED) {
		MT_rwlock_rdunlock(&b->thashlock);
		bat_iterator_end(&bi);
		BBPreclaim(bk);
		BBPreclaim(bv);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	MT_rwlock_rdunlock(&b->thashlock);
	bat_iterator_end(&bi);
	assert(BATcount(bk) == BATcount(bv));
	BBPunfix(b->batCacheid);
	*ret1 = bk->batCacheid;
	BBPkeepref(bk);
	*ret2 = bv->batCacheid;
	BBPkeepref(bv);
	return MAL_SUCCEED;
}

// get the actual size of all constituents, also for views
#define ROUND_UP(x,y) ((y)*(((x)+(y)-1)/(y)))

static str
BKCgetSize(Client ctx, lng *tot, const bat *bid)
{
	(void) ctx;
	BAT *b;
	lng size = 0;
	lng blksize = (lng) MT_pagesize();
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getDiskSize", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	size = sizeof(bat);

	MT_lock_set(&b->theaplock);
	if (!isVIEW(b)) {
		BUN cnt = BATcapacity(b);
		size += ROUND_UP(b->theap->free, blksize);
		if (b->tvheap)
			size += ROUND_UP(b->tvheap->free, blksize);
		MT_lock_unset(&b->theaplock);

		if (b->thash)
			size += ROUND_UP(sizeof(BUN) * cnt, blksize);
	} else {
		MT_lock_unset(&b->theaplock);
	}
	*tot = size;
	BBPunfix(*bid);
	return MAL_SUCCEED;
}

static str
BKCgetVHeapSize(Client ctx, lng *tot, const bat *bid)
{
	(void) ctx;
	BAT *b;
	lng size = 0;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getVHeapSize", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (ATOMvarsized(b->ttype)) {
		MT_lock_set(&b->theaplock);
		if (b->tvheap)
			size += b->tvheap->size;
		MT_lock_unset(&b->theaplock);
	}

	*tot = size;
	BBPunfix(*bid);
	return MAL_SUCCEED;
}

/*
 * Synced BATs
 */
static str
BKCisSynced(Client ctx, bit *ret, const bat *bid1, const bat *bid2)
{
	(void) ctx;
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
BKCsetName(Client ctx, void *r, const bat *bid, const char *const *s)
{
	(void) ctx;
	BAT *b;
	int ret;
	int c;
	const char *t = *s;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bat.setName", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	for (; (c = *t) != 0; t++)
		if (c != '_' && !GDKisalnum(c)) {
			BBPunfix(b->batCacheid);
			throw(MAL, "bat.setName",
				  ILLEGAL_ARGUMENT ": identifier expected: %s", *s);
		}

	t = *s;
	ret = BBPrename(b, t);
	BBPunfix(b->batCacheid);
	switch (ret) {
	case BBPRENAME_ILLEGAL:
		GDKclrerr();
		throw(MAL, "bat.setName",
			  ILLEGAL_ARGUMENT ": illegal temporary name: '%s'", t);
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
BKCgetBBPname(Client ctx, str *ret, const bat *bid)
{
	allocator *ma = ctx->curprg->def->ma;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.getName", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*ret = ma_strdup(ma, BBP_logical(b->batCacheid));
	BBPunfix(b->batCacheid);
	return *ret ? MAL_SUCCEED : createException(MAL, "bat.getName",
												SQLSTATE(HY013)
												MAL_MALLOC_FAIL);
}

static str
BKCsave(Client ctx, bit *res, const char *const *input)
{
	(void) ctx;
	bat bid = BBPindex(*input);
	BAT *b;

	*res = FALSE;
	if (!is_bat_nil(bid)) {
		if ((b = BATdescriptor(bid)) != NULL) {
			if (BATdirty(b)) {
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
BKCsave2(Client ctx, void *r, const bat *bid)
{
	(void) ctx;
	BAT *b;

	(void) r;
	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.save", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	MT_lock_set(&b->theaplock);
	if (!b->batTransient) {
		MT_lock_unset(&b->theaplock);
		BBPunfix(b->batCacheid);
		throw(MAL, "bat.save", "Only save transient columns.");
	}
	MT_lock_unset(&b->theaplock);

	if (b && BATdirty(b))
		BBPsave(b);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Accelerator Control
 */
static str
BKCsetHash(Client ctx, bit *ret, const bat *bid)
{
	(void) ctx;
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
BKCgetSequenceBase(Client ctx, oid *r, const bat *bid)
{
	(void) ctx;
	BAT *b;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "bat.setSequenceBase",
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	*r = b->hseqbase;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
BKCmergecand(Client ctx, bat *ret, const bat *aid, const bat *bid)
{
	(void) ctx;
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
		throw(MAL, "bat.mergecand", GDK_EXCEPTION);
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

static str
BKCintersectcand(Client ctx, bat *ret, const bat *aid, const bat *bid)
{
	(void) ctx;
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
		throw(MAL, "bat.intersectcand", GDK_EXCEPTION);
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

static str
BKCdiffcand(Client ctx, bat *ret, const bat *aid, const bat *bid)
{
	(void) ctx;
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
		throw(MAL, "bat.diffcand", GDK_EXCEPTION);
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func bat5_init_funcs[] = {
 command("bat", "mirror", BKCmirror, false, "Returns the head-mirror image of a BAT (two head columns).", args(1,2, batarg("",oid),batargany("b",1))),
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
 command("bat", "densebat", BKCdensebat, false, "Creates a new [void,void] BAT of size 'sz'.", args(1,2, batarg("",oid),arg("sz",lng))),
 command("bat", "info", BKCinfo, false, "Produce a table containing information about a BAT in [attribute,value] format. \nIt contains all properties of the BAT record. ", args(2,3, batarg("",str),batarg("",str),batargany("b",1))),
 command("bat", "getSize", BKCgetSize, false, "Calculate the actual size of the BAT descriptor, heaps, hashes in bytes\nrounded to the memory page size (see bbp.getPageSize()).", args(1,2, arg("",lng),batargany("b",1))),
 command("bat", "getVHeapSize", BKCgetVHeapSize, false, "Calculate the vheap size for varsized bats", args(1,2, arg("",lng),batargany("b",1))),
 command("bat", "getCapacity", BKCgetCapacity, false, "Returns the current allocation size (in max number of elements) of a BAT.", args(1,2, arg("",lng),batargany("b",1))),
 command("bat", "getColumnType", BKCgetColumnType, false, "Returns the type of the tail column of a BAT, as an integer type number.", args(1,2, arg("",str),batargany("b",1))),
 command("bat", "isaKey", BKCgetKey, false, "Return whether the column tail values are unique (key).", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setAccess", BKCsetAccess, false, "Try to change the update access privileges \nto this BAT. Mode:\nr[ead-only]      - allow only read access.\na[append-only]   - allow reads and update.\nw[riteable]      - allow all operations.\nBATs are updatable by default. On making a BAT read-only, \nall subsequent updates fail with an error message.\nReturns the BAT itself.", args(1,3, batargany("",1),batargany("b",1),arg("mode",str))),
 command("bat", "getAccess", BKCgetAccess, false, "Return the access mode attached to this BAT as a character.", args(1,2, arg("",str),batargany("b",1))),
 command("bat", "getSequenceBase", BKCgetSequenceBase, false, "Get the sequence base for the void column of a BAT.", args(1,2, arg("",oid),batargany("b",1))),
 command("bat", "isSorted", BKCisSorted, false, "Returns true if BAT values are ordered.", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "isSortedReverse", BKCisSortedReverse, false, "Returns true if BAT values are reversely ordered.", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "append", BKCappend_val_wrap, false, "append the value u to i", args(1,3, batargany("",1),batargany("i",1),argany("u",1))),
 command("bat", "setName", BKCsetName, false, "Give a logical name to a BAT. ", args(1,3, arg("",void),batargany("b",1),arg("s",str))),
 command("bat", "getName", BKCgetBBPname, false, "Gives back the logical name of a BAT.", args(1,2, arg("",str),batargany("b",1))),
 command("bat", "isTransient", BKCisTransient, false, "", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setTransient", BKCsetTransient, false, "Make the BAT transient.  Returns \nboolean which indicates if the\nBAT administration has indeed changed.", args(1,2, arg("",void),batargany("b",1))),
 command("bat", "isPersistent", BKCisPersistent, false, "", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "setPersistent", BKCsetPersistent, false, "Make the BAT persistent.", args(1,2, arg("",void),batargany("b",1))),
 command("bat", "save", BKCsave2, false, "", args(1,2, arg("",void),batargany("nme",1))),
 command("bat", "save", BKCsave, false, "Save a BAT to storage, if it was loaded and dirty.  \nReturns whether IO was necessary.  Please realize that \ncalling this function violates the atomic commit protocol!!", args(1,2, arg("",bit),arg("nme",str))),
 command("bat", "setHash", BKCsetHash, false, "Create a hash structure on the column", args(1,2, arg("",bit),batargany("b",1))),
 command("bat", "isSynced", BKCisSynced, false, "Tests whether two BATs are synced or not. ", args(1,3, arg("",bit),batargany("b1",1),batargany("b2",2))),
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
