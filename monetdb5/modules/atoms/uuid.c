/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * K.S. Mullender & A. de Rijke
 * The UUID module
 * The UUID module contains a wrapper for all function in
 * libuuid.
 */

#include "monetdb_config.h"
#if defined(HAVE_GETENTROPY) && defined(HAVE_SYS_RANDOM_H)
#include <sys/random.h>
#endif
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#if !defined(HAVE_GETENTROPY) && defined(HAVE_RAND_S)
static inline bool
generate_uuid(uuid *U)
{
	union {
		unsigned int randbuf[4];
		unsigned char uuid[16];
	} u;
	for (int i = 0; i < 4; i++)
		if (rand_s(&u.randbuf[i]) != 0)
			return false;
	/* make sure this is a variant 1 UUID (RFC 4122/DCE 1.1) */
	u.uuid[8] = (u.uuid[8] & 0x3F) | 0x80;
	/* make sure this is version 4 (random UUID) */
	u.uuid[6] = (u.uuid[6] & 0x0F) | 0x40;
	memcpy(U->u, u.uuid, 16);
	return true;
}
#endif

/**
 * Returns the string representation of the given uuid value.
 * Warning: GDK function
 * Returns the length of the string
 */
static inline void
UUIDgenerateUuid_internal(uuid *u)
{
#if defined(HAVE_GETENTROPY)
	if (getentropy(u->u, 16) == 0) {
		/* make sure this is a variant 1 UUID (RFC 4122/DCE 1.1) */
		u->u[8] = (u->u[8] & 0x3F) | 0x80;
		/* make sure this is version 4 (random UUID) */
		u->u[6] = (u->u[6] & 0x0F) | 0x40;
	} else
#elif defined(HAVE_RAND_S)
	if (!generate_uuid(u))
#endif
	{
		/* generate something like this:
		 * cefa7a9c-1dd2-41b2-8350-880020adbeef
		 * ("%08x-%04x-%04x-%04x-%012x") */
		for (int i = 0; i < 16; i += 2) {
#ifdef __COVERITY__
			int r = 0;
#else
			int r = rand();
#endif
			u->u[i] = (unsigned char) (r >> 8);
			u->u[i+1] = (unsigned char) r;
		}
		/* make sure this is a variant 1 UUID (RFC 4122/DCE 1.1) */
		u->u[8] = (u->u[8] & 0x3F) | 0x80;
		/* make sure this is version 4 (random UUID) */
		u->u[6] = (u->u[6] & 0x0F) | 0x40;
	}
}

static str
UUIDgenerateUuid(uuid *retval)
{
	UUIDgenerateUuid_internal(retval);
	return MAL_SUCCEED;
}

static str
UUIDgenerateUuidInt(uuid *retval, int *d)
{
	(void)d;
	return UUIDgenerateUuid(retval);
}

static inline bit
isaUUID(const char *s)
{
	uuid u, *pu = &u;
	size_t l = UUID_SIZE;
	ssize_t res = BATatoms[TYPE_uuid].atomFromStr(s, &l, (void **) &pu, false);

	if (res > 1)
		return true;
	else if (res == 1)
		return bit_nil;
	else
		return false;
}

static str
UUIDgenerateUuidInt_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b = NULL, *bn = NULL;
	BUN n = 0;
	str msg = MAL_SUCCEED;
	uuid *restrict bnt = NULL;
	bat *ret = getArgReference_bat(stk, pci, 0);

	(void) cntxt;
	if (isaBatType(getArgType(mb, pci, 1))) {
		bat *bid = getArgReference_bat(stk, pci, 1);
		if (!(b = BBPquickdesc(*bid))) {
			throw(MAL, "uuid.generateuuidint_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		n = BATcount(b);
	} else {
		n = (BUN) *getArgReference_lng(stk, pci, 1);
	}

	if ((bn = COLnew(b ? b->hseqbase : 0, TYPE_uuid, n, TRANSIENT)) == NULL) {
		throw(MAL, "uuid.generateuuidint_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bnt = Tloc(bn, 0);
	for (BUN i = 0 ; i < n ; i++)
		UUIDgenerateUuid_internal(&(bnt[i]));
	BATsetcount(bn, n);
	bn->tnonil = true;
	bn->tnil = false;
	bn->tsorted = n <= 1;
	bn->trevsorted = n <= 1;
	bn->tkey = n <= 1;
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return msg;
}

static str
UUIDisaUUID(bit *retval, str *s)
{
	*retval = isaUUID(*s);
	if (*retval == false)
		GDKclrerr();
	return MAL_SUCCEED;
}

static str
UUIDisaUUID_bulk(bat *ret, const bat *bid)
{
	BAT *b = NULL, *bn = NULL;
	BUN q;
	bit *restrict dst;
	str msg = MAL_SUCCEED;
	BATiter bi;

	if ((b = BATdescriptor(*bid)) == NULL)	{
		msg = createException(MAL, "uuid.isaUUID_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_bit, q, TRANSIENT)) == NULL) {
		msg = createException(MAL, "uuid.isaUUID_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	dst = Tloc(bn, 0);
	bi = bat_iterator(b);
	for (BUN p = 0 ; p < q ; p++)
		dst[p] = isaUUID(BUNtvar(bi, p));
	GDKclrerr(); /* Not interested in atomFromStr errors */
	BATsetcount(bn, q);
	bn->tnonil = bi.nonil;
	bn->tnil = bi.nil;
	bn->tsorted = bn->trevsorted = q < 2;
	bn->tkey = false;
	bat_iterator_end(&bi);
bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bn) {					/* implies msg==MAL_SUCCEED */
		*ret = bn->batCacheid;
		BBPkeepref(bn);
	}
	return msg;
}

static str
UUIDuuid2uuid(uuid *retval, uuid *i)
{
	*retval = *i;
	return MAL_SUCCEED;
}

static str
UUIDuuid2uuid_bulk(bat *res, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	uuid *restrict bv, *restrict dv;
	str msg = NULL;
	struct canditer ci;
	oid off;
	bool nils = false, btsorted = false, btrevsorted = false, btkey = false;
	BATiter bi;

	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.uuid2uuidbulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPretain(*res = *bid); /* nothing to convert, return */
		return MAL_SUCCEED;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.uuid2uuidbulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_uuid, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.uuid2uuidbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	bv = bi.base;
	dv = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			uuid v = bv[p];

			dv[i] = v;
			nils |= is_uuid_nil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			uuid v = bv[p];

			dv[i] = v;
			nils |= is_uuid_nil(v);
		}
	}
	btkey = bi.key;
	btsorted = bi.sorted;
	btrevsorted = bi.revsorted;
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst) {					/* implies msg==MAL_SUCCEED */
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = btsorted;
		dst->trevsorted = btrevsorted;
		*res = dst->batCacheid;
		BBPkeepref(dst);
	}
	return msg;
}

static str
UUIDstr2uuid(uuid *retval, str *s)
{
	size_t l = UUID_SIZE;

	if (BATatoms[TYPE_uuid].atomFromStr(*s, &l, (void **) &retval, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "uuid.uuid", "Not a UUID");
}

static str
UUIDstr2uuid_bulk(bat *res, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = NULL;
	uuid *restrict vals;
	struct canditer ci;
	oid off;
	bool nils = false, btkey = false;
	size_t l = UUID_SIZE;
	ssize_t (*conv)(const char *, size_t *, void **, bool) = BATatoms[TYPE_uuid].atomFromStr;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_uuid, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			const char *v = BUNtvar(bi, p);
			uuid *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(42000) "Not a UUID");
				bat_iterator_end(&bi);
				goto bailout;
			}
			nils |= strNil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			const char *v = BUNtvar(bi, p);
			uuid *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(42000) "Not a UUID");
				bat_iterator_end(&bi);
				goto bailout;
			}
			nils |= strNil(v);
		}
	}
	btkey = bi.key;
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*res = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

static str
UUIDuuid2str(str *retval, const uuid *u)
{
	size_t l = 0;
	*retval = NULL;
	if (BATatoms[TYPE_uuid].atomToStr(retval, &l, u, false) < 0)
		throw(MAL, "uuid.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
UUIDuuid2str_bulk(bat *res, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	str msg = NULL;
	uuid *restrict vals;
	struct canditer ci;
	oid off;
	bool nils = false, btkey = false;
	char buf[UUID_STRLEN + 2], *pbuf = buf;
	size_t l = sizeof(buf);
	ssize_t (*conv)(char **, size_t *, const void *, bool) = BATatoms[TYPE_uuid].atomToStr;
	BATiter bi;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_str, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	vals = bi.base;
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			uuid v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) { /* it should never be reallocated */
				msg = createException(MAL, "batcalc.uuid2strbulk", GDK_EXCEPTION);
				bat_iterator_end(&bi);
				goto bailout;
			}
			if (tfastins_nocheckVAR(dst, i, buf) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				bat_iterator_end(&bi);
				goto bailout;
			}
			nils |= strNil(buf);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			uuid v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) { /* it should never be reallocated */
				msg = createException(MAL, "batcalc.uuid2strbulk", GDK_EXCEPTION);
				bat_iterator_end(&bi);
				goto bailout;
			}
			if (tfastins_nocheckVAR(dst, i, buf) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				bat_iterator_end(&bi);
				goto bailout;
			}
			nils |= strNil(buf);
		}
	}
	btkey = bi.key;
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = btkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*res = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

#include "mel.h"
mel_func uuid_init_funcs[] = {
 command("uuid", "new", UUIDgenerateUuid, true, "Generate a new uuid", args(1,1, arg("",uuid))),
 command("uuid", "new", UUIDgenerateUuidInt, false, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, arg("",uuid),arg("d",int))),
 pattern("batuuid", "new", UUIDgenerateUuidInt_bulk, false, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, batarg("",uuid),batarg("d",int))),
 pattern("batuuid", "new", UUIDgenerateUuidInt_bulk, false, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, batarg("",uuid),arg("card",lng))), /* version with cardinality input */
 command("uuid", "uuid", UUIDstr2uuid, false, "Coerce a string to a uuid, validating its format", args(1,2, arg("",uuid),arg("s",str))),
 command("uuid", "str", UUIDuuid2str, false, "Coerce a uuid to its string type", args(1,2, arg("",str),arg("u",uuid))),
 command("uuid", "isaUUID", UUIDisaUUID, false, "Test a string for a UUID format", args(1,2, arg("",bit),arg("u",str))),
 command("batuuid", "isaUUID", UUIDisaUUID_bulk, false, "Test a string for a UUID format", args(1,2, batarg("",bit),batarg("u",str))),
 command("calc", "uuid", UUIDstr2uuid, false, "Coerce a string to a uuid, validating its format", args(1,2, arg("",uuid),arg("s",str))),
 command("batcalc", "uuid", UUIDstr2uuid_bulk, false, "Coerce a string to a uuid, validating its format", args(1,3, batarg("",uuid),batarg("s",str),batarg("c",oid))),
 command("calc", "uuid", UUIDuuid2uuid, false, "", args(1,2, arg("",uuid),arg("u",uuid))),
 command("batcalc", "uuid", UUIDuuid2uuid_bulk, false, "", args(1,3, batarg("",uuid),batarg("u",uuid),batarg("c",oid))),
 command("calc", "str", UUIDuuid2str, false, "Coerce a uuid to a string type", args(1,2, arg("",str),arg("s",uuid))),
 command("batcalc", "str", UUIDuuid2str_bulk, false, "Coerce a uuid to a string type", args(1,3, batarg("",str),batarg("s",uuid),batarg("c",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_uuid_mal)
{ mal_module("uuid", NULL, uuid_init_funcs); }
