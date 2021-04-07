/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * K.S. Mullender & A. de Rijke
 * The UUID module
 * The UUID module contains a wrapper for all function in
 * libuuid.
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_atom.h"			/* for malAtomSize */
#ifndef HAVE_UUID
#ifdef HAVE_OPENSSL
# include <openssl/rand.h>
#else
#ifdef HAVE_COMMONCRYPTO
#include <CommonCrypto/CommonRandom.h>
#endif
#endif
#endif

/**
 * Returns the string representation of the given uuid value.
 * Warning: GDK function
 * Returns the length of the string
 */
static inline void
UUIDgenerateUuid_internal(uuid *u)
{
#ifdef HAVE_UUID
	uuid_generate(u->u);
#else
#ifdef HAVE_OPENSSL
	if (RAND_bytes(u->u, 16) < 0)
#else
#ifdef HAVE_COMMONCRYPTO
	if (CCRandomGenerateBytes(u->u, 16) != kCCSuccess)
#endif
#endif
		/* if it failed, use rand */
		for (int i = 0; i < UUID_SIZE;) {
			int r = rand();
			u->u[i++] = (unsigned char) (r >> 8);
			u->u[i++] = (unsigned char) r;
		}
	/* make sure this is a variant 1 UUID */
	u->u[8] = (u->u[8] & 0x3F) | 0x80;
	/* make sure this is version 4 (random UUID) */
	u->u[6] = (u->u[6] & 0x0F) | 0x40;
#endif
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
UUIDgenerateUuidInt_bulk(bat *ret, const bat *bid)
{
	BAT *b = NULL, *bn = NULL;
	BUN n = 0;
	str msg = MAL_SUCCEED;
	uuid *restrict bnt = NULL;

	if ((b = BBPquickdesc(*bid, false)) == NULL)	{
		msg = createException(MAL, "uuid.generateuuidint_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_uuid, n, TRANSIENT)) == NULL) {
		msg = createException(MAL, "uuid.generateuuidint_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	bnt = Tloc(bn, 0);
	for (BUN i = 0 ; i < n ; i++)
		UUIDgenerateUuid_internal(&(bnt[i]));
	bn->tnonil = true;
	bn->tnil = false;
	BATsetcount(bn, n);
	bn->tsorted = n <= 1;
	bn->trevsorted = n <= 1;
	bn->tkey = n <= 1;

bailout:
	if (msg && bn)
		BBPreclaim(bn);
	else if (bn)
		BBPkeepref(*ret = bn->batCacheid);
	return msg;
}

static str
UUIDisaUUID(bit *retval, str *s)
{
	*retval = isaUUID(*s);
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
	bn->tnonil = b->tnonil;
	bn->tnil = b->tnil;
	BATsetcount(bn, q);
	bn->tsorted = bn->trevsorted = q < 2;
	bn->tkey = false;
bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (msg && bn)
		BBPreclaim(bn);
	else if (bn)
		BBPkeepref(*ret = bn->batCacheid);
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
	BUN q = 0;
	oid off;
	bool nils = false;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.uuid2uuidbulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.uuid2uuidbulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPkeepref(*res = b->batCacheid); /* nothing to convert, return */
		return MAL_SUCCEED;
	}
	off = b->hseqbase;
	bv = Tloc(b, 0);
	q = canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_uuid, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.uuid2uuidbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	dv = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			uuid v = bv[p];

			dv[i] = v;
			nils |= is_uuid_nil(v);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next(&ci) - off);
			uuid v = bv[p];

			dv[i] = v;
			nils |= is_uuid_nil(v);
		}
	}

bailout:
	if (dst && !msg) {
		BATsetcount(dst, q);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = b->tkey;
		dst->tsorted = b->tsorted;
		dst->trevsorted = b->trevsorted;
		BBPkeepref(*res = dst->batCacheid);
	} else if (dst)
		BBPreclaim(dst);
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
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
	BUN q = 0;
	oid off;
	bool nils = false;
	size_t l = UUID_SIZE;
	ssize_t (*conv)(const char *, size_t *, void **, bool) = BATatoms[TYPE_uuid].atomFromStr;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	q = canditer_init(&ci, b, s);
	bi = bat_iterator(b);
	if (!(dst = COLnew(ci.hseq, TYPE_uuid, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	vals = Tloc(dst, 0);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			str v = (str) BUNtvar(bi, p);
			uuid *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(42000) "Not a UUID");
				goto bailout;
			}
			nils |= strNil(v);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next(&ci) - off);
			str v = (str) BUNtvar(bi, p);
			uuid *up = &vals[i], **pp = &up;

			if (conv(v, &l, (void **) pp, false) <= 0) {
				msg = createException(SQL, "batcalc.str2uuidbulk", SQLSTATE(42000) "Not a UUID");
				goto bailout;
			}
			nils |= strNil(v);
		}
	}

bailout:
	if (dst && !msg) {
		BATsetcount(dst, q);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = b->tkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		BBPkeepref(*res = dst->batCacheid);
	} else if (dst)
		BBPreclaim(dst);
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
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
	BUN q = 0;
	struct canditer ci;
	oid off;
	bool nils = false;
	char buf[UUID_STRLEN + 2], *pbuf = buf;
	size_t l = sizeof(buf);
	ssize_t (*conv)(char **, size_t *, const void *, bool) = BATatoms[TYPE_uuid].atomToStr;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	vals = Tloc(b, 0);
	q = canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			uuid v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) { /* it should never be reallocated */
				msg = createException(MAL, "batcalc.uuid2strbulk", GDK_EXCEPTION);
				goto bailout;
			}
			if (tfastins_nocheckVAR(dst, i, buf, Tsize(dst)) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= strNil(buf);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p = (canditer_next(&ci) - off);
			uuid v = vals[p];

			if (conv(&pbuf, &l, &v, false) < 0) { /* it should never be reallocated */
				msg = createException(MAL, "batcalc.uuid2strbulk", GDK_EXCEPTION);
				goto bailout;
			}
			if (tfastins_nocheckVAR(dst, i, buf, Tsize(dst)) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.uuid2strbulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			nils |= strNil(buf);
		}
	}

bailout:
	if (dst && !msg) {
		BATsetcount(dst, q);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = b->tkey;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		BBPkeepref(*res = dst->batCacheid);
	} else if (dst)
		BBPreclaim(dst);
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	return msg;
}

#include "mel.h"
mel_func uuid_init_funcs[] = {
 command("uuid", "new", UUIDgenerateUuid, true, "Generate a new uuid", args(1,1, arg("",uuid))),
 command("uuid", "new", UUIDgenerateUuidInt, true, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, arg("",uuid),arg("d",int))),
 command("batuuid", "new", UUIDgenerateUuidInt_bulk, true, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, batarg("",uuid),batarg("d",int))),
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
