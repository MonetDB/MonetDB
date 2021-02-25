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
#ifdef HAVE_UUID_UUID_H
#include <uuid/uuid.h>
#endif
#ifndef HAVE_UUID
#ifdef HAVE_OPENSSL
# include <openssl/rand.h>
#else
#ifdef HAVE_COMMONCRYPTO
#include <CommonCrypto/CommonRandom.h>
#endif
#endif
#endif

#ifdef HAVE_UUID
#define UUID_SIZE	((int) sizeof(uuid_t)) /* size of a UUID */
#else
#define UUID_SIZE	16			/* size of a UUID */
#endif
#define UUID_STRLEN	36			/* length of string representation */

typedef union {
#ifdef HAVE_HGE
	hge h;						/* force alignment, not otherwise used */
#else
	lng l[2];					/* force alignment, not otherwise used */
#endif
#ifdef HAVE_UUID
	uuid_t u;
#else
	uint8_t u[UUID_SIZE];
#endif
} uuid;

static uuid uuid_nil;			/* automatically initialized as zeros */

int TYPE_uuid = 0;

static str
UUIDprelude(void *ret)
{
	(void) ret;
	assert(UUID_SIZE == 16);
	int u = malAtomSize(sizeof(uuid), "uuid");
	(void) u;					/* not needed if HAVE_HGE not defined */
#ifdef HAVE_HGE
	BATatoms[u].storage = TYPE_hge;
#endif
#ifdef HAVE_UUID
	uuid_clear(uuid_nil.u);
#endif
	TYPE_uuid = ATOMindex("uuid");
	return MAL_SUCCEED;
}

#ifdef HAVE_UUID
#define is_uuid_nil(x)	uuid_is_null((x)->u)
#else
#define is_uuid_nil(x)	(memcmp((x)->u, uuid_nil.u, UUID_SIZE) == 0)
#endif

/**
 * Returns the string representation of the given uuid value.
 * Warning: GDK function
 * Returns the length of the string
 */
static ssize_t
UUIDtoString(str *retval, size_t *len, const void *VALUE, bool external)
{
	const uuid *value = VALUE;
	if (*len <= UUID_STRLEN || *retval == NULL) {
		if (*retval)
			GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_STRLEN + 1)) == NULL)
			return -1;
		*len = UUID_STRLEN + 1;
	}
	if (is_uuid_nil(value)) {
		if (external) {
			return (ssize_t) strcpy_len(*retval, "nil", 4);
		}
		return (ssize_t) strcpy_len(*retval, str_nil, 2);
	}
#ifdef HAVE_UUID
	uuid_unparse_lower(value->u, *retval);
#else
	snprintf(*retval, *len,
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x"
			 "-%02x%02x-%02x%02x%02x%02x%02x%02x",
			 value->u[0], value->u[1], value->u[2], value->u[3],
			 value->u[4], value->u[5], value->u[6], value->u[7],
			 value->u[8], value->u[9], value->u[10], value->u[11],
			 value->u[12], value->u[13], value->u[14], value->u[15]);
#endif
	assert(strlen(*retval) == UUID_STRLEN);
	return UUID_STRLEN;
}

static ssize_t
UUIDfromString(const char *svalue, size_t *len, void **RETVAL, bool external)
{
	uuid **retval = (uuid **) RETVAL;
	const char *s = svalue;

	if (*len < UUID_SIZE || *retval == NULL) {
		GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_SIZE)) == NULL)
			return -1;
		*len = UUID_SIZE;
	}
	if (external && strcmp(svalue, "nil") == 0) {
		**retval = uuid_nil;
		return 3;
	}
	if (strNil(svalue)) {
		**retval = uuid_nil;
		return 1;
	}
	/* we don't use uuid_parse since we accept UUIDs without hyphens */
	uuid u;
	for (int i = 0, j = 0; i < UUID_SIZE; i++) {
		/* on select locations we allow a '-' in the source string */
		if (j == 8 || j == 12 || j == 16 || j == 20) {
			if (*s == '-')
				s++;
		}
		if (isdigit((unsigned char) *s))
			u.u[i] = *s - '0';
		else if ('a' <= *s && *s <= 'f')
			u.u[i] = *s - 'a' + 10;
		else if ('A' <= *s && *s <= 'F')
			u.u[i] = *s - 'A' + 10;
		else
			goto bailout;
		s++;
		j++;
		u.u[i] <<= 4;
		if (isdigit((unsigned char) *s))
			u.u[i] |= *s - '0';
		else if ('a' <= *s && *s <= 'f')
			u.u[i] |= *s - 'a' + 10;
		else if ('A' <= *s && *s <= 'F')
			u.u[i] |= *s - 'A' + 10;
		else
			goto bailout;
		s++;
		j++;
	}
	if (*s != 0)
		goto bailout;
	**retval = u;
	return (ssize_t) (s - svalue);

  bailout:
	**retval = uuid_nil;
	return -1;
}

static int
UUIDcompare(const void *L, const void *R)
{
	const uuid *l = L, *r = R;
	if (is_uuid_nil(r))
		return !is_uuid_nil(l);
	if (is_uuid_nil(l))
		return -1;
#ifdef HAVE_UUID
	return uuid_compare(l->u, r->u);
#else
	return memcmp(l->u, r->u, UUID_SIZE);
#endif
}

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
#ifdef HAVE_HGE
UUIDgenerateUuid(uuid *retval)
#else
UUIDgenerateUuid(uuid **retval)
#endif
{
	uuid *u;

#ifdef HAVE_HGE
	u = retval;
#else
	if (*retval == NULL && (*retval = GDKmalloc(UUID_SIZE)) == NULL)
		throw(MAL, "uuid.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	u = *retval;
#endif
	UUIDgenerateUuid_internal(u);
	return MAL_SUCCEED;
}

static str
#ifdef HAVE_HGE
UUIDgenerateUuidInt(uuid *retval, int *d)
#else
UUIDgenerateUuidInt(uuid **retval, int *d)
#endif
{
	(void)d;
	return UUIDgenerateUuid(retval);
}

static inline bit
isaUUID(str s)
{
	uuid u, *pu = &u;
	size_t l = UUID_SIZE;
	ssize_t res = UUIDfromString(s, &l, (void **) &pu, false);

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
	for (BUN p = 0 ; p < q ; p++) {
		str next = BUNtail(bi, p);
		dst[p] = isaUUID(next);
	}
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

#ifdef HAVE_HGE
static str
UUIDstr2uuid(uuid *retval, str *s)
{
	size_t l = UUID_SIZE;

	if (UUIDfromString(*s, &l, (void **) &retval, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "uuid.uuid", "Not a UUID");
}

static str
UUIDuuid2str(str *retval, const uuid *u)
{
	size_t l = 0;
	*retval = NULL;
	if (UUIDtoString(retval, &l, u, false) < 0)
		throw(MAL, "uuid.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
#else
static str
UUIDstr2uuid(uuid **retval, str *s)
{
	size_t l = *retval ? UUID_SIZE : 0;

	if (UUIDfromString(*s, &l, (void **) retval, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "uuid.uuid", "Not a UUID");
}

static str
UUIDuuid2str(str *retval, uuid **u)
{
	size_t l = 0;
	*retval = NULL;
	if (UUIDtoString(retval, &l, *u, false) < 0)
		throw(MAL, "uuid.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
#endif

static BUN
UUIDhash(const void *v)
{
	const uuid *u = (const uuid *) v;
	ulng u1, u2;

	u1 = (ulng) u->u[0] << 56 | (ulng) u->u[1] << 48 |
		(ulng) u->u[2] << 40 | (ulng) u->u[3] << 32 |
		(ulng) u->u[4] << 24 | (ulng) u->u[5] << 16 |
		(ulng) u->u[6] << 8 | (ulng) u->u[7];
	u2 = (ulng) u->u[8] << 56 | (ulng) u->u[9] << 48 |
		(ulng) u->u[10] << 40 | (ulng) u->u[11] << 32 |
		(ulng) u->u[12] << 24 | (ulng) u->u[13] << 16 |
		(ulng) u->u[14] << 8 | (ulng) u->u[15];
	/* we're not using mix_hge since this way we get the same result
	 * on systems with and without 128 bit integer support */
	return (BUN) (mix_lng(u1) ^ mix_lng(u2));
}

static const void *
UUIDnull(void)
{
	return &uuid_nil;
}

static void *
UUIDread(void *U, size_t *dstlen, stream *s, size_t cnt)
{
	uuid *u = U;
	if (u == NULL || *dstlen < cnt * sizeof(uuid)) {
		if ((u = GDKrealloc(u, cnt * sizeof(uuid))) == NULL)
			return NULL;
		*dstlen = cnt * sizeof(uuid);
	}
	if (mnstr_read(s, u, UUID_SIZE, cnt) < (ssize_t) cnt) {
		if (u != U)
			GDKfree(u);
		return NULL;
	}
	return u;
}

static gdk_return
UUIDwrite(const void *u, stream *s, size_t cnt)
{
	return mnstr_write(s, u, UUID_SIZE, cnt) ? GDK_SUCCEED : GDK_FAIL;
}

#include "mel.h"
mel_atom uuid_init_atoms[] = {
 { .name="uuid", .cmp=UUIDcompare, .fromstr=UUIDfromString, .hash=UUIDhash, .null=UUIDnull, .read=UUIDread, .tostr=UUIDtoString, .write=UUIDwrite, },  { .cmp=NULL }
};
mel_func uuid_init_funcs[] = {
 command("uuid", "prelude", UUIDprelude, false, "", args(1,1, arg("",void))),
 command("uuid", "new", UUIDgenerateUuid, true, "Generate a new uuid", args(1,1, arg("",uuid))),
 command("uuid", "new", UUIDgenerateUuidInt, true, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, arg("",uuid),arg("d",int))),
 command("batuuid", "new", UUIDgenerateUuidInt_bulk, true, "Generate a new uuid (dummy version for side effect free multiplex loop)", args(1,2, batarg("",uuid),batarg("d",int))),
 command("uuid", "uuid", UUIDstr2uuid, false, "Coerce a string to a uuid, validating its format", args(1,2, arg("",uuid),arg("s",str))),
 command("uuid", "str", UUIDuuid2str, false, "Coerce a uuid to its string type", args(1,2, arg("",str),arg("u",uuid))),
 command("uuid", "isaUUID", UUIDisaUUID, false, "Test a string for a UUID format", args(1,2, arg("",bit),arg("u",str))),
 command("batuuid", "isaUUID", UUIDisaUUID_bulk, false, "Test a string for a UUID format", args(1,2, batarg("",bit),batarg("u",str))),
 command("calc", "uuid", UUIDstr2uuid, false, "Coerce a string to a uuid, validating its format", args(1,2, arg("",uuid),arg("s",str))),
 command("calc", "uuid", UUIDuuid2uuid, false, "", args(1,2, arg("",uuid),arg("u",uuid))),
 command("calc", "str", UUIDuuid2str, false, "Coerce a uuid to a string type", args(1,2, arg("",str),arg("s",uuid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_uuid_mal)
{ mal_module("uuid", uuid_init_atoms, uuid_init_funcs); }
