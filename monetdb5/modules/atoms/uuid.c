/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
	unsigned char u[UUID_SIZE];
#endif
} uuid;

mal_export str UUIDprelude(void *ret);
mal_export int UUIDcompare(const uuid *l, const uuid *r);
mal_export ssize_t UUIDfromString(const char *svalue, size_t *len, uuid **retval, bool external);
mal_export BUN UUIDhash(const void *u);
mal_export const uuid *UUIDnull(void);
mal_export uuid *UUIDread(uuid *u, stream *s, size_t cnt);
mal_export ssize_t UUIDtoString(str *retval, size_t *len, const uuid *value, bool external);
mal_export gdk_return UUIDwrite(const uuid *u, stream *s, size_t cnt);

#ifdef HAVE_HGE
mal_export str UUIDgenerateUuid(uuid *retval);
mal_export str UUIDgenerateUuidInt(uuid *retval, int *d);
mal_export str UUIDstr2uuid(uuid *retval, str *s);
mal_export str UUIDuuid2str(str *retval, const uuid *u);
#else
mal_export str UUIDgenerateUuid(uuid **retval);
mal_export str UUIDgenerateUuidInt(uuid **retval, int *d);
mal_export str UUIDstr2uuid(uuid **retval, str *s);
mal_export str UUIDuuid2str(str *retval, uuid **u);
#endif
mal_export str UUIDisaUUID(bit *retval, str *u);

static uuid uuid_nil;			/* automatically initialized as zeros */

str
UUIDprelude(void *ret)
{
	(void) ret;
	assert(UUID_SIZE == 16);
	int u = malAtomSize(sizeof(uuid), "uuid");
	(void) u;					/* not needed if HAVE_HGE not defined */
#ifdef HAVE_HGE
	BATatoms[u].storage = TYPE_hge;
#endif
	return MAL_SUCCEED;
}

#define is_uuid_nil(x)	(memcmp((x)->u, uuid_nil.u, UUID_SIZE) == 0)

/**
 * Returns the string representation of the given uuid value.
 * Warning: GDK function
 * Returns the length of the string
 */
ssize_t
UUIDtoString(str *retval, size_t *len, const uuid *value, bool external)
{
	if (*len <= UUID_STRLEN || *retval == NULL) {
		if (*retval)
			GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_STRLEN + 1)) == NULL)
			return -1;
		*len = UUID_STRLEN + 1;
	}
	if (is_uuid_nil(value)) {
		if (external) {
			snprintf(*retval, *len, "nil");
			return 3;
		}
		strcpy(*retval, str_nil);
		return 1;
	}
	snprintf(*retval, *len,
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
			 value->u[0], value->u[1], value->u[2], value->u[3],
			 value->u[4], value->u[5], value->u[6], value->u[7],
			 value->u[8], value->u[9], value->u[10], value->u[11],
			 value->u[12], value->u[13], value->u[14], value->u[15]);
	assert(strlen(*retval) == UUID_STRLEN);
	return UUID_STRLEN;
}

ssize_t
UUIDfromString(const char *svalue, size_t *len, uuid **retval, bool external)
{
	const char *s = svalue;
	int i, j;

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
	for (i = 0, j = 0; i < UUID_SIZE; i++) {
		/* on select locations we allow a '-' in the source string */
		if (j == 8 || j == 12 || j == 16 || j == 20) {
			if (*s == '-')
				s++;
		}
		if (isdigit((unsigned char) *s))
			(*retval)->u[i] = *s - '0';
		else if ('a' <= *s && *s <= 'f')
			(*retval)->u[i] = *s - 'a' + 10;
		else if ('A' <= *s && *s <= 'F')
			(*retval)->u[i] = *s - 'A' + 10;
		else
			goto bailout;
		s++;
		j++;
		(*retval)->u[i] <<= 4;
		if (isdigit((unsigned char) *s))
			(*retval)->u[i] |= *s - '0';
		else if ('a' <= *s && *s <= 'f')
			(*retval)->u[i] |= *s - 'a' + 10;
		else if ('A' <= *s && *s <= 'F')
			(*retval)->u[i] |= *s - 'A' + 10;
		else
			goto bailout;
		s++;
		j++;
	}
	return (ssize_t) (s - svalue);

  bailout:
	**retval = uuid_nil;
	return -1;
}

int
UUIDcompare(const uuid *l, const uuid *r)
{
	return memcmp(l->u, r->u, UUID_SIZE);
}

str
#ifdef HAVE_HGE
UUIDgenerateUuid(uuid *retval)
#else
UUIDgenerateUuid(uuid **retval)
#endif
{
	uuid *u;
	int i = 0, r = 0;

#ifdef HAVE_HGE
	u = retval;
#else
	if (*retval == NULL && (*retval = GDKmalloc(UUID_SIZE)) == NULL)
		throw(MAL, "uuid.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	u = *retval;
#endif
#ifdef HAVE_UUID
	uuid_generate(u->u);
	(void) i;
	(void) r;
#else
#ifdef HAVE_OPENSSL
	if (RAND_bytes(u->u, 16) < 0)
#else
#ifdef HAVE_COMMONCRYPTO
	if (CCRandomGenerateBytes(u->u, 16) != kCCSuccess)
#endif
#endif
		/* if it failed, use rand */
		for (i = 0; i < UUID_SIZE;) {
			r = rand() % 65536;
			u->u[i++] = (unsigned char) (r >> 8);
			u->u[i++] = (unsigned char) r;
		}
#endif
	return MAL_SUCCEED;
}

str
#ifdef HAVE_HGE
UUIDgenerateUuidInt(uuid *retval, int *d)
#else
UUIDgenerateUuidInt(uuid **retval, int *d)
#endif
{
	(void)d;
	return UUIDgenerateUuid(retval);
}

str
UUIDisaUUID(bit *retval, str *s)
{
	uuid u;
	uuid *pu = &u;
	size_t l = UUID_SIZE;
	*retval = UUIDfromString(*s, &l, &pu, false) > 1; /* valid, not nil */
	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
str
UUIDstr2uuid(uuid *retval, str *s)
{
	size_t l = UUID_SIZE;

	if (UUIDfromString(*s, &l, &retval, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "uuid.uuid", "Not a UUID");
}

str
UUIDuuid2str(str *retval, const uuid *u)
{
	size_t l = 0;
	*retval = NULL;
	if (UUIDtoString(retval, &l, u, false) < 0)
		throw(MAL, "uuid.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
#else
str
UUIDstr2uuid(uuid **retval, str *s)
{
	size_t l = *retval ? UUID_SIZE : 0;

	if (UUIDfromString(*s, &l, retval, false) > 0) {
		return MAL_SUCCEED;
	}
	throw(MAL, "uuid.uuid", "Not a UUID");
}

str
UUIDuuid2str(str *retval, uuid **u)
{
	size_t l = 0;
	*retval = NULL;
	if (UUIDtoString(retval, &l, *u, false) < 0)
		throw(MAL, "uuid.str", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
#endif

BUN
UUIDhash(const void *v)
{
	const uuid *u = (const uuid *) v;
	unsigned int u1, u2, u3, u4;

	u1 = (unsigned int) u->u[0] << 24 | (unsigned int) u->u[1] << 16 |
		(unsigned int) u->u[2] << 8 | (unsigned int) u->u[3];
	u2 = (unsigned int) u->u[4] << 24 | (unsigned int) u->u[5] << 16 |
		(unsigned int) u->u[6] << 8 | (unsigned int) u->u[7];
	u3 = (unsigned int) u->u[8] << 24 | (unsigned int) u->u[9] << 16 |
		(unsigned int) u->u[10] << 8 | (unsigned int) u->u[11];
	u4 = (unsigned int) u->u[12] << 24 | (unsigned int) u->u[13] << 16 |
		(unsigned int) u->u[14] << 8 | (unsigned int) u->u[15];
	return (BUN) mix_int(u1 ^ u2 ^ u3 ^ u4);
}

const uuid *
UUIDnull(void)
{
	return &uuid_nil;
}

uuid *
UUIDread(uuid *U, stream *s, size_t cnt)
{
	uuid *u = U;
	if (u == NULL && (u = GDKmalloc(cnt * sizeof(uuid))) == NULL)
		return NULL;
	if (mnstr_read(s, u, UUID_SIZE, cnt) < (ssize_t) cnt) {
		if (u != U)
			GDKfree(u);
		return NULL;
	}
	return u;
}

gdk_return
UUIDwrite(const uuid *u, stream *s, size_t cnt)
{
	return mnstr_write(s, u, UUID_SIZE, cnt) ? GDK_SUCCEED : GDK_FAIL;
}
