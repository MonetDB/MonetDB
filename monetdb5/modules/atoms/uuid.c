/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

typedef struct {
#ifdef HAVE_UUID
	uuid_t u;
#else
	unsigned char u[UUID_SIZE];
#endif
} uuid;

mal_export str UUIDprelude(void *ret);
mal_export int UUIDcompare(const uuid *l, const uuid *r);
mal_export int UUIDfromString(const char *svalue, int *len, uuid **retval);
mal_export BUN UUIDhash(const void *u);
mal_export uuid *UUIDnull(void);
mal_export uuid *UUIDread(uuid *u, stream *s, size_t cnt);
mal_export int UUIDtoString(str *retval, int *len, const uuid *value);
mal_export gdk_return UUIDwrite(const uuid *u, stream *s, size_t cnt);

mal_export str UUIDgenerateUuid(uuid **retval);
mal_export str UUIDstr2uuid(uuid **retval, str *s);
mal_export str UUIDuuid2str(str *retval, uuid **u);
mal_export str UUIDisaUUID(bit *retval, str *u);
mal_export str UUIDequal(bit *retval, uuid **l, uuid **r);

static uuid uuid_nil;			/* automatically initialized as zeros */
static uuid *uuid_session;		/* automatically set during system restart */

str
UUIDprelude(void *ret)
{
	int len = 0;

	(void) ret;
	assert(UUID_SIZE == 16);
	(void) malAtomSize(sizeof(uuid), sizeof(oid), "uuid");
	UUIDgenerateUuid(&uuid_session);
	UUIDtoString(&mal_session_uuid, &len, uuid_session);
	//mnstr_printf(GDKerr,"Session uid:%s", uuid_session_name);
	return MAL_SUCCEED;
}

#define UUIDisnil(x)	(memcmp((x)->u, uuid_nil.u, UUID_SIZE) == 0)

/**
 * Returns the string representation of the given uuid value.
 * Warning: GDK function
 * Returns the length of the string
 */
int
UUIDtoString(str *retval, int *len, const uuid *value)
{
	if (*len <= UUID_STRLEN || *retval == NULL) {
		if (*retval)
			GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_STRLEN + 1)) == NULL)
			return 0;
		*len = UUID_STRLEN + 1;
	}
	if (UUIDisnil(value)) {
		snprintf(*retval, *len, "nil");
		return 3;
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

int
UUIDfromString(const char *svalue, int *len, uuid **retval)
{
	const char *s = svalue;
	int i, j;

	if (*len < UUID_SIZE || *retval == NULL) {
		GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_SIZE)) == NULL)
			return 0;
		*len = UUID_SIZE;
	}
	if (strcmp(svalue, "nil") == 0) {
		**retval = uuid_nil;
		return 3;
	}
	for (i = 0, j = 0; i < UUID_SIZE; i++) {
		if (j == 8 || j == 12 || j == 16 || j == 20) {
			if (*s != '-')
				goto bailout;
			s++;
		}
		if ('0' <= *s && *s <= '9')
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
		if ('0' <= *s && *s <= '9')
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
	return (int)(s - svalue);

  bailout:
	**retval = uuid_nil;
	return 0;
}

int
UUIDcompare(const uuid *l, const uuid *r)
{
	return memcmp(l->u, r->u, UUID_SIZE);
}

str
UUIDgenerateUuid(uuid **retval)
{
	uuid *u;
	int i = 0, r = 0;

	if (*retval == NULL && (*retval = GDKmalloc(UUID_SIZE)) == NULL)
		throw(MAL, "uuid.new", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	u = *retval;
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
UUIDisaUUID(bit *retval, str *s)
{
	uuid u;
	uuid *pu = &u;
	int l = UUID_SIZE;
	*retval = UUIDfromString(*s, &l, &pu) == UUID_STRLEN;
	return MAL_SUCCEED;
}

str
UUIDstr2uuid(uuid **retval, str *s)
{
	int l = *retval ? UUID_SIZE : 0;

	if (UUIDfromString(*s, &l, retval) == UUID_STRLEN) {
		return MAL_SUCCEED;
	}
	throw(MAL, "uuid.uuid", "Not a UUID");
}

str
UUIDuuid2str(str *retval, uuid **u)
{
	int l = 0;
	*retval = NULL;
	if (UUIDtoString(retval, &l, *u) == 0)
		throw(MAL, "uuid.str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
UUIDequal(bit *retval, uuid **l, uuid **r)
{
	if (UUIDisnil(*l) || UUIDisnil(*r))
		*retval = bit_nil;
	else
		*retval = memcmp((*l)->u, (*r)->u, UUID_SIZE) == 0;
	return MAL_SUCCEED;
}

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

uuid *
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
