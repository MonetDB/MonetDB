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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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

#ifdef HAVE_UUID
#define UUID_SIZE	((int) sizeof(uuid_t)) /* size of a UUID */
#else
#define UUID_SIZE	16			/* size of a UUID */
#endif
#define UUID_LEN	36			/* length of string representation */

typedef struct {
#ifdef HAVE_UUID
	uuid_t u;
#else
	unsigned char u[UUID_SIZE];
#endif
} uuid;

#ifdef WIN32
#define uuid_export extern __declspec(dllexport)
#else
#define uuid_export extern
#endif

uuid_export bat *UUIDprelude(void);
uuid_export int UUIDcompare(const uuid *l, const uuid *r);
uuid_export int UUIDfromString(const char *svalue, int *len, uuid **retval);
uuid_export BUN UUIDhash(const void *u);
uuid_export uuid *UUIDnull(void);
uuid_export uuid *UUIDread(uuid *u, stream *s, size_t cnt);
uuid_export int UUIDtoString(str *retval, int *len, const uuid *value);
uuid_export int UUIDwrite(const uuid *u, stream *s, size_t cnt);

uuid_export str UUIDgenerateUuid(uuid **retval);
uuid_export str UUIDstr2uuid(uuid **retval, str *s);
uuid_export str UUIDuuid2str(str *retval, uuid **u);
uuid_export str UUIDisaUUID(bit *retval, str *u);
uuid_export str UUIDequal(bit *retval, uuid **l, uuid **r);

static uuid uuid_nil;			/* automatically initialized as zeros */

bat *
UUIDprelude(void)
{
	assert(UUID_SIZE == 16);
	(void) malAtomSize(sizeof(uuid), sizeof(oid), "uuid");
	return NULL;
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
	if (*len <= UUID_LEN) {
		if (*retval != NULL)
			GDKfree(*retval);
		if ((*retval = GDKmalloc(UUID_LEN + 1)) == NULL)
			return 0;
		*len = UUID_LEN + 1;
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
	assert(strlen(*retval) == UUID_LEN);
	return UUID_LEN;
}

int
UUIDfromString(const char *svalue, int *len, uuid **retval)
{
	const char *s = svalue;
	int i, j;

	if (*len < UUID_SIZE || *retval == NULL) {
		if (*retval != NULL)
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
	return s - svalue;

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

	if (*retval == NULL)
		*retval = GDKmalloc(UUID_SIZE);
	u = *retval;
#ifdef HAVE_UUID
	uuid_generate(u->u);
#else
	{
		int i, r;

		for (i = 0; i < UUID_SIZE;) {
			r = rand() % 65536;
			u->u[i++] = (unsigned char) (r >> 8);
			u->u[i++] = (unsigned char) r;
		}
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
	*retval = UUIDfromString(*s, &l, &pu) == UUID_LEN;
	return MAL_SUCCEED;
}

str
UUIDstr2uuid(uuid **retval, str *s)
{
	int l = *retval ? UUID_SIZE : 0;

	if (UUIDfromString(*s, &l, retval) == UUID_LEN) {
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
		throw(MAL, "uuid.str", "Allocation failure");
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

/* taken from gdk_search.h */
#define mix_int(X)	(((X)>>7)^((X)>>13)^((X)>>21)^(X))
BUN
UUIDhash(const void *v)
{
	const uuid *u = (const uuid *) v;
	int u1, u2, u3, u4;

	u1 = u->u[0] << 24 | u->u[1] << 16 | u->u[2] << 8 | u->u[3];
	u2 = u->u[4] << 24 | u->u[5] << 16 | u->u[6] << 8 | u->u[7];
	u3 = u->u[8] << 24 | u->u[9] << 16 | u->u[10] << 8 | u->u[11];
	u4 = u->u[12] << 24 | u->u[13] << 16 | u->u[14] << 8 | u->u[15];
	return (BUN) mix_int(u1 ^ u2 ^ u3 ^ u4);
}

uuid *
UUIDnull(void)
{
	return &uuid_nil;
}

uuid *
UUIDread(uuid *u, stream *s, size_t cnt)
{
	if (mnstr_read(s, u, UUID_SIZE, cnt) < (ssize_t) cnt)
		return NULL;
	return u;
}

int
UUIDwrite(const uuid *u, stream *s, size_t cnt)
{
	return mnstr_write(s, u, UUID_SIZE, cnt) ? GDK_SUCCEED : GDK_FAIL;
}
