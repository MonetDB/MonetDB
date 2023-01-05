/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/* NOTE: for this file to work correctly, the random number generator
 * must have been seeded (srand) with something like the current time */

#include "monetdb_config.h"
#include "muuid.h"
#include <string.h> /* strdup */
#include <unistd.h>	/* for getentropy on FreeBSD */
#if defined(HAVE_GETENTROPY) && defined(HAVE_SYS_RANDOM_H)
#include <sys/random.h>
#endif

#if !defined(HAVE_GETENTROPY) && defined(HAVE_RAND_S)
static inline bool
generate_uuid(char *out)
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
	snprintf(out, 37,
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
			 "%02x%02x%02x%02x%02x%02x",
			 u.uuid[0], u.uuid[1], u.uuid[2], u.uuid[3],
			 u.uuid[4], u.uuid[5], u.uuid[6], u.uuid[7],
			 u.uuid[8], u.uuid[9], u.uuid[10], u.uuid[11],
			 u.uuid[12], u.uuid[13], u.uuid[14], u.uuid[15]);
	return true;
}
#endif

char *
generateUUID(void)
{
	char out[37];
#if defined(HAVE_GETENTROPY)
	unsigned char randbuf[16];
	if (getentropy(randbuf, 16) == 0) {
		/* make sure this is a variant 1 UUID (RFC 4122/DCE 1.1) */
		randbuf[8] = (randbuf[8] & 0x3F) | 0x80;
		/* make sure this is version 4 (random UUID) */
		randbuf[6] = (randbuf[6] & 0x0F) | 0x40;
		snprintf(out, sizeof(out),
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
			 "%02x%02x%02x%02x%02x%02x",
			 randbuf[0], randbuf[1], randbuf[2], randbuf[3],
			 randbuf[4], randbuf[5], randbuf[6], randbuf[7],
			 randbuf[8], randbuf[9], randbuf[10], randbuf[11],
			 randbuf[12], randbuf[13], randbuf[14], randbuf[15]);
	} else
#elif defined(HAVE_RAND_S)
	if (!generate_uuid(out))
#endif
	{
		/* generate something like this:
		 * cefa7a9c-1dd2-41b2-8350-880020adbeef
		 * ("%08x-%04x-%04x-%04x-%012x") */
#ifdef __COVERITY__
		/* avoid rand() when checking with coverity */
		snprintf(out, sizeof(out),
				 "00000000-0000-0000-0000-000000000000");
#else
		snprintf(out, sizeof(out),
			 "%04x%04x-%04x-4%03x-8%03x-%04x%04x%04x",
			 (unsigned) rand() & 0xFFFF, (unsigned) rand() & 0xFFFF,
			 (unsigned) rand() & 0xFFFF, (unsigned) rand() & 0x0FFF,
			 (unsigned) rand() & 0x0FFF, (unsigned) rand() & 0xFFFF,
			 (unsigned) rand() & 0xFFFF, (unsigned) rand() & 0xFFFF);
#endif
	}
	return strdup(out);
}
