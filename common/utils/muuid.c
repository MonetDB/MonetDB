/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/* NOTE: for this file to work correctly, the random number generator
 * must have been seeded (srand) with something like the current time */

#include "monetdb_config.h"
#include "muuid.h"
#include <stdlib.h> /* rand */
#include <string.h> /* strdup */
#ifdef HAVE_UUID_UUID_H
# include <uuid/uuid.h>
#endif
#ifndef HAVE_UUID
#ifdef HAVE_OPENSSL
# include <openssl/rand.h>
#else
#ifdef HAVE_COMMONCRYPTO
#include <CommonCrypto/CommonCrypto.h>
#include <CommonCrypto/CommonRandom.h>
#endif
#endif
#endif

/**
 * Shallow wrapper around uuid, that comes up with some random pseudo
 * uuid if uuid is not available
 */
char *
generateUUID(void)
{
#ifdef HAVE_UUID
# ifdef UUID_PRINTABLE_STRING_LENGTH
	/* Solaris */
	char out[UUID_PRINTABLE_STRING_LENGTH];
# else
	char out[37];
# endif
	uuid_t uuid;
	uuid_generate(uuid);
	uuid_unparse(uuid, out);
#else
	/* try to do some pseudo interesting stuff, and stash it in the
	 * format of a UUID to at least return some uniform answer */
	char out[37];
#ifdef HAVE_OPENSSL
	unsigned char randbuf[16];
	if (RAND_bytes(randbuf, 16) >= 0)
		snprintf(out, sizeof(out),
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
			 "%02x%02x%02x%02x%02x%02x",
			 randbuf[0], randbuf[1], randbuf[2], randbuf[3],
			 randbuf[4], randbuf[5], randbuf[6], randbuf[7],
			 randbuf[8], randbuf[9], randbuf[10], randbuf[11],
			 randbuf[12], randbuf[13], randbuf[14], randbuf[15]);
	else
#else
#ifdef HAVE_COMMONCRYPTO
	unsigned char randbuf[16];
	if (CCRandomGenerateBytes(randbuf, 16) == kCCSuccess)
		snprintf(out, sizeof(out),
			 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-"
			 "%02x%02x%02x%02x%02x%02x",
			 randbuf[0], randbuf[1], randbuf[2], randbuf[3],
			 randbuf[4], randbuf[5], randbuf[6], randbuf[7],
			 randbuf[8], randbuf[9], randbuf[10], randbuf[11],
			 randbuf[12], randbuf[13], randbuf[14], randbuf[15]);
	else
#endif
#endif
		/* generate something like this:
		 * cefa7a9c-1dd2-11b2-8350-880020adbeef
		 * ("%08x-%04x-%04x-%04x-%012x") */
		snprintf(out, sizeof(out),
			 "%04x%04x-%04x-%04x-%04x-%04x%04x%04x",
			 rand() % 65536, rand() % 65536,
			 rand() % 65536, rand() % 65536,
			 rand() % 65536, rand() % 65536,
			 rand() % 65536, rand() % 65536);
#endif
	return strdup(out);
}
