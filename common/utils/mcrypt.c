/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mcrypt.h"
#include <string.h>

/* only provide digest functions if not embedded */
#include "sha.h"
#include "ripemd160.h"
#include "md5.h"

/**
 * Returns a comma separated list of supported hash algorithms suitable
 * for final hashing by the client.  This list contains the smaller
 * (in char size) hashes.
 * The returned string is malloced and should be freed.
 */
const char *
mcrypt_getHashAlgorithms(void)
{
	/* Currently, four "hashes" are available, RIPEMD160, SHA-2, SHA-1
	 * and MD5.  Previous versions supported UNIX crypt and plain text
	 * login, but those were removed when SHA-1 became mandatory for
	 * hashing the plain password in wire protocol version 9.
	 * Better/stronger/faster algorithms can be added in the future upon
	 * desire.
	 */
	static const char *algorithms =
		"RIPEMD160"
		",SHA512"
		",SHA384"
		",SHA256"
		",SHA224"
		",SHA1"
#ifdef HAVE_SNAPPY
		",COMPRESSION_SNAPPY"
#endif
#ifdef HAVE_LIBLZ4
		",COMPRESSION_LZ4"
#endif
		;
	return algorithms;
}

/**
 * Returns a malloced string representing the hex representation of
 * the MD5 hash of the given string.
 */
char *
mcrypt_MD5Sum(const char *string, size_t len)
{
	MD5_CTX c;
	uint8_t md[MD5_DIGEST_LENGTH];
	char *ret;

	static_assert(MD5_DIGEST_LENGTH == 16, "MD5_DIGEST_LENGTH should be 16");
	MD5Init(&c);
	MD5Update(&c, (const uint8_t *) string, (unsigned int) len);
	MD5Final(md, &c);

	ret = malloc(MD5_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, MD5_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3],
			 md[4], md[5], md[6], md[7],
			 md[8], md[9], md[10], md[11],
			 md[12], md[13], md[14], md[15]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-1 hash of the given string.
 */
char *
mcrypt_SHA1Sum(const char *string, size_t len)
{
	SHA1Context c;
	uint8_t md[SHA_DIGEST_LENGTH];
	char *ret;

	static_assert(SHA_DIGEST_LENGTH == SHA1HashSize, "SHA_DIGEST_LENGTH should be 20");
	SHA1Reset(&c);
	SHA1Input(&c, (const uint8_t *) string, (unsigned int) len);
	SHA1Result(&c, md);

	ret = malloc(SHA_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, SHA_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3], md[4],
			 md[5], md[6], md[7], md[8], md[9],
			 md[10], md[11], md[12], md[13], md[14],
			 md[15], md[16], md[17], md[18], md[19]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-224 hash of the given string.
 */
char *
mcrypt_SHA224Sum(const char *string, size_t len)
{
	SHA224Context c;
	uint8_t md[SHA224_DIGEST_LENGTH];
	char *ret;

	static_assert(SHA224_DIGEST_LENGTH == SHA224HashSize, "SHA224_DIGEST_LENGTH should be 28");
	SHA224Reset(&c);
	SHA224Input(&c, (const uint8_t *) string, (unsigned int) len);
	SHA224Result(&c, md);

	ret = malloc(SHA224_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, SHA224_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3], md[4],
			 md[5], md[6], md[7], md[8], md[9],
			 md[10], md[11], md[12], md[13], md[14],
			 md[15], md[16], md[17], md[18], md[19],
			 md[20], md[21], md[22], md[23], md[24],
			 md[25], md[26], md[27]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-256 hash of the given string.
 */
char *
mcrypt_SHA256Sum(const char *string, size_t len)
{
	SHA256Context c;
	uint8_t md[SHA256_DIGEST_LENGTH];
	char *ret;

	static_assert(SHA256_DIGEST_LENGTH == SHA256HashSize, "SHA256_DIGEST_LENGTH should be 32");
	SHA256Reset(&c);
	SHA256Input(&c, (const uint8_t *) string, (unsigned int) len);
	SHA256Result(&c, md);

	ret = malloc(SHA256_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, SHA256_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3], md[4],
			 md[5], md[6], md[7], md[8], md[9],
			 md[10], md[11], md[12], md[13], md[14],
			 md[15], md[16], md[17], md[18], md[19],
			 md[20], md[21], md[22], md[23], md[24],
			 md[25], md[26], md[27], md[28], md[29],
			 md[30], md[31]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-384 hash of the given string.
 */
char *
mcrypt_SHA384Sum(const char *string, size_t len)
{
	SHA384Context c;
	uint8_t md[SHA384_DIGEST_LENGTH];
	char *ret;

	static_assert(SHA384_DIGEST_LENGTH == SHA384HashSize, "SHA384_DIGEST_LENGTH should be 48");
	SHA384Reset(&c);
	SHA384Input(&c, (const uint8_t *) string, (unsigned int) len);
	SHA384Result(&c, md);

	ret = malloc(SHA384_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, SHA384_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3], md[4],
			 md[5], md[6], md[7], md[8], md[9],
			 md[10], md[11], md[12], md[13], md[14],
			 md[15], md[16], md[17], md[18], md[19],
			 md[20], md[21], md[22], md[23], md[24],
			 md[25], md[26], md[27], md[28], md[29],
			 md[30], md[31], md[32], md[33], md[34],
			 md[35], md[36], md[37], md[38], md[39],
			 md[40], md[41], md[42], md[43], md[44],
			 md[45], md[46], md[47]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-512 hash of the given string.
 */
char *
mcrypt_SHA512Sum(const char *string, size_t len)
{
	SHA512Context c;
	uint8_t md[SHA512_DIGEST_LENGTH];
	char *ret;

	static_assert(SHA512_DIGEST_LENGTH == SHA512HashSize, "SHA512_DIGEST_LENGTH should be 64");
	SHA512Reset(&c);
	SHA512Input(&c, (const uint8_t *) string, (unsigned int) len);
	SHA512Result(&c, md);

	ret = malloc(SHA512_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, SHA512_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3], md[4],
			 md[5], md[6], md[7], md[8], md[9],
			 md[10], md[11], md[12], md[13], md[14],
			 md[15], md[16], md[17], md[18], md[19],
			 md[20], md[21], md[22], md[23], md[24],
			 md[25], md[26], md[27], md[28], md[29],
			 md[30], md[31], md[32], md[33], md[34],
			 md[35], md[36], md[37], md[38], md[39],
			 md[40], md[41], md[42], md[43], md[44],
			 md[45], md[46], md[47], md[48], md[49],
			 md[50], md[51], md[52], md[53], md[54],
			 md[55], md[56], md[57], md[58], md[59],
			 md[60], md[61], md[62], md[63]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the RIPEMD-160 hash of the given string.
 */
char *
mcrypt_RIPEMD160Sum(const char *string, size_t len)
{
	RIPEMD160Context c;
	uint8_t md[RIPEMD160_DIGEST_LENGTH];
	char *ret;

	static_assert(RIPEMD160_DIGEST_LENGTH == 20, "RIPEMD160_DIGEST_LENGTH should be 20");
	RIPEMD160Reset(&c);
	RIPEMD160Input(&c, (const uint8_t *) string, (unsigned int) len);
	RIPEMD160Result(&c, md);

	ret = malloc(RIPEMD160_DIGEST_LENGTH * 2 + 1);
	if(ret) {
		snprintf(ret, RIPEMD160_DIGEST_LENGTH * 2 + 1,
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			 "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
			 md[0], md[1], md[2], md[3], md[4],
			 md[5], md[6], md[7], md[8], md[9],
			 md[10], md[11], md[12], md[13], md[14],
			 md[15], md[16], md[17], md[18], md[19]);
	}

	return ret;
}

/**
 * Returns a malloced string representing the hex representation of
 * the by the backend used hash of the given string.
 */
#define concat(x,y,z)	x##y##z
#define mcryptsum(h)	concat(mcrypt_, h, Sum)
char *
mcrypt_BackendSum(const char *string, size_t len)
{
	return mcryptsum(MONETDB5_PASSWDHASH_TOKEN)(string, len);
}

/**
 * Returns the hash for the given password, challenge and algorithm.
 * The hash calculated using the given algorithm over the password
 * concatenated with the challenge.  The returned string is allocated
 * using malloc, and hence should be freed with free by the
 * caller.  Returns NULL when the given algorithm is not supported.
 */
char *
mcrypt_hashPassword(
		const char *algo,
		const char *password,
		const char *challenge)
{
	unsigned char md[64];	/* should be SHA512_DIGEST_LENGTH */
	char ret[sizeof(md) * 2 + 1];
	int len;

	/* make valgrind happy, prevent us from printing garbage afterwards */
	memset(md, 0, sizeof(md));

	if (strcmp(algo, "RIPEMD160") == 0) {
		RIPEMD160Context c;

		RIPEMD160Reset(&c);
		RIPEMD160Input(&c, (const uint8_t *) password, (unsigned int) strlen(password));
		RIPEMD160Input(&c, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		RIPEMD160Result(&c, md);

		len = 40;
	} else if (strcmp(algo, "SHA512") == 0) {
		SHA512Context sc;

		SHA512Reset(&sc);
		SHA512Input(&sc, (const uint8_t *) password, (unsigned int) strlen(password));
		SHA512Input(&sc, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		SHA512Result(&sc, md);

		len = 128;
	} else if (strcmp(algo, "SHA384") == 0) {
		SHA384Context c;

		SHA384Reset(&c);
		SHA384Input(&c, (const uint8_t *) password, (unsigned int) strlen(password));
		SHA384Input(&c, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		SHA384Result(&c, md);

		len = 96;
	} else if (strcmp(algo, "SHA256") == 0) {
		SHA256Context c;

		SHA256Reset(&c);
		SHA256Input(&c, (const uint8_t *) password, (unsigned int) strlen(password));
		SHA256Input(&c, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		SHA256Result(&c, md);

		len = 64;
	} else if (strcmp(algo, "SHA224") == 0) {
		SHA224Context c;

		SHA224Reset(&c);
		SHA224Input(&c, (const uint8_t *) password, (unsigned int) strlen(password));
		SHA224Input(&c, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		SHA224Result(&c, md);

		len = 56;
	} else if (strcmp(algo, "SHA1") == 0) {
		SHA1Context c;

		SHA1Reset(&c);
		SHA1Input(&c, (const uint8_t *) password, (unsigned int) strlen(password));
		SHA1Input(&c, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		SHA1Result(&c, md);

		len = 40;
	} else if (strcmp(algo, "MD5") == 0) {
		MD5_CTX c;

		MD5Init(&c);
		MD5Update(&c, (const uint8_t *) password, (unsigned int) strlen(password));
		MD5Update(&c, (const uint8_t *) challenge, (unsigned int) strlen(challenge));
		MD5Final(md, &c);

		len = 32;
	} else {
		fprintf(stderr, "Unrecognized hash function (%s) requested.\n", algo);
		return NULL;
	}

	snprintf(ret, sizeof(ret),
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
			"%02x%02x%02x%02x",
			md[0], md[1], md[2], md[3],
			md[4], md[5], md[6], md[7],
			md[8], md[9], md[10], md[11],
			md[12], md[13], md[14], md[15],
			md[16], md[17], md[18], md[19],
			md[20], md[21], md[22], md[23],
			md[24], md[25], md[26], md[27],
			md[28], md[29], md[30], md[31],
			md[32], md[33], md[34], md[35],
			md[36], md[37], md[38], md[39],
			md[40], md[41], md[42], md[43],
			md[44], md[45], md[46], md[47],
			md[48], md[49], md[50], md[51],
			md[52], md[53], md[54], md[55],
			md[56], md[57], md[58], md[59],
			md[60], md[61], md[62], md[63]);
	ret[len] = '\0';

	return strdup(ret);
}
