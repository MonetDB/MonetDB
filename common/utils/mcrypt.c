/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mcrypt.h"
#include <string.h>

#ifndef HAVE_EMBEDDED
/* only provide digest functions if not embedded */
#ifdef HAVE_OPENSSL
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <openssl/ripemd.h>
#else
#ifdef HAVE_COMMONCRYPTO
#define COMMON_DIGEST_FOR_OPENSSL
#include <CommonCrypto/CommonDigest.h>
#endif
#endif
#endif

/**
 * Returns a comma separated list of supported hash algorithms suitable
 * for final hashing by the client.  This list contains the smaller
 * (in char size) hashes.
 * The returned string is malloced and should be freed.
 */
char *
mcrypt_getHashAlgorithms(void)
{
	/* Currently, four "hashes" are available, RIPEMD160, SHA-2, SHA-1
	 * and MD5.  Previous versions supported UNIX crypt and plain text
	 * login, but those were removed when SHA-1 became mandatory for
	 * hashing the plain password in wire protocol version 9.
	 * Better/stronger/faster algorithms can be added in the future upon
	 * desire.
	 */
	const char *algorithms =
		"PROT10"
#ifdef HAVE_RIPEMD160_UPDATE
		",RIPEMD160"
#endif
#ifdef HAVE_SHA256_UPDATE
		",SHA256"
#endif
#ifdef HAVE_SHA1_UPDATE
		",SHA1"
#endif
#ifdef HAVE_MD5_UPDATE
		",MD5"
#endif
#ifdef HAVE_LIBSNAPPY
		",COMPRESSION_SNAPPY"
#endif
#ifdef HAVE_LIBLZ4
		",COMPRESSION_LZ4"
#endif
		;
	return strdup(algorithms);
}

/**
 * Returns a malloced string representing the hex representation of
 * the MD5 hash of the given string.
 */
char *
mcrypt_MD5Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_MD5_UPDATE)
	MD5_CTX c;
	unsigned char md[MD5_DIGEST_LENGTH];
	char *ret;

	assert(MD5_DIGEST_LENGTH == 16);
	MD5_Init(&c);
	MD5_Update(&c, string, len);
	MD5_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No MD5 digest function available.\n");
	exit(1);
	return NULL;
#endif
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-1 hash of the given string.
 */
char *
mcrypt_SHA1Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_SHA1_UPDATE)
	SHA_CTX c;
	unsigned char md[SHA_DIGEST_LENGTH];
	char *ret;

	assert(SHA_DIGEST_LENGTH == 20);
	SHA1_Init(&c);
	SHA1_Update(&c, string, len);
	SHA1_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No SHA1 digest function available.\n");
	exit(1);
	return NULL;
#endif
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-224 hash of the given string.
 */
char *
mcrypt_SHA224Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_SHA224_UPDATE)
	SHA256_CTX c;
	unsigned char md[SHA224_DIGEST_LENGTH];
	char *ret;

	assert(SHA224_DIGEST_LENGTH == 28);
	SHA224_Init(&c);
	SHA224_Update(&c, string, len);
	SHA224_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No SHA224 digest function available.\n");
	exit(1);
	return NULL;
#endif
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-256 hash of the given string.
 */
char *
mcrypt_SHA256Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_SHA256_UPDATE)
	SHA256_CTX c;
	unsigned char md[SHA256_DIGEST_LENGTH];
	char *ret;

	assert(SHA256_DIGEST_LENGTH == 32);
	SHA256_Init(&c);
	SHA256_Update(&c, string, len);
	SHA256_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No SHA256 digest function available.\n");
	exit(1);
	return NULL;
#endif
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-384 hash of the given string.
 */
char *
mcrypt_SHA384Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_SHA384_UPDATE)
	SHA512_CTX c;
	unsigned char md[SHA384_DIGEST_LENGTH];
	char *ret;

	assert(SHA384_DIGEST_LENGTH == 48);
	SHA384_Init(&c);
	SHA384_Update(&c, string, len);
	SHA384_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No SHA384 digest function available.\n");
	exit(1);
	return NULL;
#endif
}

/**
 * Returns a malloced string representing the hex representation of
 * the SHA-512 hash of the given string.
 */
char *
mcrypt_SHA512Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_SHA512_UPDATE)
	SHA512_CTX c;
	unsigned char md[SHA512_DIGEST_LENGTH];
	char *ret;

	assert(SHA512_DIGEST_LENGTH == 64);
	SHA512_Init(&c);
	SHA512_Update(&c, string, len);
	SHA512_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No SHA512 digest function available.\n");
	exit(1);
	return NULL;
#endif
}

/**
 * Returns a malloced string representing the hex representation of
 * the RIPEMD-160 hash of the given string.
 */
char *
mcrypt_RIPEMD160Sum(const char *string, size_t len)
{
#if !defined(HAVE_EMBEDDED) && defined(HAVE_RIPEMD160_UPDATE)
	RIPEMD160_CTX c;
	unsigned char md[RIPEMD160_DIGEST_LENGTH];
	char *ret;

	assert(RIPEMD160_DIGEST_LENGTH == 20);
	RIPEMD160_Init(&c);
	RIPEMD160_Update(&c, string, len);
	RIPEMD160_Final(md, &c);

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
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No RIPEMD160 digest function available.\n");
	exit(1);
	return NULL;
#endif
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
#if !defined(HAVE_EMBEDDED) && (defined(HAVE_OPENSSL) || defined(HAVE_COMMONCRYPTO))
	return mcryptsum(MONETDB5_PASSWDHASH_TOKEN)(string, len);
#else
	(void) string;
	(void) len;
	fprintf(stderr, "No digest function available.\n");
	exit(1);
	return NULL;
#endif
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
#if !defined(HAVE_EMBEDDED) && (defined(HAVE_OPENSSL) || defined(HAVE_COMMONCRYPTO))
	unsigned char md[64];	/* should be SHA512_DIGEST_LENGTH */
	char ret[sizeof(md) * 2 + 1];
	int len;

	/* make valgrind happy, prevent us from printing garbage afterwards */
	memset(md, 0, sizeof(md));

#ifdef HAVE_RIPEMD160_UPDATE
	if (strcmp(algo, "RIPEMD160") == 0) {
		RIPEMD160_CTX c;

		RIPEMD160_Init(&c);
		RIPEMD160_Update(&c, password, strlen(password));
		RIPEMD160_Update(&c, challenge, strlen(challenge));
		RIPEMD160_Final(md, &c);

		len = 40;
	} else
#endif
#ifdef HAVE_SHA512_UPDATE
	if (strcmp(algo, "SHA512") == 0) {
		SHA512_CTX c;

		SHA512_Init(&c);
		SHA512_Update(&c, password, strlen(password));
		SHA512_Update(&c, challenge, strlen(challenge));
		SHA512_Final(md, &c);

		len = 128;
	} else
#endif
#ifdef HAVE_SHA384_UPDATE
	if (strcmp(algo, "SHA384") == 0) {
		SHA512_CTX c;

		SHA384_Init(&c);
		SHA384_Update(&c, password, strlen(password));
		SHA384_Update(&c, challenge, strlen(challenge));
		SHA384_Final(md, &c);

		len = 96;
	} else
#endif
#ifdef HAVE_SHA256_UPDATE
	if (strcmp(algo, "SHA256") == 0) {
		SHA256_CTX c;

		SHA256_Init(&c);
		SHA256_Update(&c, password, strlen(password));
		SHA256_Update(&c, challenge, strlen(challenge));
		SHA256_Final(md, &c);

		len = 64;
	} else
#endif
#ifdef HAVE_SHA224_UPDATE
	if (strcmp(algo, "SHA224") == 0) {
		SHA256_CTX c;

		SHA224_Init(&c);
		SHA224_Update(&c, password, strlen(password));
		SHA224_Update(&c, challenge, strlen(challenge));
		SHA224_Final(md, &c);

		len = 56;
	} else
#endif
#ifdef HAVE_SHA1_UPDATE
	if (strcmp(algo, "SHA1") == 0) {
		SHA_CTX c;

		SHA1_Init(&c);
		SHA1_Update(&c, password, strlen(password));
		SHA1_Update(&c, challenge, strlen(challenge));
		SHA1_Final(md, &c);

		len = 40;
	} else
#endif
#ifdef HAVE_MD5_UPDATE
	if (strcmp(algo, "MD5") == 0) {
		MD5_CTX c;

		MD5_Init(&c);
		MD5_Update(&c, password, strlen(password));
		MD5_Update(&c, challenge, strlen(challenge));
		MD5_Final(md, &c);

		len = 32;
	} else
#endif
#endif
	{
		(void) algo;
		(void) password;
		(void) challenge;
		fprintf(stderr, "MonetDB was built without OpenSSL, but what you are trying to do requires it.\n");
		exit(1);
		return NULL;
	}

#if !defined(HAVE_EMBEDDED) && (defined(HAVE_OPENSSL) || defined(HAVE_COMMONCRYPTO))
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
#endif
}
