/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _SEEN_MCRYPT_H
#define _SEEN_MCRYPT_H 1

#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#if !defined(LIBMAPI) && !defined(LIBMCRYPT) && !defined(LIBMONETDB5)
#define mcrypt_export extern __declspec(dllimport)
#else
#define mcrypt_export extern __declspec(dllexport)
#endif
#else
#define mcrypt_export extern
#endif

mcrypt_export const char *mcrypt_getHashAlgorithms(void)
	__attribute__((__const__));
mcrypt_export char *mcrypt_MD5Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_SHA1Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_SHA224Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_SHA256Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_SHA384Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_SHA512Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_RIPEMD160Sum(const char *string, size_t len);
mcrypt_export char *mcrypt_BackendSum(const char *string, size_t len);
mcrypt_export char *mcrypt_hashPassword(const char *algo, const char *password, const char *challenge);

#define SHA_DIGEST_LENGTH    20
#define SHA224_DIGEST_LENGTH 28
#define SHA256_DIGEST_LENGTH 32
#define SHA384_DIGEST_LENGTH 48
#define SHA512_DIGEST_LENGTH 64

#endif
