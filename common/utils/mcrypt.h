/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _SEEN_MCRYPT_H
#define _SEEN_MCRYPT_H 1

#ifndef mutils_export
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBMUTILS
#define mutils_export extern __declspec(dllimport)
#else
#define mutils_export extern __declspec(dllexport)
#endif
#else
#define mutils_export extern
#endif
#endif

mutils_export const char *mcrypt_getHashAlgorithms(void)
	__attribute__((__const__));
mutils_export char *mcrypt_MD5Sum(const char *string, size_t len);
mutils_export char *mcrypt_SHA1Sum(const char *string, size_t len);
mutils_export char *mcrypt_SHA224Sum(const char *string, size_t len);
mutils_export char *mcrypt_SHA256Sum(const char *string, size_t len);
mutils_export char *mcrypt_SHA384Sum(const char *string, size_t len);
mutils_export char *mcrypt_SHA512Sum(const char *string, size_t len);
mutils_export char *mcrypt_RIPEMD160Sum(const char *string, size_t len);
mutils_export char *mcrypt_BackendSum(const char *string, size_t len);
mutils_export char *mcrypt_hashPassword(const char *algo, const char *password, const char *challenge);

#define SHA_DIGEST_LENGTH    20
#define SHA224_DIGEST_LENGTH 28
#define SHA256_DIGEST_LENGTH 32
#define SHA384_DIGEST_LENGTH 48
#define SHA512_DIGEST_LENGTH 64

#endif
