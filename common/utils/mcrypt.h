/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#ifdef HAVE_MD5_UPDATE
mcrypt_export char *mcrypt_MD5Sum(const char *string, size_t len);
#endif
#ifdef HAVE_SHA1_UPDATE
mcrypt_export char *mcrypt_SHA1Sum(const char *string, size_t len);
#endif
#ifdef HAVE_SHA224_UPDATE
mcrypt_export char *mcrypt_SHA224Sum(const char *string, size_t len);
#endif
#ifdef HAVE_SHA256_UPDATE
mcrypt_export char *mcrypt_SHA256Sum(const char *string, size_t len);
#endif
#ifdef HAVE_SHA384_UPDATE
mcrypt_export char *mcrypt_SHA384Sum(const char *string, size_t len);
#endif
#ifdef HAVE_SHA512_UPDATE
mcrypt_export char *mcrypt_SHA512Sum(const char *string, size_t len);
#endif
#ifdef HAVE_RIPEMD160_UPDATE
mcrypt_export char *mcrypt_RIPEMD160Sum(const char *string, size_t len);
#endif
mcrypt_export char *mcrypt_BackendSum(const char *string, size_t len);
mcrypt_export char *mcrypt_hashPassword(const char *algo, const char *password, const char *challenge);
#endif
