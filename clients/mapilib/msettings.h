/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MSETTINGS_H
#define _MSETTINGS_H 1

#include "mapi.h"
#include <stdbool.h>

#define MP__BOOL_START (100)
#define MP__LONG_START (200)
#define MP__STRING_START (300)
#define MP__MAX (400)

#ifdef __cplusplus
extern "C" {
#endif

/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBMAPI
#define mapi_export extern __declspec(dllimport)
#else
#define mapi_export extern __declspec(dllexport)
#endif
#else
#define mapi_export extern
#endif

/////////////////////////////////////////////////////////////////////
// This enum identifies properties that can be set that affect how a
// connection is made. In particular we have functies to parse strings
// into a an enum value, and back.

typedef enum mparm {
	MP_UNKNOWN,
	MP_IGNORE,

        // bool
        MP_TLS = MP__BOOL_START,
        MP_AUTOCOMMIT,
	MP_CLIENT_INFO,
	// Note: if you change anything about this enum whatsoever, make sure to
	// make the corresponding change to struct msettings in msettings.c as well.

        // long
        MP_PORT = MP__LONG_START,
        MP_TIMEZONE,
        MP_REPLYSIZE,
	MP_MAPTOLONGVARCHAR,   // specific to ODBC
	MP_CONNECT_TIMEOUT,
	MP_REPLY_TIMEOUT,
	// Note: if you change anything about this enum whatsoever, make sure to
	// make the corresponding change to struct msettings in msettings.c as well.

        // string
        MP_SOCK = MP__STRING_START,
	MP_SOCKDIR,
        MP_CERT,
        MP_CLIENTKEY,
        MP_CLIENTCERT,
        MP_HOST,
        MP_DATABASE,
        MP_TABLESCHEMA,
        MP_TABLE,
        MP_CERTHASH,
        MP_USER,
        MP_PASSWORD,
        MP_LANGUAGE,
        MP_SCHEMA,		// TODO implement this
        MP_BINARY,
	MP_LOGFILE,
	MP_CLIENT_APPLICATION,
	MP_CLIENT_REMARK,
	// Note: if you change anything about this enum whatsoever, make sure to
	// make the corresponding change to struct msettings in msettings.c as well.

	// !! Make sure to keep them all below MP__MAX !!
} mparm;

typedef enum mparm_class {
	MPCLASS_BOOL,
	MPCLASS_LONG,
	MPCLASS_STRING,
} mparm_class;

static inline mparm_class
mparm_classify(mparm parm)
{
	assert(parm > MP_IGNORE);
	if (parm < MP__LONG_START)
		return MPCLASS_BOOL;
	else if (parm >= MP__STRING_START)
		return MPCLASS_STRING;
	else
		return MPCLASS_LONG;
}


/* returns NULL if not found, pointer to mparm if found */
mapi_export mparm mparm_parse(const char *name);
mapi_export const char *mparm_name(mparm parm);
mapi_export mparm mparm_enumerate(int i);
mapi_export bool mparm_is_core(mparm parm);


/////////////////////////////////////////////////////////////////////
// This type hold all properties that can be set that affect how a
// connection is made. There are methods to create/destroy etc.,
// getters and setters based on enum mparm above, and getters
// and setters based on string values.
// Also, msettings_validate, msettings_parse_url and a number
// of helper functions.

typedef struct msettings msettings;

/* NULL means OK. non-NULL is error message. Valid until next call. Do not free. */
typedef const char *msettings_error;
mapi_export bool msettings_malloc_failed(msettings_error err);

/* these return NULL if they cannot not allocate */
typedef void *(*msettings_allocator)(void *state, void *old, size_t size);
mapi_export msettings *msettings_create(void);
mapi_export msettings *msettings_create_with(msettings_allocator alloc, void *alloc_state);
mapi_export msettings *msettings_clone(const msettings *mp);
mapi_export msettings *msettings_clone_with(msettings_allocator alloc, void *alloc_state, const msettings *mp);
mapi_export msettings_allocator msettings_get_allocator(const msettings *mp, void **put_alloc_state_here);
mapi_export void msettings_reset(msettings *mp);
mapi_export const msettings *msettings_default;

/* always returns NULL */
mapi_export msettings *msettings_destroy(msettings *mp);

mapi_export const char *msetting_parm_name(const msettings *mp, mparm parm);
mapi_export void msettings_set_localizer(msettings *mp, const char* (*localizer)(const void *data, mparm parm), void *data);

/* retrieve and set; call abort() on type error */

mapi_export const char* msetting_string(const msettings *mp, mparm parm);
mapi_export msettings_error msetting_set_string(msettings *mp, mparm parm, const char* value)
	__attribute__((__nonnull__(3)));

mapi_export long msetting_long(const msettings *mp, mparm parm);
mapi_export msettings_error msetting_set_long(msettings *mp, mparm parm, long value);

mapi_export bool msetting_bool(const msettings *mp, mparm parm);
mapi_export msettings_error msetting_set_bool(msettings *mp, mparm parm, bool value);

/* Parse into the appropriate type */
mapi_export msettings_error msetting_parse(msettings *mp, mparm parm, const char *text);
/* Render setting as a string, requires a small scratch buffer (40 bytes is fine) for rendering integers.
 * Changing the msettings or the scratch buffer makes the returned pointer invalid. */
mapi_export const char *msetting_as_string(const msettings *mp, mparm parm, char *scratch, size_t scratch_size);

/* store named parameter */
mapi_export msettings_error msetting_set_named(msettings *mp, bool allow_core, const char *key, const char *value);

/* update the msettings from the URL. */
mapi_export msettings_error msettings_parse_url(msettings *mp, const char *url);

/* render the msettings as an URL. The result is always NUL terminated
 * even if it's truncated. Returns the number of characters that have been
 * written or would have been written if the buffer were large enough,
 * excluding the trailing NUL.
*/
mapi_export size_t msettings_write_url(const msettings *mp, char *buffer, size_t);

/* 1 = true, 0 = false, -1 = could not parse */
mapi_export int msetting_parse_bool(const char *text);

/* return an error message if the validity rules are not satisfied */
mapi_export msettings_error msettings_validate(msettings *mp);


/* virtual parameters */
enum msetting_tls_verify {
	verify_none,
	verify_system,
	verify_cert,
	verify_hash,
};
mapi_export bool msettings_connect_scan(const msettings *mp);
mapi_export const char *msettings_connect_unix(const msettings *mp);
mapi_export const char *msettings_connect_tcp(const msettings *mp);
mapi_export long msettings_connect_port(const msettings *mp);
mapi_export const char *msettings_connect_certhash_digits(const msettings *mp);
mapi_export long msettings_connect_binary(const msettings *mp);
mapi_export enum msetting_tls_verify msettings_connect_tls_verify(const msettings *mp);
mapi_export const char *msettings_connect_clientkey(const msettings *mp);
mapi_export const char *msettings_connect_clientcert(const msettings *mp);

/* automatically incremented each time the corresponding field is updated */
long msettings_user_generation(const msettings *mp);
long msettings_password_generation(const msettings *mp);

/* convenience helpers*/
bool msettings_lang_is_mal(const msettings *mp);
bool msettings_lang_is_sql(const msettings *mp);
bool msettings_lang_is_profiler(const msettings *mp);

/////////////////////////////////////////////////////////////////////
// Extend mapi.h

// Mutable access settings of existing Mapi.
// Do not make changes while connected.
mapi_export msettings *mapi_get_settings(Mapi mid)
	__attribute__((__nonnull__(1)));

// Create Mapi from settings.
// Takes ownership of the settings except if malloc fails etc.
// In that case NULL is returned and ownership of the settings remains with
// the caller.
mapi_export Mapi mapi_settings(msettings *settings)
	__attribute__((__nonnull__(1)));


#ifdef __cplusplus
}
#endif
#endif				/* _MSETTINGS_H */
