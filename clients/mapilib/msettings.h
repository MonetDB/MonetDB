/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
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

        // long
        MP_PORT = MP__LONG_START,
        MP_TIMEZONE,
        MP_REPLYSIZE,

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
} mparm;

typedef enum mparm_class {
	MPCLASS_BOOL,
	MPCLASS_LONG,
	MPCLASS_STRING,
} mparm_class;

static inline mparm_class
mparm_classify(mparm parm)
{
	if (parm < MP__LONG_START)
		return MPCLASS_BOOL;
	else if (parm >= MP__STRING_START)
		return MPCLASS_STRING;
	else
		return MPCLASS_LONG;
}


/* returns NULL if not found, pointer to mparm if found */
mapi_export mparm mparm_parse(const char *name);
const char *mparm_name(mparm parm);
bool mparm_is_core(mparm parm);


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

/* returns NULL if could not allocate */
mapi_export msettings *msettings_create(void);
msettings *msettings_clone(const msettings *mp);
extern const msettings *msettings_default;

/* always returns NULL */
mapi_export msettings *msettings_destroy(msettings *mp);

/* retrieve and set; call abort() on type error */

mapi_export const char* msetting_string(const msettings *mp, mparm parm);
msettings_error msetting_set_string(msettings *mp, mparm parm, const char* value)
	__attribute__((__nonnull__(3)));

mapi_export long msetting_long(const msettings *mp, mparm parm);
msettings_error msetting_set_long(msettings *mp, mparm parm, long value);

mapi_export bool msetting_bool(const msettings *mp, mparm parm);
msettings_error msetting_set_bool(msettings *mp, mparm parm, bool value);

/* parse into the appropriate type, or format into newly malloc'ed string (NULL means malloc failed) */
msettings_error msetting_parse(msettings *mp, mparm parm, const char *text);
char *msetting_as_string(msettings *mp, mparm parm);

/* store ignored parameter */
msettings_error msetting_set_ignored(msettings *mp, const char *key, const char *value);

/* store named parameter */
mapi_export msettings_error msetting_set_named(msettings *mp, bool allow_core, const char *key, const char *value);

/* update the msettings from the URL. set *error_buffer to NULL and return true
 * if success, set *error_buffer to malloc'ed error message and return false on failure.
 * if return value is true but *error_buffer is NULL, malloc failed. */
mapi_export bool msettings_parse_url(msettings *mp, const char *url, char **error_buffer);

/* 1 = true, 0 = false, -1 = could not parse */
mapi_export int msetting_parse_bool(const char *text);

/* return an error message if the validity rules are not satisfied */
mapi_export bool msettings_validate(msettings *mp, char **errmsg);


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
