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

#ifndef MSETTINGS_INTERNAL
#define MSETTINGS_INTERNAL

#include "msettings.h"

extern const char * const MALLOC_FAILED;


struct string {
	char *str;
	bool must_free;
};

struct msettings {
	// Must match EXACTLY the order of enum mparm
	bool dummy_start_bool;
	bool tls;
	bool autocommit;
	bool client_info;
	bool dummy_end_bool;

	// Must match EXACTLY the order of enum mparm
	long dummy_start_long;
	long port;
	long timezone;
	long replysize;
	long map_to_long_varchar;
	long connect_timeout;
	long reply_timeout;
	long dummy_end_long;

	// Must match EXACTLY the order of enum mparm
	struct string dummy_start_string;
	struct string sock;
	struct string sockdir;
	struct string cert;
	struct string clientkey;
	struct string clientcert;
	struct string host;
	struct string database;
	struct string tableschema;
	struct string table;
	struct string certhash;
	struct string user;
	struct string password;
	struct string language;
	struct string schema;
	struct string binary;
	struct string logfile;
	struct string client_application;
	struct string client_remark;
	struct string dummy_end_string;

	bool lang_is_mal;
	bool lang_is_sql;
	long user_generation;
	long password_generation;
	char *unix_sock_name_buffer;
	char certhash_digits_buffer[64 + 2 + 1]; // fit more than required plus trailing '\0'
	bool validated;
	const char* (*localizer)(const void *data, mparm parm);
	void *localizer_data;
	void *(*alloc)(void *state, void *old, size_t size);    // NULL means regular realloc
	void *alloc_state;
	char error_message[256];
};


const char *format_error(msettings *mp, const char *fmt, ...)
	__attribute__((__format__(__printf__, 2, 3)));

// wrappers around mp->allocator

static inline void*
realloc_with_fallback(msettings_allocator alloc, void *alloc_state, void *old, size_t size)
{
	return alloc
		? alloc(alloc_state, old, size)
		: realloc(old, size);
}

static inline void*
msettings_realloc(const msettings *mp, void *old, size_t size)
{
	return realloc_with_fallback(mp->alloc, mp->alloc_state, old, size);
}

static inline void*
msettings_alloc(const msettings *mp, size_t size)
{
	return msettings_realloc(mp, NULL, size);
}

static inline void*
msettings_alloc_zeroed(const msettings *mp, size_t size)
{
	char *data = msettings_realloc(mp, NULL, size);
	memset(data, 0, size);
	return data;
}

static inline void*
msettings_dealloc(const msettings *mp, void *data)
{
	if (data != NULL)
		msettings_realloc(mp, data, 0);
	return NULL;
}

static inline void*
msettings_strdup(const msettings *mp, const char *string)
{
	if (string == NULL)
		return NULL;

	size_t size = strlen(string);
	char *new_string = msettings_alloc(mp, size + 1);
	if (new_string) {
		memcpy(new_string, string, size);
		new_string[size] = '\0';
	}
	return new_string;
}


static inline char* msettings_allocprintf(const msettings *mp, const char *fmt, ...)
	__attribute__((__format__(__printf__, 2, 3)));

static inline char*
msettings_allocprintf(const msettings *mp, const char *fmt, ...)
{
	char *buffer = NULL;
	va_list ap, ap2;

	va_start(ap, fmt);
	va_copy(ap2, ap);

	int len = vsnprintf("", 0, fmt, ap2);
	assert(len >= 0);
	if (len < 0)
		goto end;

	buffer = msettings_alloc(mp, len + 1);
	if (buffer == NULL)
		goto end;
	vsnprintf(buffer, len + 1, fmt, ap);
	buffer[len] = '\0';

end:
	va_end(ap2);
	va_end(ap);
	return buffer;
}

#endif
