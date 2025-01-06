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
	char error_message[256];
};


const char *format_error(msettings *mp, const char *fmt, ...)
	__attribute__((__format__(__printf__, 2, 3)));


#endif
