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

#include "ODBCGlobal.h"
#include "ODBCAttrs.h"

#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

static const char *suggest_on_off[] = {
	"On",
	"Off",
	NULL,
};

static const char *suggest_monetdb[] = {
	"monetdb",
	NULL,
};

const struct attr_setting attr_settings[] = {
	{ "UID", "User", MP_USER, .is_enum = false, .values = suggest_monetdb },
	{ "PWD", "Password", MP_PASSWORD, .is_enum = false, .values = suggest_monetdb },
	{ "DATABASE", "Database", MP_DATABASE },
	{ "PORT", "Port", MP_PORT },
	{ "HOST", "Server", MP_HOST, },
	{ "SOCK", "Unix Socket", MP_SOCK },
	{ "TLS", "Encrypt", MP_TLS, .is_enum = true, .values = suggest_on_off },
	{ "CERT", "Server Certificate", MP_CERT },
	{ "CERTHASH", "Server Certificate Hash", MP_CERTHASH },
	{ "CLIENTKEY", "Client Key", MP_CLIENTKEY },
	{ "CLIENTCERT", "Client Certificate", MP_CLIENTCERT },
	{ "AUTOCOMMIT", "Autocommit", MP_AUTOCOMMIT, .is_enum = true, .values = suggest_on_off },
	{ "SCHEMA", "Schema", MP_SCHEMA },
	{ "TIMEZONE", "Time Zone", MP_TIMEZONE },
	{ "REPLYSIZE", "Reply Size", MP_REPLYSIZE },
	{ "LOGFILE", "Log File", MP_LOGFILE },
	{ "LOGINTIMEOUT", "Login Timeout", MP_CONNECT_TIMEOUT},
	{ "CONNECTIONTIMEOUT", "Connection Timeout", MP_REPLY_TIMEOUT},
	{ "CLIENTINFO", "Send Client Info", MP_CLIENT_INFO },
	{ "APPNAME", "Application Name", MP_CLIENT_APPLICATION },
	{ "CLIENTREMARK", "Client Remark", MP_CLIENT_REMARK },
	{ "MAPTOLONGVARCHAR", NULL, MP_MAPTOLONGVARCHAR },
};

const int attr_setting_count = sizeof(attr_settings) / sizeof(attr_settings[0]);

int
attr_setting_lookup(const char *attr_name, bool allow_alt_name)
{
	for (int i = 0; i < attr_setting_count; i++) {
		const struct attr_setting *entry = &attr_settings[i];
		if (strcasecmp(attr_name, entry->name) == 0)
			return i;
		if (allow_alt_name && entry->alt_name && strcasecmp(attr_name, entry->alt_name) == 0)
			return i;
	}
	return -1;
}

