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

typedef struct DotMonetdb {
	char *user;
	char *passwd;
	char *dbname;
	char *language;
	char *host;
	bool save_history;
	char *output;
	int pagewidth;
	int port;
} DotMonetdb;

extern void parse_dotmonetdb(DotMonetdb *dotfile);
extern void destroy_dotmonetdb(DotMonetdb *dotfile);
