/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
