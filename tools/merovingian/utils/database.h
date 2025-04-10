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

#ifndef _SEEN_DATABASE_H
#define _SEEN_DATABASE_H 1

char* db_validname(const char *dbname);
char* db_create(const char *dbname);
char* db_destroy(const char *dbname);
char* db_rename(const char *olddb, const char *newdb);
char* db_lock(const char *dbname);
char* db_release(const char *dbname);

#endif
