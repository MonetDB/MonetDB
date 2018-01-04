/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SEEN_DATABASE_H
#define _SEEN_DATABASE_H 1

char* db_validname(char* dbname);
char* db_create(char* dbname);
char* db_destroy(char* dbname);
char* db_rename(char* olddb, char* newdb);
char* db_lock(char* dbname);
char* db_release(char* dbname);

#endif
