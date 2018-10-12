/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_PARTITION_H
#define _SQL_PARTITION_H

#include "sql_mvc.h"
#include "sql_catalog.h"

extern str sql_partition_validate_key(mvc *sql, sql_table *nt, sql_key *k, const char* op);
extern str bootstrap_partition_expression(mvc* sql, sql_allocator *rsa, sql_table *mt, int instantiate);
extern void find_partition_type(sql_subtype *tpe, sql_table *mt);
extern str initialize_sql_parts(mvc* sql, sql_table *mt);

#endif //_SQL_PARTITION_H
