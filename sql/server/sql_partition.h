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

#ifndef _SQL_PARTITION_H
#define _SQL_PARTITION_H

#include "sql_mvc.h"
#include "sql_catalog.h"

extern list* partition_find_mergetables(mvc *sql, sql_table *t);
extern str sql_partition_validate_key(mvc *sql, sql_table *nt, sql_key *k, const char* op);
extern str bootstrap_partition_expression(mvc* sql, sql_table *mt, int instantiate);
extern str parse_sql_parts(mvc* sql, sql_table *mt);

#endif //_SQL_PARTITION_H
