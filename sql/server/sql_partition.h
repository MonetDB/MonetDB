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

extern str find_partition_type(mvc* sql, sql_subtype *tpe, sql_table *mt);

#endif //_SQL_PARTITION_H
