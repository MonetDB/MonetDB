/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_ENV_H_
#define _SQL_ENV_H_

#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_mvc.h"

extern str sql_update_var(mvc *sql, char *name, char *sval, lng sgn);

extern int sql_create_env(mvc *sql, sql_schema *s);

#define NR_KEEPQUERY_ARGS 4
#define NR_KEEPCALL_ARGS 9

#endif /* _SQL_ENV_H_ */
