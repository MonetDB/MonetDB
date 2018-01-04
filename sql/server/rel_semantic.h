/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_SEMANTIC_H_
#define _REL_SEMANTIC_H_

#include "sql_list.h"
#include "sql_symbol.h"
#include "sql_parser.h"
#include "sql_relation.h"
#include <stdarg.h>

extern sql_rel *rel_semantic(mvc *sql, symbol *sym);
extern sql_rel *rel_parse(mvc *m, sql_schema *s, char *query, char emode);

#endif /*_REL_SEMANTIC_H_*/

