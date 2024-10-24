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

#ifndef _REL_ORDERBY_H_
#define _REL_ORDERBY_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

extern list *rel2bin_project_prepare(backend *be, sql_rel *rel);
extern stmt *sql_reorder(backend *be, stmt *order, list *exps, stmt *s, list *oexps, list *ostmts);
extern stmt *rel2bin_orderby(backend *be, sql_rel *rel, list *refs);

#endif /*_REL_ORDERBY_H_*/
