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

#ifndef _REL_BIN_H_
#define _REL_BIN_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

sql_export stmt * exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int depth, int reduce, int push);
extern stmt *output_rel_bin(backend *be, sql_rel *rel, int top);

#endif /*_REL_BIN_H_*/
