/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _BIN_SLICE_PARTITION_H_
#define _BIN_SLICE_PARTITION_H_

#include "sql_statement.h"
#include "mal_backend.h"

extern lng exp_getcard(mvc *sql, sql_rel *rel, sql_exp *e);
extern stmt *rel2bin_groupby_pp(backend *be, sql_rel *rel, list *refs);

#endif /*_BIN_SLICE_PARTITION_H_*/
