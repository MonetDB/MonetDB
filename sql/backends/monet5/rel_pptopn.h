/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_TOPN_H_
#define _REL_TOPN_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

extern list *rel_topn_prepare_pp(backend *be, sql_rel *rel, stmt *all);
extern stmt *rel_pp_topn(backend *be, list *projectresults, stmt *sub, stmt *pp, stmt *o, stmt *l);
extern stmt *rel2bin_ordered_topn(backend *be, sql_rel *rel, list *refs, sql_rel *topn, stmt *all, stmt *offset, stmt *lim, list *projectresults);

#endif /*_REL_TOPN_H_*/
