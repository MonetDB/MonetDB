/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_UTIL_H_
#define _REL_UTIL_H_

#include "sql_mvc.h"
#include "sql_relation.h"

#define is_equi_exp_(e) ((e)->flag == cmp_equal)

extern bool can_join_exp(sql_rel *rel, sql_exp *e, bool anti);
extern void split_join_exps(sql_rel *rel, list *joinable, list *not_joinable, bool anti, bool eqonly);

extern list *get_simple_equi_joins_first(mvc *sql, sql_rel *rel, list *exps);

#endif /*_REL_UTIL_H_*/
