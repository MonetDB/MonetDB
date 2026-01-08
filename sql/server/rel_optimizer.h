/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_OPTIMIZER_H_
#define _REL_OPTIMIZER_H_

#include "sql_mvc.h"
#include "sql_relation.h"

#define NSQLREWRITERS 29

extern sql_rel *rel_deadcode_elimination(mvc *sql, sql_rel *rel);
extern sql_rel *rel_optimizer(mvc *sql, sql_rel *rel, int profile, int instantiate, int value_based_opt, int storage_based_opt);
/* dead code elimination on sub relation */
extern sql_rel * rel_deadcode_elimination(mvc *sql, sql_rel *rel);


#endif /*_REL_OPTIMIZER_H_*/
