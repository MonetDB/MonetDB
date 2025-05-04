/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _REL_PP_HASH_H_
#define _REL_PP_HASH_H_

#include "sql_statement.h"
#include "mal_backend.h"

extern stmt *rel2bin_oahash(backend *be, sql_rel *rel, list *refs);
extern stmt *rel2bin_oahash_build(backend *be, sql_rel *rel, list *refs);
extern void find_cmp_exps(list **exps_hsh, list **exps_prb, const list *exps, sql_rel *rel_hsh, sql_rel *rel_prb);

#endif /*_REL_PP_HASH_H_*/
