/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_PP_HASH_H_
#define _REL_PP_HASH_H_

#include "sql_statement.h"
#include "mal_backend.h"

extern stmt *rel2bin_oahash(backend *be, sql_rel *rel, list *refs);
extern stmt *rel2bin_oahash_build(backend *be, sql_rel *rel, list *refs);
extern stmt *oahash_slicer(backend *be, stmt *ht);

#endif /*_REL_PP_HASH_H_*/
