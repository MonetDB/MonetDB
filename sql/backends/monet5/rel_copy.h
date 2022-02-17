/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _REL_COPY_H_
#define _REL_COPY_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

#define COPY_BLOCKSIZE_SETTING "copy_blocksize"
#define COPY_PARALLEL_SETTING "copy_parallel"

#define DEFAULT_COPY_BLOCKSIZE (1024 * 1024)


extern stmt *rel2bin_copyparpipe(backend *be, sql_rel *rel, list *refs, sql_exp *copyfrom);
extern bool parallel_copy_enabled(void);


#endif /*_REL_COPY_H_*/
