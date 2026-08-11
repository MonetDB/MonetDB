/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_COPY_H_
#define _REL_COPY_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

#define COPY_BLOCKSIZE_SETTING "copy_blocksize"
#define COPY_PARALLEL_SETTING "copy_parallel"

#define DEFAULT_COPY_BLOCKSIZE (256 * 1024)
//#define DEFAULT_COPY_BLOCKSIZE (32 * 1024 * 1024)


extern stmt *exp2bin_copyparpipe(backend *be, sql_exp *copyfrom);
extern int parallel_copy_level(void);


#endif /*_REL_COPY_H_*/
