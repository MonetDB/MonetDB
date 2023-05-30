/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _BIN_PARTITION_H_
#define _BIN_PARTITION_H_

#include "sql_statement.h"
#include "mal_backend.h"

extern void set_need_pipeline(backend *be);
extern bool get_and_disable_need_pipeline(backend *be);

/* TODO inline part of .h */
extern void set_pipeline(backend *be, stmt *pp);
extern stmt * get_pipeline(backend *be);

extern bool pp_can_not_start(mvc *sql, sql_rel *rel);

extern int pp_nr_slices(sql_rel *rel);

#endif /*_BIN_PARTITION_H_*/
