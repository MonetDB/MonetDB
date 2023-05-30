/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _BIN_SLICE_PARTITION_H_
#define _BIN_SLICE_PARTITION_H_

#include "mal_backend.h"

extern bool rel_groupby_2_phases(mvc *sql, sql_rel *rel);
extern bool rel_groupby_pp(sql_rel *rel, bool _2phases);
extern list *rel_groupby_prepare_pp(list **aggrresults, backend *be, sql_rel *rel, bool _2phases);

#endif /*_BIN_SLICE_PARTITION_H_*/
