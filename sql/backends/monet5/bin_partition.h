/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _BIN_PARTITION_H_
#define _BIN_PARTITION_H_

#include "mal_backend.h"

extern bool pp_can_not_start(mvc *sql, sql_rel *rel);

extern int pp_nr_slices(sql_rel *rel);

#endif /*_BIN_PARTITION_H_*/
