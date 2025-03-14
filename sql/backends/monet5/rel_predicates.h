/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _REL_PREDICATES_H_
#define _REL_PREDICATES_H_

#include "rel_rel.h"
#include "rel_exp.h"
#include "mal_backend.h"

extern sql_rel *rel_predicates(backend *be, sql_rel *rel);

#endif /*_REL_PREDICATES_H_*/
