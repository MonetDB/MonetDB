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

#ifndef ALGEBRA_H
#define ALGEBRA_H

#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

extern str ALGprojection(bat *result, const bat *lid, const bat *rid);
extern str ALGfetchoid(ptr ret, const bat *bid, const oid *pos);

#endif
