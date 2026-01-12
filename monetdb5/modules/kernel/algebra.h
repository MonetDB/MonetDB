/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef ALGEBRA_H
#define ALGEBRA_H

#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

extern str ALGprojection(Client ctx, bat *result, const bat *lid, const bat *rid);
extern str ALGfetchoid(Client ctx, ptr ret, const bat *bid, const oid *pos);

#endif
