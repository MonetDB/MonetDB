/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _OPTIMIZER_H
#define _OPTIMIZER_H

#include "mal_interpreter.h"
#include "mal_namespace.h"
#include "opt_support.h"

extern str optimizer_epilogue(Client ctx, void *ret);

#endif /* _OPTIMIZER_H */
