/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef MAL_UTILS_H
#define MAL_UTILS_H

#ifndef LIBMONETDB5
#error this file should not be included outside its source directory
#endif

#include "gdk.h"

extern str mal_quote(const char *msg, size_t size);
extern void mal_unquote(char *msg);

#endif /* MAL_UTILS_H */
