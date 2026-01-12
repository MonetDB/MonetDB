/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef MAL_UTILS_H
#define MAL_UTILS_H

#ifndef LIBMONETDB5
#error this file should not be included outside its source directory
#endif

#include "gdk.h"

extern void mal_unquote(char *msg);

#endif /* MAL_UTILS_H */
