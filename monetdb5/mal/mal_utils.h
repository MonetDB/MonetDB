/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef MAL_UTILS_H
#define MAL_UTILS_H
#include "mal.h"

mal_export str mal_quote(const char *msg, size_t size);
mal_export void mal_unquote(char *msg);

#endif /* MAL_UTILS_H */
