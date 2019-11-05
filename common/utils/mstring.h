/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

size_t strcpy_len(char *restrict dst, const char *restrict src, size_t n);
size_t strconcat_len(char *restrict dst, size_t n, const char *restrict src, ...);
