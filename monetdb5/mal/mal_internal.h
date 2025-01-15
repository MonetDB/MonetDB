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

/* This file should not be included in any file outside of the monetdb5 library */

#ifndef LIBMONETDB5
#error this file should not be included outside its source directory
#endif

void setqptimeout(lng usecs)
		__attribute__((__visibility__("hidden")));

extern size_t qsize;

extern MT_Lock mal_profileLock;
extern MT_Lock mal_copyLock;
extern MT_Lock mal_delayLock;
