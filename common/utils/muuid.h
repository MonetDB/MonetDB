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

#ifndef _SEEN_MUUID_H
#define _SEEN_MUUID_H 1

/* this function is (currently) only used in msabaoth and sql;
 * msabaoth is part of monetdb5 and we want this function to be
 * exported so that the call in sql can be satisfied by the version
 * that is included in monetdb5 */
#ifdef WIN32
#if !defined(LIBMONETDB5) && !defined(LIBMSABAOTH)
__declspec(dllimport)
#else
__declspec(dllexport)
#endif
#else
extern
#endif
char *generateUUID(void);

#endif
