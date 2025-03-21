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

#ifndef _SQL_IMPORT_H
#define _SQL_IMPORT_H

#ifdef WIN32
#ifdef LIBSQL
#define sql_export extern __declspec(dllexport)
#else
#define sql_export extern __declspec(dllimport)
#endif
#else
#define sql_export extern
#endif

sql_export void sql_register(const char *name, const unsigned char *code);

#endif /* _SQL_IMPORT_H */
