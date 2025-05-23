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

extern int describe_sequence(Mapi mid, const char *schema, const char *sname, stream *toConsole);
extern int describe_schema(Mapi mid, const char *sname, stream *toConsole);
extern int dump_table(Mapi mid, const char *schema, const char *tname, stream *sqlf, const char *ddir, const char *ext, bool describe, bool foreign, bool useInserts, bool databaseDump, bool noescape, bool percent);
extern int dump_functions(Mapi mid, stream *toConsole, char set_schema, const char *sname, const char *fname, const char *id);
extern int dump_database(Mapi mid, stream *sqlf, const char *ddir, const char *ext, bool describe, bool useInserts, bool noescape);
extern void dump_version(Mapi mid, stream *toConsole, const char *prefix);
extern char *sescape(const char *s);
