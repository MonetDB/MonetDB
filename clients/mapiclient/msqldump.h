/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

extern int describe_table(Mapi mid, const char *schema, const char *tname, stream *toConsole, int foreign);
extern int describe_sequence(Mapi mid, const char *schema, const char *sname, stream *toConsole);
extern int describe_schema(Mapi mid, const char *sname, stream *toConsole);
extern int dump_table(Mapi mid, const char *schema, const char *tname, stream *toConsole, int describe, int foreign, char useInserts);
extern int dump_functions(Mapi mid, stream *toConsole, const char *sname, const char *fname);
extern int dump_database(Mapi mid, stream *toConsole, int describe, char useInserts);
extern void dump_version(Mapi mid, stream *toConsole, const char *prefix);
