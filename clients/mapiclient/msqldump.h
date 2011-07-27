/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

extern int describe_table(Mapi mid, char *schema, char *tname, stream *toConsole, int foreign);
extern int describe_sequence(Mapi mid, char *schema, char *sname, stream *toConsole);
extern int describe_schema(Mapi mid, char *sname, stream *toConsole);
extern int dump_table_data(Mapi mid, char *schema, char *tname, stream *toConsole, const char useInserts);
extern int dump_table(Mapi mid, char *schema, char *tname, stream *toConsole, int describe, int foreign, const char useInserts);
extern int dump_functions(Mapi mid, stream *toConsole, const char *sname, const char *fname);
extern int dump_database(Mapi mid, stream *toConsole, int describe, const char useInserts);
extern void dump_version(Mapi mid, stream *toConsole, const char *prefix);
extern int has_systemfunctions(Mapi mid);
