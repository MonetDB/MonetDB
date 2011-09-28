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

#ifndef _MAL_ATOM_H
#define _MAL_ATOM_H

/* #define MAL_ATOM_DEBUG  */

#include "mal_instruction.h"
mal_export void malAtomDefinition(str name,int tpe);
mal_export int malAtomProperty(MalBlkPtr mb, InstrPtr pci);
mal_export int malAtomArray(int tpe, int idx);
mal_export int malAtomFixed(int size, int align, char *name);
mal_export int malAtomSize(int size, int align, char *name);
mal_export void showAtoms(stream *fd);  /* used in src/mal/mal_debugger.c */

#endif /*  _MAL_ATOM_H*/
