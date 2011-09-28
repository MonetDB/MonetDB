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

#ifndef _MAL_BOX_H
#define _MAL_BOX_H
#include "mal_stack.h"
#include "mal_instruction.h"

/*#define DEBUG_MAL_BOX */

typedef struct BOX {
	MT_Lock lock;		/* provide exclusive access */
	str name;
	MalBlkPtr sym;
	MalStkPtr val;
	int dirty;		/* don't save if it hasn't been changed */
} *Box, BoxRecord;

mal_export Box findBox(str name);
mal_export Box openBox(str name);
mal_export int closeBox(str name, int flag);
mal_export void destroyBox(str name);
mal_export int saveBox(Box box, int flag);
mal_export void loadBox(str nme);
mal_export int releaseAllBox(Box box);

mal_export int depositBox(Box box, str name, int type, ValPtr val);
mal_export void insertToBox(Box box, str name, str val);
mal_export int takeBox(Box box, str name, ValPtr val, int tpe);
mal_export int bindBAT(Box box, str name, str location);
mal_export int releaseBox(Box box, str name);
mal_export int discardBox(Box box, str name);
mal_export str getBoxName(Box box, lng i);
mal_export str getBoxNames(int *bid);
mal_export str toString(Box box, lng i);
mal_export int nextBoxElement(Box box, lng *cursor, ValPtr v);

#endif /* _MAL_BOX_H */
