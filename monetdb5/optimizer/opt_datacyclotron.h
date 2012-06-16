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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/
#ifndef _OPT_DATACYCLOTRON_
#define _OPT_DATACYCLOTRON_

#include "opt_prelude.h"
#include "opt_support.h"
#include "mal_interpreter.h"

#define BIND_DATACYCLOTRON_OPT

#define SCHEMA_LENG     32 
#define TABLE_LENG      32 
#define COLUMN_LENG     32 
#define DCYPARTITIONS	200
#define DCYREGS		10000

typedef struct DCYCATALOG {
	char    schema[SCHEMA_LENG],
                table[TABLE_LENG],
                column[COLUMN_LENG];
	int 	access;
	int 	partitions;
	int 	*part_id;
	int 	*f_bun;
	int 	*l_bun;
	
	struct DCYCATALOG *next;
}DCYcatalog;

DCYcatalog *catalog;

opt_export str addRegWrap (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
opt_export DCYcatalog* addRegist( str sch, str tab, str col, int acc, int part, int f_bun, int l_bun );
opt_export DCYcatalog* findRegist( str sch, str tab, str col, int acc);
opt_export DCYcatalog* removePartRegist( str sch, str tab, str col, int acc, int part);
opt_export int dropRegist( str sch, str tab, str col, int acc );
opt_export str printRegists(void);

opt_export int OPTdatacyclotronImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
opt_export str DCYbind(int *ret, str *sch, str *tab, str *col, int *kind, int *part, int *fbun, int *lbun);
opt_export str DCYpin(int *ret, int *bid);
opt_export str DCYunpin(int *ret, int *bid);
opt_export str DCYcopy(int *ret_bat, int *ret_id, str *sch, str *tab, str *col, int *kind, int *part, int *fbun, int *lbun);

#define OPTDEBUGdatacyclotron  if ( optDebug & (1 <<DEBUG_OPT_DATACYCLOTRON) )
#endif
