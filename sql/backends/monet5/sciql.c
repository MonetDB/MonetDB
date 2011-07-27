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

/*
 * @f sciql
 * @t SciQL runtime
 * @a N.Nes, M.Kersten, J.Zhang, M.Ivanova
 * @+ SciQL
 * This module contains the additions for the SciQL extensions of SQL.
 * This current focus is on testing the code generation.
 */
#include "monetdb_config.h"
#include "sql_user.h"
#include "sql_mvc.h"
#include "bat5.h"

str SCI1D(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"sciql.newDimension",PROGRAM_NYI);
}

str SCI2D(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"sciql.newDimension",PROGRAM_NYI);
}

str SCI3D(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"sciql.newDimension",PROGRAM_NYI);
}

str SCI4D(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"sciql.newDimension",PROGRAM_NYI);
}

str SCIdefault(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL,"sciql.setDefault",PROGRAM_NYI);
}
