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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) Martin Kersten
 * This module introduces a series of commands that provide access
 * to the help information stored in the runtime environment.
 *
 * The manual bulk operations ease offline inspection of all function definitions.
 */
#include "monetdb_config.h"
#include "manual.h"

str
MANUALcreateIndex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	dumpManualOverview(cntxt->fdout, cntxt->nspace, 1);
	return MAL_SUCCEED;
}

str
MANUALcreateSummary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	dumpManualHelp(cntxt->fdout, cntxt->nspace, 1);
	return MAL_SUCCEED;
}

str
MANUALcompletion(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *text= (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */
	dumpHelpTable(cntxt->fdout, cntxt->nspace, *text,1);
	return MAL_SUCCEED;
}

str
MANUALhelp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char **msg;
	int i;
	str *text= (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */

	msg= getHelp(cntxt->nspace,*text,1);
	if( msg && msg[0] ){
		for(i=0; msg[i];i++){
			mal_unquote(msg[i]);
			mnstr_printf(cntxt->fdout,"%s\n",msg[i]);
			GDKfree(msg[i]);
		}
	}
	if( msg)
		GDKfree(msg);
	return MAL_SUCCEED;
}

str
MANUALsearch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char **msg;
	int i;
	str *pat= (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */

	msg= getHelpMatch(*pat);
	if( msg && msg[0] ){
		for(i=0; msg[i];i++){
			mal_unquote(msg[i]);
			mnstr_printf(cntxt->fdout,"%s\n",msg[i]+1);
			GDKfree(msg[i]);
		}
	}
	if( msg)
		GDKfree(msg);
	return MAL_SUCCEED;
}

