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
 * @f manual
 * @a Martin Kersten
 * @v 1
 * @+ Manual Inspection
 * This module introduces a series of commands that provide access
 * to the help information stored in the runtime environment.
 *
 * The manual bulk operations ease offline inspection of all function definitions.
 * It includes an XML organized file, because we expect external
 * tools to massage it further for presentation.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_resolve.h"
#include "mal_client.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_interpreter.h"
#include "mal_namespace.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define manual_export extern __declspec(dllimport)
#else
#define manual_export extern __declspec(dllexport)
#endif
#else
#define manual_export extern
#endif

manual_export str MANUALcreateSection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreate1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreate0(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALsearch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALhelp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreateIndex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcreateSummary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
manual_export str MANUALcompletion(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

/*
 * @+ Symbol table
 * Mal symbol table and environment analysis.
 */

str
MANUALcreateSection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = (str*) getArgReference(stk,pci,1);

	(void) mb;
	dumpManualSection(cntxt->fdout, findModule(cntxt->nspace, putName(*mod, strlen(*mod))));
	return MAL_SUCCEED;
}

str
MANUALcreate1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *mod = (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */
	dumpManualHeader(cntxt->fdout);
	dumpManual(cntxt->fdout, findModule(cntxt->nspace, putName(*mod, strlen(*mod))), 0);
	dumpManualFooter(cntxt->fdout);
	return MAL_SUCCEED;
}

str
MANUALcreate0(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	dumpManualHeader(cntxt->fdout);
	dumpManual(cntxt->fdout, cntxt->nspace, 1);
	dumpManualFooter(cntxt->fdout);
	return MAL_SUCCEED;
}
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

