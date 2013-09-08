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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef SERVER_H
#define SERVER_H
/* #define DEBUG_SERVER */

#include "mal_client.h"
#include "mal_session.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_authorize.h"
#include "msabaoth.h"
#include "mcrypt.h"
#include <stream.h>

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mal_mapi_export extern __declspec(dllimport)
#else
#define mal_mapi_export extern __declspec(dllexport)
#endif
#else
#define mal_mapi_export extern
#endif


#define NEW_ARRAY( type, size )	(type*)GDKmalloc((size)*sizeof(type))
#define STREQ(a, b) 		(strcmp(a, b)==0)

#define SERVERPORT		50000
#define SERVERMAXUSERS 		5

mal_mapi_export str SERVERlisten(int *Port, str *Usockfile, int *Maxusers);
mal_mapi_export str SERVERlisten_default(int *ret);
mal_mapi_export str SERVERlisten_port(int *ret, int *pid);
mal_mapi_export str SERVERlisten_usock(int *ret, str *usock);
mal_mapi_export str SERVERstop(int *ret);
mal_mapi_export str SERVERsuspend(int *ret);
mal_mapi_export str SERVERresume(int *ret);
mal_mapi_export void SERVERexit(void);

mal_mapi_export str SERVERconnect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_mapi_export str SERVERdisconnectWithAlias(int *ret, str *db_alias);
mal_mapi_export str SERVERdisconnectALL(int *ret);
mal_mapi_export str SERVERreconnectAlias(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_mapi_export str SERVERreconnectWithoutAlias(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_mapi_export str SERVERtrace(int *ret, int *mid, int *flag);
mal_mapi_export str SERVERdisconnect(int *ret, int *mid);
mal_mapi_export str SERVERsetAlias(int *ret, int *mid, str *dbalias);
mal_mapi_export str SERVERlookup(int *ret, str *dbalias);
mal_mapi_export str SERVERdestroy(int *ret, int *mid);
mal_mapi_export str SERVERreconnect(int *ret, int *mid);
mal_mapi_export str SERVERping(int *ret, int *mid);
mal_mapi_export str SERVERquery(int *ret, int *mid, str *qry);
mal_mapi_export str SERVERquery_handle(int *ret, int *mid, str *qry);
mal_mapi_export str SERVERquery_array(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_mapi_export str SERVERprepare(int *ret, int *key, str *qry);
mal_mapi_export str SERVERexecute(int *ret, int *idx);
mal_mapi_export str SERVERfinish(int *ret, int *idx);
mal_mapi_export str SERVERrows_affected(lng *ret, int *idx);
mal_mapi_export str SERVERget_row_count(lng *ret, int *idx);
mal_mapi_export str SERVERget_field_count(int *ret, int *idx);
mal_mapi_export str SERVERfetch_row(int *ret, int *idx);
mal_mapi_export str SERVERfetch_all_rows(lng *ret, int *idx);
mal_mapi_export str SERVERfetch_field_str(str *ret, int *idx, int *fnr);
mal_mapi_export str SERVERfetch_field_int(int *ret, int *idx, int *fnr);
mal_mapi_export str SERVERfetch_field_lng(lng *ret, int *idx, int *fnr);
#ifdef HAVE_HGE
mal_mapi_export str SERVERfetch_field_hge(hge *ret, int *idx, int *fnr);
#endif
mal_mapi_export str SERVERfetch_field_sht(sht *ret, int *idx, int *fnr);
mal_mapi_export str SERVERfetch_field_void(oid *ret, int *idx, int *fnr);
mal_mapi_export str SERVERfetch_field_oid(oid *ret, int *idx, int *fnr);
mal_mapi_export str SERVERfetch_field_bte(bte *ret, int *idx, int *fnr);
mal_mapi_export str SERVERfetch_line(str *ret, int *key);
mal_mapi_export str SERVERnext_result(int *ret, int *key);
mal_mapi_export str SERVERfetch_reset(int *ret, int *key);
mal_mapi_export str SERVERfetch_field_bat(int *bid, int *idx);
mal_mapi_export str SERVERerror(int *ret, int *idx);
mal_mapi_export str SERVERgetError(str *ret, int *idx);
mal_mapi_export str SERVERexplain(str *ret, int *idx);
mal_mapi_export str SERVERmapi_rpc_single_row(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_mapi_export str SERVERmapi_rpc_single_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_mapi_export str SERVERput(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_mapi_export str SERVERputLocal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_mapi_export str SERVERbindBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_mapi_export str SERVERclient(int *res, stream **In, stream **Out);
mal_mapi_export str SERVERmapi_rpc_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* SERVER_H */
