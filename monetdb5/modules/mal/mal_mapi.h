/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef SERVER_H
#define SERVER_H
#ifdef HAVE_MAPI
/* #define DEBUG_SERVER */

#include "mal_client.h"
#include "mal_session.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_authorize.h"
#include "msabaoth.h"
#include "mcrypt.h"
#include "stream.h"
#include "streams.h"			/* for Stream */


#define NEW_ARRAY( type, size )	(type*)GDKmalloc((size)*sizeof(type))
#define STREQ(a, b) 		(strcmp(a, b)==0)

#define SERVERPORT		50000
#define SERVERMAXUSERS 		5

mal_export str SERVERlisten_default(int *ret);
mal_export str SERVERlisten_port(int *ret, int *pid);
mal_export str SERVERlisten_usock(int *ret, str *usock);
mal_export str SERVERstop(void *ret);
mal_export str SERVERsuspend(void *ret);
mal_export str SERVERresume(void *ret);

mal_export str SERVERconnect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_export str SERVERdisconnectWithAlias(int *ret, str *db_alias);
mal_export str SERVERdisconnectALL(int *ret);
mal_export str SERVERreconnectAlias(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_export str SERVERreconnectWithoutAlias(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_export str SERVERtrace(void *ret, int *mid, int *flag);
mal_export str SERVERdisconnect(void *ret, int *mid);
mal_export str SERVERsetAlias(void *ret, int *mid, str *dbalias);
mal_export str SERVERlookup(int *ret, str *dbalias);
mal_export str SERVERdestroy(void *ret, int *mid);
mal_export str SERVERreconnect(void *ret, int *mid);
mal_export str SERVERping(int *ret, int *mid);
mal_export str SERVERquery(int *ret, int *mid, str *qry);
mal_export str SERVERquery_handle(int *ret, int *mid, str *qry);
mal_export str SERVERquery_array(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pc);
mal_export str SERVERprepare(int *ret, int *key, str *qry);
mal_export str SERVERfinish(int *ret, int *idx);
mal_export str SERVERrows_affected(lng *ret, int *idx);
mal_export str SERVERget_row_count(lng *ret, int *idx);
mal_export str SERVERget_field_count(int *ret, int *idx);
mal_export str SERVERfetch_row(int *ret, int *idx);
mal_export str SERVERfetch_all_rows(lng *ret, int *idx);
mal_export str SERVERfetch_field_str(str *ret, int *idx, int *fnr);
mal_export str SERVERfetch_field_int(int *ret, int *idx, int *fnr);
mal_export str SERVERfetch_field_lng(lng *ret, int *idx, int *fnr);
#ifdef HAVE_HGE
mal_export str SERVERfetch_field_hge(hge *ret, int *idx, int *fnr);
#endif
mal_export str SERVERfetch_field_sht(sht *ret, int *idx, int *fnr);
mal_export str SERVERfetch_field_void(void *ret, int *idx, int *fnr);
mal_export str SERVERfetch_field_oid(oid *ret, int *idx, int *fnr);
mal_export str SERVERfetch_field_bte(bte *ret, int *idx, int *fnr);
mal_export str SERVERfetch_line(str *ret, int *key);
mal_export str SERVERnext_result(int *ret, int *key);
mal_export str SERVERfetch_reset(int *ret, int *key);
mal_export str SERVERfetch_field_bat(bat *bid, int *idx);
mal_export str SERVERerror(int *ret, int *idx);
mal_export str SERVERgetError(str *ret, int *idx);
mal_export str SERVERexplain(str *ret, int *idx);
mal_export str SERVERmapi_rpc_single_row(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str SERVERmapi_rpc_single_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str SERVERput(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str SERVERputLocal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str SERVERbindBAT(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str SERVERclient(void *res, const Stream *In, const Stream *Out);
mal_export str SERVERmapi_rpc_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /* HAVE_MAPI */
#endif /* SERVER_H */
