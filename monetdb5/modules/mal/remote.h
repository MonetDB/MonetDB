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


#ifndef _REMOTE_DEF
#define _REMOTE_DEF


/* #define _DEBUG_REMOTE_	    trace the interaction */
/* #define _DEBUG_MAPI_		    trace mapi interaction */

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)

#define remote_export extern __declspec(dllimport)
#else
#define remote_export extern __declspec(dllexport)
#endif
#else
#define remote_export extern
#endif
remote_export str RMTprelude(int *ret);
remote_export str RMTepilogue(int *ret);
remote_export str RMTresolve(int *ret, str *pat);
remote_export str RMTconnectScen(
		str *ret,
		str *ouri,
		str *user,
		str *passwd,
		str *scen);
remote_export str RMTconnect(
		str *ret,
		str *uri,
		str *user,
		str *passwd);
remote_export str RMTdisconnect(Client cntxt, str *conn);
remote_export str RMTget(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTput(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTregisterInternal(Client cntxt, str conn, str mod, str fcn);
remote_export str RMTregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTbatload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTbincopyto(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTbincopyfrom(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTbintype(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
remote_export str RMTisalive(int *ret, str *conn);
#endif /* _REMOTE_DEF */
