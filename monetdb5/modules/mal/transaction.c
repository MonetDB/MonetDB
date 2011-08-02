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
 * @f transaction
 * @a M.L. Kersten, P. Boncz
 * @+ Transaction management
 * In the philosophy of Monet, transaction management overhead should only
 * be paid when necessary. Transaction management is for this purpose
 * implemented as a module.
 * This code base is largely absolute and should be re-considered when
 * serious OLTP is being supported.
 * Note, however, the SQL front-end obeys transaction semantics.
 *
 */
/*
 * @+ Implementation Code
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "bat5.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define transaction_export extern __declspec(dllimport)
#else
#define transaction_export extern __declspec(dllexport)
#endif
#else
#define transaction_export extern
#endif

transaction_export str TRNglobal_sync(bit *ret);
transaction_export str TRNglobal_abort(bit *ret);
transaction_export str TRNglobal_commit(bit *ret);
transaction_export str TRNsub_commit(bit *ret, int *bid);
transaction_export str TRNtrans_clean(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
transaction_export str TRNtrans_abort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
transaction_export str TRNtrans_commit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
transaction_export str TRNsubcommit(bit *ret, int *bid);
transaction_export str TRNtrans_prev(int *ret, int *bid);
transaction_export str TRNtrans_alpha(int *ret, int *bid);
transaction_export str TRNtrans_delta(int *ret, int *bid);

/*
 * @
 * @include prelude.mx
 * @- Wrappers
 * The remainder contains the Monet 5 wrapper code to make this all work
 */
#include "mal_exception.h"
str
TRNglobal_sync(bit *ret)
{
	*ret = BBPsync(BBPsize,NULL)?FALSE:TRUE;
	return MAL_SUCCEED;
}

str
TRNglobal_abort(bit *ret)
{
	*ret = TMabort()?FALSE:TRUE;
	return MAL_SUCCEED;
}

str
TRNglobal_commit(bit *ret)
{
	*ret = TMcommit()?FALSE:TRUE;
	return MAL_SUCCEED;
}
str
TRNsubcommit(bit *ret, int *bid)
{
	BAT *b;
	b= BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "transaction.subcommit", RUNTIME_OBJECT_MISSING);
	*ret = TMsubcommit(b)?FALSE:TRUE;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
TRNtrans_clean(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, *bid;
	BAT *b;

	(void) cntxt;
	(void) mb;
	for (i = p->retc; i < p->argc; i++) {
		bid = (int *) getArgReference(stk, p, i);
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "transaction.commit",  RUNTIME_OBJECT_MISSING);
		}

		BATfakeCommit(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

str
TRNtrans_abort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, *bid;
	BAT *b;

	(void) cntxt;
	(void) mb;
	for (i = p->retc; i < p->argc; i++) {
		bid = (int *) getArgReference(stk, p, i);
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "transaction.abort",  RUNTIME_OBJECT_MISSING);
		}
		BATundo(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

str
TRNtrans_commit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, *bid;
	BAT *b;

	(void) cntxt;
	(void) mb;
	for (i = p->retc; i < p->argc; i++) {
		bid = (int *) getArgReference(stk, p, i);
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "transaction.commit",  RUNTIME_OBJECT_MISSING);
		}
		BATcommit(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

str
TRNtrans_prev(int *ret, int *bid)
{
	BAT *b,*bn= NULL;
	b= BATdescriptor(*bid);
	if (b  == NULL)
		throw(MAL, "transaction.prev", RUNTIME_OBJECT_MISSING);
	bn = BATprev(b);
	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
TRNtrans_alpha(int *ret, int *bid)
{
	return BKCgetAlpha(ret, bid);
}

str
TRNtrans_delta(int *ret, int *bid)
{
	return BKCgetDelta(ret, bid);
}
