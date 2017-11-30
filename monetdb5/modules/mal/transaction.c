/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
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
#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "bat5.h"

mal_export str TRNglobal_sync(bit *ret);
mal_export str TRNglobal_abort(bit *ret);
mal_export str TRNglobal_commit(bit *ret);
mal_export str TRNtrans_clean(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str TRNtrans_abort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str TRNtrans_commit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
mal_export str TRNsubcommit(bit *ret, bat *bid);

#include "mal_exception.h"
str
TRNglobal_sync(bit *ret)
{
	*ret = BBPsync(getBBPsize(), NULL) == GDK_SUCCEED;
	return MAL_SUCCEED;
}

str
TRNglobal_abort(bit *ret)
{
	TMabort();
	*ret = TRUE;
	return MAL_SUCCEED;
}

str
TRNglobal_commit(bit *ret)
{
	*ret = TMcommit()?FALSE:TRUE;
	return MAL_SUCCEED;
}
str
TRNsubcommit(bit *ret, bat *bid)
{
	BAT *b;
	b= BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "transaction.subcommit", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	*ret = TMsubcommit(b) == GDK_SUCCEED;
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
TRNtrans_clean(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	bat *bid;
	BAT *b;

	(void) cntxt;
	(void) mb;
	for (i = p->retc; i < p->argc; i++) {
		bid = getArgReference_bat(stk, p, i);
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "transaction.commit", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}

		BATfakeCommit(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

str
TRNtrans_abort(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	bat *bid;
	BAT *b;

	(void) cntxt;
	(void) mb;
	for (i = p->retc; i < p->argc; i++) {
		bid = getArgReference_bat(stk, p, i);
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "transaction.abort", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		BATundo(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

str
TRNtrans_commit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	bat *bid;
	BAT *b;

	(void) cntxt;
	(void) mb;
	for (i = p->retc; i < p->argc; i++) {
		bid = getArgReference_bat(stk, p, i);
		if ((b = BATdescriptor(*bid)) == NULL) {
			throw(MAL, "transaction.commit", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		BATcommit(b);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}
