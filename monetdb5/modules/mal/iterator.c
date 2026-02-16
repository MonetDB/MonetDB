/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * author M.L.Kersten
 * BAT Iterators
 * Many low level algorithms rely on an iterator to break a
 * collection into smaller pieces. Each piece is subsequently processed
 * by a block.
 *
 * For very large BATs it may make sense to break it into chunks
 * and process them separately to solve a query. An iterator pair is
 * provided to chop a BAT into fixed size elements.
 * Each chunk is made available as a BATview.
 * It provides read-only access to an underlying BAT. Adjusting the bounds
 * is cheap, once the BATview descriptor has been constructed.
 *
 * The smallest granularity is a single BUN, which can be used
 * to realize an iterator over the individual BAT elements.
 * For larger sized chunks, the operators return a BATview.
 *
 * All iterators require storage space to administer the
 * location of the next element. The BAT iterator module uses a simple
 * lng variable, which also acts as a cursor for barrier statements.
 *
 * The larger chunks produced are currently static, i.e.
 * their size is a parameter of the call. Dynamic chunk sizes
 * are interesting for time-series query processing. (See another module)
 *
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

/*
 * @-
 * The BUN- and BAT-stream manipulate a long handle, i.e.
 * the destination variable. It assumes it has been set to
 * zero as part of runtime stack initialization. Subsequently,
 * it fetches a bun and returns the increment to the control
 * variable. If it returns zero the control variable has been reset
 * to zero and end of stream has been reached.
 */
static str
ITRbunIterator(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *b;
	oid *head;
	bat *bid;
	ValPtr tail;

	(void) cntxt;
	(void) mb;
	head = getArgReference_oid(stk, pci, 0);
	tail = &stk->stk[pci->argv[1]];
	bid = getArgReference_bat(stk, pci, 2);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iterator.new", INTERNAL_BAT_ACCESS);
	}

	if (BATcount(b) == 0) {
		*head = oid_nil;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	*head = 0;

	bi = bat_iterator(b);
	if (VALinit(mb->ma, tail, ATOMtype(b->ttype), BUNtail(&bi, *head)) == NULL) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
		throw(MAL, "iterator.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bat_iterator_end(&bi);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

static str
ITRbunNext(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *b;
	oid *head;
	bat *bid;
	ValPtr tail;

	(void) cntxt;
	(void) mb;
	head = getArgReference_oid(stk, pci, 0);
	tail = &stk->stk[pci->argv[1]];
	bid = getArgReference_bat(stk, pci, 2);

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "iterator.next", INTERNAL_BAT_ACCESS);
	}

	*head = *head + 1;
	if (*head >= BATcount(b)) {
		*head = oid_nil;
		BBPunfix(b->batCacheid);
		return MAL_SUCCEED;
	}
	bi = bat_iterator(b);
	if (VALinit(mb->ma, tail, ATOMtype(b->ttype), BUNtail(&bi, *head)) == NULL) {
		bat_iterator_end(&bi);
		BBPunfix(b->batCacheid);
		throw(MAL, "iterator.next", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bat_iterator_end(&bi);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func iterator_init_funcs[] = {
 pattern("iterator", "new", ITRbunIterator, false, "Process the buns one by one extracted from a void table.", args(2,3, arg("h",oid),argany("t",1),batargany("b",1))),
 pattern("iterator", "next", ITRbunNext, false, "Produce the next bun for processing.", args(2,3, arg("h",oid),argany("t",1),batargany("b",1))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_iterator_mal)
{ mal_module("iterator", NULL, iterator_init_funcs); }
