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
 * @f dcoperator
 * @a Martin Kersten
 * @v 1
 * @+ Petri-net engine
 * This module is a prototype for DataCell operators.
 * @example
 * -see test directory
 * @end example
 */

#include "monetdb_config.h"
#include "dcoperator.h"
#include "mal_builder.h"

str DCselect(int *ret, int *bid, ptr low, ptr high)
{
	int *rdel = NULL;
	/*BAT *b;*/
	ALGselect(ret, bid, low, high);
	/*b= BATdescriptor(*ret);
	   BATprint(b);*/
	BBPunfix(*ret);
	/*printf("\nNow I will delete from the original bat the selected items\n");*/
	return BKCdelete_bat_bun(rdel, bid, ret);
}


str DCselectInsert(int *ret, int *res, int *bid, lng *low, lng *hgh)
{
	BAT *b, *r;

	lng *readerH, *writerH;
	lng *readerT, *writerT;

	BUN size, i;

	(void)ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "dc.selectInsert", "Cannot access input BAT");

	if ((r = BATdescriptor(*res)) == NULL)
		throw(MAL, "dc.selectInsert", "Cannot access result BAT");

	size = BATcount(b);

	if (size > BATcapacity(r) - BATcount(r)) {
		BUN ncap;
		BUN grows;
		BUN needed = size - (BATcapacity(r) - BATcount(r));
		ncap = BATcapacity(r) + needed;
		grows = BATgrows(r);
		if (ncap > grows)
			grows = ncap;
		if (BATextend(r, grows) == NULL)
			throw(MAL, "dc.selectInsert", "Failed to make room for the new values");
	}
/*printf("in dc.selectInsert size is "OIDFMT,size);*/
	writerH = (lng*)Hloc(r, BUNfirst(r));
	writerT = (lng*)Tloc(r, BUNfirst(r));

	readerH = (lng*)Hloc(b, BUNfirst(b));
	readerT = (lng*)Tloc(b, BUNfirst(b));

	for (i = 0; i < size; i++) {
		if (*readerT >= *low && *readerT <= *hgh) {
			*writerH = *readerH;
			*writerT = *readerT;

			writerH++;
			writerT++;
		}
		readerH++;
		readerT++;
	}
	BATsetcount(r, (BUN)(writerT - (lng*)Tloc(r, BUNfirst(r))));

	BBPunfix(*bid);
	BBPunfix(*res);

	return MAL_SUCCEED;
}

/*
 * @-
 * The operator below is only working for a very limited
 * case. It also re-uses oids, which may become a semantic
 * problem quickly.
 */
str DCdeleteUpperSlice(int *ret, int *bid, int *pos)
{
	BAT *b;
	int *readerT, *writerT;
	BUN size, i;

	(void)ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "dc.deleteUpperSlice", "Cannot access input BAT");
	/* check for a failure */
	assert(b != NULL);

	/* remove Hashes etc */
	if (b->H->hash)
		HASHremove(b);
	if (b->T->hash)
		HASHremove(BATmirror(b));

	size = BATcount(b);
	writerT = (int*)Tloc(b, BUNfirst(b));
	readerT = (int*)Tloc(b, BUNfirst(b)) + *pos;

	for (i = *pos; i < size; i++)
		*writerT++ = *readerT++;
	b->batInserted -= *pos;

	BATsetcount(b, (BUN)(writerT - (int*)Tloc(b, BUNfirst(b))));
	BBPunfix(*bid);
	b->batDirty = TRUE;
	return MAL_SUCCEED;
}


/*
 * @-
 * The operator below is only working for a very limited cases.
 */
str DCreplaceTailBasedOnHead(int *ret, int *res, int *bid)
{
	BAT *b, *r;
	oid *readerH_b;
	int *writerT_r, *readerT_b;
	BUN size_b, size_r, i;

	(void)ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "dc.replaceTailBasedOnHead", "Cannot access input BAT");
	/* check for a failure */
	assert(b != NULL);

	if ((r = BATdescriptor(*res)) == NULL)
		throw(MAL, "dc.replaceTailBasedOnHead", "Cannot access result BAT");
	/* check for a failure */
	assert(r != NULL);


	/* remove Hashes etc */
	if (r->H->hash)
		HASHremove(r);
	if (r->T->hash)
		HASHremove(BATmirror(r));

	size_r = BATcount(r);
	size_b = BATcount(b);


	if ((b->htype == TYPE_void) && (size_b == size_r)) {
		writerT_r = (int*)Tloc(r, BUNfirst(r));
		readerT_b = (int*)Tloc(b, BUNfirst(b));
		for (i = 0; i < size_r; i++) {
			*writerT_r = *readerT_b;
			writerT_r++;
			readerT_b++;
		}
	} else if ((b->htype != TYPE_void) && (size_b < size_r)) {
		readerH_b = (oid*)Hloc(b, BUNfirst(b));
		readerT_b = (int*)Tloc(b, BUNfirst(b));
		for (i = 0; i < size_b; i++) {
			writerT_r = (int*)Tloc(r, BUNfirst(r)) + *readerH_b;
			*writerT_r = *readerT_b;
			readerH_b++;
			readerT_b++;
		}
	}

	BBPunfix(*bid);
	BBPunfix(*res);
	r->batDirty = TRUE;
	return MAL_SUCCEED;
}


str DCselectInsertDelete(int *ret, int *res, int *bid, lng *low, lng *hgh)
{
	BAT *b, *r;

	lng *readerH, *writerH, *writerHr;
	lng *readerT, *writerT, *writerTr;

	BUN size, i;

	(void)ret;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "dc.selectInsertDelete", "Cannot access input BAT");

	if ((r = BATdescriptor(*res)) == NULL)
		throw(MAL, "dc.selectInsertDelete", "Cannot access result BAT");

	size = BATcount(b);

	if (size > BATcapacity(r) - BATcount(r)) {
		BUN ncap;
		BUN grows;
		BUN needed = size - (BATcapacity(r) - BATcount(r));
		ncap = BATcapacity(r) + needed;
		grows = BATgrows(r);
		if (ncap > grows)
			grows = ncap;
		if (BATextend(r, grows) == NULL)
			throw(MAL, "dcoperator.DCselectInsertDelete", "Failed to make room for the new values");
	}

	writerHr = (lng*)Hloc(r, BUNfirst(r));
	writerTr = (lng*)Tloc(r, BUNfirst(r));

	writerH = (lng*)Hloc(b, BUNfirst(b));
	writerT = (lng*)Tloc(b, BUNfirst(b));

	readerH = (lng*)Hloc(b, BUNfirst(b));
	readerT = (lng*)Tloc(b, BUNfirst(b));

	for (i = 0; i < size; i++) {
		if (*readerT < *low || *readerT > *hgh) {
			writerH++;
			writerT++;
			readerH++;
			readerT++;
		} else
			break;
	}

	for (; i < size; i++) {
		if (*readerT >= *low && *readerT <= *hgh) {
			*writerHr = *readerH;
			*writerTr = *readerT;

			writerHr++;
			writerTr++;
		} else {
			*writerH = *readerH;
			*writerT = *readerT;

			writerH++;
			writerT++;
		}
		readerH++;
		readerT++;
	}


	BATsetcount(b, (BUN)(writerT - (lng*)Tloc(b, BUNfirst(b))));
	BATsetcount(r, (BUN)(writerTr - (lng*)Tloc(r, BUNfirst(r))));

	BBPunfix(*bid);
	BBPunfix(*res);

	return MAL_SUCCEED;
}


str
DCsliceStrict(int *ret, bat *bid, lng *start, lng *end)
{
	BAT *b, *bn = NULL;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "dcoperator.sliceStrict", "Cannot access descriptor");
	}

	assert(*start >= 0);
	assert(*end >= 0);
	assert(*start <= (lng)BUN_MAX);
	assert(*end < (lng)BUN_MAX);
	assert(*start <= *end);

	if ((BUN)((*end - *start) + 1) > BATcount(b)) {
		bn = BATnew(b->htype, b->ttype, 0);
		BATsetcount(bn, 0);
		*ret = bn->batCacheid;
		BBPkeepref(*ret);
		return MAL_SUCCEED;
	}

	bn = BATslice(b, (BUN)*start, (BUN)*end + 1);

	BBPreleaseref(b->batCacheid);
	if (bn != NULL) {
		if (!(bn->batDirty & 2)) bn = BATsetaccess(bn, BAT_READ);
		*ret = bn->batCacheid;
		BBPkeepref(*ret);
		return MAL_SUCCEED;
	}
	throw(MAL, "dcoperator.sliceStrict", "GDKerror");
}



