/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @f blob
 * @v 1.0
 * @a Wilko Quak, Peter Boncz, M. Kersten, N. Nes
 * @+ The blob data type
 * The datatype 'blob' introduced here illustrates the power
 * in the hands of a programmer to extend the functionality of the
 * Monet GDK library. It consists of an interface specification for
 * the necessary operators, a startup routine to register the
 * type in thekernel, and some additional operators used outside
 * the kernel itself.
 *
 * The 'blob' data type is used in many database engines to
 * store a variable sized atomary value.
 * Its definition forms a generic base to store arbitrary structures
 * in the database, without knowing its internal coding, layout,
 * or interpretation.
 *
 * The blob memory layout consists of first 4 bytes containing
 * the bytes-size of the blob (excluding the integer), and then just binary data.
 *
 * @+ Module Definition
 */
#include "monetdb_config.h"
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

static str
BLOBnitems(int *ret, blob **b)
{
	if (is_blob_nil(*b)) {
		*ret = int_nil;
	} else {
		assert((*b)->nitems < INT_MAX);
		*ret = (int) (*b)->nitems;
	}
	return MAL_SUCCEED;
}

static str
BLOBnitems_bulk(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	int *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "blob.nitems_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "blob.nitems_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "blob.nitems_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const blob *b = BUNtvar(bi, p1);

			if (is_blob_nil(b)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				assert((int) b->nitems < INT_MAX);
				vals[i] = (int) b->nitems;
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const blob *b = BUNtvar(bi, p1);

			if (is_blob_nil(b)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				assert((int) b->nitems < INT_MAX);
				vals[i] = (int) b->nitems;
			}
		}
	}
	bat_iterator_end(&bi);

	BATsetcount(bn, ci1.ncand);
	bn->tnil = nils;
	bn->tnonil = !nils;
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	*res = bn->batCacheid;
	BBPkeepref(bn);
  bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (bs)
		BBPunfix(bs->batCacheid);
	return msg;
}

static str
BLOBtoblob(blob **retval, str *s)
{
	size_t len = strLen(*s);
	blob *b = (blob *) GDKmalloc(blobsize(len));

	if( b == NULL)
		throw(MAL, "blob.toblob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->nitems = len;
	memcpy(b->data, *s, len);
	*retval = b;
	return MAL_SUCCEED;
}

static str
BLOBblob_blob(blob **d, blob **s)
{
	size_t len = blobsize((*s)->nitems);
	blob *b;

	*d = b = GDKmalloc(len);
	if (b == NULL)
		throw(MAL,"blob", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b->nitems = (*s)->nitems;
	if (!is_blob_nil(b) && b->nitems != 0)
		memcpy(b->data, (*s)->data, b->nitems);
	return MAL_SUCCEED;
}

static str
BLOBblob_blob_bulk(bat *res, const bat *bid, const bat *sid)
{
	BAT *b = NULL, *s = NULL, *dst = NULL;
	BATiter bi;
	str msg = NULL;
	struct canditer ci;
	oid off;
	bool nils = false;

	if (sid && !is_bat_nil(*sid)) {
		if ((s = BATdescriptor(*sid)) == NULL) {
			msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
			goto bailout;
		}
	} else {
		BBPretain(*res = *bid); /* nothing to convert, return */
		return MAL_SUCCEED;
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	off = b->hseqbase;
	canditer_init(&ci, b, s);
	if (!(dst = COLnew(ci.hseq, TYPE_blob, ci.ncand, TRANSIENT))) {
		msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	if (ci.tpe == cand_dense) {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next_dense(&ci) - off);
			const blob *v = BUNtvar(bi, p);

			if (tfastins_nocheckVAR(dst, i, v) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				bat_iterator_end(&bi);
				goto bailout;
			}
			nils |= is_blob_nil(v);
		}
	} else {
		for (BUN i = 0; i < ci.ncand; i++) {
			oid p = (canditer_next(&ci) - off);
			const blob *v = BUNtvar(bi, p);

			if (tfastins_nocheckVAR(dst, i, v) != GDK_SUCCEED) {
				msg = createException(SQL, "batcalc.blob_blob_bulk", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				bat_iterator_end(&bi);
				goto bailout;
			}
			nils |= is_blob_nil(v);
		}
	}
	bat_iterator_end(&bi);

bailout:
	if (b)
		BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (dst && !msg) {
		BATsetcount(dst, ci.ncand);
		dst->tnil = nils;
		dst->tnonil = !nils;
		dst->tkey = BATcount(dst) <= 1;
		dst->tsorted = BATcount(dst) <= 1;
		dst->trevsorted = BATcount(dst) <= 1;
		*res = dst->batCacheid;
		BBPkeepref(dst);
	} else if (dst)
		BBPreclaim(dst);
	return msg;
}

static str
BLOBblob_fromstr(blob **b, const char **s)
{
	size_t len = 0;

	if (BATatoms[TYPE_blob].atomFromStr(*s, &len, (void **) b, false) < 0)
		throw(MAL, "blob", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static str
BLOBblob_fromstr_bulk(bat *res, const bat *bid, const bat *sid)
{
	BAT *b, *s = NULL, *bn;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.blob", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.blob", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = BATconvert(b, s, TYPE_blob, 0, 0, 0);
	BBPunfix(b->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (bn == NULL)
		throw(MAL, "batcalc.blob", GDK_EXCEPTION);
	*res = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func blob_init_funcs[] = {
 command("blob", "blob", BLOBblob_blob, false, "Noop routine.", args(1,2, arg("",blob),arg("s",blob))),
 command("blob", "blob", BLOBblob_fromstr, false, "", args(1,2, arg("",blob),arg("s",str))),
 command("blob", "toblob", BLOBtoblob, false, "store a string as a blob.", args(1,2, arg("",blob),arg("v",str))),
 command("blob", "nitems", BLOBnitems, false, "get the number of bytes in this blob.", args(1,2, arg("",int),arg("b",blob))),
 pattern("batblob", "nitems", BLOBnitems_bulk, false, "", args(1,2, batarg("",int),batarg("b",blob))),
 pattern("batblob", "nitems", BLOBnitems_bulk, false, "", args(1,3, batarg("",int),batarg("b",blob),batarg("s",oid))),
 command("calc", "blob", BLOBblob_blob, false, "", args(1,2, arg("",blob),arg("b",blob))),
 command("batcalc", "blob", BLOBblob_blob_bulk, false, "", args(1,3, batarg("",blob),batarg("b",blob),batarg("s",oid))),
 command("calc", "blob", BLOBblob_fromstr, false, "", args(1,2, arg("",blob),arg("s",str))),
 command("batcalc", "blob", BLOBblob_fromstr_bulk, false, "", args(1,3, batarg("",blob),batarg("b",str),batarg("s",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_blob_mal)
{ mal_module("blob", NULL, blob_init_funcs); }
