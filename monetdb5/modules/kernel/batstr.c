/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 *  M.L. Kersten
 * String multiplexes
 * [TODO: property propagations]
 * The collection of routines provided here are map operations
 * for the atom string primitives.
 *
 * In line with the batcalc module, we assume that if two bat operands
 * are provided that they are aligned.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include <ctype.h>
#include <string.h>
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "str.h"

/* In order to make avaialble a bulk version of a string function with candidates, all possible combinations of scalar/vector
	version of each argument must be avaiable for the function. Obviously this won't scale for functions with a large number of
	arguments, so we keep a blacklist for functions without candidate versions. */
static const char* batstr_funcs_with_no_cands[8] = {"lpad3","rpad3","splitpart","substitute","locate3","insert","replace",NULL};

bool
batstr_func_has_candidates(const char *func)
{
	for (size_t i = 0; batstr_funcs_with_no_cands[i]; i++)
		if (strcmp(batstr_funcs_with_no_cands[i], func) == 0)
			return false;
	return true;
}

static inline void
finalize_ouput(bat *res, BAT *bn, str msg, bool nils, BUN q)
{
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
}

static void
unfix_inputs(int nargs, ...)
{
	va_list valist;

	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		if (b)
			BBPunfix(b->batCacheid);
	}
	va_end(valist);
}

static inline str
str_prefix(str *buf, size_t *buflen, const char *s, int l)
{
	return str_Sub_String(buf, buflen, s, 0, l);
}

static str
do_batstr_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char *restrict))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
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
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if (strNil(x)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if (strNil(x)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatLength(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_int(cntxt, mb, stk, pci, "batstr.length", UTF8_strlen);
}

static str
STRbatBytes(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_int(cntxt, mb, stk, pci, "batstr.bytes", str_strlen);
}

static str
STRbatAscii(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals, next;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if ((msg = str_wchr_at(&next, x, 0)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if ((msg = str_wchr_at(&next, x, 0)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatFromWChr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = MAX(strlen(str_nil) + 1, 8);
	int *restrict vals, x;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	off1 = b->hseqbase;
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			x = vals[p1];

			if (is_int_nil(x)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					bat_iterator_end(&bi);
					msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				nils = true;
			} else {
				if ((msg = str_from_wchr(&buf, &buflen, vals[p1])) != MAL_SUCCEED) {
					bat_iterator_end(&bi);
					goto bailout;
				}
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					bat_iterator_end(&bi);
					msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			x = vals[p1];

			if (is_int_nil(x)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					bat_iterator_end(&bi);
					msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				nils = true;
			} else {
				if ((msg = str_from_wchr(&buf, &buflen, vals[p1])) != MAL_SUCCEED) {
					bat_iterator_end(&bi);
					goto bailout;
				}
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					bat_iterator_end(&bi);
					msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatSpace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals, x;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	const char space[]= " ", *s = space;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.search", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			x = vals[p1];

			if (is_int_nil(x) || x < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, s, x)) != MAL_SUCCEED)
					goto bailout;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			x = vals[p1];

			if (is_int_nil(x) || x < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, s, x)) != MAL_SUCCEED)
					goto bailout;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 3 ? getArgReference_bat(stk, pci, 2) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if (strNil(x)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if (strNil(x)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/* Input: a BAT of strings 'b' and a constant string 'y'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_conststr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, size_t buflen, str (*func)(str*, size_t*, const char*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	const char *y = *getArgReference_str(stk, pci, 2);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/* Input: a const string 'x' and a BAT of strings 'y'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_str_conststr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, size_t buflen, str (*func)(str*, size_t*, const char*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/* Input: two BATs of strings 'l' and 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batstr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, size_t buflen, str (*func)(str*, size_t*, const char*, const char*))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*l2 = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*l2))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

/* Input: a BAT of strings 'l' and a constant int 'y'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	int y = *getArgReference_int(stk, pci, 2);
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/* Input: a constant string 'x' and a BAT of integers 'y'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_int_conststr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	int y, *restrict inputs;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	inputs = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			y = inputs[p1];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			y = inputs[p1];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout;
				}
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/* Input: a BAT of strings 'l' and a BAT of integers 'n'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int *restrict righti, y;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*n = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*n))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(rs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, right, rs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
	bat_iterator_end(&bi);
bailout1:
	bat_iterator_end(&lefti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, ls, right, rs);
	return msg;
}

/* Input: a BAT of strings 'l', a constant int 'y' and a constant str 'z'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_conststr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	const char *z = *getArgReference_str(stk, pci, 3);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	int y = *getArgReference_int(stk, pci, 2);
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/* Input: a BAT of strings 'l', a BAT of integers 'n' and a constant str 'z'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_conststr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int, const char*))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	const char *z = *getArgReference_str(stk, pci, 3);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int *restrict righti, y;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*n = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*n))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(rs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, right, rs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
	bat_iterator_end(&bi);
bailout1:
	bat_iterator_end(&lefti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, ls, right, rs);
	return msg;
}

/* Input: a BAT of strings 'l', a constant int 'y' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_constint_batstr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int, const char*))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int y = *getArgReference_int(stk, pci, 2);
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*l2 = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*l2))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(rs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, right, rs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *z = BUNtvar(righti, p2);

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *z = BUNtvar(righti, p2);

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, ls, right, rs);
	return msg;
}

/* Input: a BAT of strings 'l', a BAT of int 'n' and a BAT of strings 'l2'
 * Output type: str (a BAT of strings)
 */
static str
do_batstr_batint_batstr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int, const char*))
{
	BATiter arg1i, arg3i, bi;
	BAT *bn = NULL, *arg1 = NULL, *arg1s = NULL, *arg2 = NULL, *arg2s = NULL, *arg3 = NULL, *arg3s = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	int *restrict arg2i, y;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0};
	oid off1, off2, off3;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*n = getArgReference_bat(stk, pci, 2), *l2 = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 7 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid3 = pci->argc == 7 ? getArgReference_bat(stk, pci, 6) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*n)) || !(arg3 = BATdescriptor(*l2))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(arg1s = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(arg2s = BATdescriptor(*sid2))) || (sid3 && !is_bat_nil(*sid3) && !(arg3s = BATdescriptor(*sid3)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, arg1, arg1s);
	if (canditer_init(&ci2, arg2, arg2s) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, arg3, arg3s) != q || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = arg1->hseqbase;
	off2 = arg2->hseqbase;
	off3 = arg3->hseqbase;
	arg1i = bat_iterator(arg1);
	bi = bat_iterator(arg2);
	arg2i = bi.base;
	arg3i = bat_iterator(arg3);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2), p3 = (canditer_next_dense(&ci3) - off3);
			const char *x = BUNtvar(arg1i, p1);
			y = arg2i[p2];
			const char *z = BUNtvar(arg3i, p3);

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2), p3 = (canditer_next(&ci3) - off3);
			const char *x = BUNtvar(arg1i, p1);
			y = arg2i[p2];
			const char *z = BUNtvar(arg3i, p3);

			if (strNil(x) || is_int_nil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&arg1i);
	bat_iterator_end(&arg3i);
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(6, arg1, arg1s, arg2, arg2s, arg3, arg3s);
	return msg;
}

static str
STRbatLower(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	if ((msg = str_case_hash_lock(false)))
		return msg;
	msg = do_batstr_str(cntxt, mb, stk, pci, "batstr.lower", str_lower);
	str_case_hash_unlock(false);
	return msg;
}

static str
STRbatUpper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	if ((msg = str_case_hash_lock(true)))
		return msg;
	msg = do_batstr_str(cntxt, mb, stk, pci, "batstr.upper", str_upper);
	str_case_hash_unlock(true);
	return msg;
}

static str
STRbatStrip(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str(cntxt, mb, stk, pci, "batstr.strip", str_strip);
}

static str
STRbatLtrim(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str(cntxt, mb, stk, pci, "batstr.ltrim", str_ltrim);
}

static str
STRbatRtrim(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str(cntxt, mb, stk, pci, "batstr.rtrim", str_rtrim);
}

static str
STRbatStrip2_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_conststr_str(cntxt, mb, stk, pci, "batstr.strip", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_strip2);
}

static str
STRbatLtrim2_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_conststr_str(cntxt, mb, stk, pci, "batstr.ltrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_ltrim2);
}

static str
STRbatRtrim2_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_conststr_str(cntxt, mb, stk, pci, "batstr.rtrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_rtrim2);
}

static str
STRbatStrip2_1st_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_conststr(cntxt, mb, stk, pci, "batstr.strip", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_strip2);
}

static str
STRbatLtrim2_1st_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_conststr(cntxt, mb, stk, pci, "batstr.ltrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_ltrim2);
}

static str
STRbatRtrim2_1st_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_conststr(cntxt, mb, stk, pci, "batstr.rtrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_rtrim2);
}

static str
STRbatStrip2_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batstr_str(cntxt, mb, stk, pci, "batstr.strip", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_strip2);
}

static str
STRbatLtrim2_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batstr_str(cntxt, mb, stk, pci, "batstr.ltrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_ltrim2);
}

static str
STRbatRtrim2_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batstr_str(cntxt, mb, stk, pci, "batstr.rtrim", INITIAL_STR_BUFFER_LENGTH * sizeof(int), str_rtrim2);
}

static str
STRbatLpad_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_constint_str(cntxt, mb, stk, pci, "batstr.lpad", str_lpad);
}

static str
STRbatRpad_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_constint_str(cntxt, mb, stk, pci, "batstr.rpad", str_rpad);
}

static str
STRbatLpad_1st_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_int_conststr(cntxt, mb, stk, pci, "batstr.lpad", str_lpad);
}

static str
STRbatRpad_1st_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_int_conststr(cntxt, mb, stk, pci, "batstr.rpad", str_rpad);
}

static str
STRbatLpad_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batint_str(cntxt, mb, stk, pci, "batstr.lpad", str_lpad);
}

static str
STRbatRpad_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batint_str(cntxt, mb, stk, pci, "batstr.rpad", str_rpad);
}

static str
STRbatLpad3_const_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_constint_conststr_str(cntxt, mb, stk, pci, "batstr.lpad", str_lpad3);
}

static str
STRbatRpad3_const_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_constint_conststr_str(cntxt, mb, stk, pci, "batstr.rpad", str_rpad3);
}

static str
STRbatLpad3_bat_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batint_conststr_str(cntxt, mb, stk, pci, "batstr.lpad", str_lpad3);
}

static str
STRbatRpad3_bat_const(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batint_conststr_str(cntxt, mb, stk, pci, "batstr.rpad", str_rpad3);
}

static str
STRbatLpad3_const_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_constint_batstr_str(cntxt, mb, stk, pci, "batstr.lpad", str_lpad3);
}

static str
STRbatRpad3_const_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_constint_batstr_str(cntxt, mb, stk, pci, "batstr.rpad", str_rpad3);
}

static str
STRbatLpad3_bat_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batint_batstr_str(cntxt, mb, stk, pci, "batstr.lpad", str_lpad3);
}

static str
STRbatRpad3_bat_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_batint_batstr_str(cntxt, mb, stk, pci, "batstr.rpad", str_rpad3);
}

/*
 * A general assumption in all cases is the bats are synchronized on their
 * head column. This is not checked and may be mis-used to deploy the
 * implementation for shifted window arithmetic as well.
 */

static str
prefix_or_suffix(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, bit (*func)(const char*, const char*))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	bit *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	}
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatPrefix(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return prefix_or_suffix(cntxt, mb, stk, pci, "batstr.startsWith", str_is_prefix);
}

static str
STRbatSuffix(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return prefix_or_suffix(cntxt, mb, stk, pci, "batstr.endsWith", str_is_suffix);
}

static str
prefix_or_suffix_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, bit (*func)(const char*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	bit *restrict vals;
	const char *y = *getArgReference_str(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = bit_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatPrefixcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return prefix_or_suffix_cst(cntxt, mb, stk, pci, "batstr.startsWith", str_is_prefix);
}

static str
STRbatSuffixcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return prefix_or_suffix_cst(cntxt, mb, stk, pci, "batstr.endsWith", str_is_suffix);
}

static str
prefix_or_suffix_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, bit (*func)(const char*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	bit *restrict vals;
	const char *x = *getArgReference_str(stk, pci, 1);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_bit, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = bit_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatPrefix_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return prefix_or_suffix_strcst(cntxt, mb, stk, pci, "batstr.startsWith", str_is_prefix);
}

static str
STRbatSuffix_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return prefix_or_suffix_strcst(cntxt, mb, stk, pci, "batstr.endsWith", str_is_suffix);
}

static str
search_string_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char*, const char*))
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	int *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	}
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatstrSearch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return search_string_bat(cntxt, mb, stk, pci, "batstr.search", str_search);
}

static str
STRbatRstrSearch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return search_string_bat(cntxt, mb, stk, pci, "batstr.r_search", str_reverse_str_search);
}

static str
search_string_bat_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals;
	const char *y = *getArgReference_str(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrSearchcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return search_string_bat_cst(cntxt, mb, stk, pci, "batstr.search", str_search);
}

static str
STRbatRstrSearchcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return search_string_bat_cst(cntxt, mb, stk, pci, "batstr.r_search", str_reverse_str_search);
}

static str
search_string_bat_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals;
	const char *x = *getArgReference_str(stk, pci, 1);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrSearch_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return search_string_bat_strcst(cntxt, mb, stk, pci, "batstr.search", str_search);
}

static str
STRbatRstrSearch_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return search_string_bat_strcst(cntxt, mb, stk, pci, "batstr.r_search", str_reverse_str_search);
}

static str
STRbatWChrAt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, bi;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti, *restrict vals, next, y;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.unicodeAt", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&lefti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatWChrAtcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2), *restrict vals, next;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatWChrAt_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y, *restrict vals, *restrict input, next;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	input = bi.base;
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			y = input[p1];

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			y = input[p1];

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_str_int_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatprefixcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int_cst(cntxt, mb, stk, pci, "batstr.prefix", str_prefix);
}

static str
STRbatsuffixcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int_cst(cntxt, mb, stk, pci, "batstr.suffix", str_suffix);
}

static str
STRbatTailcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int_cst(cntxt, mb, stk, pci, "batstr.tail", str_tail);
}

static str
STRbatsubstringTailcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int_cst(cntxt, mb, stk, pci, "batstr.substring", str_substring_tail);
}

static str
STRbatrepeatcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || y < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || y < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals, y;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			y = vals[p1];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			y = vals[p1];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatprefix_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_strcst(cntxt, mb, stk, pci, "batstr.prefix", str_prefix);
}

static str
STRbatsuffix_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_strcst(cntxt, mb, stk, pci, "batstr.suffix", str_suffix);
}

static str
STRbatTail_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_strcst(cntxt, mb, stk, pci, "batstr.tail", str_tail);
}

static str
STRbatsubstringTail_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_strcst(cntxt, mb, stk, pci, "batstr.substring", str_substring_tail);
}

static str
STRbatrepeat_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict vals, y;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			y = vals[p1];

			if (strNil(x) || is_int_nil(y) || y < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			y = vals[p1];

			if (strNil(x) || is_int_nil(y) || y < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_str_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti, y;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = (*func)(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&lefti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatprefix(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int(cntxt, mb, stk, pci, "batstr.prefix", str_prefix);
}

static str
STRbatsuffix(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int(cntxt, mb, stk, pci, "batstr.suffix", str_suffix);
}

static str
STRbatTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int(cntxt, mb, stk, pci, "batstr.tail", str_tail);
}

static str
STRbatsubstringTail(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return do_batstr_str_int(cntxt, mb, stk, pci, "batstr.substring", str_substring_tail);
}

static str
STRbatrepeat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict righti, y;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, lefts);
	if (canditer_init(&ci2, right, rights) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.repeat", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y) || y < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if (strNil(x) || is_int_nil(y) || y < 0) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_repeat(&buf, &buflen, x, y)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&lefti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatSubstitutecst_imp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int cand_nargs, const bit *rep)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	const char *y = *getArgReference_str(stk, pci, 2), *z = *getArgReference_str(stk, pci, 3);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	bit w = *rep;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == cand_nargs ? getArgReference_bat(stk, pci, cand_nargs - 1) : NULL;

	if (!buf) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	(void) cntxt;
	(void) mb;
	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y) || strNil(z) || is_bit_nil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_substitute(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y) || strNil(z) || is_bit_nil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_substitute(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatSubstitutecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const bit *rep = getArgReference_bit(stk, pci, 0);
	return STRbatSubstitutecst_imp(cntxt, mb, stk, pci, 6, rep);
}

static str
STRbatSubstitute(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter arg1i, arg2i, arg3i;
	BAT *bn = NULL, *arg1 = NULL, *arg1s = NULL, *arg2 = NULL, *arg2s = NULL, *arg3 = NULL, *arg3s = NULL, *arg4 = NULL, *arg4s = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	bit *restrict arg4i, w;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0}, ci4 = {0};
	oid off1, off2, off3, off4;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2), *s = getArgReference_bat(stk, pci, 3),
		*rep = getArgReference_bat(stk, pci, 4),
		*sid1 = pci->argc == 9 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid2 = pci->argc == 9 ? getArgReference_bat(stk, pci, 6) : NULL,
		*sid3 = pci->argc == 9 ? getArgReference_bat(stk, pci, 7) : NULL,
		*sid4 = pci->argc == 9 ? getArgReference_bat(stk, pci, 8) : NULL;
	BATiter bi;

	if (!buf) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*r)) || !(arg3 = BATdescriptor(*s)) || !(arg4 = BATdescriptor(*rep))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(arg1s = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(arg2s = BATdescriptor(*sid2))) ||
		(sid3 && !is_bat_nil(*sid3) && !(arg2s = BATdescriptor(*sid3))) || (sid4 && !is_bat_nil(*sid4) && !(arg4s = BATdescriptor(*sid4)))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, arg1, arg1s);
	if (canditer_init(&ci2, arg2, arg2s) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, arg3, arg3s) != q ||
		ci2.hseq != ci3.hseq || canditer_init(&ci4, arg4, arg4s) != q || ci3.hseq != ci4.hseq) {
		msg = createException(MAL, "batstr.substritute", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	(void) cntxt;
	(void) mb;
	off1 = arg1->hseqbase;
	off2 = arg2->hseqbase;
	off3 = arg3->hseqbase;
	off4 = arg4->hseqbase;
	arg1i = bat_iterator(arg1);
	arg2i = bat_iterator(arg2);
	arg3i = bat_iterator(arg3);
	bi = bat_iterator(arg4);
	arg4i = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense && ci4.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2),
				p3 = (canditer_next_dense(&ci3) - off3), p4 = (canditer_next_dense(&ci4) - off4);
			const char *x = BUNtvar(arg1i, p1);
			const char *y = BUNtvar(arg2i, p2);
			const char *z = BUNtvar(arg3i, p3);
			w = arg4i[p4];

			if (strNil(x) || strNil(y) || strNil(z) || is_bit_nil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_substitute(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2),
				p3 = (canditer_next(&ci3) - off3), p4 = (canditer_next(&ci4) - off4);
			const char *x = BUNtvar(arg1i, p1);
			const char *y = BUNtvar(arg2i, p2);
			const char *z = BUNtvar(arg3i, p3);
			w = arg4i[p4];

			if (strNil(x) || strNil(y) || strNil(z) || is_bit_nil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_substitute(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&arg1i);
	bat_iterator_end(&arg2i);
	bat_iterator_end(&arg3i);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(8, arg1, arg1, arg2, arg2s, arg3, arg3s, arg4, arg4s);
	return msg;
}

static str
STRbatsplitpartcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int z = *getArgReference_int(stk, pci, 3);
	const char *y = *getArgReference_str(stk, pci, 2);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsplitpart_needlecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi, fi;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *f = NULL, *fs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict field, z;
	const char *y = *getArgReference_str(stk, pci, 2);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*fid = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;

	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(f = BATdescriptor(*fid))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(fs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (canditer_init(&ci2, f, fs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	(void) cntxt;
	(void) mb;
	off1 = b->hseqbase;
	off2 = f->hseqbase;
	bi = bat_iterator(b);
	fi = bat_iterator(f);
	field = fi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			z = field[p2];

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			z = field[p2];

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&fi);
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, b, bs, f, fs);
	return msg;
}

static str
STRbatsplitpart_fieldcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi, ni;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *n = NULL, *ns = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int z = *getArgReference_int(stk, pci, 3);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*nid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(n = BATdescriptor(*nid))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(ns = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (canditer_init(&ci2, n, ns) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = n->hseqbase;
	bi = bat_iterator(b);
	ni = bat_iterator(n);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			const char *y = BUNtvar(ni, p2);

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			const char *y = BUNtvar(ni, p2);

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&ni);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, b, bs, n, ns);
	return msg;
}

static str
STRbatsplitpart(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter arg1i, arg2i;
	BAT *bn = NULL, *arg1 = NULL, *arg1s = NULL, *arg2 = NULL, *arg2s = NULL, *arg3 = NULL, *arg3s = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *restrict arg3i, z;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0};
	oid off1, off2, off3;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2), *t = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 7 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid3 = pci->argc == 7 ? getArgReference_bat(stk, pci, 6) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*r)) || !(arg3 = BATdescriptor(*t))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(arg1s = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(arg2s = BATdescriptor(*sid2))) || (sid3 && !is_bat_nil(*sid3) && !(arg3s = BATdescriptor(*sid3)))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, arg1, arg1s);
	if (canditer_init(&ci2, arg2, arg2s) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, arg3, arg3s) != q || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = arg1->hseqbase;
	off2 = arg2->hseqbase;
	off3 = arg3->hseqbase;
	arg1i = bat_iterator(arg1);
	arg2i = bat_iterator(arg2);
	bi = bat_iterator(arg3);
	arg3i = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2), p3 = (canditer_next_dense(&ci3) - off3);
			const char *x = BUNtvar(arg1i, p1);
			const char *y = BUNtvar(arg2i, p2);
			z = arg3i[p3];

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2), p3 = (canditer_next(&ci3) - off3);
			const char *x = BUNtvar(arg1i, p1);
			const char *y = BUNtvar(arg2i, p2);
			z = arg3i[p3];

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_splitpart(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&arg1i);
	bat_iterator_end(&arg2i);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(6, arg1, arg1s, arg2, arg2s, arg3, arg3s);
	return msg;
}

static str
STRbatReplacecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit rep = TRUE;

	return STRbatSubstitutecst_imp(cntxt, mb, stk, pci, 5, &rep);
}

static str
STRbatReplace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter arg1i, arg2i, arg3i;
	BAT *bn = NULL, *arg1 = NULL, *arg1s = NULL, *arg2 = NULL, *arg2s = NULL, *arg3 = NULL, *arg3s = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0};
	oid off1, off2, off3;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*s = getArgReference_bat(stk, pci, 2), *s2 = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 7 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid3 = pci->argc == 7 ? getArgReference_bat(stk, pci, 6) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(arg1 = BATdescriptor(*l)) || !(arg2 = BATdescriptor(*s)) || !(arg3 = BATdescriptor(*s2))) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(arg1s = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(arg2s = BATdescriptor(*sid2))) || (sid3 && !is_bat_nil(*sid3) && !(arg3s = BATdescriptor(*sid3)))) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, arg1, arg1s);
	if (canditer_init(&ci2, arg2, arg2s) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, arg3, arg3s) != q || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.replace", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = arg1->hseqbase;
	off2 = arg2->hseqbase;
	off3 = arg3->hseqbase;
	arg1i = bat_iterator(arg1);
	arg2i = bat_iterator(arg2);
	arg3i = bat_iterator(arg3);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2), p3 = (canditer_next_dense(&ci3) - off3);
			const char *x = BUNtvar(arg1i, p1);
			const char *y = BUNtvar(arg2i, p2);
			const char *z = BUNtvar(arg3i, p3);

			if (strNil(x) || strNil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_substitute(&buf, &buflen, x, y, z, TRUE)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2), p3 = (canditer_next(&ci3) - off3);
			const char *x = BUNtvar(arg1i, p1);
			const char *y = BUNtvar(arg2i, p2);
			const char *z = BUNtvar(arg3i, p3);

			if (strNil(x) || strNil(y) || strNil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_substitute(&buf, &buflen, x, y, z, TRUE)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.replace", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&arg1i);
	bat_iterator_end(&arg2i);
	bat_iterator_end(&arg3i);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(6, arg1, arg1s, arg2, arg2s, arg3, arg3s);
	return msg;
}

static str
STRbatInsert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, righti, starti, ncharsi;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *start = NULL, *ss = NULL, *nchars = NULL, *ns = NULL, *right = NULL, *rs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *sval, *lval, y, z;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0}, ci4 = {0};
	oid off1, off2, off3, off4;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*s = getArgReference_bat(stk, pci, 2), *chars = getArgReference_bat(stk, pci, 3),
		*s2 = getArgReference_bat(stk, pci, 4),
		*sid1 = pci->argc == 9 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid2 = pci->argc == 9 ? getArgReference_bat(stk, pci, 6) : NULL,
		*sid3 = pci->argc == 9 ? getArgReference_bat(stk, pci, 7) : NULL,
		*sid4 = pci->argc == 9 ? getArgReference_bat(stk, pci, 8) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(start = BATdescriptor(*s)) || !(nchars = BATdescriptor(*chars)) || !(right = BATdescriptor(*s2))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rs = BATdescriptor(*sid2))) ||
		(sid3 && !is_bat_nil(*sid3) && !(ss = BATdescriptor(*sid3))) || (sid4 && !is_bat_nil(*sid4) && !(ns = BATdescriptor(*sid4)))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, start, ss) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, nchars, ns) != q ||
		ci2.hseq != ci3.hseq || canditer_init(&ci4, right, rs) != q || ci3.hseq != ci4.hseq) {
		msg = createException(MAL, "batstr.insert", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = start->hseqbase;
	off3 = nchars->hseqbase;
	off4 = right->hseqbase;
	lefti = bat_iterator(left);
	starti = bat_iterator(start);
	ncharsi = bat_iterator(nchars);
	sval = starti.base;
	lval = ncharsi.base;
	righti = bat_iterator(right);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense && ci4.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2),
				p3 = (canditer_next_dense(&ci3) - off3), p4 = (canditer_next_dense(&ci4) - off4);
			const char *x = BUNtvar(lefti, p1);
			y = sval[p2];
			z = lval[p3];
			const char *w = BUNtvar(righti, p4);

			if (strNil(x) || is_int_nil(y) || is_int_nil(z) || strNil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_insert(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2),
				p3 = (canditer_next(&ci3) - off3), p4 = (canditer_next(&ci4) - off4);
			const char *x = BUNtvar(lefti, p1);
			y = sval[p2];
			z = lval[p3];
			const char *w = BUNtvar(righti, p4);

			if (strNil(x) || is_int_nil(y) || is_int_nil(z) || strNil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_insert(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&starti);
	bat_iterator_end(&ncharsi);
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(8, left, ls, start, ss, nchars, ns, right, rs);
	return msg;
}

static str
STRbatInsertcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2), z = *getArgReference_int(stk, pci, 3);
	const char *w = *getArgReference_str(stk, pci, 4);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || is_int_nil(z) || strNil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_insert(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || is_int_nil(z) || strNil(w)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_insert(&buf, &buflen, x, y, z, w)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

/*
 * The substring functions require slightly different arguments
 */
static str
STRbatsubstring_2nd_3rd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2), z = *getArgReference_int(stk, pci, 3);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsubstring_1st_2nd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2), z, *restrict input;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	input = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			z = input[p1];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			z = input[p1];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsubstring_1st_3rd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y, z = *getArgReference_int(stk, pci, 3), *restrict input;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;
	BATiter bi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	input = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			y = input[p1];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			y = input[p1];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsubstring_1st_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL, *lb = NULL, *lbs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y, z, *vals1, *vals2;
	const char *x = *getArgReference_str(stk, pci, 1);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2),
		*l = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;
	BATiter bi;
	BATiter lbi;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(lb = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(lbs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (canditer_init(&ci2, lb, lbs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = lb->hseqbase;
	bi = bat_iterator(b);
	lbi = bat_iterator(lb);
	vals1 = bi.base;
	vals2 = lbi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			y = vals1[p1];
			z = vals2[p2];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			y = vals1[p1];
			z = vals2[p2];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&bi);
	bat_iterator_end(&lbi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, b, bs, lb, lbs);
	return msg;
}

static str
STRbatsubstring_2nd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BATiter lbi;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *lb = NULL, *lbs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int y = *getArgReference_int(stk, pci, 2), *len, z;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*l = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(lb = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(lbs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (canditer_init(&ci2, lb, lbs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = lb->hseqbase;
	bi = bat_iterator(b);
	lbi = bat_iterator(lb);
	len = lbi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			z = len[p2];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			z = len[p2];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&lbi);
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, b, bs, lb, lbs);
	return msg;
}

static str
STRbatsubstring_3rd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi, lbi;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *lb = NULL, *lbs = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *start, y, z = *getArgReference_int(stk, pci, 3);
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1),
		*l = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 6 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 6 ? getArgReference_bat(stk, pci, 5) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(b = BATdescriptor(*bid)) || !(lb = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(lbs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (canditer_init(&ci2, lb, lbs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = lb->hseqbase;
	bi = bat_iterator(b);
	lbi = bat_iterator(lb);
	start = lbi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			y = start[p2];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(bi, p1);
			y = start[p2];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&lbi);
	bat_iterator_end(&bi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, b, bs, lb, lbs);
	return msg;
}

static str
STRbatsubstring(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, starti, lengthi;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *start = NULL, *ss = NULL, *length = NULL, *lens = NULL;
	BUN q = 0;
	size_t buflen = INITIAL_STR_BUFFER_LENGTH;
	int *svals, *lvals, y, z;
	str buf = GDKmalloc(buflen), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0};
	oid off1, off2, off3;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2), *t = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 7 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid3 = pci->argc == 7 ? getArgReference_bat(stk, pci, 6) : NULL;

	(void) cntxt;
	(void) mb;
	if (!buf) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*l)) || !(start = BATdescriptor(*r)) || !(length = BATdescriptor(*t))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(ss = BATdescriptor(*sid2))) || (sid3 && !is_bat_nil(*sid3) && !(lens = BATdescriptor(*sid3)))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, start, ss) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, length, lens) != q || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = start->hseqbase;
	off3 = length->hseqbase;
	lefti = bat_iterator(left);
	starti = bat_iterator(start);
	lengthi = bat_iterator(length);
	svals = starti.base;
	lvals = lengthi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2), p3 = (canditer_next_dense(&ci3) - off3);
			const char *x = BUNtvar(lefti, p1);
			y = svals[p2];
			z = lvals[p3];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2), p3 = (canditer_next(&ci3) - off3);
			const char *x = BUNtvar(lefti, p1);
			y = svals[p2];
			z = lvals[p3];

			if (strNil(x) || is_int_nil(y) || is_int_nil(z)) {
				if (tfastins_nocheckVAR(bn, i, str_nil) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
				nils = true;
			} else {
				if ((msg = str_sub_string(&buf, &buflen, x, y, z)) != MAL_SUCCEED)
					goto bailout1;
				if (tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
					msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto bailout1;
				}
			}
		}
	}
bailout1:
	bat_iterator_end(&lefti);
	bat_iterator_end(&starti);
	bat_iterator_end(&lengthi);
bailout:
	GDKfree(buf);
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(6, left, ls, start, ss, length, lens);
	return msg;
}

static str
STRbatstrLocatecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals;
	const char *y = *getArgReference_str(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, 1);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, 1);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrLocate_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals;
	const char *x = *getArgReference_str(stk, pci, 1);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 4 ? getArgReference_bat(stk, pci, 3) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, 1);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *y = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, 1);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrLocate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL;
	BUN q = 0;
	int *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 3) : NULL,
		*sid2 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(rs = BATdescriptor(*sid2)))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, right, rs) != q || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.locate", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, 1);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, 1);
			}
		}
	}
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(4, left, ls, right, rs);
	return msg;
}

static str
STRbatstrLocate3cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	BUN q = 0;
	int *restrict vals, z = *getArgReference_int(stk, pci, 3);
	const char *y = *getArgReference_str(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*sid1 = pci->argc == 5 ? getArgReference_bat(stk, pci, 4) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(b = BATdescriptor(*l))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, z);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, z);
			}
		}
	}
	bat_iterator_end(&bi);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrLocate3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, righti, starti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL, *start = NULL, *ss = NULL;
	BUN q = 0;
	int *restrict vals, *restrict svals, z;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0}, ci3 = {0};
	oid off1, off2, off3;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2), *s = getArgReference_bat(stk, pci, 3),
		*sid1 = pci->argc == 7 ? getArgReference_bat(stk, pci, 4) : NULL,
		*sid2 = pci->argc == 7 ? getArgReference_bat(stk, pci, 5) : NULL,
		*sid3 = pci->argc == 7 ? getArgReference_bat(stk, pci, 6) : NULL;

	(void) cntxt;
	(void) mb;
	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r)) || !(start = BATdescriptor(*s))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(ls = BATdescriptor(*sid1))) ||
		(sid2 && !is_bat_nil(*sid2) && !(rs = BATdescriptor(*sid2))) || (sid3 && !is_bat_nil(*sid3) && !(ss = BATdescriptor(*sid3)))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = canditer_init(&ci1, left, ls);
	if (canditer_init(&ci2, right, rs) != q || ci1.hseq != ci2.hseq || canditer_init(&ci3, start, ss) != q || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.locate2", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, q, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	off3 = start->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	starti = bat_iterator(start);
	svals = starti.base;
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense && ci3.tpe == cand_dense) {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2), p3 = (canditer_next_dense(&ci3) - off3);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);
			z = svals[p3];

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, z);
			}
		}
	} else {
		for (BUN i = 0; i < q; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2), p3 = (canditer_next(&ci3) - off3);
			const char *x = BUNtvar(lefti, p1);
			const char *y = BUNtvar(righti, p2);
			z = svals[p3];

			if (strNil(x) || strNil(y) || is_int_nil(z)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = str_locate2(x, y, z);
			}
		}
	}
	bat_iterator_end(&starti);
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
bailout:
	finalize_ouput(res, bn, msg, nils, q);
	unfix_inputs(6, left, ls, right, rs, start, ss);
	return msg;
}

#include "mel.h"
mel_func batstr_init_funcs[] = {
 pattern("batstr", "length", STRbatLength, false, "Return the length of a string.", args(1,2, batarg("",int),batarg("s",str))),
 pattern("batstr", "length", STRbatLength, false, "Return the length of a string.", args(1,3, batarg("",int),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "nbytes", STRbatBytes, false, "Return the string length in bytes.", args(1,2, batarg("",int),batarg("s",str))),
 pattern("batstr", "nbytes", STRbatBytes, false, "Return the string length in bytes.", args(1,3, batarg("",int),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "toLower", STRbatLower, false, "Convert a string to lower case.", args(1,2, batarg("",str),batarg("s",str))),
 pattern("batstr", "toLower", STRbatLower, false, "Convert a string to lower case.", args(1,3, batarg("",str),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "toUpper", STRbatUpper, false, "Convert a string to upper case.", args(1,2, batarg("",str),batarg("s",str))),
 pattern("batstr", "toUpper", STRbatUpper, false, "Convert a string to upper case.", args(1,3, batarg("",str),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "trim", STRbatStrip, false, "Strip whitespaces around a string.", args(1,2, batarg("",str),batarg("s",str))),
 pattern("batstr", "trim", STRbatStrip, false, "Strip whitespaces around a string.", args(1,3, batarg("",str),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "ltrim", STRbatLtrim, false, "Strip whitespaces from start of a string.", args(1,2, batarg("",str),batarg("s",str))),
 pattern("batstr", "ltrim", STRbatLtrim, false, "Strip whitespaces from start of a string.", args(1,3, batarg("",str),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "rtrim", STRbatRtrim, false, "Strip whitespaces from end of a string.", args(1,2, batarg("",str),batarg("s",str))),
 pattern("batstr", "rtrim", STRbatRtrim, false, "Strip whitespaces from end of a string.", args(1,3, batarg("",str),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "trim2", STRbatStrip2_const, false, "Strip characters in the second string around the first strings.", args(1,3, batarg("",str),batarg("s",str),arg("s2",str))),
 pattern("batstr", "trim2", STRbatStrip2_const, false, "Strip characters in the second string around the first strings.", args(1,4, batarg("",str),batarg("s",str),arg("s2",str),batarg("s",oid))),
 pattern("batstr", "trim2", STRbatStrip2_1st_const, false, "Strip characters in the second string around the first strings.", args(1,3, batarg("",str),arg("s",str),batarg("s2",str))),
 pattern("batstr", "trim2", STRbatStrip2_1st_const, false, "Strip characters in the second string around the first strings.", args(1,4, batarg("",str),arg("s",str),batarg("s2",str),batarg("s",oid))),
 pattern("batstr", "ltrim2", STRbatLtrim2_const, false, "Strip characters in the second string from start of the first strings.", args(1,3, batarg("",str),batarg("s",str),arg("s2",str))),
 pattern("batstr", "ltrim2", STRbatLtrim2_const, false, "Strip characters in the second string from start of the first strings.", args(1,4, batarg("",str),batarg("s",str),arg("s2",str),batarg("s",oid))),
 pattern("batstr", "ltrim2", STRbatLtrim2_1st_const, false, "Strip characters in the second string from start of the first strings.", args(1,3, batarg("",str),arg("s",str),batarg("s2",str))),
 pattern("batstr", "ltrim2", STRbatLtrim2_1st_const, false, "Strip characters in the second string from start of the first strings.", args(1,4, batarg("",str),arg("s",str),batarg("s2",str),batarg("s",oid))),
 pattern("batstr", "rtrim2", STRbatRtrim2_const, false, "Strip characters in the second string from end of the first strings.", args(1,3, batarg("",str),batarg("s",str),arg("s2",str))),
 pattern("batstr", "rtrim2", STRbatRtrim2_const, false, "Strip characters in the second string from end of the first strings.", args(1,4, batarg("",str),batarg("s",str),arg("s2",str),batarg("s",oid))),
 pattern("batstr", "rtrim2", STRbatRtrim2_1st_const, false, "Strip characters in the second string from end of the first strings.", args(1,3, batarg("",str),arg("s",str),batarg("s2",str))),
 pattern("batstr", "rtrim2", STRbatRtrim2_1st_const, false, "Strip characters in the second string from end of the first strings.", args(1,4, batarg("",str),arg("s",str),batarg("s2",str),batarg("s",oid))),
 pattern("batstr", "trim2", STRbatStrip2_bat, false, "Strip characters in the second strings around the first strings.", args(1,3, batarg("",str),batarg("s",str),batarg("s2",str))),
 pattern("batstr", "trim2", STRbatStrip2_bat, false, "Strip characters in the second strings around the first strings.", args(1,5, batarg("",str),batarg("s",str),batarg("s2",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "ltrim2", STRbatLtrim2_bat, false, "Strip characters in the second strings from start of the first strings.", args(1,3, batarg("",str),batarg("s",str),batarg("s2",str))),
 pattern("batstr", "ltrim2", STRbatLtrim2_bat, false, "Strip characters in the second strings from start of the first strings.", args(1,5, batarg("",str),batarg("s",str),batarg("s2",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "rtrim2", STRbatRtrim2_bat, false, "Strip characters in the second strings from end of the first strings.", args(1,3, batarg("",str),batarg("s",str),batarg("s2",str))),
 pattern("batstr", "rtrim2", STRbatRtrim2_bat, false, "Strip characters in the second strings from end of the first strings.", args(1,5, batarg("",str),batarg("s",str),batarg("s2",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "lpad", STRbatLpad_const, false, "Prepend whitespaces to the strings to reach the given length. Truncate the strings on the right if their lengths is larger than the given length.", args(1,3, batarg("",str),batarg("s",str),arg("n",int))),
 pattern("batstr", "lpad", STRbatLpad_const, false, "Prepend whitespaces to the strings to reach the given length. Truncate the strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),batarg("s",oid))),
 pattern("batstr", "rpad", STRbatRpad_const, false, "Append whitespaces to the strings to reach the given length. Truncate the strings on the right if their lengths is larger than the given length.", args(1,3, batarg("",str),batarg("s",str),arg("n",int))),
 pattern("batstr", "rpad", STRbatRpad_const, false, "Append whitespaces to the strings to reach the given length. Truncate the strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),batarg("s",oid))),
 pattern("batstr", "lpad", STRbatLpad_1st_const, false, "Prepend whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,3, batarg("",str),arg("s",str),batarg("n",int))),
 pattern("batstr", "lpad", STRbatLpad_1st_const, false, "Prepend whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),arg("s",str),batarg("n",int),batarg("s",oid))),
 pattern("batstr", "rpad", STRbatRpad_1st_const, false, "Append whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,3, batarg("",str),arg("s",str),batarg("n",int))),
 pattern("batstr", "rpad", STRbatRpad_1st_const, false, "Append whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),arg("s",str),batarg("n",int),batarg("s",oid))),
 pattern("batstr", "lpad", STRbatLpad_bat, false, "Prepend whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,3, batarg("",str),batarg("s",str),batarg("n",int))),
 pattern("batstr", "lpad", STRbatLpad_bat, false, "Prepend whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,5, batarg("",str),batarg("s",str),batarg("n",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "rpad", STRbatRpad_bat, false, "Append whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,3, batarg("",str),batarg("s",str),batarg("n",int))),
 pattern("batstr", "rpad", STRbatRpad_bat, false, "Append whitespaces to the strings to reach the given lengths. Truncate the strings on the right if their lengths is larger than the given lengths.", args(1,5, batarg("",str),batarg("s",str),batarg("n",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "lpad3", STRbatLpad3_const_const, false, "Prepend the second string to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),arg("s2",str))),
 pattern("batstr", "rpad3", STRbatRpad3_const_const, false, "Append the second string to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),arg("s2",str))),
 pattern("batstr", "lpad3", STRbatLpad3_bat_const, false, "Prepend the second string to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),arg("s2",str))),
 pattern("batstr", "rpad3", STRbatRpad3_bat_const, false, "Append the second string to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),arg("s2",str))),
 pattern("batstr", "lpad3", STRbatLpad3_const_bat, false, "Prepend the second strings to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),batarg("s2",str))),
 pattern("batstr", "rpad3", STRbatRpad3_const_bat, false, "Append the second strings to the first strings to reach the given length. Truncate the first strings on the right if their lengths is larger than the given length.", args(1,4, batarg("",str),batarg("s",str),arg("n",int),batarg("s2",str))),
 pattern("batstr", "lpad3", STRbatLpad3_bat_bat, false, "Prepend the second strings to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),batarg("s2",str))),
 pattern("batstr", "rpad3", STRbatRpad3_bat_bat, false, "Append the second strings to the first strings to reach the given lengths. Truncate the first strings on the right if their lengths is larger than the given lengths.", args(1,4, batarg("",str),batarg("s",str),batarg("n",int),batarg("s2",str))),
 pattern("batstr", "startsWith", STRbatPrefix, false, "Prefix check.", args(1,3, batarg("",bit),batarg("s",str),batarg("prefix",str))),
 pattern("batstr", "startsWith", STRbatPrefix, false, "Prefix check.", args(1,5, batarg("",bit),batarg("s",str),batarg("prefix",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "startsWith", STRbatPrefixcst, false, "Prefix check.", args(1,3, batarg("",bit),batarg("s",str),arg("prefix",str))),
 pattern("batstr", "startsWith", STRbatPrefixcst, false, "Prefix check.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),batarg("s",oid))),
 pattern("batstr", "startsWith", STRbatPrefix_strcst, false, "Prefix check.", args(1,3, batarg("",bit),arg("s",str),batarg("prefix",str))),
 pattern("batstr", "startsWith", STRbatPrefix_strcst, false, "Prefix check.", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),batarg("s",oid))),
 pattern("batstr", "endsWith", STRbatSuffix, false, "Suffix check.", args(1,3, batarg("",bit),batarg("s",str),batarg("suffix",str))),
 pattern("batstr", "endsWith", STRbatSuffix, false, "Suffix check.", args(1,5, batarg("",bit),batarg("s",str),batarg("suffix",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "endsWith", STRbatSuffixcst, false, "Suffix check.", args(1,3, batarg("",bit),batarg("s",str),arg("suffix",str))),
 pattern("batstr", "endsWith", STRbatSuffixcst, false, "Suffix check.", args(1,4, batarg("",bit),batarg("s",str),arg("suffix",str),batarg("s",oid))),
 pattern("batstr", "endsWith", STRbatSuffix_strcst, false, "Suffix check.", args(1,3, batarg("",bit),arg("s",str),batarg("suffix",str))),
 pattern("batstr", "endsWith", STRbatSuffix_strcst, false, "Suffix check.", args(1,4, batarg("",bit),arg("s",str),batarg("suffix",str),batarg("s",oid))),
 pattern("batstr", "splitpart", STRbatsplitpart, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),batarg("needle",str),batarg("field",int))),
 pattern("batstr", "splitpart", STRbatsplitpartcst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),arg("needle",str),arg("field",int))),
 pattern("batstr", "splitpart", STRbatsplitpart_needlecst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),arg("needle",str),batarg("field",int))),
 pattern("batstr", "splitpart", STRbatsplitpart_fieldcst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),batarg("needle",str),arg("field",int))),
 pattern("batstr", "search", STRbatstrSearch, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
 pattern("batstr", "search", STRbatstrSearch, false, "Search for a substring. Returns position, -1 if not found.", args(1,5, batarg("",int),batarg("s",str),batarg("c",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "search", STRbatstrSearchcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
 pattern("batstr", "search", STRbatstrSearchcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,4, batarg("",int),batarg("s",str),arg("c",str),batarg("s",oid))),
 pattern("batstr", "search", STRbatstrSearch_strcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),arg("s",str),batarg("c",str))),
 pattern("batstr", "search", STRbatstrSearch_strcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,4, batarg("",int),arg("s",str),batarg("c",str),batarg("s",oid))),
 pattern("batstr", "r_search", STRbatRstrSearch, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
 pattern("batstr", "r_search", STRbatRstrSearch, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,5, batarg("",int),batarg("s",str),batarg("c",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "r_search", STRbatRstrSearchcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
 pattern("batstr", "r_search", STRbatRstrSearchcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,4, batarg("",int),batarg("s",str),arg("c",str),batarg("s",oid))),
 pattern("batstr", "r_search", STRbatRstrSearch_strcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),arg("s",str),batarg("c",str))),
 pattern("batstr", "r_search", STRbatRstrSearch_strcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,4, batarg("",int),arg("s",str),batarg("c",str),batarg("s",oid))),
 pattern("batstr", "string", STRbatTail, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),batarg("b",str),batarg("offset",int))),
 pattern("batstr", "string", STRbatTail, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,5, batarg("",str),batarg("b",str),batarg("offset",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "string", STRbatTailcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),batarg("b",str),arg("offset",int))),
 pattern("batstr", "string", STRbatTailcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,4, batarg("",str),batarg("b",str),arg("offset",int),batarg("s",oid))),
 pattern("batstr", "string", STRbatTail_strcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,3, batarg("",str),arg("b",str),batarg("offset",int))),
 pattern("batstr", "string", STRbatTail_strcst, false, "Return the tail s[offset..n] of a string s[0..n].", args(1,4, batarg("",str),arg("b",str),batarg("offset",int),batarg("s",oid))),
 pattern("batstr", "ascii", STRbatAscii, false, "Return unicode of head of string", args(1,2, batarg("",int),batarg("s",str))),
 pattern("batstr", "ascii", STRbatAscii, false, "Return unicode of head of string", args(1,3, batarg("",int),batarg("s",str),batarg("s",oid))),
 pattern("batstr", "substring", STRbatsubstringTail, false, "Extract the tail of a string", args(1,3, batarg("",str),batarg("s",str),batarg("start",int))),
 pattern("batstr", "substring", STRbatsubstringTail, false, "Extract the tail of a string", args(1,5, batarg("",str),batarg("s",str),batarg("start",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "substring", STRbatsubstringTailcst, false, "Extract the tail of a string", args(1,3, batarg("",str),batarg("s",str),arg("start",int))),
 pattern("batstr", "substring", STRbatsubstringTailcst, false, "Extract the tail of a string", args(1,4, batarg("",str),batarg("s",str),arg("start",int),batarg("s",oid))),
 pattern("batstr", "substring", STRbatsubstringTail_strcst, false, "Extract the tail of a string", args(1,3, batarg("",str),arg("s",str),batarg("start",int))),
 pattern("batstr", "substring", STRbatsubstringTail_strcst, false, "Extract the tail of a string", args(1,4, batarg("",str),arg("s",str),batarg("start",int),batarg("s",oid))),
 pattern("batstr", "substring3", STRbatsubstring, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),batarg("start",int),batarg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring, false, "Substring extraction using [start,start+length]", args(1,7, batarg("",str),batarg("s",str),batarg("start",int),batarg("index",int),batarg("s1",oid),batarg("s2",oid),batarg("s3",oid))),
 pattern("batstr", "substring3", STRbatsubstring_2nd_3rd_cst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),arg("start",int),arg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring_2nd_3rd_cst, false, "Substring extraction using [start,start+length]", args(1,5, batarg("",str),batarg("s",str),arg("start",int),arg("index",int),batarg("s",oid))),
 pattern("batstr", "substring3", STRbatsubstring_2nd_cst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),arg("start",int),batarg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring_2nd_cst, false, "Substring extraction using [start,start+length]", args(1,6, batarg("",str),batarg("s",str),arg("start",int),batarg("index",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "substring3", STRbatsubstring_3rd_cst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),batarg("s",str),batarg("start",int),arg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring_3rd_cst, false, "Substring extraction using [start,start+length]", args(1,6, batarg("",str),batarg("s",str),batarg("start",int),arg("index",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "substring3", STRbatsubstring_1st_2nd_cst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),arg("s",str),arg("start",int),batarg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring_1st_2nd_cst, false, "Substring extraction using [start,start+length]", args(1,5, batarg("",str),arg("s",str),arg("start",int),batarg("index",int),batarg("s",oid))),
 pattern("batstr", "substring3", STRbatsubstring_1st_3rd_cst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),arg("s",str),batarg("start",int),arg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring_1st_3rd_cst, false, "Substring extraction using [start,start+length]", args(1,5, batarg("",str),arg("s",str),batarg("start",int),arg("index",int),batarg("s",oid))),
 pattern("batstr", "substring3", STRbatsubstring_1st_cst, false, "Substring extraction using [start,start+length]", args(1,4, batarg("",str),arg("s",str),batarg("start",int),batarg("index",int))),
 pattern("batstr", "substring3", STRbatsubstring_1st_cst, false, "Substring extraction using [start,start+length]", args(1,6, batarg("",str),arg("s",str),batarg("start",int),batarg("index",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "unicode", STRbatFromWChr, false, "convert a unicode to a character.", args(1,2, batarg("",str),batarg("wchar",int))),
 pattern("batstr", "unicode", STRbatFromWChr, false, "convert a unicode to a character.", args(1,3, batarg("",str),batarg("wchar",int),batarg("s",oid))),
 pattern("batstr", "unicodeAt", STRbatWChrAt, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),batarg("s",str),batarg("index",int))),
 pattern("batstr", "unicodeAt", STRbatWChrAt, false, "get a unicode character (as an int) from a string position.", args(1,5, batarg("",int),batarg("s",str),batarg("index",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "unicodeAt", STRbatWChrAtcst, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),batarg("s",str),arg("index",int))),
 pattern("batstr", "unicodeAt", STRbatWChrAtcst, false, "get a unicode character (as an int) from a string position.", args(1,4, batarg("",int),batarg("s",str),arg("index",int),batarg("s",oid))),
 pattern("batstr", "unicodeAt", STRbatWChrAt_strcst, false, "get a unicode character (as an int) from a string position.", args(1,3, batarg("",int),arg("s",str),batarg("index",int))),
 pattern("batstr", "unicodeAt", STRbatWChrAt_strcst, false, "get a unicode character (as an int) from a string position.", args(1,4, batarg("",int),arg("s",str),batarg("index",int),batarg("s",oid))),
 pattern("batstr", "substitute", STRbatSubstitute, false, "Substitute first occurrence of 'src' by\n'dst'. Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, batarg("",str),batarg("s",str),batarg("src",str),batarg("dst",str),batarg("rep",bit))),
 pattern("batstr", "substitute", STRbatSubstitutecst, false, "Substitute first occurrence of 'src' by\n'dst'. Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, batarg("",str),batarg("s",str),arg("src",str),arg("dst",str),arg("rep",bit))),
 pattern("batstr", "stringleft", STRbatprefix, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("l",int))),
 pattern("batstr", "stringleft", STRbatprefix, false, "", args(1,5, batarg("",str),batarg("s",str),batarg("l",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "stringleft", STRbatprefixcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("l",int))),
 pattern("batstr", "stringleft", STRbatprefixcst, false, "", args(1,4, batarg("",str),batarg("s",str),arg("l",int),batarg("s",oid))),
 pattern("batstr", "stringleft", STRbatprefix_strcst, false, "", args(1,3, batarg("",str),arg("s",str),batarg("l",int))),
 pattern("batstr", "stringleft", STRbatprefix_strcst, false, "", args(1,4, batarg("",str),arg("s",str),batarg("l",int),batarg("s",oid))),
 pattern("batstr", "stringright", STRbatsuffix, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("l",int))),
 pattern("batstr", "stringright", STRbatsuffix, false, "", args(1,5, batarg("",str),batarg("s",str),batarg("l",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "stringright", STRbatsuffixcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("l",int))),
 pattern("batstr", "stringright", STRbatsuffixcst, false, "", args(1,4, batarg("",str),batarg("s",str),arg("l",int),batarg("s",oid))),
 pattern("batstr", "stringright", STRbatsuffix_strcst, false, "", args(1,3, batarg("",str),arg("s",str),batarg("l",int))),
 pattern("batstr", "stringright", STRbatsuffix_strcst, false, "", args(1,4, batarg("",str),arg("s",str),batarg("l",int),batarg("s",oid))),
 pattern("batstr", "locate", STRbatstrLocate, false, "Locate the start position of a string", args(1,3, batarg("",int),batarg("s1",str),batarg("s2",str))),
 pattern("batstr", "locate", STRbatstrLocate, false, "Locate the start position of a string", args(1,5, batarg("",int),batarg("s1",str),batarg("s2",str),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "locate", STRbatstrLocatecst, false, "Locate the start position of a string", args(1,3, batarg("",int),batarg("s1",str),arg("s2",str))),
 pattern("batstr", "locate", STRbatstrLocatecst, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),arg("s2",str),batarg("s",oid))),
 pattern("batstr", "locate", STRbatstrLocate_strcst, false, "Locate the start position of a string", args(1,3, batarg("",int),arg("s1",str),batarg("s2",str))),
 pattern("batstr", "locate", STRbatstrLocate_strcst, false, "Locate the start position of a string", args(1,4, batarg("",int),arg("s1",str),batarg("s2",str),batarg("s",oid))),
 pattern("batstr", "locate3", STRbatstrLocate3, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),batarg("s2",str),batarg("start",int))),
 pattern("batstr", "locate3", STRbatstrLocate3cst, false, "Locate the start position of a string", args(1,4, batarg("",int),batarg("s1",str),arg("s2",str),arg("start",int))),
 pattern("batstr", "insert", STRbatInsert, false, "Insert a string into another", args(1,5, batarg("",str),batarg("s",str),batarg("start",int),batarg("l",int),batarg("s2",str))),
 pattern("batstr", "insert", STRbatInsertcst, false, "Insert a string into another", args(1,5, batarg("",str),batarg("s",str),arg("start",int),arg("l",int),arg("s2",str))),
 pattern("batstr", "replace", STRbatReplace, false, "Insert a string into another", args(1,4, batarg("",str),batarg("s",str),batarg("pat",str),batarg("s2",str))),
 pattern("batstr", "replace", STRbatReplacecst, false, "Insert a string into another", args(1,4, batarg("",str),batarg("s",str),arg("pat",str),arg("s2",str))),
 pattern("batstr", "repeat", STRbatrepeat, false, "", args(1,3, batarg("",str),batarg("s",str),batarg("c",int))),
 pattern("batstr", "repeat", STRbatrepeat, false, "", args(1,5, batarg("",str),batarg("s",str),batarg("c",int),batarg("s1",oid),batarg("s2",oid))),
 pattern("batstr", "repeat", STRbatrepeatcst, false, "", args(1,3, batarg("",str),batarg("s",str),arg("c",int))),
 pattern("batstr", "repeat", STRbatrepeatcst, false, "", args(1,4, batarg("",str),batarg("s",str),arg("c",int),batarg("s",oid))),
 pattern("batstr", "repeat", STRbatrepeat_strcst, false, "", args(1,3, batarg("",str),arg("s",str),batarg("c",int))),
 pattern("batstr", "repeat", STRbatrepeat_strcst, false, "", args(1,4, batarg("",str),arg("s",str),batarg("c",int),batarg("s",oid))),
 pattern("batstr", "space", STRbatSpace, false, "", args(1,2, batarg("",str),batarg("l",int))),
 pattern("batstr", "space", STRbatSpace, false, "", args(1,3, batarg("",str),batarg("l",int),batarg("s",oid))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batstr_mal)
{ mal_module("batstr", NULL, batstr_init_funcs); }
