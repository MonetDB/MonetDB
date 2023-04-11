/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include <ctype.h>
#include <string.h>
#include "mal_client.h"
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "str.h"
#ifdef HAVE_ICONV
#include <iconv.h>
#endif

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
finalize_output(bat *res, BAT *bn, str msg, bool nils, BUN q)
{
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap->dirty |= BATcount(bn) > 0;
		*res = bn->batCacheid;
		BBPkeepref(bn);
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
		BBPreclaim(b);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *restrict x = BUNtvar(bi, p1);

			if ((msg = str_wchr_at(&next, x, 0)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatFromWChr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	bi = bat_iterator(b);
	off1 = b->hseqbase;
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatSpace(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_str(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, left, lefts);
	canditer_init(&ci2, right, rights);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	inputs = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, right, rs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, right, rs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, right, rs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, arg1, arg1s);
	canditer_init(&ci2, arg2, arg2s);
	canditer_init(&ci3, arg3, arg3s);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
prefix_or_suffix(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, bit (*func)(const char*, const char*, int), bit *icase)
{
	(void) cntxt;
	(void) mb;

	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	bit *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc >= 5 ? getArgReference_bat(stk, pci, icase?4:3) : NULL,
		*sid2 = pci->argc >= 5 ? getArgReference_bat(stk, pci, icase?5:4) : NULL;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	canditer_init(&ci1, left, lefts);
	canditer_init(&ci2, right, rights);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto exit2;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_bit, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit2;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			char *x = BUNtvar(lefti, p1);
			char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			char *x = BUNtvar(lefti, p1);
			char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	}
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
 exit2:
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
BATSTRstarts_with(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if (pci->argc == 4 || pci->argc == 6) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix(cntxt, mb, stk, pci, "batstr.startsWith", (icase && *icase)?str_is_iprefix:str_is_prefix, icase);
}

static str
BATSTRends_with(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if (pci->argc == 4 || pci->argc == 6) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix(cntxt, mb, stk, pci, "batstr.endsWith", (icase && *icase)?str_is_isuffix:str_is_suffix, icase);
}

static str
BATSTRcontains(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if (pci->argc == 4 || pci->argc == 6) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix(cntxt, mb, stk, pci, "batstr.contains", (icase && *icase)?str_icontains:str_contains, icase);
}

static str
prefix_or_suffix_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, bit (*func)(const char*, const char*, int), bit *icase)
{
	(void) cntxt;
	(void) mb;

	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	bit *restrict vals;
	str y = *getArgReference_str(stk, pci, 2), msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1), *sid1 = NULL;
	int ynil, ylen;
	if ((!icase && pci->argc == 4) || pci->argc == 5) {
		assert(getArgType(mb, pci, icase?4:3) == TYPE_bat);
		sid1 = getArgReference_bat(stk, pci, icase?4:3);
	}

	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_bit, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit2;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	ynil = strNil(y);
	ylen = str_strlen(y);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			char *x = BUNtvar(bi, p1);

			if (ynil || strNil(x)) {
				vals[i] = bit_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, ylen);
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			char *x = BUNtvar(bi, p1);

			if (ynil || strNil(x)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, ylen);
			}
		}
	}
	bat_iterator_end(&bi);
 exit2:
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
BATSTRstarts_with_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if ((pci->argc == 4 && getArgType(mb, pci, 3) == TYPE_bit) || pci->argc == 5) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix_cst(cntxt, mb, stk, pci, "batstr.startsWith", (icase && *icase)?str_is_iprefix:str_is_prefix, icase);
}

static str
BATSTRends_with_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if ((pci->argc == 4 && getArgType(mb, pci, 3) == TYPE_bit) || pci->argc == 5) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix_cst(cntxt, mb, stk, pci, "batstr.endsWith", (icase && *icase)?str_is_isuffix:str_is_suffix, icase);
}

static str
BATSTRcontains_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if ((pci->argc == 4 && getArgType(mb, pci, 3) == TYPE_bit) || pci->argc == 5) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix_cst(cntxt, mb, stk, pci, "batstr.contains", (icase && *icase)?str_icontains:str_contains, icase);
}

static str
prefix_or_suffix_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, bit (*func)(const char*, const char*, int), bit *icase)
{
	(void) cntxt;
	(void) mb;

	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	bit *restrict vals;
	char *x = *getArgReference_str(stk, pci, 1);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2), *sid1 = NULL;
	int xnil;
	if ((!icase && pci->argc == 4) || pci->argc == 5) {
		assert(getArgType(mb, pci, icase?4:3) == TYPE_bat);
		sid1 = getArgReference_bat(stk, pci, icase?4:3);
	}

	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_bit, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit2;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	xnil = strNil(x);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			char *y = BUNtvar(bi, p1);

			if (xnil || strNil(y)) {
				vals[i] = bit_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			char *y = BUNtvar(bi, p1);

			if (xnil || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	}
	bat_iterator_end(&bi);
 exit2:
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
BATSTRstarts_with_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if ((pci->argc == 4 && getArgType(mb, pci, 3) == TYPE_bit) || pci->argc == 5) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix_strcst(cntxt, mb, stk, pci, "batstr.startsWith", (icase && *icase)?str_is_iprefix:str_is_prefix, icase);
}

static str
BATSTRends_with_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if ((pci->argc == 4 && getArgType(mb, pci, 3) == TYPE_bit) || pci->argc == 5) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix_strcst(cntxt, mb, stk, pci, "batstr.endsWith", (icase && *icase)?str_is_isuffix:str_is_suffix, icase);
}

static str
BATSTRcontains_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	if ((pci->argc == 4 && getArgType(mb, pci, 3) == TYPE_bit) || pci->argc == 5) {
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
	}
	return prefix_or_suffix_strcst(cntxt, mb, stk, pci, "batstr.contains", (icase && *icase)?str_icontains:str_contains, icase);
}

/* scan select loop with or without candidates */
#define scanloop(TEST, KEEP_NULLS)									    \
	do {																\
		TRC_DEBUG(ALGO,													\
				  "scanselect(b=%s#"BUNFMT",anti=%d): "					\
				  "scanselect %s\n", BATgetId(b), BATcount(b),			\
				  anti, #TEST);											\
		if (!s || BATtdense(s)) {										\
			for (; p < q; p++) {										\
				GDK_CHECK_TIMEOUT(timeoffset, counter,					\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
				const char *restrict v = BUNtvar(bi, p - off);			\
				if ((TEST) || ((KEEP_NULLS) && *v == '\200'))			\
					vals[cnt++] = p;									\
			}															\
		} else {														\
			for (; p < ncands; p++) {									\
				GDK_CHECK_TIMEOUT(timeoffset, counter,					\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout));	\
				oid o = canditer_next(ci);								\
				const char *restrict v = BUNtvar(bi, o - off);			\
				if ((TEST) || ((KEEP_NULLS) && *v == '\200'))			\
					vals[cnt++] = o;									\
			}															\
		}																\
	} while (0)

static str
do_string_select(BAT *bn, BAT *b, BAT *s, struct canditer *ci, BUN p, BUN q, BUN *rcnt, const char *key, bool anti,
		bit (*str_cmp)(const char*, const char*, int))
{
	BATiter bi = bat_iterator(b);
	BUN cnt = 0, ncands = ci->ncand;
	oid off = b->hseqbase, *restrict vals = Tloc(bn, 0);
	str msg = MAL_SUCCEED;
	int klen = str_strlen(key);

	size_t counter = 0;
	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL)
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;

	if (anti) /* keep nulls ? (use false for now) */
		scanloop(v && *v != '\200' && str_cmp(v, key, klen) != 0, false);
	else
		scanloop(v && *v != '\200' && str_cmp(v, key, klen) == 0, false);

bailout:
	bat_iterator_end(&bi);
	*rcnt = cnt;
	return msg;
}

static str
string_select(bat *ret, const bat *bid, const bat *sid, const str *key, const bit *anti, bit (*str_cmp)(const char*, const char*, int), const str fname)
{
	BAT *b, *s = NULL, *bn = NULL;
	str msg = MAL_SUCCEED;
	BUN p = 0, q = 0, rcnt = 0;
	struct canditer ci;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(MAL, fname , SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	assert(ATOMstorage(b->ttype) == TYPE_str);

	canditer_init(&ci, b, s);
	if (!(bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT))) {
		msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (!s || BATtdense(s)) {
		if (s) {
			assert(BATtdense(s));
			p = (BUN) s->tseqbase;
			q = p + BATcount(s);
			if ((oid) p < b->hseqbase)
				p = b->hseqbase;
			if ((oid) q > b->hseqbase + BATcount(b))
				q = b->hseqbase + BATcount(b);
		} else {
			p = b->hseqbase;
			q = BATcount(b) + b->hseqbase;
		}
	}

	msg = do_string_select(bn, b, s, &ci, p, q, &rcnt, *key, *anti, str_cmp);

	if (!msg) { /* set some properties */
		BATsetcount(bn, rcnt);
		bn->tsorted = true;
		bn->trevsorted = bn->batCount <= 1;
		bn->tkey = true;
		bn->tnil = false;
		bn->tnonil = true;
		bn->tseqbase = rcnt == 0 ? 0 : rcnt == 1 ? *(const oid*)Tloc(bn, 0) : rcnt == b->batCount ? b->hseqbase : oid_nil;
	}

bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	if (bn && !msg) {
		*ret = bn->batCacheid;
		BBPkeepref(bn);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
BATSTRstartswithselect(bat *ret, const bat *bid, const bat *sid, const str *key, const bit *caseignore, const bit *anti)
{
		return string_select(ret, bid, sid, key, anti, (*caseignore)?str_is_iprefix:str_is_prefix,
				"batstr.startswithselect");
}

static str
BATSTRendswithselect(bat *ret, const bat *bid, const bat *sid, const str *key, const bit *caseignore, const bit *anti)
{
		return string_select(ret, bid, sid, key, anti, (*caseignore)?str_is_isuffix:str_is_suffix,
				"batstr.endswithselect");
}

static str
BATSTRcontainsselect(bat *ret, const bat *bid, const bat *sid, const str *key, const bit *caseignore, const bit *anti)
{
		return string_select(ret, bid, sid, key, anti, (*caseignore)?str_icontains:str_contains,
				"batstr.containsselect");
}

#define APPEND(b, o)	(((oid *) b->theap->base)[b->batCount++] = (o))
#define VALUE(s, x)		(s##vars + VarHeapVal(s##vals, (x), s##i.width))

/* nested loop implementation for batstr joins */
#define batstr_join_loop(STRCMP, STR_LEN) \
	do { \
		for (BUN ridx = 0; ridx < rci.ncand; ridx++) { \
			GDK_CHECK_TIMEOUT(timeoffset, counter, \
					GOTO_LABEL_TIMEOUT_HANDLER(bailout)); \
			ro = canditer_next(&rci); \
			vr = VALUE(r, ro - rbase); \
			rlen = STR_LEN; \
			nl = 0; \
			canditer_reset(&lci); \
			for (BUN lidx = 0; lidx < lci.ncand; lidx++) { \
				lo = canditer_next(&lci); \
				vl = VALUE(l, lo - lbase); \
				if (strNil(vl)) { \
					continue; \
				} else if (!(STRCMP)) { \
					continue; \
				} \
				if (BATcount(r1) == BATcapacity(r1)) { \
					newcap = BATgrows(r1); \
					BATsetcount(r1, BATcount(r1)); \
					if (r2) \
						BATsetcount(r2, BATcount(r2)); \
					if (BATextend(r1, newcap) != GDK_SUCCEED || (r2 && BATextend(r2, newcap) != GDK_SUCCEED)) { \
						msg = createException(MAL, "pcre.join", SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						goto bailout; \
					} \
					assert(!r2 || BATcapacity(r1) == BATcapacity(r2)); \
				} \
				if (BATcount(r1) > 0) { \
					if (lastl + 1 != lo) \
						r1->tseqbase = oid_nil; \
					if (nl == 0) { \
						if (r2) \
							r2->trevsorted = false; \
						if (lastl > lo) { \
							r1->tsorted = false; \
							r1->tkey = false; \
						} else if (lastl < lo) { \
							r1->trevsorted = false; \
						} else { \
							r1->tkey = false; \
						} \
					} \
				} \
				APPEND(r1, lo); \
				if (r2) \
					APPEND(r2, ro); \
				lastl = lo; \
				nl++; \
			} \
			if (r2) { \
				if (nl > 1) { \
					r2->tkey = false; \
					r2->tseqbase = oid_nil; \
					r1->trevsorted = false; \
				} else if (nl == 0) { \
					rskipped = BATcount(r2) > 0; \
				} else if (rskipped) { \
					r2->tseqbase = oid_nil; \
				} \
			} else if (nl > 1) { \
				r1->trevsorted = false; \
			} \
		} \
	} while (0)

static str
batstrjoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, bit anti, bit (*str_cmp)(const char*, const char*, int), const str fname)
{
	struct canditer lci, rci;
	const char *lvals, *rvals, *lvars, *rvars, *vl, *vr;
	int rskipped = 0, rlen = 0;			/* whether we skipped values in r */
	oid lbase, rbase, lo, ro, lastl = 0;		/* last value inserted into r1 */
	BUN nl, newcap;
	char *msg = MAL_SUCCEED;

	size_t counter = 0;
	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	TRC_DEBUG(ALGO,
			  "%s(l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s)\n",
			  fname,
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  sl ? BATgetId(sl) : "NULL", sl ? BATcount(sl) : 0,
			  sl && sl->tsorted ? "-sorted" : "",
			  sl && sl->trevsorted ? "-revsorted" : "",
			  sr ? BATgetId(sr) : "NULL", sr ? BATcount(sr) : 0,
			  sr && sr->tsorted ? "-sorted" : "",
			  sr && sr->trevsorted ? "-revsorted" : "");

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	canditer_init(&lci, l, sl);
	canditer_init(&rci, r, sr);

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	lbase = l->hseqbase;
	rbase = r->hseqbase;
	lvals = (const char *) li.base;
	rvals = (const char *) ri.base;
	assert(ri.vh && r->ttype);
	lvars = li.vh->base;
	rvars = ri.vh->base;

	r1->tkey = true;
	r1->tsorted = true;
	r1->trevsorted = true;
	r1->tnil = false;
	r1->tnonil = true;
	if (r2) {
		r2->tkey = true;
		r2->tsorted = true;
		r2->trevsorted = true;
		r2->tnil = false;
		r2->tnonil = true;
	}

	if (anti) {
		batstr_join_loop(str_cmp(vl, vr, rlen) == 0, str_strlen(vr));
	} else {
		batstr_join_loop(str_cmp(vl, vr, rlen) != 0, str_strlen(vr));
	}
	bat_iterator_end(&li);
	bat_iterator_end(&ri);

	assert(!r2 || BATcount(r1) == BATcount(r2));
	/* also set other bits of heap to correct value to indicate size */
	BATsetcount(r1, BATcount(r1));
	if (r2)
		BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2)
			r2->tseqbase = 0;
	}
	if (r2)
		TRC_DEBUG(ALGO,
				"%s(l=%s,r=%s)=(%s#"BUNFMT"%s%s,%s#"BUNFMT"%s%s\n",
				fname,
				BATgetId(l), BATgetId(r),
				BATgetId(r1), BATcount(r1),
				r1->tsorted ? "-sorted" : "",
				r1->trevsorted ? "-revsorted" : "",
				BATgetId(r2), BATcount(r2),
				r2->tsorted ? "-sorted" : "",
				r2->trevsorted ? "-revsorted" : "");
	else
		TRC_DEBUG(ALGO,
			"%s(l=%s,r=%s)=(%s#"BUNFMT"%s%s\n",
			fname,
			BATgetId(l), BATgetId(r),
			BATgetId(r1), BATcount(r1),
			r1->tsorted ? "-sorted" : "",
			r1->trevsorted ? "-revsorted" : "");
	return MAL_SUCCEED;

bailout:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	assert(msg != MAL_SUCCEED);
	return msg;
}


static str
BATSTRjoin(bat *r1, bat *r2, const bat lid, const bat rid, const bat slid, const bat srid, const bit anti, bit (*str_cmp)(const char*, const char*, int), const str fname)
{
	BAT *left = NULL, *right = NULL, *candleft = NULL, *candright = NULL;
	BAT *result1 = NULL, *result2 = NULL;
	char *msg = MAL_SUCCEED;

	left = BATdescriptor(lid);
	right = BATdescriptor(rid);
	if (!left || !right) {
		msg = createException(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto fail;
	}
	if ((!is_bat_nil(slid) && (candleft = BATdescriptor(slid)) == NULL) ||
		(!is_bat_nil(srid) && (candright = BATdescriptor(srid)) == NULL) ) {
		msg = createException(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto fail;
	}
	result1 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (r2)
		result2 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (!result1 || (r2 && !result2)) {
		BBPreclaim(result1);
		BBPreclaim(result2);
		msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto fail;
	}
	result1->tnil = false;
	result1->tnonil = true;
	result1->tkey = true;
	result1->tsorted = true;
	result1->trevsorted = true;
	result1->tseqbase = 0;
	if (r2) {
		result2->tnil = false;
		result2->tnonil = true;
		result2->tkey = true;
		result2->tsorted = true;
		result2->trevsorted = true;
		result2->tseqbase = 0;
	}
	msg = batstrjoin(result1, result2, left, right, candleft, candright, anti, str_cmp, fname);
	if (!msg) {
		*r1 = result1->batCacheid;
		BBPkeepref(result1);
		if (r2) {
			*r2 = result2->batCacheid;
			BBPkeepref(result2);
		}
	} else {
		BBPreclaim(result1);
		BBPreclaim(result2);
	}
  fail:
	BBPunfix(left->batCacheid);
	BBPunfix(right->batCacheid);
	BBPreclaim(candleft);
	BBPreclaim(candright);
	return msg;
}

static str
join_caseignore(const bat *cid, bool *caseignore, str fname)
{
	BAT *c = NULL;

	if ((c = BATdescriptor(*cid)) == NULL)
		return createException(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BATcount(c) != 1)
		return createException(MAL, fname, SQLSTATE(42000) "At the moment, only one value is allowed for the case ignore input at pcre join");
	BATiter bi = bat_iterator(c);
	*caseignore = *(bit*)BUNtloc(bi, 0);
	bat_iterator_end(&bi);
	BBPreclaim(c);
	return MAL_SUCCEED;
}

static str
BATSTRstartswithjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
    (void) nil_matches;
    (void) estimate;
	bool caseignore = false;
	str msg = join_caseignore(cid, &caseignore, "batstr.startswithjoin");
	if (msg)
		return msg;
    return BATSTRjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *anti, (caseignore)?str_is_iprefix:str_is_prefix, "batstr.startswithjoin");
}

static str
BATSTRstartswithjoin1(bat *r1, const bat *lid, const bat *rid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
    (void) nil_matches;
    (void) estimate;
	bool caseignore = false;
	str msg = join_caseignore(cid, &caseignore, "batstr.startswithjoin");
	if (msg)
		return msg;
    return BATSTRjoin(r1, NULL, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *anti, (caseignore)?str_is_iprefix:str_is_prefix, "batstr.startswithjoin");
}

static str
BATSTRendswithjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
    (void) nil_matches;
    (void) estimate;
	bool caseignore = false;
	str msg = join_caseignore(cid, &caseignore, "batstr.endswithjoin");
	if (msg)
		return msg;
    return BATSTRjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *anti, (caseignore)?str_is_isuffix:str_is_suffix, "batstr.endswithjoin");
}

static str
BATSTRendswithjoin1(bat *r1, const bat *lid, const bat *rid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
    (void) nil_matches;
    (void) estimate;
	bool caseignore = false;
	str msg = join_caseignore(cid, &caseignore, "batstr.endswithjoin");
	if (msg)
		return msg;
    return BATSTRjoin(r1, NULL, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *anti, (caseignore)?str_is_isuffix:str_is_suffix, "batstr.endswithjoin");
}

static str
BATSTRcontainsjoin(bat *r1, bat *r2, const bat *lid, const bat *rid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
    (void) nil_matches;
    (void) estimate;
	bool caseignore = false;
	str msg = join_caseignore(cid, &caseignore, "batstr.containsjoin");
	if (msg)
		return msg;
    return BATSTRjoin(r1, r2, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *anti, (caseignore)?str_icontains:str_contains, "batstr.containsjoin");
}

static str
BATSTRcontainsjoin1(bat *r1, const bat *lid, const bat *rid, const bat *cid, const bat *slid, const bat *srid, const bit *nil_matches, const lng *estimate, const bit *anti)
{
    (void) nil_matches;
    (void) estimate;
	bool caseignore = false;
	str msg = join_caseignore(cid, &caseignore, "batstr.containsjoin");
	if (msg)
		return msg;
    return BATSTRjoin(r1, NULL, *lid, *rid, slid ? *slid : 0, srid ? *srid : 0, *anti, (caseignore)?str_icontains:str_contains, "batstr.containsjoin");
}

static str
search_string_bat(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char*, const char*, int), bit *icase)
{
	(void) cntxt;
	(void) mb;

	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
	int *restrict vals;
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0}, ci2 = {0};
	oid off1, off2;
	bat *res = getArgReference_bat(stk, pci, 0), *l = getArgReference_bat(stk, pci, 1),
		*r = getArgReference_bat(stk, pci, 2),
		*sid1 = pci->argc >= 5 ? getArgReference_bat(stk, pci, icase?4:3) : NULL,
		*sid2 = pci->argc >= 5 ? getArgReference_bat(stk, pci, icase?5:4) : NULL;

	if (!(left = BATdescriptor(*l)) || !(right = BATdescriptor(*r))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	if ((sid1 && !is_bat_nil(*sid1) && !(lefts = BATdescriptor(*sid1))) || (sid2 && !is_bat_nil(*sid2) && !(rights = BATdescriptor(*sid2)))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	canditer_init(&ci1, left, lefts);
	canditer_init(&ci2, right, rights);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto exit2;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit2;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			char *x = BUNtvar(lefti, p1);
			char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1), p2 = (canditer_next(&ci2) - off2);
			char *x = BUNtvar(lefti, p1);
			char *y = BUNtvar(righti, p2);

			if (strNil(x) || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	}
	bat_iterator_end(&lefti);
	bat_iterator_end(&righti);
 exit2:
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
BATSTRstr_search(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	switch (pci->argc) {
	case 4:
		if (getArgType(mb, pci, 3) == TYPE_bit)
			icase = getArgReference_bit(stk, pci, 3);
		break;
	case 6:
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
		break;
	}
	return search_string_bat(cntxt, mb, stk, pci, "batstr.search", (icase&&*icase)?str_isearch:str_search, icase);
}

static str
BATSTRrevstr_search(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	switch (pci->argc) {
	case 4:
		if (getArgType(mb, pci, 3) == TYPE_bit)
			icase = getArgReference_bit(stk, pci, 3);
		break;
	case 6:
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
		break;
	}
	return search_string_bat(cntxt, mb, stk, pci, "batstr.r_search", (icase&&*icase)?str_reverse_str_isearch:str_reverse_str_search, icase);
}

static str
search_string_bat_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char*, const char*, int), bit *icase)
{
	(void) cntxt;
	(void) mb;

	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	int *restrict vals;
	const char *y = *getArgReference_str(stk, pci, 2);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 1), *sid1 = NULL;
	int ynil, ylen;
	/* checking if icase is ~NULL and not if it is true or false */
	if (pci->argc == 4 || pci->argc == 5) {
		assert(getArgType(mb, pci, icase?4:3) == TYPE_bat);
		sid1 = getArgReference_bat(stk, pci, icase?4:3);
	}

	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit2;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	ynil = strNil(y);
	ylen = str_strlen(y);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			char *x = BUNtvar(bi, p1);

			if (ynil || strNil(x)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, ylen);
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			char *x = BUNtvar(bi, p1);

			if (ynil || strNil(x)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, ylen);
			}
		}
	}
	bat_iterator_end(&bi);
 exit2:
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
BATSTRstr_search_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	switch (pci->argc) {
	case 4:
		if (getArgType(mb, pci, 3) == TYPE_bit)
			icase = getArgReference_bit(stk, pci, 3);
		break;
	case 5:
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
		break;
	}
	return search_string_bat_cst(cntxt, mb, stk, pci, "batstr.search", (icase && *icase)?str_isearch:str_search, icase);
}

static str
BATSTRrevstr_search_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	switch (pci->argc) {
	case 4:
		if (getArgType(mb, pci, 3) == TYPE_bit)
			icase = getArgReference_bit(stk, pci, 3);
		break;
	case 5:
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
		break;
	}
	return search_string_bat_cst(cntxt, mb, stk, pci, "batstr.r_search", (icase && *icase)?str_reverse_str_isearch:str_reverse_str_search, icase);
}

static str
search_string_bat_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, int (*func)(const char*, const char*, int), bit *icase)
{
	(void) cntxt;
	(void) mb;

	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
	int *restrict vals;
	char *x = *getArgReference_str(stk, pci, 1);
	str msg = MAL_SUCCEED;
	bool nils = false;
	struct canditer ci1 = {0};
	oid off1;
	bat *res = getArgReference_bat(stk, pci, 0), *bid = getArgReference_bat(stk, pci, 2), *sid1 = NULL;
	int xnil;
	/* checking if icase is ~NULL and not if it is true or false */
	if (pci->argc == 4 || pci->argc == 5) {
		assert(getArgType(mb, pci, icase?4:3) == TYPE_bat);
		sid1 = getArgReference_bat(stk, pci, icase?4:3);
	}

	if (!(b = BATdescriptor(*bid))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	if (sid1 && !is_bat_nil(*sid1) && !(bs = BATdescriptor(*sid1))) {
		msg = createException(MAL, name, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit2;
	}
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit2;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	xnil = strNil(x);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			char *y = BUNtvar(bi, p1);

			if (xnil || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next(&ci1) - off1);
			char *y = BUNtvar(bi, p1);

			if (xnil || strNil(y)) {
				vals[i] = int_nil;
				nils = true;
			} else {
				vals[i] = func(x, y, str_strlen(y));
			}
		}
	}
	bat_iterator_end(&bi);
 exit2:
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
BATSTRstr_search_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	switch (pci->argc) {
	case 4:
		if (getArgType(mb, pci, 3) == TYPE_bit)
			icase = getArgReference_bit(stk, pci, 3);
		break;
	case 5:
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
		break;
	}
	return search_string_bat_strcst(cntxt, mb, stk, pci, "batstr.search", (icase && *icase)?str_isearch:str_search, icase);
}

static str
BATSTRrevstr_search_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *icase = NULL;
	switch (pci->argc) {
	case 4:
		if (getArgType(mb, pci, 3) == TYPE_bit)
			icase = getArgReference_bit(stk, pci, 3);
		break;
	case 5:
		assert(getArgType(mb, pci, 3) == TYPE_bit);
		icase = getArgReference_bit(stk, pci, 3);
		break;
	}
	return search_string_bat_strcst(cntxt, mb, stk, pci, "batstr.r_search", (icase && *icase)?str_reverse_str_isearch:str_reverse_str_search, icase);
}

static str
STRbatWChrAt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, bi;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
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
	canditer_init(&ci1, left, lefts);
	canditer_init(&ci2, right, rights);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.unicodeAt", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1), p2 = (canditer_next_dense(&ci2) - off2);
			const char *x = BUNtvar(lefti, p1);
			y = righti[p2];

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatWChrAtcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			const char *x = BUNtvar(bi, p1);

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatWChrAt_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.unicodeAt", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	input = bi.base;
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
			oid p1 = (canditer_next_dense(&ci1) - off1);
			y = input[p1];

			if ((msg = str_wchr_at(&next, x, y)) != MAL_SUCCEED)
				goto bailout1;
			vals[i] = next;
			nils |= is_int_nil(next);
		}
	} else {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_str_int_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
do_batstr_str_int(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *name, str (*func)(str*, size_t*, const char*, int))
{
	BATiter lefti;
	BAT *bn = NULL, *left = NULL, *lefts = NULL, *right = NULL, *rights = NULL;
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
	canditer_init(&ci1, left, lefts);
	canditer_init(&ci2, right, rights);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, name, ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, name, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, left, lefts);
	canditer_init(&ci2, right, rights);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.repeat", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	bi = bat_iterator(right);
	righti = bi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, left, lefts, right, rights);
	return msg;
}

static str
STRbatSubstitutecst_imp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int cand_nargs, const bit *rep)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.substritute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	(void) cntxt;
	(void) mb;
	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatSubstitutecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	const bit *rep = getArgReference_bit(stk, pci, 4);
	return STRbatSubstitutecst_imp(cntxt, mb, stk, pci, 6, rep);
}

static str
STRbatSubstitute(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter arg1i, arg2i, arg3i;
	BAT *bn = NULL, *arg1 = NULL, *arg1s = NULL, *arg2 = NULL, *arg2s = NULL, *arg3 = NULL, *arg3s = NULL, *arg4 = NULL, *arg4s = NULL;
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
	canditer_init(&ci1, arg1, arg1s);
	canditer_init(&ci2, arg2, arg2s);
	canditer_init(&ci3, arg3, arg3s);
	canditer_init(&ci4, arg4, arg4s);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand ||
		ci2.hseq != ci3.hseq || ci4.ncand != ci1.ncand || ci3.hseq != ci4.hseq) {
		msg = createException(MAL, "batstr.substritute", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(8, arg1, arg1, arg2, arg2s, arg3, arg3s, arg4, arg4s);
	return msg;
}

static str
STRbatsplitpartcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsplitpart_needlecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi, fi;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *f = NULL, *fs = NULL;
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
	canditer_init(&ci1, b, bs);
	canditer_init(&ci2, f, fs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, b, bs, f, fs);
	return msg;
}

static str
STRbatsplitpart_fieldcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi, ni;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *n = NULL, *ns = NULL;
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
	canditer_init(&ci1, b, bs);
	canditer_init(&ci2, n, ns);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = n->hseqbase;
	bi = bat_iterator(b);
	ni = bat_iterator(n);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, b, bs, n, ns);
	return msg;
}

static str
STRbatsplitpart(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter arg1i, arg2i;
	BAT *bn = NULL, *arg1 = NULL, *arg1s = NULL, *arg2 = NULL, *arg2s = NULL, *arg3 = NULL, *arg3s = NULL;
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
	canditer_init(&ci1, arg1, arg1s);
	canditer_init(&ci2, arg2, arg2s);
	canditer_init(&ci3, arg3, arg3s);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.splitpart", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, arg1, arg1s);
	canditer_init(&ci2, arg2, arg2s);
	canditer_init(&ci3, arg3, arg3s);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.replace", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(6, arg1, arg1s, arg2, arg2s, arg3, arg3s);
	return msg;
}

static str
STRbatInsert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, righti, starti, ncharsi;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *start = NULL, *ss = NULL, *nchars = NULL, *ns = NULL, *right = NULL, *rs = NULL;
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, start, ss);
	canditer_init(&ci3, nchars, ns);
	canditer_init(&ci4, right, rs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand ||
		ci2.hseq != ci3.hseq || ci4.ncand != ci1.ncand || ci3.hseq != ci4.hseq) {
		msg = createException(MAL, "batstr.insert", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(8, left, ls, start, ss, nchars, ns, right, rs);
	return msg;
}

static str
STRbatInsertcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsubstring_1st_2nd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	input = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsubstring_1st_3rd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	input = bi.base;
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatsubstring_1st_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *bn = NULL, *b = NULL, *bs = NULL, *lb = NULL, *lbs = NULL;
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
	canditer_init(&ci1, b, bs);
	canditer_init(&ci2, lb, lbs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, b, bs, lb, lbs);
	return msg;
}

static str
STRbatsubstring_2nd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BATiter lbi;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *lb = NULL, *lbs = NULL;
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
	canditer_init(&ci1, b, bs);
	canditer_init(&ci2, lb, lbs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = lb->hseqbase;
	bi = bat_iterator(b);
	lbi = bat_iterator(lb);
	len = lbi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, b, bs, lb, lbs);
	return msg;
}

static str
STRbatsubstring_3rd_cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi, lbi;
	BAT *bn = NULL, *b = NULL, *bs = NULL, *lb = NULL, *lbs = NULL;
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
	canditer_init(&ci1, b, bs);
	canditer_init(&ci2, lb, lbs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	off2 = lb->hseqbase;
	bi = bat_iterator(b);
	lbi = bat_iterator(lb);
	start = lbi.base;
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, b, bs, lb, lbs);
	return msg;
}

static str
STRbatsubstring(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, starti, lengthi;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *start = NULL, *ss = NULL, *length = NULL, *lens = NULL;
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, start, ss);
	canditer_init(&ci3, length, lens);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.substring", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_str, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(6, left, ls, start, ss, length, lens);
	return msg;
}

static str
STRbatstrLocatecst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrLocate_strcst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrLocate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL;
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, right, rs);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq) {
		msg = createException(MAL, "batstr.locate", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = left->hseqbase;
	off2 = right->hseqbase;
	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense && ci2.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(4, left, ls, right, rs);
	return msg;
}

static str
STRbatstrLocate3cst(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter bi;
	BAT *bn = NULL, *b = NULL, *bs = NULL;
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
	canditer_init(&ci1, b, bs);
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
		msg = createException(MAL, "batstr.locate2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	off1 = b->hseqbase;
	bi = bat_iterator(b);
	vals = Tloc(bn, 0);
	if (ci1.tpe == cand_dense) {
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(2, b, bs);
	return msg;
}

static str
STRbatstrLocate3(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BATiter lefti, righti, starti;
	BAT *bn = NULL, *left = NULL, *ls = NULL, *right = NULL, *rs = NULL, *start = NULL, *ss = NULL;
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
	canditer_init(&ci1, left, ls);
	canditer_init(&ci2, right, rs);
	canditer_init(&ci3, start, ss);
	if (ci2.ncand != ci1.ncand || ci1.hseq != ci2.hseq || ci3.ncand != ci1.ncand || ci2.hseq != ci3.hseq) {
		msg = createException(MAL, "batstr.locate2", ILLEGAL_ARGUMENT " Requires bats of identical size");
		goto bailout;
	}
	if (!(bn = COLnew(ci1.hseq, TYPE_int, ci1.ncand, TRANSIENT))) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
		for (BUN i = 0; i < ci1.ncand; i++) {
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
	finalize_output(res, bn, msg, nils, ci1.ncand);
	unfix_inputs(6, left, ls, right, rs, start, ss);
	return msg;
}

static str
BATSTRasciify(bat *ret, bat *bid)
{
#ifdef HAVE_ICONV
	BAT *b = NULL, *bn = NULL;
	BATiter bi;
	BUN p, q;
	bool nils = false;
	size_t prev_out_len = 0, in_len = 0, out_len = 0;
	str s = NULL, out = NULL, in = NULL, msg = MAL_SUCCEED;
	iconv_t cd;
	const str f = "UTF8", t = "ASCII//TRANSLIT";

	/* man iconv; /TRANSLIT */
	if ((cd = iconv_open(t, f)) == (iconv_t)(-1))
		throw(MAL, "batstr.asciify", "ICONV: cannot convert from (%s) to (%s).", f, t);
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batstr.asciify", RUNTIME_OBJECT_MISSING);
	if ((bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT)) == NULL) {
		BBPreclaim(b);
		throw(MAL, "batstr.asciify", GDK_EXCEPTION);
	}
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		in = (str) BUNtail(bi, p);
		if (strNil(in)) {
			if (BUNappend(bn, str_nil, false) != GDK_SUCCEED) {
				msg = createException(MAL,"batstr.asciify", "ICONV: string conversion failed");
				goto exit;
			}
			nils = true;
			continue;
		}
		in_len = strlen(in), out_len = in_len + 1;
		if (out == NULL) {
			if ((out = GDKmalloc(out_len)) == NULL) {
				msg = createException(MAL,"batstr.asciify", MAL_MALLOC_FAIL);
				goto exit;
			}
			prev_out_len = out_len;
		}
		else if (out_len > prev_out_len) {
			if ((out = GDKrealloc(s, out_len)) == NULL) {
				msg = createException(MAL,"batstr.asciify", MAL_MALLOC_FAIL);
				goto exit;
			}
			prev_out_len = out_len;
		}
		s = out;
		if (iconv(cd, &in, &in_len, &out, &out_len) == (size_t) - 1) {
			GDKfree(out);
			s = NULL;
			msg = createException(MAL,"batstr.asciify", "ICONV: string conversion failed");
			goto exit;
		}
		*out = '\0';
		if (BUNappend(bn, s, false) != GDK_SUCCEED) {
			msg = createException(MAL,"batstr.asciify", GDK_EXCEPTION);
			goto exit;
		}
	}
 exit:
	bat_iterator_end(&bi);
	iconv_close(cd);
	finalize_output(ret, bn, msg, nils, q);
	BBPreclaim(b);
	return msg;
#else
	throw(MAL, "batstr.asciify", "ICONV library not available.");
#endif
}

static inline void
str_reverse(char *dst, const char *src, size_t len)
{
	dst[len] = 0;
	if (strNil(src)) {
		assert(len == strlen(str_nil));
		strcpy(dst, str_nil);
		return;
	}
	while (*src) {
		if ((*src & 0xF8) == 0xF0) {
			/* 4 byte UTF-8 sequence */
			assert(len >= 4);
			dst[len - 4] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 3] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 4;
		} else if ((*src & 0xF0) == 0xE0) {
			/* 3 byte UTF-8 sequence */
			assert(len >= 3);
			dst[len - 3] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 3;
		} else if ((*src & 0xE0) == 0xC0) {
			/* 2 byte UTF-8 sequence */
			assert(len >= 2);
			dst[len - 2] = *src++;
			assert((*src & 0xC0) == 0x80);
			dst[len - 1] = *src++;
			len -= 2;
		} else {
			/* 1 byte UTF-8 "sequence" */
			assert(len >= 1);
			assert((*src & 0x80) == 0);
			dst[--len] = *src++;
		}
	}
	assert(len == 0);
}

static str
BATSTRreverse(bat *res, const bat *arg)
{
	BAT *b = NULL, *bn = NULL;
	BATiter bi;
	BUN p, q;
	const char *src;
	size_t len = 0, dst_len = 1024;
	str dst, msg = MAL_SUCCEED;
	bool nils = false;

	if (!(dst = GDKzalloc(dst_len)))
		throw(MAL, "batstr.reverse", MAL_MALLOC_FAIL);
	if (!(b = BATdescriptor(*arg))) {
		GDKfree(dst);
		throw(MAL, "batstr.reverse", RUNTIME_OBJECT_MISSING);
	}
	assert(b->ttype == TYPE_str);
	if (!(bn = COLnew(b->hseqbase, TYPE_str, BATcount(b), TRANSIENT))) {
		GDKfree(dst);
		BBPreclaim(b);
		throw(MAL, "batstr.reverse", MAL_MALLOC_FAIL);
	}
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		src = (const char *) BUNtail(bi, p);
		if (strNil(src)) {
			assert(len > strlen(src));
			nils = true;
			strcpy(dst, str_nil);
		}
		else {
			len = strlen(src);
			if (len >= dst_len) {
				dst_len = len + 1024;
				if ((dst = GDKrealloc(dst, dst_len)) == NULL) {
					msg = createException(MAL,"batstr.reverse", MAL_MALLOC_FAIL);
					goto exit;
				}
			}
			str_reverse(dst, src, len);
		}
		if (tfastins_nocheckVAR(bn, p, dst) != GDK_SUCCEED) {
			msg = createException(MAL,"batstr.reverse", GDK_EXCEPTION);
			goto exit;
		}
	}
 exit:
	bat_iterator_end(&bi);
	GDKfree(dst);
	finalize_output(res, bn, msg, nils, q);
	BBPreclaim(b);
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
	pattern("batstr", "startsWith", BATSTRstarts_with, false, "Check if bat string starts with bat substring.", args(1,3, batarg("",bit),batarg("s",str),batarg("prefix",str))),
	pattern("batstr", "startsWith", BATSTRstarts_with, false, "Check if bat string starts with bat substring, icase flag.", args(1,4, batarg("",bit),batarg("s",str),batarg("prefix",str),arg("icase",bit))),
	pattern("batstr", "startsWith", BATSTRstarts_with, false, "Check if bat string starts with bat substring (with CLs).", args(1,5, batarg("",bit),batarg("s",str),batarg("prefix",str),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "startsWith", BATSTRstarts_with, false, "Check if bat string starts with bat substring (with CLs) + icase flag.", args(1,6, batarg("",bit),batarg("s",str),batarg("prefix",str),arg("icase",bit),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "startsWith", BATSTRstarts_with_cst, false, "Check if bat string starts with substring.", args(1,3, batarg("",bit),batarg("s",str),arg("prefix",str))),
	pattern("batstr", "startsWith", BATSTRstarts_with_cst, false, "Check if bat string starts with substring, icase flag.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),arg("icase",bit))),
	pattern("batstr", "startsWith", BATSTRstarts_with_cst, false, "Check if bat string(with CL) starts with substring.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),batarg("s",oid))),
	pattern("batstr", "startsWith", BATSTRstarts_with_cst, false, "Check if bat string(with CL) starts with substring + icase flag.", args(1,5, batarg("",bit),batarg("s",str),arg("prefix",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "startsWith", BATSTRstarts_with_strcst, false, "Check if string starts with bat substring.", args(1,3, batarg("",bit),arg("s",str),batarg("prefix",str))),
	pattern("batstr", "startsWith", BATSTRstarts_with_strcst, false, "Check if string starts with bat substring + icase flag.", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),arg("icase",bit))),
	pattern("batstr", "startsWith", BATSTRstarts_with_strcst, false, "Check if string starts with bat substring(with CL).", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),batarg("s",oid))),
	pattern("batstr", "startsWith", BATSTRstarts_with_strcst, false, "Check if string starts with bat substring(with CL) + icase flag.", args(1,5, batarg("",bit),arg("s",str),batarg("prefix",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "endsWith", BATSTRends_with, false, "Check if bat string ends with bat substring.", args(1,3, batarg("",bit),batarg("s",str),batarg("prefix",str))),
	pattern("batstr", "endsWith", BATSTRends_with, false, "Check if bat string ends with bat substring, icase flag.", args(1,4, batarg("",bit),batarg("s",str),batarg("prefix",str),arg("icase",bit))),
	pattern("batstr", "endsWith", BATSTRends_with, false, "Check if bat string ends with bat substring (with CLs).", args(1,5, batarg("",bit),batarg("s",str),batarg("prefix",str),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "endsWith", BATSTRends_with, false, "Check if bat string ends with bat substring (with CLs) + icase flag.", args(1,6, batarg("",bit),batarg("s",str),batarg("prefix",str),arg("icase",bit),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "endsWith", BATSTRends_with_cst, false, "Check if bat string ends with substring.", args(1,3, batarg("",bit),batarg("s",str),arg("prefix",str))),
	pattern("batstr", "endsWith", BATSTRends_with_cst, false, "Check if bat string ends with substring, icase flag.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),arg("icase",bit))),
	pattern("batstr", "endsWith", BATSTRends_with_cst, false, "Check if bat string(with CL) ends with substring.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),batarg("s",oid))),
	pattern("batstr", "endsWith", BATSTRends_with_cst, false, "Check if bat string(with CL) ends with substring + icase flag.", args(1,5, batarg("",bit),batarg("s",str),arg("prefix",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "endsWith", BATSTRends_with_strcst, false, "Check if string ends with bat substring.", args(1,3, batarg("",bit),arg("s",str),batarg("prefix",str))),
	pattern("batstr", "endsWith", BATSTRends_with_strcst, false, "Check if string ends with bat substring + icase flag.", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),arg("icase",bit))),
	pattern("batstr", "endsWith", BATSTRends_with_strcst, false, "Check if string ends with bat substring(with CL).", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),batarg("s",oid))),
	pattern("batstr", "endsWith", BATSTRends_with_strcst, false, "Check if string ends with bat substring(with CL) + icase flag.", args(1,5, batarg("",bit),arg("s",str),batarg("prefix",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "contains", BATSTRcontains, false, "Check if bat string haystack contains bat string needle.", args(1,3, batarg("",bit),batarg("s",str),batarg("prefix",str))),
	pattern("batstr", "contains", BATSTRcontains, false, "Check if bat string haystack contains bat string needle, icase flag.", args(1,4, batarg("",bit),batarg("s",str),batarg("prefix",str),arg("icase",bit))),
	pattern("batstr", "contains", BATSTRcontains, false, "Check if bat string haystack contains bat string needle (with CLs).", args(1,5, batarg("",bit),batarg("s",str),batarg("prefix",str),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "contains", BATSTRcontains, false, "Check if bat string haystack contains bat string needle (with CLs) + icase flag.", args(1,6, batarg("",bit),batarg("s",str),batarg("prefix",str),arg("icase",bit),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "contains", BATSTRcontains_cst, false, "Check if bat string haystack contains string needle.", args(1,3, batarg("",bit),batarg("s",str),arg("prefix",str))),
	pattern("batstr", "contains", BATSTRcontains_cst, false, "Check if bat string haystack contains string needle, icase flag.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),arg("icase",bit))),
	pattern("batstr", "contains", BATSTRcontains_cst, false, "Check if bat string haystack contains string needle (with CL) ends with substring.", args(1,4, batarg("",bit),batarg("s",str),arg("prefix",str),batarg("s",oid))),
	pattern("batstr", "contains", BATSTRcontains_cst, false, "Check if bat string haystack contains string needle (with CL) ends with substring + icase flag.", args(1,5, batarg("",bit),batarg("s",str),arg("prefix",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "contains", BATSTRcontains_strcst, false, "Check if string haystack contains bat string needle.", args(1,3, batarg("",bit),arg("s",str),batarg("prefix",str))),
	pattern("batstr", "contains", BATSTRcontains_strcst, false, "Check if string haystack contains bat string needle + icase flag.", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),arg("icase",bit))),
	pattern("batstr", "contains", BATSTRcontains_strcst, false, "Check if string haystack contains bat string needle (with CL).", args(1,4, batarg("",bit),arg("s",str),batarg("prefix",str),batarg("s",oid))),
	pattern("batstr", "contains", BATSTRcontains_strcst, false, "Check if string haystack contains bat string needle (with CL) + icase flag.", args(1,5, batarg("",bit),arg("s",str),batarg("prefix",str),arg("icase",bit),batarg("s",oid))),
	command("batstr", "startsWithselect", BATSTRstartswithselect, false, "Select all head values of the first input BAT for which the\ntail value starts with the given prefix.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("prefix",str),arg("caseignore",bit),arg("anti",bit))),
	command("batstr", "endsWithselect", BATSTRendswithselect, false, "Select all head values of the first input BAT for which the\ntail value end with the given suffix.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("suffix",str),arg("caseignore",bit),arg("anti",bit))),
	command("batstr", "containsselect", BATSTRcontainsselect, false, "Select all head values of the first input BAT for which the\ntail value contains the given needle.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("needle",str),arg("caseignore",bit),arg("anti",bit))),
	command("algebra", "startsWithjoin", BATSTRstartswithjoin, false, "Join the string bat L with the prefix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	command("algebra", "startsWithjoin", BATSTRstartswithjoin1, false, "The same as BATSTRstartswithjoin, but only produce one output", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	command("algebra", "endsWithjoin", BATSTRendswithjoin, false, "Join the string bat L with the suffix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	command("algebra", "endsWithjoin", BATSTRendswithjoin1, false, "The same as BATSTRendswithjoin, but only produce one output", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	command("algebra", "containsWithjoin", BATSTRcontainsjoin, false, "Join the string bat L with the bat R if L contains the string of R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	command("algebra", "containsWithjoin", BATSTRcontainsjoin1, false, "The same as BATSTRcontainsjoin, but only produce one output", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
	pattern("batstr", "splitpart", STRbatsplitpart, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),batarg("needle",str),batarg("field",int))),
	pattern("batstr", "splitpart", STRbatsplitpartcst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),arg("needle",str),arg("field",int))),
	pattern("batstr", "splitpart", STRbatsplitpart_needlecst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),arg("needle",str),batarg("field",int))),
	pattern("batstr", "splitpart", STRbatsplitpart_fieldcst, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, batarg("",str),batarg("s",str),batarg("needle",str),arg("field",int))),
	pattern("batstr", "search", BATSTRstr_search, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
	pattern("batstr", "search", BATSTRstr_search, false, "Search for a substring. Returns position, -1 if not found, icase flag.", args(1,4, batarg("",int),batarg("s",str),batarg("c",str),arg("icase",bit))),
	pattern("batstr", "search", BATSTRstr_search, false, "Search for a substring. Returns position, -1 if not found.", args(1,5, batarg("",int),batarg("s",str),batarg("c",str),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "search", BATSTRstr_search, false, "Search for a substring. Returns position, -1 if not found, icase flag.", args(1,6, batarg("",int),batarg("s",str),batarg("c",str),arg("icase",bit),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "search", BATSTRstr_search_cst, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
	pattern("batstr", "search", BATSTRstr_search_cst, false, "Search for a substring. Returns position, -1 if not found, icase flag.", args(1,4, batarg("",int),batarg("s",str),arg("c",str),arg("icase",bit))),
	pattern("batstr", "search", BATSTRstr_search_cst, false, "Search for a substring. Returns position, -1 if not found.", args(1,4, batarg("",int),batarg("s",str),arg("c",str),batarg("s",oid))),
	pattern("batstr", "search", BATSTRstr_search_cst, false, "Search for a substring. Returns position, -1 if not found, icase flag.", args(1,5, batarg("",int),batarg("s",str),arg("c",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "search", BATSTRstr_search_strcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),arg("s",str),batarg("c",str))),
	pattern("batstr", "search", BATSTRstr_search_strcst, false, "Search for a substring. Returns position, -1 if not found, icase flag.", args(1,4, batarg("",int),arg("s",str),batarg("c",str),arg("icase",bit))),
	pattern("batstr", "search", BATSTRstr_search_strcst, false, "Search for a substring. Returns position, -1 if not found.", args(1,4, batarg("",int),arg("s",str),batarg("c",str),batarg("s",oid))),
	pattern("batstr", "search", BATSTRstr_search_strcst, false, "Search for a substring. Returns position, -1 if not found, icase flag.", args(1,5, batarg("",int),arg("s",str),batarg("c",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "r_search", BATSTRrevstr_search, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),batarg("c",str))),
	pattern("batstr", "r_search", BATSTRrevstr_search, false, "Reverse search for a substring + icase flag. Returns position, -1 if not found.", args(1,4, batarg("",int),batarg("s",str),batarg("c",str),arg("icase",bit))),
	pattern("batstr", "r_search", BATSTRrevstr_search, false, "Reverse search for a substring (with CLs). Returns position, -1 if not found.", args(1,5, batarg("",int),batarg("s",str),batarg("c",str),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "r_search", BATSTRrevstr_search, false, "Reverse search for a substring (with CLs) + icase flag. Returns position, -1 if not found.", args(1,6, batarg("",int),batarg("s",str),batarg("c",str),arg("icase",bit),batarg("s1",oid),batarg("s2",oid))),
	pattern("batstr", "r_search", BATSTRrevstr_search_cst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),batarg("s",str),arg("c",str))),
	pattern("batstr", "r_search", BATSTRrevstr_search_cst, false, "Reverse search for a substring + icase flag. Returns position, -1 if not found.", args(1,4, batarg("",int),batarg("s",str),arg("c",str),arg("icase",bit))),
	pattern("batstr", "r_search", BATSTRrevstr_search_cst, false, "Reverse search for a substring (with CL). Returns position, -1 if not found.", args(1,4, batarg("",int),batarg("s",str),arg("c",str),batarg("s",oid))),
	pattern("batstr", "r_search", BATSTRrevstr_search_cst, false, "Reverse search for a substring (with CL) + icase flag. Returns position, -1 if not found.", args(1,5, batarg("",int),batarg("s",str),arg("c",str),arg("icase",bit),batarg("s",oid))),
	pattern("batstr", "r_search", BATSTRrevstr_search_strcst, false, "Reverse search for a substring. Returns position, -1 if not found.", args(1,3, batarg("",int),arg("s",str),batarg("c",str))),
	pattern("batstr", "r_search", BATSTRrevstr_search_strcst, false, "Reverse search for a substring + icase flag. Returns position, -1 if not found.", args(1,4, batarg("",int),arg("s",str),batarg("c",str),arg("icase",bit))),
	pattern("batstr", "r_search", BATSTRrevstr_search_strcst, false, "Reverse search for a substring (with CL). Returns position, -1 if not found.", args(1,4, batarg("",int),arg("s",str),batarg("c",str),batarg("s",oid))),
	pattern("batstr", "r_search", BATSTRrevstr_search_strcst, false, "Reverse search for a substring (with CL) + icase flag. Returns position, -1 if not found.", args(1,5, batarg("",int),arg("s",str),batarg("c",str),arg("icase",bit),batarg("s",oid))),
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
	command("batstr", "asciify", BATSTRasciify, false, "Transform BAT of strings from UTF8 to ASCII", args(1, 2, batarg("",str), batarg("b",str))),
	command("batstr", "reverse", BATSTRreverse, false, "Reverse a BAT of strings", args(1, 2, batarg("",str), batarg("b",str))),
	{ .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batstr_mal)
{ mal_module("batstr", NULL, batstr_init_funcs); }
