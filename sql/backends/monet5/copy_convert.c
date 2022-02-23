
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#include "copy.h"

#define INSIDE_COPY_CONVERT 1

str
COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	BAT *ret = NULL;
	BAT *block = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	BAT *indices = BATdescriptor(*getArgReference_bat(stk, pci, 2));
	int tpe = getArgGDKType(mb, pci, 3);
	int n;
	void *buffer;
	size_t buffer_len;
	const void *nil_ptr;
	size_t nil_len;

	if (block == NULL || indices == NULL)
		bailout("copy.parse_generic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(indices);

	ret = COLnew(0, tpe, n, TRANSIENT);
	if (!ret)
		bailout("copy.parse_generic",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	nil_ptr = ATOMnilptr(tpe);
	nil_len = ATOMlen(tpe, ATOMnilptr(tpe));

	buffer = NULL;
	buffer_len = 0;
	for (int i = 0; i < n; i++) {
		int offset = *(int*)Tloc(indices, i);
		const char *src = Tloc(block, offset);
		const void *p;
		ssize_t len;
		if (is_int_nil(offset)) {
			p = nil_ptr;
			len = nil_len;
		} else {
			len = BATatoms[tpe].atomFromStr(src, &buffer_len, &buffer, false);
			p = buffer;
			if (len < 0)
				bailout("copy.parse_generic", SQLSTATE(42000)"Conversion failed for value '%s'", src);
		}
		if (bunfastapp(ret, p) != GDK_SUCCEED)
			bailout("copy.parse_generic", GDK_EXCEPTION);
	}
	BATsetcount(ret, n);
end:
	GDKfree(buffer);
	if (ret) {
		if (msg == MAL_SUCCEED) {
			*getArgReference_bat(stk, pci, 0) = ret->batCacheid;
			BBPkeepref(ret->batCacheid);
		}
		else
			BBPunfix(ret->batCacheid);
	}
	if (block)
		BBPunfix(block->batCacheid);
	if (indices)
		BBPunfix(indices->batCacheid);
	return msg;
}

str
parse_fixed_width_column(
	bat *ret,
	const char *fname,
	bat block_bat_id, bat offsets_bat_id,
	int tpe,
	str (*f)(struct error_handling*, void*, int, void*, char*, int*),
	void *fx)
{
	str msg = MAL_SUCCEED;
	BAT *block_bat;
	BAT *offsets_bat;
	BAT *parsed_bat;
	struct error_handling errors = {
		.rel_row = -1,
	};

	block_bat = BATdescriptor(block_bat_id);
	offsets_bat = BATdescriptor(offsets_bat_id);
	if (!block_bat || !offsets_bat)
		bailout(fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	parsed_bat = COLnew(0, tpe, BATcount(offsets_bat), TRANSIENT);
	if (!parsed_bat)
		bailout(fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	msg = f(&errors, fx, BATcount(offsets_bat), Tloc(parsed_bat, 0), Tloc(block_bat, 0), Tloc(offsets_bat, 0));
	if (msg != MAL_SUCCEED)
		goto end;

	BATsetcount(parsed_bat, BATcount(offsets_bat));

	if (errors.count > 0)
		bailout(fname, "At least %d conversion errors, example: %s", errors.count, errors.message);

end:
	if (parsed_bat) {
		if (msg == MAL_SUCCEED) {
			*ret = parsed_bat->batCacheid;
			BBPkeepref(parsed_bat->batCacheid);
		} else {
			BBPunfix(parsed_bat->batCacheid);
		}
	}
	if (block_bat)
		BBPunfix(block_bat->batCacheid);
	if (offsets_bat)
		BBPunfix(offsets_bat->batCacheid);
	return msg;
}

#define TMPL_TYPE bte
#define TMPL_SUFFIXED(s) s##_bte
#include "copy_convert_num.h"

#define TMPL_TYPE sht
#define TMPL_SUFFIXED(s) s##_sht
#include "copy_convert_num.h"

#define TMPL_TYPE int
#define TMPL_SUFFIXED(s) s##_int
#include "copy_convert_num.h"

#define TMPL_TYPE lng
#define TMPL_SUFFIXED(s) s##_lng
#include "copy_convert_num.h"

#ifdef HAVE_HGE
#define TMPL_TYPE hge
#define TMPL_SUFFIXED(s) s##_hge
#include "copy_convert_num.h"
#endif
