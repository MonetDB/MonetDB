
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
