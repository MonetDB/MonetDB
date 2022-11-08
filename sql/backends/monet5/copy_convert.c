
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
#include "str.h"

#include "copy.h"

#define INSIDE_COPY_CONVERT 1

struct decimal_parms {
	int digits;
	int scale;
};


str
COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	struct error_handling errors;
	BAT *ret = NULL;
	BAT *block = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	BAT *indices = BATdescriptor(*getArgReference_bat(stk, pci, 2));
	int tpe = getArgGDKType(mb, pci, 3);
	bat failures_bat = *getArgReference_bat(stk, pci, 4);
	lng starting_row = *getArgReference_lng(stk, pci, 5);
	int col_no = *getArgReference_int(stk, pci, 6);
	const char *col_name = *getArgReference_str(stk, pci, 7);
	int n;
	void *buffer;
	size_t buffer_len;
	const void *nil_ptr;

	copy_init_error_handling(&errors, failures_bat, starting_row, col_no, col_name);

	if (block == NULL || indices == NULL)
		bailout("copy.parse_generic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(indices);

	ret = COLnew(0, tpe, n, TRANSIENT);
	if (!ret)
		bailout("copy.parse_generic",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	nil_ptr = ATOMnilptr(tpe);

	buffer = NULL;
	buffer_len = 0;
	for (int i = 0; i < n; i++) {
		gdk_return ok = GDK_SUCCEED;
		int offset = *(int*)Tloc(indices, i);
		const char *src = Tloc(block, offset);
		const void *to_insert;

		if (is_int_nil(offset)) {
			to_insert = nil_ptr;
		} else if (!checkUTF8(src)) {
			ok = copy_report_error(&errors, i, -1, "incorrectly encoded UTF-8");
			to_insert = nil_ptr;
		} else {
			ssize_t len = BATatoms[tpe].atomFromStr(src, &buffer_len, &buffer, false);
			if (len >= 0) {
				to_insert = buffer;
			} else {
				ok = copy_report_error(&errors, i, -1, "invalid %s: %s", ATOMname(tpe), src);
				GDKclrerr();
				to_insert = nil_ptr;
			}
		}
		if (ok != GDK_SUCCEED) {
			msg = copy_check_too_many_errors(&errors, "copy.parse_generic");
			if (msg != MAL_SUCCEED)
				goto end;
			else
				ok = GDK_SUCCEED;
		}
		if (bunfastapp(ret, to_insert) != GDK_SUCCEED)
			bailout("copy.parse_generic", GDK_EXCEPTION);
	}
	BATsetcount(ret, n);
	// we don't know anything about the data we just parsed
	ret->tkey = false;
	ret->tnil = false;
	ret->tnonil = false;
	ret->tsorted = false;
	ret->trevsorted = false;
end:
	GDKfree(buffer);
	copy_destroy_error_handling(&errors);
	if (ret) {
		if (msg == MAL_SUCCEED) {
			*getArgReference_bat(stk, pci, 0) = ret->batCacheid;
			BBPkeepref(ret);
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

static bool
fits_varchar(const char *s, int maxlen) {
	int len;
	// There are three relevant lengths:
	// 1. the length in bytes, computed by strlen()
	// 2. the length in Unicode code points, computed by UTF8_strlen()
	// 3. the width in printable units.
	// So, width <= codepoint_len <= byte_len.
	//
	// We are interested in the width but it is much more
	// expensive to compute than the other two so we try those first
	len = strlen(s);
	if (len <= maxlen)
		return true;
	len = UTF8_strlen(s);
	if (len <= maxlen)
		return true;
	len = UTF8_strwidth(s);
	if (len <= maxlen)
		return true;
	return false;
}

str
COPYparse_string(
	bat *parsed_bat_id,
	bat *block_bat_id, bat *offsets_bat_id,
	int *maxlen,
	bat *failures_bat, lng *starting_row, int *col_no, str *col_name)
{
	str msg = MAL_SUCCEED;
	const char *fname = "copy.parse_string";
	struct error_handling errors;
	BAT *block_bat = BATdescriptor(*block_bat_id);
	BAT *offsets_bat = BATdescriptor(*offsets_bat_id);
	BAT *parsed_bat = NULL;
	int colwidth = *maxlen;
	int n;

	if (!block_bat || !offsets_bat)
		bailout(fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	parsed_bat = COLnew(0, TYPE_str, BATcount(offsets_bat), TRANSIENT);
	if (!parsed_bat)
		bailout(fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	copy_init_error_handling(&errors, *failures_bat, *starting_row, *col_no, *col_name);

	n = BATcount(offsets_bat);
	for (int i = 0; i < n; i++) {
		gdk_return ok;
		int offset = *(int*)Tloc(offsets_bat, i);
		const void *to_insert = str_nil;

		if (is_int_nil(offset)) {
			ok = GDK_SUCCEED;
		} else {
			const char *src = Tloc(block_bat, offset);
			if (!checkUTF8(src)) {
				ok = copy_report_error(&errors, i, -1, "incorrectly encoded UTF-8");
			} else if (colwidth > 0 && !fits_varchar(src, colwidth)) {
				ok = copy_report_error(&errors, i, -1, "field too long, max length is %d", colwidth);
			} else {
				to_insert = src;
			}
		}
		if (ok != GDK_SUCCEED) {
			msg = copy_check_too_many_errors(&errors, "copy.parse_generic");
			if (msg != MAL_SUCCEED)
				goto end;
			else
				ok = GDK_SUCCEED;
		}
		if (bunfastapp(parsed_bat, to_insert) != GDK_SUCCEED)
			bailout("copy.parse_generic", GDK_EXCEPTION);
	}

	BATsetcount(parsed_bat, n);
	// we don't know anything about the data we just parsed
	parsed_bat->tkey = false;
	parsed_bat->tnil = false;
	parsed_bat->tnonil = false;
	parsed_bat->tsorted = false;
	parsed_bat->trevsorted = false;

end:
	copy_destroy_error_handling(&errors);
	if (parsed_bat) {
		if (msg == MAL_SUCCEED) {
			*parsed_bat_id = parsed_bat->batCacheid;
			BBPkeepref(parsed_bat);
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



str
parse_fixed_width_column(
	bat *ret,
	struct error_handling *errors,
	const char *fname,
	bat block_bat_id, bat offsets_bat_id,
	int tpe,
	void (*f)(struct error_handling*, void*, int, void*, char*, int*),
	void *fx)
{
	str msg = MAL_SUCCEED;
	BAT *block_bat = NULL;
	BAT *offsets_bat = NULL;
	BAT *parsed_bat = NULL;


	block_bat = BATdescriptor(block_bat_id);
	offsets_bat = BATdescriptor(offsets_bat_id);
	if (!block_bat || !offsets_bat)
		bailout(fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	parsed_bat = COLnew(0, tpe, BATcount(offsets_bat), TRANSIENT);
	if (!parsed_bat)
		bailout(fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	f(errors, fx, BATcount(offsets_bat), Tloc(parsed_bat, 0), Tloc(block_bat, 0), Tloc(offsets_bat, 0));
	msg = copy_check_too_many_errors(errors, fname);
	if (msg != MAL_SUCCEED)
		goto end;

	BATsetcount(parsed_bat, BATcount(offsets_bat));
	// we don't know anything about the data we just parsed
	parsed_bat->tkey = false;
	parsed_bat->tnil = false;
	parsed_bat->tnonil = false;
	parsed_bat->tsorted = false;
	parsed_bat->trevsorted = false;

end:
	if (parsed_bat) {
		if (msg == MAL_SUCCEED) {
			*ret = parsed_bat->batCacheid;
			BBPkeepref(parsed_bat);
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
#define TMPL_NIL bte_nil
#define TMPL_MAX GDK_bte_max
#define TMPL_SUFFIXED(s) s##_bte
#include "copy_convert_num.h"

#define TMPL_TYPE sht
#define TMPL_NIL sht_nil
#define TMPL_MAX GDK_sht_max
#define TMPL_SUFFIXED(s) s##_sht
#include "copy_convert_num.h"

#define TMPL_TYPE int
#define TMPL_NIL int_nil
#define TMPL_MAX GDK_int_max
#define TMPL_SUFFIXED(s) s##_int
#include "copy_convert_num.h"

#define TMPL_TYPE lng
#define TMPL_NIL lng_nil
#define TMPL_MAX GDK_lng_max
#define TMPL_SUFFIXED(s) s##_lng
#include "copy_convert_num.h"

#ifdef HAVE_HGE
#define TMPL_TYPE hge
#define TMPL_NIL hge_nil
#define TMPL_MAX GDK_hge_max
#define TMPL_SUFFIXED(s) s##_hge
#include "copy_convert_num.h"
#endif
