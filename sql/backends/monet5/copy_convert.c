/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "str.h"

#include "copy.h"

#define INSIDE_COPY_CONVERT 1

struct decimal_parms {
	int nils;
	int digits;
	int scale;
	char sep;
	char skip;
};


str
COPYparse_generic(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	BAT *ret = NULL;
	BAT *block = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 2);
	BAT *indices = BATdescriptor(*getArgReference_bat(stk, pci, 3));
	int tpe = getArgGDKType(mb, pci, 4);
	bat rows = *getArgReference_bat(stk, pci, 5);
	int col_no = *getArgReference_int(stk, pci, 6);
	const char *col_name = *getArgReference_str(stk, pci, 7);
	BUN n;
	void *buffer = NULL;
	size_t buffer_len;
	const void *nil_ptr;
	struct error_handling errors;

	errors.init = 0;

	if (block == NULL || indices == NULL)
		bailout("copy.parse_generic", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(indices);
	reader *r = (reader*)block->tsink;
	copy_init_error_handling(&errors, cntxt, r->line_count[p->wid], col_no, col_name, rows);
	errors.r = r;

	ret = COLnew(0, tpe, n, TRANSIENT);
	if (!ret)
		bailout("copy.parse_generic",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	nil_ptr = ATOMnilptr(tpe);

	buffer_len = 0;
	const char *start = (char*)r->bs->buf[p->wid];
	const int *offsetp = (int*)Tloc(indices, 0);
	allocator *ma = cntxt->curprg->def->ma;
	for (BUN i = 0; i < n; i++) {
		gdk_return ok = GDK_SUCCEED;
		int offset = offsetp[i];
		const char *src = start + offset;
		const void *to_insert;

		if (is_int_nil(offset)) {
			to_insert = nil_ptr;
		} else if (!checkUTF8(src, NULL)) {
			ok = copy_report_error(&errors, (lng) i, -1, "incorrectly encoded UTF-8");
			to_insert = nil_ptr;
		} else {
			ssize_t len = BATatoms[tpe].atomFromStr(ma, src, &buffer_len, &buffer, false);
			if (len >= 0) {
				to_insert = buffer;
			} else {
				ok = copy_report_error(&errors, (lng) i, -1, "invalid %s: %s", ATOMname(tpe), src);
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
	if (errors.init)
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

static const char *
fltdbl_sepskip(const char *s, const char sep, const char skip)
{
	// The regular fltFromStr/dblFromStr functions do not take decimal commas
	// and thousands separators into account. When these are in use, this
	// function first converts them to decimal dots and empty strings,
	// respectively. We use a fixed size buffer so abnormally long floats such
	// as
	// +00000000000000000000000000000000000000000000000000000000000000000000001.5e1
	// will be rejected.

	if (skip || sep != '.') {
		/* inplace */
		char *p = (char*)s, *o = p;

		while (GDKisspace(*s))
			s++;
		while (*s != '\0') {
			char ch = *s++;
			if (ch == skip) {
				continue;
			} else if (ch == sep) {
				ch = '.';
			} else if (ch == '.') {
				// We're mapping sep to '.', if there are already
				// periods in the input we're losing information
				return NULL;
			}
			*p++ = ch;
		}
		// If we're here either we either encountered the end of s or the buffer is
		// full. In the latter case we still need to write the NUL.
		// We left room for it.
		*p = '\0';
		s = o;
	}
	return s;
}

str
COPYparse_float(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	str msg = MAL_SUCCEED;
	BAT *ret = NULL;
	BAT *block = BATdescriptor(*getArgReference_bat(stk, pci, 1));
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 2);
	BAT *indices = BATdescriptor(*getArgReference_bat(stk, pci, 3));
	int tpe = getArgGDKType(mb, pci, 4);
	bat rows = *getArgReference_bat(stk, pci, 5);
	int col_no = *getArgReference_int(stk, pci, 6);
	const char *col_name = *getArgReference_str(stk, pci, 7);
	str dec_sep = *getArgReference_str(stk, pci, 8);
	str dec_skip = *getArgReference_str(stk, pci, 9);
	BUN n;
	dbl localdbl;
	void *buffer = &localdbl;
	size_t buffer_len = sizeof(dbl);
	const void *nil_ptr;
	struct error_handling errors;
	int nils = 0;

	const char sep = strNil(dec_sep) ? '.' : dec_sep[0];
	const char skip = strNil(dec_skip) ? '\0' : dec_skip[0];
	errors.init = 0;

	if (block == NULL || indices == NULL)
		bailout("copy.parse_float", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(indices);
	reader *r = (reader*)block->tsink;
	copy_init_error_handling(&errors, cntxt, r->line_count[p->wid], col_no, col_name, rows);
	errors.r = r;

	ret = COLnew(0, tpe, n, TRANSIENT);
	if (!ret)
		bailout("copy.parse_float",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	nil_ptr = ATOMnilptr(tpe);

	const char *start = (char*)r->bs->buf[p->wid];
	const int *offsetp = (int*)Tloc(indices, 0);
	allocator *ma = cntxt->curprg->def->ma;
	for (BUN i = 0; i < n; i++) {
		gdk_return ok = GDK_SUCCEED;
		int offset = offsetp[i];
		const char *src = start + offset;
		const void *to_insert;

		if (is_int_nil(offset)) {
			to_insert = nil_ptr;
			nils++;
		} else {
			ssize_t len = -1;
			src = fltdbl_sepskip(src, sep, skip);
			if (src)
				len = BATatoms[tpe].atomFromStr(ma, src, &buffer_len, &buffer, false);
			if (len >= 0) {
				to_insert = buffer;
			} else {
				ok = copy_report_error(&errors, (lng) i, -1, "invalid %s: %s", ATOMname(tpe), src);
				GDKclrerr();
				to_insert = nil_ptr;
				nils++;
			}
		}
		if (ok != GDK_SUCCEED) {
			msg = copy_check_too_many_errors(&errors, "copy.parse_float");
			if (msg != MAL_SUCCEED)
				goto end;
			else
				ok = GDK_SUCCEED;
		}
		if (bunfastapp(ret, to_insert) != GDK_SUCCEED)
			bailout("copy.parse_float", GDK_EXCEPTION);
	}
	BATsetcount(ret, n);
	// we don't know anything about the data we just parsed
	ret->tkey = false;
	ret->tnil = (nils)?true:false;
	ret->tnonil = (!nils)?true:false;
	ret->tsorted = false;
	ret->trevsorted = false;
end:
	if (errors.init)
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

static BAT *
string_sharing_bat(BUN cnt, const char *base)
{
	BAT *b = COLnew(0, TYPE_int, cnt, TRANSIENT);
	Heap *hp = NULL;

	if (!b)
		return NULL;
	if ((hp = GDKmalloc(sizeof(Heap))) == NULL){
		BBPreclaim(b);
		return NULL;
    }
	char *nme = BBP_physical(b->batCacheid);
    *hp = (Heap) {
		.farmid = 1,//BBPselectfarm(b->batRole, b->ttype, varheap), // find the inmemory farm
        .parentid = b->batCacheid,
        .dirty = true,
        .refs = ATOMIC_VAR_INIT(1),
		.storage = STORE_NOWN,
		.free = GDK_ELIMLIMIT,
		.size = GDK_ELIMLIMIT,
    };
	strtconcat(hp->filename, sizeof(hp->filename), nme, ".theap", NULL);
	hp->base = (char*)base;
	b->tvheap = hp;
	b->ttype = TYPE_str;
	/* upgrade filename */
	strtconcat(b->theap->filename, sizeof(b->theap->filename), nme, ".tail4", NULL);
	return b;
}

str
COPYparse_string(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	const char *fname = "copy.parse_string";

	(void)mb;
	bat *parsed_bat_id = getArgReference_bat(stk, pci, 0);
	bat block_bat_id = *getArgReference_bat(stk, pci, 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 2);
	bat offsets_bat_id = *getArgReference_bat(stk, pci, 3);
	int maxlen = *getArgReference_int(stk, pci, 4);
	bat rows = *getArgReference_bat(stk, pci, 5);
	int col_no = *getArgReference_int(stk, pci, 6);
	str col_name = *getArgReference_str(stk, pci, 7);

	BAT *block_bat = BATdescriptor(block_bat_id);
	BAT *offsets_bat = BATdescriptor(offsets_bat_id);
	BAT *parsed_bat = NULL;
	int colwidth = maxlen;
	BUN n;

	struct error_handling errors;
	errors.init = 0;

	if (!block_bat || !offsets_bat)
		bailout(fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	reader *r = (reader*)block_bat->tsink;
	copy_init_error_handling(&errors, cntxt, r->line_count[p->wid], col_no, col_name, rows);
	errors.r = r;

	const char *start = (char*)r->bs->buf[p->wid];
	size_t nil_offset = r->bs->sz[p->wid]+2;
	/*
	parsed_bat = COLnew(0, TYPE_str, BATcount(offsets_bat), TRANSIENT);
		*/
	parsed_bat = string_sharing_bat(BATcount(offsets_bat), start);
	if (!parsed_bat)
		bailout(fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	n = BATcount(offsets_bat);
	const int *offsetp = (int*)Tloc(offsets_bat, 0);
	int *offsetr = (int*)Tloc(parsed_bat, 0);
	int nils = 0;
	for (BUN i = 0; i < n; i++) {
		gdk_return ok;
		int offset = offsetp[i];
		//const void *to_insert = str_nil;

		if (is_int_nil(offset)) {
			ok = GDK_SUCCEED;
			offset = (int) nil_offset;
			nils++;
		} else {
			const char *src = start + offset;
			size_t n;
			if (!checkUTF8(src, &n)) {
				offset = (int) nil_offset;
				nils++;
				ok = copy_report_error(&errors, (lng) i, -1, "incorrectly encoded UTF-8");
			} else if (n > (size_t) colwidth) {
				ok = copy_report_error(&errors, (lng) i, -1, "field too long, max length is %d", colwidth);
			} else {
				ok = GDK_SUCCEED;
				//to_insert = src;
			}
		}
		if (ok != GDK_SUCCEED) {
			msg = copy_check_too_many_errors(&errors, "copy.parse_generic");
			if (msg != MAL_SUCCEED)
				goto end;
			else
				ok = GDK_SUCCEED;
		}
		//if (bunfastapp_nocheck(parsed_bat, to_insert) != GDK_SUCCEED)
		//	bailout("copy.parse_generic", GDK_EXCEPTION);
		offsetr[i] = offset;
	}

	BATsetcount(parsed_bat, n);
	// we don't know anything about the data we just parsed
	parsed_bat->tkey = false;
	parsed_bat->tnil = (nils)?true:false;
	parsed_bat->tnonil = (!nils)?true:false;
	parsed_bat->tsorted = false;
	parsed_bat->trevsorted = false;

end:
	if (errors.init)
		copy_destroy_error_handling(&errors);
	if (parsed_bat) {
		if (msg == MAL_SUCCEED) {
			*parsed_bat_id = parsed_bat->batCacheid;
			//BBPkeepref(parsed_bat); -- no BAT_READ
			BBPretain(parsed_bat->batCacheid);
			BBPunfix(parsed_bat->batCacheid);
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
	bat block_bat_id, Pipeline *p, bat offsets_bat_id,
	int tpe,
	void (*f)(struct error_handling*, void*, BUN, void*, char*, int*),
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

	reader *r = (reader*)block_bat->tsink;
	errors->r = r;
	errors->starting_row = r->line_count[p->wid];
	parsed_bat = COLnew(0, tpe, BATcount(offsets_bat), TRANSIENT);
	if (!parsed_bat)
		bailout(fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	f(errors, fx, BATcount(offsets_bat), Tloc(parsed_bat, 0), (char*)r->bs->buf[p->wid], Tloc(offsets_bat, 0));
	msg = copy_check_too_many_errors(errors, fname);
	if (msg != MAL_SUCCEED)
		goto end;

	BATsetcount(parsed_bat, BATcount(offsets_bat));
	// we don't know anything about the data we just parsed
	lng nils = fx?*(int*)fx:0;
	nils += errors->count;
	parsed_bat->tkey = false;
	parsed_bat->tnil = nils?true:false;
	parsed_bat->tnonil = !nils?true:false;
	parsed_bat->tsorted = false;
	parsed_bat->trevsorted = false;
	if (BATcount(parsed_bat) <= 1)
		BATsettrivprop(parsed_bat);
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
