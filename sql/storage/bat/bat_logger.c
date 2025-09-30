/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */
#include "gdk_logger_internals.h"
#include "mutils.h"

#define CATALOG_JUL2021 52300	/* first in Jul2021 */
#define CATALOG_JAN2022 52301	/* first in Jan2022 */
#define CATALOG_SEP2022 52302	/* first in Sep2022 */
#define CATALOG_AUG2024 52303	/* first in Aug2024 */
#define CATALOG_MAR2025 52304	/* first in Mar2025 */

/* Note, CATALOG version 52300 is the first one where the basic system
 * tables (the ones created in store.c) have fixed and unchangeable
 * ids. */

/* return GDK_SUCCEED if we can handle the upgrade from oldversion to
 * newversion */
static gdk_return
bl_preversion(sqlstore *store, int oldversion, int newversion)
{
	(void)newversion;

#ifdef CATALOG_JUL2021
	if (oldversion == CATALOG_JUL2021) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_JAN2022
	if (oldversion == CATALOG_JAN2022) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_SEP2022
	if (oldversion == CATALOG_SEP2022) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_AUG2024
	if (oldversion == CATALOG_AUG2024) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_MAR2025
	if (oldversion == CATALOG_MAR2025) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

	return GDK_FAIL;
}

#if defined CATALOG_JUL2021 || defined CATALOG_JAN2022
/* replace a column in a system table with a new column
 * colid is the SQL id for the column, newcol is the new BAT */
static gdk_return
replace_bat(logger *lg, int colid, BAT *newcol)
{
	gdk_return rc;
	newcol = BATsetaccess(newcol, BAT_READ);
	if (newcol == NULL)
		return GDK_FAIL;
	if ((rc = BAThash(lg->catalog_id)) == GDK_SUCCEED) {
		BATiter cii = bat_iterator_nolock(lg->catalog_id);
		BUN p;
		MT_rwlock_rdlock(&cii.b->thashlock);
		HASHloop_int(cii, cii.b->thash, p, &colid) {
			if (BUNfnd(lg->dcatalog, &(oid){(oid)p}) == BUN_NONE) {
				if (BUNappend(lg->dcatalog, &(oid){(oid)p}, true) != GDK_SUCCEED ||
					BUNreplace(lg->catalog_lid, (oid) p, &(lng){0}, false) != GDK_SUCCEED) {
					MT_rwlock_rdunlock(&cii.b->thashlock);
					return GDK_FAIL;
				}
				break;
			}
		}
		MT_rwlock_rdunlock(&cii.b->thashlock);
		if ((rc = BUNappend(lg->catalog_id, &colid, true)) == GDK_SUCCEED &&
			(rc = BUNappend(lg->catalog_bid, &newcol->batCacheid, true)) == GDK_SUCCEED &&
			(rc = BUNappend(lg->catalog_lid, &lng_nil, false)) == GDK_SUCCEED &&
			(rc = BUNappend(lg->catalog_cnt, &(lng){BATcount(newcol)}, false)) == GDK_SUCCEED) {
			BBPretain(newcol->batCacheid);
		}
	}
	return rc;
}
#endif

static BAT *
log_temp_descriptor(log_bid b)
{
	if (b <= 0)
		return NULL;
	return temp_descriptor(b);
}

#if defined CATALOG_JAN2022 || defined CATALOG_SEP2022 || defined CATALOG_AUG2024
/* cannot use attribute((sentinel)) since sentinel is not a pointer */
static gdk_return
tabins(logger *lg, ...)
{
	va_list va;
	int cid;
	const void *cval;
	gdk_return rc;
	BAT *b;

	va_start(va, lg);
	BATiter cni = bat_iterator(lg->catalog_id);
	while ((cid = va_arg(va, int)) != 0) {
		cval = va_arg(va, void *);
		if ((b = log_temp_descriptor(log_find_bat(lg, cid))) == NULL) {
			rc = GDK_FAIL;
			break;
		}
		rc = BUNappend(b, cval, true);
		if (rc == GDK_SUCCEED) {
			BUN p;
			MT_rwlock_rdlock(&cni.b->thashlock);
			HASHloop_int(cni, cni.b->thash, p, &cid) {
				if (BUNfnd(lg->dcatalog, &(oid){p}) == BUN_NONE) {
					rc = BUNreplace(lg->catalog_cnt, p, &(lng){BATcount(b)}, false);
					break;
				}
			}
			MT_rwlock_rdunlock(&cni.b->thashlock);
		}
		bat_destroy(b);
		if (rc != GDK_SUCCEED)
			break;
	}
	bat_iterator_end(&cni);
	va_end(va);
	return rc;
}
#endif

static gdk_return
bl_postversion(void *Store, logger *lg)
{
	sqlstore *store = Store;
	gdk_return rc;

#ifdef CATALOG_JUL2021
	if (store->catalog_version <= CATALOG_JUL2021) {
		/* change the language attribute in sys.functions for sys.env,
		 * sys.var, and sys.db_users from SQL to MAL */

		/* sys.functions i.e. deleted rows */
		BAT *del_funcs = log_temp_descriptor(log_find_bat(lg, 2016));
		if (del_funcs == NULL)
			return GDK_FAIL;
		BAT *func_tid = BATmaskedcands(0, BATcount(del_funcs), del_funcs, false);
		bat_destroy(del_funcs);
		/* sys.functions.schema_id */
		BAT *func_schem = log_temp_descriptor(log_find_bat(lg, 2026));
		if (func_tid == NULL || func_schem == NULL) {
			bat_destroy(func_tid);
			bat_destroy(func_schem);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 */
		BAT *cands = BATselect(func_schem, func_tid, &(int) {2000}, NULL, true, true, false, false);
		bat_destroy(func_schem);
		if (cands == NULL) {
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		/* the functions we need to change */
		BAT *funcs = COLnew(0, TYPE_str, 3, TRANSIENT);
		if (funcs == NULL ||
			BUNappend(funcs, "db_users", false) != GDK_SUCCEED ||
			BUNappend(funcs, "env", false) != GDK_SUCCEED ||
			BUNappend(funcs, "var", false) != GDK_SUCCEED) {
			bat_destroy(cands);
			bat_destroy(funcs);
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		/* sys.functions.name */
		BAT *func_name = log_temp_descriptor(log_find_bat(lg, 2018));
		if (func_name == NULL) {
			bat_destroy(cands);
			bat_destroy(funcs);
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 and name in (...) */
		BAT *b = BATintersect(func_name, funcs, cands, NULL, false, false, 3);
		bat_destroy(cands);
		bat_destroy(func_name);
		bat_destroy(funcs);
		cands = b;
		if (cands == NULL) {
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		/* sys.functions.language */
		BAT *func_lang = log_temp_descriptor(log_find_bat(lg, 2021));
		if (func_lang == NULL) {
			bat_destroy(cands);
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 and name in (...)
		 * and language = FUNC_LANG_SQL */
		b = BATselect(func_lang, cands, &(int) {FUNC_LANG_SQL}, NULL, true, true, false, false);
		bat_destroy(cands);
		cands = b;
		if (cands == NULL) {
			bat_destroy(func_lang);
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		b = BATconstant(0, TYPE_int, &(int) {FUNC_LANG_MAL}, BATcount(cands), TRANSIENT);
		if (b == NULL) {
			bat_destroy(func_lang);
			bat_destroy(cands);
			bat_destroy(func_tid);
			return GDK_FAIL;
		}
		rc = GDK_FAIL;
		BAT *b2 = COLcopy(func_lang, func_lang->ttype, true, PERSISTENT);
		if (b2 == NULL ||
			BATreplace(b2, cands, b, false) != GDK_SUCCEED) {
			bat_destroy(b2);
			bat_destroy(cands);
			bat_destroy(b);
			bat_destroy(func_tid);
			bat_destroy(func_lang);
			return GDK_FAIL;
		}
		bat_destroy(b);
		bat_destroy(cands);

		/* additionally, update the language attribute for entries
		 * that were declared using "EXTERNAL NAME" to be MAL functions
		 * instead of SQL functions (a problem that seems to have
		 * occurred in ancient databases) */

		/* sys.functions.func */
		BAT *func_func = log_temp_descriptor(log_find_bat(lg, 2019));
		if (func_func == NULL) {
			bat_destroy(func_tid);
			bat_destroy(b2);
			return GDK_FAIL;
		}
		cands = BATselect(func_lang, func_tid, &(int){FUNC_LANG_SQL}, NULL, true, true, false, false);
		bat_destroy(func_lang);
		bat_destroy(func_tid);
		if (cands == NULL) {
			bat_destroy(b2);
			bat_destroy(func_func);
			return GDK_FAIL;
		}
		struct canditer ci;
		canditer_init(&ci, func_func, cands);
		BATiter ffi = bat_iterator_nolock(func_func);
		for (BUN p = 0; p < ci.ncand; p++) {
			oid o = canditer_next(&ci);
			const char *f = BUNtvar(ffi, o - func_func->hseqbase);
			const char *e;
			if (!strNil(f) &&
				(e = strstr(f, "external")) != NULL &&
				e > f && isspace((unsigned char) e[-1]) && isspace((unsigned char) e[8]) && strncmp(e + 9, "name", 4) == 0 && isspace((unsigned char) e[13]) &&
				BUNreplace(b2, o, &(int){FUNC_LANG_MAL}, false) != GDK_SUCCEED) {
				bat_destroy(b2);
				bat_destroy(func_func);
				return GDK_FAIL;
			}
		}
		rc = replace_bat(lg, 2021, b2);
		bat_destroy(b2);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	if (store->catalog_version <= CATALOG_JUL2021) {
		/* change the side_effects attribute in sys.functions for
		 * selected functions */

		/* sys.functions i.e. deleted rows */
		BAT *del_funcs = log_temp_descriptor(log_find_bat(lg, 2016));
		if (del_funcs == NULL)
			return GDK_FAIL;
		BAT *func_tid = BATmaskedcands(0, BATcount(del_funcs), del_funcs, false);
		bat_destroy(del_funcs);
		/* sys.functions.schema_id */
		BAT *func_schem = log_temp_descriptor(log_find_bat(lg, 2026));
		if (func_tid == NULL || func_schem == NULL) {
			bat_destroy(func_tid);
			bat_destroy(func_schem);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 */
		BAT *cands = BATselect(func_schem, func_tid, &(int) {2000}, NULL, true, true, false, false);
		bat_destroy(func_schem);
		bat_destroy(func_tid);
		if (cands == NULL) {
			return GDK_FAIL;
		}
		/* sys.functions.side_effect */
		BAT *func_se = log_temp_descriptor(log_find_bat(lg, 2023));
		if (func_se == NULL) {
			bat_destroy(cands);
			return GDK_FAIL;
		}
		/* make a copy that we can modify */
		BAT *b = COLcopy(func_se, func_se->ttype, true, PERSISTENT);
		bat_destroy(func_se);
		if (b == NULL) {
			bat_destroy(cands);
			return GDK_FAIL;
		}
		func_se = b;
		/* sys.functions.func */
		BAT *func_func = log_temp_descriptor(log_find_bat(lg, 2019));
		if (func_func == NULL) {
			bat_destroy(cands);
			bat_destroy(func_se);
			return GDK_FAIL;
		}
		/* the functions we need to change to FALSE */
		BAT *funcs = COLnew(0, TYPE_str, 1, TRANSIENT);
		if (funcs == NULL ||
			BUNappend(funcs, "sqlrand", false) != GDK_SUCCEED) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			bat_destroy(funcs);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 and func in (...) */
		b = BATintersect(func_func, funcs, cands, NULL, false, false, 1);
		bat_destroy(funcs);
		if (b == NULL) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			return GDK_FAIL;
		}
		/* while we're at it, also change sys.env and sys.db_users to
		 * being without side effect (legacy from ancient databases) */
		/* sys.functions.name */
		BAT *func_name = log_temp_descriptor(log_find_bat(lg, 2018));
		if (func_name == NULL) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			bat_destroy(b);
			return GDK_FAIL;
		}
		BAT *b2 = BATselect(func_name, cands, "env", NULL, true, true, false, false);
		if (b2 == NULL || BATappend(b, b2, NULL, false) != GDK_SUCCEED) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			bat_destroy(b);
			bat_destroy(func_name);
			bat_destroy(b2);
			return GDK_FAIL;
		}
		bat_destroy(b2);
		b2 = BATselect(func_name, cands, "db_users", NULL, true, true, false, false);
		bat_destroy(func_name);
		if (b2 == NULL || BATappend(b, b2, NULL, false) != GDK_SUCCEED) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			bat_destroy(b);
			bat_destroy(b2);
			return GDK_FAIL;
		}
		bat_destroy(b2);

		BAT *vals = BATconstant(0, TYPE_bit, &(bit) {FALSE}, BATcount(b), TRANSIENT);
		if (vals == NULL) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			bat_destroy(b);
			return GDK_FAIL;
		}
		rc = BATreplace(func_se, b, vals, false);
		bat_destroy(b);
		bat_destroy(vals);
		if (rc != GDK_SUCCEED) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			return GDK_FAIL;
		}
		/* the functions we need to change to TRUE */
		funcs = COLnew(0, TYPE_str, 5, TRANSIENT);
		if (funcs == NULL ||
			BUNappend(funcs, "copy_from", false) != GDK_SUCCEED ||
			BUNappend(funcs, "next_value", false) != GDK_SUCCEED ||
			BUNappend(funcs, "update_schemas", false) != GDK_SUCCEED ||
			BUNappend(funcs, "update_tables", false) != GDK_SUCCEED) {
			bat_destroy(cands);
			bat_destroy(func_se);
			bat_destroy(func_func);
			bat_destroy(funcs);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 and func in (...) */
		b = BATintersect(func_func, funcs, cands, NULL, false, false, 7);
		bat_destroy(funcs);
		bat_destroy(cands);
		bat_destroy(func_func);
		if (b == NULL) {
			bat_destroy(func_se);
			return GDK_FAIL;
		}
		vals = BATconstant(0, TYPE_bit, &(bit) {TRUE}, BATcount(b), TRANSIENT);
		if (vals == NULL) {
			bat_destroy(func_se);
			bat_destroy(b);
			return GDK_FAIL;
		}
		rc = BATreplace(func_se, b, vals, false);
		bat_destroy(b);
		bat_destroy(vals);
		if (rc != GDK_SUCCEED) {
			bat_destroy(func_se);
			return GDK_FAIL;
		}
		/* replace old column with modified copy */
		rc = replace_bat(lg, 2023, func_se);
		bat_destroy(func_se);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	if (store->catalog_version <= CATALOG_JUL2021) {
		/* upgrade some columns in sys.sequences:
		 * if increment is zero, set it to one (see ChangeLog);
		 * if increment is greater than zero and maxvalue is zero,
		 * set maxvalue to GDK_lng_max;
		 * if increment is less than zero and minvalue is zero,
		 * set minvalue to GDK_lng_min */

		/* sys.sequences i.e. deleted rows */
		BAT *del_seqs = log_temp_descriptor(log_find_bat(lg, 2037));
		if (del_seqs == NULL)
			return GDK_FAIL;
		BAT *seq_tid = BATmaskedcands(0, BATcount(del_seqs), del_seqs, false);
		bat_destroy(del_seqs);
		BAT *seq_min = log_temp_descriptor(log_find_bat(lg, 2042)); /* sys.sequences.minvalue */
		BAT *seq_max = log_temp_descriptor(log_find_bat(lg, 2043)); /* sys.sequences.maxvalue */
		BAT *seq_inc = log_temp_descriptor(log_find_bat(lg, 2044)); /* sys.sequences.increment */
		if (seq_tid == NULL || seq_min == NULL || seq_max == NULL || seq_inc == NULL) {
			bat_destroy(seq_tid);
			bat_destroy(seq_min);
			bat_destroy(seq_max);
			bat_destroy(seq_inc);
			return GDK_FAIL;
		}
		/* select * from sys.sequences where increment = 0 */
		BAT *inczero = BATselect(seq_inc, seq_tid, &(lng){0}, NULL, false, true, false, false);
		if (inczero == NULL) {
			bat_destroy(seq_tid);
			bat_destroy(seq_min);
			bat_destroy(seq_max);
			bat_destroy(seq_inc);
			return GDK_FAIL;
		}
		if (BATcount(inczero) > 0) {
			BAT *b = BATconstant(0, TYPE_lng, &(lng) {1}, BATcount(inczero), TRANSIENT);
			if (b == NULL) {
				bat_destroy(seq_tid);
				bat_destroy(seq_min);
				bat_destroy(seq_max);
				bat_destroy(seq_inc);
				bat_destroy(inczero);
				return GDK_FAIL;
			}
			BAT *b2 = COLcopy(seq_inc, seq_inc->ttype, true, PERSISTENT);
			rc = GDK_FAIL;
			if (b2 == NULL)
				rc = BATreplace(b2, inczero, b, false);
			bat_destroy(b);
			if (rc != GDK_SUCCEED) {
				bat_destroy(b2);
				bat_destroy(seq_tid);
				bat_destroy(seq_min);
				bat_destroy(seq_max);
				bat_destroy(seq_inc);
				bat_destroy(inczero);
				return GDK_FAIL;
			}
			rc = replace_bat(lg, 2044, b2);
			bat_destroy(seq_inc);
			seq_inc = b2;
			if (rc != GDK_SUCCEED) {
				bat_destroy(seq_tid);
				bat_destroy(seq_min);
				bat_destroy(seq_max);
				bat_destroy(seq_inc);
				bat_destroy(inczero);
				return rc;
			}
		}
		bat_destroy(inczero);
		/* select * from sys.sequences where increment > 0 */
		BAT *incpos = BATselect(seq_inc, seq_tid, &(lng){0}, &lng_nil, false, true, false, false);
		bat_destroy(seq_inc);
		if (incpos == NULL) {
			bat_destroy(seq_tid);
			bat_destroy(seq_min);
			bat_destroy(seq_max);
			return GDK_FAIL;
		}
		/* select * from sys.sequences where increment > 0 and maxvalue = 0 */
		BAT *cands = BATselect(seq_max, incpos, &(lng) {0}, NULL, true, true, false, false);
		bat_destroy(incpos);
		if (cands == NULL) {
			bat_destroy(seq_tid);
			bat_destroy(seq_min);
			bat_destroy(seq_max);
			return GDK_FAIL;
		}
		if (BATcount(cands) > 0) {
			BAT *b = BATconstant(0, TYPE_lng, &(lng){GDK_lng_max}, BATcount(cands), TRANSIENT);
			BAT *b2 = COLcopy(seq_max, seq_max->ttype, true, PERSISTENT);
			rc = GDK_FAIL;
			if (b != NULL && b2 != NULL)
				rc = BATreplace(b2, cands, b, false);
			bat_destroy(b);
			if (rc == GDK_SUCCEED)
				rc = replace_bat(lg, 2043, b2);
			bat_destroy(b2);
			if (rc != GDK_SUCCEED) {
				bat_destroy(cands);
				bat_destroy(seq_tid);
				bat_destroy(seq_min);
				bat_destroy(seq_max);
				return rc;
			}
		}
		bat_destroy(seq_max);
		bat_destroy(cands);
		/* select * from sys.sequences where increment < 0 */
		BAT *incneg = BATselect(seq_inc, seq_tid, &lng_nil, &(lng){0}, false, true, false, false);
		bat_destroy(seq_tid);
		/* select * from sys.sequences where increment < 0 and minvalue = 0 */
		cands = BATselect(seq_min, incneg, &(lng) {0}, NULL, true, true, false, false);
		bat_destroy(incneg);
		if (cands == NULL) {
			bat_destroy(seq_min);
			return GDK_FAIL;
		}
		if (BATcount(cands) > 0) {
			BAT *b = BATconstant(0, TYPE_lng, &(lng){GDK_lng_min}, BATcount(cands), TRANSIENT);
			BAT *b2 = COLcopy(seq_min, seq_min->ttype, true, PERSISTENT);
			rc = GDK_FAIL;
			if (b != NULL && b2 != NULL)
				rc = BATreplace(b2, cands, b, false);
			bat_destroy(b);
			if (rc == GDK_SUCCEED)
				rc = replace_bat(lg, 2042, b2);
			bat_destroy(b2);
			if (rc != GDK_SUCCEED) {
				bat_destroy(cands);
				bat_destroy(seq_min);
				return rc;
			}
		}
		bat_destroy(seq_min);
		bat_destroy(cands);
	}
#endif

#ifdef CATALOG_JAN2022
	if (store->catalog_version <= CATALOG_JAN2022) {
		/* GRANT SELECT ON sys.db_user_info TO monetdb;
		 * except the grantor is 0 instead of user monetdb
		 *
		 * we need to find the IDs of the sys.db_user_info table and of
		 * the sys.privileges table and its columns since none of these
		 * have fixed IDs */
		BAT *b = log_temp_descriptor(log_find_bat(lg, 2067)); /* sys._tables */
		if (b == NULL)
			return GDK_FAIL;
		BAT *del_tabs = BATmaskedcands(0, BATcount(b), b, false);
		bat_destroy(b);
		if (del_tabs == NULL)
			return GDK_FAIL;
		b = log_temp_descriptor(log_find_bat(lg, 2076)); /* sys._columns */
		if (b == NULL) {
			bat_destroy(del_tabs);
			return GDK_FAIL;
		}
		BAT *del_cols = BATmaskedcands(0, BATcount(b), b, false);
		bat_destroy(b);
		b = log_temp_descriptor(log_find_bat(lg, 2070)); /* sys._tables.schema_id */
		if (del_cols == NULL || b == NULL) {
			bat_destroy(del_cols);
			bat_destroy(b);
			bat_destroy(del_tabs);
			return GDK_FAIL;
		}
		BAT *cands = BATselect(b, del_tabs, &(int) {2000}, NULL, true, true, false, false);
		bat_destroy(b);
		bat_destroy(del_tabs);
		/* cands contains undeleted rows from sys._tables for tables in
		 * sys schema */
		BAT *tabnme = log_temp_descriptor(log_find_bat(lg, 2069)); /* sys._tables.name */
		if (cands == NULL || tabnme == NULL) {
			bat_destroy(cands);
			bat_destroy(tabnme);
			bat_destroy(del_cols);
			return GDK_FAIL;
		}
		b = BATselect(tabnme, cands, "db_user_info", NULL, true, true, false, false);
		if (b == NULL) {
			bat_destroy(cands);
			bat_destroy(tabnme);
			bat_destroy(del_cols);
			return GDK_FAIL;
		}
		oid dbpos = BUNtoid(b, 0);
		bat_destroy(b);
		b = BATselect(tabnme, cands, "privileges", NULL, true, true, false, false);
		bat_destroy(tabnme);
		bat_destroy(cands);
		BAT *tabid = log_temp_descriptor(log_find_bat(lg, 2068)); /* sys._tables.id */
		if (b == NULL || tabid == NULL) {
			bat_destroy(b);
			bat_destroy(tabid);
			bat_destroy(del_cols);
			return GDK_FAIL;
		}
		int dbid = ((int *) tabid->theap->base)[dbpos];
		int prid = ((int *) tabid->theap->base)[BUNtoid(b, 0)];
		BAT *coltid = log_temp_descriptor(log_find_bat(lg, 2082)); /* sys._columns.table_id */
		if (coltid == NULL) {
			bat_destroy(b);
			bat_destroy(del_cols);
			bat_destroy(tabid);
			return GDK_FAIL;
		}
		BAT *b1;
		rc = BATjoin(&b1, NULL, coltid, tabid, del_cols, b, false, 5);
		bat_destroy(coltid);
		bat_destroy(tabid);
		bat_destroy(del_cols);
		bat_destroy(b);
		BAT *colnr = log_temp_descriptor(log_find_bat(lg, 2085)); /* sys._columns.number */
		BAT *colid = log_temp_descriptor(log_find_bat(lg, 2077)); /* sys._columns.id */
		if (rc != GDK_SUCCEED || colnr == NULL || colid == NULL) {
			if (rc == GDK_SUCCEED)
				bat_destroy(b1);
			bat_destroy(colnr);
			bat_destroy(colid);
			return GDK_FAIL;
		}
		int privids[5];
		for (int i = 0; i < 5; i++) {
			oid p = BUNtoid(b1, i);
			privids[((int *) colnr->theap->base)[p]] = ((int *) colid->theap->base)[p];
		}
		bat_destroy(b1);
		bat_destroy(colnr);
		bat_destroy(colid);
		rc = tabins(lg,
					prid, &(msk) {false}, /* sys.privileges */
					privids[0], &dbid, /* sys.privileges.obj_id */
					privids[1], &(int) {USER_MONETDB}, /* sys.privileges.auth_id */
					privids[2], &(int) {PRIV_SELECT}, /* sys.privileges.privileges */
					privids[3], &(int) {0}, /* sys.privileges.grantor */
					privids[4], &(int) {0}, /* sys.privileges.grantee */
					0);
		if (rc != GDK_SUCCEED)
			return rc;
	}
#endif

#ifdef CATALOG_SEP2022
	if (store->catalog_version <= CATALOG_SEP2022) {
		/* new STRING column sys.keys.check */
		BAT *b = log_temp_descriptor(log_find_bat(lg, 2088)); /* sys.keys.id */
		if (b == NULL)
			return GDK_FAIL;
		BAT *check = BATconstant(b->hseqbase, TYPE_str, ATOMnilptr(TYPE_str), BATcount(b), PERSISTENT);
		bat_destroy(b);
		if (check == NULL)
			return GDK_FAIL;
		if ((check = BATsetaccess(check, BAT_READ)) == NULL ||
				/* 2165 is sys.keys.check */
				BUNappend(lg->catalog_id, &(int) {2165}, true) != GDK_SUCCEED ||
				BUNappend(lg->catalog_bid, &check->batCacheid, true) != GDK_SUCCEED ||
				BUNappend(lg->catalog_lid, &lng_nil, false) != GDK_SUCCEED ||
				BUNappend(lg->catalog_cnt, &(lng){BATcount(check)}, false) != GDK_SUCCEED
		) {
			bat_destroy(check);
			return GDK_FAIL;
		}
		BBPretain(check->batCacheid);
		bat_destroy(check);

		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2165 is sys.keys.check */
				   2077, &(int) {2165},		/* sys._columns.id */
				   2078, "check",			/* sys._columns.name */
				   2079, "varchar",			/* sys._columns.type */
				   2080, &(int) {2048},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2087 is sys.keys */
				   2082, &(int) {2087},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {6},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2166 is tmp.keys.check */
				   2077, &(int) {2166},		/* sys._columns.id */
				   2078, "check",			/* sys._columns.name */
				   2079, "varchar",			/* sys._columns.type */
				   2080, &(int) {2048},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2135 is tmp.keys */
				   2082, &(int) {2135},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {6},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
	}
#endif

#ifdef CATALOG_AUG2024
	if (store->catalog_version <= CATALOG_AUG2024) {
		/* remove function sys.st_interiorrings and its arguments since
		 * it references the now removed type GEOMETRYA */
		BAT *del_funcs = log_temp_descriptor(log_find_bat(lg, 2016)); /* sys.functions */
		if (del_funcs == NULL)
			return GDK_FAIL;
		BAT *dels = BATmaskedcands(0, BATcount(del_funcs), del_funcs, false);
		if (dels == NULL) {
			bat_destroy(del_funcs);
			return GDK_FAIL;
		}
		BAT *b = log_temp_descriptor(log_find_bat(lg, 2026)); /* sys.functions.schema_id */
		if (b == NULL) {
			bat_destroy(del_funcs);
			bat_destroy(dels);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 */
		BAT *cands = BATselect(b, dels, &(int) {2000}, NULL, true, true, false, false);
		bat_destroy(b);
		bat_destroy(dels);
		b = log_temp_descriptor(log_find_bat(lg, 2018)); /* sys.functions.name */
		if (cands == NULL || b == NULL) {
			bat_destroy(del_funcs);
			bat_destroy(cands);
			bat_destroy(b);
			return GDK_FAIL;
		}
		/* select * from sys.functions where schema_id = 2000 and name = 'st_interiorrings' */
		BAT *funcs = BATselect(b, cands, "st_interiorrings", NULL, true, true, false, false);
		bat_destroy(cands);
		bat_destroy(b);
		if (funcs == NULL) {
			bat_destroy(del_funcs);
			return GDK_FAIL;
		}
		/* here, funcs contains the BUNs for the function
		 * sys.st_interiorrings; if there are none, we're done */
		if (BATcount(funcs) > 0) {
			b = log_temp_descriptor(log_find_bat(lg, 2017)); /* sys.functions.id */
			if (b == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(funcs);
				return GDK_FAIL;
			}
			BAT *del_args = log_temp_descriptor(log_find_bat(lg, 2028)); /* sys.args */
			if (del_args == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(funcs);
				bat_destroy(b);
				return GDK_FAIL;
			}
			dels = BATmaskedcands(0, BATcount(del_args), del_args, false);
			if (dels == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(del_args);
				bat_destroy(funcs);
				bat_destroy(b);
				return GDK_FAIL;
			}
			BAT *a = log_temp_descriptor(log_find_bat(lg, 2030)); /* sys.args.func_id */
			if (a == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(del_args);
				bat_destroy(funcs);
				bat_destroy(b);
				return GDK_FAIL;
			}
			BAT *r1, *r2;
			gdk_return rc;
			/* find arguments to function sys.st_interiorrings */
			rc = BATjoin(&r1, &r2, b, a, funcs, dels, false, 10);
			bat_destroy(dels);
			bat_destroy(b);
			bat_destroy(a);
			if (rc != GDK_SUCCEED) {
				bat_destroy(del_funcs);
				bat_destroy(del_args);
				bat_destroy(funcs);
				return GDK_FAIL;
			}
			b = COLcopy(del_funcs, del_funcs->ttype, true, PERSISTENT);
			a = COLcopy(del_args, del_args->ttype, true, PERSISTENT);
			bat_destroy(del_funcs);
			bat_destroy(del_args);
			if (b == NULL || a == NULL) {
				bat_destroy(funcs);
				bat_destroy(r1);
				bat_destroy(r2);
				return GDK_FAIL;
			}
			/* now set the deleted bit for all functions and all
			 * arguments that we've found (i.e. just the input and
			 * output arg for sys.st_interiorrings and the function
			 * itself) */
			BUN p, q;
			BATloop (r1, p, q) {
				oid o = BUNtoid(r1, p);
				if (BUNreplace(b, o, &(bool) {true}, false) != GDK_SUCCEED) {
					bat_destroy(funcs);
					bat_destroy(r1);
					bat_destroy(r2);
					bat_destroy(b);
					bat_destroy(a);
					return GDK_FAIL;
				}
				o = BUNtoid(r2, p);
				if (BUNreplace(a, o, &(bool) {true}, false) != GDK_SUCCEED) {
					bat_destroy(funcs);
					bat_destroy(r1);
					bat_destroy(r2);
					bat_destroy(b);
					bat_destroy(a);
					return GDK_FAIL;
				}
			}
			bat_destroy(r1);
			bat_destroy(r2);
			rc = replace_bat(lg, 2016, b);
			if (rc == GDK_SUCCEED)
				rc = replace_bat(lg, 2028, a);
			bat_destroy(b);
			bat_destroy(a);
			if (rc != GDK_SUCCEED) {
				bat_destroy(funcs);
				return rc;
			}
		}
		bat_destroy(funcs);
	}
	if (store->catalog_version <= CATALOG_AUG2024) {
		/* new TINYINT column sys.functions.order_specification */
		BAT *ftype = log_temp_descriptor(log_find_bat(lg, 2022)); /* sys.functions.type (int) */
		BAT *fname = log_temp_descriptor(log_find_bat(lg, 2018)); /* sys.functions.name (str) */
		if (ftype == NULL || fname == NULL)
			return GDK_FAIL;
		bte zero = 0;
		BAT *order_spec = BATconstant(ftype->hseqbase, TYPE_bte, &zero, BATcount(ftype), PERSISTENT);
		/* update functions set order_specification=1 where type == aggr and name in ('group_concat', 'listagg', 'xmlagg')
		 * update functions set order_specification=2 where type == aggr and name = 'quantile' */
		if (order_spec == NULL) {
			bat_destroy(ftype);
			bat_destroy(fname);
			return GDK_FAIL;
		}
		bte *os = (bte*)Tloc(order_spec, 0);
		int *ft = (int*)Tloc(ftype, 0);
		BATiter fni = bat_iterator_nolock(fname);
		for(BUN b = 0; b < BATcount(ftype); b++) {
			if (ft[b] == F_AGGR) {
				const char *f = BUNtvar(fni, b);
				if (strcmp(f, "group_concat") == 0 || strcmp(f, "listagg") == 0 || strcmp(f, "xmlagg") == 0)
					os[b] = 1;
				else if (strcmp(f, "quantile") == 0 || strcmp(f, "quantile_avg") == 0)
					os[b] = 2;
			}
		}
		bat_destroy(ftype);
		bat_destroy(fname);
		if ((order_spec = BATsetaccess(order_spec, BAT_READ)) == NULL ||
			/* 2167 is sys.functions.order_specification */
			BUNappend(lg->catalog_id, &(int) {2167}, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_bid, &order_spec->batCacheid, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_lid, &lng_nil, false) != GDK_SUCCEED ||
			BUNappend(lg->catalog_cnt, &(lng){BATcount(order_spec)}, false) != GDK_SUCCEED
			) {
			bat_destroy(order_spec);
			return GDK_FAIL;
		}
		BBPretain(order_spec->batCacheid);
		bat_destroy(order_spec);

		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2167 is sys.functions.order_specification */
				   2077, &(int) {2167},		/* sys._columns.id */
				   2078, "order_specification",			/* sys._columns.name */
				   2079, "tinyint",			/* sys._columns.type */
				   2080, &(int) {7},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2016 is sys.functions */
				   2082, &(int) {2016},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {12},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	if (store->catalog_version <= CATALOG_AUG2024) {
		/* new TINYINT column sys._columns.column_type */
		BAT *b = log_temp_descriptor(log_find_bat(lg, 2077)); /* sys._columns.id */
		if (b == NULL)
			return GDK_FAIL;
		BUN colcnt = BATcount(b); /* we'll need it a few times */
		bat_destroy(b);
		BAT *coltype = BATconstant(b->hseqbase, TYPE_bte, &(bte) {0}, colcnt, PERSISTENT);
		if (coltype == NULL)
			return GDK_FAIL;
		if ((coltype = BATsetaccess(coltype, BAT_READ)) == NULL ||
			/* 2168 is sys._columns.column_type */
			BUNappend(lg->catalog_id, &(int) {2168}, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_bid, &coltype->batCacheid, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_lid, &lng_nil, false) != GDK_SUCCEED ||
			BUNappend(lg->catalog_cnt, &(lng) {colcnt}, false) != GDK_SUCCEED
			) {
			bat_destroy(coltype);
			return GDK_FAIL;
		}
		BBPretain(coltype->batCacheid);
		bat_destroy(coltype);

		/* new TINYINT column sys._columns.multiset */
		BAT *multiset = BATconstant(b->hseqbase, TYPE_bte, &(bte) {MS_VALUE}, colcnt, PERSISTENT);
		if (multiset == NULL)
			return GDK_FAIL;
		if ((multiset = BATsetaccess(multiset, BAT_READ)) == NULL ||
			/* 2170 is sys._columns.multiset */
			BUNappend(lg->catalog_id, &(int) {2170}, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_bid, &multiset->batCacheid, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_lid, &lng_nil, false) != GDK_SUCCEED ||
			BUNappend(lg->catalog_cnt, &(lng) {colcnt}, false) != GDK_SUCCEED
			) {
			bat_destroy(multiset);
			return GDK_FAIL;
		}
		BBPretain(multiset->batCacheid);
		bat_destroy(multiset);

		/* new TINYINT column sys._columns.multiset */
		b = log_temp_descriptor(log_find_bat(lg, 2029)); /* sys.args.id */
		if (b == NULL)
			return GDK_FAIL;
		multiset = BATconstant(b->hseqbase, TYPE_bte, &(bte) {MS_VALUE}, BATcount(b), PERSISTENT);
		bat_destroy(b);
		if (multiset == NULL)
			return GDK_FAIL;
		if ((multiset = BATsetaccess(multiset, BAT_READ)) == NULL ||
			/* 2172 is sys.args.multiset */
			BUNappend(lg->catalog_id, &(int) {2172}, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_bid, &multiset->batCacheid, true) != GDK_SUCCEED ||
			BUNappend(lg->catalog_lid, &lng_nil, false) != GDK_SUCCEED ||
			BUNappend(lg->catalog_cnt, &(lng) {BATcount(multiset)}, false) != GDK_SUCCEED
			) {
			bat_destroy(multiset);
			return GDK_FAIL;
		}
		BBPretain(multiset->batCacheid);
		bat_destroy(multiset);

		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2168 is sys._columns.column_type */
				   2077, &(int) {2168},		/* sys._columns.id */
				   2078, "column_type",		/* sys._columns.name */
				   2079, "tinyint",			/* sys._columns.type */
				   2080, &(int) {7},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2076 is sys._columns */
				   2082, &(int) {2076},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {10},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   2168, &(bte) {0},		/* sys._columns.column_type */
				   2170, &(bte) {MS_VALUE},	/* sys._columns.multiset */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2169 is tmp._columns.column_type */
				   2077, &(int) {2169},		/* sys._columns.id */
				   2078, "column_type",		/* sys._columns.name */
				   2079, "tinyint",			/* sys._columns.type */
				   2080, &(int) {7},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2124 is tmp._columns */
				   2082, &(int) {2124},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {10},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   2168, &(bte) {0},		/* sys._columns.column_type */
				   2170, &(bte) {MS_VALUE},	/* sys._columns.multiset */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2170 is sys._columns.multiset */
				   2077, &(int) {2170},		/* sys._columns.id */
				   2078, "multiset",		/* sys._columns.name */
				   2079, "tinyint",			/* sys._columns.type */
				   2080, &(int) {7},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2076 is sys._columns */
				   2082, &(int) {2076},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {11},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   2168, &(bte) {0},		/* sys._columns.column_type */
				   2170, &(bte) {MS_VALUE},	/* sys._columns.multiset */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2171 is tmp._columns.multiset */
				   2077, &(int) {2171},		/* sys._columns.id */
				   2078, "multiset",		/* sys._columns.name */
				   2079, "tinyint",			/* sys._columns.type */
				   2080, &(int) {7},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2124 is tmp._columns */
				   2082, &(int) {2124},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {11},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   2168, &(bte) {0},		/* sys._columns.column_type */
				   2170, &(bte) {MS_VALUE},	/* sys._columns.multiset */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
		if (tabins(lg,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2172 is sy.args.multiset */
				   2077, &(int) {2172},		/* sys._columns.id */
				   2078, "multiset",		/* sys._columns.name */
				   2079, "tinyint",			/* sys._columns.type */
				   2080, &(int) {7},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2028 is sy.args */
				   2082, &(int) {2028},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {8},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   2168, &(bte) {0},		/* sys._columns.column_type */
				   2170, &(bte) {MS_VALUE},	/* sys._columns.multiset */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
	}
#endif

#ifdef CATALOG_MAR2025
	if (store->catalog_version <= CATALOG_MAR2025) {
		/* nothing to do */
	}
#endif

	return GDK_SUCCEED;
}

static int
bl_create(sqlstore *store, int debug, const char *logdir, int cat_version)
{
	if (store->logger)
		return LOG_ERR;
	store->logger = log_create(debug, "sql", logdir, cat_version, (preversionfix_fptr)&bl_preversion, (postversionfix_fptr)&bl_postversion, store);
	if (store->logger)
		return LOG_OK;
	return LOG_ERR;
}

static void
bl_destroy(sqlstore *store)
{
	logger *l = store->logger;

	store->logger = NULL;
	if (l)
		log_destroy(l);
}

static int
bl_flush(sqlstore *store, lng save_id)
{
	if (store->logger)
		return log_flush(store->logger, save_id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static int
bl_activate(sqlstore *store)
{
	if (store->logger)
		return log_activate(store->logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static int
bl_changes(sqlstore *store)
{
	return (int) MIN(log_changes(store->logger), GDK_int_max);
}

static int
bl_get_sequence(sqlstore *store, int seq, lng *id)
{
	return log_sequence(store->logger, seq, id);
}

static int
bl_log_isnew(sqlstore *store)
{
	logger *bat_logger = store->logger;
	if (BATcount(bat_logger->catalog_bid) > 10) {
		return 0;
	}
	return 1;
}

static int
bl_tstart(sqlstore *store, bool flush, ulng *log_file_id)
{
	return log_tstart(store->logger, flush, log_file_id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
bl_tend(sqlstore *store)
{
	return log_tend(store->logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
bl_tflush(sqlstore *store, ulng log_file_id, ulng commit_ts)
{
	return log_tflush(store->logger, log_file_id, commit_ts) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
bl_sequence(sqlstore *store, int seq, lng id)
{
	return log_tsequence(store->logger, seq, id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

/* Write a plan entry to copy part of the given file.
 * That part of the file must remain unchanged until the plan is executed.
 */
__attribute__((__warn_unused_result__))
static gdk_return
snapshot_lazy_copy_file(stream *plan, const char *name, uint64_t extent)
{
	if (mnstr_printf(plan, "c %" PRIu64 " %s\n", extent, name) < 0) {
		GDKerror("%s", mnstr_peek_error(plan));
		return GDK_FAIL;
	}
	return GDK_SUCCEED;
}

/* Write a plan entry to write the current contents of the given file.
 * The contents are included in the plan so the source file is allowed to
 * change in the mean time.
 */
__attribute__((__warn_unused_result__))
static gdk_return
snapshot_immediate_copy_file(stream *plan, const char *path, const char *name)
{
	gdk_return ret = GDK_FAIL;
	const size_t bufsize = 64 * 1024;
	struct stat statbuf;
	char *buf = NULL;
	stream *s = NULL;
	size_t to_copy;

	if (MT_stat(path, &statbuf) < 0) {
		GDKsyserror("stat failed on %s", path);
		goto end;
	}
	to_copy = (size_t) statbuf.st_size;

	s = open_rstream(path);
	if (!s) {
		GDKerror("%s", mnstr_peek_error(NULL));
		goto end;
	}

	buf = GDKmalloc(bufsize);
	if (!buf) {
		GDKerror("GDKmalloc failed");
		goto end;
	}

	if (mnstr_printf(plan, "w %zu %s\n", to_copy, name) < 0) {
		GDKerror("%s", mnstr_peek_error(plan));
		goto end;
	}

	while (to_copy > 0) {
		size_t chunk = (to_copy <= bufsize) ? to_copy : bufsize;
		ssize_t bytes_read = mnstr_read(s, buf, 1, chunk);
		if (bytes_read < 0) {
			GDKerror("Reading bytes of component %s failed: %s", path, mnstr_peek_error(s));
			goto end;
		} else if (bytes_read < (ssize_t) chunk) {
			GDKerror("Read only %zu/%zu bytes of component %s: %s", (size_t) bytes_read, chunk, path, mnstr_peek_error(s));
			goto end;
		}

		ssize_t bytes_written = mnstr_write(plan, buf, 1, chunk);
		if (bytes_written < 0) {
			GDKerror("Writing to plan failed: %s", mnstr_peek_error(plan));
			goto end;
		} else if (bytes_written < (ssize_t) chunk) {
			GDKerror("write to plan truncated");
			goto end;
		}
		to_copy -= chunk;
	}

	ret = GDK_SUCCEED;
end:
	GDKfree(buf);
	if (s)
		close_stream(s);
	return ret;
}

/* Add plan entries for all relevant files in the Write Ahead Log */
__attribute__((__warn_unused_result__))
static gdk_return
snapshot_wal(logger *bat_logger, stream *plan, const char *db_dir)
{
	char log_file[FILENAME_MAX];
	int len;

	len = snprintf(log_file, sizeof(log_file), "%s/%s%s", db_dir, bat_logger->dir, LOGFILE);
	if (len == -1 || (size_t)len >= sizeof(log_file)) {
		GDKerror("Could not open %s, filename is too large", log_file);
		return GDK_FAIL;
	}
	if (snapshot_immediate_copy_file(plan, log_file, log_file + strlen(db_dir) + 1) != GDK_SUCCEED)
		return GDK_FAIL;

	for (ulng id = bat_logger->saved_id+1; id <= bat_logger->id; id++) {
		struct stat statbuf;

		len = snprintf(log_file, sizeof(log_file), "%s/%s%s." ULLFMT, db_dir, bat_logger->dir, LOGFILE, id);
		if (len == -1 || (size_t)len >= sizeof(log_file)) {
			GDKerror("Could not open %s, filename is too large", log_file);
			return GDK_FAIL;
		}
		if (MT_stat(log_file, &statbuf) == 0) {
			if (snapshot_lazy_copy_file(plan, log_file + strlen(db_dir) + 1, statbuf.st_size) != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			GDKerror("Could not open %s", log_file);
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

__attribute__((__warn_unused_result__))
static gdk_return
snapshot_heap(stream *plan, const char *db_dir, bat batid, const char *filename, const char *suffix, uint64_t extent)
{
	char path1[FILENAME_MAX];
	char path2[FILENAME_MAX];
	const size_t offset = strlen(db_dir) + 1;
	struct stat statbuf;
	int len;

	if (extent == 0) {
		/* nothing to copy */
		return GDK_SUCCEED;
	}
	// first check the backup dir
	len = snprintf(path1, sizeof(path1), "%s/%s/%o.%s", db_dir, BAKDIR, (unsigned) batid, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path1[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path1);
		return GDK_FAIL;
	}
	if (MT_stat(path1, &statbuf) == 0) {
		return snapshot_lazy_copy_file(plan, path1 + offset, extent);
	}
	if (errno != ENOENT) {
		GDKsyserror("Error stat'ing %s", path1);
		return GDK_FAIL;
	}

	// then check the regular location
	len = snprintf(path2, sizeof(path2), "%s/%s/%s.%s", db_dir, BATDIR, filename, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path2[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path2);
		return GDK_FAIL;
	}
	if (MT_stat(path2, &statbuf) == 0) {
		return snapshot_lazy_copy_file(plan, path2 + offset, extent);
	}
	if (errno != ENOENT) {
		GDKsyserror("Error stat'ing %s", path2);
		return GDK_FAIL;
	}

	GDKerror("One of %s and %s must exist", path1, path2);
	return GDK_FAIL;
}

static gdk_return
patch_bbpdir(BAT *bats_to_omit)
{
	gdk_return r, ret = GDK_FAIL;

	FILE *in = NULL;
	FILE *out = NULL;
	int state = 0;
	char buf[3000];
	BAT *sorted = NULL;
	BATiter item_iter;
	bool item_iter_initialized = false;

	assert(BATttype(bats_to_omit) == TYPE_int);

	if (BBPdir_first(true, -1, &in, &out) != GDK_SUCCEED)
		goto end;

	r = BATsort(&sorted, NULL, NULL, bats_to_omit, NULL, NULL, false, false, false);
	if (r != GDK_SUCCEED)
		goto end;
	item_iter = bat_iterator(sorted);
	item_iter_initialized = true;
	bat *items = item_iter.base;
	for (BUN i = 0; i < item_iter.count; i++) {
		bat id = items[i];
		BATiter scratch = bat_iterator(BBP_desc(id));
		state = BBPdir_step(id, 0, state, buf, sizeof(buf), &in, out, &scratch, NULL);
		bat_iterator_end(&scratch);
		if (state < -1)
			goto end;
	}

	if (BBPdir_last(state, buf, sizeof(buf), in, out) == GDK_SUCCEED) {
		/* _last closed them for us */
		in = NULL;
		out = NULL;
	} else {
		goto end;
	}

	ret = GDK_SUCCEED;
end:
	if (item_iter_initialized)
		bat_iterator_end(&item_iter);
	if (sorted)
		BBPreclaim(sorted);
	if (in)
		fclose(in);
	if (out) {
		fclose(out);
	}
	return ret;
}

/* Add plan entries for all persistent BATs by looping over the BBP.dir.
 * Also include the BBP.dir itself.
 */
__attribute__((__warn_unused_result__))
static gdk_return
snapshot_bats(stream *plan, BAT *bats_to_omit, const char *db_dir)
{
	char bbpdir[FILENAME_MAX];
	char dest_bbp[20];
	FILE *fp = NULL;
	gdk_return r, ret = GDK_FAIL;
	int lineno = 0;
	bat bbpsize = 0;
	lng logno;
	unsigned bbpversion;

	// bbpdir is the full path to the patched version of BBP.dir that we will
	// be reading. dest_bbp is the path inside the snapshot where we will store it.
	if (GDKfilepath(bbpdir, sizeof(bbpdir), 0, BATDIR, "BBP", "dir") != GDK_SUCCEED ||
		GDKfilepath(dest_bbp, sizeof(dest_bbp), NOFARM, BAKDIR, "BBP", "dir") != GDK_SUCCEED)
		return GDK_FAIL;

	// At this point 'bbpdir' (bat/BBP.dir) does not exist, only BACKUP/bat/BBP.dir exists.
	if (patch_bbpdir(bats_to_omit) != GDK_SUCCEED)
		goto end;
	// At this point, 'bbpdir' (bat/BBP.dir) does exist and is a modified copy of BACKUP/bat/BBP.dir

	ret = snapshot_immediate_copy_file(plan, bbpdir, dest_bbp);
	if (ret != GDK_SUCCEED)
		goto end;

	// Open the catalog and parse the header
	fp = fopen(bbpdir, "r");
	if (fp == NULL) {
		GDKerror("Could not open %s for reading: %s", bbpdir, mnstr_peek_error(NULL));
		goto end;
	}
	bbpversion = BBPheader(fp, &lineno, &bbpsize, &logno, false);
	if (bbpversion == 0)
		goto end;
	assert(bbpversion == GDKLIBRARY);

	for (;;) {
		BAT b;
		Heap h;
		Heap vh;
		vh = h = (Heap) {
			.free = 0,
		};
		b = (BAT) {
			.theap = &h,
			.tvheap = &vh,
		};
		char *options;
		char filename[sizeof(BBP_physical(0))];
		char batname[129];
#ifdef GDKLIBRARY_HASHASH
		int hashash;
#endif

		switch (BBPreadBBPline(fp, bbpversion, &lineno, &b,
#ifdef GDKLIBRARY_HASHASH
							   &hashash,
#endif
							   batname, filename, &options)) {
		case 0:
			/* end of file */
			ret = GDK_SUCCEED;
			goto end;
		case 1:
			/* successfully read an entry */
			break;
		default:
			/* error */
			ret = GDK_FAIL;
			goto end;
		}
#ifdef GDKLIBRARY_HASHASH
		assert(hashash == 0);
#endif

		if (b.batCount == 0) {
			continue;
		}

		// Include the heaps in the plan
		if (ATOMvarsized(b.ttype)) {
			r = snapshot_heap(plan, db_dir, b.batCacheid, filename, "theap", b.tvheap->free);
			if (r != GDK_SUCCEED)
				goto end;
		}
		r = snapshot_heap(plan, db_dir, b.batCacheid, filename, BATtailname(&b), b.theap->free);
		if (r != GDK_SUCCEED)
			goto end;
	}

	ret = GDK_SUCCEED;
end:
	if (fp) {
		fclose(fp);
	}
	return ret;
}

__attribute__((__warn_unused_result__))
static gdk_return
snapshot_vaultkey(stream *plan, const char *db_dir)
{
	char path[FILENAME_MAX];
	struct stat statbuf;

	int len = snprintf(path, sizeof(path), "%s/.vaultkey", db_dir);
	if (len == -1 || len >= FILENAME_MAX) {
		path[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path);
		return GDK_FAIL;
	}
	if (MT_stat(path, &statbuf) == 0) {
		return snapshot_lazy_copy_file(plan, ".vaultkey", statbuf.st_size);
	}
	if (errno == ENOENT) {
		// No .vaultkey? Fine.
		return GDK_SUCCEED;
	}

	GDKsyserror("Error stat'ing %s", path);
	return GDK_FAIL;
}

static gdk_return
bl_snapshot(sqlstore *store, BAT *bats_to_omit, stream *plan)
{
	logger *bat_logger = store->logger;
	gdk_return ret;
	char db_dir[MAXPATH];
	size_t db_dir_len;

	// Farm 0 is always the persistent farm.
	if (GDKfilepath(db_dir, sizeof(db_dir), 0, NULL, "", NULL) != GDK_SUCCEED)
		return GDK_FAIL;
	db_dir_len = strlen(db_dir);
	if (db_dir[db_dir_len - 1] == DIR_SEP)
		db_dir[db_dir_len - 1] = '\0';

	if (mnstr_printf(plan, "%s\n", db_dir) < 0 ||
		// Please monetdbd
		mnstr_printf(plan, "w 0 .uplog\n") < 0) {
		GDKerror("%s", mnstr_peek_error(plan));
		ret = GDK_FAIL;
		goto end;
	}

	ret = snapshot_vaultkey(plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_bats(plan, bats_to_omit, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_wal(bat_logger, plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = GDK_SUCCEED;
end:
	return ret;
}

void
bat_logger_init( logger_functions *lf )
{
	lf->create = bl_create;
	lf->destroy = bl_destroy;
	lf->flush = bl_flush;
	lf->activate = bl_activate;
	lf->changes = bl_changes;
	lf->get_sequence = bl_get_sequence;
	lf->log_isnew = bl_log_isnew;
	lf->log_tstart = bl_tstart;
	lf->log_tend = bl_tend;
	lf->log_tflush = bl_tflush;
	lf->log_tsequence = bl_sequence;
	lf->get_snapshot_files = bl_snapshot;
}
