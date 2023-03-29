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
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */
#include "wlc.h"
#include "gdk_logger_internals.h"
#include "mutils.h"

#define CATALOG_NOV2019 52203	/* first in Apr2019 */
#define CATALOG_JUN2020 52204	/* first in Jun2020 */
#define CATALOG_JUN2020_MMT 52206	/* only in Jun2020-mmt */
#define CATALOG_OCT2020 52205	/* first in Oct2020 */
#define CATALOG_JUL2021 52300	/* first in Jul2021 */
#define CATALOG_JAN2022 52301	/* first in Jan2022 */

/* Note, CATALOG version 52300 is the first one where the basic system
 * tables (the ones created in store.c) have fixed and unchangeable
 * ids. */

/* return GDK_SUCCEED if we can handle the upgrade from oldversion to
 * newversion */
static gdk_return
bl_preversion(sqlstore *store, int oldversion, int newversion)
{
	(void)newversion;

#ifdef CATALOG_NOV2019
	if (oldversion == CATALOG_NOV2019) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_JUN2020
	if (oldversion == CATALOG_JUN2020) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_JUN2020_MMT
	if (oldversion == CATALOG_JUN2020_MMT) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_OCT2020
	if (oldversion == CATALOG_OCT2020) {
		/* upgrade to default releases */
		store->catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

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

	return GDK_FAIL;
}

#if defined CATALOG_JUN2020 || defined CATALOG_OCT2020 || defined CATALOG_JUL2021
/* replace a column in a system table with a new column
 * colid is the SQL id for the column, oldcolid is the BAT id of the
 * to-be-replaced BAT */
static gdk_return
replace_bat(old_logger *old_lg, logger *lg, int colid, bat oldcolid, BAT *newcol)
{
	gdk_return rc;
	newcol = BATsetaccess(newcol, BAT_READ);
	if (old_lg != NULL) {
		if ((rc = BUNappend(old_lg->del, &oldcolid, false)) == GDK_SUCCEED &&
			(rc = BUNappend(old_lg->add, &newcol->batCacheid, false)) == GDK_SUCCEED &&
			(rc = BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &colid), &newcol->batCacheid, false)) == GDK_SUCCEED) {
			BBPretain(newcol->batCacheid);
			BBPretain(newcol->batCacheid);
		}
	} else {
		if ((rc = BAThash(lg->catalog_id)) == GDK_SUCCEED) {
			BATiter cii = bat_iterator_nolock(lg->catalog_id);
			BUN p;
			MT_rwlock_rdlock(&cii.b->thashlock);
			HASHloop_int(cii, cii.b->thash, p, &colid) {
				if (BUNfnd(lg->dcatalog, &(oid){(oid)p}) == BUN_NONE) {
					if (BUNappend(lg->dcatalog, &(oid){(oid)p}, false) != GDK_SUCCEED ||
						BUNreplace(lg->catalog_lid, (oid) p, &(lng){0}, false) != GDK_SUCCEED) {
						MT_rwlock_rdunlock(&cii.b->thashlock);
						return GDK_FAIL;
					}
					lg->deleted++;
					break;
				}
			}
			MT_rwlock_rdunlock(&cii.b->thashlock);
			if ((rc = BUNappend(lg->catalog_id, &colid, false)) == GDK_SUCCEED &&
				(rc = BUNappend(lg->catalog_bid, &newcol->batCacheid, false)) == GDK_SUCCEED &&
				(rc = BUNappend(lg->catalog_lid, &lng_nil, false)) == GDK_SUCCEED &&
				(rc = BUNappend(lg->catalog_cnt, &(lng){BATcount(newcol)}, false)) == GDK_SUCCEED) {
				BBPretain(newcol->batCacheid);
			}
			lg->cnt++;
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

#if defined CATALOG_JUN2020 || defined CATALOG_OCT2020 || defined CATALOG_JAN2022
static gdk_return
tabins(logger *lg, old_logger *old_lg, bool first, int tt, int nid, ...)
{
	va_list va;
	int cid;
	const void *cval;
	gdk_return rc;
	BAT *b;

	va_start(va, nid);
	while ((cid = va_arg(va, int)) != 0) {
		cval = va_arg(va, void *);
		if ((b = log_temp_descriptor(log_find_bat(lg, cid))) == NULL) {
			va_end(va);
			return GDK_FAIL;
		}
		if (first &&
			(old_lg == NULL || BUNfnd(old_lg->add, &b->batCacheid) == BUN_NONE)) {
			BAT *bn = COLcopy(b, b->ttype, true, PERSISTENT);
			if (bn == NULL) {
				va_end(va);
				bat_destroy(b);
				return GDK_FAIL;
			}
			if (replace_bat(old_lg, lg, cid, b->batCacheid, bn) != GDK_SUCCEED) {
				va_end(va);
				bat_destroy(b);
				bat_destroy(bn);
				return GDK_FAIL;
			}
			/* logical refs of b stay the same: it is moved from catalog_bid to del */
			bat_destroy(b);
			b = bn;
		}
		rc = BUNappend(b, cval, true);
		if (rc == GDK_SUCCEED && old_lg == NULL) {
			BATiter cii = bat_iterator_nolock(lg->catalog_id);
			BUN p;
			MT_rwlock_rdlock(&cii.b->thashlock);
			rc = GDK_FAIL;          /* the BUNreplace should get executed */
			HASHloop_int(cii, cii.b->thash, p, &cid) {
				if (BUNfnd(lg->dcatalog, &(oid){(oid)p}) == BUN_NONE) {
					rc = BUNreplace(lg->catalog_cnt, (oid) p, &(lng){BATcount(b)}, false);
					break;
				}
			}
			MT_rwlock_rdunlock(&cii.b->thashlock);
		}
		bat_destroy(b);
		if (rc != GDK_SUCCEED) {
			va_end(va);
			return rc;
		}
	}
	va_end(va);

	if (tt >= 0) {
		if ((b = COLnew(0, tt, 0, PERSISTENT)) == NULL)
			return GDK_FAIL;
		rc = log_bat_persists(lg, b, nid);
		bat_destroy(b);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return GDK_SUCCEED;
}
#endif

const struct table {
	const char *schema;
	const char *table;
	const char *column;
	const char *fullname;
	int newid;
	bool hasids;
} tables[] = {
	{
		.schema = "sys",
		.newid = 2000,
	},
	{
		.schema = "sys",
		.table = "schemas",
		.fullname = "D_sys_schemas",
		.newid = 2001,
	},
	{
		.schema = "sys",
		.table = "schemas",
		.column = "id",
		.fullname = "sys_schemas_id",
		.newid = 2002,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "schemas",
		.column = "name",
		.fullname = "sys_schemas_name",
		.newid = 2003,
	},
	{
		.schema = "sys",
		.table = "schemas",
		.column = "authorization",
		.fullname = "sys_schemas_authorization",
		.newid = 2004,
	},
	{
		.schema = "sys",
		.table = "schemas",
		.column = "owner",
		.fullname = "sys_schemas_owner",
		.newid = 2005,
	},
	{
		.schema = "sys",
		.table = "schemas",
		.column = "system",
		.fullname = "sys_schemas_system",
		.newid = 2006,
	},
	{
		.schema = "sys",
		.table = "types",
		.fullname = "D_sys_types",
		.newid = 2007,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "id",
		.fullname = "sys_types_id",
		.newid = 2008,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "systemname",
		.fullname = "sys_types_systemname",
		.newid = 2009,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "sqlname",
		.fullname = "sys_types_sqlname",
		.newid = 2010,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "digits",
		.fullname = "sys_types_digits",
		.newid = 2011,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "scale",
		.fullname = "sys_types_scale",
		.newid = 2012,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "radix",
		.fullname = "sys_types_radix",
		.newid = 2013,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "eclass",
		.fullname = "sys_types_eclass",
		.newid = 2014,
	},
	{
		.schema = "sys",
		.table = "types",
		.column = "schema_id",
		.fullname = "sys_types_schema_id",
		.newid = 2015,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "functions",
		.fullname = "D_sys_functions",
		.newid = 2016,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "id",
		.fullname = "sys_functions_id",
		.newid = 2017,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "name",
		.fullname = "sys_functions_name",
		.newid = 2018,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "func",
		.fullname = "sys_functions_func",
		.newid = 2019,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "mod",
		.fullname = "sys_functions_mod",
		.newid = 2020,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "language",
		.fullname = "sys_functions_language",
		.newid = 2021,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "type",
		.fullname = "sys_functions_type",
		.newid = 2022,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "side_effect",
		.fullname = "sys_functions_side_effect",
		.newid = 2023,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "varres",
		.fullname = "sys_functions_varres",
		.newid = 2024,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "vararg",
		.fullname = "sys_functions_vararg",
		.newid = 2025,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "schema_id",
		.fullname = "sys_functions_schema_id",
		.newid = 2026,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "system",
		.fullname = "sys_functions_system",
		.newid = 2027,
	},
	{
		.schema = "sys",
		.table = "functions",
		.column = "semantics",
		.fullname = "sys_functions_semantics",
		.newid = 2162,
	},
	{
		.schema = "sys",
		.table = "args",
		.fullname = "D_sys_args",
		.newid = 2028,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "id",
		.fullname = "sys_args_id",
		.newid = 2029,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "func_id",
		.fullname = "sys_args_func_id",
		.newid = 2030,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "name",
		.fullname = "sys_args_name",
		.newid = 2031,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "type",
		.fullname = "sys_args_type",
		.newid = 2032,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "type_digits",
		.fullname = "sys_args_type_digits",
		.newid = 2033,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "type_scale",
		.fullname = "sys_args_type_scale",
		.newid = 2034,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "inout",
		.fullname = "sys_args_inout",
		.newid = 2035,
	},
	{
		.schema = "sys",
		.table = "args",
		.column = "number",
		.fullname = "sys_args_number",
		.newid = 2036,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.fullname = "D_sys_sequences",
		.newid = 2037,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "id",
		.fullname = "sys_sequences_id",
		.newid = 2038,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "schema_id",
		.fullname = "sys_sequences_schema_id",
		.newid = 2039,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "name",
		.fullname = "sys_sequences_name",
		.newid = 2040,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "start",
		.fullname = "sys_sequences_start",
		.newid = 2041,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "minvalue",
		.fullname = "sys_sequences_minvalue",
		.newid = 2042,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "maxvalue",
		.fullname = "sys_sequences_maxvalue",
		.newid = 2043,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "increment",
		.fullname = "sys_sequences_increment",
		.newid = 2044,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "cacheinc",
		.fullname = "sys_sequences_cacheinc",
		.newid = 2045,
	},
	{
		.schema = "sys",
		.table = "sequences",
		.column = "cycle",
		.fullname = "sys_sequences_cycle",
		.newid = 2046,
	},
	{
		.schema = "sys",
		.table = "table_partitions",
		.fullname = "D_sys_table_partitions",
		.newid = 2047,
	},
	{
		.schema = "sys",
		.table = "table_partitions",
		.column = "id",
		.fullname = "sys_table_partitions_id",
		.newid = 2048,
	},
	{
		.schema = "sys",
		.table = "table_partitions",
		.column = "table_id",
		.fullname = "sys_table_partitions_table_id",
		.newid = 2049,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "table_partitions",
		.column = "column_id",
		.fullname = "sys_table_partitions_column_id",
		.newid = 2050,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "table_partitions",
		.column = "expression",
		.fullname = "sys_table_partitions_expression",
		.newid = 2051,
	},
	{
		.schema = "sys",
		.table = "table_partitions",
		.column = "type",
		.fullname = "sys_table_partitions_type",
		.newid = 2052,
	},
	{
		.schema = "sys",
		.table = "range_partitions",
		.fullname = "D_sys_range_partitions",
		.newid = 2053,
	},
	{
		.schema = "sys",
		.table = "range_partitions",
		.column = "table_id",
		.fullname = "sys_range_partitions_table_id",
		.newid = 2054,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "range_partitions",
		.column = "partition_id",
		.fullname = "sys_range_partitions_partition_id",
		.newid = 2055,
	},
	{
		.schema = "sys",
		.table = "range_partitions",
		.column = "minimum",
		.fullname = "sys_range_partitions_minimum",
		.newid = 2056,
	},
	{
		.schema = "sys",
		.table = "range_partitions",
		.column = "maximum",
		.fullname = "sys_range_partitions_maximum",
		.newid = 2057,
	},
	{
		.schema = "sys",
		.table = "range_partitions",
		.column = "with_nulls",
		.fullname = "sys_range_partitions_with_nulls",
		.newid = 2058,
	},
	{
		.schema = "sys",
		.table = "value_partitions",
		.fullname = "D_sys_value_partitions",
		.newid = 2059,
	},
	{
		.schema = "sys",
		.table = "value_partitions",
		.column = "table_id",
		.fullname = "sys_value_partitions_table_id",
		.newid = 2060,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "value_partitions",
		.column = "partition_id",
		.fullname = "sys_value_partitions_partition_id",
		.newid = 2061,
	},
	{
		.schema = "sys",
		.table = "value_partitions",
		.column = "value",
		.fullname = "sys_value_partitions_value",
		.newid = 2062,
	},
	{
		.schema = "sys",
		.table = "dependencies",
		.fullname = "D_sys_dependencies",
		.newid = 2063,
	},
	{
		.schema = "sys",
		.table = "dependencies",
		.column = "id",
		.fullname = "sys_dependencies_id",
		.newid = 2064,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "dependencies",
		.column = "depend_id",
		.fullname = "sys_dependencies_depend_id",
		.newid = 2065,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "dependencies",
		.column = "depend_type",
		.fullname = "sys_dependencies_depend_type",
		.newid = 2066,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.fullname = "D_sys__tables",
		.newid = 2067,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "id",
		.fullname = "sys__tables_id",
		.newid = 2068,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "name",
		.fullname = "sys__tables_name",
		.newid = 2069,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "schema_id",
		.fullname = "sys__tables_schema_id",
		.newid = 2070,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "query",
		.fullname = "sys__tables_query",
		.newid = 2071,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "type",
		.fullname = "sys__tables_type",
		.newid = 2072,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "system",
		.fullname = "sys__tables_system",
		.newid = 2073,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "commit_action",
		.fullname = "sys__tables_commit_action",
		.newid = 2074,
	},
	{
		.schema = "sys",
		.table = "_tables",
		.column = "access",
		.fullname = "sys__tables_access",
		.newid = 2075,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.fullname = "D_sys__columns",
		.newid = 2076,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "id",
		.fullname = "sys__columns_id",
		.newid = 2077,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "name",
		.fullname = "sys__columns_name",
		.newid = 2078,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "type",
		.fullname = "sys__columns_type",
		.newid = 2079,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "type_digits",
		.fullname = "sys__columns_type_digits",
		.newid = 2080,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "type_scale",
		.fullname = "sys__columns_type_scale",
		.newid = 2081,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "table_id",
		.fullname = "sys__columns_table_id",
		.newid = 2082,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "default",
		.fullname = "sys__columns_default",
		.newid = 2083,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "null",
		.fullname = "sys__columns_null",
		.newid = 2084,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "number",
		.fullname = "sys__columns_number",
		.newid = 2085,
	},
	{
		.schema = "sys",
		.table = "_columns",
		.column = "storage",
		.fullname = "sys__columns_storage",
		.newid = 2086,
	},
	{
		.schema = "sys",
		.table = "keys",
		.fullname = "D_sys_keys",
		.newid = 2087,
	},
	{
		.schema = "sys",
		.table = "keys",
		.column = "id",
		.fullname = "sys_keys_id",
		.newid = 2088,
	},
	{
		.schema = "sys",
		.table = "keys",
		.column = "table_id",
		.fullname = "sys_keys_table_id",
		.newid = 2089,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "keys",
		.column = "type",
		.fullname = "sys_keys_type",
		.newid = 2090,
	},
	{
		.schema = "sys",
		.table = "keys",
		.column = "name",
		.fullname = "sys_keys_name",
		.newid = 2091,
	},
	{
		.schema = "sys",
		.table = "keys",
		.column = "rkey",
		.fullname = "sys_keys_rkey",
		.newid = 2092,
	},
	{
		.schema = "sys",
		.table = "keys",
		.column = "action",
		.fullname = "sys_keys_action",
		.newid = 2093,
	},
	{
		.schema = "sys",
		.table = "idxs",
		.fullname = "D_sys_idxs",
		.newid = 2094,
	},
	{
		.schema = "sys",
		.table = "idxs",
		.column = "id",
		.fullname = "sys_idxs_id",
		.newid = 2095,
	},
	{
		.schema = "sys",
		.table = "idxs",
		.column = "table_id",
		.fullname = "sys_idxs_table_id",
		.newid = 2096,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "idxs",
		.column = "type",
		.fullname = "sys_idxs_type",
		.newid = 2097,
	},
	{
		.schema = "sys",
		.table = "idxs",
		.column = "name",
		.fullname = "sys_idxs_name",
		.newid = 2098,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.fullname = "D_sys_triggers",
		.newid = 2099,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "id",
		.fullname = "sys_triggers_id",
		.newid = 2100,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "name",
		.fullname = "sys_triggers_name",
		.newid = 2101,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "table_id",
		.fullname = "sys_triggers_table_id",
		.newid = 2102,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "time",
		.fullname = "sys_triggers_time",
		.newid = 2103,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "orientation",
		.fullname = "sys_triggers_orientation",
		.newid = 2104,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "event",
		.fullname = "sys_triggers_event",
		.newid = 2105,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "old_name",
		.fullname = "sys_triggers_old_name",
		.newid = 2106,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "new_name",
		.fullname = "sys_triggers_new_name",
		.newid = 2107,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "condition",
		.fullname = "sys_triggers_condition",
		.newid = 2108,
	},
	{
		.schema = "sys",
		.table = "triggers",
		.column = "statement",
		.fullname = "sys_triggers_statement",
		.newid = 2109,
	},
	{
		.schema = "sys",
		.table = "objects",
		.fullname = "D_sys_objects",
		.newid = 2110,
	},
	{
		.schema = "sys",
		.table = "objects",
		.column = "id",
		.fullname = "sys_objects_id",
		.newid = 2111,
	},
	{
		.schema = "sys",
		.table = "objects",
		.column = "name",
		.fullname = "sys_objects_name",
		.newid = 2112,
	},
	{
		.schema = "sys",
		.table = "objects",
		.column = "nr",
		.fullname = "sys_objects_nr",
		.newid = 2113,
		.hasids = true,
	},
	{
		.schema = "sys",
		.table = "objects",
		.column = "sub",
		.fullname = "sys_objects_sub",
		.newid = 2163,
		.hasids = true,
	},
	{
		.schema = "tmp",
		.newid = 2114,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.fullname = "D_tmp__tables",
		.newid = 2115,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "id",
		.fullname = "tmp__tables_id",
		.newid = 2116,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "name",
		.fullname = "tmp__tables_name",
		.newid = 2117,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "schema_id",
		.fullname = "tmp__tables_schema_id",
		.newid = 2118,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "query",
		.fullname = "tmp__tables_query",
		.newid = 2119,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "type",
		.fullname = "tmp__tables_type",
		.newid = 2120,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "system",
		.fullname = "tmp__tables_system",
		.newid = 2121,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "commit_action",
		.fullname = "tmp__tables_commit_action",
		.newid = 2122,
	},
	{
		.schema = "tmp",
		.table = "_tables",
		.column = "access",
		.fullname = "tmp__tables_access",
		.newid = 2123,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.fullname = "D_tmp__columns",
		.newid = 2124,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "id",
		.fullname = "tmp__columns_id",
		.newid = 2125,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "name",
		.fullname = "tmp__columns_name",
		.newid = 2126,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "type",
		.fullname = "tmp__columns_type",
		.newid = 2127,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "type_digits",
		.fullname = "tmp__columns_type_digits",
		.newid = 2128,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "type_scale",
		.fullname = "tmp__columns_type_scale",
		.newid = 2129,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "table_id",
		.fullname = "tmp__columns_table_id",
		.newid = 2130,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "default",
		.fullname = "tmp__columns_default",
		.newid = 2131,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "null",
		.fullname = "tmp__columns_null",
		.newid = 2132,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "number",
		.fullname = "tmp__columns_number",
		.newid = 2133,
	},
	{
		.schema = "tmp",
		.table = "_columns",
		.column = "storage",
		.fullname = "tmp__columns_storage",
		.newid = 2134,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.fullname = "D_tmp_keys",
		.newid = 2135,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.column = "id",
		.fullname = "tmp_keys_id",
		.newid = 2136,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.column = "table_id",
		.fullname = "tmp_keys_table_id",
		.newid = 2137,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.column = "type",
		.fullname = "tmp_keys_type",
		.newid = 2138,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.column = "name",
		.fullname = "tmp_keys_name",
		.newid = 2139,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.column = "rkey",
		.fullname = "tmp_keys_rkey",
		.newid = 2140,
	},
	{
		.schema = "tmp",
		.table = "keys",
		.column = "action",
		.fullname = "tmp_keys_action",
		.newid = 2141,
	},
	{
		.schema = "tmp",
		.table = "idxs",
		.fullname = "D_tmp_idxs",
		.newid = 2142,
	},
	{
		.schema = "tmp",
		.table = "idxs",
		.column = "id",
		.fullname = "tmp_idxs_id",
		.newid = 2143,
	},
	{
		.schema = "tmp",
		.table = "idxs",
		.column = "table_id",
		.fullname = "tmp_idxs_table_id",
		.newid = 2144,
	},
	{
		.schema = "tmp",
		.table = "idxs",
		.column = "type",
		.fullname = "tmp_idxs_type",
		.newid = 2145,
	},
	{
		.schema = "tmp",
		.table = "idxs",
		.column = "name",
		.fullname = "tmp_idxs_name",
		.newid = 2146,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.fullname = "D_tmp_triggers",
		.newid = 2147,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "id",
		.fullname = "tmp_triggers_id",
		.newid = 2148,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "name",
		.fullname = "tmp_triggers_name",
		.newid = 2149,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "table_id",
		.fullname = "tmp_triggers_table_id",
		.newid = 2150,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "time",
		.fullname = "tmp_triggers_time",
		.newid = 2151,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "orientation",
		.fullname = "tmp_triggers_orientation",
		.newid = 2152,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "event",
		.fullname = "tmp_triggers_event",
		.newid = 2153,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "old_name",
		.fullname = "tmp_triggers_old_name",
		.newid = 2154,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "new_name",
		.fullname = "tmp_triggers_new_name",
		.newid = 2155,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "condition",
		.fullname = "tmp_triggers_condition",
		.newid = 2156,
	},
	{
		.schema = "tmp",
		.table = "triggers",
		.column = "statement",
		.fullname = "tmp_triggers_statement",
		.newid = 2157,
	},
	{
		.schema = "tmp",
		.table = "objects",
		.fullname = "D_tmp_objects",
		.newid = 2158,
	},
	{
		.schema = "tmp",
		.table = "objects",
		.column = "id",
		.fullname = "tmp_objects_id",
		.newid = 2159,
	},
	{
		.schema = "tmp",
		.table = "objects",
		.column = "name",
		.fullname = "tmp_objects_name",
		.newid = 2160,
	},
	{
		.schema = "tmp",
		.table = "objects",
		.column = "nr",
		.fullname = "tmp_objects_nr",
		.newid = 2161,
	},
	{
		.schema = "tmp",
		.table = "objects",
		.column = "sub",
		.fullname = "tmp_objects_sub",
		.newid = 2164,
	},
	{0}
};

/* more system tables with schema/table/column ids that need to be remapped */
const struct mapids {
	// const char *schema;			/* always "sys" */
	const char *table;
	const char *column;
} mapids[] = {
	{
		.table = "comments",
		.column = "id",
	},
	{
		.table = "db_user_info",
		.column = "default_schema",
	},
	{
		.table = "privileges",
		.column = "obj_id",
	},
	{
		.table = "statistics",
		.column = "column_id",
	},
	{0}
};

static gdk_return
upgrade(old_logger *lg)
{
	gdk_return rc = GDK_FAIL;
	struct bats {
		BAT *nmbat;
		BAT *idbat;
		BAT *parbat;
		BAT *cands;
	} bats[3];
	BAT *mapold = COLnew(0, TYPE_int, 256, TRANSIENT);
	BAT *mapnew = COLnew(0, TYPE_int, 256, TRANSIENT);

	bats[0].nmbat = log_temp_descriptor(old_logger_find_bat(lg, "sys_schemas_name", 0, 0));
	bats[0].idbat = log_temp_descriptor(old_logger_find_bat(lg, "sys_schemas_id", 0, 0));
	bats[0].parbat = NULL;
	bats[0].cands = log_temp_descriptor(old_logger_find_bat(lg, "D_sys_schemas", 0, 0));
	bats[1].nmbat = log_temp_descriptor(old_logger_find_bat(lg, "sys__tables_name", 0, 0));
	bats[1].idbat = log_temp_descriptor(old_logger_find_bat(lg, "sys__tables_id", 0, 0));
	bats[1].parbat = log_temp_descriptor(old_logger_find_bat(lg, "sys__tables_schema_id", 0, 0));
	bats[1].cands = log_temp_descriptor(old_logger_find_bat(lg, "D_sys__tables", 0, 0));
	bats[2].nmbat = log_temp_descriptor(old_logger_find_bat(lg, "sys__columns_name", 0, 0));
	bats[2].idbat = log_temp_descriptor(old_logger_find_bat(lg, "sys__columns_id", 0, 0));
	bats[2].parbat = log_temp_descriptor(old_logger_find_bat(lg, "sys__columns_table_id", 0, 0));
	bats[2].cands = log_temp_descriptor(old_logger_find_bat(lg, "D_sys__columns", 0, 0));
	if (mapold == NULL || mapnew == NULL)
		goto bailout;
	for (int i = 0; i < 3; i++) {
		if (bats[i].nmbat == NULL || bats[i].idbat == NULL || bats[i].cands == NULL)
			goto bailout;
		if (i > 0 && bats[i].parbat == NULL)
			goto bailout;
		/* create a candidate list from the deleted rows bat */
		if (BATcount(bats[i].cands) == 0) {
			/* no deleted rows -> no candidate list */
			bat_destroy(bats[i].cands);
			bats[i].cands = NULL;
		} else {
			BAT *b;
			if ((rc = BATsort(&b, NULL, NULL, bats[i].cands, NULL, NULL, false, false, false)) != GDK_SUCCEED)
				goto bailout;
			rc = GDK_FAIL;
			bat_destroy(bats[i].cands);
			bats[i].cands = BATnegcands(BATcount(bats[i].nmbat), b);
			bat_destroy(b);
			if (bats[i].cands == NULL) {
				goto bailout;
			}
		}
	}

	/* figure out mapping from old IDs to new stable IDs, result in two
	 * aligned BATs, mapold and mapnew */
	int schid, tabid, parid;
	schid = tabid = parid = 0;	/* restrict search to parent object */
	for (int i = 0; tables[i].schema != NULL; i++) {
		int lookup;				/* which system table to look the name up in */
		const char *name;		/* the name to look up */
		if (tables[i].table == NULL) {
			/* it's a schema */
			name = tables[i].schema;
			lookup = 0;
			parid = 0;			/* no parent object */
		} else if (tables[i].column == NULL) {
			/* it's a table */
			name = tables[i].table;
			lookup = 1;
			parid = schid;		/* parent object is last schema */
		} else {
			/* it's a column */
			name = tables[i].column;
			lookup = 2;
			parid = tabid;		/* parent object is last table */
		}
		/* restrict search to non-deleted rows */
		BAT *cand = bats[lookup].cands;
		if (bats[lookup].parbat != NULL) {
			/* further restrict search to parent object */
			cand = BATselect(bats[lookup].parbat, cand, &parid, NULL, true, true, false);
			if (cand == NULL)
				goto bailout;
		}
		/* look for name, should be one (or maybe zero) result */
		BAT *b = BATselect(bats[lookup].nmbat, cand, name, NULL, true, true, false);
		if (cand != bats[lookup].cands)
			bat_destroy(cand);
		if (b == NULL)
			goto bailout;
		if (BATcount(b) > 0) {
			int oldid = ((int *) bats[lookup].idbat->theap->base)[BUNtoid(b, 0) - bats[lookup].nmbat->hseqbase];
			if (oldid != tables[i].newid &&
				((rc = BUNappend(mapold, &oldid, false)) != GDK_SUCCEED ||
				 (rc = BUNappend(mapnew, &tables[i].newid, false)) != GDK_SUCCEED)) {
				bat_destroy(b);
				goto bailout;
			}
			rc = GDK_FAIL;
			if (tables[i].table == NULL)
				schid = oldid;
			else if (tables[i].column == NULL)
				tabid = oldid;
		}
		bat_destroy(b);
	}

	if (BATcount(mapold) == 0) {
		/* skip unnecessary work if there is no need for mapping */
		bat_destroy(mapold);
		bat_destroy(mapnew);
		mapold = NULL;
		mapnew = NULL;
	}

	/* do the mapping in the system tables: all columns with the .hasids
	 * flag set may contain IDs that have to be mapped; also add all
	 * system tables to the new catalog bats and add the new ones to the
	 * lg->add bat and the old ones that were replaced to the lg->del bat */
	const char *delname;
	delname = NULL;
	int delidx;
	delidx = -1;
	for (int i = 0; tables[i].schema != NULL; i++) {
		if (tables[i].fullname == NULL) /* schema */
			continue;
		if (tables[i].column == NULL) { /* table */
			delname = tables[i].fullname;
			delidx = i;
			continue;
		}
		BAT *b = log_temp_descriptor(old_logger_find_bat(lg, tables[i].fullname, 0, 0));
		if (b == NULL)
			continue;
		if (delidx >= 0) {
			BAT *d = log_temp_descriptor(old_logger_find_bat(lg, delname, 0, 0));
			BAT *m = BATconstant(0, TYPE_msk, &(msk){false}, BATcount(b), PERSISTENT);
			if (m == NULL) {
				bat_destroy(d);
				bat_destroy(m);
				goto bailout;
			}
			if (d != NULL) {
				const oid *dels = (const oid *) Tloc(d, 0);
				for (BUN q = BATcount(d), p = 0; p < q; p++)
					mskSetVal(m, (BUN) dels[p], true);
				BBPretain(d->batCacheid);
			}
			if ((rc = BUNappend(lg->add, &m->batCacheid, false)) != GDK_SUCCEED ||
				(rc = BUNappend(lg->lg->catalog_bid, &m->batCacheid, false)) != GDK_SUCCEED ||
				(rc = BUNappend(lg->lg->catalog_id, &tables[delidx].newid, false)) != GDK_SUCCEED ||
				(d != NULL &&
				 (rc = BUNappend(lg->del, &d->batCacheid, false)) != GDK_SUCCEED)) {
				bat_destroy(d);
				bat_destroy(m);
				goto bailout;
			}
			rc = GDK_FAIL;
			BBPretain(m->batCacheid);
			BBPretain(m->batCacheid);
			bat_destroy(d);
			bat_destroy(m);
			delidx = -1;
		}
		if (tables[i].hasids && mapold) {
			BAT *b1, *b2;
			BAT *cands = log_temp_descriptor(old_logger_find_bat(lg, delname, 0, 0));
			if (cands) {
				if (BATcount(cands) == 0) {
					bat_destroy(cands);
					cands = NULL;
				} else {
					rc = BATsort(&b1, NULL, NULL, cands, NULL, NULL, false, false, false);
					bat_destroy(cands);
					if (rc != GDK_SUCCEED) {
						bat_destroy(b);
						goto bailout;
					}
					rc = GDK_FAIL;
					cands = BATnegcands(BATcount(b), b1);
					bat_destroy(b1);
					if (cands == NULL) {
						bat_destroy(b);
						goto bailout;
					}
				}
			}
			rc = BATjoin(&b1, &b2, b, mapold, cands, NULL, false, BATcount(mapold));
			bat_destroy(cands);
			if (rc != GDK_SUCCEED) {
				bat_destroy(b);
				goto bailout;
			}
			rc = GDK_FAIL;
			if (BATcount(b1) == 0) {
				bat_destroy(b1);
				bat_destroy(b2);
			} else {
				BAT *orig = b;
				b = COLcopy(orig, orig->ttype, true, PERSISTENT);
				if (b == NULL) {
					bat_destroy(orig);
					bat_destroy(b1);
					bat_destroy(b2);
					goto bailout;
				}
				BAT *b3;
				b3 = BATproject(b2, mapnew);
				bat_destroy(b2);
				if (b3 == NULL) {
					bat_destroy(b1);
					bat_destroy(orig);
					bat_destroy(b);
					goto bailout;
				}
				rc = BATreplace(b, b1, b3, false);
				bat_destroy(b1);
				bat_destroy(b3);
				if (rc != GDK_SUCCEED) {
					bat_destroy(orig);
					bat_destroy(b);
					goto bailout;
				}
				if ((rc = BUNappend(lg->del, &orig->batCacheid, false)) != GDK_SUCCEED ||
					(rc = BUNappend(lg->add, &b->batCacheid, false)) != GDK_SUCCEED) {
					bat_destroy(orig);
					bat_destroy(b);
					goto bailout;
				}
				rc = GDK_FAIL;
				BBPretain(orig->batCacheid);
				BBPretain(b->batCacheid);
				switch (tables[i].newid) {
				case 2002:		/* sys.schemas.id */
					bat_destroy(bats[0].idbat);
					bats[0].idbat = b;
					BBPfix(b->batCacheid);
					break;
				case 2068:		/* sys._tables.id */
					bat_destroy(bats[1].idbat);
					bats[1].idbat = b;
					BBPfix(b->batCacheid);
					break;
				case 2070:		/* sys._tables.schema_id */
					bat_destroy(bats[1].parbat);
					bats[1].parbat = b;
					BBPfix(b->batCacheid);
					break;
				case 2077:		/* sys._columns.id */
					bat_destroy(bats[2].idbat);
					bats[2].idbat = b;
					BBPfix(b->batCacheid);
					break;
				case 2082:		/* sys._columns.table_id */
					bat_destroy(bats[2].parbat);
					bats[2].parbat = b;
					BBPfix(b->batCacheid);
					break;
				}
				bat_destroy(orig);
			}
			/* now b contains the updated values for the column in tables[i] */
		}
		/* here, b is either the original, unchanged bat or the updated one */
		if ((rc = BUNappend(lg->lg->catalog_bid, &b->batCacheid, false)) != GDK_SUCCEED ||
			(rc = BUNappend(lg->lg->catalog_id, &tables[i].newid, false)) != GDK_SUCCEED) {
			bat_destroy(b);
			goto bailout;
		}
		rc = GDK_FAIL;
		BBPretain(b->batCacheid);
		bat_destroy(b);
	}

	/* add all extant non-system bats to the new catalog */
	BAT *cands, *b;
	if (BATcount(lg->dcatalog) == 0) {
		cands = NULL;
	} else {
		if ((rc = BATsort(&b, NULL, NULL, lg->dcatalog, NULL, NULL, false, false, false)) != GDK_SUCCEED)
			goto bailout;
		rc = GDK_FAIL;
		cands = BATnegcands(BATcount(lg->catalog_oid), b);
		bat_destroy(b);
		if (cands == NULL)
			goto bailout;
	}
	b = BATselect(lg->catalog_oid, cands, &(lng){0}, NULL, true, true, true);
	bat_destroy(cands);
	if (b == NULL)
		goto bailout;
	cands = b;
	b = BATconvert(lg->catalog_oid, cands, TYPE_int, 0, 0, 0);
	if (b == NULL) {
		bat_destroy(cands);
		goto bailout;
	}
	if ((rc = BATappend(lg->lg->catalog_id, b, NULL, false)) != GDK_SUCCEED ||
		(rc = BATappend(lg->lg->catalog_bid, lg->catalog_bid, cands, false)) != GDK_SUCCEED) {
		bat_destroy(cands);
		bat_destroy(b);
		goto bailout;
	}
	rc = GDK_FAIL;
	const int *bids;
	bids = (const int *) Tloc(lg->lg->catalog_bid, lg->lg->catalog_bid->batCount - BATcount(cands));
	for (BUN j = BATcount(cands), i = 0; i < j; i++)
		BBPretain(bids[i]);
	bat_destroy(cands);
	bat_destroy(b);

	/* convert deleted rows bats (catalog id equals table id) from list
	 * of deleted rows to mask of deleted rows */
	BAT *tabs;
	/* 2164 is the largest fixed id, so select anything larger */
	tabs = BATselect(lg->lg->catalog_id, NULL, &(int){2164}, &int_nil, false, true, false);
	if (tabs == NULL)
		goto bailout;
	BAT *b1;
	/* extract those rows that refer to a known table (in bats[1].idbat) */
	b1 = BATintersect(lg->lg->catalog_id, bats[1].idbat, tabs, bats[1].cands, false, false, BUN_NONE);
	bat_destroy(tabs);
	if (b1 == NULL)
		goto bailout;
	BAT *b3, *b4;
	/* find a column (any column) in each of the tables */
	if ((rc = BATsemijoin(&b3, &b4, lg->lg->catalog_id, bats[2].parbat, b1, bats[2].cands, false, false, BUN_NONE)) != GDK_SUCCEED) {
		bat_destroy(b1);
		goto bailout;
	}
	rc = GDK_FAIL;
	bat_destroy(b3);
	/* extract column id */
	b3 = BATproject(b4, bats[2].idbat);
	bat_destroy(b4);
	if (b3 == NULL) {
		bat_destroy(b1);
		goto bailout;
	}
	BAT *b2;
	rc = BATleftjoin(&b2, &b4, b3, lg->lg->catalog_id, NULL, NULL, false, BUN_NONE);
	bat_destroy(b3);
	if (rc != GDK_SUCCEED) {
		bat_destroy(b1);
		goto bailout;
	}
	bat_destroy(b2);
	struct canditer ci;
	canditer_init(&ci, lg->lg->catalog_bid, b1);
	const oid *cbids;
	bids = Tloc(lg->lg->catalog_bid, 0);
	cbids = Tloc(b4, 0);
	for (BUN i = 0; i < ci.ncand; i++) {
		bat cbid = bids[cbids[i]];
		b = temp_descriptor(cbid);
		if (b == NULL) {
			bat_destroy(b1);
			bat_destroy(b3);
			goto bailout;
		}
		BUN len;
		len = BATcount(b);
		bat_destroy(b);
		oid o;
		o = canditer_next(&ci);
		bat tbid;
		tbid = bids[o - lg->lg->catalog_bid->hseqbase];
		b = temp_descriptor(tbid);
		BAT *bn;
		bn = BATconstant(0, TYPE_msk, &(msk){false}, len, PERSISTENT);
		if (b == NULL || bn == NULL) {
			bat_destroy(b);
			bat_destroy(bn);
			bat_destroy(b1);
			bat_destroy(b3);
			goto bailout;
		}
		const oid *dels;
		dels = Tloc(b, 0);
		for (BUN q = BATcount(b), p = 0; p < q; p++) {
			mskSetVal(bn, (BUN) dels[p], true);
		}
		bat_destroy(b);
		if ((rc = BUNappend(lg->del, &tbid, false)) != GDK_SUCCEED ||
		    (rc = BUNappend(lg->add, &bn->batCacheid, false)) != GDK_SUCCEED ||
		    (rc = BUNreplace(lg->lg->catalog_bid, o, &bn->batCacheid, false)) != GDK_SUCCEED) {
			bat_destroy(bn);
			bat_destroy(b1);
			bat_destroy(b3);
			goto bailout;
		}
		rc = GDK_FAIL;
		/* moving tbid from lg->lg->catalog_bid to lg->del does not change
		 * lrefs of tbid (old location is overwritten by new table id) */
		BBPretain(bn->batCacheid);
		BBPretain(bn->batCacheid); /* yep, twice */
		bat_destroy(bn);
	}
	bat_destroy(b1);
	bat_destroy(b4);

	/* map schema/table/column ids in other system tables */
	if (mapold) {
		/* select tables in sys schema */
		b1 = BATselect(bats[1].parbat, bats[1].cands, &(int){2000}, NULL, true, true, false);
		if (b1 == NULL)
			goto bailout;
		bids = Tloc(lg->lg->catalog_bid, 0);
		for (int i = 0; mapids[i].column != NULL; i++) {
			/* row ids for table in sys schema */
			BAT *b2 = BATselect(bats[1].nmbat, b1, mapids[i].table, NULL, true, true, false);
			if (b2 == NULL) {
				bat_destroy(b1);
				goto bailout;
			}
			/* table ids for table */
			b3 = BATproject(b2, bats[1].idbat);
			bat_destroy(b2);
			if (b3 == NULL) {
				bat_destroy(b1);
				goto bailout;
			}
			/* row ids for columns of table */
			b2 = BATintersect(bats[2].parbat, b3, NULL, NULL, false, false, BUN_NONE);
			bat_destroy(b3);
			if (b2 == NULL) {
				bat_destroy(b1);
				goto bailout;
			}
			/* row id for the column in the table we're looking for */
			b3 = BATselect(bats[2].nmbat, b2, mapids[i].column, NULL, true, true, false);
			bat_destroy(b2);
			if (b3 == NULL) {
				bat_destroy(b1);
				goto bailout;
			}
			/* row ids in catalog for column in table */
			b2 = BATintersect(lg->lg->catalog_id, bats[2].idbat, NULL, b3, false, false, 1);
			bat_destroy(b3);
			if (b2 == NULL) {
				bat_destroy(b1);
				goto bailout;
			}
			for (BUN j = 0; j < BATcount(b2); j++) {
				oid p = BUNtoid(b2, j);
				b3 = BATdescriptor(bids[p]);
				if (b3 == NULL) {
					bat_destroy(b1);
					bat_destroy(b2);
					goto bailout;
				}
				BAT *b4, *b5;
				if ((rc = BATjoin(&b4, &b5, b3, mapold, NULL, NULL, false, BUN_NONE)) != GDK_SUCCEED) {
					bat_destroy(b1);
					bat_destroy(b2);
					bat_destroy(b3);
					goto bailout;
				}
				rc = GDK_FAIL;
				if (BATcount(b4) == 0) {
					bat_destroy(b3);
					bat_destroy(b4);
					bat_destroy(b5);
				} else {
					BAT *b6;
					b6 = COLcopy(b3, b3->ttype, true, PERSISTENT);
					bat_destroy(b3);
					b3 = BATproject(b5, mapnew);
					bat_destroy(b5);
					if (b3 == NULL || b6 == NULL) {
						bat_destroy(b1);
						bat_destroy(b2);
						bat_destroy(b3);
						bat_destroy(b4);
						bat_destroy(b6);
						goto bailout;
					}
					if ((rc = BATreplace(b6, b4, b3, false)) == GDK_SUCCEED &&
						(rc = BUNappend(lg->del, &bids[p], false)) == GDK_SUCCEED &&
						(rc = BUNappend(lg->add, &b6->batCacheid, false)) == GDK_SUCCEED)
						rc = BUNreplace(lg->lg->catalog_bid, p, &b6->batCacheid, false);
					BBPretain(b6->batCacheid);
					BBPretain(b6->batCacheid);
					bat_destroy(b3);
					bat_destroy(b4);
					bat_destroy(b6);
					if (rc != GDK_SUCCEED) {
						bat_destroy(b1);
						bat_destroy(b2);
						goto bailout;
					}
					rc = GDK_FAIL;
				}
			}
			bat_destroy(b2);
		}
		bat_destroy(b1);
	}

	/* add all bats that were added by processing the WAL and that have
	 * not been deleted since to the list of new bats */
	bids = (const int *) Tloc(lg->catalog_bid, 0);
	for (BUN p = lg->catalog_bid->batInserted, q = lg->catalog_bid->batCount;
		 p < q;
		 p++) {
		bat bid = bids[p];
		if (BUNfnd(lg->lg->catalog_bid, &(int){bid}) != BUN_NONE) {
			b = BATdescriptor(bid);
			if (b) {
				if (BATmode(b, false) != GDK_SUCCEED ||
					BUNappend(lg->add, &(int){bid}, false) != GDK_SUCCEED) {
					BBPunfix(bid);
					goto bailout;
				}
				BBPkeepref(b);
			}
		}
	}

	rc = GDK_SUCCEED;

  bailout:
	bat_destroy(mapold);
	bat_destroy(mapnew);
	for (int i = 0; i < 3; i++) {
		bat_destroy(bats[i].nmbat);
		bat_destroy(bats[i].idbat);
		bat_destroy(bats[i].parbat);
		bat_destroy(bats[i].cands);
	}
	return rc;
}

static gdk_return
bl_postversion(void *Store, void *Lg)
{
	sqlstore *store = Store;
	old_logger *old_lg;
	logger *lg;
	gdk_return rc;

	if (store->catalog_version < 52300) { /* the watershed */
		/* called from gdk_logger_old.c; Lg is the old logger */
		old_lg = Lg;
		if (upgrade(old_lg) != GDK_SUCCEED)
			return GDK_FAIL;
		lg = old_lg->lg;
	} else {
		/* called from gdk_logger.c; Lg is the new logger, there is no old */
		old_lg = NULL;
		lg = Lg;
	}
	bool tabins_first = true;

#ifdef CATALOG_NOV2019
	if (store->catalog_version <= CATALOG_NOV2019) {
		BAT *te, *tne;
		const int *ocl;	/* old eclass */
		int *ncl;	/* new eclass */

		te = log_temp_descriptor(log_find_bat(lg, 2014)); /* sys.types.eclass */
		if (te == NULL)
			return GDK_FAIL;
		tne = COLnew(te->hseqbase, TYPE_int, BATcount(te), PERSISTENT);
		if (tne == NULL) {
			bat_destroy(te);
			return GDK_FAIL;
		}
		ocl = Tloc(te, 0);
		ncl = Tloc(tne, 0);
		for (BUN p = 0, q = BATcount(te); p < q; p++) {
			switch (ocl[p]) {
			case EC_TIME_TZ:		/* old EC_DATE */
				ncl[p] = EC_DATE;
				break;
			case EC_DATE:			/* old EC_TIMESTAMP */
				ncl[p] = EC_TIMESTAMP;
				break;
			case EC_TIMESTAMP:		/* old EC_GEOM */
				ncl[p] = EC_GEOM;
				break;
			case EC_TIMESTAMP_TZ:	/* old EC_EXTERNAL */
				ncl[p] = EC_EXTERNAL;
				break;
			default:
				/* others stay unchanged */
				ncl[p] = ocl[p];
				break;
			}
		}
		BATsetcount(tne, BATcount(te));
		tne->tnil = false;
		tne->tnonil = true;
		tne->tsorted = false;
		tne->trevsorted = false;
		tne->tkey = false;
		if (BUNappend(old_lg->del, &te->batCacheid, false) != GDK_SUCCEED ||
			BUNappend(old_lg->add, &tne->batCacheid, false) != GDK_SUCCEED ||
			BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &(int){2014}), &tne->batCacheid, false) != GDK_SUCCEED) {
			bat_destroy(te);
			bat_destroy(tne);
			return GDK_FAIL;
		}
		BBPretain(tne->batCacheid);
		BBPretain(tne->batCacheid);
		bat_destroy(te);
		bat_destroy(tne);
	}
#endif

#ifdef CATALOG_JUN2020
	if (store->catalog_version <= CATALOG_JUN2020
#ifdef CATALOG_JUN2020_MMT
		|| store->catalog_version == CATALOG_JUN2020_MMT
#endif
		) {
		BAT *b;								 /* temp variable */
		{
			/* new BOOLEAN column sys.functions.semantics */
			b = log_temp_descriptor(log_find_bat(lg, 2017)); /* sys.functions.id */
			if (b == NULL)
				return GDK_FAIL;
			BAT *sem = BATconstant(b->hseqbase, TYPE_bit, &(bit){1}, BATcount(b), PERSISTENT);
			bat_destroy(b);
			if (sem == NULL)
				return GDK_FAIL;
			if ((sem = BATsetaccess(sem, BAT_READ)) == NULL ||
				/* 2162 is sys.functions.semantics */
				BUNappend(lg->catalog_id, &(int) {2162}, false) != GDK_SUCCEED ||
				BUNappend(lg->catalog_bid, &sem->batCacheid, false) != GDK_SUCCEED ||
				BUNappend(old_lg->add, &sem->batCacheid, false) != GDK_SUCCEED) {
				bat_destroy(sem);
				return GDK_FAIL;
			}
			BBPretain(sem->batCacheid);
			BBPretain(sem->batCacheid); /* yep, twice */
			bat_destroy(sem);

			if (tabins(lg, old_lg, tabins_first, -1, 0,
					   2076, &(msk) {false},	/* sys._columns */
					   /* 2162 is sys.functions.semantics */
					   2077, &(int) {2162},		/* sys._columns.id */
					   2078, "semantics",		/* sys._columns.name */
					   2079, "boolean",			/* sys._columns.type */
					   2080, &(int) {1},		/* sys._columns.type_digits */
					   2081, &(int) {0},		/* sys._columns.type_scale */
					   /* 2016 is sys.functions */
					   2082, &(int) {2016},		/* sys._columns.table_id */
					   2083, str_nil,			/* sys._columns.default */
					   2084, &(bit) {TRUE},		/* sys._columns.null */
					   2085, &(int) {11},		/* sys._columns.number */
					   2086, str_nil,			/* sys._columns.storage */
					   0) != GDK_SUCCEED)
				return GDK_FAIL;
			tabins_first = false;
		}

		/* sys.functions i.e. deleted rows */
		BAT *del_funcs = log_temp_descriptor(log_find_bat(lg, 2016));
		{
			/* move sql.degrees, sql.radians, sql.like and sql.ilike functions
			 * from 09_like.sql and 10_math.sql script to sql_types list */
			/* sys.functions.name */
			BAT *func_func = log_temp_descriptor(log_find_bat(lg, 2018));
			/* sys.functions.schema_id */
			BAT *func_schem = log_temp_descriptor(log_find_bat(lg, 2026));
			BAT *func_tid;
			BAT *cands;
			if (del_funcs == NULL || func_func == NULL || func_schem == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(func_func);
				bat_destroy(func_schem);
				return GDK_FAIL;
			}
			func_tid = BATmaskedcands(0, BATcount(del_funcs), del_funcs, false);
			if (func_tid == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(func_func);
				bat_destroy(func_schem);
				return GDK_FAIL;
			}
			/* select * from sys.functions where schema_id = 2000; */
			b = BATselect(func_schem, func_tid, &(int) {2000}, NULL, true, true, false);
			bat_destroy(func_schem);
			bat_destroy(func_tid);
			cands = b;
			if (cands == NULL) {
				bat_destroy(del_funcs);
				bat_destroy(func_func);
				return GDK_FAIL;
			}

			BAT *funcs;
			if ((funcs = COLnew(0, TYPE_str, 4, TRANSIENT)) == NULL ||
				BUNappend(funcs, "degrees", false) != GDK_SUCCEED ||
				BUNappend(funcs, "ilike", false) != GDK_SUCCEED ||
				BUNappend(funcs, "like", false) != GDK_SUCCEED ||
				BUNappend(funcs, "radians", false) != GDK_SUCCEED) {
				bat_destroy(funcs);
				bat_destroy(del_funcs);
				bat_destroy(func_func);
				return GDK_FAIL;
			}
			b = BATintersect(func_func, funcs, cands, NULL, false, false, 4);
			bat_destroy(func_func);
			bat_destroy(funcs);
			bat_destroy(cands);
			funcs = NULL;
			rc = GDK_FAIL;
			if (b != NULL &&
				(funcs = BATconstant(0, TYPE_msk, &(msk){true}, BATcount(b), TRANSIENT)) != NULL)
				rc = BATreplace(del_funcs, b, funcs, false);
			bat_destroy(b);
			bat_destroy(funcs);
			if (rc != GDK_SUCCEED)
				return rc;
		}

		{
			/* Fix SQL aggregation functions defined on the wrong modules:
			 * sql.null, sql.all, sql.zero_or_one and sql.not_unique */
			BAT *func_tid = BATmaskedcands(0, BATcount(del_funcs), del_funcs, false);
			/* sys.functions.mod */
			BAT *func_mod = log_temp_descriptor(log_find_bat(lg, 2020));
			bat_destroy(del_funcs);
			if (func_tid == NULL || func_mod == NULL) {
				bat_destroy(func_tid);
				bat_destroy(func_mod);
				return GDK_FAIL;
			}

			/* find the (undeleted) functions defined on "sql" module */
			BAT *sqlfunc = BATselect(func_mod, func_tid, "sql", NULL, true, true, false);
			bat_destroy(func_tid);
			if (sqlfunc == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}
			/* sys.functions.type */
			BAT *func_type = log_temp_descriptor(log_find_bat(lg, 2022));
			if (func_type == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlfunc);
				return GDK_FAIL;
			}
			/* and are aggregates (3) */
			BAT *sqlaggr_func = BATselect(func_type, sqlfunc, &(int) {3}, NULL, true, true, false);
			bat_destroy(sqlfunc);
			bat_destroy(func_type);
			if (sqlaggr_func == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}

			/* sys.functions.func */
			BAT *func_func = log_temp_descriptor(log_find_bat(lg, 2019));
			if (func_func == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				return GDK_FAIL;
			}
			b = COLcopy(func_mod, func_mod->ttype, true, PERSISTENT);
			if (b == NULL) {
				bat_destroy(func_func);
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				return GDK_FAIL;
			}
			if (BUNappend(old_lg->del, &func_mod->batCacheid, false) != GDK_SUCCEED ||
				BUNappend(old_lg->add, &b->batCacheid, false) != GDK_SUCCEED ||
				/* 2020 is sys.functions.mod */
				BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &(int){2020}), &b->batCacheid, false) != GDK_SUCCEED) {
				bat_destroy(func_func);
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				bat_destroy(b);
				return GDK_FAIL;
			}
			BBPretain(b->batCacheid);
			BBPretain(b->batCacheid);
			bat_destroy(func_mod);
			func_mod = b;
			BAT *aggrs;
			if ((aggrs = COLnew(0, TYPE_str, 4, TRANSIENT)) == NULL ||
				BUNappend(aggrs, "all", false) != GDK_SUCCEED ||
				BUNappend(aggrs, "no_unique", false) != GDK_SUCCEED ||
				BUNappend(aggrs, "null", false) != GDK_SUCCEED ||
				BUNappend(aggrs, "zero_or_one", false) != GDK_SUCCEED) {
				bat_destroy(aggrs);
				bat_destroy(func_func);
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				return GDK_FAIL;
			}
			b = BATintersect(func_func, aggrs, sqlaggr_func, NULL, false, false, 4);
			bat_destroy(func_func);
			bat_destroy(aggrs);
			bat_destroy(sqlaggr_func);
			aggrs = NULL;
			rc = GDK_FAIL;
			if (b != NULL &&
				(aggrs = BATconstant(0, TYPE_str, "aggr", BATcount(b), TRANSIENT)) != NULL)
				rc = BATreplace(func_mod, b, aggrs, false);
			bat_destroy(b);
			bat_destroy(aggrs);
			bat_destroy(func_mod);
			if (rc != GDK_SUCCEED)
				return rc;
		}
	}
#endif

#ifdef CATALOG_OCT2020
	if (store->catalog_version <= CATALOG_OCT2020) { /* not for Jun2020-mmt! */
		/* add sub column to "objects" table. This is required for merge tables */
		/* alter table sys.objects add column sub integer; */
		if (tabins(lg, old_lg, tabins_first, -1, 0,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2163 is sys.objects.sub */
				   2077, &(int) {2163},		/* sys._columns.id */
				   2078, "sub",				/* sys._columns.name */
				   2079, "int",				/* sys._columns.type */
				   2080, &(int) {32},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2110 is sys.objects */
				   2082, &(int) {2110},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {3},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;
		tabins_first = false;
		if (tabins(lg, old_lg, tabins_first, -1, 0,
				   2076, &(msk) {false},	/* sys._columns */
				   /* 2164 is tmp.objects.sub */
				   2077, &(int) {2164},		/* sys._columns.id */
				   2078, "sub",				/* sys._columns.name */
				   2079, "int",				/* sys._columns.type */
				   2080, &(int) {32},		/* sys._columns.type_digits */
				   2081, &(int) {0},		/* sys._columns.type_scale */
				   /* 2158 is tmp.objects */
				   2082, &(int) {2158},		/* sys._columns.table_id */
				   2083, str_nil,			/* sys._columns.default */
				   2084, &(bit) {TRUE},		/* sys._columns.null */
				   2085, &(int) {3},		/* sys._columns.number */
				   2086, str_nil,			/* sys._columns.storage */
				   0) != GDK_SUCCEED)
			return GDK_FAIL;

		/* create bat for new column sys.objects.sub with value NULL, then
		 * update sys.objects set sub = nr, nr = id where nr > 2000 */
		{
			BAT *objs_id = log_temp_descriptor(log_find_bat(lg, 2111)); /* sys.objects.id */
			BAT *objs_nr = log_temp_descriptor(log_find_bat(lg, 2113)); /* sys.objects.nr */
			BAT *objs_sub = BATconstant(objs_id->hseqbase, TYPE_int, &int_nil, BATcount(objs_id), PERSISTENT);
			BAT *b = log_temp_descriptor(log_find_bat(lg, 2110)); /* sys.objects */
			if (objs_id == NULL || objs_nr == NULL || objs_sub == NULL || b == NULL) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(b);
				bat_destroy(objs_sub);
				return GDK_FAIL;
			}
			BAT *cands = BATmaskedcands(0, BATcount(b), b, false);
			bat_destroy(b);
			if (cands == NULL) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				return GDK_FAIL;
			}
			b = BATselect(objs_nr, cands, &(int) {2000}, &int_nil, false, false, false);
			bat_destroy(cands);
			if (b == NULL) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				return GDK_FAIL;
			}
			cands = b;
			b = BATproject2(cands, objs_nr, NULL);
			if (b == NULL) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				return GDK_FAIL;
			}
			rc = BATreplace(objs_sub, cands, b, false);
			bat_destroy(b);
			if (rc != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				return GDK_FAIL;
			}
			b = COLcopy(objs_nr, objs_nr->ttype, true, PERSISTENT);
			rc = BUNappend(old_lg->del, &objs_nr->batCacheid, false);
			bat_destroy(objs_nr);
			if (b == NULL || rc != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				bat_destroy(b);
				return GDK_FAIL;
			}
			objs_nr = b;
			if (BUNappend(old_lg->add, &objs_nr->batCacheid, false) != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_sub);
				bat_destroy(objs_nr);
				bat_destroy(cands);
				return GDK_FAIL;
			}
			BBPretain(objs_nr->batCacheid);
			b = BATproject2(cands, objs_id, NULL);
			if (b == NULL) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				return GDK_FAIL;
			}
			rc = BATreplace(objs_nr, cands, b, false);
			bat_destroy(b);
			if (rc != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				return GDK_FAIL;
			}

			b = COLcopy(objs_id, objs_id->ttype, true, PERSISTENT);
			rc = BUNappend(old_lg->del, &objs_id->batCacheid, false);
			bat_destroy(objs_id);
			if (b == NULL || rc != GDK_SUCCEED) {
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				bat_destroy(b);
				return GDK_FAIL;
			}
			objs_id = b;
			if (BUNappend(old_lg->add, &objs_id->batCacheid, false) != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				return GDK_FAIL;
			}
			BBPretain(objs_id->batCacheid);

			BUN cnt = BATcount(cands), p;

			if (!(b = COLnew(objs_id->hseqbase, TYPE_int, cnt, TRANSIENT)) ||
				(p = BUNfnd(old_lg->seqs_id, &(int){OBJ_SID})) == BUN_NONE) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				bat_destroy(cands);
				bat_destroy(b);
				return GDK_FAIL;
			}

			int *bp = (int*)Tloc(b, 0), id = (int) *(lng *) Tloc(old_lg->seqs_val, p);
			for (BUN i = 0; i < cnt; i++)
				bp[i] = id++;
			BATsetcount(b, cnt);
			b->tsorted = b->trevsorted = b->tkey = b->tnonil = b->tnil = false;
			b->tnosorted = b->tnorevsorted = 0;
			lng lid = (lng) id;

			rc = BATreplace(objs_id, cands, b, false);
			bat_destroy(cands);
			bat_destroy(b);
			if (rc != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				return GDK_FAIL;
			}

			/* 2111 is sys.objects.id and 2113 is sys.objects.nr */
			if (BUNreplace(old_lg->seqs_val, BUNfnd(old_lg->seqs_id, &(int){OBJ_SID}), &lid, false) != GDK_SUCCEED ||
				BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &(int){2111}), &objs_id->batCacheid, false) != GDK_SUCCEED ||
				BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &(int){2113}), &objs_nr->batCacheid, false) != GDK_SUCCEED ||
				(objs_sub = BATsetaccess(objs_sub, BAT_READ)) == NULL ||
				(objs_id = BATsetaccess(objs_id, BAT_READ)) == NULL ||
				(objs_nr = BATsetaccess(objs_nr, BAT_READ)) == NULL ||
				/* 2163 is sys.objects.sub */
				BUNappend(lg->catalog_id, &(int) {2163}, false) != GDK_SUCCEED ||
				BUNappend(old_lg->add, &objs_sub->batCacheid, false) != GDK_SUCCEED ||
				BUNappend(lg->catalog_bid, &objs_sub->batCacheid, false) != GDK_SUCCEED) {
				bat_destroy(objs_id);
				bat_destroy(objs_nr);
				bat_destroy(objs_sub);
				return GDK_FAIL;
			}
			BBPretain(objs_sub->batCacheid);
			BBPretain(objs_sub->batCacheid);
			BBPretain(objs_nr->batCacheid);
			BBPretain(objs_id->batCacheid);
			bat_destroy(objs_id);
			bat_destroy(objs_nr);
			bat_destroy(objs_sub);

			/* alter table tmp.objects add column sub integer; */
			objs_sub = BATconstant(0, TYPE_int, &int_nil, 0, PERSISTENT);
			if (objs_sub == NULL) {
				return GDK_FAIL;
			}
			if ((objs_sub = BATsetaccess(objs_sub, BAT_READ)) == NULL ||
				/* 2164 is tmp.objects.sub */
				BUNappend(lg->catalog_id, &(int) {2164}, false) != GDK_SUCCEED ||
				BUNappend(old_lg->add, &objs_sub->batCacheid, false) != GDK_SUCCEED ||
				BUNappend(lg->catalog_bid, &objs_sub->batCacheid, false) != GDK_SUCCEED) {
				bat_destroy(objs_sub);
				return GDK_FAIL;
			}
			BBPretain(objs_sub->batCacheid);
			BBPretain(objs_sub->batCacheid);
			bat_destroy(objs_sub);
		}
	}
#endif
#ifdef CATALOG_OCT2020
	if (store->catalog_version <= CATALOG_OCT2020
#ifdef CATALOG_JUN2020_MMT
		|| store->catalog_version == CATALOG_JUN2020_MMT
#endif
		) {
		/* update sys.functions set mod = 'sql' where mod = 'user'; */
		{
			BAT *b1, *b2, *b3;
			b1 = log_temp_descriptor(log_find_bat(lg, 2016)); /* sys.functions */
			if (b1 == NULL)
				return GDK_FAIL;
			/* undeleted rows of sys.functions */
			b2 = BATmaskedcands(0, BATcount(b1), b1, false);
			b3 = log_temp_descriptor(log_find_bat(lg, 2020)); /* sys.functions.mod */
			bat_destroy(b1);
			if (b2 == NULL || b3 == NULL) {
				bat_destroy(b2);
				bat_destroy(b3);
				return GDK_FAIL;
			}
			/* mod = 'user' */
			b1 = BATselect(b3, b2, "user", NULL, true, true, false);
			bat_destroy(b2);
			if (b1 == NULL) {
				bat_destroy(b3);
				return GDK_FAIL;
			}
			if (BATcount(b1) > 0) {
				if (BUNfnd(old_lg->add, &b3->batCacheid) == BUN_NONE) {
					/* replace sys.functions.mod with a copy that we can modify */
					b2 = COLcopy(b3, b3->ttype, true, PERSISTENT);
					if (b2 == NULL) {
						bat_destroy(b1);
						bat_destroy(b3);
						return GDK_FAIL;
					}
					if (BUNappend(old_lg->del, &b3->batCacheid, false) != GDK_SUCCEED ||
						BUNappend(old_lg->add, &b2->batCacheid, false) != GDK_SUCCEED ||
						/* 2020 is sys.functions.mod */
						BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &(int){2020}), &b2->batCacheid, false) != GDK_SUCCEED) {
						bat_destroy(b1);
						bat_destroy(b3);
						bat_destroy(b2);
						return GDK_FAIL;
					}
					BBPretain(b2->batCacheid);
					BBPretain(b2->batCacheid);
					bat_destroy(b3);
					b3 = b2;
				}
				b2 = BATconstant(0, TYPE_str, "sql", BATcount(b1), TRANSIENT);
				if (b2 == NULL) {
					bat_destroy(b1);
					bat_destroy(b3);
					return GDK_FAIL;
				}
				rc = BATreplace(b3, b1, b2, false);
				bat_destroy(b2);
				bat_destroy(b3);
				if (rc != GDK_SUCCEED) {
					bat_destroy(b1);
					return rc;
				}
			}
			bat_destroy(b1);
		}

		/* update sys.args set type = 'clob' where type = 'char' and type_digits = 0 and func_id > 2000 */
		{
			BAT *b1, *b2, *b3;
			b1 = log_temp_descriptor(log_find_bat(lg, 2028)); /* sys.args */
			if (b1 == NULL)
				return GDK_FAIL;
			b2 = BATmaskedcands(0, BATcount(b1), b1, false);
			bat_destroy(b1);
			b3 = log_temp_descriptor(log_find_bat(lg, 2030)); /* sys.args.func_id */
			if (b2 == NULL || b3 == NULL) {
				bat_destroy(b2);
				bat_destroy(b3);
				return GDK_FAIL;
			}
			/* func_id > 2000 */
			b1 = BATselect(b3, b2, &(int){2000}, &int_nil, false, false, false);
			bat_destroy(b2);
			bat_destroy(b3);
			b3 = log_temp_descriptor(log_find_bat(lg, 2033)); /* sys.args.type_digits */
			if (b1 == NULL || b3 == NULL) {
				bat_destroy(b1);
				bat_destroy(b3);
				return GDK_FAIL;
			}
			/* and type_digits = 0 */
			b2 = BATselect(b3, b1, &(int){0}, NULL, true, false, false);
			bat_destroy(b3);
			bat_destroy(b1);
			b1 = log_temp_descriptor(log_find_bat(lg, 2032)); /* sys.args.type */
			if (b1 == NULL || b2 == NULL) {
				bat_destroy(b1);
				bat_destroy(b2);
				return GDK_FAIL;
			}
			/* and type = 'char' */
			b3 = BATselect(b1, b2, "char", NULL, true, false, false);
			bat_destroy(b2);
			if (BATcount(b3) > 0) {
				if (BUNfnd(old_lg->add, &b1->batCacheid) == BUN_NONE) {
					/* replace sys.args.type with a copy that we can modify */
					b2 = COLcopy(b1, b1->ttype, true, PERSISTENT);
					if (b2 == NULL ||
						BUNappend(old_lg->del, &b1->batCacheid, false) != GDK_SUCCEED ||
						BUNappend(old_lg->add, &b2->batCacheid, false) != GDK_SUCCEED ||
						/* 2032 is sys.args.type */
						BUNreplace(lg->catalog_bid, BUNfnd(lg->catalog_id, &(int){2032}), &b2->batCacheid, false) != GDK_SUCCEED) {
						bat_destroy(b1);
						bat_destroy(b2);
						bat_destroy(b3);
						return GDK_FAIL;
					}
					BBPretain(b2->batCacheid);
					BBPretain(b2->batCacheid);
					bat_destroy(b1);
					b1 = b2;
				}
				b2 = BATconstant(0, TYPE_str, "clob", BATcount(b3), TRANSIENT);
				if (b2 == NULL ||
					/* do the update */
					BATreplace(b1, b3, b2, false) != GDK_SUCCEED) {
					bat_destroy(b1);
					bat_destroy(b2);
					bat_destroy(b3);
					return GDK_FAIL;
				}
				bat_destroy(b2);
			}
			bat_destroy(b1);
			bat_destroy(b3);
		}

		{
			/* drop STREAM TABLEs
			 * these tables don't actually have a disk presence, only
			 * one in the catalog (so that's why we drop them and don't
			 * convert them); we drop them by marking the relevant rows
			 * in various catalog tables as deleted */
			BAT *dt = log_temp_descriptor(log_find_bat(lg, 2067)); /* sys._tables */
			BAT *tt = log_temp_descriptor(log_find_bat(lg, 2072)); /* sys._tables.type */
			if (dt == NULL || tt == NULL) {
				bat_destroy(dt);
				bat_destroy(tt);
				return GDK_FAIL;
			}
			BAT *cands = BATmaskedcands(0, BATcount(dt), dt, false);
			if (cands == NULL) {
				bat_destroy(dt);
				bat_destroy(tt);
				return GDK_FAIL;
			}
			BAT *strm = BATselect(tt, cands, &(sht){4}, NULL, true, true, false);
			bat_destroy(cands);
			bat_destroy(tt);
			if (strm == NULL) {
				bat_destroy(dt);
				return GDK_FAIL;
			}
			if (strm->batCount > 0) {
				for (BUN p = 0; p < strm->batCount; p++)
					mskSetVal(dt, BUNtoid(strm, p), true);
				bat_destroy(dt);
				BAT *ids = COLnew(0, TYPE_int, 0, TRANSIENT);
				BAT *ti = log_temp_descriptor(log_find_bat(lg, 2068)); /* sys._tables.id */
				if (ids == NULL || ti == NULL) {
					bat_destroy(ids);
					bat_destroy(ti);
					bat_destroy(strm);
					return GDK_FAIL;
				}
				struct {
					int id, tabid, dels; /* id, table_id and deleted rows */
				} foreign[10] = {
					/* the first 7 entries are references to the table id
					 * the first column non-zero means the ids get collected */
					{   0, 2049, 2047}, /* sys.table_partitions.table_id */
					{   0, 2054, 2053}, /* sys.range_partitions.table_id */
					{   0, 2060, 2059}, /* sys.value_partitions.table_id */
					{2077, 2082, 2076}, /* sys._columns.table_id */
					{2088, 2089, 2087}, /* sys.keys.table_id */
					{2095, 2096, 2094}, /* sys.idxs.table_id */
					{2100, 2102, 2099}, /* sys.triggers.table_id */

					/* the remaining 3 are references to collected object ids */
					{   0, 2111, 2110}, /* sys.objects.id */
					{   0, 2064, 2063}, /* sys.dependencies.id */
					{   0, 2065, 2063}, /* sys.dependencies.depend_id */
				};

				for (int i = 0; i < 10; i++) {
					if (i == 7) {
						/* change gear: we now need to delete the
						 * collected ids from sys.objects */
						bat_destroy(ti);
						ti = ids;
						ids = NULL;
						bat_destroy(strm);
						strm = NULL;
					}
					BAT *ct = log_temp_descriptor(log_find_bat(lg, foreign[i].tabid));
					BAT *dc = log_temp_descriptor(log_find_bat(lg, foreign[i].dels));
					if (ct == NULL || dc == NULL) {
						bat_destroy(ids);
						bat_destroy(strm);
						bat_destroy(ti);
						bat_destroy(ct);
						bat_destroy(dc);
						return GDK_FAIL;
					}
					cands = BATmaskedcands(0, BATcount(dc), dc, false);
					if (cands == NULL) {
						bat_destroy(ids);
						bat_destroy(strm);
						bat_destroy(ti);
						bat_destroy(ct);
						bat_destroy(dc);
						return GDK_FAIL;
					}
					BAT *strc = BATintersect(ct, ti, cands, strm, false, false, BUN_NONE);
					bat_destroy(cands);
					if (strc == NULL) {
						bat_destroy(ids);
						bat_destroy(strm);
						bat_destroy(ti);
						bat_destroy(ct);
						bat_destroy(dc);
						return GDK_FAIL;
					}
					for (BUN p = 0; p < strc->batCount; p++)
						mskSetVal(dc, BUNtoid(strc, p), true);
					if (foreign[i].id != 0) {
						BAT *ci = log_temp_descriptor(log_find_bat(lg, foreign[i].id));
						if (ci == NULL) {
							bat_destroy(ids);
							bat_destroy(strc);
							bat_destroy(ct);
							bat_destroy(dc);
							bat_destroy(strm);
							bat_destroy(ti);
							return GDK_FAIL;
						}
						if (BATappend(ids, ci, strc, false) != GDK_SUCCEED) {
							bat_destroy(ids);
							bat_destroy(strc);
							bat_destroy(ct);
							bat_destroy(dc);
							bat_destroy(strm);
							bat_destroy(ti);
							bat_destroy(ci);
							return GDK_FAIL;
						}
						bat_destroy(ci);
					}
					bat_destroy(strc);
					bat_destroy(ct);
					bat_destroy(dc);
				}
				bat_destroy(ti);
			} else {
				bat_destroy(dt);
				bat_destroy(strm);
			}
		}
	}
#endif

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
		BAT *cands = BATselect(func_schem, func_tid, &(int) {2000}, NULL, true, true, false);
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
		b = BATselect(func_lang, cands, &(int) {FUNC_LANG_SQL}, NULL, true, true, false);
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
		bat bid = func_lang->batCacheid;
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
		cands = BATselect(func_lang, func_tid, &(int){FUNC_LANG_SQL}, NULL, true, true, false);
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
				e > f && isspace(e[-1]) && isspace(e[8]) && strncmp(e + 9, "name", 4) == 0 && isspace(e[13]) &&
				BUNreplace(b2, o, &(int){FUNC_LANG_MAL}, false) != GDK_SUCCEED) {
				bat_destroy(b2);
				bat_destroy(func_func);
				return GDK_FAIL;
			}
		}
		rc = replace_bat(old_lg, lg, 2021, bid, b2);
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
		BAT *cands = BATselect(func_schem, func_tid, &(int) {2000}, NULL, true, true, false);
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
		bat bid = func_se->batCacheid;
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
		BAT *b2 = BATselect(func_name, cands, "env", NULL, true, true, false);
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
		b2 = BATselect(func_name, cands, "db_users", NULL, true, true, false);
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
			BUNappend(funcs, "importTable", false) != GDK_SUCCEED ||
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
		rc = replace_bat(old_lg, lg, 2023, bid, func_se);
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
		BAT *inczero = BATselect(seq_inc, seq_tid, &(lng){0}, NULL, false, true, false);
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
			rc = replace_bat(old_lg, lg, 2044, seq_inc->batCacheid, b2);
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
		BAT *incpos = BATselect(seq_inc, seq_tid, &(lng){0}, &lng_nil, false, true, false);
		bat_destroy(seq_inc);
		if (incpos == NULL) {
			bat_destroy(seq_tid);
			bat_destroy(seq_min);
			bat_destroy(seq_max);
			return GDK_FAIL;
		}
		/* select * from sys.sequences where increment > 0 and maxvalue = 0 */
		BAT *cands = BATselect(seq_max, incpos, &(lng) {0}, NULL, true, true, false);
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
				rc = replace_bat(old_lg, lg, 2043, seq_max->batCacheid, b2);
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
		BAT *incneg = BATselect(seq_inc, seq_tid, &lng_nil, &(lng){0}, false, true, false);
		bat_destroy(seq_tid);
		/* select * from sys.sequences where increment < 0 and minvalue = 0 */
		cands = BATselect(seq_min, incneg, &(lng) {0}, NULL, true, true, false);
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
				rc = replace_bat(old_lg, lg, 2042, seq_min->batCacheid, b2);
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
		BAT *cands = BATselect(b, del_tabs, &(int) {2000}, NULL, true, true, false);
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
		b = BATselect(tabnme, cands, "db_user_info", NULL, true, true, false);
		if (b == NULL) {
			bat_destroy(cands);
			bat_destroy(tabnme);
			bat_destroy(del_cols);
			return GDK_FAIL;
		}
		oid dbpos = BUNtoid(b, 0);
		bat_destroy(b);
		b = BATselect(tabnme, cands, "privileges", NULL, true, true, false);
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
		rc = tabins(lg, old_lg, true, -1, 0,
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
static gdk_return __attribute__((__warn_unused_result__))
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
static gdk_return __attribute__((__warn_unused_result__))
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
static gdk_return __attribute__((__warn_unused_result__))
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

		len = snprintf(log_file, sizeof(log_file), "%s/%s%s." LLFMT, db_dir, bat_logger->dir, LOGFILE, id);
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

static gdk_return __attribute__((__warn_unused_result__))
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
	len = snprintf(path1, FILENAME_MAX, "%s/%s/%o.%s", db_dir, BAKDIR, (int) batid, suffix);
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
	len = snprintf(path2, FILENAME_MAX, "%s/%s/%s.%s", db_dir, BATDIR, filename, suffix);
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

/* Add plan entries for all persistent BATs by looping over the BBP.dir.
 * Also include the BBP.dir itself.
 */
static gdk_return __attribute__((__warn_unused_result__))
snapshot_bats(stream *plan, const char *db_dir)
{
	char bbpdir[FILENAME_MAX];
	FILE *fp = NULL;
	int len;
	gdk_return ret = GDK_FAIL;
	int lineno = 0;
	bat bbpsize = 0;
	lng logno, transid;
	unsigned bbpversion;

	len = snprintf(bbpdir, FILENAME_MAX, "%s/%s/%s", db_dir, BAKDIR, "BBP.dir");
	if (len == -1 || len >= FILENAME_MAX) {
		GDKerror("Could not open %s, filename is too large", bbpdir);
		return GDK_FAIL;
	}
	ret = snapshot_immediate_copy_file(plan, bbpdir, bbpdir + strlen(db_dir) + 1);
	if (ret != GDK_SUCCEED)
		return ret;

	// Open the catalog and parse the header
	fp = fopen(bbpdir, "r");
	if (fp == NULL) {
		GDKerror("Could not open %s for reading: %s", bbpdir, mnstr_peek_error(NULL));
		return GDK_FAIL;
	}
	bbpversion = BBPheader(fp, &lineno, &bbpsize, &logno, &transid);
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
			fclose(fp);
			return GDK_SUCCEED;
		case 1:
			/* successfully read an entry */
			break;
		default:
			/* error */
			fclose(fp);
			return GDK_FAIL;
		}
#ifdef GDKLIBRARY_HASHASH
		assert(hashash == 0);
#endif
		if (ATOMvarsized(b.ttype)) {
			ret = snapshot_heap(plan, db_dir, b.batCacheid, filename, "theap", b.tvheap->free);
			if (ret != GDK_SUCCEED)
				goto end;
		}
		ret = snapshot_heap(plan, db_dir, b.batCacheid, filename, BATtailname(&b), b.theap->free);
		if (ret != GDK_SUCCEED)
			goto end;
	}

end:
	if (fp) {
		fclose(fp);
	}
	return ret;
}

/* Add a file to the plan which records the current wlc status, if any.
 * In particular, `wlc_batches`.
 *
 * With this information, a replica initialized from this snapshot can
 * be configured to catch up with its master by replaying later transactions.
 */
static gdk_return __attribute__((__warn_unused_result__))
snapshot_wlc(stream *plan, const char *db_dir)
{
	const char name[] = "wlr.config.in";
	char buf[1024];
	int len;

	(void)db_dir;

	if (wlc_state != WLC_RUN)
		return GDK_SUCCEED;

	len = snprintf(buf, sizeof(buf),
		"beat=%d\n"
		"batches=%d\n"
		, wlc_beat, wlc_batches
	);

	if (mnstr_printf(plan, "w %d %s\n", len, name) < 0 ||
		mnstr_write(plan, buf, 1, len) < 0) {
		GDKerror("%s", mnstr_peek_error(plan));
		return GDK_FAIL;
	}

	return GDK_SUCCEED;
}

static gdk_return __attribute__((__warn_unused_result__))
snapshot_vaultkey(stream *plan, const char *db_dir)
{
	char path[FILENAME_MAX];
	struct stat statbuf;

	int len = snprintf(path, FILENAME_MAX, "%s/.vaultkey", db_dir);
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
bl_snapshot(sqlstore *store, stream *plan)
{
	logger *bat_logger = store->logger;
	gdk_return ret;
	char *db_dir = NULL;
	size_t db_dir_len;

	// Farm 0 is always the persistent farm.
	db_dir = GDKfilepath(0, NULL, "", NULL);
	if (db_dir == NULL)
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

	ret = snapshot_bats(plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_wal(bat_logger, plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = snapshot_wlc(plan, db_dir);
	if (ret != GDK_SUCCEED)
		goto end;

	ret = GDK_SUCCEED;
end:
	GDKfree(db_dir);
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
