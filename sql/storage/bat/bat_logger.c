/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */
#include "wlc.h"
#include "gdk_logger_internals.h"
#include "mutils.h"

#define CATALOG_JUN2020 52204	/* first in Jun2020 */
#define CATALOG_OCT2020 52205	/* first in Oct2020 */

/* Note, CATALOG version 52300 is the first one where the basic system
 * tables (the ones created in store.c) have fixed and unchangeable
 * ids. */

/* return GDK_SUCCEED if we can handle the upgrade from oldversion to
 * newversion */
static gdk_return
bl_preversion(sqlstore *store, int oldversion, int newversion)
{
	(void)newversion;

/* disable upgrades for now */
	if (store->catalog_version < 52300)
		return GDK_FAIL;

#ifdef CATALOG_JUN2020
	if (oldversion == CATALOG_JUN2020) {
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

	return GDK_FAIL;
}

#define N(schema, table, column)	schema "_" table "_" column

#define D(schema, table)	"D_" schema "_" table

#if defined CATALOG_JUN2020 || defined CATALOG_OCT2020
static int
old_find_table_id(old_logger *lg, const char *val, int *sid)
{
	BAT *s = NULL;
	BAT *b, *t;
	BATiter bi;
	oid o;
	int id;

	b = temp_descriptor(old_logger_find_bat(lg, N("sys", "schemas", "name"), 0, 0));
	if (b == NULL)
		return 0;
	s = BATselect(b, NULL, "sys", NULL, 1, 1, 0);
	bat_destroy(b);
	if (s == NULL)
		return 0;
	if (BATcount(s) == 0) {
		bat_destroy(s);
		return 0;
	}
	o = BUNtoid(s, 0);
	bat_destroy(s);
	b = temp_descriptor(old_logger_find_bat(lg, N("sys", "schemas", "id"), 0, 0));
	if (b == NULL)
		return 0;
	bi = bat_iterator(b);
	id = * (const int *) BUNtloc(bi, o - b->hseqbase);
	bat_destroy(b);
	/* store id of schema "sys" */
	*sid = id;

	b = temp_descriptor(old_logger_find_bat(lg, N("sys", "_tables", "name"), 0, 0));
	if (b == NULL) {
		bat_destroy(s);
		return 0;
	}
	s = BATselect(b, NULL, val, NULL, 1, 1, 0);
	bat_destroy(b);
	if (s == NULL)
		return 0;
	if (BATcount(s) == 0) {
		bat_destroy(s);
		return 0;
	}
	b = temp_descriptor(old_logger_find_bat(lg, N("sys", "_tables", "schema_id"), 0, 0));
	if (b == NULL) {
		bat_destroy(s);
		return 0;
	}
	t = BATselect(b, s, &id, NULL, 1, 1, 0);
	bat_destroy(b);
	bat_destroy(s);
	s = t;
	if (s == NULL)
		return 0;
	if (BATcount(s) == 0) {
		bat_destroy(s);
		return 0;
	}

	o = BUNtoid(s, 0);
	bat_destroy(s);

	b = temp_descriptor(old_logger_find_bat(lg, N("sys", "_tables", "id"), 0, 0));
	if (b == NULL)
		return 0;
	bi = bat_iterator(b);
	id = * (const int *) BUNtloc(bi, o - b->hseqbase);
	bat_destroy(b);
	return id;
}

static gdk_return
tabins(void *lg, bool first, int tt, const char *nname, const char *sname, const char *tname, ...)
{
	va_list va;
	char lname[64];
	const char *cname;
	const void *cval;
	gdk_return rc;
	BAT *b;
	int len;

	va_start(va, tname);
	while ((cname = va_arg(va, char *)) != NULL) {
		cval = va_arg(va, void *);
		len = snprintf(lname, sizeof(lname), "%s_%s_%s", sname, tname, cname);
		if (len == -1 || (size_t)len >= sizeof(lname) ||
			(b = temp_descriptor(old_logger_find_bat(lg, lname, 0, 0))) == NULL) {
			va_end(va);
			return GDK_FAIL;
		}
		if (first) {
			BAT *bn;
			if ((bn = COLcopy(b, b->ttype, true, PERSISTENT)) == NULL) {
				bat_destroy(b);
				va_end(va);
				return GDK_FAIL;
			}
			bat_destroy(b);
			if (BATsetaccess(bn, BAT_READ) != GDK_SUCCEED ||
			    old_logger_add_bat(lg, bn, lname, 0, 0) != GDK_SUCCEED) {
				bat_destroy(bn);
				va_end(va);
				return GDK_FAIL;
			}
			b = bn;
		}
		rc = BUNappend(b, cval, true);
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
		if ((rc = BATsetaccess(b, BAT_READ)) == GDK_SUCCEED)
			rc = old_logger_add_bat(lg, b, nname, 0, 0);
		bat_destroy(b);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return GDK_SUCCEED;
}
#endif

struct table {
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
	},
	{
		.schema = "sys",
		.table = "objects",
		.column = "sub",
		.fullname = "sys_objects_sub",
		.newid = 2163,
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
	BAT *catalog_bid = NULL;
	BAT *catalog_id = NULL;
	BAT *dcatalog = NULL;
	int schid = 0;
	int tabid = 0;
	int parid = 0;

	bats[0].nmbat = temp_descriptor(old_logger_find_bat(lg, "sys_schemas_name", 0, 0));
	bats[0].idbat = temp_descriptor(old_logger_find_bat(lg, "sys_schemas_id", 0, 0));
	bats[0].parbat = NULL;
	bats[0].cands = temp_descriptor(old_logger_find_bat(lg, "D_sys_schemas", 0, 0));
	bats[1].nmbat = temp_descriptor(old_logger_find_bat(lg, "sys__tables_name", 0, 0));
	bats[1].idbat = temp_descriptor(old_logger_find_bat(lg, "sys__tables_id", 0, 0));
	bats[1].parbat = temp_descriptor(old_logger_find_bat(lg, "sys__tables_schema_id", 0, 0));
	bats[1].cands = temp_descriptor(old_logger_find_bat(lg, "D_sys__tables", 0, 0));
	bats[2].nmbat = temp_descriptor(old_logger_find_bat(lg, "sys__columns_name", 0, 0));
	bats[2].idbat = temp_descriptor(old_logger_find_bat(lg, "sys__columns_id", 0, 0));
	bats[2].parbat = temp_descriptor(old_logger_find_bat(lg, "sys__columns_table_id", 0, 0));
	bats[2].cands = temp_descriptor(old_logger_find_bat(lg, "D_sys__columns", 0, 0));
	if (mapold == NULL || mapnew == NULL)
		goto bailout;
	for (int i = 0; i < 3; i++) {
		if (bats[i].nmbat == NULL || bats[i].idbat == NULL || bats[i].cands == NULL)
			goto bailout;
		if (i > 0 && bats[i].parbat == NULL)
			goto bailout;
		if (BATcount(bats[i].cands) == 0) {
			bat_destroy(bats[i].cands);
			bats[i].cands = NULL;
		} else {
			BAT *b;
			if (BATsort(&b, NULL, NULL, bats[i].cands, NULL, NULL, false, false, false) != GDK_SUCCEED)
				goto bailout;
			bat_destroy(bats[i].cands);
			bats[i].cands = BATnegcands(BATcount(bats[i].nmbat), b);
			bat_destroy(b);
			if (bats[i].cands == NULL)
				goto bailout;
		}
	}

	for (int i = 0; tables[i].schema != NULL; i++) {
		int lookup;
		const char *name;
		if (tables[i].table == NULL) {
			/* it's a schema */
			name = tables[i].schema;
			lookup = 0;
			parid = 0;
		} else if (tables[i].column == NULL) {
			/* it's a table */
			name = tables[i].table;
			lookup = 1;
			parid = schid;
		} else {
			/* it's a column */
			name = tables[i].column;
			lookup = 2;
			parid = tabid;
		}
		BAT *cand = bats[lookup].cands;
		if (bats[lookup].parbat != NULL) {
			cand = BATselect(bats[lookup].parbat, cand, &parid, NULL, true, true, false);
			if (cand == NULL)
				goto bailout;
		}
		BAT *b = BATselect(bats[lookup].nmbat, cand, name, NULL, true, true, false);
		if (cand != bats[lookup].cands)
			bat_destroy(cand);
		if (b == NULL)
			goto bailout;
		if (BATcount(b) > 0) {
			int oldid = ((int *) bats[lookup].idbat->theap->base)[BUNtoid(b, 0) - bats[lookup].nmbat->hseqbase];
			if (oldid != tables[i].newid &&
				(BUNappend(mapold, &oldid, false) != GDK_SUCCEED ||
				 BUNappend(mapnew, &tables[i].newid, false) != GDK_SUCCEED)) {
				bat_destroy(b);
				goto bailout;
			}
			if (tables[i].table == NULL)
				schid = oldid;
			else if (tables[i].column == NULL)
				tabid = oldid;
		}
		bat_destroy(b);
	}

	if (BATcount(mapold) == 0) {
		bat_destroy(mapold);
		bat_destroy(mapnew);
		mapold = NULL;
		mapnew = NULL;
	}

	catalog_bid = COLnew(0, TYPE_int, 0, PERSISTENT);
	catalog_id = COLnew(0, TYPE_int, 0, PERSISTENT);
	dcatalog = COLnew(0, TYPE_oid, 0, PERSISTENT);

	const char *delname;
	delname = NULL;
	for (int i = 0; tables[i].schema != NULL; i++) {
		if (tables[i].fullname == NULL) /* schema */
			continue;
		if (tables[i].column == NULL) /* table */
			delname = tables[i].fullname;
		BAT *b = temp_descriptor(old_logger_find_bat(lg, tables[i].fullname, 0, 0));
		if (b == NULL)
			continue;
		BAT *orig = b;
		if (tables[i].hasids && mapold) {
			BAT *b1, *b2;
			BAT *cands = temp_descriptor(old_logger_find_bat(lg, delname, 0, 0));
			gdk_return rc;
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
			if (BATcount(b1) == 0) {
				bat_destroy(b1);
				bat_destroy(b2);
			} else {
				BAT *b3 = COLcopy(b, b->ttype, true, PERSISTENT);
				b = b3;
				if (b == NULL) {
					bat_destroy(orig);
					bat_destroy(b1);
					bat_destroy(b2);
					goto bailout;
				}
				b3 = BATproject(b2, mapnew);
				bat_destroy(b2);
				rc = BATreplace(b, b1, b3, false);
				bat_destroy(b1);
				bat_destroy(b3);
				if (rc != GDK_SUCCEED) {
					bat_destroy(orig);
					bat_destroy(b);
					goto bailout;
				}
			}
			/* now b contains the updated values for the column in tables[i] */
		}
		/* here, b is either the original, unchanged bat or the updated one */
		if (BUNappend(catalog_bid, &b->batCacheid, false) != GDK_SUCCEED ||
			BUNappend(catalog_id, &tables[i].newid, false) != GDK_SUCCEED) {
			if (b != orig)
				bat_destroy(orig);
			bat_destroy(b);
			goto bailout;
		}
		if (b != orig) {
			BBPretain(b->batCacheid);
			BBPrelease(orig->batCacheid);
			BATmode(b, false);
			BATmode(orig, true);
			bat_destroy(orig);
		}
		bat_destroy(b);
	}

	BAT *cands, *b;
	if (BATcount(lg->dcatalog) == 0) {
		cands = NULL;
	} else {
		if (BATsort(&b, NULL, NULL, lg->dcatalog, NULL, NULL, false, false, false) != GDK_SUCCEED)
			goto bailout;
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
	b = BATconvert(lg->catalog_oid, cands, TYPE_int, true, 0, 0, 0);
	if (b == NULL) {
		bat_destroy(cands);
		goto bailout;
	}
	if (BATappend(catalog_id, b, NULL, false) != GDK_SUCCEED ||
		BATappend(catalog_bid, lg->catalog_bid, cands, false) != GDK_SUCCEED) {
		bat_destroy(cands);
		bat_destroy(b);
		goto bailout;
	}
	bat_destroy(cands);
	bat_destroy(b);

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
	bat_destroy(catalog_bid);
	bat_destroy(catalog_id);
	bat_destroy(dcatalog);
	return rc;
}

static gdk_return
bl_postversion(void *Store, void *lg)
{
	sqlstore *store = Store;
	(void)store;
	(void)lg;
	upgrade(lg);
#ifdef CATALOG_JUN2020
	if (store->catalog_version <= CATALOG_JUN2020) {
		int id;
		lng lid;
		BAT *fid = temp_descriptor(old_logger_find_bat(lg, N("sys", "functions", "id"), 0, 0));
		if (logger_sequence(lg, OBJ_SID, &lid) == 0 ||
		    fid == NULL) {
			bat_destroy(fid);
			return GDK_FAIL;
		}
		id = (int) lid;
		BAT *sem = COLnew(fid->hseqbase, TYPE_bit, BATcount(fid), PERSISTENT);
		if (sem == NULL) {
			bat_destroy(fid);
			return GDK_FAIL;
		}
		bit *fsys = (bit *) Tloc(sem, 0);
		for (BUN p = 0, q = BATcount(fid); p < q; p++) {
			fsys[p] = 1;
		}

		sem->tkey = false;
		sem->tsorted = sem->trevsorted = true;
		sem->tnonil = true;
		sem->tnil = false;
		BATsetcount(sem, BATcount(fid));
		bat_destroy(fid);
		if (BATsetaccess(sem, BAT_READ) != GDK_SUCCEED ||
		    old_logger_add_bat(lg, sem, N("sys", "functions", "semantics"), 0, 0) != GDK_SUCCEED) {

			bat_destroy(sem);
			return GDK_FAIL;
		}
		bat_destroy(sem);
		int sid;
		int tid = old_find_table_id(lg, "functions", &sid);
		if (tabins(lg, true, -1, NULL, "sys", "_columns",
			   "id", &id,
			   "name", "semantics",
			   "type", "boolean",
			   "type_digits", &((const int) {1}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &((const int) {11}),
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		{	/* move sql.degrees, sql.radians, sql.like and sql.ilike functions from 09_like.sql and 10_math.sql script to sql_types list */
			BAT *func_func = temp_descriptor(old_logger_find_bat(lg, N("sys", "functions", "name"), 0, 0));
			if (func_func == NULL) {
				return GDK_FAIL;
			}
			BAT *degrees_func = BATselect(func_func, NULL, "degrees", NULL, 1, 1, 0);
			if (degrees_func == NULL) {
				bat_destroy(func_func);
				return GDK_FAIL;
			}
			BAT *radians_func = BATselect(func_func, NULL, "radians", NULL, 1, 1, 0);
			if (radians_func == NULL) {
				bat_destroy(degrees_func);
				return GDK_FAIL;
			}

			BAT *cands = BATmergecand(degrees_func, radians_func);
			bat_destroy(degrees_func);
			bat_destroy(radians_func);
			if (cands == NULL) {
				bat_destroy(func_func);
				return GDK_FAIL;
			}

			BAT *like_func = BATselect(func_func, NULL, "like", NULL, 1, 1, 0);
			if (like_func == NULL) {
				bat_destroy(func_func);
				bat_destroy(cands);
				return GDK_FAIL;
			}

			BAT *ncands = BATmergecand(cands, like_func);
			bat_destroy(cands);
			bat_destroy(like_func);
			if (ncands == NULL) {
				bat_destroy(func_func);
				return GDK_FAIL;
			}

			BAT *ilike_func = BATselect(func_func, NULL, "ilike", NULL, 1, 1, 0);
			bat_destroy(func_func);
			if (ilike_func == NULL) {
				bat_destroy(ncands);
				return GDK_FAIL;
			}

			BAT *final_cands = BATmergecand(ncands, ilike_func);
			bat_destroy(ncands);
			bat_destroy(ilike_func);
			if (final_cands == NULL) {
				return GDK_FAIL;
			}

			BAT *sys_funcs = temp_descriptor(old_logger_find_bat(lg, D("sys", "functions"), 0, 0));
			if (sys_funcs == NULL) {
				bat_destroy(final_cands);
				return GDK_FAIL;
			}
			gdk_return res = BATappend(sys_funcs, final_cands, NULL, true);
			bat_destroy(final_cands);
			bat_destroy(sys_funcs);
			if (res != GDK_SUCCEED)
				return res;
			if ((res = old_logger_upgrade_bat(lg, D("sys", "functions"), LOG_TAB, 0)) != GDK_SUCCEED)
				return res;
		}
		{	/* Fix SQL aggregation functions defined on the wrong modules: sql.null, sql.all, sql.zero_or_one and sql.not_unique */
			BAT *func_mod = temp_descriptor(old_logger_find_bat(lg, N("sys", "functions", "mod"), 0, 0));
			if (func_mod == NULL)
				return GDK_FAIL;

			BAT *sqlfunc = BATselect(func_mod, NULL, "sql", NULL, 1, 1, 0); /* Find the functions defined on sql module */
			if (sqlfunc == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}
			BAT *func_type = temp_descriptor(old_logger_find_bat(lg, N("sys", "functions", "type"), 0, 0));
			if (func_type == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlfunc);
				return GDK_FAIL;
			}
			int three = 3; /* and are aggregates */
			BAT *sqlaggr_func = BATselect(func_type, sqlfunc, &three, NULL, 1, 1, 0);
			bat_destroy(sqlfunc);
			if (sqlaggr_func == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}

			BAT *func_func = temp_descriptor(old_logger_find_bat(lg, N("sys", "functions", "func"), 0, 0));
			if (func_func == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				return GDK_FAIL;
			}
			BAT *nullfunc = BATselect(func_func, sqlaggr_func, "null", NULL, 1, 1, 0);
			if (nullfunc == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				bat_destroy(func_func);
				return GDK_FAIL;
			}
			BAT *allfunc = BATselect(func_func, sqlaggr_func, "all", NULL, 1, 1, 0);
			if (allfunc == NULL) {
				bat_destroy(func_mod);
				bat_destroy(sqlaggr_func);
				bat_destroy(func_func);
				bat_destroy(nullfunc);
				return GDK_FAIL;
			}
			BAT *zero_or_onefunc = BATselect(func_func, sqlaggr_func, "zero_or_one", NULL, 1, 1, 0);
			if (zero_or_onefunc == NULL) {
				bat_destroy(func_func);
				bat_destroy(sqlaggr_func);
				bat_destroy(func_mod);
				bat_destroy(nullfunc);
				bat_destroy(allfunc);
				return GDK_FAIL;
			}
			BAT *not_uniquefunc = BATselect(func_func, sqlaggr_func, "not_unique", NULL, 1, 1, 0);
			bat_destroy(func_func);
			bat_destroy(sqlaggr_func);
			if (not_uniquefunc == NULL) {
				bat_destroy(func_mod);
				bat_destroy(nullfunc);
				bat_destroy(allfunc);
				bat_destroy(zero_or_onefunc);
				return GDK_FAIL;
			}

			BAT *cands1 = BATmergecand(nullfunc, allfunc);
			bat_destroy(nullfunc);
			bat_destroy(allfunc);
			if (cands1 == NULL) {
				bat_destroy(func_mod);
				bat_destroy(zero_or_onefunc);
				bat_destroy(not_uniquefunc);
				return GDK_FAIL;
			}
			BAT *cands2 = BATmergecand(cands1, zero_or_onefunc);
			bat_destroy(zero_or_onefunc);
			bat_destroy(cands1);
			if (cands2 == NULL) {
				bat_destroy(func_mod);
				bat_destroy(not_uniquefunc);
				return GDK_FAIL;
			}
			BAT *cands3 = BATmergecand(cands2, not_uniquefunc);
			bat_destroy(not_uniquefunc);
			bat_destroy(cands2);
			if (cands3 == NULL) {
				bat_destroy(func_mod);
				return GDK_FAIL;
			}

			BAT *cands_project = BATproject(cands3, func_mod);
			if (cands_project == NULL) {
				bat_destroy(func_mod);
				bat_destroy(cands3);
				return GDK_FAIL;
			}
			const char *right_module = "aggr"; /* set module to 'aggr' */
			BAT *update_bat = BATconstant(cands_project->hseqbase, TYPE_str, right_module, BATcount(cands_project), TRANSIENT);
			bat_destroy(cands_project);
			if (update_bat == NULL) {
				bat_destroy(func_mod);
				bat_destroy(cands3);
				return GDK_FAIL;
			}

			gdk_return res = BATreplace(func_mod, cands3, update_bat, TRUE);
			bat_destroy(func_mod);
			bat_destroy(cands3);
			bat_destroy(update_bat);
			if (res != GDK_SUCCEED)
				return res;
			if ((res = old_logger_upgrade_bat(lg, N("sys", "functions", "mod"), LOG_COL, 0)) != GDK_SUCCEED)
				return res;
		}
	}
#endif

#ifdef CATALOG_OCT2020
	if (store->catalog_version <= CATALOG_OCT2020) {
		lng lid;
		if (logger_sequence(lg, OBJ_SID, &lid) == 0)
			return GDK_FAIL;
		(void)lid;
#if 0 /* TODO */
		int id = (int) lid;
		char *schemas[2] = {"sys", "tmp"};
		for (int i = 0 ; i < 2; i++) { /* create for both tmp and sys schemas */
			int sid, tid = old_find_table_id(lg, schemas[i], "objects", &sid);
			if (tabins(lg, true, -1, NULL, "sys", "_columns",
				"id", &id,
				"name", "sub",
				"type", "int",
				"type_digits", &((const int) {32}),
				"type_scale", &((const int) {0}),
				"table_id", &tid,
				"default", str_nil,
				"null", &((const bit) {TRUE}),
				"number", &((const int) {3}),
				"storage", str_nil,
				NULL) != GDK_SUCCEED)
				return GDK_FAIL;
			id++;
		}

		/* add sub column to "objects" table. This is required for merge tables */
		BAT *objs_id = temp_descriptor(old_old_logger_find_bat(lg, N("sys", "objects", "id"), 0, 0));
		if (!objs_id)
			return GDK_FAIL;

		BAT *objs_sub = BATconstant(objs_id->hseqbase, TYPE_int, ATOMnilptr(TYPE_int), BATcount(objs_id), PERSISTENT);
		if (!objs_sub) {
			bat_destroy(objs_id);
			return GDK_FAIL;
		}
		if (BATsetaccess(objs_sub, BAT_READ) != GDK_SUCCEED || old_logger_add_bat(lg, objs_sub, N("sys", "objects", "sub"), 0, 0) != GDK_SUCCEED) {
			bat_destroy(objs_id);
			bat_destroy(objs_sub);
			return GDK_FAIL;
		}

		BAT *objs_nr = temp_descriptor(old_old_logger_find_bat(lg, N("sys", "objects", "nr"), 0, 0));
		if (!objs_nr) {
			bat_destroy(objs_id);
			bat_destroy(objs_nr);
			bat_destroy(objs_sub);
			return GDK_FAIL;
		}

		/* hopefully no one will create a key or index with more than 2000 columns */
		BAT *tids = BATthetaselect(objs_nr, NULL, &((const int) {2000}), ">");
		if (!tids) {
			bat_destroy(objs_id);
			bat_destroy(objs_nr);
			bat_destroy(objs_sub);
			return GDK_FAIL;
		}

		BAT *prj = BATproject2(tids, objs_nr, NULL);
		if (!prj) {
			bat_destroy(objs_id);
			bat_destroy(objs_nr);
			bat_destroy(objs_sub);
			bat_destroy(tids);
			return GDK_FAIL;
		}
		gdk_return res = BATreplace(objs_sub, tids, prj, TRUE); /* 'sub' takes the id of the child */
		bat_destroy(objs_sub);
		bat_destroy(prj);
		if (res != GDK_SUCCEED) {
			bat_destroy(objs_id);
			bat_destroy(objs_nr);
			bat_destroy(tids);
			return res;
		}

		if (!(prj = BATproject2(tids, objs_id, NULL))) {
			bat_destroy(objs_id);
			bat_destroy(objs_nr);
			bat_destroy(objs_sub);
			bat_destroy(tids);
			return GDK_FAIL;
		}
		res = BATreplace(objs_nr, tids, prj, TRUE); /* 'nr' takes the id of the parent */
		bat_destroy(objs_nr);
		bat_destroy(prj);
		if (res != GDK_SUCCEED) {
			bat_destroy(objs_id);
			bat_destroy(tids);
			return res;
		}
		if (old_logger_upgrade_bat(lg, N("sys", "objects", "nr"), LOG_COL, 0) != GDK_SUCCEED || old_logger_upgrade_bat(lg, N("sys", "objects", "sub"), LOG_COL, 0) != GDK_SUCCEED) {
			bat_destroy(objs_id);
			bat_destroy(tids);
			return GDK_FAIL;
		}

		BAT *new_ids = BATconstant(objs_id->hseqbase, TYPE_int, ATOMnilptr(TYPE_int), BATcount(tids), PERSISTENT);
		if (!new_ids) {
			bat_destroy(objs_id);
			bat_destroy(tids);
			return GDK_FAIL;
		}
		res = BATreplace(objs_id, tids, new_ids, TRUE); /* 'id' will get initialized at load_part */
		bat_destroy(objs_id);
		bat_destroy(tids);
		bat_destroy(new_ids);
		if (res != GDK_SUCCEED)
			return res;

		if (old_logger_upgrade_bat(lg, N("sys", "objects", "id"), LOG_COL, 0) != GDK_SUCCEED)
			return GDK_FAIL;
#endif
	}
#endif

	return GDK_SUCCEED;
}

static int
bl_create(sqlstore *store, int debug, const char *logdir, int cat_version)
{
	if (store->logger)
		return LOG_ERR;
	store->logger = logger_create(debug, "sql", logdir, cat_version, (preversionfix_fptr)&bl_preversion, (postversionfix_fptr)&bl_postversion, store);
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
		logger_destroy(l);
}

static int
bl_flush(sqlstore *store, lng save_id)
{
	if (store->logger)
		return logger_flush(store->logger, save_id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static int
bl_changes(sqlstore *store)
{
	return (int) MIN(logger_changes(store->logger), GDK_int_max);
}

static int
bl_get_sequence(sqlstore *store, int seq, lng *id)
{
	return logger_sequence(store->logger, seq, id);
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
bl_tstart(sqlstore *store, ulng commit_ts)
{
	return log_tstart(store->logger, commit_ts) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
bl_tend(sqlstore *store)
{
	return log_tend(store->logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int
bl_sequence(sqlstore *store, int seq, lng id)
{
	return log_sequence(store->logger, seq, id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

/* Write a plan entry to copy part of the given file.
 * That part of the file must remain unchanged until the plan is executed.
 */
static void
snapshot_lazy_copy_file(stream *plan, const char *name, uint64_t extent)
{
	mnstr_printf(plan, "c %" PRIu64 " %s\n", extent, name);
}

/* Write a plan entry to write the current contents of the given file.
 * The contents are included in the plan so the source file is allowed to
 * change in the mean time.
 */
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

	mnstr_printf(plan, "w %zu %s\n", to_copy, name);

	while (to_copy > 0) {
		size_t chunk = (to_copy <= bufsize) ? to_copy : bufsize;
		ssize_t bytes_read = mnstr_read(s, buf, 1, chunk);
		if (bytes_read < 0) {
			char *err = mnstr_error(s);
			GDKerror("Reading bytes of component %s failed: %s", path, err);
			free(err);
			goto end;
		} else if (bytes_read < (ssize_t) chunk) {
			char *err = mnstr_error(s);
			GDKerror("Read only %zu/%zu bytes of component %s: %s", (size_t) bytes_read, chunk, path, err);
			free(err);
			goto end;
		}

		ssize_t bytes_written = mnstr_write(plan, buf, 1, chunk);
		if (bytes_written < 0) {
			GDKerror("Writing to plan failed");
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
	snapshot_immediate_copy_file(plan, log_file, log_file + strlen(db_dir) + 1);

	for (ulng id = bat_logger->saved_id+1; id <= bat_logger->id; id++) {
		struct stat statbuf;

		len = snprintf(log_file, sizeof(log_file), "%s/%s%s." LLFMT, db_dir, bat_logger->dir, LOGFILE, id);
		if (len == -1 || (size_t)len >= sizeof(log_file)) {
			GDKerror("Could not open %s, filename is too large", log_file);
			return GDK_FAIL;
		}
		if (MT_stat(log_file, &statbuf) == 0) {
			snapshot_lazy_copy_file(plan, log_file + strlen(db_dir) + 1, statbuf.st_size);
		} else {
			GDKerror("Could not open %s", log_file);
			return GDK_FAIL;
		}
	}
	return GDK_SUCCEED;
}

static gdk_return
snapshot_heap(stream *plan, const char *db_dir, uint64_t batid, const char *filename, const char *suffix, uint64_t extent)
{
	char path1[FILENAME_MAX];
	char path2[FILENAME_MAX];
	const size_t offset = strlen(db_dir) + 1;
	struct stat statbuf;
	int len;

	// first check the backup dir
	len = snprintf(path1, FILENAME_MAX, "%s/%s/%" PRIo64 "%s", db_dir, BAKDIR, batid, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path1[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path1);
		return GDK_FAIL;
	}
	if (MT_stat(path1, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, path1 + offset, extent);
		return GDK_SUCCEED;
	}
	if (errno != ENOENT) {
		GDKsyserror("Error stat'ing %s", path1);
		return GDK_FAIL;
	}

	// then check the regular location
	len = snprintf(path2, FILENAME_MAX, "%s/%s/%s%s", db_dir, BATDIR, filename, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path2[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path2);
		return GDK_FAIL;
	}
	if (MT_stat(path2, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, path2 + offset, extent);
		return GDK_SUCCEED;
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
static gdk_return
snapshot_bats(stream *plan, const char *db_dir)
{
	char bbpdir[FILENAME_MAX];
	stream *cat = NULL;
	char line[1024];
	int gdk_version, len;
	gdk_return ret = GDK_FAIL;

	len = snprintf(bbpdir, FILENAME_MAX, "%s/%s/%s", db_dir, BAKDIR, "BBP.dir");
	if (len == -1 || len >= FILENAME_MAX) {
		GDKerror("Could not open %s, filename is too large", bbpdir);
		goto end;
	}
	ret = snapshot_immediate_copy_file(plan, bbpdir, bbpdir + strlen(db_dir) + 1);
	if (ret != GDK_SUCCEED)
		goto end;

	// Open the catalog and parse the header
	cat = open_rastream(bbpdir);
	if (cat == NULL) {
		GDKerror("Could not open %s for reading: %s", bbpdir, mnstr_peek_error(NULL));
		goto end;
	}
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Could not read first line of %s", bbpdir);
		goto end;
	}
	if (sscanf(line, "BBP.dir, GDKversion %d", &gdk_version) != 1) {
		GDKerror("Invalid first line of %s", bbpdir);
		goto end;
	}
	if (gdk_version != 061043U) {
		// If this version number has changed, the structure of BBP.dir
		// may have changed. Update this whole function to take this
		// into account.
		// Note: when startup has completed BBP.dir is guaranteed
		// to the latest format so we don't have to support any older
		// formats in this function.
		GDKerror("GDK version mismatch in snapshot yet");
		goto end;
	}
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Couldn't skip the second line of %s", bbpdir);
		goto end;
	}
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Couldn't skip the third line of %s", bbpdir);
		goto end;
	}

	/* TODO get transaction id and last processed log file id */
	if (mnstr_readline(cat, line, sizeof(line)) < 0) {
		GDKerror("Couldn't skip the 4th line of %s", bbpdir);
		goto end;
	}

	while (mnstr_readline(cat, line, sizeof(line)) > 0) {
		uint64_t batid;
		uint64_t tail_free;
		uint64_t theap_free;
		char filename[sizeof(BBP_physical(0))];
		// The lines in BBP.dir come in various lengths.
		// we try to parse the longest variant then check
		// the return value of sscanf to see which fields
		// were actually present.
		int scanned = sscanf(line,
				// Taken from the sscanf in BBPreadEntries() in gdk_bbp.c.
				// 8 fields, we need field 1 (batid) and field 4 (filename)
				"%" SCNu64 " %*s %*s %19s %*s %*s %*s %*s"

				// Taken from the sscanf in heapinit() in gdk_bbp.c.
				// 14 fields, we need field 10 (free)
				" %*s %*s %*s %*s %*s %*s %*s %*s %*s %" SCNu64 " %*s %*s %*s %*s"

				// Taken from the sscanf in vheapinit() in gdk_bbp.c.
				// 3 fields, we need field 1 (free).
				"%" SCNu64 " %*s ^*s"
				,
				&batid, filename,
				&tail_free,
				&theap_free);

		// The following switch uses fallthroughs to make
		// the larger cases include the work of the smaller cases.
		switch (scanned) {
			default:
				GDKerror("Couldn't parse (%d) %s line: %s", scanned, bbpdir, line);
				goto end;
			case 4:
				// tail and theap
				ret = snapshot_heap(plan, db_dir, batid, filename, ".theap", theap_free);
				if (ret != GDK_SUCCEED)
					goto end;
				/* fallthrough */
			case 3:
				// tail only
				snapshot_heap(plan, db_dir, batid, filename, ".tail", tail_free);
				if (ret != GDK_SUCCEED)
					goto end;
				/* fallthrough */
			case 2:
				// no tail?
				break;
		}
	}

end:
	if (cat) {
		close_stream(cat);
	}
	return ret;
}

/* Add a file to the plan which records the current wlc status, if any.
 * In particular, `wlc_batches`.
 *
 * With this information, a replica initialized from this snapshot can
 * be configured to catch up with its master by replaying later transactions.
 */
static gdk_return
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

	mnstr_printf(plan, "w %d %s\n", len, name);
	mnstr_write(plan, buf, 1, len);

	return GDK_SUCCEED;
}

static gdk_return
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
		snapshot_lazy_copy_file(plan, ".vaultkey", statbuf.st_size);
		return GDK_SUCCEED;
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
	db_dir_len = strlen(db_dir);
	if (db_dir[db_dir_len - 1] == DIR_SEP)
		db_dir[db_dir_len - 1] = '\0';

	mnstr_printf(plan, "%s\n", db_dir);

	// Please monetdbd
	mnstr_printf(plan, "w 0 .uplog\n");

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
	if (db_dir)
		GDKfree(db_dir);
	return ret;
}

void
bat_logger_init( logger_functions *lf )
{
	lf->create = bl_create;
	lf->destroy = bl_destroy;
	lf->flush = bl_flush;
	lf->changes = bl_changes;
	lf->get_sequence = bl_get_sequence;
	lf->log_isnew = bl_log_isnew;
	lf->log_tstart = bl_tstart;
	lf->log_tend = bl_tend;
	lf->log_sequence = bl_sequence;
	lf->get_snapshot_files = bl_snapshot;
}
