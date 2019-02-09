/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */

#define CATALOG_MAR2018 52201
#define CATALOG_AUG2018 52202

logger *bat_logger = NULL;

/* return GDK_SUCCEED if we can handle the upgrade from oldversion to
 * newversion */
static gdk_return
bl_preversion(int oldversion, int newversion)
{
	(void)newversion;

#ifdef CATALOG_MAR2018
	if (oldversion == CATALOG_MAR2018) {
		/* upgrade to Aug2018 releases */
		catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

#ifdef CATALOG_AUG2018
	if (oldversion == CATALOG_AUG2018) {
		/* upgrade to default releases */
		catalog_version = oldversion;
		return GDK_SUCCEED;
	}
#endif

	return GDK_FAIL;
}

#define N(schema, table, column)	schema "_" table "_" column

#ifdef CATALOG_AUG2018
static int
find_table_id(logger *lg, const char *val, int *sid)
{
	BAT *s = NULL;
	BAT *b, *t;
	BATiter bi;
	oid o;
	int id;

	b = temp_descriptor(logger_find_bat(lg, N("sys", "schemas", "name"), 0, 0));
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
	b = temp_descriptor(logger_find_bat(lg, N("sys", "schemas", "id"), 0, 0));
	if (b == NULL)
		return 0;
	bi = bat_iterator(b);
	id = * (const int *) BUNtloc(bi, o - b->hseqbase);
	bat_destroy(b);
	/* store id of schema "sys" */
	*sid = id;

	b = temp_descriptor(logger_find_bat(lg, N("sys", "_tables", "name"), 0, 0));
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
	b = temp_descriptor(logger_find_bat(lg, N("sys", "_tables", "schema_id"), 0, 0));
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

	b = temp_descriptor(logger_find_bat(lg, N("sys", "_tables", "id"), 0, 0));
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

	va_start(va, tname);
	while ((cname = va_arg(va, char *)) != NULL) {
		cval = va_arg(va, void *);
		snprintf(lname, sizeof(lname), "%s_%s_%s", sname, tname, cname);
		if ((b = temp_descriptor(logger_find_bat(lg, lname, 0, 0))) == NULL)
			return GDK_FAIL;
		if (first) {
			BAT *bn;
			if ((bn = COLcopy(b, b->ttype, true, PERSISTENT)) == NULL) {
				BBPunfix(b->batCacheid);
				return GDK_FAIL;
			}
			BBPunfix(b->batCacheid);
			if (BATsetaccess(bn, BAT_READ) != GDK_SUCCEED ||
			    logger_add_bat(lg, bn, lname, 0, 0) != GDK_SUCCEED) {
				BBPunfix(bn->batCacheid);
				return GDK_FAIL;
			}
			b = bn;
		}
		rc = BUNappend(b, cval, true);
		BBPunfix(b->batCacheid);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	if (tt >= 0) {
		if ((b = COLnew(0, tt, 0, PERSISTENT)) == NULL)
			return GDK_FAIL;
		if ((rc = BATsetaccess(b, BAT_READ)) == GDK_SUCCEED)
			rc = logger_add_bat(lg, b, nname, 0, 0);
		BBPunfix(b->batCacheid);
		if (rc != GDK_SUCCEED)
			return rc;
	}
	return GDK_SUCCEED;
}
#endif

static gdk_return
bl_postversion(void *lg)
{
	(void)lg;

#ifdef CATALOG_MAR2018
	if (catalog_version <= CATALOG_MAR2018) {
		/* In the past, the sys._tables.readlonly and
		 * tmp._tables.readonly columns were renamed to
		 * (sys|tmp)._tables.access and the type was changed
		 * from BOOLEAN to SMALLINT.  It may be that this
		 * change didn't make it to the sys._columns table but
		 * was only done in the internal representation of the
		 * (sys|tmp)._tables tables.  Here we fix this. */

		/* first figure out whether there are any columns in
		 * the catalog called "readonly" (if there are fewer
		 * then 2, then we don't have to do anything) */
		BAT *cn = temp_descriptor(logger_find_bat(lg, N("sys", "_columns", "name"), 0, 0));
		if (cn == NULL)
			return GDK_FAIL;
		BAT *cs = BATselect(cn, NULL, "readonly", NULL, 1, 1, 0);
		if (cs == NULL) {
			bat_destroy(cn);
			return GDK_FAIL;
		}
		if (BATcount(cs) >= 2) {
			/* find the OIDs of the rows of sys.schemas
			 * where the name is either 'sys' or 'tmp',
			 * result in ss */
			BAT *sn = temp_descriptor(logger_find_bat(lg, N("sys", "schemas", "name"), 0, 0));
			if (sn == NULL) {
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			BAT *ss1 = BATselect(sn, NULL, "sys", NULL, 1, 1, 0);
			BAT *ss2 = BATselect(sn, NULL, "tmp", NULL, 1, 1, 0);
			bat_destroy(sn);
			if (ss1 == NULL || ss2 == NULL) {
				bat_destroy(ss1);
				bat_destroy(ss2);
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			assert(BATcount(ss1) == 1);
			assert(BATcount(ss2) == 1);
			BAT *ss = BATmergecand(ss1, ss2);
			bat_destroy(ss1);
			bat_destroy(ss2);
			if (ss == NULL) {
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			assert(BATcount(ss) == 2);
			/* find the OIDs of the rows of sys._tables
			 * where the name is '_tables', result in
			 * ts */
			BAT *tn = temp_descriptor(logger_find_bat(lg, N("sys", "_tables", "name"), 0, 0));
			if (tn == NULL) {
				bat_destroy(ss);
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			BAT *ts = BATselect(tn, NULL, "_tables", NULL, 1, 1, 0);
			bat_destroy(tn);
			if (ts == NULL) {
				bat_destroy(ss);
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			/* find the OIDs of the rows of sys._tables
			 * where the name is '_tables' (candidate list
			 * ts) and the schema is either 'sys' or 'tmp'
			 * (candidate list ss for sys.schemas.id
			 * column), result in ts1 */
			tn = temp_descriptor(logger_find_bat(lg, N("sys", "_tables", "schema_id"), 0, 0));
			sn = temp_descriptor(logger_find_bat(lg, N("sys", "schemas", "id"), 0, 0));
			if (tn == NULL || sn == NULL) {
				bat_destroy(tn);
				bat_destroy(sn);
				bat_destroy(ts);
				bat_destroy(ss);
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			BAT *ts1 = BATintersect(tn, sn, ts, ss, 0, 2);
			bat_destroy(tn);
			bat_destroy(sn);
			bat_destroy(ts);
			bat_destroy(ss);
			if (ts1 == NULL) {
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			/* find the OIDs of the rows of sys._columns
			 * where the name is 'readonly' (candidate
			 * list cs) and the table is either
			 * sys._tables or tmp._tables (candidate list
			 * ts1 for sys._tables.table_id), result in
			 * cs1, transferred to cs) */
			BAT *ct = temp_descriptor(logger_find_bat(lg, N("sys", "_columns", "table_id"), 0, 0));
			tn = temp_descriptor(logger_find_bat(lg, N("sys", "_tables", "id"), 0, 0));
			if (ct == NULL || tn == NULL) {
				bat_destroy(ct);
				bat_destroy(tn);
				bat_destroy(ts1);
				bat_destroy(cs);
				bat_destroy(cn);
				return GDK_FAIL;
			}
			BAT *cs1 = BATintersect(ct, tn, cs, ts1, 0, 2);
			bat_destroy(ct);
			bat_destroy(tn);
			bat_destroy(ts1);
			bat_destroy(cs);
			if (cs1 == NULL) {
				bat_destroy(cn);
				return GDK_FAIL;
			}
			cs = cs1;
			if (BATcount(cs) == 2) {
				/* in cs we now have the OIDs of the
				 * rows in sys._columns where we have
				 * to change the name and type from
				 * "readonly" and "boolean" to
				 * "access" and "smallint" */
				ct = temp_descriptor(logger_find_bat(lg, N("sys", "_columns", "type"), 0, 0));
				BAT *cd = temp_descriptor(logger_find_bat(lg, N("sys", "_columns", "type_digits"), 0, 0));
				BAT *ctn = COLnew(ct->hseqbase, ct->ttype, BATcount(ct), PERSISTENT);
				BAT *cdn = COLnew(cd->hseqbase, cd->ttype, BATcount(cd), PERSISTENT);
				BAT *cnn = COLnew(cn->hseqbase, cn->ttype, BATcount(cn), PERSISTENT);
				if (ct == NULL || cd == NULL || ctn == NULL || cdn == NULL || cnn == NULL) {
				  bailout1:
					bat_destroy(ct);
					bat_destroy(cd);
					bat_destroy(ctn);
					bat_destroy(cdn);
					bat_destroy(cnn);
					bat_destroy(cs);
					bat_destroy(cn);
					return GDK_FAIL;
				}
				BATiter cti = bat_iterator(ct);
				BATiter cdi = bat_iterator(cd);
				BATiter cni = bat_iterator(cn);
				BUN p;
				BUN q = BUNtoid(cs, 0) - cn->hseqbase;
				for (p = 0; p < q; p++) {
					if (BUNappend(cnn, BUNtvar(cni, p), false) != GDK_SUCCEED ||
					    BUNappend(cdn, BUNtloc(cdi, p), false) != GDK_SUCCEED ||
					    BUNappend(ctn, BUNtloc(cti, p), false) != GDK_SUCCEED) {
						goto bailout1;
					}
				}
				int i = 16;
				if (BUNappend(cnn, "access", false) != GDK_SUCCEED ||
				    BUNappend(cdn, &i, false) != GDK_SUCCEED ||
				    BUNappend(ctn, "smallint", false) != GDK_SUCCEED) {
					goto bailout1;
				}
				q = BUNtoid(cs, 1) - cn->hseqbase;
				for (p++; p < q; p++) {
					if (BUNappend(cnn, BUNtvar(cni, p), false) != GDK_SUCCEED ||
					    BUNappend(cdn, BUNtloc(cdi, p), false) != GDK_SUCCEED ||
					    BUNappend(ctn, BUNtloc(cti, p), false) != GDK_SUCCEED) {
						goto bailout1;
					}
				}
				if (BUNappend(cnn, "access", false) != GDK_SUCCEED ||
				    BUNappend(cdn, &i, false) != GDK_SUCCEED ||
				    BUNappend(ctn, "smallint", false) != GDK_SUCCEED) {
					goto bailout1;
				}
				q = BATcount(cn);
				for (p++; p < q; p++) {
					if (BUNappend(cnn, BUNtvar(cni, p), false) != GDK_SUCCEED ||
					    BUNappend(cdn, BUNtloc(cdi, p), false) != GDK_SUCCEED ||
					    BUNappend(ctn, BUNtloc(cti, p), false) != GDK_SUCCEED) {
						goto bailout1;
					}
				}
				bat_destroy(ct);
				bat_destroy(cd);
				bat_destroy(cs); cs = NULL;
				bat_destroy(cn); cn = NULL;
				if (BATsetaccess(cnn, BAT_READ) != GDK_SUCCEED ||
				    BATsetaccess(cdn, BAT_READ) != GDK_SUCCEED ||
				    BATsetaccess(ctn, BAT_READ) != GDK_SUCCEED ||
				    logger_add_bat(lg, cnn, N("sys", "_columns", "name"), 0, 0) != GDK_SUCCEED ||
				    logger_add_bat(lg, cdn, N("sys", "_columns", "type_digits"), 0, 0) != GDK_SUCCEED ||
				    logger_add_bat(lg, ctn, N("sys", "_columns", "type"), 0, 0) != GDK_SUCCEED) {
					bat_destroy(ctn);
					bat_destroy(cdn);
					bat_destroy(cnn);
					return GDK_FAIL;
				}
				bat_destroy(ctn);
				bat_destroy(cdn);
				bat_destroy(cnn);
			}
		}
		bat_destroy(cs);
		bat_destroy(cn);
	}
#endif

#ifdef CATALOG_AUG2018
	if (catalog_version <= CATALOG_AUG2018) {
		int id;
		lng lid;
		BAT *fid = temp_descriptor(logger_find_bat(lg, N("sys", "functions", "id"), 0, 0));
		BAT *sf = temp_descriptor(logger_find_bat(lg, N("sys", "systemfunctions", "function_id"), 0, 0));
		if (logger_sequence(lg, OBJ_SID, &lid) == 0 ||
		    fid == NULL || sf == NULL) {
			bat_destroy(fid);
			bat_destroy(sf);
			return GDK_FAIL;
		}
		id = (int) lid;
		BAT *b = COLnew(fid->hseqbase, TYPE_bit, BATcount(fid), PERSISTENT);
		if (b == NULL) {
			bat_destroy(fid);
			bat_destroy(sf);
			return GDK_FAIL;
		}
		const int *fids = (const int *) Tloc(fid, 0);
		bit *fsys = (bit *) Tloc(b, 0);
		BATiter sfi = bat_iterator(sf);
		if (BAThash(sf) != GDK_SUCCEED) {
			BBPreclaim(b);
			bat_destroy(fid);
			bat_destroy(sf);
			return GDK_FAIL;
		}
		for (BUN p = 0, q = BATcount(fid); p < q; p++) {
			BUN i;
			fsys[p] = 0;
			HASHloop_int(sfi, sf->thash, i, fids + p) {
				fsys[p] = 1;
				break;
			}
		}
		b->tkey = false;
		b->tsorted = b->trevsorted = false;
		b->tnonil = true;
		b->tnil = false;
		BATsetcount(b, BATcount(fid));
		bat_destroy(fid);
		bat_destroy(sf);
		if (BATsetaccess(b, BAT_READ) != GDK_SUCCEED ||
		    logger_add_bat(lg, b, N("sys", "functions", "system"), 0, 0) != GDK_SUCCEED) {

			bat_destroy(b);
			return GDK_FAIL;
		}
		bat_destroy(b);
		int sid;
		int tid = find_table_id(lg, "functions", &sid);
		if (tabins(lg, true, -1, NULL, "sys", "_columns",
			   "id", &id,
			   "name", "system",
			   "type", "boolean",
			   "type_digits", &((const int) {1}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &((const int) {10}),
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;

		/* also create entries for new tables
		 * {table,range,value}_partitions */

		tid = id;
		if (tabins(lg, true, -1, NULL, "sys", "_tables",
			   "id", &tid,
			   "name", "table_partitions",
			   "schema_id", &sid,
			   "query", str_nil,
			   "type", &((const sht) {tt_table}),
			   "system", &((const bit) {TRUE}),
			   "commit_action", &((const sht) {CA_COMMIT}),
			   "access", &((const sht) {0}),
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		int col = 0;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "table_partitions", "id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "table_partitions", "table_id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "table_id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "table_partitions", "column_id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "column_id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_str,
			   N("sys", "table_partitions", "expression"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "expression",
			   "type", "varchar",
			   "type_digits", &((const int) {STORAGE_MAX_VALUE_LENGTH}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_bte,
			   N("sys", "table_partitions", "type"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "type",
			   "type", "tinyint",
			   "type_digits", &((const int) {8}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		int pub = ROLE_PUBLIC;
		int priv = PRIV_SELECT;
		if (tabins(lg, true, -1, NULL, "sys", "privileges",
			   "obj_id", &tid,
			   "auth_id", &pub,
			   "privileges", &priv,
			   "grantor", &((const int) {0}),
			   "grantable", &((const int) {0}),
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		tid = id;
		if (tabins(lg, false, -1, NULL, "sys", "_tables",
			   "id", &tid,
			   "name", "range_partitions",
			   "schema_id", &sid,
			   "query", str_nil,
			   "type", &((const sht) {tt_table}),
			   "system", &((const bit) {TRUE}),
			   "commit_action", &((const sht) {CA_COMMIT}),
			   "access", &((const sht) {0}),
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col = 0;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "range_partitions", "table_id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "table_id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "range_partitions", "partition_id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "partition_id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_str,
			   N("sys", "range_partitions", "minimum"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "minimum",
			   "type", "varchar",
			   "type_digits", &((const int) {STORAGE_MAX_VALUE_LENGTH}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_str,
			   N("sys", "range_partitions", "maximum"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "maximum",
			   "type", "varchar",
			   "type_digits", &((const int) {STORAGE_MAX_VALUE_LENGTH}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_bit,
			   N("sys", "range_partitions", "with_nulls"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "with_nulls",
			   "type", "boolean",
			   "type_digits", &((const int) {1}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		if (tabins(lg, false, -1, NULL, "sys", "privileges",
			   "obj_id", &tid,
			   "auth_id", &pub,
			   "privileges", &priv,
			   "grantor", &((const int) {0}),
			   "grantable", &((const int) {0}),
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;

		tid = id;
		if (tabins(lg, false, -1, NULL, "sys", "_tables",
			   "id", &tid,
			   "name", "value_partitions",
			   "schema_id", &sid,
			   "query", str_nil,
			   "type", &((const sht) {tt_table}),
			   "system", &((const bit) {TRUE}),
			   "commit_action", &((const sht) {CA_COMMIT}),
			   "access", &((const sht) {0}),
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col = 0;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "value_partitions", "table_id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "table_id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_int,
			   N("sys", "value_partitions", "partition_id"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "partition_id",
			   "type", "int",
			   "type_digits", &((const int) {32}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		id++;
		col++;
		if (tabins(lg, false, TYPE_str,
			   N("sys", "value_partitions", "value"),
			   "sys", "_columns",
			   "id", &id,
			   "name", "value",
			   "type", "varchar",
			   "type_digits", &((const int) {STORAGE_MAX_VALUE_LENGTH}),
			   "type_scale", &((const int) {0}),
			   "table_id", &tid,
			   "default", str_nil,
			   "null", &((const bit) {TRUE}),
			   "number", &col,
			   "storage", str_nil,
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		if (tabins(lg, false, -1, NULL, "sys", "privileges",
			   "obj_id", &tid,
			   "auth_id", &pub,
			   "privileges", &priv,
			   "grantor", &((const int) {0}),
			   "grantable", &((const int) {0}),
			   NULL) != GDK_SUCCEED)
			return GDK_FAIL;
		//log_sequence(lg, OBJ_SID, id);
	}
#endif

	return GDK_SUCCEED;
}

static int 
bl_create(int debug, const char *logdir, int cat_version)
{
	if (bat_logger)
		return LOG_ERR;
	bat_logger = logger_create(debug, "sql", logdir, cat_version, bl_preversion, bl_postversion);
	if (bat_logger)
		return LOG_OK;
	return LOG_ERR;
}

static void 
bl_destroy(void)
{
	logger *l = bat_logger;

	bat_logger = NULL;
	if (l) {
		close_stream(l->log);
		GDKfree(l->fn);
		GDKfree(l->dir);
		GDKfree(l->local_dir);
		GDKfree(l->buf);
		GDKfree(l);
	}
}

static int 
bl_restart(void)
{
	if (bat_logger)
		return logger_restart(bat_logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static int
bl_cleanup(void)
{
	if (bat_logger)
		return logger_cleanup(bat_logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
	return LOG_OK;
}

static void
bl_with_ids(void)
{
	if (bat_logger)
		logger_with_ids(bat_logger);
}

static int
bl_changes(void)
{	
	return (int) MIN(logger_changes(bat_logger), GDK_int_max);
}

static int 
bl_get_sequence(int seq, lng *id)
{
	return logger_sequence(bat_logger, seq, id);
}

static int
bl_log_isnew(void)
{
	if (BATcount(bat_logger->catalog_bid) > 10) {
		return 0;
	}
	return 1;
}

static bool
bl_log_needs_update(void)
{
	return !bat_logger->with_ids;
}

static int 
bl_tstart(void)
{
	return log_tstart(bat_logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int 
bl_tend(void)
{
	return log_tend(bat_logger) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static int 
bl_sequence(int seq, lng id)
{
	return log_sequence(bat_logger, seq, id) == GDK_SUCCEED ? LOG_OK : LOG_ERR;
}

static void *
bl_find_table_value(const char *tabnam, const char *tab, const void *val, ...)
{
	BAT *s = NULL;
	BAT *b;
	va_list va;

	va_start(va, val);
	do {
		b = temp_descriptor(logger_find_bat(bat_logger, tab, 0, 0));
		if (b == NULL) {
			bat_destroy(s);
			return NULL;
		}
		BAT *t = BATselect(b, s, val, val, 1, 1, 0);
		bat_destroy(b);
		bat_destroy(s);
		if (t == NULL)
			return NULL;
		s = t;
		if (BATcount(s) == 0) {
			bat_destroy(s);
			return NULL;
		}
	} while ((tab = va_arg(va, const char *)) != NULL &&
		 (val = va_arg(va, const void *)) != NULL);
	va_end(va);

	oid o = BUNtoid(s, 0);
	bat_destroy(s);

	b = temp_descriptor(logger_find_bat(bat_logger, tabnam, 0, 0));
	if (b == NULL)
		return NULL;
	BATiter bi = bat_iterator(b);
	val = BUNtail(bi, o - b->hseqbase);
	size_t sz = ATOMlen(b->ttype, val);
	void *res = GDKmalloc(sz);
	if (res)
		memcpy(res, val, sz);
	bat_destroy(b);
	return res;
}

void
bat_logger_init( logger_functions *lf )
{
	lf->create = bl_create;
	lf->destroy = bl_destroy;
	lf->restart = bl_restart;
	lf->cleanup = bl_cleanup;
	lf->with_ids = bl_with_ids;
	lf->changes = bl_changes;
	lf->get_sequence = bl_get_sequence;
	lf->log_isnew = bl_log_isnew;
	lf->log_needs_update = bl_log_needs_update;
	lf->log_tstart = bl_tstart;
	lf->log_tend = bl_tend;
	lf->log_sequence = bl_sequence;
	lf->log_find_table_value = bl_find_table_value;
}
