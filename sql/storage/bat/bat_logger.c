/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */
#include "wlc.h"

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
	int len;

	va_start(va, tname);
	while ((cname = va_arg(va, char *)) != NULL) {
		cval = va_arg(va, void *);
		len = snprintf(lname, sizeof(lname), "%s_%s_%s", sname, tname, cname);
		if (len == -1 || (size_t)len >= sizeof(lname))
			return GDK_FAIL;
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

	if (stat(path, &statbuf) < 0) {
		GDKerror("stat failed on %s: %s", path, strerror(errno));
		goto end;
	}
	to_copy = (size_t) statbuf.st_size;

	s = open_rstream(path);
	if (!s) {
		GDKerror("could not open %s", path);
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
			GDKerror("Reading bytes of component %s failed: %s", path, mnstr_error(s));
			goto end;
		} else if (bytes_read < (ssize_t) chunk) {
			GDKerror("Read only %zu/%zu bytes of component %s: %s", (size_t) bytes_read, chunk, path, mnstr_error(s));
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
snapshot_wal(stream *plan, const char *db_dir)
{
	stream *log = bat_logger->log;
	char log_file[FILENAME_MAX];
	int len;

	len = snprintf(log_file, sizeof(log_file), "%s/%s%s", db_dir, bat_logger->dir, LOGFILE);
	if (len == -1 || (size_t)len >= sizeof(log_file)) {
		GDKerror("Could not open %s, filename is too large", log_file);
		return GDK_FAIL;
	}
	snapshot_immediate_copy_file(plan, log_file, log_file + strlen(db_dir) + 1);

	len = snprintf(log_file, sizeof(log_file), "%s%s." LLFMT, bat_logger->dir, LOGFILE, bat_logger->id);
	if (len == -1 || (size_t)len >= sizeof(log_file)) {
		GDKerror("Could not open %s, filename is too large", log_file);
		return GDK_FAIL;
	}
	uint64_t extent = getFileSize(log);

	snapshot_lazy_copy_file(plan, log_file, extent);

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
	if (stat(path1, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, path1 + offset, extent);
		return GDK_SUCCEED;
	}
	if (errno != ENOENT) {
		GDKerror("Error stat'ing %s: %s", path1, strerror(errno));
		return GDK_FAIL;
	}

	// then check the regular location
	len = snprintf(path2, FILENAME_MAX, "%s/%s/%s%s", db_dir, BATDIR, filename, suffix);
	if (len == -1 || len >= FILENAME_MAX) {
		path2[FILENAME_MAX - 1] = '\0';
		GDKerror("Could not open %s, filename is too large", path2);
		return GDK_FAIL;
	}
	if (stat(path2, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, path2 + offset, extent);
		return GDK_SUCCEED;
	}
	if (errno != ENOENT) {
		GDKerror("Error stat'ing %s: %s", path2, strerror(errno));
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
		GDKerror("Could not open %s for reading", bbpdir);
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
	if (gdk_version != 061042U) {
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
				// 12 fields, we need field 10 (free)
				" %*s %*s %*s %*s %*s %*s %*s %*s %*s %" SCNu64 " %*s %*s"

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
	if (stat(path, &statbuf) == 0) {
		snapshot_lazy_copy_file(plan, ".vaultkey", statbuf.st_size);
		return GDK_SUCCEED;
	}
	if (errno == ENOENT) {
		// No .vaultkey? Fine.
		return GDK_SUCCEED;
	}

	GDKerror("Error stat'ing %s: %s", path, strerror(errno));
	return GDK_FAIL;
}
static gdk_return
bl_snapshot(stream *plan)
{
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

	ret = snapshot_wal(plan, db_dir);
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
	lf->get_snapshot_files = bl_snapshot;
}
