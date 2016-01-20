/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */
#include "libgeom.h"

logger *bat_logger = NULL;
logger *bat_logger_shared = NULL;

static int
bl_preversion( int oldversion, int newversion)
{
#define CATALOG_OCT2014 52100
#define CATALOG_OCT2014SP3 52101
#define CATALOG_JUL2015 52200

	(void)newversion;
	if (oldversion == CATALOG_OCT2014SP3) {
		catalog_version = oldversion;
		return 0;
	}
	if (oldversion == CATALOG_OCT2014) {
		catalog_version = oldversion;
		return 0;
	}
	if (oldversion == CATALOG_JUL2015) {
		catalog_version = oldversion;
		return 0;
	}

	return -1;
}

static char *
N( char *buf, char *pre, char *schema, char *post)
{
	if (pre)
		snprintf(buf, 64, "%s_%s_%s", pre, schema, post);
	else
		snprintf(buf, 64, "%s_%s", schema, post);
	return buf;
}

static void 
bl_postversion( void *lg) 
{
	(void)lg;
	if (catalog_version <= CATALOG_OCT2014) {
		BAT *te, *tn, *tne;
		BATiter tei, tni;
		char *s = "sys", n[64];
		BUN p,q;

		te = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "types_eclass")));
		tn = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "types_sqlname")));
		if (!te || !tn)
			return;
		tei = bat_iterator(te);
		tni = bat_iterator(tn);
		tne = BATnew(TYPE_void, TYPE_int, BATcount(te), PERSISTENT);
		if (!tne)
			return;
        	BATseqbase(tne, te->hseqbase);
		for(p=BUNfirst(te), q=BUNlast(te); p<q; p++) {
			int eclass = *(int*)BUNtail(tei, p);
			char *name = BUNtail(tni, p);

			if (eclass == EC_POS)		/* old EC_NUM */
				eclass = strcmp(name, "oid") == 0 ? EC_POS : EC_NUM;
			else if (eclass == EC_NUM)	/* old EC_INTERVAL */
				eclass = strcmp(name, "sec_interval") == 0 ? EC_SEC : EC_MONTH;
			else if (eclass >= EC_MONTH)	/* old EC_DEC */
				eclass += 2;
			BUNappend(tne, &eclass, TRUE);
		}
		BATsetaccess(tne, BAT_READ);
		logger_add_bat(lg, tne, N(n, NULL, s, "types_eclass"));
		bat_destroy(te);
		bat_destroy(tn);
	} else if (catalog_version == CATALOG_OCT2014SP3) {
		BAT *te, *tn, *tne;
		BATiter tei, tni;
		char *s = "sys", n[64];
		BUN p,q;

		te = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "types_eclass")));
		tn = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "types_sqlname")));
		if (!te || !tn)
			return;
		tei = bat_iterator(te);
		tni = bat_iterator(tn);
		tne = BATnew(TYPE_void, TYPE_int, BATcount(te), PERSISTENT);
		if (!tne)
			return;
        	BATseqbase(tne, te->hseqbase);
		for(p=BUNfirst(te), q=BUNlast(te); p<q; p++) {
			int eclass = *(int*)BUNtail(tei, p);
			char *name = BUNtail(tni, p);

			if (eclass == EC_MONTH)		/* old EC_INTERVAL */
				eclass = strcmp(name, "sec_interval") == 0 ? EC_SEC : EC_MONTH;
			else if (eclass >= EC_SEC)	/* old EC_DEC */
				eclass += 1;
			BUNappend(tne, &eclass, TRUE);
		}
		BATsetaccess(tne, BAT_READ);
		logger_add_bat(lg, tne, N(n, NULL, s, "types_eclass"));
		bat_destroy(te);
		bat_destroy(tn);
	}
	if (catalog_version == CATALOG_OCT2014 ||
	    catalog_version == CATALOG_OCT2014SP3) {
		/* we need to replace tables.readonly by tables.access column */
		BAT *b, *b1;
		BATiter bi;
		char *s = "sys", n[64];
		BUN p,q;

		while(s) {
			b = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_tables_readonly")));
			if (!b)
				return;
			bi = bat_iterator(b);
			b1 = BATnew(TYPE_void, TYPE_sht, BATcount(b), PERSISTENT);
			if (!b1)
				return;
        		BATseqbase(b1, b->hseqbase);

			bi = bat_iterator(b);
			for(p=BUNfirst(b), q=BUNlast(b); p<q; p++) {
				bit ro = *(bit*)BUNtail(bi, p);
				sht access = 0;
				if (ro)
					access = TABLE_READONLY;
				BUNappend(b1, &access, TRUE);
			}
			BATsetaccess(b1, BAT_READ);
			logger_add_bat(lg, b1, N(n, NULL, s, "_tables_access"));
			/* delete functions.sql */
			logger_del_bat(lg, b->batCacheid);
			bat_destroy(b);
			bat_destroy(b1);
			if (strcmp(s,"sys")==0)
				s = "tmp";
			else
				s = NULL;
		}
	}
	if (catalog_version <= CATALOG_JUL2015) {
		/* Do the updates needed for the new geom module */
		BAT *ct, *cnt, *cd, *cnd, *cs, *cns, *cn, *ctid, *ti, *tn, *ts, *si, *sn, *g;
		BATiter cti, cdi, csi, cni, ctidi, tsi, tni, sni, gi;
		char *s = "sys", n[64];
		BUN p,q;
		char *nt[] = {"types_id", "types_systemname", "types_sqlname", "types_digits", "types_scale", "types_radix", "types_eclass", "types_schema_id"};
		unsigned char ntt[] = {TYPE_int, TYPE_str, TYPE_str, TYPE_int, TYPE_int, TYPE_int, TYPE_int, TYPE_int};
		char *nf[] = {"functions_id", "functions_name", "functions_func", "functions_mod", "functions_language", "functions_type", "functions_side_effect", "functions_varres", "functions_vararg", "functions_schema_id"};
		unsigned char nft[] = {TYPE_int, TYPE_str, TYPE_str, TYPE_str, TYPE_int, TYPE_int, TYPE_bit, TYPE_bit, TYPE_bit, TYPE_int};
		BAT *tt[8], *ttn[8], *ff[10], *ffn[10];
		BATiter tti[8], ffi[10];
		int val, maxid;
		bit bval;

		/* Update the catalog to use the new geometry types */
		ct = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_type")));
		cti = bat_iterator(ct);
		cd = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_type_digits")));
		cdi = bat_iterator(cd);
		cs = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_type_scale")));
		csi = bat_iterator(cs);
		cn = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_name")));
		cni = bat_iterator(cn);
		ctid = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_table_id")));
		ctidi = bat_iterator(ctid);
		ti = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_tables_id")));
		tn = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_tables_name")));
		tni = bat_iterator(tn);
		ts = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_tables_schema_id")));
		tsi = bat_iterator(ts);
		si = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "schemas_id")));
		sn = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "schemas_name")));
		sni = bat_iterator(sn);

		cnt = BATnew(TYPE_void, TYPE_str, BATcount(ct), PERSISTENT);
		cnd = BATnew(TYPE_void, TYPE_int, BATcount(cd), PERSISTENT);
		cns = BATnew(TYPE_void, TYPE_int, BATcount(cs), PERSISTENT);

		if (!cnt || !cnd || !cns || !ct || !cd || !cs || 
		    !cn || !ctid || !ti || !tn || !ts || !si || !sn)
			return;
        	BATseqbase(cnt, ct->hseqbase);
		BATseqbase(cnd, cd->hseqbase);
		BATseqbase(cns, cs->hseqbase);

		for(p=BUNfirst(ct), q=BUNlast(ct); p<q; p++) {
			bool isGeom = false;
			char *type = BUNtail(cti, p);
			int digits = *(int*)BUNtail(cdi, p);
			int scale = *(int*)BUNtail(csi, p);
			char* colname = BUNtail(cni, p);
			
			if (strcmp(toLower(type), "point") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbPoint;
				scale = 0; // in the past we did not save the srid
			} else if (strcmp(toLower(type), "linestring") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbLineString;
				scale = 0;
			} else if (strcmp(toLower(type), "curve") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbLineString;
				scale = 0;
			} else if (strcmp(toLower(type), "linearring") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbLinearRing;
				scale = 0;
			} else if (strcmp(toLower(type), "polygon") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbPolygon;
				scale = 0;
			} else if (strcmp(toLower(type), "surface") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbPolygon;
				scale = 0;
			} else if (strcmp(toLower(type), "multipoint") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbMultiPoint;
				scale = 0;
			} else if (strcmp(toLower(type), "multilinestring") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbMultiLineString;
				scale = 0;
			} else if (strcmp(toLower(type), "multicurve") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbMultiLineString;
				scale = 0;
			} else if (strcmp(toLower(type), "multipolygon") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbMultiPolygon;
				scale = 0;
			} else if (strcmp(toLower(type), "multisurface") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbMultiPolygon;
				scale = 0;
			} else if (strcmp(toLower(type), "geomcollection") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbGeometryCollection;
				scale = 0;
			} else if (strcmp(toLower(type), "geometrycollection") == 0) {
				type = "geometry";
				isGeom = true;
				digits = wkbGeometryCollection;
				scale = 0;
			}  else if (strcmp(toLower(type), "geometry") == 0) {
				type = "geometry";
				isGeom = true;
				digits = 0;
				scale = 0;
			} 

			BUNappend(cnt, type, TRUE);
			BUNappend(cnd, &digits, TRUE);
			BUNappend(cns, &scale, TRUE);

			/* The wkb struct has changed. Update the respective BATs */
			if (isGeom) {
				typedef struct wkb_old {int len; char data[1];} wkb_old;
				BAT *gn;
				BUN k,l;
				int table_id, schema_id;
				char *sn, *tblname;

				table_id = *(int*)BUNtail(ctidi, p);
				if ((k = BUNfnd(ti, &table_id)) == BUN_NONE)
					return;
				tblname = BUNtail(tni, k);
				schema_id = *(int*)BUNtail(tsi, k);
				if ((k = BUNfnd(si, &schema_id)) == BUN_NONE)
					return;
				sn = BUNtail(sni, k);
				g = temp_descriptor(logger_find_bat(lg, N(n, sn, tblname, colname)));
				gi = bat_iterator(g);
				gn = BATnew(TYPE_void, ATOMindex("wkb"), BATcount(g), PERSISTENT);
				if (!gn)
					return;
				BATseqbase(gn, g->hseqbase);
				for(k=BUNfirst(g), l=BUNlast(g); k<l; k++) {
					wkb_old *wo = (wkb_old*)BUNtail(gi, k);
					wkb *wn;
					if (wo->len == ~0) {
						wn = GDKmalloc(sizeof(wkb));
						wn->len = ~(int)0;
						(wn->data)[0] = 0;
					} else {
						wn  = GDKmalloc(sizeof(wkb) - 1 + wo->len);
						wn->len = wo->len;
						memcpy(wn->data, wo->data, wn->len);
					}
					wn->srid = 0;// we did not save the srid in the past
					BUNappend(gn, wn, TRUE);
				}
				BATsetaccess(gn, BAT_READ);
				logger_add_bat(lg, gn, N(n, sn, tblname, colname));
				bat_destroy(g);
			}
		}

		BATsetaccess(cnt, BAT_READ);
		BATsetaccess(cnd, BAT_READ);
		BATsetaccess(cns, BAT_READ);

		logger_add_bat(lg, cnt, N(n, NULL, s, "_columns_type"));
		logger_add_bat(lg, cnd, N(n, NULL, s, "_columns_type_digits"));
		logger_add_bat(lg, cns, N(n, NULL, s, "_columns_type_scale"));

		bat_destroy(ct);
		bat_destroy(cd);
		bat_destroy(cs);
		bat_destroy(cn);
		bat_destroy(ctid);
		bat_destroy(ti);
		bat_destroy(ts);
		bat_destroy(tn);
		bat_destroy(si);
		bat_destroy(sn);

		/* Add the new geometrya type and update the mbr type */
		for (int i = 0; i < 8; i++) {
			if (!(tt[i] = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, nt[i])))))
				return;
			tti[i] = bat_iterator(tt[i]);
			if (!(ttn[i] = BATnew(TYPE_void, ntt[i], BATcount(tt[i]), PERSISTENT)))
				return;
			BATseqbase(ttn[i], tt[i]->hseqbase);
		}
		maxid = 0;
		for(p=BUNfirst(tt[0]), q=BUNlast(tt[0]); p<q; p++) {
			char *systemname = BUNtail(tti[1], p);
			char *sqlname = BUNtail(tti[2], p);
			for (int i = 0; i <= 5; i++)
				BUNappend(ttn[i], BUNtail(tti[i], p), TRUE);
			if (strcmp(systemname, "mbr") == 0) {
				val = EC_EXTERNAL;
				BUNappend(ttn[6], &val, TRUE);
				val = 0;
				BUNappend(ttn[7], &val, TRUE); // the new types use schema_id=0
			} else if (strcmp(systemname, "wkb") == 0) {
				val = EC_GEOM;
				BUNappend(ttn[6], &val, TRUE);
				if (strcmp(sqlname, "geometry") == 0 ) {
					val = 0;
					BUNappend(ttn[7], &val, TRUE); // the new types use schema_id=0
				} else
					BUNappend(ttn[7], BUNtail(tti[7], p), TRUE);
			} else {
				BUNappend(ttn[6], BUNtail(tti[6], p), TRUE);
				BUNappend(ttn[7], BUNtail(tti[7], p), TRUE);
			}
			maxid = maxid < *(int*)BUNtail(tti[0], p) ? *(int*)BUNtail(tti[0], p) : maxid;
		}

		val = ++maxid;
		BUNappend(ttn[0], &val, TRUE);
		BUNappend(ttn[1], "wkba", TRUE);
		BUNappend(ttn[2], "geometrya", TRUE);
		val = 0; BUNappend(ttn[3], &val, TRUE);
		val = 0; BUNappend(ttn[4], &val, TRUE);
		val = 0; BUNappend(ttn[5], &val, TRUE);
		val = EC_EXTERNAL; BUNappend(ttn[6], &val, TRUE);
		val = 0; BUNappend(ttn[7], &val, TRUE); // the new types use schema_id=0

		for (int i = 0; i < 8; i++) {
			BATsetaccess(ttn[i], BAT_READ);
			logger_add_bat(lg, ttn[i], N(n, NULL, s, nt[i]));
			bat_destroy(tt[i]);
		}

		/* Add the new functions */
		for (int i = 0; i < 10; i++) {
			if (!(ff[i] = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, nf[i])))))
				return;
			ffi[i] = bat_iterator(ff[i]);
			if (!(ffn[i] = BATnew(TYPE_void, nft[i], BATcount(ff[i]), PERSISTENT)))
				return;
			BATseqbase(ffn[i], ff[i]->hseqbase);
		}
		maxid = 0;
		for(p=BUNfirst(ff[0]), q=BUNlast(ff[0]); p<q; p++) {
			for (int i = 0; i < 10; i++)
				BUNappend(ffn[i], BUNtail(ffi[i], p), TRUE);
			maxid = maxid < *(int*)BUNtail(ffi[0], p) ? *(int*)BUNtail(ffi[0], p) : maxid;
		}

#define GEOM_UPGRADE_STORE_FUNC(ba, id, name, mod, sqlname) 	\
	do {							\
		val = id;					\
		BUNappend(ba[0], &val, TRUE);			\
		BUNappend(ba[1], name, TRUE);			\
		BUNappend(ba[2], sqlname, TRUE);		\
		BUNappend(ba[3], mod, TRUE);			\
		val = 0; BUNappend(ba[4], &val, TRUE);		\
		val = 1; BUNappend(ba[5], &val, TRUE);		\
		bval = false; BUNappend(ba[6], &bval, TRUE);	\
		bval = false; BUNappend(ba[7], &bval, TRUE);	\
		bval = false; BUNappend(ba[8], &bval, TRUE);	\
		val = 0; BUNappend(ba[9], &val, TRUE);		\
	} while (0)
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap", "geom", "mbrOverlaps");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap", "geom", "mbrOverlaps");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_above", "geom", "mbrAbove");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_above", "geom", "mbrAbove");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_below", "geom", "mbrBelow");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_below", "geom", "mbrBelow");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_right", "geom", "mbrRight");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_right", "geom", "mbrRight");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_left", "geom", "mbrLeft");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_left", "geom", "mbrLeft");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_contains", "geom", "mbrContains");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_contains", "geom", "mbrContains");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_contained", "geom", "mbrContained");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_contained", "geom", "mbrContained");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_equal", "geom", "mbrEqual");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_equal", "geom", "mbrEqual");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_distance", "geom", "mbrDistance");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "mbr_distance", "geom", "mbrDistance");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "left_shift", "geom", "mbrLeft");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "left_shift", "geom", "mbrLeft");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "right_shift", "geom", "mbrRight");
		GEOM_UPGRADE_STORE_FUNC(ffn, ++maxid, "right_shift", "geom", "mbrRight");
#undef GEOM_UPGRADE_STORE_FUNC
		for (int i = 0; i < 10; i++) {
			BATsetaccess(ffn[i], BAT_READ);
			logger_add_bat(lg, ffn[i], N(n, NULL, s, nf[i]));
			bat_destroy(ff[i]);
		}
	}
}

static int 
bl_create(int debug, const char *logdir, int cat_version, int keep_persisted_log_files)
{
	if (bat_logger)
		return LOG_ERR;
	bat_logger = logger_create(debug, "sql", logdir, cat_version, bl_preversion, bl_postversion, keep_persisted_log_files);
	if (bat_logger)
		return LOG_OK;
	return LOG_ERR;
}

static int
bl_create_shared(int debug, const char *logdir, int cat_version, const char *local_logdir)
{
	if (bat_logger_shared)
		return LOG_ERR;
	bat_logger_shared = logger_create_shared(debug, "sql", logdir, local_logdir, cat_version, bl_preversion, bl_postversion);
	if (bat_logger_shared)
		return LOG_OK;
	return LOG_ERR;
}

static void 
bl_destroy(void)
{
	logger *l = bat_logger;

	bat_logger = NULL;
	if (l) {
		logger_exit(l);
		logger_destroy(l);
	}
}

static void
bl_destroy_shared(void)
{
	logger *l = bat_logger_shared;

	bat_logger_shared = NULL;
	if (l) {
		logger_exit(l);
		logger_destroy(l);
	}
}

static int 
bl_restart(void)
{
	if (bat_logger)
		return logger_restart(bat_logger);
	return LOG_OK;
}

static int
bl_cleanup(int keep_persisted_log_files)
{
	if (bat_logger)
		return logger_cleanup(bat_logger, keep_persisted_log_files);
	return LOG_OK;
}

static int
bl_cleanup_shared(int keep_persisted_log_files)
{
	if (bat_logger_shared)
		return logger_cleanup(bat_logger_shared, keep_persisted_log_files);
	return LOG_OK;
}

static int
bl_changes(void)
{	
	return (int) MIN(logger_changes(bat_logger), GDK_int_max);
}

static lng
bl_read_last_transaction_id_shared(void)
{
	return logger_read_last_transaction_id(bat_logger_shared, bat_logger_shared->dir, LOGFILE, bat_logger_shared->dbfarm_role);
}

static lng
bl_get_transaction_drift_shared(void)
{
	lng res = bl_read_last_transaction_id_shared();
	if (res != LOG_ERR) {
		return MIN(res, GDK_int_max) - MIN(bat_logger_shared->id, GDK_int_max);
	}
	return res;
}

static int 
bl_get_sequence(int seq, lng *id)
{
	return logger_sequence(bat_logger, seq, id);
}

static int
bl_get_sequence_shared(int seq, lng *id)
{
	return logger_sequence(bat_logger_shared, seq, id);
}

static int
bl_log_isnew(void)
{
	if (BATcount(bat_logger->catalog_bid) > 10) {
		return 0;
	}
	return 1;
}

static int
bl_log_isnew_shared(void)
{
	if (BATcount(bat_logger_shared->catalog_bid) > 10) {
		return 0;
	}
	return 1;
}

static int 
bl_tstart(void)
{
	return log_tstart(bat_logger);
}

static int 
bl_tend(void)
{
	return log_tend(bat_logger);
}

static int 
bl_sequence(int seq, lng id)
{
	return log_sequence(bat_logger, seq, id);
}

static int
bl_reload_shared(void)
{
	return logger_reload(bat_logger_shared);
}

int 
bat_logger_init( logger_functions *lf )
{
	lf->create = bl_create;
	lf->destroy = bl_destroy;
	lf->restart = bl_restart;
	lf->cleanup = bl_cleanup;
	lf->changes = bl_changes;
	lf->get_sequence = bl_get_sequence;
	lf->log_isnew = bl_log_isnew;
	lf->log_tstart = bl_tstart;
	lf->log_tend = bl_tend;
	lf->log_sequence = bl_sequence;
	return LOG_OK;
}

int
bat_logger_init_shared( logger_functions *lf )
{
	lf->create_shared = bl_create_shared;
	lf->destroy = bl_destroy_shared;
	lf->cleanup = bl_cleanup_shared;
	lf->get_sequence = bl_get_sequence_shared;
	lf->read_last_transaction_id = bl_read_last_transaction_id_shared;
	lf->get_transaction_drift = bl_get_transaction_drift_shared;
	lf->log_isnew = bl_log_isnew_shared;
	lf->reload = bl_reload_shared;
	return LOG_OK;
}
