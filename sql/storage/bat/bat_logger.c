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
		/* Prexisting columns of type point, linestring, polygon etc 
		 * have to converted to geometry(0), geometry(1) etc. */
		BAT *ct, *cnt, *cd, *cnd, *cs, *cns;
		BATiter cti, cdi, csi;
		char *s = "sys", n[64];
		BUN p,q;

		ct = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_type")));
		cti = bat_iterator(ct);
		cd = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_type_digits")));
		cdi = bat_iterator(cd);
		cs = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "_columns_type_scale")));
		csi = bat_iterator(cs);

		cnt = BATnew(TYPE_void, TYPE_str, BATcount(ct), PERSISTENT);
		cnd = BATnew(TYPE_void, TYPE_int, BATcount(cd), PERSISTENT);
		cns = BATnew(TYPE_void, TYPE_int, BATcount(cs), PERSISTENT);

		if (!cnt || !cnd || !cns)
			return;
        	BATseqbase(cnt, ct->hseqbase);
		BATseqbase(cnd, cd->hseqbase);
		BATseqbase(cns, cs->hseqbase);

		for(p=BUNfirst(ct), q=BUNlast(ct); p<q; p++) {
			char *type = BUNtail(cti, p);
			int digits = *(int*)BUNtail(cdi, p);
			int scale = *(int*)BUNtail(csi, p);

			if (strcmp(toLower(type), "point") == 0) {
				type = "geometry";
				digits = wkbPoint;
				scale = 0; // in the past we did not save the srid
			} else if (strcmp(toLower(type), "linestring") == 0) {
				type = "geometry";
				digits = wkbLineString;
				scale = 0;
			} else if (strcmp(toLower(type), "linearring") == 0) {
				type = "geometry";
				digits = wkbLinearRing;
				scale = 0;
			} else if (strcmp(toLower(type), "polygon") == 0) {
				type = "geometry";
				digits = wkbPolygon;
				scale = 0;
			} else if (strcmp(toLower(type), "multipoint") == 0) {
				type = "geometry";
				digits = wkbMultiPoint;
				scale = 0;
			} else if (strcmp(toLower(type), "multilinestring") == 0) {
				type = "geometry";
				digits = wkbMultiLineString;
				scale = 0;
			} else if (strcmp(toLower(type), "multipolygon") == 0) {
				type = "geometry";
				digits = wkbMultiPolygon;
				scale = 0;
			} else if (strcmp(toLower(type), "geometrycollection") == 0) {
				type = "geometry";
				digits = wkbGeometryCollection;
				scale = 0;
			}

			BUNappend(cnt, type, TRUE);
			BUNappend(cnd, &digits, TRUE);
			BUNappend(cns, &scale, TRUE);
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
