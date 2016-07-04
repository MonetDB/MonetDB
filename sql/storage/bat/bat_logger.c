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

logger *bat_logger = NULL;
logger *bat_logger_shared = NULL;

static int
bl_preversion( int oldversion, int newversion)
{
#define CATALOG_JUL2015 52200

	(void)newversion;
	if (oldversion == CATALOG_JUL2015) {
		catalog_version = oldversion;
		geomversion_set();
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

	if (catalog_version <= CATALOG_JUL2015) {
		BAT *b;
		BATiter bi;
		BAT *te, *tne;
		BUN p,q;
		char geomUpgrade = 0;
		char *s = "sys", n[64];

		te = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "types_eclass")));
		if (te == NULL)
			return;
		bi = bat_iterator(te);
		tne = COLnew(te->hseqbase, TYPE_int, BATcount(te), PERSISTENT);
		if (!tne)
			return;
		for(p=0, q=BUNlast(te); p<q; p++) {
			int eclass = *(int*)BUNtail(bi, p);

			if (eclass == EC_GEOM)		/* old EC_EXTERNAL */
				eclass++;		/* shift up */
			BUNappend(tne, &eclass, TRUE);
		}
		BATsetaccess(tne, BAT_READ);
		logger_add_bat(lg, tne, N(n, NULL, s, "types_eclass"));
		bat_destroy(te);

		/* in the past, the args.inout column may have been
		 * incorrectly upgraded to a bit instead of a bte
		 * column */
		te = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "args_inout")));
		if (te == NULL)
			return;
		if (te->ttype == TYPE_bit) {
			bi = bat_iterator(te);
			tne = COLnew(te->hseqbase, TYPE_bte, BATcount(te), PERSISTENT);
			if (!tne)
				return;
			for(p=0, q=BUNlast(te); p<q; p++) {
				bte inout = (bte) *(bit*)BUNtail(bi, p);

				BUNappend(tne, &inout, TRUE);
			}
			BATsetaccess(tne, BAT_READ);
			logger_add_bat(lg, tne, N(n, NULL, s, "args_inout"));
		}
		bat_destroy(te);

		/* test whether the catalog contains information
		 * regarding geometry types */
		b = BATdescriptor((bat) logger_find_bat(lg, N(n, NULL, s, "types_systemname")));
		bi = bat_iterator(b);
		for (p=0, q=BUNlast(b); p<q; p++) {
			char *t = toLower(BUNtail(bi, p));
			geomUpgrade = strcmp(t, "wkb") == 0;
			GDKfree(t);
			if (geomUpgrade)
				break;
		}
		bat_destroy(b);

		if (!geomUpgrade) {
			/* test whether the catalog contains
			 * information about geometry columns */
			b = BATdescriptor((bat) logger_find_bat(lg, N(n, NULL, s, "_columns_type")));
			bi = bat_iterator(b);
			for (p=0, q=BUNlast(b); p<q; p++) {
				char *t = toLower(BUNtail(bi, p));
				geomUpgrade = strcmp(t, "point") == 0 ||
					strcmp(t, "curve") == 0 ||
					strcmp(t, "linestring") == 0 ||
					strcmp(t, "surface") == 0 ||
					strcmp(t, "polygon") == 0 ||
					strcmp(t, "multipoint") == 0 ||
					strcmp(t, "multicurve") == 0 ||
					strcmp(t, "multilinestring") == 0 ||
					strcmp(t, "multisurface") == 0 ||
					strcmp(t, "multipolygon") == 0 ||
					strcmp(t, "geometry") == 0 ||
					strcmp(t, "geometrycollection") == 0;
				GDKfree(t);
				if (geomUpgrade)
					break;
			}
			bat_destroy(b);
		}

		if (!geomUpgrade && geomcatalogfix_get() == NULL) {
			/* The catalog knew nothing about geometries
			 * and the geom module is not loaded:
			 * Do nothing */
		} else if (!geomUpgrade && geomcatalogfix_get() != NULL) {
			/* The catalog knew nothing about geometries
			 * but the geom module is loaded:
			 * Add geom functionality */
			(*(geomcatalogfix_get()))(lg, 0);
		} else if (geomUpgrade && geomcatalogfix_get() == NULL) {
			/* The catalog needs to be updated but the
			 * geom module has not been loaded.
			 * The case is prohibited by the sanity check
			 * performed during initialization */
			GDKfatal("the catalogue needs to be updated but the geom module is not loaded.\n");
		} else if (geomUpgrade && geomcatalogfix_get() != NULL) {
			/* The catalog needs to be updated and the
			 * geom module has been loaded */
			(*(geomcatalogfix_get()))(lg, 1);
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
		// FIXME: either of those corrupts stuff
		//logger_exit(l);
		//logger_destroy(l);
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
