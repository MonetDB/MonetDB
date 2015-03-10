/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"
#include "sql_types.h" /* EC_POS */

logger *bat_logger = NULL;

static int
bl_preversion( int oldversion, int newversion)
{
#define CATALOG_OCT2014 52100

	(void)newversion;
	if (oldversion == CATALOG_OCT2014) {
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
	if (catalog_version == CATALOG_OCT2014) {
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
	}
	if (catalog_version == CATALOG_OCT2014) {
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
bl_cleanup(void)
{
	if (bat_logger)
		return logger_cleanup(bat_logger);
	return LOG_OK;
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
