/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "bat_logger.h"
#include "bat_utils.h"

logger *bat_logger = NULL;

static int
bl_preversion( int oldversion, int newversion)
{
#define CATALOG_FEB2013 52001
#define CATALOG_OCT2014 52100

	(void)newversion;
	if (oldversion == CATALOG_FEB2013) {
		catalog_version = oldversion;
		return 0;
	}
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


#ifndef HAVE_STRCASESTR
static const char *
strcasestr(const char *haystack, const char *needle)
{
	const char *p, *np = 0, *startn = 0;

	for (p = haystack; *p; p++) {
		if (np) {
			if (toupper(*p) == toupper(*np)) {
				if (!*++np)
					return startn;
			} else
				np = 0;
		} else if (toupper(*p) == toupper(*needle)) {
			np = needle + 1;
			startn = p;
			if (!*np)
				return startn;
		}
	}

	return 0;
}
#endif

static void 
bl_postversion( void *lg) 
{
	(void)lg;
	if (catalog_version == CATALOG_FEB2013) {
		/* we need to add the new schemas.system column */
		BAT *b, *b1, *b2, *b3, *u, *f, *l;
		BATiter bi, fi, li;
		char *s = "sys", n[64];
		BUN p,q;

		b = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "schemas_name")));
		if (!b)
			return;
		bi = bat_iterator(b);
		b1 = BATnew(TYPE_void, TYPE_bit, BATcount(b), PERSISTENT);
		if (!b1)
			return;
        	BATseqbase(b1, b->hseqbase);
		/* only sys and tmp are system schemas */
		for(p=BUNfirst(b), q=BUNlast(b); p<q; p++) {
			bit v = FALSE;
			char *name = BUNtail(bi, p);
			if (strcmp(name, "sys") == 0 || strcmp(name, "tmp") == 0)
				v = TRUE;
			BUNappend(b1, &v, TRUE);
		}
		BATsetaccess(b1, BAT_READ);
		logger_add_bat(lg, b1, N(n, NULL, s, "schemas_system"));
		bat_destroy(b);
		bat_destroy(b1);

		/* add args.inout (default to ARG_IN) */
		b = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "args_name")));
		if (!b)
			return;
		bi = bat_iterator(b);
		b1 = BATnew(TYPE_void, TYPE_bte, BATcount(b), PERSISTENT);
		if (!b1)
			return;
        	BATseqbase(b1, b->hseqbase);
		/* default to ARG_IN, names starting with 'res' are ARG_OUT */
		bi = bat_iterator(b);
		for(p=BUNfirst(b), q=BUNlast(b); p<q; p++) {
			bte v = ARG_IN;
			char *name = BUNtail(bi, p);
			if (strncmp(name, "res", 3) == 0)
				v = ARG_OUT;
			BUNappend(b1, &v, TRUE);
		}
		BATsetaccess(b1, BAT_READ);
		logger_add_bat(lg, b1, N(n, NULL, s, "args_inout"));
		bat_destroy(b);
		bat_destroy(b1);

		/* add functions.vararg/varres */
		b = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "functions_sql")));
		u = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "functions_type")));
		f = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "functions_func")));
		l = temp_descriptor(logger_find_bat(lg, N(n, NULL, s, "functions_name")));
		fi = bat_iterator(f);
		li = bat_iterator(l);

		if (!b || !u || !f || !l)
			return;
		bi = bat_iterator(b);
		b1 = BATnew(TYPE_void, TYPE_bit, BATcount(b), PERSISTENT);
		b2 = BATnew(TYPE_void, TYPE_bit, BATcount(b), PERSISTENT);
		b3 = BATnew(TYPE_void, TYPE_int, BATcount(b), PERSISTENT);

		if (!b1 || !b2 || !b3)
			return;
        	BATseqbase(b1, b->hseqbase);
        	BATseqbase(b2, b->hseqbase);
        	BATseqbase(b3, b->hseqbase);

		/* default to no variable arguments and results */
		for(p=BUNfirst(b), q=BUNlast(b); p<q; p++) {
			bit v = FALSE, t = TRUE;
			int lang, type = F_UNION;
			char *name = BUNtail(li, p);

			if (strcmp(name, "copyfrom") == 0) {
				/* var in and out, and union func */
				void_inplace(u, p, &type, TRUE);
				BUNappend(b1, &t, TRUE);
				BUNappend(b2, &t, TRUE);

				lang = 0;
				BUNappend(b3, &lang, TRUE);
			} else {
				BUNappend(b1, &v, TRUE);
				BUNappend(b2, &v, TRUE);

				/* this should be value of functions_sql + 1*/
				lang = *(bit*) BUNtloc(bi,p) + 1;
				BUNappend(b3, &lang, TRUE);
			}

			/* beware these will all be drop and recreated in the sql
			 * upgrade code */
			name = BUNtail(fi, p);
			if (strcasestr(name, "RETURNS TABLE") != NULL) 
				void_inplace(u, p, &type, TRUE);
		}
		BATsetaccess(b1, BAT_READ);
		BATsetaccess(b2, BAT_READ);
		BATsetaccess(b3, BAT_READ);

		logger_add_bat(lg, b1, N(n, NULL, s, "functions_vararg"));
		logger_add_bat(lg, b2, N(n, NULL, s, "functions_varres"));
		logger_add_bat(lg, b3, N(n, NULL, s, "functions_language"));

		bat_destroy(b);
		bat_destroy(u);
		bat_destroy(l);

		/* delete functions.sql */
		logger_del_bat(lg, b->batCacheid);

		bat_destroy(b1);
		bat_destroy(b2);
		bat_destroy(b3);
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
