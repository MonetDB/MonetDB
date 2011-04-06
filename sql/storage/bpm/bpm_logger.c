/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "bpm_logger.h"

logger *bpm_logger = NULL;

static int 
bl_create(char *logdir, char *dbname, int catalog_version)
{
	if (bpm_logger)
		return LOG_ERR;
	bpm_logger = logger_create(0, "sql", logdir, dbname, catalog_version, NULL, NULL);
	if (bpm_logger)
		return LOG_OK;
	return LOG_ERR;
}

static void 
bl_destroy(void)
{
	logger *l = bpm_logger;

	bpm_logger = NULL;
	if (l) {
		logger_exit(l);
		logger_destroy(l);
	}
}

static int 
bl_restart(void)
{
	if (bpm_logger)
		return logger_restart(bpm_logger);
	return LOG_OK;
}

static int
bl_cleanup(void)
{
	if (bpm_logger)
		return logger_cleanup(bpm_logger);
	return LOG_OK;
}

static int
bl_changes(void)
{	
	return (int) MIN(logger_changes(bpm_logger), GDK_int_max);
}

static int 
bl_get_sequence(int seq, lng *id)
{
	return logger_sequence(bpm_logger, seq, id);
}

static int
bl_log_isnew(void)
{
	if (BATcount(bpm_logger->catalog) > 10) {
		return 0;
	}
	return 1;
}

static int 
bl_tstart(void)
{
	return log_tstart(bpm_logger);
}

static int 
bl_tend(void)
{
	return log_tend(bpm_logger);
}

static int 
bl_sequence(int seq, lng id)
{
	return log_sequence(bpm_logger, seq, id);
}

int 
bpm_logger_init( logger_functions *lf )
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
