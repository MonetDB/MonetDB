/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "nop_logger.h"

static int
nl_create(int debug, const char *logdir, int cat_version)
{
	(void) debug;
	(void) logdir;
	(void) cat_version;
	return LOG_OK;
}

static void
nl_destroy(void)
{
}

static int
nl_restart(void)
{
	return LOG_OK;
}

static int
nl_cleanup(void)
{
	return LOG_OK;
}

static int
nl_changes(void)
{
	return 0;
}

static int
nl_get_sequence(int seq, lng *id)
{
	(void) seq;
	*id = 42;
	return LOG_OK;
}

static int
nl_log_isnew(void)
{
	return 1;
}

static int
nl_tstart(void)
{
	return LOG_OK;
}

static int
nl_tend(void)
{
	return LOG_OK;
}

static int
nl_sequence(int seq, lng id)
{
	(void) seq;
	(void) id;
	return LOG_OK;
}

void
nop_logger_init( logger_functions *lf )
{
	lf->create = nl_create;
	lf->destroy = nl_destroy;
	lf->restart = nl_restart;
	lf->cleanup = nl_cleanup;
	lf->changes = nl_changes;
	lf->get_sequence = nl_get_sequence;
	lf->log_isnew = nl_log_isnew;
	lf->log_tstart = nl_tstart;
	lf->log_tend = nl_tend;
	lf->log_sequence = nl_sequence;
}
