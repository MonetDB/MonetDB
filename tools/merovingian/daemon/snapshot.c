/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include <stdio.h>

#include "monetdb_config.h"
#include "msabaoth.h"
#include "merovingian.h"
#include "mapi.h"
#include "snapshot.h"

/* Create a snapshot of database dbname to file dest.
 * TODO: verify that dest is a safe location.
 * TODO: Make it work for databases without monetdb/monetdb root account.
 */
err
snapshot_adhoc(char *dbname, char *dest)
{
	err e = NO_ERR;
	sabdb *stats = NULL;
	int port = -1;
	Mapi conn = NULL;
	MapiHdl handle = NULL;


	/* First look up the database in our administration. */
	e = msab_getStatus(&stats, dbname);
	if (e) {
		goto bailout;
	}
	if (!stats) {
		e = newErr("No such database: '%s'", dbname);
		goto bailout;
	}

	/* Connect. This is a dirty hack, making two assumptions:
	 * 1. we're listening on localhost
	 * 2. the database has a root user 'monetdb' with password 'monetdb'.
	 */
	port = getConfNum(_mero_props, "port");
	conn = mapi_connect("localhost", port, "monetdb", "monetdb", "sql", dbname);
	if (conn == NULL || mapi_error(conn)) {
		e = newErr("connection error: %s", mapi_error_str(conn));
		goto bailout;
	}

	/* Trigger the snapshot */
	handle = mapi_prepare(conn, "CALL sys.hot_snapshot(?)");
	if (handle == NULL || mapi_error(conn)) {
		e = newErr("prepare failed: %s", mapi_error_str(conn));
		goto bailout;
	}
	if (mapi_param_string(handle, 0, 12, dest, NULL) != MOK) {
		e = newErr("internal error: mapi_param_string: %s", mapi_error_str(conn));
		goto bailout;
	}
	if (mapi_execute(handle) != MOK) {
		e = newErr("internal error: execute failed: %s", mapi_result_error(handle));
		goto bailout;
	}

bailout:
	if (handle != NULL)
		mapi_close_handle(handle);
	if (conn != NULL)
		mapi_destroy(conn);
	if (stats != NULL)
		msab_freeStatus(&stats);
	return e;
}