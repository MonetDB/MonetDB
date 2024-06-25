/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 *
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

/**********************************************
 * ODBCDbc.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC connection structures/objects (see ODBCDbc.h).
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCAttrs.h"

#define ODBC_DBC_MAGIC_NR  1365	/* for internal sanity check only */

static const char*
odbc_parm_localizer(const void *data, mparm parm)
{
	(void)data;
	for (int i = 0; i < attr_setting_count; i++) {
		const struct attr_setting *entry = &attr_settings[i];
		if (entry->parm == parm)
			return entry->name;
	}
	return NULL;
}

/*
 * Creates a new allocated ODBCDbc object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCDbc object
 */
ODBCDbc *
newODBCDbc(ODBCEnv *env)
{
	ODBCDbc *dbc;
	msettings *settings;

	assert(env);

	dbc = (ODBCDbc *) malloc(sizeof(ODBCDbc));
	settings = msettings_create();

	if (dbc == NULL || settings == NULL) {
		free(dbc);
		msettings_destroy(settings);
		/* Memory allocation error */
		addEnvError(env, "HY001", NULL, 0);
		return NULL;
	}

	msettings_set_localizer(settings, odbc_parm_localizer, NULL);

	*dbc = (ODBCDbc) {
		.Env = env,
		.settings = settings,
		.sql_attr_autocommit = SQL_AUTOCOMMIT_ON,	/* default is autocommit */
		.sql_attr_metadata_id = SQL_FALSE,
		/* add this dbc to start of the administrative linked dbc list */
		.next = env->FirstDbc,
		.Type = ODBC_DBC_MAGIC_NR,	/* set it valid */
		.setting_touched = { 0 },
	};
	env->FirstDbc = dbc;
	return dbc;
}


/*
 * Check if the connection handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid connection handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int
isValidDbc(ODBCDbc *dbc)
{
#ifdef ODBCDEBUG
	if (!(dbc && dbc->Type == ODBC_DBC_MAGIC_NR))
		ODBCLOG("dbc %p: not a valid connection handle\n", dbc);
#endif
	return dbc && dbc->Type == ODBC_DBC_MAGIC_NR;
}



/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCDbc struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: dbc must be valid. SQLState and errMsg may be NULL.
 */
void
addDbcError(ODBCDbc *dbc, const char *SQLState, const char *errMsg, int nativeErrCode)
{
	ODBCError *error = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("addDbcError %p %s %s %d\n", dbc, SQLState, errMsg ? errMsg : getStandardSQLStateMsg(SQLState), nativeErrCode);
#endif
	assert(isValidDbc(dbc));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	appendODBCError(&dbc->Error, error);
}


/*
 * Extracts an error object from the error list of this ODBCDbc struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: dbc and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *
getDbcError(ODBCDbc *dbc)
{
	assert(isValidDbc(dbc));
	return dbc->Error;
}


/*
 * Destroys the ODBCDbc object including its own managed data.
 *
 * Precondition: dbc must be valid, inactive (not connected) and
 * no ODBCStmt (or ODBCDesc) objects may refer to this dbc.
 * Postcondition: dbc is completely destroyed, dbc handle is become invalid.
 */
void
destroyODBCDbc(ODBCDbc *dbc)
{
	assert(isValidDbc(dbc));
	assert(!dbc->Connected);
	assert(dbc->FirstStmt == NULL);

	/* first set this object to invalid */
	dbc->Type = 0;

	/* remove this dbc from the env */
	assert(dbc->Env);
	assert(dbc->Env->FirstDbc);
	{
		/* search for this dbc in the list */
		ODBCDbc *tmp_dbc = (ODBCDbc *) dbc->Env->FirstDbc;
		ODBCDbc *prv_dbc = NULL;

		while ((tmp_dbc != NULL) && (tmp_dbc != dbc)) {
			prv_dbc = tmp_dbc;
			tmp_dbc = tmp_dbc->next;
		}

		assert(tmp_dbc == dbc);	/* we must have found it */

		/* now remove it from the linked list */
		if (prv_dbc != NULL) {
			prv_dbc->next = dbc->next;
		} else {
			dbc->Env->FirstDbc = dbc->next;
		}
	}

	/* cleanup own managed data */
	deleteODBCErrorList(&dbc->Error);
	msettings_destroy(dbc->settings);
	free(dbc->dsn);

	free(dbc);
}
