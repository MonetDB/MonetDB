/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/**************************************************
 *
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 18.FEB.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/

#include "monetdb_config.h"

#include "drvcfg.h"
#include <string.h>		/* for memset(), memcpy(), strncpy() */
#include "mstring.h"

static const char *aHost[] = {
	"localhost",
	NULL
};


int
ODBCINSTGetProperties(HODBCINSTPROPERTY lastprop)
{
	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_COMBOBOX;
	lastprop->aPromptData = malloc(sizeof(aHost));
	memcpy(lastprop->aPromptData, aHost, sizeof(aHost));
	strtcpy(lastprop->szName, "Host", sizeof(lastprop->szName));
	strtcpy(lastprop->szValue, "", sizeof(lastprop->szValue));

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strtcpy(lastprop->szName, "Port", sizeof(lastprop->szName));
	strtcpy(lastprop->szValue, "", sizeof(lastprop->szValue));

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strtcpy(lastprop->szName, "Database", sizeof(lastprop->szName));
	strtcpy(lastprop->szValue, "", sizeof(lastprop->szValue));

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strtcpy(lastprop->szName, "User", sizeof(lastprop->szName));
	strtcpy(lastprop->szValue, "", sizeof(lastprop->szValue));

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strtcpy(lastprop->szName, "Password", sizeof(lastprop->szName));
	strtcpy(lastprop->szValue, "", sizeof(lastprop->szValue));

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strtcpy(lastprop->szName, "Debug", sizeof(lastprop->szName));
	strtcpy(lastprop->szValue, "", sizeof(lastprop->szValue));

	return 1;
}
