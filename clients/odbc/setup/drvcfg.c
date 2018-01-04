/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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

#include <drvcfg.h>
#include <string.h>		/* for memset(), memcpy(), strncpy() */

#include <stdlib.h>		/* for malloc() on Darwin */
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

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
	strncpy(lastprop->szName, "Host", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Port", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Database", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "User", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Password", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) calloc(1, sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Debug", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	return 1;
}
