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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
	lastprop->pNext = (HODBCINSTPROPERTY) malloc(sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	memset(lastprop, 0, sizeof(ODBCINSTPROPERTY));
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_COMBOBOX;
	lastprop->aPromptData = malloc(sizeof(aHost));
	memcpy(lastprop->aPromptData, aHost, sizeof(aHost));
	strncpy(lastprop->szName, "Host", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) malloc(sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	memset(lastprop, 0, sizeof(ODBCINSTPROPERTY));
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Port", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) malloc(sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	memset(lastprop, 0, sizeof(ODBCINSTPROPERTY));
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Database", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) malloc(sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	memset(lastprop, 0, sizeof(ODBCINSTPROPERTY));
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "User", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) malloc(sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	memset(lastprop, 0, sizeof(ODBCINSTPROPERTY));
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Password", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	lastprop->pNext = (HODBCINSTPROPERTY) malloc(sizeof(ODBCINSTPROPERTY));
	lastprop = lastprop->pNext;
	memset(lastprop, 0, sizeof(ODBCINSTPROPERTY));
	lastprop->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
	strncpy(lastprop->szName, "Debug", INI_MAX_PROPERTY_NAME);
	strncpy(lastprop->szValue, "", INI_MAX_PROPERTY_VALUE);

	return 1;
}
