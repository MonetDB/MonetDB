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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

#include "sql_config.h"
#include <windows.h>
#include <stdio.h>
#define ALREADY_HAVE_WINDOWS_TYPE
#include <sql.h>
#include <sqlext.h>
#ifdef EXPORT
#undef EXPORT
#endif
#define EXPORT __declspec(dllexport)
#include <odbcinst.h>

static char *DriverName = "MonetDB ODBC Driver";
static char *DataSourceName = "MonetDB";

static void
ODBCLOG(const char *fmt, ...)
{
	va_list ap;
	char *s = getenv("ODBCDEBUG");

	va_start(ap, fmt);
	if (s && *s) {
		FILE *f;

		f = fopen(s, "a");
		if (f) {
			vfprintf(f, fmt, ap);
			fclose(f);
		} else
			vfprintf(stderr, fmt, ap);
	}
	va_end(ap);
}

BOOL
ConfigDriver(HWND hwnd, WORD request, LPCSTR driver, LPCSTR args, LPSTR msg, WORD msgmax, WORD * msgout)
{
	(void)hwnd;
	ODBCLOG("ConfigDriver %d %s %s\n", request, driver ? driver : "(null)", args ? args : "(null)");

	if (msgout)
		*msgout = 0;
	if (msg && msgmax > 0)
		*msg = 0;
	switch (request) {
	case ODBC_INSTALL_DRIVER:
	case ODBC_REMOVE_DRIVER:
		break;
	case ODBC_CONFIG_DRIVER:
		break;
	default:
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE, "Invalid request");
		return FALSE;
	}
	if (strcmp(driver, DriverName) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
		return FALSE;
	}
	return TRUE;
}

BOOL
ConfigDSN(HWND parent, WORD request, LPCSTR driver, LPCSTR attributes)
{
	(void)parent;
	ODBCLOG("ConfigDSN %d %s %s\n", request, driver ? driver : "(null)", attributes ? attributes : "(null)");

	if (strcmp(driver, DriverName) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
		return FALSE;
	}
	switch (request) {
	case ODBC_ADD_DSN:
		SQLWriteDSNToIni(DataSourceName, driver);
		break;
	case ODBC_CONFIG_DSN:
		break;
	case ODBC_REMOVE_DSN:
		SQLRemoveDSNFromIni(DataSourceName);
		break;
	default:
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE, "Invalid request");
		return FALSE;
	}

	return TRUE;
}

BOOL WINAPI
DllMain(HINSTANCE hinstDLL, DWORD reason, LPVOID reserved)
{
	(void)hinstDLL;
	(void)reserved;
	ODBCLOG("DllMain %d\n", reason);

	return TRUE;
}
