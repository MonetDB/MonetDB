/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "ODBCGlobal.h"
#include <winver.h>
#include <shlwapi.h>
#include <string.h>

#ifdef __MINGW32__
#define DLL "-0.dll"
#else 
#define DLL ".dll"
#endif

static char *DriverName = "MonetDB ODBC Driver";
static char *DataSourceName = "MonetDB";
static char *DriverDLL = "libMonetODBC" DLL;
static char *DriverDLLs = "libMonetODBCs" DLL;

/* General error handler for installer functions */

static BOOL
ProcessSQLErrorMessages(const char *func)
{
	WORD errnr = 1;
	DWORD errcode;
	char errmsg[300];
	WORD errmsglen;
	int rc;
	BOOL func_rc = FALSE;

	do {
		errmsg[0] = '\0';
		rc = SQLInstallerError(errnr, &errcode,
				       errmsg, sizeof(errmsg), &errmsglen);
		if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
			MessageBox(NULL, errmsg, func,
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			func_rc = TRUE;
		}
		errnr++;
	} while (rc != SQL_NO_DATA);
	return func_rc;
}

static void
ProcessSysErrorMessage(DWORD err, const char *func)
{
	char *lpMsgBuf;

	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
		      NULL, err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		      (LPTSTR) & lpMsgBuf, 0, NULL);
	MessageBox(NULL, (LPCTSTR) lpMsgBuf, func, MB_OK | MB_ICONINFORMATION);
	LocalFree(lpMsgBuf);
}

int 
CheckIfFileExists(const char *filepath, const char *filename)
{
	char buf[300];
	LPTSTR b;

	return SearchPath(filepath, filename, NULL, sizeof(buf), buf, &b) > 0;
}

static BOOL
InstallMyDriver(const char *driverpath)
{
	char driver[300];
	char outpath[301];
	WORD outpathlen;
	DWORD usagecount;
	char *p;

	/* the correct format of driver keywords are
	 * "DriverName\0Driver=...\xxxxxx.DLL\0Setup=...\xxxxxx.DLL\0\0" */

	snprintf(driver, sizeof(driver),
		 "%s;Driver=%s\\%s;Setup=%s\\%s;APILevel=1;"
		 "ConnectFunctions=YYY;DriverODBCVer=%s;SQLLevel=3;",
		 DriverName, driverpath, DriverDLL, driverpath, DriverDLLs,
		 MONETDB_ODBC_VER);

	for (p = driver; *p; p++)
		if (*p == ';')
			*p = '\0';

	/* call SQLInstallDriverEx to install the driver in the
	 * registry */
	if (!SQLInstallDriverEx(driver, driverpath,
				outpath, sizeof(outpath), &outpathlen,
				ODBC_INSTALL_COMPLETE, &usagecount) &&
	    ProcessSQLErrorMessages("SQLInstallDriverEx"))
		return FALSE;

	return TRUE;
}

static BOOL
RemoveMyDriver()
{
	char buf[300];
	DWORD usagecount;
	DWORD valtype, valsize, rc;

	/* most of this is equivalent to what SQLRemoveDriver is
	   suppposed to do, except that it consistently causes a
	   crash, so we do it ourselves */
	snprintf(buf, sizeof(buf), "SOFTWARE\\ODBC\\ODBCINST.INI\\%s",
		 DriverName);
	valsize = sizeof(usagecount);
	usagecount = 0;
	valtype = REG_DWORD;
	rc = SHGetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount",
			&valtype, &usagecount, &valsize);
	if (rc == ERROR_FILE_NOT_FOUND) {
		/* not installed, do nothing */
		exit(0);
	}
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "one");
		return FALSE;
	}
	if (usagecount > 1) {
		usagecount--;
		rc = SHSetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount",
				REG_DWORD, &usagecount, sizeof(usagecount));
		if (rc != ERROR_SUCCESS) {
			ProcessSysErrorMessage(rc, "two");
			return FALSE;
		}
		return TRUE;
	}
	rc = SHDeleteKey(HKEY_LOCAL_MACHINE, buf);
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "three");
		return FALSE;
	}
	rc = SHDeleteValue(HKEY_LOCAL_MACHINE,
			   "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers",
			   DriverName);
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "four");
		return FALSE;
	}

	return TRUE;
}

static void
CreateAttributeString(char *attrs, size_t len)
{
	snprintf(attrs, len,
		 "DSN=%s;Server=localhost;Database=;UID=monetdb;PWD=monetdb;",
		 DataSourceName);

	for (; *attrs; attrs++)
		if (*attrs == ';')
			*attrs = '\0';
}

static BOOL
AddMyDSN()
{
	char attrs[200];

	CreateAttributeString(attrs, sizeof(attrs));

	/* I choose to remove the DSN if it already existed */
	SQLConfigDataSource(NULL, ODBC_REMOVE_SYS_DSN, DriverName, attrs);

	/* then create a new DSN */
	if (!SQLConfigDataSource(NULL, ODBC_ADD_SYS_DSN, DriverName, attrs) &&
	    ProcessSQLErrorMessages("SQLConfigDataSource"))
		return FALSE;

	return TRUE;
}

static BOOL
RemoveMyDSN()
{
	char buf[200];
	char *p;

	snprintf(buf, sizeof(buf), "DSN=%s;", DataSourceName);
	for (p = buf; *p; p++)
		if (*p == ';')
			*p = 0;
	SQLConfigDataSource(NULL, ODBC_REMOVE_SYS_DSN, DriverName, buf);
	return TRUE;
}

static BOOL
Install(const char *driverpath)
{
	char path[300];
	WORD pathlen;
	BOOL rc;
	DWORD usagecount;

	/* first, retrieve the path the driver should be installed to
	 * in path */
	if (!SQLInstallDriverManager(path, sizeof(path), &pathlen) &&
	    ProcessSQLErrorMessages("SQLInstallDriverManager"))
		return FALSE;

	if (!CheckIfFileExists(path, "odbc32.dll")) {
		MessageBox(NULL,
			   "You must install MDAC before you can use the ODBC driver",
			   "Install",
			   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		SQLRemoveDriverManager(&usagecount);
		return FALSE;
	}

	rc = InstallMyDriver(driverpath);

	if (rc) {
		/* after the driver is installed create the new DSN */
		rc = AddMyDSN();
	}

	if (!rc)
		SQLRemoveDriverManager(&usagecount);

	return rc;
}

static BOOL
Uninstall()
{
	DWORD usagecount;

	RemoveMyDSN();
	RemoveMyDriver();
	SQLRemoveDriverManager(&usagecount);
	return TRUE;
}

int
main(int argc, char **argv)
{
/* for some bizarre reason, the content of buf gets mangled in the
 * call to Install().  For that reason we use a copy buf2. */
	char *buf = malloc(strlen(argv[0]) + 30);
	char *buf2;
	char *p;

	strcpy(buf, argv[0]);
	if ((p = strrchr(buf, '\\')) != 0 || (p = strrchr(buf, '/')) != 0)
		*p = 0;
	else
		strcpy(buf, ".");
	strcat(buf, "\\lib");

	if (argc != 2) {
		MessageBox(NULL, "/Install or /Uninstall argument expected",
			   argv[0],
			   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}
	buf2 = malloc(strlen(buf) + 25);
	strcpy(buf2, buf);
	if (strcmp("/Install", argv[1]) == 0) {
		if (!Install(buf)) {
			MessageBox(NULL, "ODBC Install Failed", argv[0],
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			exit(1);
		}
		/* create a file to indicate that we've installed the driver */
		strcat(buf2, "\\ODBCDriverInstalled.txt");
		CloseHandle(CreateFile(buf2, READ_CONTROL, 0, NULL,
				       CREATE_ALWAYS, FILE_ATTRIBUTE_HIDDEN,
				       NULL));
	} else if (strcmp("/Uninstall", argv[1]) == 0) {
		/* only uninstall the driver if the file exists */
		strcat(buf2, "\\ODBCDriverInstalled.txt");
		if (!DeleteFile(buf2)) {
			if (GetLastError() == ERROR_FILE_NOT_FOUND) {
				/* not installed, so don't uninstall */
				return 0;
			}
			MessageBox(NULL, "Cannot delete file for wrong reason",
				   argv[0],
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		}

		if (!Uninstall()) {
			MessageBox(NULL, "ODBC Uninstall Failed", argv[0],
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			exit(1);
		}
	} else {
		MessageBox(NULL, "/Install or /Uninstall argument expected",
			   argv[0],
			   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}
	free(buf);
	free(buf2);
	return 0;
}
