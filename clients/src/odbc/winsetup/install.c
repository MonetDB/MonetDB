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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

#include "ODBCGlobal.h"
#include <winver.h>
#include <shlwapi.h>
#include <string.h>
#include <stdio.h>
#include <malloc.h>

#ifdef __MINGW32__
#define DLL "-0.dll"
#else 
#define DLL ".dll"
#endif

static char *DriverName = "MonetDB ODBC Driver";
static char *DataSourceName = "MonetDB";
static char *InstallDLLs[] = {
	"libMonetODBC" DLL,
	"libMonetODBCs" DLL,
	"libMapi" DLL,
	"libstream" DLL,
	"libmutils" DLL,
	NULL
};

#define DriverDLL	(InstallDLLs[0])
#define DriverDLLs	(InstallDLLs[1])

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
		rc = SQLInstallerError(errnr, &errcode, errmsg, sizeof(errmsg), &errmsglen);
		if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
			MessageBox(NULL, errmsg, func, MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
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

	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR) & lpMsgBuf, 0, NULL);
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
GetFileVersion(char *filepath, char *version, int maxversionlen)
{
	DWORD handle = 0;
	DWORD versioninfosize;
	DWORD error;
	PVOID fileinfo;
	PBYTE versioninfo;
	PDWORD translation = NULL;
	UINT length = 0;
	char string[512] = "";
	LPSTR versionstr;
	PVOID ptr;

	versioninfosize = GetFileVersionInfoSize(filepath, &handle);
	if (!versioninfosize) {
		error = GetLastError();
		return FALSE;
	}

	fileinfo = (PVOID) malloc(versioninfosize);
	versioninfo = (PBYTE) malloc(versioninfosize);

	if (!GetFileVersionInfo(filepath, handle, versioninfosize, fileinfo)) {
		error = GetLastError();
		free(fileinfo);
		free(versioninfo);
		return FALSE;
	}

	ptr = (PVOID) &translation;
	if (!VerQueryValue(fileinfo, TEXT("\\VarFileInfo\\Translation"), (LPVOID *) ptr, &length)) {
		error = GetLastError();
		free(fileinfo);
		free(versioninfo);
		return FALSE;
	}

	snprintf(string, sizeof(string), "\\StringFileInfo\\%04x%04x\\FileVersion", LOWORD(*translation), HIWORD(*translation));

	ptr = (PVOID) &versionstr;
	if (!VerQueryValue(fileinfo, string, (PVOID *) ptr, &length)) {
		error = GetLastError();
		free(fileinfo);
		free(versioninfo);
		return FALSE;
	}

	if (lstrlen(versionstr) >= maxversionlen)
		lstrcpyn(version, versionstr, maxversionlen - 1);
	else
		lstrcpy(version, versionstr);

	free(fileinfo);
	free(versioninfo);

	return TRUE;
}

static BOOL
VersionCheckCopyFile(const char *srcpath, const char *dstpath, const char *filename)
{
	BOOL fileexists = FALSE;
	char srcfile[512];
	char dstfile[512];
	char srcfileVersion[512];
	char dstfileVersion[512];

	snprintf(srcfile, sizeof(srcfile), "%s\\%s", srcpath, filename);
	snprintf(dstfile, sizeof(dstfile), "%s\\%s", dstpath, filename);

	if (CheckIfFileExists(dstpath, filename)) {
		if (!GetFileVersion(srcfile, srcfileVersion, sizeof(srcfileVersion)) || !GetFileVersion(dstfile, dstfileVersion, (int) sizeof(dstfileVersion)))
			return FALSE;

		if (strcmp(dstfileVersion, srcfileVersion) >= 0) {
			/* file is up-to-date, so don't copy */
			return TRUE;
		}
		/* file exists but is not up-to-date, so copy */
		fileexists = TRUE;

		/* move the existing file out of the way */
		/* reuse dstfileVersion as temporary file name */
		strcpy(dstfileVersion, dstfile);
		/* change extension */
		dstfileVersion[strlen(dstfileVersion) - 1] = '~';
		if (!MoveFileEx(dstfile, dstfileVersion, MOVEFILE_REPLACE_EXISTING)) {
			snprintf(srcfileVersion, sizeof(srcfileVersion), "Unable to move %s to %s\n", dstfile, dstfileVersion);
			MessageBox(NULL, srcfileVersion, "VersionCheckCopyFile", MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			return FALSE;
		}
	}
	/* else file does not exist, so copy */

	if (!CopyFile(srcfile, dstfile, FALSE)) {
		snprintf(srcfileVersion, sizeof(srcfileVersion), "Unable to copy %s to %s\n", srcfile, dstfile);
		if (fileexists) {
			/* move original file back */
			MoveFileEx(dstfileVersion, dstfile, MOVEFILE_REPLACE_EXISTING);
		}
		MessageBox(NULL, srcfileVersion, "VersionCheckCopyFile", MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		return FALSE;
	}

	if (fileexists) {
		/* tell system to remove original file on reboot */
		if (!MoveFileEx(dstfileVersion, NULL, MOVEFILE_DELAY_UNTIL_REBOOT)) {
			snprintf(srcfileVersion, sizeof(srcfileVersion), "Unable to delete %s\n", dstfileVersion);
			MessageBox(NULL, srcfileVersion, "VersionCheckCopyFile", MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			return FALSE;
		}
	}

	return TRUE;
}

static BOOL
InstallMyDriver(const char *driverpath)
{
	char driver[300];
	char inpath[301];
	char outpath[301];
	WORD outpathlen;
	DWORD usagecount;
	char *p;
	char **dll;

	/* the correct format of driver keywords are
	 * "DriverName\0Driver=xxxxxx.DLL\0Setup=xxxxxx.DLL\0\0" */

	snprintf(driver, sizeof(driver), "%s;Driver=%s;Setup=%s;", DriverName, DriverDLL, DriverDLLs);

	for (p = driver; *p; p++)
		if (*p == ';')
			*p = '\0';

	/* the driver array is filled in before calling
	 * SQLInstallDriverEx so that SQLInstallDriverEx will return
	 * where to install the driver in the inpath */

	SQLInstallDriverEx(driver, NULL, inpath, sizeof(inpath), &outpathlen, ODBC_INSTALL_INQUIRY, &usagecount);

	/* the correct format of driver keywords are
	 * "DriverName\0Driver=c:\winnt\system32\xxxxxx.DLL\0Setup=c:\winnt\system32\xxxxxx.DLL\0\0" */

	snprintf(driver, sizeof(driver),
		 "%s;Driver=%s\\%s;Setup=%s\\%s;APILevel=1;"
		 "ConnectFunctions=YYY;DriverODBCVer=%s;SQLLevel=3;",
		 DriverName, inpath, DriverDLL, inpath, DriverDLLs, MONETDB_ODBC_VER);

	for (p = driver; *p; p++)
		if (*p == ';')
			*p = '\0';

	for (dll = InstallDLLs; *dll; dll++)
		if (!VersionCheckCopyFile(driverpath, inpath, *dll) && ProcessSQLErrorMessages("SQLInstallDriverEx"))
			return FALSE;

	/* call SQLInstallDriverEx to install the driver in the
	 * registry */
	if (!SQLInstallDriverEx(driver, inpath, outpath, sizeof(outpath), &outpathlen, ODBC_INSTALL_COMPLETE, &usagecount) && ProcessSQLErrorMessages("SQLInstallDriverEx"))
		return FALSE;

	return TRUE;
}

static BOOL
RemoveMyDriver()
{
	char buf[300];
	char dirname[300];
	WORD len;
	DWORD usagecount;
	DWORD valtype, valsize, rc;
	char *p;
	char **dll;

	/* most of this is equivalent to what SQLRemoveDriver is
	   suppposed to do, except that it consistently causes a
	   crash, so we do it ourselves */
	snprintf(buf, sizeof(buf), "SOFTWARE\\ODBC\\ODBCINST.INI\\%s", DriverName);
	valsize = sizeof(usagecount);
	usagecount = 0;
	valtype = REG_DWORD;
	rc = SHGetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount", &valtype, &usagecount, &valsize);
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
		rc = SHSetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount", REG_DWORD, &usagecount, sizeof(usagecount));
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
	rc = SHDeleteValue(HKEY_LOCAL_MACHINE, "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers", DriverName);
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "four");
		return FALSE;
	}
	/* figure out where the files were installed */
	snprintf(buf, sizeof(buf), "%s;Driver=%s;Setup=%s;", DriverName, DriverDLL, DriverDLLs);
	for (p = buf; *p; p++)
		if (*p == ';')
			*p = '\0';
	SQLInstallDriverEx(buf, NULL, dirname, sizeof(dirname), &len, ODBC_INSTALL_INQUIRY, &usagecount);
	/* and the delete them */
	for (dll = InstallDLLs; *dll; dll++) {
		snprintf(buf, sizeof(buf), "%s\\%s", dirname, *dll);
		DeleteFile(buf);
	}

	return TRUE;
}

static void
CreateAttributeString(char *attrs, size_t len)
{
	snprintf(attrs, len, "DSN=%s;Server=localhost;Database=;UID=monetdb;PWD=monetdb;", DataSourceName);

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
	if (!SQLConfigDataSource(NULL, ODBC_ADD_SYS_DSN, DriverName, attrs) && ProcessSQLErrorMessages("SQLConfigDataSource"))
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
	if (!SQLInstallDriverManager(path, sizeof(path), &pathlen) && ProcessSQLErrorMessages("SQLInstallDriverManager"))
		return FALSE;

	if (!CheckIfFileExists(path, "odbc32.dll")) {
		MessageBox(NULL, "You must install MDAC before you can use the ODBC driver", "Install", MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
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
	char *buf = malloc(strlen(argv[0]) + 30);
	char *p;

	strcpy(buf, argv[0]);
	if ((p = strrchr(buf, '\\')) != 0 || (p = strrchr(buf, '/')) != 0)
		*p = 0;
	else
		strcpy(buf, ".");
	strcat(buf, "\\lib");

	if (argc != 2) {
		MessageBox(NULL, "/Install or /Uninstall argument expected", argv[0], MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}
	if (strcmp("/Install", argv[1]) == 0) {
		if (!Install(buf)) {
			MessageBox(NULL, "ODBC Install Failed", argv[0], MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			exit(1);
		}
		/* create a file to indicate that we've installed the driver */
		strcat(buf, "\\ODBCDriverInstalled.txt");
		CloseHandle(CreateFile(buf, READ_CONTROL, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_HIDDEN, NULL));
	} else if (strcmp("/Uninstall", argv[1]) == 0) {
		/* only uninstall the driver if the file exists */
		strcat(buf, "\\ODBCDriverInstalled.txt");
		if (!DeleteFile(buf)) {
			if (GetLastError() == ERROR_FILE_NOT_FOUND) {
				/* not installed, so don't uninstall */
				return 0;
			}
			MessageBox(NULL, "Cannot delete file for wrong reason", argv[0], MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		}

		if (!Uninstall()) {
			MessageBox(NULL, "ODBC Uninstall Failed", argv[0], MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			exit(1);
		}
	} else {
		MessageBox(NULL, "/Install or /Uninstall argument expected", argv[0], MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}
	free(buf);
	return 0;
}
