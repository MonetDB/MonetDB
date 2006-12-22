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
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

/* Visual Studio 8 has deprecated lots of stuff: suppress warnings */
#define _CRT_SECURE_NO_DEPRECATE 1

#include "clients_config.h"
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#define ALREADY_HAVE_WINDOWS_TYPE
#include <sql.h>
#include <sqlext.h>
#ifdef EXPORT
#undef EXPORT
#endif
#define EXPORT __declspec(dllexport)
#include <odbcinst.h>
#include "resource.h"

static char *DriverName = "MonetDB ODBC Driver";
static HINSTANCE instance;

#define strncasecmp(x,y,l) _strnicmp(x,y,l)


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

struct data {
	char *dsn;
	char *uid;
	char *pwd;
	char *host;
	char *port;
	char *database;
	HWND parent;
	WORD request;
};

static void
MergeFromProfileString(const char *dsn, char **datap, const char *entry, const char *defval)
{
	char buf[256];

	if (*datap != NULL)
		return;
	if (dsn == NULL || *dsn == 0) {
		*datap = strdup(defval);
		return;
	}
	if (SQLGetPrivateProfileString(dsn, entry, defval, buf, sizeof(buf), "odbc.ini") == 0)
		return;
	*datap = strdup(buf);
}

static INT_PTR CALLBACK
DialogProc(HWND hwndDlg, UINT uMsg, WPARAM wParam, LPARAM lParam)
{
	static struct data *datap;
	char buf[128];
	RECT rcDlg, rcOwner;

	ODBCLOG("DialogProc 0x%x 0x%x 0x%x\n", uMsg, (int) wParam, (int) lParam);

	switch (uMsg) {
	case WM_INITDIALOG:
		datap = (struct data *) lParam;
		/* center dialog on parent */
		GetWindowRect(datap->parent, &rcOwner);
		GetWindowRect(hwndDlg, &rcDlg);
		SetWindowPos(hwndDlg, 0,
			     rcOwner.left + (rcOwner.right - rcOwner.left - (rcDlg.right - rcDlg.left)) / 2,
			     rcOwner.top + (rcOwner.bottom - rcOwner.top - (rcDlg.bottom - rcDlg.top)) / 2,
			     0, 0, /* ignores size arguments */
			     SWP_NOSIZE | SWP_NOZORDER);
		
		/* fill in text fields */
		SetDlgItemText(hwndDlg, IDC_EDIT_DSN, datap->dsn ? datap->dsn : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_UID, datap->uid ? datap->uid : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_PWD, datap->pwd ? datap->pwd : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_HOST, datap->host ? datap->host : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_PORT, datap->port ? datap->port : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_DATABASE, datap->database ? datap->database : "");
		if (datap->request == ODBC_ADD_DSN && datap->dsn && *datap->dsn)
			EnableWindow(GetDlgItem(hwndDlg, IDC_EDIT_DSN), FALSE);
		return TRUE;
	case WM_COMMAND:
		switch (LOWORD(wParam)) {
		case IDOK:
			if (datap->request != ODBC_ADD_DSN || datap->dsn == NULL || *datap->dsn == 0) {
				GetDlgItemText(hwndDlg, IDC_EDIT_DSN, buf, sizeof(buf));
				if (!SQLValidDSN(buf)) {
					MessageBox(hwndDlg,
						   "Invalid Datasource Name",
						   NULL,
						   MB_ICONERROR);
					return TRUE;
				}
				if (datap->dsn)
					free(datap->dsn);
				datap->dsn = strdup(buf);
			}
			GetDlgItemText(hwndDlg, IDC_EDIT_UID, buf, sizeof(buf));
			if (datap->uid)
				free(datap->uid);
			datap->uid = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_PWD, buf, sizeof(buf));
			if (datap->pwd)
				free(datap->pwd);
			datap->pwd = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_HOST, buf, sizeof(buf));
			if (datap->host)
				free(datap->host);
			datap->host = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_PORT, buf, sizeof(buf));
			if (datap->port)
				free(datap->port);
			datap->port = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_DATABASE, buf, sizeof(buf));
			if (datap->database)
				free(datap->database);
			datap->database = strdup(buf);
			/* fall through */
		case IDCANCEL:
			EndDialog(hwndDlg, wParam);
			return TRUE;
		}
	}
	return FALSE;
}

BOOL
ConfigDSN(HWND parent, WORD request, LPCSTR driver, LPCSTR attributes)
{
	struct data data;
	char *dsn = NULL;
	BOOL rc;

	ODBCLOG("ConfigDSN %d %s %s 0x%x\n", request, driver ? driver : "(null)", attributes ? attributes : "(null)", (int) &data);

	if (strcmp(driver, DriverName) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
		return FALSE;
	}
	switch (request) {
	case ODBC_ADD_DSN:
	case ODBC_CONFIG_DSN:
	case ODBC_REMOVE_DSN:
		break;
	default:
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE, "Invalid request");
		return FALSE;
	}

	data.dsn = NULL;
	data.uid = NULL;
	data.pwd = NULL;
	data.host = NULL;
	data.port = NULL;
	data.database = NULL;
	data.parent = parent;
	data.request = request;

	while (*attributes) {
		char *value = strchr(attributes, '=');

		if (value == NULL) {
			SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE, "Invalid attributes string");
			return FALSE;
		}
		value++;
		if (strncasecmp("dsn=", attributes, value - attributes) == 0) {
			dsn = value;
			data.dsn = strdup(value);
		} else if (strncasecmp("uid=", attributes, value - attributes) == 0)
			data.uid = strdup(value);
		else if (strncasecmp("pwd=", attributes, value - attributes) == 0)
			data.pwd = strdup(value);
		else if (strncasecmp("host=", attributes, value - attributes) == 0)
			data.host = strdup(value);
		else if (strncasecmp("port=", attributes, value - attributes) == 0)
			data.port = strdup(value);
		else if (strncasecmp("database=", attributes, value - attributes) == 0)
			data.database = strdup(value);
		attributes = value + strlen(value) + 1;
	}

	if (request == ODBC_REMOVE_DSN) {
		if (data.dsn == NULL) {
			SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE, "No DSN specified");
			return FALSE;
		}
		rc = SQLRemoveDSNFromIni(data.dsn);

		goto finish;
	}

	MergeFromProfileString(data.dsn, &data.uid, "uid", "monetdb");
	MergeFromProfileString(data.dsn, &data.pwd, "pwd", "monetdb");
	MergeFromProfileString(data.dsn, &data.host, "host", "localhost");
	MergeFromProfileString(data.dsn, &data.port, "port", "50000");
	MergeFromProfileString(data.dsn, &data.database, "database", "");

	if (parent)
		rc = DialogBoxParam(instance, MAKEINTRESOURCE(IDD_SETUP_DIALOG), parent, DialogProc, (LPARAM) &data) == IDOK;
	else
		rc = TRUE;

	if (rc) {
		if (request == ODBC_ADD_DSN || strcmp(dsn, data.dsn) != 0) {
			if (!SQLValidDSN(data.dsn)) {
				rc = FALSE;
				if (parent)
					MessageBox(parent,
						   "Invalid Datasource Name",
						   NULL,
						   MB_ICONERROR);
				goto finish;
			}
			if (dsn == NULL || strcmp(dsn, data.dsn) != 0) {
				char *drv = NULL;

				/* figure out whether the new dsn already exists */
				MergeFromProfileString(data.dsn, &drv, "driver", "");
				if (drv && *drv) {
					free(drv);
					if (parent &&
					    MessageBox(parent,
						       "Replace existing Datasource Name?",
						       NULL,
						       MB_OKCANCEL | MB_ICONQUESTION) != IDOK) {
						rc = FALSE;
						goto finish;
					}
					if (!SQLRemoveDSNFromIni(data.dsn)) {
						rc = FALSE;
						MessageBox(parent,
							   "Failed to remove old Datasource Name",
							   NULL,
							   MB_ICONERROR);
						goto finish;
					}
				} else if (drv)
					free(drv);
			}
			if (dsn && !SQLRemoveDSNFromIni(dsn)) {
				rc = FALSE;
				if (parent)
					MessageBox(parent,
						   "Failed to remove old Datasource Name",
						   NULL,
						   MB_ICONERROR);
				goto finish;
			}
			if (!SQLWriteDSNToIni(data.dsn, driver)) {
				rc = FALSE;
				if (parent)
					MessageBox(parent,
						   "Failed to add new Datasource Name",
						   NULL,
						   MB_ICONERROR);
				goto finish;
			}
		}
		SQLWritePrivateProfileString(data.dsn, "uid", data.uid, "odbc.ini");
		SQLWritePrivateProfileString(data.dsn, "pwd", data.pwd, "odbc.ini");
		SQLWritePrivateProfileString(data.dsn, "host", data.host, "odbc.ini");
		SQLWritePrivateProfileString(data.dsn, "port", data.port, "odbc.ini");
		SQLWritePrivateProfileString(data.dsn, "database", data.database, "odbc.ini");
	}

  finish:
	if (data.dsn)
		free(data.dsn);
	if (data.uid)
		free(data.uid);
	if (data.pwd)
		free(data.pwd);
	if (data.host)
		free(data.host);
	if (data.port)
		free(data.port);
	if (data.database)
		free(data.database);
	return rc;
}

BOOL WINAPI
DllMain(HINSTANCE hinstDLL, DWORD reason, LPVOID reserved)
{
	instance = hinstDLL;
	(void) reserved;
	ODBCLOG("DllMain %d\n", reason);

	return TRUE;
}
