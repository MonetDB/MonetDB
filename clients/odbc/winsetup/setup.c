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

/* Visual Studio 8 has deprecated lots of stuff: suppress warnings */
#ifndef _CRT_SECURE_NO_DEPRECATE
#define _CRT_SECURE_NO_DEPRECATE 1
#endif

#include "monetdb_config.h"
#include <windows.h>
/* indicate to sqltypes.h that windows.h has already been included and
   that it doesn't have to define Windows constants */
#define ALREADY_HAVE_WINDOWS_TYPE 1
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

static void
ODBCLOG(const char *fmt, ...)
{
	va_list ap;
#ifdef NATIVE_WIN32
	wchar_t *s = _wgetenv(L"ODBCDEBUG");
#else
	char *s = getenv("ODBCDEBUG");
#endif

	va_start(ap, fmt);
	if (s && *s) {
		FILE *f;

#ifdef NATIVE_WIN32
		f = _wfopen(s, L"a");
#else
		f = fopen(s, "a");
#endif
		if (f) {
			vfprintf(f, fmt, ap);
			fclose(f);
		} else
			vfprintf(stderr, fmt, ap);
	}
	va_end(ap);
}

BOOL INSTAPI
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
	char *schema;
	char *autocommit;
	char *replysize;
	char *timezone;
	char *logfile;
	// TLS settings
	char *use_tls;
	char *servercert;
	char *servercerthash;
	char *clientkey;
	char *clientcert;

	HWND parent;
	WORD request;
};

static void
MergeFromProfileString(const char *dsn, char **datap, const char *entry, const char *defval)
{
	char buf[2048];

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
	char buf[2048];
	RECT rcDlg, rcOwner;

	switch (uMsg) {
	case WM_INITDIALOG:
		ODBCLOG("DialogProc WM_INITDIALOG 0x%x 0x%x\n", (unsigned) wParam, (unsigned) lParam);

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
		SetDlgItemText(hwndDlg, IDC_EDIT_SCHEMA, datap->schema ? datap->schema : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_AUTOCOMMIT, datap->autocommit ? datap->autocommit : "on");
		SetDlgItemText(hwndDlg, IDC_EDIT_REPLYSIZE, datap->replysize ? datap->replysize : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_TIMEZONE, datap->timezone ? datap->timezone : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_LOGFILE, datap->logfile ? datap->logfile : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_USETLS, datap->use_tls ? datap->use_tls : "off");
		SetDlgItemText(hwndDlg, IDC_EDIT_SERVERCERT, datap->servercert ? datap->servercert : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_SERVERCERTHASH, datap->servercerthash ? datap->servercerthash : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_CLIENTKEY, datap->clientkey ? datap->clientkey : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_CLIENTCERT, datap->clientcert ? datap->clientcert : "");
		if (datap->request == ODBC_ADD_DSN && datap->dsn && *datap->dsn)
			EnableWindow(GetDlgItem(hwndDlg, IDC_EDIT_DSN), FALSE);
		return TRUE;
	case WM_COMMAND:
		ODBCLOG("DialogProc WM_COMMAND 0x%x 0x%x\n", (unsigned) wParam, (unsigned) lParam);

		switch (LOWORD(wParam)) {
		case IDOK:
			if (datap->request != ODBC_ADD_DSN || datap->dsn == NULL || *datap->dsn == 0) {
				GetDlgItemText(hwndDlg, IDC_EDIT_DSN, buf, sizeof(buf));
				if (!SQLValidDSN(buf)) {
					MessageBox(hwndDlg, "Invalid Datasource Name", NULL, MB_ICONERROR);
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
			GetDlgItemText(hwndDlg, IDC_EDIT_SCHEMA, buf, sizeof(buf));
			if (datap->schema)
				free(datap->schema);
			datap->schema = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_AUTOCOMMIT, buf, sizeof(buf));
			if (datap->autocommit)
				free(datap->autocommit);
			datap->autocommit = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_REPLYSIZE, buf, sizeof(buf));
			if (datap->replysize)
				free(datap->replysize);
			datap->replysize = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_TIMEZONE, buf, sizeof(buf));
			if (datap->timezone)
				free(datap->timezone);
			datap->timezone = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_LOGFILE, buf, sizeof(buf));
			if (datap->logfile)
				free(datap->logfile);
			datap->logfile = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_USETLS, buf, sizeof(buf));
			if (datap->use_tls)
				free(datap->use_tls);
			datap->use_tls = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_SERVERCERT, buf, sizeof(buf));
			if (datap->servercert)
				free(datap->servercert);
			datap->servercert = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_SERVERCERTHASH, buf, sizeof(buf));
			if (datap->servercerthash)
				free(datap->servercerthash);
			datap->servercerthash = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_CLIENTKEY, buf, sizeof(buf));
			if (datap->clientkey)
				free(datap->clientkey);
			datap->clientkey = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_CLIENTCERT, buf, sizeof(buf));
			if (datap->clientcert)
				free(datap->clientcert);
			datap->clientcert = strdup(buf);
			/* fall through */
		case IDCANCEL:
			EndDialog(hwndDlg, LOWORD(wParam));
			return TRUE;
		}
	default:
		ODBCLOG("DialogProc 0x%x 0x%x 0x%x\n", uMsg, (unsigned) wParam, (unsigned) lParam);
		break;
	}
	return FALSE;
}

BOOL INSTAPI
ConfigDSN(HWND parent, WORD request, LPCSTR driver, LPCSTR attributes)
{
	struct data data;
	char *dsn = NULL;
	BOOL rc;

	ODBCLOG("ConfigDSN %d %s %s 0x%" PRIxPTR "\n", request, driver ? driver : "(null)", attributes ? attributes : "(null)", (uintptr_t) &data);

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
	data.logfile = NULL;
	data.schema = NULL;
	data.autocommit = NULL;
	data.replysize = NULL;
	data.timezone = NULL;
	data.logfile = NULL;
	// TLS settings
	data.use_tls = NULL;
	data.servercert = NULL;
	data.servercerthash = NULL;
	data.clientkey = NULL;
	data.clientcert = NULL;

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
		else if (strncasecmp("schema=", attributes, value - attributes) == 0)
			data.schema = strdup(value);
		else if (strncasecmp("autocommit=", attributes, value - attributes) == 0)
			data.autocommit = strdup(value);
		else if (strncasecmp("replysize=", attributes, value - attributes) == 0)
			data.replysize = strdup(value);
		else if (strncasecmp("timezone=", attributes, value - attributes) == 0)
			data.timezone = strdup(value);
		else if (strncasecmp("logfile=", attributes, value - attributes) == 0)
			data.logfile = strdup(value);
		else if (strncasecmp("tls=", attributes, value - attributes) == 0)
			data.use_tls = strdup(value);
		else if (strncasecmp("cert=", attributes, value - attributes) == 0)
			data.servercert = strdup(value);
		else if (strncasecmp("certhash=", attributes, value - attributes) == 0)
			data.servercerthash = strdup(value);
		else if (strncasecmp("clientkey=", attributes, value - attributes) == 0)
			data.clientkey = strdup(value);
		else if (strncasecmp("clientcert=", attributes, value - attributes) == 0)
			data.clientcert = strdup(value);
		attributes = value + strlen(value) + 1;
	}

	if (request == ODBC_REMOVE_DSN) {
		if (data.dsn == NULL) {
			SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE,	"No DSN specified");
			return FALSE;
		}
		rc = SQLRemoveDSNFromIni(data.dsn);

		goto finish;
	}

	MergeFromProfileString(data.dsn, &data.uid, "uid", "monetdb");
	MergeFromProfileString(data.dsn, &data.pwd, "pwd", "monetdb");
	MergeFromProfileString(data.dsn, &data.host, "host", "localhost");
	MergeFromProfileString(data.dsn, &data.port, "port", MAPI_PORT_STR);
	MergeFromProfileString(data.dsn, &data.database, "database", "");
	MergeFromProfileString(data.dsn, &data.schema, "schema", "");
	MergeFromProfileString(data.dsn, &data.autocommit, "autocommit", "on");
	MergeFromProfileString(data.dsn, &data.replysize, "replysize", "");
	MergeFromProfileString(data.dsn, &data.timezone, "timezone", "");
	MergeFromProfileString(data.dsn, &data.logfile, "logfile", "");
	MergeFromProfileString(data.dsn, &data.use_tls, "tls", "off");
	MergeFromProfileString(data.dsn, &data.servercert, "cert", "");
	MergeFromProfileString(data.dsn, &data.servercerthash, "certhash", "");
	MergeFromProfileString(data.dsn, &data.clientkey, "clientkey", "");
	MergeFromProfileString(data.dsn, &data.clientcert, "clientcert", "");

	ODBCLOG("ConfigDSN values: dsn=%s uid=%s pwd=%s host=%s port=%s database=%s schema=%s autocommit=%s replysize=%s timezone=%s logfile=%s tls=%s cert=%s certhash=%s clientkey=%s clientcert=%s\n",
		data.dsn ? data.dsn : "(null)",
		data.uid ? data.uid : "(null)",
		data.pwd ? data.pwd : "(null)",
		data.host ? data.host : "(null)",
		data.port ? data.port : "(null)",
		data.database ? data.database : "(null)",
		data.schema ? data.schema : "(null)",
		data.autocommit ? data.autocommit : "(null)",
		data.replysize ? data.replysize : "(null)",
		data.timezone ? data.timezone : "(null)",
		data.logfile ? data.logfile : "(null)",
		data.use_tls ? data.use_tls : "(null)",
		data.servercert ? data.servercert : "(null)",
		data.servercerthash ? data.servercerthash : "(null)",
		data.clientkey ? data.clientkey : "(null)",
		data.clientcert ? data.clientcert : "(null)");

	/* we're optimistic: default return value */
	rc = TRUE;

	if (parent) {
		switch (DialogBoxParam(instance,
				       MAKEINTRESOURCE(IDD_SETUP_DIALOG),
				       parent,
				       DialogProc,
				       (LPARAM) &data)) {
		case IDOK:
			break;
		default:
			rc = FALSE;
			SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Error creating configuration dialog");
			/* fall through */
		case IDCANCEL:
			goto finish;
		}
	}

	if (request == ODBC_ADD_DSN || strcmp(dsn, data.dsn) != 0) {
		if (!SQLValidDSN(data.dsn)) {
			rc = FALSE;
			if (parent)
				MessageBox(parent, "Invalid Datasource Name", NULL, MB_ICONERROR);
			SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
			goto finish;
		}
		if (dsn == NULL || strcmp(dsn, data.dsn) != 0) {
			char *drv = NULL;

			/* figure out whether the new dsn already exists */
			MergeFromProfileString(data.dsn, &drv, "driver", "");
			if (drv && *drv) {
				free(drv);
				if (parent &&
				    MessageBox(parent, "Replace existing Datasource Name?", NULL, MB_OKCANCEL | MB_ICONQUESTION) != IDOK) {
					goto finish;
				}
				ODBCLOG("ConfigDSN removing dsn %s\n", data.dsn);
				if (!SQLRemoveDSNFromIni(data.dsn)) {
					rc = FALSE;
					MessageBox(parent, "Failed to remove old Datasource Name", NULL, MB_ICONERROR);
					SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Failed to remove old Datasource Name");
					goto finish;
				}
			} else if (drv)
				free(drv);
		}
		if (dsn && !SQLRemoveDSNFromIni(dsn)) {
			rc = FALSE;
			if (parent)
				MessageBox(parent, "Failed to remove old Datasource Name", NULL, MB_ICONERROR);
			SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Failed to remove old Datasource Name");
			goto finish;
		}
		if (!SQLWriteDSNToIni(data.dsn, driver)) {
			rc = FALSE;
			if (parent)
				MessageBox(parent, "Failed to add new Datasource Name", NULL, MB_ICONERROR);
			SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Failed to add new Datasource Name");
			goto finish;
		}
	}
	ODBCLOG("ConfigDSN writing values: dsn=%s uid=%s pwd=%s host=%s port=%s database=%s schema=%s autocommit=%s replysize=%s timezone=%s logfile=%s tls=%s cert=%s certhash=%s clientkey=%s clientcert=%s\n",
		data.dsn ? data.dsn : "(null)",
		data.uid ? data.uid : "(null)",
		data.pwd ? data.pwd : "(null)",
		data.host ? data.host : "(null)",
		data.port ? data.port : "(null)",
		data.database ? data.database : "(null)",
		data.schema ? data.schema : "(null)",
		data.autocommit ? data.autocommit : "(null)",
		data.replysize ? data.replysize : "(null)",
		data.timezone ? data.timezone : "(null)",
		data.logfile ? data.logfile : "(null)",
		data.use_tls ? data.use_tls : "(null)",
		data.servercert ? data.servercert : "(null)",
		data.servercerthash ? data.servercerthash : "(null)",
		data.clientkey ? data.clientkey : "(null)",
		data.clientcert ? data.clientcert : "(null)");

	if (!SQLWritePrivateProfileString(data.dsn, "uid", data.uid, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "pwd", data.pwd, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "host", data.host, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "port", data.port, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "database", data.database, "odbc.ini")) {
		rc = FALSE;
		if (parent)
			MessageBox(parent, "Error writing configuration data to registry", NULL, MB_ICONERROR);
		SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Error writing configuration data to registry");
		goto finish;
	}

	if (!SQLWritePrivateProfileString(data.dsn, "schema", data.schema, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "autocommit", data.autocommit, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "replysize", data.replysize, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "timezone", data.timezone, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "logfile", data.logfile, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "tls", data.use_tls, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "cert", data.servercert, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "certhash", data.servercerthash, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "clientkey", data.clientkey, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "clientcert", data.clientcert, "odbc.ini")) {
		if (parent)
			MessageBox(parent, "Error writing optional configuration data to registry", NULL, MB_ICONERROR);
		goto finish;
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
	if (data.schema)
		free(data.schema);
	if (data.autocommit)
		free(data.autocommit);
	if (data.replysize)
		free(data.replysize);
	if (data.timezone)
		free(data.timezone);
	if (data.logfile)
		free(data.logfile);
	if (data.use_tls)
		free(data.use_tls);
	if (data.servercert)
		free(data.servercert);
	if (data.servercerthash)
		free(data.servercerthash);
	if (data.clientkey)
		free(data.clientkey);
	if (data.clientcert)
		free(data.clientcert);
	ODBCLOG("ConfigDSN returning %s\n", rc ? "TRUE" : "FALSE");
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
