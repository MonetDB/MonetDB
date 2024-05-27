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
	char *desc;
	char *uid;
	char *pwd;
	char *host;
	char *port;
	char *database;
	char *schema;
	char *logintimeout;
	char *replytimeout;
	char *replysize;
	char *autocommit;
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
		SetDlgItemText(hwndDlg, IDC_EDIT_DESC, datap->desc ? datap->desc : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_UID, datap->uid ? datap->uid : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_PWD, datap->pwd ? datap->pwd : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_HOST, datap->host ? datap->host : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_PORT, datap->port ? datap->port : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_DATABASE, datap->database ? datap->database : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_SCHEMA, datap->schema ? datap->schema : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_LOGINTIMEOUT, datap->logintimeout ? datap->logintimeout : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_REPLYTIMEOUT, datap->replytimeout ? datap->replytimeout : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_REPLYSIZE, datap->replysize ? datap->replysize : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_AUTOCOMMIT, datap->autocommit ? datap->autocommit : "on");
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
			GetDlgItemText(hwndDlg, IDC_EDIT_DESC, buf, sizeof(buf));
			if (datap->desc)
				free(datap->desc);
			datap->desc = strdup(buf);
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
			GetDlgItemText(hwndDlg, IDC_EDIT_LOGINTIMEOUT, buf, sizeof(buf));
			if (datap->logintimeout)
				free(datap->logintimeout);
			datap->logintimeout = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_REPLYTIMEOUT, buf, sizeof(buf));
			if (datap->replytimeout)
				free(datap->replytimeout);
			datap->replytimeout = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_REPLYSIZE, buf, sizeof(buf));
			if (datap->replysize)
				free(datap->replysize);
			datap->replysize = strdup(buf);
			GetDlgItemText(hwndDlg, IDC_EDIT_AUTOCOMMIT, buf, sizeof(buf));
			if (datap->autocommit)
				free(datap->autocommit);
			datap->autocommit = strdup(buf);
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
	data.desc = NULL;
	data.uid = NULL;
	data.pwd = NULL;
	data.host = NULL;
	data.port = NULL;
	data.database = NULL;
	data.logfile = NULL;
	data.schema = NULL;
	data.logintimeout = NULL;
	data.replytimeout = NULL;
	data.replysize = NULL;
	data.autocommit = NULL;
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
		if (strncasecmp("DSN=", attributes, value - attributes) == 0) {
			dsn = value;
			data.dsn = strdup(value);
		} else if (strncasecmp("Description=", attributes, value - attributes) == 0)
			data.desc = strdup(value);
		else if (strncasecmp("UID=", attributes, value - attributes) == 0)
			data.uid = strdup(value);
		else if (strncasecmp("PWD=", attributes, value - attributes) == 0)
			data.pwd = strdup(value);
		else if (strncasecmp("Host=", attributes, value - attributes) == 0)
			data.host = strdup(value);
		else if (strncasecmp("Port=", attributes, value - attributes) == 0)
			data.port = strdup(value);
		else if (strncasecmp("Database=", attributes, value - attributes) == 0)
			data.database = strdup(value);
		else if (strncasecmp("Schema=", attributes, value - attributes) == 0)
			data.schema = strdup(value);
		else if (strncasecmp("LoginTimeout=", attributes, value - attributes) == 0)
			data.logintimeout = strdup(value);
		else if (strncasecmp("ReplyTimeout=", attributes, value - attributes) == 0)
			data.replytimeout = strdup(value);
		else if (strncasecmp("ReplySize=", attributes, value - attributes) == 0)
			data.replysize = strdup(value);
		else if (strncasecmp("AutoCommit=", attributes, value - attributes) == 0)
			data.autocommit = strdup(value);
		else if (strncasecmp("TimeZone=", attributes, value - attributes) == 0)
			data.timezone = strdup(value);
		else if (strncasecmp("LogFile=", attributes, value - attributes) == 0)
			data.logfile = strdup(value);
		else if (strncasecmp("TLS=", attributes, value - attributes) == 0)
			data.use_tls = strdup(value);
		else if (strncasecmp("Cert=", attributes, value - attributes) == 0)
			data.servercert = strdup(value);
		else if (strncasecmp("CertHash=", attributes, value - attributes) == 0)
			data.servercerthash = strdup(value);
		else if (strncasecmp("ClientKey=", attributes, value - attributes) == 0)
			data.clientkey = strdup(value);
		else if (strncasecmp("ClientCert=", attributes, value - attributes) == 0)
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

	MergeFromProfileString(data.dsn, &data.desc, "Description", "");
	MergeFromProfileString(data.dsn, &data.uid, "UID", "");
	MergeFromProfileString(data.dsn, &data.pwd, "PWD", "");
	MergeFromProfileString(data.dsn, &data.host, "Host", "localhost");
	MergeFromProfileString(data.dsn, &data.port, "Port", MAPI_PORT_STR);
	MergeFromProfileString(data.dsn, &data.database, "Database", "");
	MergeFromProfileString(data.dsn, &data.schema, "Schema", "");
	MergeFromProfileString(data.dsn, &data.logintimeout, "LoginTimeout", "");
	MergeFromProfileString(data.dsn, &data.replytimeout, "ReplyTimeout", "");
	MergeFromProfileString(data.dsn, &data.replysize, "ReplySize", "");
	MergeFromProfileString(data.dsn, &data.autocommit, "AutoCommit", "on");
	MergeFromProfileString(data.dsn, &data.timezone, "TimeZone", "");
	MergeFromProfileString(data.dsn, &data.logfile, "LogFile", "");
	MergeFromProfileString(data.dsn, &data.use_tls, "TLS", "off");
	MergeFromProfileString(data.dsn, &data.servercert, "Cert", "");
	MergeFromProfileString(data.dsn, &data.servercerthash, "CertHash", "");
	MergeFromProfileString(data.dsn, &data.clientkey, "ClientKey", "");
	MergeFromProfileString(data.dsn, &data.clientcert, "ClientCert", "");

	ODBCLOG("ConfigDSN values: DSN=%s UID=%s PWD=%s Host=%s Port=%s Database=%s Schema=%s LoginTimeout=%s ReplyTimeout=%s ReplySize=%s AutoCommit=%s TimeZone=%s LogFile=%s TLSs=%s Cert=%s CertHash=%s ClientKey=%s ClientCert=%s\n",
		data.dsn ? data.dsn : "(null)",
		data.uid ? data.uid : "(null)",
		data.pwd ? data.pwd : "(null)",
		data.host ? data.host : "(null)",
		data.port ? data.port : "(null)",
		data.database ? data.database : "(null)",
		data.schema ? data.schema : "(null)",
		data.logintimeout ? data.logintimeout : "(null)",
		data.replytimeout ? data.replytimeout : "(null)",
		data.replysize ? data.replysize : "(null)",
		data.autocommit ? data.autocommit : "(null)",
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
	ODBCLOG("ConfigDSN writing values: DSN=%s UID=%s PWD=%s Host=%s Port=%s Database=%s Schema=%s LoginTimeout=%s ReplyTimeout=%s ReplySize=%s AutoCommit=%s TimeZone=%s LogFile=%s TLSs=%s Cert=%s CertHash=%s ClientKey=%s ClientCert=%s\n",
		data.dsn ? data.dsn : "(null)",
		data.uid ? data.uid : "(null)",
		data.pwd ? data.pwd : "(null)",
		data.host ? data.host : "(null)",
		data.port ? data.port : "(null)",
		data.database ? data.database : "(null)",
		data.schema ? data.schema : "(null)",
		data.logintimeout ? data.logintimeout : "(null)",
		data.replytimeout ? data.replytimeout : "(null)",
		data.replysize ? data.replysize : "(null)",
		data.autocommit ? data.autocommit : "(null)",
		data.timezone ? data.timezone : "(null)",
		data.logfile ? data.logfile : "(null)",
		data.use_tls ? data.use_tls : "(null)",
		data.servercert ? data.servercert : "(null)",
		data.servercerthash ? data.servercerthash : "(null)",
		data.clientkey ? data.clientkey : "(null)",
		data.clientcert ? data.clientcert : "(null)");

	if (!SQLWritePrivateProfileString(data.dsn, "UID", data.uid, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "PWD", data.pwd, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "Host", data.host, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "Port", data.port, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "Database", data.database, "odbc.ini")) {
		rc = FALSE;
		if (parent)
			MessageBox(parent, "Error writing configuration data to registry", NULL, MB_ICONERROR);
		SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Error writing configuration data to registry");
		goto finish;
	}

	if (!SQLWritePrivateProfileString(data.dsn, "Description", data.desc, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "Schema", data.schema, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "LoginTimeout", data.logintimeout, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "ReplyTimeout", data.replytimeout, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "ReplySize", data.replysize, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "AutoCommit", data.autocommit, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "TimeZone", data.timezone, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "LogFile", data.logfile, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "TLS", data.use_tls, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "Cert", data.servercert, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "CertHash", data.servercerthash, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "ClientKey", data.clientkey, "odbc.ini")
	 || !SQLWritePrivateProfileString(data.dsn, "ClientCert", data.clientcert, "odbc.ini")) {
		if (parent)
			MessageBox(parent, "Error writing optional configuration data to registry", NULL, MB_ICONERROR);
		goto finish;
	}

  finish:
	if (data.dsn)
		free(data.dsn);
	if (data.desc)
		free(data.desc);
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
	if (data.logintimeout)
		free(data.logintimeout);
	if (data.replytimeout)
		free(data.replytimeout);
	if (data.replysize)
		free(data.replysize);
	if (data.autocommit)
		free(data.autocommit);
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
