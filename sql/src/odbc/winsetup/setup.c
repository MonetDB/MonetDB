#include "sql_config.h"
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#ifdef EXPORT
#undef EXPORT
#endif
#define EXPORT __declspec(dllexport)
#include <odbcinst.h>

static char *DriverName = "MonetDB ODBC Driver";
static char *DataSourceName = "MonetDB";

BOOL
ConfigDriver(HWND hwnd, WORD request, LPCSTR driver, LPCSTR args, LPSTR msg,
	     WORD msgmax, WORD *msgout)
{
	MessageBox(NULL, "ConfigDriver called", "ConfigDriver",
		   MB_ICONSTOP|MB_OK|MB_TASKMODAL|MB_SETFOREGROUND);
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
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE,
				      "Invalid request");
		return FALSE;
	}
	if (strcmp(driver, DriverName) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME,
				      "Invalid driver name");
		return FALSE;
	}
	return TRUE;
}

BOOL
ConfigDSN(HWND parent, WORD request, LPCSTR driver, LPCSTR attributes)
{
	MessageBox(NULL, "ConfigDSN called", "ConfigDSN",
		   MB_ICONSTOP|MB_OK|MB_TASKMODAL|MB_SETFOREGROUND);
	if (strcmp(driver, DriverName) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME,
				      "Invalid driver name");
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
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE,
				      "Invalid request");
		return FALSE;
	}
	
	return TRUE;
}
