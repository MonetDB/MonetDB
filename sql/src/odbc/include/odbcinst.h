/**************************************************
 * odbcinst.h
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#ifndef __ODBCINST_H
#define __ODBCINST_H

#ifndef BOOL
#define BOOL	int
#endif

#ifndef __SQL
#include "sql.h"
#endif


/********************************************************
 * WINDOW HANDLE
 * Create and init one of these before calling a function which requires
 * a HWND. Then pass this as HWND arg with a cast.
 * The unixODBC function will use the szGUI to look for a GUI plugin lib.
 ********************************************************/
typedef struct  tODBCINSTWND
{
    char szGUI[21];                                 /* SHORT NAME FOR GUI; Qt, GTK, X, CONSOLE (case insensitive)                           */
    HWND hWnd;                                      /* WINDOW HANDLE (i.e. pointer to a QWidget for Qt)                                     */
		 
} ODBCINSTWND, *HODBCINSTWND;


#ifdef __cplusplus
extern "C" {
#endif

#ifndef ODBCVER
#define ODBCVER 0x0351
#endif

#ifndef WINVER
#define  WINVER  0x0400
#endif

/* SQLConfigDataSource request flags */
#define  ODBC_ADD_DSN     1
#define  ODBC_CONFIG_DSN  2
#define  ODBC_REMOVE_DSN  3

#if (ODBCVER >= 0x0250)
#define  ODBC_ADD_SYS_DSN 4			
#define  ODBC_CONFIG_SYS_DSN	5	
#define  ODBC_REMOVE_SYS_DSN	6	
#if (ODBCVER >= 0x0300)
#define	 ODBC_REMOVE_DEFAULT_DSN	7
#endif  /* ODBCVER >= 0x0300 */

/* install request flags */
#define	 ODBC_INSTALL_INQUIRY	1		
#define  ODBC_INSTALL_COMPLETE	2

/* config driver flags */
#define  ODBC_INSTALL_DRIVER	1
#define  ODBC_REMOVE_DRIVER		2
#define  ODBC_CONFIG_DRIVER		3
#define  ODBC_CONFIG_DRIVER_MAX 100
#endif

/* SQLGetConfigMode and SQLSetConfigMode flags */
#if (ODBCVER >= 0x0300)
#define ODBC_BOTH_DSN		0
#define ODBC_USER_DSN		1
#define ODBC_SYSTEM_DSN		2
#endif  /* ODBCVER >= 0x0300 */

/* SQLInstallerError code */
#if (ODBCVER >= 0x0300)
#define ODBC_ERROR_GENERAL_ERR                   1
#define ODBC_ERROR_INVALID_BUFF_LEN              2
#define ODBC_ERROR_INVALID_HWND                  3
#define ODBC_ERROR_INVALID_STR                   4
#define ODBC_ERROR_INVALID_REQUEST_TYPE          5
#define ODBC_ERROR_COMPONENT_NOT_FOUND           6
#define ODBC_ERROR_INVALID_NAME                  7
#define ODBC_ERROR_INVALID_KEYWORD_VALUE         8
#define ODBC_ERROR_INVALID_DSN                   9
#define ODBC_ERROR_INVALID_INF                  10
#define ODBC_ERROR_REQUEST_FAILED               11
#define ODBC_ERROR_INVALID_PATH                 12
#define ODBC_ERROR_LOAD_LIB_FAILED              13
#define ODBC_ERROR_INVALID_PARAM_SEQUENCE       14
#define ODBC_ERROR_INVALID_LOG_FILE             15
#define ODBC_ERROR_USER_CANCELED                16
#define ODBC_ERROR_USAGE_UPDATE_FAILED          17
#define ODBC_ERROR_CREATE_DSN_FAILED            18
#define ODBC_ERROR_WRITING_SYSINFO_FAILED       19
#define ODBC_ERROR_REMOVE_DSN_FAILED            20
#define ODBC_ERROR_OUT_OF_MEM                   21
#define ODBC_ERROR_OUTPUT_STRING_TRUNCATED      22
#endif /* ODBCVER >= 0x0300 */

#ifndef EXPORT
#define EXPORT
#endif

#define INSTAPI

/* HIGH LEVEL CALLS */
BOOL INSTAPI SQLInstallODBC          (HWND       hwndParent,
                                      LPCSTR     lpszInfFile,
									  LPCSTR     lpszSrcPath,
									  LPCSTR     lpszDrivers);
BOOL INSTAPI SQLManageDataSources    (HWND       hwndParent);
BOOL INSTAPI SQLCreateDataSource     (HWND       hwndParent,
                                      LPCSTR     lpszDSN);
BOOL INSTAPI SQLGetTranslator        (HWND       hwnd,
									   LPSTR      lpszName,
									   WORD       cbNameMax,
									   WORD  	*pcbNameOut,
									   LPSTR      lpszPath,
									   WORD       cbPathMax,
									   WORD  	*pcbPathOut,
									   DWORD 	*pvOption);

/* LOW LEVEL CALLS */
BOOL INSTAPI SQLInstallDriver        (LPCSTR     lpszInfFile,
                                      LPCSTR     lpszDriver,
                                      LPSTR      lpszPath,
                                      WORD       cbPathMax,
                                      WORD 		* pcbPathOut);
BOOL INSTAPI SQLInstallDriverManager (LPSTR      lpszPath,
                                      WORD       cbPathMax,
                                      WORD 		* pcbPathOut);
BOOL INSTAPI SQLGetInstalledDrivers  (LPSTR      lpszBuf,
                                      WORD       cbBufMax,
                                      WORD 		* pcbBufOut);
BOOL INSTAPI SQLGetAvailableDrivers  (LPCSTR     lpszInfFile,
                                      LPSTR      lpszBuf,
                                      WORD       cbBufMax,
                                      WORD 		* pcbBufOut);
BOOL INSTAPI SQLConfigDataSource     (HWND       hwndParent,
                                      WORD       fRequest,
                                      LPCSTR     lpszDriver,
                                      LPCSTR     lpszAttributes);
BOOL INSTAPI SQLRemoveDefaultDataSource(void);
BOOL INSTAPI SQLWriteDSNToIni        (LPCSTR     lpszDSN,
                                      LPCSTR     lpszDriver);
BOOL INSTAPI SQLRemoveDSNFromIni     (LPCSTR     lpszDSN);
BOOL INSTAPI SQLValidDSN             (LPCSTR     lpszDSN);

BOOL INSTAPI SQLWritePrivateProfileString(LPCSTR lpszSection,
										 LPCSTR lpszEntry,
										 LPCSTR lpszString,
										 LPCSTR lpszFilename);

int  INSTAPI SQLGetPrivateProfileString( LPCSTR lpszSection,
										LPCSTR lpszEntry,
										LPCSTR lpszDefault,
										LPSTR  lpszRetBuffer,
										int    cbRetBuffer,
										LPCSTR lpszFilename);

#if (ODBCVER >= 0x0250)
BOOL INSTAPI SQLRemoveDriverManager(LPDWORD lpdwUsageCount);
BOOL INSTAPI SQLInstallTranslator(LPCSTR lpszInfFile,
								  LPCSTR lpszTranslator,
								  LPCSTR lpszPathIn,
								  LPSTR  lpszPathOut,
								  WORD   cbPathOutMax,
								  WORD 	*pcbPathOut,
								  WORD	 fRequest,
								  LPDWORD	lpdwUsageCount);
BOOL INSTAPI SQLRemoveTranslator(LPCSTR lpszTranslator,
								 LPDWORD lpdwUsageCount);
BOOL INSTAPI SQLRemoveDriver(LPCSTR lpszDriver,
							 BOOL fRemoveDSN,
							 LPDWORD lpdwUsageCount);
BOOL INSTAPI SQLConfigDriver(HWND hwndParent,
							 WORD fRequest,
							 LPCSTR lpszDriver,
							 LPCSTR lpszArgs,
							 LPSTR  lpszMsg,
							 WORD   cbMsgMax,
                             WORD 	*pcbMsgOut);
#endif

#if (ODBCVER >=  0x0300)
SQLRETURN INSTAPI SQLInstallerError(WORD iError,
							DWORD *pfErrorCode,
							LPSTR	lpszErrorMsg,
							WORD	cbErrorMsgMax,
							WORD	*pcbErrorMsg);
SQLRETURN INSTAPI SQLPostInstallerError(DWORD dwErrorCode, LPCSTR lpszErrMsg);

BOOL INSTAPI SQLWriteFileDSN(LPCSTR  lpszFileName,
                             LPCSTR  lpszAppName,
                             LPCSTR  lpszKeyName,
                             LPCSTR  lpszString);

BOOL INSTAPI  SQLReadFileDSN(LPCSTR  lpszFileName,
                             LPCSTR  lpszAppName,
                             LPCSTR  lpszKeyName,
                             LPSTR   lpszString,
                             WORD    cbString,
                             WORD   *pcbString);
BOOL INSTAPI SQLInstallDriverEx(LPCSTR lpszDriver,
							 LPCSTR	   lpszPathIn,
							 LPSTR	   lpszPathOut,
							 WORD	   cbPathOutMax,
							 WORD	  *pcbPathOut,
							 WORD		fRequest,
							 LPDWORD	lpdwUsageCount);
BOOL INSTAPI SQLInstallTranslatorEx(LPCSTR lpszTranslator,
								  LPCSTR lpszPathIn,
								  LPSTR  lpszPathOut,
								  WORD   cbPathOutMax,
								  WORD 	*pcbPathOut,
								  WORD	 fRequest,
								  LPDWORD	lpdwUsageCount);
BOOL INSTAPI SQLGetConfigMode(UWORD	*pwConfigMode);
BOOL INSTAPI SQLSetConfigMode(UWORD wConfigMode);
#endif /* ODBCVER >= 0x0300 */

/*	Driver specific Setup APIs called by installer */
BOOL INSTAPI ConfigDSN (HWND	hwndParent,
						WORD	fRequest,
						LPCSTR	lpszDriver,
						LPCSTR	lpszAttributes);

BOOL INSTAPI ConfigTranslator (	HWND		hwndParent,
								DWORD 		*pvOption);

#if (ODBCVER >= 0x0250)
BOOL INSTAPI ConfigDriver(HWND hwndParent,
						  WORD fRequest,
                          LPCSTR lpszDriver,
				          LPCSTR lpszArgs,
                          LPSTR  lpszMsg,
                          WORD   cbMsgMax,
                          WORD 	*pcbMsgOut);
#endif


#ifdef __cplusplus
}
#endif

#endif
