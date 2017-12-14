/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/**************************************************
 * drvcfg.h
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#ifndef _ODBCINST_H
#define _ODBCINST_H

#include <unistd.h>
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif
#include <sys/types.h>

#define ODBCVER 0x0351

#ifdef WIN32
#ifndef LIBMONETODBCS
#define odbc_export extern __declspec(dllimport)
#else
#define odbc_export extern __declspec(dllexport)
#endif
#else
#define odbc_export extern
#endif

#include <odbcinst.h>

/********************************************************
 * CONSTANTS WHICH DO NOT EXIST ELSEWHERE
 ********************************************************/
#ifndef TRUE
#define FALSE 0;
#define TRUE 1;
#endif

/*********************************
 * ODBCINST - PROPERTIES
 *********************************
 *
 * PURPOSE:
 *
 * To provide the caller a mechanism to interact with Data Source
 * properties containing Driver specific options while avoiding
 * embedding GUI code in the ODBC infrastructure.
 *
 * DETAILS:
 *
 * 1. Application calls libodbcinst.ODBCINSTConstructProperties()
 *    - odbcinst will load the driver and call
 *      libMyDrvS.ODBCINSTGetProperties() to build a list of all
 *      possible properties
 * 2. Application calls libodbcinst.ODBCINSTSetProperty()
 *    - use, as required, to init values (ie if configuring existing
 *      DataSource)
 *    - use libodbcinst.SetConfigMode() &
 *      libodbcinst.SQLGetPrivateProfileString() to read existing Data
 *      Source info (do not forget to set the mode back)
 *    - do not forget to set mode back to ODBC_BOTH_DSN using
 *      SetConfigMode() when done reading
 *    - no call to Driver Setup
 * 3. Application calls libodbcinst.ODBCINSTValidateProperty()
 *    - use as required (ie on leave widget event)
 *    - an assesment of the entire property list is also done
 *    - this is passed onto the driver setup DLL
 * 4. Application should refresh widgets in case aPromptData or
 *    szValue has changed
 *    - refresh should occur for each property where bRefresh = 1
 * 5. Application calls libodbcinst.ODBCINSTValidateProperties()
 *    - call this just before saving new Data Source or updating
 *      existing Data Source
 *    - should always call this before saving
 *    - use libodbcinst.SetConfigMode() &
 *      libodbcinst.SQLWritePrivateProfileString() to save Data Source
 *      info
 *    - do not forget to set mode back to ODBC_BOTH_DSN using
 *      SetConfigMode() when done saving
 *    - this is passed onto the driver setup DLL
 * 6. Application calls ODBCINSTDestructProperties() to free up memory
 *    - unload Driver Setup DLL
 *    - frees memory (Driver Setup allocates most of the memory but we
 *      free ALL of it in odbcinst)
 *
 * NOTES
 *
 * 1. odbcinst implements 5 functions to support this GUI config stuff
 * 2. Driver Setup DLL implements just 3 functions for its share of
 *    the work
 *
 *********************************/

#define ODBCINST_SUCCESS		0
#define ODBCINST_WARNING		1
#define ODBCINST_ERROR			2

#define ODBCINST_PROMPTTYPE_LABEL	0	/* readonly */
#define ODBCINST_PROMPTTYPE_TEXTEDIT	1
#define ODBCINST_PROMPTTYPE_LISTBOX	2
#define ODBCINST_PROMPTTYPE_COMBOBOX	3
#define ODBCINST_PROMPTTYPE_FILENAME	4
#define ODBCINST_PROMPTTYPE_HIDDEN	5

#define INI_MAX_LINE			1000
#define INI_MAX_OBJECT_NAME		INI_MAX_LINE
#define INI_MAX_PROPERTY_NAME		INI_MAX_LINE
#define INI_MAX_PROPERTY_VALUE		INI_MAX_LINE

typedef struct tODBCINSTPROPERTY {
	struct tODBCINSTPROPERTY *pNext;	/* linked list */

	char szName[INI_MAX_PROPERTY_NAME + 1];	/* property name */
	char szValue[INI_MAX_PROPERTY_VALUE + 1];	/* property value */
	int nPromptType;	/* PROMPTTYPE_TEXTEDIT, PROMPTTYPE_LISTBOX,
				   PROMPTTYPE_COMBOBOX, PROMPTTYPE_FILENAME */
	char **aPromptData;	/* array of pointers terminated with a
				   NULL value in array. */
	char *pszHelp;		/* help on this property (driver
				   setups should keep it short) */
	void *pWidget;		/* CALLER CAN STORE A POINTER TO ? HERE */
	int bRefresh;		/* app should refresh widget ie Driver
				   Setup has changed aPromptData or
				   szValue */
	void *hDLL;		/* for odbcinst internal use... only
				   first property has valid one */
} ODBCINSTPROPERTY, *HODBCINSTPROPERTY;


#if defined(__cplusplus)
extern "C" {
#endif

/* ONLY IMPLEMENTED IN DRIVER SETUP (not in ODBCINST) */
	odbc_export int ODBCINSTGetProperties(HODBCINSTPROPERTY hFirstProperty);

#if defined(__cplusplus)
}
#endif
#endif
