/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************
 * ODBCGlobal.h
 *
 * Description:
 * The global MonetDB ODBC include file which
 * includes all needed external include files.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCGLOBAL
#define _H_ODBCGLOBAL

/**** Define the ODBC Version this ODBC driver complies with ****/
#define ODBCVER 0x0351	/* Important: this must be defined before include of sqlext.h */

/* some general defines */
#define MONETDB_ODBC_VER     "3.51"	/* must be synchrone with ODBCVER */
#define MONETDB_DRIVER_NAME  "MonetDBODBClib"
#define MONETDB_DRIVER_VER   "1.00"
#define MONETDB_PRODUCT_NAME "MonetDB ODBC driver"
#define MONETDB_SERVER_NAME  "MonetDB"


/* standard ODBC driver include files */
#include <sqltypes.h>		/* ODBC C typedefs */
/* Note: sqlext.h includes sql.h so it is not needed here to be included */
/* Note2: if you include sql.h it will give an error because it will find
	src/sql/common/sql.h instead, which is not the one we need */
#include <sqlext.h>		/* ODBC API definitions and prototypes */
#include <sqlucode.h>		/* ODBC Unicode defs and prototypes */


/* standard ODBC driver installer & configurator include files */
#include <odbcinstext.h>	/* ODBC installer definitions and prototypes */
#include <ini.h>		/* ODBC configuration defs and prototypes */


/* MonetDB / SQL frontend include files */
#include <gdk.h>	/* for GDKmalloc(), GDKfree(), GDKstrdup() */
#include "comm.h"	/* for ??() */
#include <context.h>	/* for ??() */
#include <statement.h>	/* for ??() */
#include <mem.h>	/* for ??() */
#include <query.h>	/* for ??() */


/* standard C include files */
#include <string.h>	/* for strcpy() etc. */
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

#endif
