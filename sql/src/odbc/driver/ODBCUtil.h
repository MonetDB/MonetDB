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
 * ODBCUtil.h
 *
 * Description:
 * This file contains ODBC driver utility function prototypes.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCUTIL
#define _H_ODBCUTIL

#include "ODBCGlobal.h"


/*
 * Utility function to duplicate an ODBC string (with a
 * length/special code specified, may be non null terminated)
 * to a normal C string (null terminated) or NULL.
 *
 * Precondition: none
 * Postcondition: returns a new allocated null terminated string or NULL
 */
char * copyODBCstr2Cstr(SQLCHAR * inStr, SQLSMALLINT lenCode);


#endif	/* _H_ODBCUTIL */
