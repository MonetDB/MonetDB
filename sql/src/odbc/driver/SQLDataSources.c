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

/**********************************************************************
 * SQLDataSources()
 * CLI Compliance: ISO 92
 *
 * Author: Sjoerd Mullender
 * Date  : 4 sep 2003
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLDataSources(
	SQLHENV EnvironmentHandle,
	SQLUSMALLINT Direction,
	SQLCHAR *ServerName,
	SQLSMALLINT BufferLength1,
	SQLSMALLINT *NameLength1,
	SQLCHAR *Description,
	SQLSMALLINT BufferLength2,
	SQLSMALLINT *NameLength2)
{
	ODBCEnv * env = (ODBCEnv *) EnvironmentHandle;

	(void) Direction;	/* Stefan: unused!? */
	(void) ServerName;	/* Stefan: unused!? */
	(void) BufferLength1;	/* Stefan: unused!? */
	(void) NameLength1;	/* Stefan: unused!? */
	(void) Description;	/* Stefan: unused!? */
	(void) BufferLength2;	/* Stefan: unused!? */
	(void) NameLength2;	/* Stefan: unused!? */

	if (! isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	/* TODO: implement the requested behavior */

	/* for now always return error: Driver does not support this function */
	addEnvError(env, "IM001", NULL, 0);
	return SQL_ERROR;
}
