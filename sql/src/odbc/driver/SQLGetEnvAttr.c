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
 * SQLGetEnvAttr()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"


SQLRETURN SQLGetEnvAttr(
	SQLHENV		EnvironmentHandle,
	SQLINTEGER	Attribute,
	SQLPOINTER	Value,
	SQLINTEGER	BufferLength,
	SQLINTEGER *	StringLength)
{
	ODBCEnv * env = (ODBCEnv *) EnvironmentHandle;

	if (! isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	/* TODO: implement this function and corresponding behavior */

	/* for now return error IM001: driver not capable */
	addEnvError(env, "IM001", NULL, 0);
	return SQL_ERROR;
}
