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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

#ifndef SQL_QUERY_H
#define SQL_QUERY_H

/* when making changes to this enum, also make the same change in Mapi.mx */
typedef enum sql_query_t {
	Q_PARSE = 0,
	Q_TABLE = 1,
	Q_UPDATE = 2,
	Q_SCHEMA = 3,
	Q_TRANS = 4,
	Q_PREPARE = 5,
	Q_BLOCK = 6
} sql_query_t;

#endif /* SQL_QUERY_H */
