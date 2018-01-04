/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_QUERY_H_
#define _SQL_QUERY_H_

typedef enum sql_query_t {
	Q_PARSE = 0,
	Q_TABLE = 1,
	Q_UPDATE = 2,
	Q_SCHEMA = 3,
	Q_TRANS = 4,
	Q_PREPARE = 5,
	Q_BLOCK = 6
} sql_query_t;

#endif /* _SQL_QUERY_H_ */
