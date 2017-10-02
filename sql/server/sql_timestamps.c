/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * author: Pedro Ferreira, M. Kersten
 * This module converts a MonetDB atom into a UNIX timestamp if the conversation is possible. An exception is thrown
 * otherwise.
 */

#include "sql_timestamps.h"
#include "sql.h"
#include "sql_atom.h"
#include "gdk.h"
#include "mtime.h"

static int GetSQLTypeFromAtom(sql_subtype *sql_subtype)
{
	if (!sql_subtype)
		return -1;
	if (!sql_subtype->type)
		return -1;
	return sql_subtype->type->eclass;
}

str
convert_atom_into_unix_timestamp(atom *a, lng* res)
{
	str msg = MAL_SUCCEED;
	*res = 0;

	if(a->isnull) {
		msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "The begin value cannot be null\n");
		goto finish;
	}
	switch (GetSQLTypeFromAtom(&a->tpe)) {
		case EC_TIMESTAMP: {
			timestamp *tempp = (timestamp*) &(a->data.val.lval);
			if((msg = MTIMEepoch2lng(res, tempp)) != MAL_SUCCEED) {
				msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "%s\n", msg);
				goto finish;
			}
			break;
		}
		case EC_DATE: {
			timestamp tempd;
			tempd.days = a->data.val.ival;
			tempd.msecs = 0;
			if((msg = MTIMEepoch2lng(res, &tempd)) != MAL_SUCCEED) {
				msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "%s\n", msg);
				goto finish;
			}
			break;
		}
		case EC_TIME: {
			timestamp tempt;
			date dateother;
			if((msg = MTIMEcurrent_date(&dateother)) != MAL_SUCCEED) {
				msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "%s\n", msg);
				goto finish;
			}
			tempt.days = dateother;
			tempt.msecs = a->data.val.ival;
			if((msg = MTIMEepoch2lng(res, &tempt)) != MAL_SUCCEED) {
				msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "%s\n", msg);
				goto finish;
			}
			break;
		}
		case EC_NUM: {
			switch (a->data.vtype) {
			#ifdef HAVE_HGE
				case TYPE_hge:
					*res = (lng) a->data.val.hval;
					break;
			#endif
				case TYPE_lng:
					*res = a->data.val.lval;
					break;
				case TYPE_int:
					*res = (lng) a->data.val.ival;
					break;
				case TYPE_sht:
					*res = (lng) a->data.val.shval;
					break;
				case TYPE_bte:
					*res = (lng) a->data.val.btval;
					break;
				default:
					msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "Unknown SQL type for conversion\n");
					goto finish;
			}
			if(*res < 0) {
				msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "Negative UNIX timestamps are not allowed\n");
				goto finish;
			}
			break;
		}
		/*case EC_CHAR:
		case EC_STRING:*/
		default:
			msg = createException(SQL, "sql.timestamp", SQLSTATE(42000) "Only number, time, date and timestamp fields "
																		"are allowed for begin value\n");
	}
finish:
	return msg;
}
