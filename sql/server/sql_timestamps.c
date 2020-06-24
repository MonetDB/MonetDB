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
#include "sql_mvc.h"
#include "sql_atom.h"
#include "sql_parser.h"
#include "gdk_time.h"

#include "mtime.h"

static int GetSQLTypeFromAtom(sql_subtype *sql_subtype)
{
	if (!sql_subtype)
		return -1;
	if (!sql_subtype->type)
		return -1;
	return sql_subtype->type->eclass;
}

lng
convert_atom_into_unix_timestamp(mvc *sql, atom *a)
{
	lng res = 0;

	if (a->isnull) {
		(void) sql_error(sql, 02, SQLSTATE(42000) "The begin value cannot be null\n");
		return 0;
	}
	switch (GetSQLTypeFromAtom(&a->tpe)) {
		case EC_TIMESTAMP: {
			size_t len = sizeof(timestamp);
			timestamp *t = NULL;

			timestamp_fromstr(a->data.val.sval, &len, &t, false);
			if (!t) {
				(void) sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return 0;
			}
			res = timestamp_diff(*t, (timestamp) {0});
			GDKfree(t);
			break;
		}
		case EC_DATE: {
			size_t len = sizeof(date);
			date *d = NULL;
			timestamp t;

			date_fromstr(a->data.val.sval, &len, &d, false);
			if (!d) {
				sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return 0;
			}
			t = timestamp_fromdate(*d);
			res = timestamp_diff(t, (timestamp) {0});
			GDKfree(d);
			break;
		}
		case EC_TIME: {
			size_t len = sizeof(daytime);
			daytime *d = NULL;
			timestamp t = timestamp_current();

			daytime_fromstr(a->data.val.sval, &len, &d, false);
			if (!d) {
				sql_error(sql, 02,  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				return 0;
			}
			t = timestamp_add_usec(t, daytime_usec(*d));
			res = timestamp_diff(t, (timestamp) {0});
			GDKfree(d);
			break;
		}
		case EC_NUM: {
			switch (a->data.vtype) {
			#ifdef HAVE_HGE
				case TYPE_hge:
					res = (lng) a->data.val.hval;
					break;
			#endif
				case TYPE_lng:
					res = a->data.val.lval;
					break;
				case TYPE_int:
					res = (lng) a->data.val.ival;
					break;
				case TYPE_sht:
					res = (lng) a->data.val.shval;
					break;
				case TYPE_bte:
					res = (lng) a->data.val.btval;
					break;
				default:
					sql_error(sql, 02, SQLSTATE(42000) "Unknown SQL type for conversion\n");
					return 0;
			}
			if (res < 0) {
				sql_error(sql, 02, SQLSTATE(42000) "Negative UNIX timestamps are not allowed\n");
				return 0;
			}
			break;
		}
		default:
			sql_error(sql, 02,  SQLSTATE(42000) "Only number, time, date and timestamp fields "
												"are allowed for begin value\n");
	}
	return res;
}
