/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifdef _MSC_VER
/* suppress deprecation warning for snprintf */
#define _CRT_SECURE_NO_WARNINGS

#include <WTypes.h>
#endif
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sql.h>
#include <sqlext.h>

#ifdef _MSC_VER
#define snprintf _snprintf
#endif

static void
prerr(SQLSMALLINT tpe, SQLHANDLE hnd, const char *func, const char *pref)
{
	SQLCHAR state[6];
	SQLINTEGER errnr;
	SQLCHAR msg[256];
	SQLSMALLINT msglen;

	switch (SQLGetDiagRec(tpe, hnd, 1, state, &errnr,
			      msg, sizeof msg, &msglen)) {
	case SQL_SUCCESS_WITH_INFO:
		if (msglen >= (signed int) sizeof msg)
			fprintf(stderr, "(message truncated)\n");
	case SQL_SUCCESS:
		fprintf(stderr,
			"%s: %s: SQLstate %s, Errnr %d, Message %s\n",
			func, pref, (char*)state, (int)errnr, (char*)msg);
		break;
	case SQL_INVALID_HANDLE:
		fprintf(stderr,
			"%s: %s, invalid handle passed to error function\n",
			func, pref);
		break;
	case SQL_ERROR:
		fprintf(stderr,
			"%s: %s, unexpected error from SQLGetDiagRec\n",
			func, pref);
		break;
	case SQL_NO_DATA:
		fprintf(stderr,
			"%s: %s, no error message from driver\n",
			func, pref);
		break;
	default:
		fprintf(stderr,
			"%s: %s, weird return value from SQLGetDiagRec\n",
			func, pref);
		break;
	}
}

static void
check(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func)
{
	switch (ret) {
	case SQL_SUCCESS:
		break;
	case SQL_SUCCESS_WITH_INFO:
		prerr(tpe, hnd, func, "Info");
		break;
	case SQL_ERROR:
		prerr(tpe, hnd, func, "Error");
		exit(1);
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: Error: invalid handle\n", func);
		exit(1);
	default:
		fprintf(stderr, "%s: Unexpected return value\n", func);
		break;
	}
}

#define NRECORD		2000
#define NRECORD2	(NRECORD / 2)
#define STRSIZE		200

struct record {
	short int i;
	char s[STRSIZE];
	double f;
	SQL_DATE_STRUCT d;
	SQL_TIME_STRUCT t;
	SQLLEN slen;
} data[NRECORD];

int
main(int argc, char **argv)
{
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	char *dsn = "MonetDB";
	char *user = "monetdb";
	char *pass = "monetdb";
	int i;
	SQLSMALLINT coltype;
	SQLULEN colsize;
	SQLSMALLINT decdig;
	SQLSMALLINT nullable;
	SQLULEN *processed;
	SQLUSMALLINT *status;
	SQLULEN offset;
	int *data_i;
	char (*data_s)[20];
	SQLLEN *data_slen;
	float *data_f;
	SQL_DATE_STRUCT *data_d;
	SQL_TIME_STRUCT *data_t;

	if (argc > 1)
		dsn = argv[1];
	if (argc > 2)
		user = argv[2];
	if (argc > 3)
		pass = argv[3];
	if (argc > 4 || *dsn == '-') {
		fprintf(stderr, "Usage: %s [datasource [user [password]]]\n",
			argv[0]);
		exit(1);
	}

	ret = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env);
	if (ret != SQL_SUCCESS) {
		fprintf(stderr, "Cannot allocate ODBC environment handle\n");
		exit(1);
	}
	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
			    (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
	check(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	check(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");
	ret = SQLConnect(dbc, (SQLCHAR *) dsn, SQL_NTS,
			 (SQLCHAR *) user, SQL_NTS,
			 (SQLCHAR *) pass, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");
	ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT,
				(SQLPOINTER) (uintptr_t) SQL_AUTOCOMMIT_OFF, 0);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLSetConnectAttr");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle (STMT)");

	ret = SQLExecDirect(stmt,
			    (SQLCHAR *) "CREATE TABLE arraytest ("
			    "i INTEGER DEFAULT '0' NOT NULL,"
			    "s VARCHAR(200),"
			    "f FLOAT,"
			    "d DATE,"
			    "t TIME)", SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect");

	for (i = 0; i < NRECORD; i++) {
		data[i].i = i;
		data[i].slen = (SQLLEN) snprintf(data[i].s,
						 sizeof(data[i].s),
						 "value \342\200\230%d\342\200\231", i);
		data[i].f = 1.0 / (i + 1);
		if (i == 0) {
			data[i].d.year = 2012;
			data[i].d.month = 3;
			data[i].d.day = 19;
			data[i].t.hour = 11;
			data[i].t.minute = 30;
			data[i].t.second = 35;
		} else {
			data[i].d.year = data[i - 1].d.year;
			data[i].d.month = data[i - 1].d.month;
			data[i].d.day = data[i - 1].d.day + 1;
			if ((data[i].d.day == 29 &&
			     data[i].d.month == 2 &&
			     data[i].d.year % 4 != 0) ||
			    (data[i].d.day == 30 &&
			     data[i].d.month == 2 &&
			     data[i].d.year % 4 == 0) ||
			    (data[i].d.day == 31 &&
			     (data[i].d.month == 4 ||
			      data[i].d.month == 6 ||
			      data[i].d.month == 9 ||
			      data[i].d.month == 11)) ||
			    data[i].d.day == 32) {
				data[i].d.day = 1;
				data[i].d.month++;
				if (data[i].d.month == 13) {
					data[i].d.month = 1;
					data[i].d.year++;
				}
			}
			data[i].t.hour = data[i - 1].t.hour;
			data[i].t.minute = data[i - 1].t.minute;
			data[i].t.second = data[i - 1].t.second + 1;
			if (data[i].t.second == 60) {
				data[i].t.second = 0;
				data[i].t.minute++;
				if (data[i].t.minute == 60) {
					data[i].t.minute = 0;
					data[i].t.hour++;
					if (data[i].t.hour == 24)
						data[i].t.hour = 0;
				}
			}
		}
	}

	ret = SQLPrepare(stmt, (SQLCHAR *) "INSERT INTO arraytest VALUES (?, ?, ?, ?, ?)", SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLPrepare");

	ret = SQLDescribeParam(stmt, 1, &coltype, &colsize, &decdig, &nullable);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeParam");
	ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SSHORT, coltype,
			       0, 0, &data[0].i, sizeof(data[0].i), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");

	ret = SQLDescribeParam(stmt, 2, &coltype, &colsize, &decdig, &nullable);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeParam");
	ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, coltype,
			       0, 0, &data[0].s, sizeof(data[0].s),
			       &data[0].slen);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");

	ret = SQLDescribeParam(stmt, 3, &coltype, &colsize, &decdig, &nullable);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeParam");
	ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_DOUBLE, coltype,
			       0, 0, &data[0].f, sizeof(data[0].f), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");

	ret = SQLDescribeParam(stmt, 4, &coltype, &colsize, &decdig, &nullable);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeParam");
	ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
			       coltype, 0, 0, &data[0].d, sizeof(data[0].d),
			       NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");

	ret = SQLDescribeParam(stmt, 5, &coltype, &colsize, &decdig, &nullable);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeParam");
	ret = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
			       coltype, 0, 0, &data[0].t, sizeof(data[0].t),
			       NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");

	ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE,
			     (SQLPOINTER) (uintptr_t) sizeof(data[0]), 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

	processed = malloc(NRECORD * sizeof(*processed));
	ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
			     (SQLPOINTER) processed, 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

	status = malloc(NRECORD * sizeof(*status));
	ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR,
			     (SQLPOINTER) status, 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

#if 0				/* doesn't currently work */
	ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE,
			     (SQLPOINTER) (uintptr_t) NRECORD, 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

	ret = SQLExecute(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecute");
#else
	ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
			     (SQLPOINTER) &offset, 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

	for (i = 0, offset = 0; i < NRECORD; i++, offset += sizeof(data[0])) {
		ret = SQLExecute(stmt);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLExecute");
	}
#endif

	ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE,
			     (SQLPOINTER) (uintptr_t) NRECORD2, 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");
	ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE,
			     (SQLPOINTER) (uintptr_t) SQL_BIND_BY_COLUMN, 0);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

	data_i = malloc(NRECORD * sizeof(*data_i));
	data_s = malloc(NRECORD * sizeof(*data_s));
	data_slen = malloc(NRECORD * sizeof(*data_slen));
	data_f = malloc(NRECORD * sizeof(*data_f));
	data_d = malloc(NRECORD * sizeof(*data_d));
	data_t = malloc(NRECORD * sizeof(*data_t));

	ret = SQLBindCol(stmt, 1, SQL_C_LONG, data_i, sizeof(*data_i), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 2, SQL_C_CHAR, data_s, sizeof(*data_s),
			 data_slen);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 3, SQL_C_FLOAT, data_f, sizeof(*data_f), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 4, SQL_C_TYPE_DATE, data_d, sizeof(*data_d),
			 NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 5, SQL_C_TYPE_TIME, data_t, sizeof(*data_t),
			 NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");

	ret = SQLExecDirect(stmt, (SQLCHAR *) "SELECT * FROM arraytest",
			    SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect");

	ret = SQLFetch(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch");

	ret = SQLBindCol(stmt, 1, SQL_C_LONG, data_i + NRECORD2,
			 sizeof(*data_i), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 2, SQL_C_CHAR, data_s + NRECORD2,
			 sizeof(*data_s), data_slen + NRECORD2);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 3, SQL_C_FLOAT, data_f + NRECORD2,
			 sizeof(*data_f), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 4, SQL_C_TYPE_DATE, data_d + NRECORD2,
			 sizeof(*data_d), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 5, SQL_C_TYPE_TIME, data_t + NRECORD2,
			 sizeof(*data_t), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");

	ret = SQLFetch(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch");

	for (i = 0; i < NRECORD; i++) {
		if (data[i].i != data_i[i] ||
		    (float) data[i].f != data_f[i] ||
		    data[i].d.year != data_d[i].year ||
		    data[i].d.month != data_d[i].month ||
		    data[i].d.day != data_d[i].day ||
		    data[i].t.hour != data_t[i].hour ||
		    data[i].t.minute != data_t[i].minute ||
		    data[i].t.second != data_t[i].second ||
		    data[i].slen != data_slen[i] ||
		    strcmp(data[i].s, data_s[i]) != 0) {
			fprintf(stderr, "Received incorrect data on row %d\n",
				i);
			fprintf(stderr,
				"%d %g %s %04d-%02d-%02d %02d:%02d:%02d\n",
				data[i].i, data[i].f, data[i].s,
				data[i].d.year, data[i].d.month, data[i].d.day,
				data[i].t.hour, data[i].t.minute,
				data[i].t.second);
			fprintf(stderr,
				"%d %g %s %04d-%02d-%02d %02d:%02d:%02d\n",
				data_i[i], data_f[i], data_s[i],
				data_d[i].year, data_d[i].month, data_d[i].day,
				data_t[i].hour, data_t[i].minute,
				data_t[i].second);
			exit(1);
		}
	}

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle 2");

	ret = SQLDisconnect(dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle 3");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle 4");

	return 0;
}
