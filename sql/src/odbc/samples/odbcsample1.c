#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>

void
prerr(SQLSMALLINT tpe, SQLHANDLE hnd, const char *func, const char *pref)
{
	SQLCHAR state[6];
	SQLINTEGER errno;
	SQLCHAR msg[256];
	SQLSMALLINT msglen;

	switch (SQLGetDiagRec(tpe, hnd, 1, state, &errno, msg, sizeof(msg), &msglen)) {
	case SQL_SUCCESS_WITH_INFO:
		if (msglen >= sizeof(msg))
			fprintf(stderr, "(message truncated)\n");
	case SQL_SUCCESS:
		fprintf(stderr,
			"%s: %s: SQLstate %s, Errno %d, Message %s\n",
			func, pref, state, errno, msg);
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
		fprintf(stderr, "%s: %s, no error message from driver\n",
			func, pref);
		break;
	default:
		fprintf(stderr,
			"%s: %s, weird return value from SQLGetDiagRec\n",
			func, pref);
		break;
	}
}

void
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

int
main(int argc, char **argv)
{
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	SQLCHAR *host = "Default";
	SQLCHAR *user = "monetdb";
	SQLCHAR *pass = "monetdb";
	SQLRETURN ret;
	int i;
	SQLSMALLINT f1;
	SQLCHAR f2[30];
	SQLDOUBLE f3;
	SQL_DATE_STRUCT f4;
	SQL_TIME_STRUCT f5;

	if (argc > 1)
		host = argv[1];
	if (argc > 2)
		user = argv[2];
	if (argc > 3)
		pass = argv[3];
	if (argc > 4 || *host == '-') {
		fprintf(stderr, "Usage: %s [ host [ user [ password ] ] ]\n",
			argv[0]);
		exit(1);
	}

	if (SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env) != SQL_SUCCESS) {
		fprintf(stderr, "Cannot allocate ODBC environment handle\n");
		exit(1);
	}

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	check(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle");

	ret = SQLConnect(dbc, host, SQL_NTS, user, SQL_NTS, pass, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle");

	ret = SQLExecDirect(stmt,
			    "CREATE TABLE test (\n"
			    "   i INT(11) DEFAULT '0' NOT NULL,\n"
			    "   s VARCHAR,\n"
			    "   f FLOAT,\n"
			    "   d DATE,\n"
			    "   t TIME\n"
			    ")", SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect");

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle");

	ret = SQLPrepare(stmt, "INSERT INTO test VALUES (?, ?, ?, ?, ?)",
			 SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLPrepare");

	ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SSHORT,
			       SQL_INTEGER, 0, 0, &f1, sizeof(f1), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");
	ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
			       SQL_VARCHAR, 0, 0, &f2, sizeof(f2), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");
	ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_DOUBLE,
			       SQL_FLOAT, 0, 0, &f3, sizeof(f3), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");
	ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
			       SQL_TYPE_DATE, 0, 0, &f4, sizeof(f4), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");
	ret = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
			       SQL_TYPE_TIME, 0, 0, &f5, sizeof(f5), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindParameter");

	for (i = 0; i < 200; i++) {
		f1 = i;
		snprintf(f2, sizeof(f2), "value %d", i);
		f3 = i * 1.5;
		f4.year = 2003;
		f4.month = i % 12 + 1;
		f4.day = i / 12 + 1;
		f5.hour = i % 24;
		f5.minute = i / 24;
		f5.second = 25;
		ret = SQLExecute(stmt);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLExecute");
	}

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle");

	ret = SQLPrepare(stmt, "SELECT * FROM test", SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLPrepare");

	ret = SQLBindCol(stmt, 1, SQL_C_SSHORT, &f1, sizeof(f1), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &f2, sizeof(f2), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 3, SQL_C_DOUBLE, &f3, sizeof(f3), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 4, SQL_C_TYPE_DATE, &f4, sizeof(f4), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");
	ret = SQLBindCol(stmt, 5, SQL_C_TYPE_TIME, &f5, sizeof(f5), NULL);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLBindCol");

	ret = SQLExecute(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecute");

	for (;;) {
		ret = SQLFetch(stmt);
		if (ret == SQL_NO_DATA)
			break;
		check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch");

		printf("%d %s %g %04d:%02d:%02d %02d-%02d-%02d\n",
		       f1, f2, f3, f4.year, f4.month, f4.day,
		       f5.hour, f5.minute, f5.second);
	}

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle");

	ret = SQLDisconnect(dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle");

	return 0;
}
