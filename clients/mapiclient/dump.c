/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "monet_options.h"
#include "mapi.h"
#include "stream.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include "msqldump.h"

static const char *
get_with_comments_as_clause(Mapi mid)
{
	const char *query = /* check whether sys.comments exists */
		"SELECT t.id "
		"FROM sys._tables t JOIN sys.schemas s ON t.schema_id = s.id "
		"WHERE s.name = 'sys' AND t.name = 'comments'";
	const char *new_clause =
		"WITH comments AS (SELECT * FROM sys.comments), "
		     "function_types AS (SELECT * FROM sys.function_types), "
		     "function_languages AS (SELECT * FROM sys.function_languages)";
	const char *old_clause =
		"WITH comments AS ("
			"SELECT 42 AS id, 'no comment' AS remark WHERE FALSE"
		     "), "
		     "function_types AS ("
			"SELECT function_type_id, function_type_name, function_type_keyword "
			"FROM sys.function_types, "
			     "(VALUES "
				"(1, 'FUNCTION'),  "
				"(2, 'PROCEDURE'), "
				"(3, 'AGGREGATE'), "
				"(4, 'FILTER FUNCTION'), "
				"(5, 'FUNCTION'),  "
				"(6, 'FUNCTION'),  "
				"(7, 'LOADER')) AS (id, function_type_keyword) "
			"WHERE id = function_type_id"
		     "), "
		     "function_languages AS ("
			"SELECT language_id, language_name, language_keyword "
			"FROM sys.function_languages, (VALUES "
				"(3, 'R'), "
				"(4, 'C'), "
				"(6, 'PYTHON'), "
				"(7, 'PYTHON_MAP'), "
				"(8, 'PYTHON2'), "
				"(9, 'PYTHON2_MAP'), "
				"(10, 'PYTHON3'), "
				"(11, 'PYTHON3_MAP'), "
				"(12, 'CPP')) AS (id, language_keyword) "
			"WHERE id = language_id"
		     ")";

	MapiHdl hdl;
	const char *comments_clause;

	hdl = mapi_query(mid, query);
	if (mapi_error(mid)) {
		if (hdl) {
			mapi_explain_result(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return NULL;
	}
	comments_clause = mapi_fetch_row(hdl) ? new_clause : old_clause;
	mapi_close_handle(hdl);

	return comments_clause;
}

const char *
get_comments_clause(Mapi mid)
{
	static const char *comments_clause = NULL;
	if (comments_clause == NULL) {
		comments_clause = get_with_comments_as_clause(mid);
	}
	return comments_clause;
}

static int
quoted_print(stream *f, const char *s, bool singleq)
{
	if (mnstr_write(f, singleq ? "'" : "\"", 1, 1) < 0)
		return -1;
	while (*s) {
		switch (*s) {
		case '\\':
			if (mnstr_write(f, "\\\\", 1, 2) < 0)
				return -1;
			break;
		case '"':
			if (mnstr_write(f, "\"\"", 1, singleq ? 1 : 2) < 0)
				return -1;
			break;
		case '\'':
			if (mnstr_write(f, "''", 1, singleq ? 2 : 1) < 0)
				return -1;
			break;
		case '\n':
			if (mnstr_write(f, "\\n", 1, 2) < 0)
				return -1;
			break;
		case '\t':
			if (mnstr_write(f, "\\t", 1, 2) < 0)
				return -1;
			break;
		default:
			if ((0 < *s && *s < 32) || *s == '\177') {
				if (mnstr_printf(f, "\\%03o", (uint8_t) *s) < 0)
					return -1;
			} else {
				if (mnstr_write(f, s, 1, 1) < 0)
					return -1;
			}
			break;
		}
		s++;
	}
	if (mnstr_write(f, singleq ? "'" : "\"", 1, 1) < 0)
		return -1;
	return 0;
}

static int
comment_on(stream *toConsole, const char *object,
	   const char *ident1, const char *ident2, const char *ident3,
	   const char *remark)
{
	if (remark) {
		if (mnstr_printf(toConsole, "COMMENT ON %s \"%s\"", object, ident1) < 0)
			return -1;
		if (ident2) {
			if (mnstr_printf(toConsole, ".\"%s\"", ident2) < 0)
				return -1;
			if (ident3) {
				if (mnstr_printf(toConsole, ".\"%s\"", ident3) < 0)
					return -1;
			}
		}
		if (mnstr_write(toConsole, " IS ", 1, 4) < 0 ||
		    quoted_print(toConsole, remark, true) < 0 ||
		    mnstr_write(toConsole, ";\n", 1, 2) < 0)
			return -1;
	}
	return 0;
}

static char *actions[] = {
	0,
	"CASCADE",
	"RESTRICT",
	"SET NULL",
	"SET DEFAULT",
};
#define NR_ACTIONS	((int) (sizeof(actions) / sizeof(actions[0])))

static char *
get_schema(Mapi mid)
{
	char *sname = NULL;
	MapiHdl hdl;

	if ((hdl = mapi_query(mid, "SELECT current_schema")) == NULL ||
	    mapi_error(mid))
		goto bailout;
	while ((mapi_fetch_row(hdl)) != 0) {
		sname = mapi_fetch_field(hdl, 0);

		if (mapi_error(mid))
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	/* copy before closing the handle */
	if (sname)
		sname = strdup(sname);
	mapi_close_handle(hdl);
	return sname;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else
		mapi_explain(mid, stderr);
	return NULL;
}

/* return TRUE if the HUGEINT type exists */
static bool
has_hugeint(Mapi mid)
{
	MapiHdl hdl;
	bool ret;
	static int hashge = -1;

	if (hashge >= 0)
		return (bool) hashge;

	if ((hdl = mapi_query(mid,
			      "SELECT id "
			      "FROM sys.types "
			      "WHERE sqlname = 'hugeint'")) == NULL ||
	    mapi_error(mid))
		goto bailout;
	ret = mapi_get_row_count(hdl) == 1;
	while ((mapi_fetch_row(hdl)) != 0) {
		if (mapi_error(mid))
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hashge = (int) ret;
	return ret;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else
		mapi_explain(mid, stderr);
	return 0;
}

static int
dump_foreign_keys(Mapi mid, const char *schema, const char *tname, const char *tid, stream *toConsole)
{
	MapiHdl hdl = NULL;
	int cnt, i;
	char *query;
	size_t maxquerylen = 0;

	if (tname != NULL) {
		maxquerylen = 1024 + strlen(tname) + strlen(schema);
		query = malloc(maxquerylen);
		if (query == NULL)
			goto bailout;
		snprintf(query, maxquerylen,
			 "SELECT ps.name, "		/* 0 */
			        "pkt.name, "		/* 1 */
				"pkkc.name, "		/* 2 */
				"fkkc.name, "		/* 3 */
				"fkkc.nr, "		/* 4 */
				"fkk.name, "		/* 5 */
				"fkk.\"action\", "	/* 6 */
				"fs.name, "		/* 7 */
				"fkt.name "		/* 8 */
			 "FROM sys._tables fkt, "
			      "sys.objects fkkc, "
			      "sys.keys fkk, "
			      "sys._tables pkt, "
			      "sys.objects pkkc, "
			      "sys.keys pkk, "
			      "sys.schemas ps, "
			      "sys.schemas fs "
			 "WHERE fkt.id = fkk.table_id AND "
			       "pkt.id = pkk.table_id AND "
			       "fkk.id = fkkc.id AND "
			       "pkk.id = pkkc.id AND "
			       "fkk.rkey = pkk.id AND "
			       "fkkc.nr = pkkc.nr AND "
			       "pkt.schema_id = ps.id AND "
			       "fkt.schema_id = fs.id AND "
			       "fs.name = '%s' AND "
			       "fkt.name = '%s' "
			 "ORDER BY fkk.name, nr", schema, tname);
	} else if (tid != NULL) {
		maxquerylen = 1024 + strlen(tid);
		query = malloc(maxquerylen);
		if (query == NULL)
			goto bailout;
		snprintf(query, maxquerylen,
			 "SELECT ps.name, "		/* 0 */
			        "pkt.name, "		/* 1 */
				"pkkc.name, "		/* 2 */
				"fkkc.name, "		/* 3 */
				"fkkc.nr, "		/* 4 */
				"fkk.name, "		/* 5 */
				"fkk.\"action\", "	/* 6 */
				"0, "			/* 7 */
				"fkt.name "		/* 8 */
			 "FROM sys._tables fkt, "
			      "sys.objects fkkc, "
			      "sys.keys fkk, "
			      "sys._tables pkt, "
			      "sys.objects pkkc, "
			      "sys.keys pkk, "
			      "sys.schemas ps "
			 "WHERE fkt.id = fkk.table_id AND "
			       "pkt.id = pkk.table_id AND "
			       "fkk.id = fkkc.id AND "
			       "pkk.id = pkkc.id AND "
			       "fkk.rkey = pkk.id AND "
			       "fkkc.nr = pkkc.nr AND "
			       "pkt.schema_id = ps.id AND "
			       "fkt.id = %s "
			 "ORDER BY fkk.name, nr", tid);
	} else {
		query = "SELECT ps.name, "		/* 0 */
			       "pkt.name, "		/* 1 */
			       "pkkc.name, "		/* 2 */
			       "fkkc.name, "		/* 3 */
			       "fkkc.nr, "		/* 4 */
			       "fkk.name, "		/* 5 */
			       "fkk.\"action\", "	/* 6 */
			       "fs.name, "		/* 7 */
			       "fkt.name "		/* 8 */
			"FROM sys._tables fkt, "
			     "sys.objects fkkc, "
			     "sys.keys fkk, "
			     "sys._tables pkt, "
			     "sys.objects pkkc, "
			     "sys.keys pkk, "
			     "sys.schemas ps, "
			     "sys.schemas fs "
			"WHERE fkt.id = fkk.table_id AND "
			      "pkt.id = pkk.table_id AND "
			      "fkk.id = fkkc.id AND "
			      "pkk.id = pkkc.id AND "
			      "fkk.rkey = pkk.id AND "
			      "fkkc.nr = pkkc.nr AND "
			      "pkt.schema_id = ps.id AND "
			      "fkt.schema_id = fs.id AND "
			      "fkt.system = FALSE "
			"ORDER BY fs.name, fkt.name, "
			      "fkk.name, nr";
	}
	hdl = mapi_query(mid, query);
	if (query != NULL && maxquerylen != 0)
		free(query);
	maxquerylen = 0;
	if (hdl == NULL || mapi_error(mid))
		goto bailout;

	cnt = mapi_fetch_row(hdl);
	while (cnt != 0) {
		const char *c_psname = mapi_fetch_field(hdl, 0);
		const char *c_ptname = mapi_fetch_field(hdl, 1);
		const char *c_pcolumn = mapi_fetch_field(hdl, 2);
		const char *c_fcolumn = mapi_fetch_field(hdl, 3);
		const char *c_nr = mapi_fetch_field(hdl, 4);
		const char *c_fkname = mapi_fetch_field(hdl, 5);
		const char *c_faction = mapi_fetch_field(hdl, 6);
		const char *c_fsname = mapi_fetch_field(hdl, 7);
		const char *c_ftname = mapi_fetch_field(hdl, 8);
		const char **fkeys, **pkeys;
		int nkeys = 0;

		if (mapi_error(mid))
			goto bailout;
		assert(strcmp(c_nr, "0") == 0);
		(void) c_nr;	/* pacify compilers in case assertions are disabled */
		nkeys = 1;
		fkeys = malloc(nkeys * sizeof(*fkeys));
		pkeys = malloc(nkeys * sizeof(*pkeys));
		if (fkeys == NULL || pkeys == NULL) {
			if (fkeys)
				free((void *) fkeys);
			if (pkeys)
				free((void *) pkeys);
			goto bailout;
		}
		pkeys[nkeys - 1] = c_pcolumn;
		fkeys[nkeys - 1] = c_fcolumn;
		while ((cnt = mapi_fetch_row(hdl)) != 0 && strcmp(mapi_fetch_field(hdl, 4), "0") != 0) {
			const char **tkeys;
			nkeys++;
			tkeys = realloc((void *) pkeys, nkeys * sizeof(*pkeys));
			if (tkeys == NULL) {
				free((void *) pkeys);
				free((void *) fkeys);
				goto bailout;
			}
			pkeys = tkeys;
			tkeys = realloc((void *) fkeys, nkeys * sizeof(*fkeys));
			if (tkeys == NULL) {
				free((void *) pkeys);
				free((void *) fkeys);
				goto bailout;
			}
			fkeys = tkeys;
			pkeys[nkeys - 1] = mapi_fetch_field(hdl, 2);
			fkeys[nkeys - 1] = mapi_fetch_field(hdl, 3);
		}
		if (tname == NULL && tid == NULL) {
			mnstr_printf(toConsole,
				     "ALTER TABLE \"%s\".\"%s\" ADD ",
				     c_fsname, c_ftname);
		} else {
			mnstr_printf(toConsole, ",\n\t");
		}
		if (c_fkname) {
			mnstr_printf(toConsole, "CONSTRAINT \"%s\" ",
				c_fkname);
		}
		mnstr_printf(toConsole, "FOREIGN KEY (");
		for (i = 0; i < nkeys; i++) {
			mnstr_printf(toConsole, "%s\"%s\"",
				     i > 0 ? ", " : "", fkeys[i]);
		}
		mnstr_printf(toConsole, ") REFERENCES \"%s\".\"%s\" (",
			     c_psname, c_ptname);
		for (i = 0; i < nkeys; i++) {
			mnstr_printf(toConsole, "%s\"%s\"",
				     i > 0 ? ", " : "", pkeys[i]);
		}
		mnstr_printf(toConsole, ")");
		free((void *) fkeys);
		free((void *) pkeys);
		if (c_faction) {
			int action = atoi(c_faction);
			int on_update;
			int on_delete;

			if ((on_delete = action & 255) != 0 &&
			    on_delete < NR_ACTIONS &&
			    on_delete != 2	   /* RESTRICT -- default */)
				mnstr_printf(toConsole, " ON DELETE %s",
					     actions[on_delete]);
			if ((on_update = (action >> 8) & 255) != 0 &&
			    on_update < NR_ACTIONS &&
			    on_update != 2	   /* RESTRICT -- default */)
				mnstr_printf(toConsole, " ON UPDATE %s",
					     actions[on_update]);
		}
		if (tname == NULL && tid == NULL)
			mnstr_printf(toConsole, ";\n");

		if (mnstr_errnr(toConsole))
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	if (hdl)
		mapi_close_handle(hdl);
	return 0;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);

	return 1;
}

static const char *
toUpper(const char *s)
{
	static char toupperbuf[64];
	size_t i;
	size_t len = strlen(s);

	if (len >= sizeof(toupperbuf))
		return s;	/* too long: it's not *that* important */
	for (i = 0; i < len; i++)
		toupperbuf[i] = toupper((int)s[i]);
	toupperbuf[i] = '\0';
	return toupperbuf;
}

static int dump_column_definition(
	Mapi mid,
	stream *toConsole,
	const char *schema,
	const char *tname,
	const char *tid,
	bool foreign,
	bool hashge);

static const char *geomsubtypes[] = {
	NULL,			/* 0 */
	"POINT",		/* 1 */
	"LINESTRING",		/* 2 */
	NULL,			/* 3 */
	"POLYGON",		/* 4 */
	"MULTIPOINT",		/* 5 */
	"MULTILINESTRING",	/* 6 */
	"MULTIPOLYGON",		/* 7 */
	"GEOMETRYCOLLECTION",	/* 8 */
};

static int
dump_type(Mapi mid, stream *toConsole, const char *c_type, const char *c_type_digits, const char *c_type_scale, bool hashge)
{
	int space = 0;

	if (strcmp(c_type, "boolean") == 0) {
		space = mnstr_printf(toConsole, "BOOLEAN");
	} else if (strcmp(c_type, "int") == 0) {
		space = mnstr_printf(toConsole, "INTEGER");
	} else if (strcmp(c_type, "smallint") == 0) {
		space = mnstr_printf(toConsole, "SMALLINT");
	} else if (strcmp(c_type, "tinyint") == 0) {
		space = mnstr_printf(toConsole, "TINYINT");
	} else if (strcmp(c_type, "bigint") == 0) {
		space = mnstr_printf(toConsole, "BIGINT");
	} else if (strcmp(c_type, "hugeint") == 0) {
		space = mnstr_printf(toConsole, "HUGEINT");
	} else if (strcmp(c_type, "date") == 0) {
		space = mnstr_printf(toConsole, "DATE");
	} else if (strcmp(c_type, "month_interval") == 0) {
		if (strcmp(c_type_digits, "1") == 0)
			space = mnstr_printf(toConsole, "INTERVAL YEAR");
		else if (strcmp(c_type_digits, "2") == 0)
			space = mnstr_printf(toConsole, "INTERVAL YEAR TO MONTH");
		else if (strcmp(c_type_digits, "3") == 0)
			space = mnstr_printf(toConsole, "INTERVAL MONTH");
		else
			fprintf(stderr, "Internal error: unrecognized month interval %s\n", c_type_digits);
	} else if (strcmp(c_type, "sec_interval") == 0) {
		if (strcmp(c_type_digits, "4") == 0)
			space = mnstr_printf(toConsole, "INTERVAL DAY");
		else if (strcmp(c_type_digits, "5") == 0)
			space = mnstr_printf(toConsole, "INTERVAL DAY TO HOUR");
		else if (strcmp(c_type_digits, "6") == 0)
			space = mnstr_printf(toConsole, "INTERVAL DAY TO MINUTE");
		else if (strcmp(c_type_digits, "7") == 0)
			space = mnstr_printf(toConsole, "INTERVAL DAY TO SECOND");
		else if (strcmp(c_type_digits, "8") == 0)
			space = mnstr_printf(toConsole, "INTERVAL HOUR");
		else if (strcmp(c_type_digits, "9") == 0)
			space = mnstr_printf(toConsole, "INTERVAL HOUR TO MINUTE");
		else if (strcmp(c_type_digits, "10") == 0)
			space = mnstr_printf(toConsole, "INTERVAL HOUR TO SECOND");
		else if (strcmp(c_type_digits, "11") == 0)
			space = mnstr_printf(toConsole, "INTERVAL MINUTE");
		else if (strcmp(c_type_digits, "12") == 0)
			space = mnstr_printf(toConsole, "INTERVAL MINUTE TO SECOND");
		else if (strcmp(c_type_digits, "13") == 0)
			space = mnstr_printf(toConsole, "INTERVAL SECOND");
		else
			fprintf(stderr, "Internal error: unrecognized second interval %s\n", c_type_digits);
	} else if (strcmp(c_type, "clob") == 0 ||
		   (strcmp(c_type, "varchar") == 0 &&
		    strcmp(c_type_digits, "0") == 0)) {
		space = mnstr_printf(toConsole, "CHARACTER LARGE OBJECT");
		if (strcmp(c_type_digits, "0") != 0)
			space += mnstr_printf(toConsole, "(%s)", c_type_digits);
	} else if (strcmp(c_type, "blob") == 0) {
		space = mnstr_printf(toConsole, "BINARY LARGE OBJECT");
		if (strcmp(c_type_digits, "0") != 0)
			space += mnstr_printf(toConsole, "(%s)", c_type_digits);
	} else if (strcmp(c_type, "timestamp") == 0 ||
		   strcmp(c_type, "timestamptz") == 0) {
		space = mnstr_printf(toConsole, "TIMESTAMP");
		if (strcmp(c_type_digits, "7") != 0)
			space += mnstr_printf(toConsole, "(%d)", atoi(c_type_digits) - 1);
		if (strcmp(c_type, "timestamptz") == 0)
			space += mnstr_printf(toConsole, " WITH TIME ZONE");
	} else if (strcmp(c_type, "time") == 0 ||
		   strcmp(c_type, "timetz") == 0) {
		space = mnstr_printf(toConsole, "TIME");
		if (strcmp(c_type_digits, "1") != 0)
			space += mnstr_printf(toConsole, "(%d)", atoi(c_type_digits) - 1);
		if (strcmp(c_type, "timetz") == 0)
			space += mnstr_printf(toConsole, " WITH TIME ZONE");
	} else if (strcmp(c_type, "real") == 0) {
		if (strcmp(c_type_digits, "24") == 0 &&
		    strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(toConsole, "REAL");
		else if (strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(toConsole, "FLOAT(%s)", c_type_digits);
		else
			space = mnstr_printf(toConsole, "FLOAT(%s,%s)",
					c_type_digits, c_type_scale);
	} else if (strcmp(c_type, "double") == 0) {
		if (strcmp(c_type_digits, "53") == 0 &&
		    strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(toConsole, "DOUBLE");
		else if (strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(toConsole, "FLOAT(%s)", c_type_digits);
		else
			space = mnstr_printf(toConsole, "FLOAT(%s,%s)",
					c_type_digits, c_type_scale);
	} else if (strcmp(c_type, "decimal") == 0 &&
		   strcmp(c_type_digits, "1") == 0 &&
		   strcmp(c_type_scale, "0") == 0) {
		space = mnstr_printf(toConsole, "DECIMAL");
	} else if (strcmp(c_type, "table") == 0) {
		mnstr_printf(toConsole, "TABLE ");
		dump_column_definition(mid, toConsole, NULL, NULL, c_type_digits, 1, hashge);
	} else if (strcmp(c_type, "geometry") == 0 &&
		   strcmp(c_type_digits, "0") != 0) {
		const char *geom = NULL;
		int sub = atoi(c_type_digits);

		if (sub > 0 && (sub & 3) == 0 &&
		    (sub >> 2) < (int) (sizeof(geomsubtypes) / sizeof(geomsubtypes[0])))
			geom = geomsubtypes[sub >> 2];
		if (geom) {
			mnstr_printf(toConsole, "GEOMETRY(%s", geom);
			if (strcmp(c_type_scale, "0") != 0)
				mnstr_printf(toConsole, ",%s", c_type_scale);
			mnstr_printf(toConsole, ")");
		} else {
			mnstr_printf(toConsole, "GEOMETRY");
		}
	} else if (strcmp(c_type_digits, "0") == 0) {
		space = mnstr_printf(toConsole, "%s", toUpper(c_type));
	} else if (strcmp(c_type_scale, "0") == 0) {
		space = mnstr_printf(toConsole, "%s(%s)",
				toUpper(c_type), c_type_digits);
	} else {
		if (strcmp(c_type, "decimal") == 0) {
			if (strcmp(c_type_digits, "39") == 0)
				c_type_digits = "38";
			else if (!hashge && strcmp(c_type_digits, "19") == 0)
				c_type_digits = "18";
		}
		space = mnstr_printf(toConsole, "%s(%s,%s)",
				toUpper(c_type), c_type_digits, c_type_scale);
	}
	return space;
}

static int
dump_column_definition(Mapi mid, stream *toConsole, const char *schema, const char *tname, const char *tid, bool foreign, bool hashge)
{
	MapiHdl hdl = NULL;
	char *query;
	size_t maxquerylen;
	int cnt;
	int slen;
	int cap;
#define CAP(X) ((cap = (int) (X)) < 0 ? 0 : cap)

	maxquerylen = 1024;
	if (tid == NULL)
		maxquerylen += strlen(tname) + strlen(schema);
	else
		maxquerylen += strlen(tid);
	if ((query = malloc(maxquerylen)) == NULL)
		goto bailout;

	mnstr_printf(toConsole, "(\n");

	if (tid)
		snprintf(query, maxquerylen,
			 "SELECT c.name, "		/* 0 */
				"c.type, "		/* 1 */
				"c.type_digits, "	/* 2 */
				"c.type_scale, "	/* 3 */
				"c.\"null\", "		/* 4 */
				"c.\"default\", "	/* 5 */
				"c.number "		/* 6 */
			 "FROM sys._columns c "
			 "WHERE c.table_id = %s "
			 "ORDER BY number", tid);
	else
		snprintf(query, maxquerylen,
			 "SELECT c.name, "		/* 0 */
				"c.type, "		/* 1 */
				"c.type_digits, "	/* 2 */
				"c.type_scale, "	/* 3 */
				"c.\"null\", "		/* 4 */
				"c.\"default\", "	/* 5 */
				"c.number "		/* 6 */
			 "FROM sys._columns c, "
			      "sys._tables t, "
			      "sys.schemas s "
			 "WHERE c.table_id = t.id AND "
			       "'%s' = t.name AND "
			       "t.schema_id = s.id AND "
			       "s.name = '%s' "
			 "ORDER BY number", tname, schema);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	slen = mapi_get_len(hdl, 0);
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *c_name = mapi_fetch_field(hdl, 0);
		const char *c_type = mapi_fetch_field(hdl, 1);
		const char *c_type_digits = mapi_fetch_field(hdl, 2);
		const char *c_type_scale = mapi_fetch_field(hdl, 3);
		const char *c_null = mapi_fetch_field(hdl, 4);
		const char *c_default = mapi_fetch_field(hdl, 5);
		int space;

		if (mapi_error(mid))
			goto bailout;
		if (cnt)
			mnstr_printf(toConsole, ",\n");

		mnstr_printf(toConsole, "\t\"%s\"%*s ",
			     c_name, CAP(slen - strlen(c_name)), "");
		space = dump_type(mid, toConsole, c_type, c_type_digits, c_type_scale, hashge);
		if (strcmp(c_null, "false") == 0) {
			mnstr_printf(toConsole, "%*s NOT NULL",
					CAP(13 - space), "");
			space = 13;
		}
		if (c_default != NULL)
			mnstr_printf(toConsole, "%*s DEFAULT %s",
					CAP(13 - space), "", c_default);

		cnt++;
		if (mnstr_errnr(toConsole))
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;
	/* presumably we don't need to order on id, since there should
	   only be a single primary key, but it doesn't hurt, and the
	   code is then close to the code for the uniqueness
	   constraint */
	if (tid)
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "		/* 1 */
				"k.name, "		/* 2 */
				"k.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k "
			 "WHERE kc.id = k.id AND "
			       "k.table_id = %s AND "
			       "k.type = 0 "
			 "ORDER BY id, nr", tid);
	else
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "		/* 1 */
				"k.name, "		/* 2 */
				"k.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k, "
			      "sys.schemas s, "
			      "sys._tables t "
			 "WHERE kc.id = k.id AND "
			       "k.table_id = t.id AND "
			       "k.type = 0 AND "
			       "t.schema_id = s.id AND "
			       "s.name = '%s' AND "
			       "t.name = '%s' "
			 "ORDER BY id, nr", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *c_column = mapi_fetch_field(hdl, 0);
		const char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid))
			goto bailout;
		if (cnt == 0) {
			mnstr_printf(toConsole, ",\n\t");
			if (k_name) {
				mnstr_printf(toConsole, "CONSTRAINT \"%s\" ",
					k_name);
			}
			mnstr_printf(toConsole, "PRIMARY KEY (");
		} else
			mnstr_printf(toConsole, ", ");
		mnstr_printf(toConsole, "\"%s\"", c_column);
		cnt++;
		if (mnstr_errnr(toConsole))
			goto bailout;
	}
	if (cnt)
		mnstr_printf(toConsole, ")");
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	if (tid)
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "		/* 1 */
				"k.name, "		/* 2 */
				"k.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k "
			 "WHERE kc.id = k.id AND "
			       "k.table_id = %s AND "
			       "k.type = 1 "
			 "ORDER BY id, nr", tid);
	else
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "		/* 1 */
				"k.name, "		/* 2 */
				"k.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k, "
			      "sys.schemas s, "
			      "sys._tables t "
			 "WHERE kc.id = k.id AND "
			       "k.table_id = t.id AND "
			       "k.type = 1 AND "
			       "t.schema_id = s.id AND "
			       "s.name = '%s' AND "
			       "t.name = '%s' "
			 "ORDER BY id, nr", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *c_column = mapi_fetch_field(hdl, 0);
		const char *kc_nr = mapi_fetch_field(hdl, 1);
		const char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid))
			goto bailout;
		if (strcmp(kc_nr, "0") == 0) {
			if (cnt)
				mnstr_write(toConsole, ")", 1, 1);
			mnstr_printf(toConsole, ",\n\t");
			if (k_name) {
				mnstr_printf(toConsole, "CONSTRAINT \"%s\" ",
					k_name);
			}
			mnstr_printf(toConsole, "UNIQUE (");
			cnt = 1;
		} else
			mnstr_printf(toConsole, ", ");
		mnstr_printf(toConsole, "\"%s\"", c_column);
		if (mnstr_errnr(toConsole))
			goto bailout;
	}
	if (cnt)
		mnstr_write(toConsole, ")", 1, 1);
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	if (foreign &&
	    dump_foreign_keys(mid, schema, tname, tid, toConsole))
		goto bailout;

	mnstr_printf(toConsole, "\n");

	mnstr_printf(toConsole, ")");

	free(query);
	return 0;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	if (query != NULL)
		free(query);
	return 1;
}

int
describe_table(Mapi mid, const char *schema, const char *tname, stream *toConsole, int foreign)
{
	int cnt;
	MapiHdl hdl = NULL;
	char *query;
	char *view = NULL;
	char *remark = NULL;
	int type = 0;
	size_t maxquerylen;
	char *sname = NULL;
	bool hashge;
	const char *comments_clause = get_comments_clause(mid);

	if (comments_clause == NULL)
		return 1;

	if (schema == NULL) {
		if ((sname = strchr(tname, '.')) != NULL) {
			size_t len = sname - tname;

			sname = malloc(len + 1);
			strncpy(sname, tname, len);
			sname[len] = 0;
			tname += len + 1;
		} else if ((sname = get_schema(mid)) == NULL) {
			return 1;
		}
		schema = sname;
	}

	hashge = has_hugeint(mid);

	maxquerylen = 5120 + strlen(tname) + strlen(schema);

	query = malloc(maxquerylen);
	snprintf(query, maxquerylen,
		 "%s "
		 "SELECT t.name, t.query, t.type, c.remark "
		 "FROM sys.schemas s, sys._tables t LEFT OUTER JOIN comments c ON t.id = c.id "
		 "WHERE s.name = '%s' AND "
		       "t.schema_id = s.id AND "
		       "t.name = '%s'",
		 comments_clause,
		 schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		cnt++;
		view = mapi_fetch_field(hdl, 2);
		if (view)
			type = atoi(view);
		view = mapi_fetch_field(hdl, 1);
		remark = mapi_fetch_field(hdl, 3);
	}
	if (mapi_error(mid)) {
		view = NULL;
		remark = NULL;
		goto bailout;
	}
	if (view) {
		/* skip initial comments and empty lines */
		while ((view[0] == '-' && view[1] == '-') || view[0] == '\n') {
			view = strchr(view, '\n');
			if (view == NULL)
				view = "";
			else
				view++;
		}
		view = strdup(view);
	}
	if (remark)
		remark = strdup(remark);
	mapi_close_handle(hdl);
	hdl = NULL;

	if (cnt != 1) {
		if (cnt == 0)
			fprintf(stderr, "table %s.%s does not exist\n", schema, tname);
		else
			fprintf(stderr, "table %s.%s is not unique, corrupt catalog?\n",
					schema, tname);
		goto bailout;
	}

	if (type == 1) {
		/* the table is actually a view */
		mnstr_printf(toConsole, "%s\n", view);
		comment_on(toConsole, "VIEW", schema, tname, NULL, remark);
	} else {
		/* the table is a real table */
		mnstr_printf(toConsole, "CREATE %sTABLE \"%s\".\"%s\" ",
			     type == 3 ? "MERGE " :
			     type == 4 ? "STREAM " :
			     type == 5 ? "REMOTE " :
			     type == 6 ? "REPLICA " :
			     "",
			     schema, tname);

		if (dump_column_definition(mid, toConsole, schema, tname, NULL, foreign, hashge))
			goto bailout;
		if (type == 5) { /* remote table */
			char *rt_user = NULL;
			char *rt_hash = NULL;
			snprintf(query, maxquerylen,
				 "SELECT username, hash FROM sys.remote_table_credentials('%s.%s')",
				 schema, tname);
			if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
				goto bailout;
			cnt = 0;
			while(mapi_fetch_row(hdl) != 0) {
				rt_user = mapi_fetch_field(hdl, 0);
				rt_hash = mapi_fetch_field(hdl, 1);
			}
			mnstr_printf(toConsole, " ON '%s' WITH USER '%s' ENCRYPTED PASSWORD '%s'", view, rt_user, rt_hash);
			mapi_close_handle(hdl);
			hdl = NULL;
		}
		mnstr_printf(toConsole, ";\n");
		comment_on(toConsole, "TABLE", schema, tname, NULL, remark);

		snprintf(query, maxquerylen,
			 "SELECT i.name, "		/* 0 */
				"k.name, "		/* 1 */
				"kc.nr, "		/* 2 */
				"c.name, "		/* 3 */
				"i.type "		/* 4 */
			 "FROM sys.idxs AS i "
			 	"LEFT JOIN sys.keys AS k ON i.name = k.name, "
			      "sys.objects AS kc, "
			      "sys._columns AS c, "
			      "sys.schemas s, "
			      "sys._tables AS t "
			 "WHERE i.table_id = t.id AND "
			       "i.id = kc.id AND "
			       "t.id = c.table_id AND "
			       "kc.name = c.name AND "
			       "(k.type IS NULL OR k.type = 1) AND "
			       "t.schema_id = s.id AND "
			       "s.name = '%s' AND "
			       "t.name = '%s' AND "
			       "i.type in (0, 4, 5) "
			 "ORDER BY i.name, kc.nr", schema, tname);
		if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
			goto bailout;
		cnt = 0;
		while (mapi_fetch_row(hdl) != 0) {
			const char *i_name = mapi_fetch_field(hdl, 0);
			const char *k_name = mapi_fetch_field(hdl, 1);
			const char *kc_nr = mapi_fetch_field(hdl, 2);
			const char *c_name = mapi_fetch_field(hdl, 3);
			const char *i_type = mapi_fetch_field(hdl, 4);

			if (mapi_error(mid))
				goto bailout;
			if (k_name != NULL) {
				/* unique key, already handled */
				continue;
			}

			if (strcmp(kc_nr, "0") == 0) {
				if (cnt)
					mnstr_printf(toConsole, ");\n");
				switch (atoi(i_type)) {
				case 0: /* hash_idx */
					mnstr_printf(toConsole,
						     "CREATE INDEX \"%s\" ON \"%s\".\"%s\" (",
						     i_name, schema, tname);
					break;
				case 5: /* ordered_idx */
					mnstr_printf(toConsole,
						     "CREATE ORDERED INDEX \"%s\" ON \"%s\".\"%s\" (",
						     i_name, schema, tname);
					break;
				case 4: /* imprints_idx */
					mnstr_printf(toConsole,
						     "CREATE IMPRINTS INDEX \"%s\" ON \"%s\".\"%s\" (",
						     i_name, schema, tname);
					break;
				default:
					/* cannot happen due to WHERE clause */
					goto bailout;
				}
				cnt = 1;
			} else
				mnstr_printf(toConsole, ", ");
			mnstr_printf(toConsole, "\"%s\"", c_name);
			if (mnstr_errnr(toConsole))
				goto bailout;
		}
		mapi_close_handle(hdl);
		hdl = NULL;
		if (cnt)
			mnstr_printf(toConsole, ");\n");
		snprintf(query, maxquerylen,
			"%s "
			 "SELECT i.name, c.remark "
			 "FROM sys.idxs i, comments c "
			 "WHERE i.id = c.id "
			   "AND i.table_id = (SELECT id FROM sys._tables WHERE schema_id = (select id FROM sys.schemas WHERE name = '%s') AND name = '%s') "
			 "ORDER BY i.name",
			 comments_clause,
			 schema, tname);
		if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
			goto bailout;
		while (mapi_fetch_row(hdl) != 0) {
			comment_on(toConsole, "INDEX", schema,
				   mapi_fetch_field(hdl, 0), NULL,
				   mapi_fetch_field(hdl, 1));
		}
		mapi_close_handle(hdl);
		hdl = NULL;
	}

	snprintf(query, maxquerylen,
		"%s "
		 "SELECT col.name, com.remark "
		 "FROM sys._columns col, comments com "
		 "WHERE col.id = com.id "
		   "AND col.table_id = (SELECT id FROM sys._tables WHERE schema_id = (SELECT id FROM sys.schemas WHERE name = '%s') AND name = '%s') "
		 "ORDER BY number",
		 comments_clause,
		 schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	while (mapi_fetch_row(hdl) != 0) {
		comment_on(toConsole, "COLUMN", schema, tname,
				mapi_fetch_field(hdl, 0),
				mapi_fetch_field(hdl, 1));
	}
	mapi_close_handle(hdl);
	hdl = NULL;
	if (mapi_error(mid))
		goto bailout;

	if (view)
		free(view);
	if (remark)
		free(remark);
	if (query != NULL)
		free(query);
	if (sname != NULL)
		free(sname);
	return 0;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	if (view)
		free(view);
	if (remark)
		free(remark);
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	return 1;
}

int
describe_sequence(Mapi mid, const char *schema, const char *tname, stream *toConsole)
{
	MapiHdl hdl = NULL;
	char *query;
	size_t maxquerylen;
	char *sname = NULL;
	const char *comments_clause = get_comments_clause(mid);

	if (comments_clause == NULL)
		return 1;

	if (schema == NULL) {
		if ((sname = strchr(tname, '.')) != NULL) {
			size_t len = sname - tname;

			sname = malloc(len + 1);
			strncpy(sname, tname, len);
			sname[len] = 0;
			tname += len + 1;
		} else if ((sname = get_schema(mid)) == NULL) {
			return 1;
		}
		schema = sname;
	}

	maxquerylen = 5120 + strlen(tname) + strlen(schema);

	query = malloc(maxquerylen);
	snprintf(query, maxquerylen,
		"%s "
		"SELECT s.name, "
		       "seq.name, "
		       "get_value_for(s.name, seq.name), "
		       "seq.\"minvalue\", "
		       "seq.\"maxvalue\", "
		       "seq.\"increment\", "
		       "seq.\"cycle\", "
		       "rem.\"remark\" "
		"FROM sys.sequences seq LEFT OUTER JOIN comments rem ON seq.id = rem.id, "
		     "sys.schemas s "
		"WHERE s.id = seq.schema_id AND "
		      "s.name = '%s' AND "
		      "seq.name = '%s' "
		"ORDER BY s.name, seq.name",
		comments_clause,
		schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *schema = mapi_fetch_field(hdl, 0);
		const char *name = mapi_fetch_field(hdl, 1);
		const char *start = mapi_fetch_field(hdl, 2);
		const char *minvalue = mapi_fetch_field(hdl, 3);
		const char *maxvalue = mapi_fetch_field(hdl, 4);
		const char *increment = mapi_fetch_field(hdl, 5);
		const char *cycle = mapi_fetch_field(hdl, 6);
		const char *remark = mapi_fetch_field(hdl, 7);

		mnstr_printf(toConsole,
				 "CREATE SEQUENCE \"%s\".\"%s\" START WITH %s",
				 schema, name, start);
		if (strcmp(increment, "1") != 0)
			mnstr_printf(toConsole, " INCREMENT BY %s", increment);
		if (strcmp(minvalue, "0") != 0)
			mnstr_printf(toConsole, " MINVALUE %s", minvalue);
		if (strcmp(maxvalue, "0") != 0)
			mnstr_printf(toConsole, " MAXVALUE %s", maxvalue);
		mnstr_printf(toConsole, " %sCYCLE;\n", strcmp(cycle, "true") == 0 ? "" : "NO ");
		comment_on(toConsole, "SEQUENCE", schema, name, NULL, remark);
		if (mnstr_errnr(toConsole)) {
			mapi_close_handle(hdl);
			hdl = NULL;
			goto bailout;
		}
	}
	if (mapi_error(mid))
		goto bailout;
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	mapi_close_handle(hdl);
	hdl = NULL;
	return 0;

bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	return 1;
}

int
describe_schema(Mapi mid, const char *sname, stream *toConsole)
{
	MapiHdl hdl = NULL;
	char schemas[5120];
	const char *comments_clause = get_comments_clause(mid);

	if (comments_clause == NULL)
		return 1;

	snprintf(schemas, sizeof(schemas),
		"%s "
		"SELECT s.name, a.name, c.remark "
		"FROM sys.auths a, "
		     "sys.schemas s LEFT OUTER JOIN comments c ON s.id = c.id "
		"WHERE s.\"authorization\" = a.id AND "
		      "s.name = '%s' "
		"ORDER BY s.name",
		comments_clause,
		sname);

	if ((hdl = mapi_query(mid, schemas)) == NULL || mapi_error(mid)) {
		if (hdl) {
			if (mapi_result_error(hdl))
				mapi_explain_result(hdl, stderr);
			else
				mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);

		return 1;
	}

	while (mapi_fetch_row(hdl) != 0) {
		const char *sname = mapi_fetch_field(hdl, 0);
		const char *aname = mapi_fetch_field(hdl, 1);
		const char *remark = mapi_fetch_field(hdl, 2);

		mnstr_printf(toConsole, "CREATE SCHEMA \"%s\"", sname);
		if (strcmp(aname, "sysadmin") != 0) {
			mnstr_printf(toConsole,
					 " AUTHORIZATION \"%s\"", aname);
		}
		mnstr_printf(toConsole, ";\n");
		comment_on(toConsole, "SCHEMA", sname, NULL, NULL, remark);
	}

	return 0;
}

static int
dump_table_data(Mapi mid, const char *schema, const char *tname, stream *toConsole,
		bool useInserts)
{
	int cnt, i;
	int64_t rows;
	MapiHdl hdl = NULL;
	char *query = NULL;
	size_t maxquerylen;
	unsigned char *string = NULL;
	char *sname = NULL;

	if (schema == NULL) {
		if ((sname = strchr(tname, '.')) != NULL) {
			size_t len = sname - tname;

			sname = malloc(len + 1);
			if (sname == NULL)
				goto bailout;
			strncpy(sname, tname, len);
			sname[len] = 0;
			tname += len + 1;
		} else if ((sname = get_schema(mid)) == NULL) {
			goto bailout;
		}
		schema = sname;
	}

	maxquerylen = 5120 + strlen(tname) + strlen(schema);
	query = malloc(maxquerylen);
	if (query == NULL)
		goto bailout;

	snprintf(query, maxquerylen,
		 "SELECT t.name, t.query, t.type "
		 "FROM sys._tables t, sys.schemas s "
		 "WHERE s.name = '%s' AND "
		       "t.schema_id = s.id AND "
		       "t.name = '%s'",
		 schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	if (mapi_rows_affected(hdl) != 1) {
		if (mapi_rows_affected(hdl) == 0)
			fprintf(stderr, "table '%s.%s' does not exist\n", schema, tname);
		else
			fprintf(stderr, "table '%s.%s' is not unique\n", schema, tname);
		goto bailout;
	}
	while ((mapi_fetch_row(hdl)) != 0) {
		if (strcmp(mapi_fetch_field(hdl, 2), "1") == 0) {
			/* the table is actually a view */
			goto doreturn;
		}
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	snprintf(query, maxquerylen, "SELECT * FROM \"%s\".\"%s\"",
		 schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	rows = mapi_get_row_count(hdl);
	if (rows == 0) {
		/* nothing more to do */
		goto doreturn;
	}

	cnt = mapi_get_field_count(hdl);
	if (cnt < 1 || cnt >= 1 << 29)
		goto bailout;	/* ridiculous number of columns */
	if (!useInserts) {
		mnstr_printf(toConsole,
			     "COPY %" PRId64 " RECORDS INTO \"%s\".\"%s\" "
			     "FROM stdin USING DELIMITERS '\\t','\\n','\"';\n",
			     rows, schema, tname);
	}
	string = malloc(sizeof(unsigned char) * cnt);
	if (string == NULL)
		goto bailout;
	for (i = 0; i < cnt; i++) {
		string[i] = (strcmp(mapi_get_type(hdl, i), "char") == 0 ||
			     strcmp(mapi_get_type(hdl, i), "varchar") == 0 ||
			     strcmp(mapi_get_type(hdl, i), "clob") == 0 ||
			     strcmp(mapi_get_type(hdl, i), "timestamp") == 0 ||
			     strcmp(mapi_get_type(hdl, i), "timestamptz") == 0);
	}
	while (mapi_fetch_row(hdl)) {
		const char *s;

		if (useInserts)
			mnstr_printf(toConsole, "INSERT INTO \"%s\".\"%s\" VALUES (",
				     schema, tname);

		for (i = 0; i < cnt; i++) {
			s = mapi_fetch_field(hdl, i);
			if (s == NULL)
				mnstr_printf(toConsole, "NULL");
			else if (useInserts) {
				const char *tp = mapi_get_type(hdl, i);
				if (strcmp(tp, "sec_interval") == 0) {
					const char *p = strchr(s, '.');
					if (p == NULL)
						p = s + strlen(s);
					mnstr_printf(toConsole, "INTERVAL '%.*s' SECOND", (int) (p - s), s);
				} else if (strcmp(tp, "month_interval") == 0)
					mnstr_printf(toConsole, "INTERVAL '%s' MONTH", s);
				else if (strcmp(tp, "timestamptz") == 0)
					mnstr_printf(toConsole, "TIMESTAMP WITH TIME ZONE '%s'", s);
				else if (strcmp(tp, "timestamp") == 0)
					mnstr_printf(toConsole, "TIMESTAMP '%s'", s);
				else if (strcmp(tp, "timetz") == 0)
					mnstr_printf(toConsole, "TIME WITH TIME ZONE '%s'", s);
				else if (strcmp(tp, "time") == 0)
					mnstr_printf(toConsole, "TIME '%s'", s);
				else if (strcmp(tp, "date") == 0)
					mnstr_printf(toConsole, "DATE '%s'", s);
				else if (strcmp(tp, "blob") == 0)
					mnstr_printf(toConsole, "BINARY LARGE OBJECT '%s'", s);
				else if (strcmp(tp, "inet") == 0 ||
					 strcmp(tp, "json") == 0 ||
					 strcmp(tp, "url") == 0 ||
					 strcmp(tp, "uuid") == 0 ||
					 string[i])
					quoted_print(toConsole, s, true);
				else
					mnstr_printf(toConsole, "%s", s);
			} else if (string[i]) {
				/* write double or single-quoted
				   string with certain characters
				   escaped */
				quoted_print(toConsole, s, useInserts);
			} else
				mnstr_printf(toConsole, "%s", s);

			if (useInserts) {
				if (i < cnt - 1)
					mnstr_printf(toConsole, ", ");
				else
					mnstr_printf(toConsole, ");\n");
			} else {
				if (i < cnt - 1)
					mnstr_write(toConsole, "\t", 1, 1);
				else
					mnstr_write(toConsole, "\n", 1, 1);
			}
		}
		if (mnstr_errnr(toConsole))
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	free(string);

  doreturn:
	if (hdl)
		mapi_close_handle(hdl);
	if (query != NULL)
		free(query);
	if (sname != NULL)
		free(sname);
	return 0;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else
		fprintf(stderr, "malloc failure\n");
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	if (string != NULL)
		free(string);
	return 1;
}

int
dump_table(Mapi mid, const char *schema, const char *tname, stream *toConsole, int describe, int foreign, bool useInserts)
{
	int rc;

	rc = describe_table(mid, schema, tname, toConsole, foreign);
	if (rc == 0 && !describe)
		rc = dump_table_data(mid, schema, tname, toConsole, useInserts);
	return rc;
}

static int
dump_function(Mapi mid, stream *toConsole, const char *fid, bool hashge)
{
	MapiHdl hdl;
	size_t query_size = 5120 + strlen(fid);
	int query_len;
	char *query;
	const char *sep;
	char *ffunc = NULL, *flkey = NULL, *remark = NULL;
	char *sname, *fname, *ftkey;
	int flang, ftype;
	const char *comments_clause = get_comments_clause(mid);

	if (comments_clause == NULL)
		return 1;

	if ((query = malloc(query_size)) == NULL)
		return 1;

	query_len = snprintf(query, query_size,
		      "%s "
		      "SELECT f.id, "
			     "f.func, "
			     "f.language, "
			     "f.type, "
			     "s.name, "
			     "f.name, "
			     "ft.function_type_keyword, "
			     "fl.language_keyword, "
		             "c.remark "
		      "FROM sys.functions f "
			   "JOIN sys.schemas s ON f.schema_id = s.id "
			   "JOIN function_types ft ON f.type = ft.function_type_id "
			   "LEFT OUTER JOIN function_languages fl ON f.language = fl.language_id "
			   "LEFT OUTER JOIN comments c ON f.id = c.id "
		      "WHERE f.id = %s",
		      comments_clause, fid);
	assert(query_len < (int) query_size);
	if (query_len < 0 || query_len >= (int) query_size ||
	    (hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		free(query);
		return 1;
	}

	if (mapi_fetch_row(hdl) == 0) {
		free(query);
		mapi_close_handle(hdl);
		return 0;	/* no such function, apparently */
	}
	ffunc = mapi_fetch_field(hdl, 1);
	flang = atoi(mapi_fetch_field(hdl, 2));
	ftype = atoi(mapi_fetch_field(hdl, 3));
	sname = mapi_fetch_field(hdl, 4);
	fname = mapi_fetch_field(hdl, 5);
	ftkey = mapi_fetch_field(hdl, 6);
	flkey = mapi_fetch_field(hdl, 7);
	remark = mapi_fetch_field(hdl, 8);
	if (remark) {
		remark = strdup(remark);
		sname = strdup(sname);
		fname = strdup(fname);
		ftkey = strdup(ftkey);
	}
	if (flang == 1 || flang == 2) {
		/* all information is stored in the func column
		 * first skip initial comments and empty lines */
		while ((ffunc[0] == '-' && ffunc[1] == '-') || ffunc[0] == '\n') {
			ffunc = strchr(ffunc, '\n');
			if (ffunc == NULL)
				ffunc = "";
			else
				ffunc++;
		}
		mnstr_printf(toConsole, "%s\n", ffunc);
		if (remark == NULL) {
			mapi_close_handle(hdl);
			free(query);
			return 0;
		}
	} else {
		mnstr_printf(toConsole, "CREATE %s ", ftkey);
		quoted_print(toConsole, sname, false);
		mnstr_printf(toConsole, ".");
		quoted_print(toConsole, fname, false);
		mnstr_printf(toConsole, "(");
	}
	/* strdup these two because they are needed after another query */
	ffunc = strdup(ffunc);
	if (flkey)
		flkey = strdup(flkey);
	query_len = snprintf(query, query_size, "SELECT a.name, a.type, a.type_digits, a.type_scale, a.inout FROM sys.args a, sys.functions f WHERE a.func_id = f.id AND f.id = %s ORDER BY a.inout DESC, a.number", fid);
	assert(query_len < (int) query_size);
	if (query_len < 0 || query_len >= (int) query_size) {
		free(ffunc);
		free(flkey);
		if (remark)
			free(remark);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);
	hdl = mapi_query(mid, query);
	free(query);
	if (hdl == NULL || mapi_error(mid)) {
		free(ffunc);
		free(flkey);
		if (remark)
			free(remark);
		return 1;
	}
	if (flang != 1 && flang != 2) {
		sep = "";
		while (mapi_fetch_row(hdl) != 0) {
			const char *aname = mapi_fetch_field(hdl, 0);
			const char *atype = mapi_fetch_field(hdl, 1);
			const char *adigs = mapi_fetch_field(hdl, 2);
			const char *ascal = mapi_fetch_field(hdl, 3);
			const char *ainou = mapi_fetch_field(hdl, 4);

			if (strcmp(ainou, "0") == 0) {
				/* end of arguments */
				break;
			}

			mnstr_printf(toConsole, "%s", sep);
			quoted_print(toConsole, aname, false);
			mnstr_printf(toConsole, " ");
			dump_type(mid, toConsole, atype, adigs, ascal, hashge);
			sep = ", ";
		}
		mnstr_printf(toConsole, ")");
		if (ftype == 1 || ftype == 3 || ftype == 5) {
			sep = "TABLE (";
			mnstr_printf(toConsole, " RETURNS ");
			do {
				const char *aname = mapi_fetch_field(hdl, 0);
				const char *atype = mapi_fetch_field(hdl, 1);
				const char *adigs = mapi_fetch_field(hdl, 2);
				const char *ascal = mapi_fetch_field(hdl, 3);

				assert(strcmp(mapi_fetch_field(hdl, 4), "0") == 0);
				if (ftype == 5) {
					mnstr_printf(toConsole, "%s", sep);
					quoted_print(toConsole, aname, false);
					mnstr_printf(toConsole, " ");
					sep = ", ";
				}
				dump_type(mid, toConsole, atype, adigs, ascal, hashge);
			} while (mapi_fetch_row(hdl) != 0);
		}
		if (flkey) {
			mnstr_printf(toConsole, " LANGUAGE %s", flkey);
			free(flkey);
		}
		mnstr_printf(toConsole, "\n%s\n", ffunc);
	}
	free(ffunc);
	if (remark) {
		if (mapi_seek_row(hdl, 0, MAPI_SEEK_SET) != MOK ||
		    mnstr_printf(toConsole, "COMMENT ON %s ", ftkey) < 0 ||
		    quoted_print(toConsole, sname, false) < 0 ||
		    mnstr_printf(toConsole, ".") < 0 ||
		    quoted_print(toConsole, fname, false) < 0 ||
		    mnstr_printf(toConsole, "(") < 0) {
			free(sname);
			free(fname);
			free(ftkey);
			mapi_close_handle(hdl);
			return 1;
		}
		free(sname);
		free(fname);
		free(ftkey);
		sep = "";
		while (mapi_fetch_row(hdl) != 0) {
			const char *atype = mapi_fetch_field(hdl, 1);
			const char *adigs = mapi_fetch_field(hdl, 2);
			const char *ascal = mapi_fetch_field(hdl, 3);
			const char *ainou = mapi_fetch_field(hdl, 4);

			if (strcmp(ainou, "0") == 0) {
				/* end of arguments */
				break;
			}
			mnstr_printf(toConsole, "%s", sep);
			dump_type(mid, toConsole, atype, adigs, ascal, hashge);
			sep = ", ";
		}
		mnstr_printf(toConsole, ") IS ");
		quoted_print(toConsole, remark, true);
		mnstr_printf(toConsole, ";\n");
		free(remark);
	}
	mapi_close_handle(hdl);
	return 0;
}

int
dump_functions(Mapi mid, stream *toConsole, char set_schema, const char *sname, const char *fname, const char *id)
{
	MapiHdl hdl = NULL;
	char *query;
	size_t query_size;
	int query_len;
	bool hashge;
	char *to_free = NULL;
	bool wantSystem;
	long prev_sid;

	if (fname != NULL) {
		/* dump a single function */
		wantSystem = true;

		if (sname == NULL) {
			/* no schema given, so figure it out */
			const char *dot = strchr(fname, '.');
			if (dot != NULL) {
				size_t len = dot - fname;

				to_free = malloc(len + 1);
				strncpy(to_free, fname, len);
				to_free[len] = 0;
				fname += len + 1;
			} else if ((to_free = get_schema(mid)) == NULL) {
				return 1;
			}
			sname = to_free;
		}
	} else {
		wantSystem = false;
	}

	hashge = has_hugeint(mid);

	query_size = 5120 + (sname ? strlen(sname) : 0) + (fname ? strlen(fname) : 0);
	query = malloc(query_size);
	if (query == NULL) {
		if (to_free)
			free(to_free);
		return 1;
	}

	query_len = snprintf(query, query_size,
		      "SELECT s.id, s.name, f.id "
		      "FROM sys.schemas s "
			   "JOIN sys.functions f ON s.id = f.schema_id "
		      "WHERE f.language > 0 ");
	if (id) {
		query_len += snprintf(query + query_len,
				      query_size - query_len,
				      "AND f.id = %s ", id);
	} else {
		if (sname)
			query_len += snprintf(query + query_len,
					      query_size - query_len,
					      "AND s.name = '%s' ", sname);
		if (fname)
			query_len += snprintf(query + query_len,
					      query_size - query_len,
					      "AND f.name = '%s' ", fname);
		if (!wantSystem)
			query_len += snprintf(query + query_len,
					      query_size - query_len,
					      "AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions) ");
	}
	query_len += snprintf(query + query_len, query_size - query_len,
			      "ORDER BY f.func, f.id");
	assert(query_len < (int) query_size);
	if (query_len >= (int) query_size)
		goto bailout;

	hdl = mapi_query(mid, query);
	free(query);
	if (hdl == NULL || mapi_error(mid))
		goto bailout;
	prev_sid = 0;
	while (!mnstr_errnr(toConsole) && mapi_fetch_row(hdl) != 0) {
		long sid = strtol(mapi_fetch_field(hdl, 0), NULL, 10);
		const char *schema = mapi_fetch_field(hdl, 1);
		const char *fid = mapi_fetch_field(hdl, 2);
		if (set_schema && sid != prev_sid) {
			mnstr_printf(toConsole, "SET SCHEMA ");
			quoted_print(toConsole, schema, false);
			mnstr_printf(toConsole, ";\n");
			prev_sid = sid;
		}
		dump_function(mid, toConsole, fid, hashge);
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if (to_free)
		free(to_free);
	return mnstr_errnr(toConsole) != 0;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else
		mapi_explain(mid, stderr);
	if (to_free)
		free(to_free);
	return 1;
}

int
dump_database(Mapi mid, stream *toConsole, int describe, bool useInserts)
{
	const char *start = "START TRANSACTION";
	const char *end = "ROLLBACK";
	const char *users =
		"SELECT ui.name, "
		       "ui.fullname, "
		       "password_hash(ui.name), "
		       "s.name "
		"FROM sys.db_user_info ui, "
		     "sys.schemas s "
		"WHERE ui.default_schema = s.id AND "
		      "ui.name <> 'monetdb' "
		"ORDER BY ui.name";
	const char *roles =
		"SELECT name "
		"FROM sys.auths "
		"WHERE name NOT IN (SELECT name "
				       "FROM sys.db_user_info) AND "
		      "grantor <> 0 "
		"ORDER BY name";
	const char *grants =
		"SELECT a1.name, "
		       "a2.name "
		"FROM sys.auths a1, "
		     "sys.auths a2, "
		     "sys.user_role ur "
		"WHERE a1.id = ur.login_id AND "
		      "a2.id = ur.role_id "
		"ORDER BY a1.name, a2.name";
	const char *table_grants =
		"SELECT s.name, t.name, "
		       "a.name, "
		       "sum(p.privileges), "
		       "g.name, p.grantable "
		"FROM sys.schemas s, sys.tables t, "
		     "sys.auths a, sys.privileges p, "
		     "sys.auths g "
		"WHERE p.obj_id = t.id AND "
		      "p.auth_id = a.id AND "
		      "t.schema_id = s.id AND "
		      "t.system = FALSE AND "
		      "p.grantor = g.id "
		"GROUP BY s.name, t.name, a.name, g.name, p.grantable "
		"ORDER BY s.name, t.name, a.name, g.name, p.grantable";
	const char *column_grants =
		"SELECT s.name, t.name, "
		       "c.name, a.name, "
		       "CASE p.privileges "
			    "WHEN 1 THEN 'SELECT' "
			    "WHEN 2 THEN 'UPDATE' "
			    "WHEN 4 THEN 'INSERT' "
			    "WHEN 8 THEN 'DELETE' "
			    "WHEN 16 THEN 'EXECUTE' "
			    "WHEN 32 THEN 'GRANT' "
			    "WHEN 64 THEN 'TRUNCATE' END, "
		       "g.name, p.grantable "
		"FROM sys.schemas s, sys.tables t, "
		     "sys.columns c, sys.auths a, "
		     "sys.privileges p, sys.auths g "
		"WHERE p.obj_id = c.id AND "
		      "c.table_id = t.id AND "
		      "p.auth_id = a.id AND "
		      "t.schema_id = s.id AND "
		      "t.system = FALSE AND "
		      "p.grantor = g.id "
		"ORDER BY s.name, t.name, c.name, a.name, g.name, p.grantable";
	const char *function_grants =
		"SELECT s.name, f.name, a.name, "
		       "CASE p.privileges "
			    "WHEN 1 THEN 'SELECT' "
			    "WHEN 2 THEN 'UPDATE' "
			    "WHEN 4 THEN 'INSERT' "
			    "WHEN 8 THEN 'DELETE' "
			    "WHEN 16 THEN 'EXECUTE' "
			    "WHEN 32 THEN 'GRANT' "
			    "WHEN 64 THEN 'TRUNCATE' END, "
		       "g.name, p.grantable "
		"FROM sys.schemas s, sys.functions f, "
		     "sys.auths a, sys.privileges p, sys.auths g "
		"WHERE s.id = f.schema_id AND "
		      "f.id = p.obj_id AND "
		      "p.auth_id = a.id AND "
		      "p.grantor = g.id "
		      "AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions) "
		"ORDER BY s.name, f.name, a.name, g.name, p.grantable";
	const char *schemas =
		"SELECT s.name, a.name, rem.remark "
		"FROM sys.schemas s LEFT OUTER JOIN comments rem ON s.id = rem.id, "
		     "sys.auths a "
		"WHERE s.\"authorization\" = a.id AND "
		      "s.system = FALSE "
		"ORDER BY s.name";
	const char *sequences1 =
		"SELECT sch.name, seq.name, rem.remark "
		"FROM sys.schemas sch, "
		     "sys.sequences seq LEFT OUTER JOIN comments rem ON seq.id = rem.id "
		"WHERE sch.id = seq.schema_id "
		"ORDER BY sch.name, seq.name";
	const char *sequences2 =
		"SELECT s.name, "
		     "seq.name, "
		     "get_value_for(s.name, seq.name), "
		     "seq.\"minvalue\", "
		     "seq.\"maxvalue\", "
		     "seq.\"increment\", "
		     "seq.\"cycle\" "
		"FROM sys.sequences seq, "
		     "sys.schemas s "
		"WHERE s.id = seq.schema_id "
		"ORDER BY s.name, seq.name";
	const char *tables =
		"SELECT s.name AS sname, "
		       "t.name AS name, "
		       "t.type AS type "
		"FROM sys.schemas s, "
		     "sys._tables t "
		"WHERE t.type IN (0, 3, 4, 5, 6) AND "
		      "t.system = FALSE AND "
		      "s.id = t.schema_id AND "
		      "s.name <> 'tmp' "
		"ORDER BY t.id";
	const char *mergetables = "SELECT s1.name, t1.name, s2.name, t2.name FROM sys.schemas s1, sys._tables t1, sys.dependencies d, sys.schemas s2, sys._tables t2 WHERE t1.type = 3 AND t1.schema_id = s1.id AND s1.name <> 'tmp' AND t1.system = FALSE AND t1.id = d.depend_id AND d.id = t2.id AND t2.schema_id = s2.id ORDER BY t1.id, t2.id";
	/* we must dump views, functions and triggers in order of
	 * creation since they can refer to each other */
	const char *views_functions_triggers =
		", vft (sname, name, id, query, remark) AS ("
			"SELECT s.name AS sname, "
			       "t.name AS name, "
			       "t.id AS id, "
			       "t.query AS query, "
			       "rem.remark AS remark "
			"FROM sys.schemas s, "
			     "sys._tables t LEFT OUTER JOIN comments rem ON t.id = rem.id "
			"WHERE t.type = 1 AND "
			      "t.system = FALSE AND "
			      "s.id = t.schema_id AND "
			      "s.name <> 'tmp' "
			"UNION ALL "
			"SELECT s.name AS sname, "
			       "f.name AS name, "
			       "f.id AS id, "
			       "NULL AS query, "
			       "NULL AS remark " /* emitted separately */
			"FROM sys.schemas s, "
			     "sys.functions f "
			"WHERE s.id = f.schema_id "
			"AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions) "
			"UNION ALL "
			"SELECT s.name AS sname, "
			       "tr.name AS name, "
			       "tr.id AS id, "
			       "tr.\"statement\" AS query, "
			       "NULL AS remark " /* not available yet */
			"FROM sys.triggers tr, "
			     "sys.schemas s, "
			     "sys._tables t "
			"WHERE s.id = t.schema_id AND "
			      "t.id = tr.table_id AND t.system = FALSE"
		") "
		"SELECT id, sname, name, query, remark FROM vft ORDER BY id";
	char *sname = NULL;
	char *curschema = NULL;
	MapiHdl hdl = NULL;
	int rc = 0;
	char *query;
	size_t query_size = 5120;
	int query_len = 0;
	const char *comments_clause = get_comments_clause(mid);

	if (comments_clause == NULL)
		return 1;

	query = malloc(query_size);
	if (!query)
		goto bailout;

	/* start a transaction for the dump */
	if (!describe)
		mnstr_printf(toConsole, "START TRANSACTION;\n");

	if ((hdl = mapi_query(mid, start)) == NULL || mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	sname = get_schema(mid);
	if (sname == NULL)
		goto bailout2;
	if (strcmp(sname, "sys") == 0 || strcmp(sname, "tmp") == 0) {
		free(sname);
		sname = NULL;

		/* dump roles */
		if ((hdl = mapi_query(mid, roles)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *name = mapi_fetch_field(hdl, 0);

			mnstr_printf(toConsole, "CREATE ROLE \"%s\";\n", name);
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);

		/* dump users, part 1 */
		if ((hdl = mapi_query(mid, users)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *uname = mapi_fetch_field(hdl, 0);
			const char *fullname = mapi_fetch_field(hdl, 1);
			const char *pwhash = mapi_fetch_field(hdl, 2);
			const char *sname = mapi_fetch_field(hdl, 3);

			mnstr_printf(toConsole, "CREATE USER \"%s\" ", uname);
			if (describe)
				mnstr_printf(toConsole,
					     "WITH ENCRYPTED PASSWORD '%s' "
					     "NAME '%s' SCHEMA \"%s\";\n",
					     pwhash, fullname, sname);
			else
				mnstr_printf(toConsole,
					     "WITH ENCRYPTED PASSWORD '%s' "
					     "NAME '%s' SCHEMA \"sys\";\n",
					     pwhash, fullname);
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);

		/* dump schemas */
		query_len = snprintf(query, query_size, "%s %s",
				     comments_clause, schemas);
		assert(query_len < (int) query_size);
		if (query_len < 0 ||
		    query_len >= (int) query_size ||
		    (hdl = mapi_query(mid, query)) == NULL ||
		    mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *sname = mapi_fetch_field(hdl, 0);
			const char *aname = mapi_fetch_field(hdl, 1);
			const char *remark = mapi_fetch_field(hdl, 2);

			mnstr_printf(toConsole, "CREATE SCHEMA \"%s\"", sname);
			if (strcmp(aname, "sysadmin") != 0) {
				mnstr_printf(toConsole,
					     " AUTHORIZATION \"%s\"", aname);
			}
			mnstr_printf(toConsole, ";\n");
			comment_on(toConsole, "SCHEMA", sname, NULL, NULL, remark);
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);

		if (!describe) {
			/* dump users, part 2 */
			if ((hdl = mapi_query(mid, users)) == NULL ||
			    mapi_error(mid))
				goto bailout;

			while (mapi_fetch_row(hdl) != 0) {
				char *uname = mapi_fetch_field(hdl, 0);
				char *sname = mapi_fetch_field(hdl, 3);

				if (strcmp(sname, "sys") == 0)
					continue;
				mnstr_printf(toConsole,
					     "ALTER USER \"%s\" "
					     "SET SCHEMA \"%s\";\n",
					     uname, sname);
			}
			if (mapi_error(mid))
				goto bailout;
			mapi_close_handle(hdl);
		}

		/* grant user privileges */
		if ((hdl = mapi_query(mid, grants)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *uname = mapi_fetch_field(hdl, 0);
			const char *rname = mapi_fetch_field(hdl, 1);

			mnstr_printf(toConsole, "GRANT \"%s\" TO ", rname);
			if (strcmp(uname, "public") == 0)
				mnstr_printf(toConsole, "PUBLIC");
			else
				mnstr_printf(toConsole, "\"%s\"", uname);
			/* optional WITH ADMIN OPTION and FROM
			   (CURRENT_USER|CURRENT_ROLE) are ignored by
			   server, so we can't dump them */
			mnstr_printf(toConsole, ";\n");
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);
	} else {
		mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n", sname);
		curschema = strdup(sname);
	}

	/* dump sequences, part 1 */
	query_len = snprintf(query, query_size, "%s %s",
			     comments_clause, sequences1);
	assert(query_len < (int) query_size);
	if (query_len < 0 ||
	    query_len >= (int) query_size ||
	    (hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *schema = mapi_fetch_field(hdl, 0);
		const char *name = mapi_fetch_field(hdl, 1);
		const char *remark = mapi_fetch_field(hdl, 2);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		mnstr_printf(toConsole,
			     "CREATE SEQUENCE \"%s\".\"%s\" AS INTEGER;\n",
			     schema, name);
		comment_on(toConsole, "SEQUENCE", schema, name, NULL, remark);
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	/* dump tables, note that merge tables refer to other tables,
	 * so we make sure the contents of merge tables are added
	 * (ALTERed) after all table definitions */
	if ((hdl = mapi_query(mid, tables)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       !mnstr_errnr(toConsole) &&
	       mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *tname = mapi_fetch_field(hdl, 1);
		int type = atoi(mapi_fetch_field(hdl, 2));

		if (mapi_error(mid))
			goto bailout;
		if (schema == NULL) {
			/* cannot happen, but make analysis tools happy */
			continue;
		}
		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     curschema);
		}
		schema = strdup(schema);
		tname = strdup(tname);
		rc = dump_table(mid, schema, tname, toConsole, type == 3 || type == 5 ? 1 : describe, describe, useInserts);
		free(schema);
		free(tname);
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	if ((hdl = mapi_query(mid, mergetables)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       !mnstr_errnr(toConsole) &&
	       mapi_fetch_row(hdl) != 0) {
		const char *schema1 = mapi_fetch_field(hdl, 0);
		const char *tname1 = mapi_fetch_field(hdl, 1);
		const char *schema2 = mapi_fetch_field(hdl, 2);
		const char *tname2 = mapi_fetch_field(hdl, 3);

		if (mapi_error(mid))
			goto bailout;
		if (schema1 == NULL || schema2 == NULL) {
			/* cannot happen, but make analysis tools happy */
			continue;
		}
		if (sname != NULL && strcmp(schema1, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema2, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema2);
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     curschema);
		}
		mnstr_printf(toConsole, "ALTER TABLE \"%s\".\"%s\" ADD TABLE \"%s\";\n",
				     schema1, tname1, tname2);
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	/* dump views, functions, and triggers */
	query_len = snprintf(query, query_size, "%s%s",
			      comments_clause, views_functions_triggers);
	assert(query_len < (int) query_size);
	if (query_len < 0 ||
	    query_len >= (int) query_size ||
	    (hdl = mapi_query(mid, query)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       !mnstr_errnr(toConsole) &&
	       mapi_fetch_row(hdl) != 0) {
		const char *id = mapi_fetch_field(hdl, 0);
		const char *schema = mapi_fetch_field(hdl, 1);
		const char *name = mapi_fetch_field(hdl, 2);
		const char *query = mapi_fetch_field(hdl, 3);
		const char *remark = mapi_fetch_field(hdl, 4);

		if (mapi_error(mid))
			goto bailout;
		if (schema == NULL) {
			/* cannot happen, but make analysis tools happy */
			continue;
		}
		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     curschema);
		}
		if (query) {
			/* view or trigger */
			mnstr_printf(toConsole, "%s\n", query);
			/* only views have comments due to query */
			comment_on(toConsole, "VIEW", schema, name, NULL, remark);
		} else {
			/* function */
			dump_functions(mid, toConsole, 0, schema, name, id);
		}
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	if (curschema) {
		if (strcmp(sname ? sname : "sys", curschema) != 0) {
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     sname ? sname : "sys");
		}
		free(curschema);
		curschema = strdup(sname ? sname : "sys");
	}
	if (mapi_error(mid))
		goto bailout;
	if (mnstr_errnr(toConsole))
		goto bailout2;

	if (!describe) {
		if (dump_foreign_keys(mid, NULL, NULL, NULL, toConsole))
			goto bailout2;

		/* dump sequences, part 2 */
		if ((hdl = mapi_query(mid, sequences2)) == NULL ||
		    mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *schema = mapi_fetch_field(hdl, 0);
			const char *name = mapi_fetch_field(hdl, 1);
			const char *restart = mapi_fetch_field(hdl, 2);
			const char *minvalue = mapi_fetch_field(hdl, 3);
			const char *maxvalue = mapi_fetch_field(hdl, 4);
			const char *increment = mapi_fetch_field(hdl, 5);
			const char *cycle = mapi_fetch_field(hdl, 6);

			if (sname != NULL && strcmp(schema, sname) != 0)
				continue;

			mnstr_printf(toConsole,
				     "ALTER SEQUENCE \"%s\".\"%s\" RESTART WITH %s",
				     schema, name, restart);
			if (strcmp(increment, "1") != 0)
				mnstr_printf(toConsole, " INCREMENT BY %s", increment);
			if (strcmp(minvalue, "0") != 0)
				mnstr_printf(toConsole, " MINVALUE %s", minvalue);
			if (strcmp(maxvalue, "0") != 0)
				mnstr_printf(toConsole, " MAXVALUE %s", maxvalue);
			mnstr_printf(toConsole, " %sCYCLE;\n", strcmp(cycle, "true") == 0 ? "" : "NO ");
			if (mnstr_errnr(toConsole)) {
				mapi_close_handle(hdl);
				hdl = NULL;
				goto bailout2;
			}
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);
	}

	if ((hdl = mapi_query(mid, table_grants)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *schema = mapi_fetch_field(hdl, 0);
		const char *tname = mapi_fetch_field(hdl, 1);
		const char *aname = mapi_fetch_field(hdl, 2);
		int priv = atoi(mapi_fetch_field(hdl, 3));
		const char *grantable = mapi_fetch_field(hdl, 5);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     curschema);
		}
		mnstr_printf(toConsole, "GRANT");
		if (priv == 15) {
			mnstr_printf(toConsole, " ALL PRIVILEGES");
		} else {
			const char *sep = "";

			if (priv & 1) {
				mnstr_printf(toConsole, "%s SELECT", sep);
				sep = ",";
			}
			if (priv & 2) {
				mnstr_printf(toConsole, "%s UPDATE", sep);
				sep = ",";
			}
			if (priv & 4) {
				mnstr_printf(toConsole, "%s INSERT", sep);
				sep = ",";
			}
			if (priv & 8) {
				mnstr_printf(toConsole, "%s DELETE", sep);
				sep = ",";
			}
			if (priv & 16) {
				mnstr_printf(toConsole, "%s EXECUTE", sep);
				sep = ",";
			}
			if (priv & 32) {
				mnstr_printf(toConsole, "%s GRANT", sep);
				sep = ",";
			}
			if (priv & 64) {
				mnstr_printf(toConsole, "%s TRUNCATE", sep);
				sep = ",";
			}
		}
		mnstr_printf(toConsole, " ON TABLE \"%s\" TO \"%s\"",
			     tname, aname);
		if (strcmp(grantable, "1") == 0)
			mnstr_printf(toConsole, " WITH GRANT OPTION");
		mnstr_printf(toConsole, ";\n");
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if ((hdl = mapi_query(mid, column_grants)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *schema = mapi_fetch_field(hdl, 0);
		const char *tname = mapi_fetch_field(hdl, 1);
		const char *cname = mapi_fetch_field(hdl, 2);
		const char *aname = mapi_fetch_field(hdl, 3);
		const char *priv = mapi_fetch_field(hdl, 4);
		const char *grantable = mapi_fetch_field(hdl, 6);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     curschema);
		}
		mnstr_printf(toConsole, "GRANT %s(\"%s\") ON \"%s\" TO \"%s\"",
			     priv, cname, tname, aname);
		if (strcmp(grantable, "1") == 0)
			mnstr_printf(toConsole, " WITH GRANT OPTION");
		mnstr_printf(toConsole, ";\n");
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if ((hdl = mapi_query(mid, function_grants)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *schema = mapi_fetch_field(hdl, 0);
		const char *fname = mapi_fetch_field(hdl, 1);
		const char *aname = mapi_fetch_field(hdl, 2);
		const char *priv = mapi_fetch_field(hdl, 3);
		const char *grantable = mapi_fetch_field(hdl, 5);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     curschema);
		}
		mnstr_printf(toConsole, "GRANT %s ON \"%s\" TO \"%s\"",
			     priv, fname, aname);
		if (strcmp(grantable, "1") == 0)
			mnstr_printf(toConsole, " WITH GRANT OPTION");
		mnstr_printf(toConsole, ";\n");
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if (curschema) {
		if (strcmp(sname ? sname : "sys", curschema) != 0) {
			mnstr_printf(toConsole, "SET SCHEMA \"%s\";\n",
				     sname ? sname : "sys");
		}
		free(curschema);
		curschema = NULL;
	}

	if ((hdl = mapi_query(mid, end)) == NULL || mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	/* finally commit the whole transaction */
	if (!describe)
		mnstr_printf(toConsole, "COMMIT;\n");
	if (sname)
		free(sname);
	if (query)
		free(query);
	return rc;

  bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else
			mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
	} else
		mapi_explain(mid, stderr);

  bailout2:
	if (sname)
		free(sname);
	if (curschema)
		free(curschema);
	hdl = mapi_query(mid, end);
	if (hdl)
		mapi_close_handle(hdl);
	if (query)
		free(query);
	return 1;
}

void
dump_version(Mapi mid, stream *toConsole, const char *prefix)
{
	MapiHdl hdl;
	char *dbname = NULL, *uri = NULL, *dbver = NULL, *dbrel = NULL;
	const char *name, *val;

	if ((hdl = mapi_query(mid,
			      "SELECT name, value "
			      "FROM sys.env() AS env "
			      "WHERE name IN ('gdk_dbname', "
					"'monet_version', "
					"'monet_release', "
					"'merovingian_uri')")) == NULL ||
			mapi_error(mid))
		goto cleanup;

	while ((mapi_fetch_row(hdl)) != 0) {
		name = mapi_fetch_field(hdl, 0);
		val = mapi_fetch_field(hdl, 1);

		if (mapi_error(mid))
			goto cleanup;

		if (name != NULL && val != NULL) {
			if (strcmp(name, "gdk_dbname") == 0) {
				assert(dbname == NULL);
				dbname = *val == '\0' ? NULL : strdup(val);
			} else if (strcmp(name, "monet_version") == 0) {
				assert(dbver == NULL);
				dbver = *val == '\0' ? NULL : strdup(val);
			} else if (strcmp(name, "monet_release") == 0) {
				assert(dbrel == NULL);
				dbrel = *val == '\0' ? NULL : strdup(val);
			} else if (strcmp(name, "merovingian_uri") == 0) {
				assert(uri == NULL);
				uri = strdup(val);
			}
		}
	}
	if (uri != NULL) {
		if (dbname != NULL)
			free(dbname);
		dbname = uri;
		uri = NULL;
	}
	if (dbname != NULL && dbver != NULL) {
		mnstr_printf(toConsole, "%s MonetDB v%s%s%s%s, '%s'\n",
			     prefix,
				 dbver,
				 dbrel != NULL ? " (" : "",
				 dbrel != NULL ? dbrel : "",
				 dbrel != NULL ? ")" : "",
				 dbname);
	}

  cleanup:
	if (dbname != NULL)
		free(dbname);
	if (dbver != NULL)
		free(dbver);
	if (dbrel != NULL)
		free(dbrel);
	if (uri != NULL)
		free(uri);
	if (hdl)
		mapi_close_handle(hdl);
}
