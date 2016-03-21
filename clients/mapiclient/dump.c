/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <monet_options.h>
#include "mapi.h"
#include "stream.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "msqldump.h"

static void
quoted_print(stream *f, const char *s, const char singleq)
{
	mnstr_write(f, singleq ? "'" : "\"", 1, 1);
	while (*s) {
		switch (*s) {
		case '\\':
			mnstr_write(f, "\\\\", 1, 2);
			break;
		case '"':
			mnstr_write(f, "\"\"", 1, singleq ? 1 : 2);
			break;
		case '\'':
			mnstr_write(f, "''", 1, singleq ? 2 : 1);
			break;
		case '\n':
			mnstr_write(f, "\\n", 1, 2);
			break;
		case '\t':
			mnstr_write(f, "\\t", 1, 2);
			break;
		default:
			if ((0 < *s && *s < 32) || *s == '\177')
				mnstr_printf(f, "\\%03o", *s & 0377);
			else
				mnstr_write(f, s, 1, 1);
			break;
		}
		s++;
	}
	mnstr_write(f, singleq ? "'" : "\"", 1, 1);
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

	if ((hdl = mapi_query(mid, "SELECT current_schema")) == NULL || mapi_error(mid))
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

/* return TRUE if the sys.systemfunctions table exists */
int
has_systemfunctions(Mapi mid)
{
	MapiHdl hdl;
	int ret;

	if ((hdl = mapi_query(mid,
			      "SELECT t.id "
			      "FROM sys._tables t, "
			           "sys.schemas s "
			      "WHERE t.name = 'systemfunctions' AND "
			            "t.schema_id = s.id AND "
			            "s.name = 'sys'")) == NULL ||
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

/* return TRUE if the sys.schemas table has a column named system */
int
has_schemas_system(Mapi mid)
{
	MapiHdl hdl;
	int ret;

	if ((hdl = mapi_query(mid,
			      "SELECT c.id "
			      "FROM sys._columns c, "
			           "sys._tables t, "
			           "sys.schemas s "
			      "WHERE c.name = 'system' AND "
				    "c.table_id = t.id AND "
				    "t.name = 'schemas' AND "
				    "t.schema_id = s.id AND "
				    "s.name = 'sys'")) == NULL ||
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

/* return TRUE if the HUGEINT type exists */
static int
has_hugeint(Mapi mid)
{
	MapiHdl hdl;
	int ret;

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
			"ORDER BY fs.name,fkt.name, "
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
		char *c_psname = mapi_fetch_field(hdl, 0);
		char *c_ptname = mapi_fetch_field(hdl, 1);
		char *c_pcolumn = mapi_fetch_field(hdl, 2);
		char *c_fcolumn = mapi_fetch_field(hdl, 3);
		char *c_nr = mapi_fetch_field(hdl, 4);
		char *c_fkname = mapi_fetch_field(hdl, 5);
		char *c_faction = mapi_fetch_field(hdl, 6);
		char *c_fsname = mapi_fetch_field(hdl, 7);
		char *c_ftname = mapi_fetch_field(hdl, 8);
		char **fkeys, **pkeys;
		int nkeys = 0;

		if (mapi_error(mid))
			goto bailout;
		assert(strcmp(c_nr, "0") == 0);
		(void) c_nr;	/* pacify compilers in case assertions are disabled */
		nkeys = 1;
		fkeys = malloc(nkeys * sizeof(*fkeys));
		pkeys = malloc(nkeys * sizeof(*pkeys));
		pkeys[nkeys - 1] = c_pcolumn;
		fkeys[nkeys - 1] = c_fcolumn;
		while ((cnt = mapi_fetch_row(hdl)) != 0 && strcmp(mapi_fetch_field(hdl, 4), "0") != 0) {
			nkeys++;
			pkeys = realloc(pkeys, nkeys * sizeof(*pkeys));
			fkeys = realloc(fkeys, nkeys * sizeof(*fkeys));
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
		free(fkeys);
		free(pkeys);
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
	int foreign,
	int hashge);

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
dump_type(Mapi mid, stream *toConsole, char *c_type, char *c_type_digits, char *c_type_scale, int hashge)
{
	int space = 0;

	/* map wrd type to something legal */
	if (strcmp(c_type, "wrd") == 0) {
		if (strcmp(c_type_scale, "32") == 0)
			c_type = "int";
		else
			c_type = "bigint";
	}
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
dump_column_definition(Mapi mid, stream *toConsole, const char *schema, const char *tname, const char *tid, int foreign, int hashge)
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
		char *c_name = mapi_fetch_field(hdl, 0);
		char *c_type = mapi_fetch_field(hdl, 1);
		char *c_type_digits = mapi_fetch_field(hdl, 2);
		char *c_type_scale = mapi_fetch_field(hdl, 3);
		char *c_null = mapi_fetch_field(hdl, 4);
		char *c_default = mapi_fetch_field(hdl, 5);
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
		char *c_column = mapi_fetch_field(hdl, 0);
		char *k_name = mapi_fetch_field(hdl, 2);

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
		char *c_column = mapi_fetch_field(hdl, 0);
		char *kc_nr = mapi_fetch_field(hdl, 1);
		char *k_name = mapi_fetch_field(hdl, 2);

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
describe_table(Mapi mid, char *schema, char *tname, stream *toConsole, int foreign)
{
	int cnt;
	MapiHdl hdl = NULL;
	char *query;
	char *view = NULL;
	int type = 0;
	size_t maxquerylen;
	char *sname = NULL;
	int hashge;

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

	maxquerylen = 512 + strlen(tname) + strlen(schema);

	query = malloc(maxquerylen);
	snprintf(query, maxquerylen,
		 "SELECT t.name, t.query, t.type "
		 "FROM sys._tables t, sys.schemas s "
		 "WHERE s.name = '%s' AND "
		       "t.schema_id = s.id AND "
		       "t.name = '%s'",
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
	}
	if (mapi_error(mid)) {
		view = NULL;
		goto bailout;
	}
	if (view)
		view = strdup(view);
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
		if (type == 5)
			mnstr_printf(toConsole, " ON '%s'", view);
		mnstr_printf(toConsole, ";\n");

		snprintf(query, maxquerylen,
			 "SELECT i.name, "		/* 0 */
				"k.name, "		/* 1 */
				"kc.nr, "		/* 2 */
				"c.name "		/* 3 */
			 "FROM sys.idxs AS i LEFT JOIN sys.keys AS k "
					"ON i.name = k.name, "
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
			       "t.name = '%s' "
			 "ORDER BY i.name, kc.nr", schema, tname);
		if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
			goto bailout;
		cnt = 0;
		while (mapi_fetch_row(hdl) != 0) {
			char *i_name = mapi_fetch_field(hdl, 0);
			char *k_name = mapi_fetch_field(hdl, 1);
			char *kc_nr = mapi_fetch_field(hdl, 2);
			char *c_name = mapi_fetch_field(hdl, 3);

			if (mapi_error(mid))
				goto bailout;
			if (k_name != NULL) {
				/* unique key, already handled */
				continue;
			}

			if (strcmp(kc_nr, "0") == 0) {
				if (cnt)
					mnstr_printf(toConsole, ");\n");
				mnstr_printf(toConsole,
					     "CREATE INDEX \"%s\" ON \"%s\".\"%s\" (",
					     i_name, schema, tname);
				cnt = 1;
			} else
				mnstr_printf(toConsole, ", ");
			mnstr_printf(toConsole, "\"%s\"", c_name);
			if (mnstr_errnr(toConsole))
				goto bailout;
		}
		if (cnt)
			mnstr_printf(toConsole, ");\n");
		if (mapi_error(mid))
			goto bailout;
	}

	if (hdl)
		mapi_close_handle(hdl);
	if (view)
		free(view);
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
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	return 1;
}

int
describe_sequence(Mapi mid, char *schema, char *tname, stream *toConsole)
{
	MapiHdl hdl = NULL;
	char *query;
	size_t maxquerylen;
	char *sname = NULL;

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

	maxquerylen = 512 + strlen(tname) + strlen(schema);

	query = malloc(maxquerylen);
	snprintf(query, maxquerylen,
		"SELECT s.name, "
		     "seq.name, "
		     "get_value_for(s.name,seq.name), "
		     "seq.\"minvalue\", "
		     "seq.\"maxvalue\", "
		     "seq.\"increment\", "
		     "seq.\"cycle\" "
		"FROM sys.sequences seq, "
		     "sys.schemas s "
		"WHERE s.id = seq.schema_id AND "
		      "s.name = '%s' AND "
		      "seq.name = '%s' "
		"ORDER BY s.name,seq.name",
		schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *name = mapi_fetch_field(hdl, 1);
		char *start = mapi_fetch_field(hdl, 2);
		char *minvalue = mapi_fetch_field(hdl, 3);
		char *maxvalue = mapi_fetch_field(hdl, 4);
		char *increment = mapi_fetch_field(hdl, 5);
		char *cycle = mapi_fetch_field(hdl, 6);

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
describe_schema(Mapi mid, char *sname, stream *toConsole)
{
	MapiHdl hdl = NULL;
	char schemas[256];

	snprintf(schemas, 256,
		"SELECT s.name, a.name "
		"FROM sys.schemas s, "
		     "sys.auths a "
		"WHERE s.\"authorization\" = a.id AND "
		      "s.name = '%s' "
		"ORDER BY s.name",
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
		char *sname = mapi_fetch_field(hdl, 0);
		char *aname = mapi_fetch_field(hdl, 1);

		mnstr_printf(toConsole, "CREATE SCHEMA \"%s\"", sname);
		if (strcmp(aname, "sysadmin") != 0) {
			mnstr_printf(toConsole,
					 " AUTHORIZATION \"%s\"", aname);
		}
		mnstr_printf(toConsole, ";\n");
	}

	return 0;
}

static int
dump_table_data(Mapi mid, char *schema, char *tname, stream *toConsole,
		const char useInserts)
{
	int cnt, i;
	MapiHdl hdl = NULL;
	char *query;
	size_t maxquerylen;
	unsigned char *string = NULL;
	char *sname = NULL;

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

	maxquerylen = 512 + strlen(tname) + strlen(schema);
	query = malloc(maxquerylen);

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

	if (!useInserts) {
		snprintf(query, maxquerylen, "SELECT count(*) FROM \"%s\".\"%s\"",
			 schema, tname);
		if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
			goto bailout;
		if (mapi_fetch_row(hdl)) {
			char *cntfld = mapi_fetch_field(hdl, 0);

			if (strcmp(cntfld, "0") == 0) {
				/* no records to dump, so return early */
				goto doreturn;
			}

			mnstr_printf(toConsole,
				     "COPY %s RECORDS INTO \"%s\".\"%s\" "
				     "FROM stdin USING DELIMITERS '\\t','\\n','\"';\n",
				     cntfld, schema, tname);
		}
		mapi_close_handle(hdl);
		hdl = NULL;
	}

	snprintf(query, maxquerylen, "SELECT * FROM \"%s\".\"%s\"",
		 schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	cnt = mapi_get_field_count(hdl);
	if (cnt < 1 || cnt >= 1 << 29)
		goto bailout;	/* ridiculous number of columns */
	string = malloc(sizeof(unsigned char) * cnt);
	for (i = 0; i < cnt; i++) {
		string[i] = (strcmp(mapi_get_type(hdl, i), "char") == 0 ||
			     strcmp(mapi_get_type(hdl, i), "varchar") == 0 ||
			     strcmp(mapi_get_type(hdl, i), "clob") == 0);
	}
	while (mapi_fetch_row(hdl)) {
		char *s;

		if (useInserts)
			mnstr_printf(toConsole, "INSERT INTO \"%s\".\"%s\" VALUES (",
				     schema, tname);

		for (i = 0; i < cnt; i++) {
			s = mapi_fetch_field(hdl, i);
			if (s == NULL)
				mnstr_printf(toConsole, "NULL");
			else if (string[i]) {
				/* write double or single-quoted string with
				   certain characters escaped */
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
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	if (string != NULL)
		free(string);
	return 1;
}

int
dump_table(Mapi mid, char *schema, char *tname, stream *toConsole, int describe, int foreign, const char useInserts)
{
	int rc;

	rc = describe_table(mid, schema, tname, toConsole, foreign);
	if (rc == 0 && !describe)
		rc = dump_table_data(mid, schema, tname, toConsole, useInserts);
	return rc;
}

int
dump_functions(Mapi mid, stream *toConsole, const char *sname, const char *fname)
{
	const char functions[] =
		"SELECT f.func "
		"FROM sys.schemas s, "
		     "sys.functions f "
		"WHERE f.language  < 3 AND "
		      "s.id = f.schema_id "
		      "%s%s"
		      "%s%s%s%s%s%s"
		"ORDER BY f.id";
	MapiHdl hdl;
	char *q;
	size_t l;
	char dumpSystem;
	char *schema = NULL;

	if (sname == NULL) {
		if (fname == NULL) {
			schema = NULL;
		} else if ((schema = strchr(fname, '.')) != NULL) {
			size_t len = schema - fname;

			schema = malloc(len + 1);
			strncpy(schema, fname, len);
			schema[len] = 0;
			fname += len + 1;
		} else if ((schema = get_schema(mid)) == NULL) {
			return 1;
		}
		sname = schema;
	}

	dumpSystem = sname && fname;

	l = sizeof(functions) + (sname ? strlen(sname) : 0) + 100;
	q = malloc(l);
	snprintf(q, l, functions,
		 dumpSystem ? "" : "AND f.id ",
		 dumpSystem ? "" : has_systemfunctions(mid) ? "NOT IN (SELECT function_id FROM sys.systemfunctions) " : "> 2000 ",
		 sname ? "AND s.name = '" : "",
		 sname ? sname : "",
		 sname ? "' " : "",
		 fname ? "AND f.name = '" : "",
		 fname ? fname : "",
		 fname ? "' " : "");
	hdl = mapi_query(mid, q);
	free(q);
	if (hdl == NULL || mapi_error(mid))
		goto bailout;
	while (!mnstr_errnr(toConsole) && mapi_fetch_row(hdl) != 0) {
		char *query = mapi_fetch_field(hdl, 0);

		mnstr_printf(toConsole, "%s\n", query);
	}
	if (mapi_error(mid))
		goto bailout;
	if (schema)
		free(schema);
	mapi_close_handle(hdl);
	return mnstr_errnr(toConsole) != 0;

  bailout:
	if (schema)
		free(schema);
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

int
dump_database(Mapi mid, stream *toConsole, int describe, const char useInserts)
{
	const char *start = "START TRANSACTION";
	const char *end = "ROLLBACK";
	const char *chkhash =
		"SELECT id "
		"FROM sys.functions "
		"WHERE name = 'password_hash'";
	const char *createhash =
		"CREATE FUNCTION password_hash (username STRING) "
		"RETURNS STRING "
		"EXTERNAL NAME sql.password";
	const char *drophash = "DROP FUNCTION password_hash";
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
			    "WHEN 32 THEN 'GRANT' END, "
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
			    "WHEN 32 THEN 'GRANT' END, "
		       "g.name, p.grantable "
		"FROM sys.schemas s, sys.functions f, "
		     "sys.auths a, sys.privileges p, sys.auths g "
		"WHERE s.id = f.schema_id AND "
		      "f.id = p.obj_id AND "
		      "p.auth_id = a.id AND "
		      "p.grantor = g.id "
		      "%s"	/* and f.id not in systemfunctions */
		"ORDER BY s.name, f.name, a.name, g.name, p.grantable";
	const char *schemas =
		"SELECT s.name, a.name "
		"FROM sys.schemas s, "
		     "sys.auths a "
		"WHERE s.\"authorization\" = a.id AND "
		      "%s "
		"ORDER BY s.name";
	/* alternative, but then need to handle NULL in second column:
	   SELECT "s"."name", "a"."name"
	   FROM "sys"."schemas" "s"
		LEFT OUTER JOIN "sys"."auths" "a"
		     ON "s"."authorization" = "a"."id" AND
		        "s"."system" = FALSE
	   ORDER BY "s"."name"

	   This may be needed after a sequence:

	   CREATE USER "voc" WITH PASSWORD 'voc' NAME 'xxx' SCHEMA "sys";
	   CREATE SCHEMA "voc" AUTHORIZATION "voc";
	   ALTER USER "voc" SET SCHEMA "voc";
	   DROP USER "voc";

	   In this case, the authorization value for voc in the
	   schemas table has no corresponding value in the auths table
	   anymore.
	 */
	const char *sequences1 =
		"SELECT sch.name,seq.name "
		"FROM sys.schemas sch, "
		     "sys.sequences seq "
		"WHERE sch.id = seq.schema_id "
		"ORDER BY sch.name,seq.name";
	const char *sequences2 =
		"SELECT s.name, "
		     "seq.name, "
		     "get_value_for(s.name,seq.name), "
		     "seq.\"minvalue\", "
		     "seq.\"maxvalue\", "
		     "seq.\"increment\", "
		     "seq.\"cycle\" "
		"FROM sys.sequences seq, "
		     "sys.schemas s "
		"WHERE s.id = seq.schema_id "
		"ORDER BY s.name,seq.name";
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
		"WITH vft AS ("
			"SELECT s.name AS sname, "
			       "t.id AS id, "
			       "t.query AS query "
			"FROM sys.schemas s, "
			     "sys._tables t "
			"WHERE t.type = 1 AND "
			      "t.system = FALSE AND "
			      "s.id = t.schema_id AND "
			      "s.name <> 'tmp' "
			"UNION "
			"SELECT s.name AS sname, "
			       "f.id AS id, "
			       "f.func AS query "
			"FROM sys.schemas s, "
			     "sys.functions f "
			"WHERE f.language < 3 AND "
			      "s.id = f.schema_id "
			"%s"		/* and f.id not in systemfunctions */
			"UNION "
			"SELECT s.name AS sname, "
			       "tr.id AS id, "
			       "tr.\"statement\" AS query "
			"FROM sys.triggers tr, "
			     "sys.schemas s, "
			     "sys._tables t "
			"WHERE s.id = t.schema_id AND "
			      "t.id = tr.table_id"
		") "
		"SELECT sname, query FROM vft ORDER BY id";
	char *sname;
	char *curschema = NULL;
	MapiHdl hdl;
	int create_hash_func = 0;
	int rc = 0;
	char query[1024];

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
			char *name = mapi_fetch_field(hdl, 0);

			mnstr_printf(toConsole, "CREATE ROLE \"%s\";\n", name);
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);

		/* dump users, part 1 */
		/* first make sure the password_hash function exists */
		if ((hdl = mapi_query(mid, chkhash)) == NULL ||
		    mapi_error(mid))
			goto bailout;
		create_hash_func = mapi_rows_affected(hdl) == 0;
		mapi_close_handle(hdl);
		if (create_hash_func) {
			if ((hdl = mapi_query(mid, createhash)) == NULL ||
			    mapi_error(mid))
				goto bailout;
			mapi_close_handle(hdl);
		}

		if ((hdl = mapi_query(mid, users)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			char *uname = mapi_fetch_field(hdl, 0);
			char *fullname = mapi_fetch_field(hdl, 1);
			char *pwhash = mapi_fetch_field(hdl, 2);
			char *sname = mapi_fetch_field(hdl, 3);

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
		snprintf(query, sizeof(query), schemas,
			 has_schemas_system(mid) ?
				"s.system = FALSE" :
				"s.name NOT IN ('sys', 'tmp')");
		if ((hdl = mapi_query(mid, query)) == NULL ||
		    mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			char *sname = mapi_fetch_field(hdl, 0);
			char *aname = mapi_fetch_field(hdl, 1);

			mnstr_printf(toConsole, "CREATE SCHEMA \"%s\"", sname);
			if (strcmp(aname, "sysadmin") != 0) {
				mnstr_printf(toConsole,
					     " AUTHORIZATION \"%s\"", aname);
			}
			mnstr_printf(toConsole, ";\n");
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

		/* clean up -- not strictly necessary due to ROLLBACK */
		if (create_hash_func) {
			if ((hdl = mapi_query(mid, drophash)) == NULL ||
			    mapi_error(mid))
				goto bailout;
			mapi_close_handle(hdl);
		}

		/* grant user privileges */
		if ((hdl = mapi_query(mid, grants)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			char *uname = mapi_fetch_field(hdl, 0);
			char *rname = mapi_fetch_field(hdl, 1);

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
	if ((hdl = mapi_query(mid, sequences1)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *name = mapi_fetch_field(hdl, 1);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		mnstr_printf(toConsole,
			     "CREATE SEQUENCE \"%s\".\"%s\" AS INTEGER;\n",
			     schema, name);
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
		char *schema1 = mapi_fetch_field(hdl, 0);
		char *tname1 = mapi_fetch_field(hdl, 1);
		char *schema2 = mapi_fetch_field(hdl, 2);
		char *tname2 = mapi_fetch_field(hdl, 3);

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
	snprintf(query, sizeof(query), views_functions_triggers,
		 has_systemfunctions(mid) ? "AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions) " : "");
	if ((hdl = mapi_query(mid, query)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       !mnstr_errnr(toConsole) &&
	       mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *func = mapi_fetch_field(hdl, 1);

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
		mnstr_printf(toConsole, "%s\n", func);
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
			char *schema = mapi_fetch_field(hdl, 0);
			char *name = mapi_fetch_field(hdl, 1);
			char *restart = mapi_fetch_field(hdl, 2);
			char *minvalue = mapi_fetch_field(hdl, 3);
			char *maxvalue = mapi_fetch_field(hdl, 4);
			char *increment = mapi_fetch_field(hdl, 5);
			char *cycle = mapi_fetch_field(hdl, 6);

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
		char *schema = mapi_fetch_field(hdl, 0);
		char *tname = mapi_fetch_field(hdl, 1);
		char *aname = mapi_fetch_field(hdl, 2);
		int priv = atoi(mapi_fetch_field(hdl, 3));
		char *grantable = mapi_fetch_field(hdl, 5);

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
		char *schema = mapi_fetch_field(hdl, 0);
		char *tname = mapi_fetch_field(hdl, 1);
		char *cname = mapi_fetch_field(hdl, 2);
		char *aname = mapi_fetch_field(hdl, 3);
		char *priv = mapi_fetch_field(hdl, 4);
		char *grantable = mapi_fetch_field(hdl, 6);

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

	snprintf(query, sizeof(query), function_grants,
		 has_systemfunctions(mid) ? "AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions) " : "");
	if ((hdl = mapi_query(mid, query)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *fname = mapi_fetch_field(hdl, 1);
		char *aname = mapi_fetch_field(hdl, 2);
		char *priv = mapi_fetch_field(hdl, 3);
		char *grantable = mapi_fetch_field(hdl, 5);

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
	if (curschema)
		free(curschema);
	hdl = mapi_query(mid, end);
	if (hdl)
		mapi_close_handle(hdl);
	return 1;
}

void
dump_version(Mapi mid, stream *toConsole, const char *prefix)
{
	MapiHdl hdl;
	char *dbname = NULL, *uri = NULL, *dbver = NULL, *dbrel = NULL;
	char *name, *val;

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
