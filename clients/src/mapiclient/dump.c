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
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

#include "clients_config.h"
#include <monet_options.h>
#include "mapilib/Mapi.h"
#include "stream.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "msqldump.h"

#ifdef NATIVE_WIN32
/* The POSIX name for this item is deprecated. Instead, use the ISO
   C++ conformant name: _strdup. See online help for details. */
#define strdup _strdup
#endif

static void
quoted_print(stream *f, const char *s)
{
	stream_write(f, "\"", 1, 1);
	while (*s) {
		switch (*s) {
		case '\\':
		case '"':
			stream_write(f, "\\", 1, 1);
			stream_write(f, s, 1, 1);
			break;
		case '\n':
			stream_write(f, "\\n", 1, 2);
			break;
		case '\t':
			stream_write(f, "\\t", 1, 2);
			break;
		default:
			if ((0 < *s && *s < 32) || *s == '\377')
				stream_printf(f, "\\%03o", *s & 0377);
			else
				stream_write(f, s, 1, 1);
			break;
		}
		s++;
	}
	stream_write(f, "\"", 1, 1);
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

	if ((hdl = mapi_query(mid, "SELECT \"current_schema\"")) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return NULL;
	}
	while ((mapi_fetch_row(hdl)) != 0) {
		sname = mapi_fetch_field(hdl, 0);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return NULL;
		}
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return NULL;
	}
	/* copy before closing the handle */
	if (sname)
		sname = strdup(sname);
	mapi_close_handle(hdl);
	return sname;
}

static int
dump_foreign_keys(Mapi mid, char *schema, char *tname, stream *toConsole)
{
	MapiHdl hdl = NULL;
	int cnt, i;
	char *query;
	size_t maxquerylen = 0;
	int rc = 0;

	if (tname != NULL) {
		maxquerylen = 1024 + strlen(tname) + strlen(schema);
		query = malloc(maxquerylen);
		snprintf(query, maxquerylen,
			 "SELECT \"ps\".\"name\","		/* 0 */
			        "\"pkt\".\"name\","		/* 1 */
				"\"pkkc\".\"column\","		/* 2 */
				"\"fkkc\".\"column\","		/* 3 */
				"\"fkkc\".\"nr\","		/* 4 */
				"\"fkk\".\"name\","		/* 5 */
				"\"fkk\".\"action\","		/* 6 */
				"\"fs\".\"name\","		/* 7 */
				"\"fkt\".\"name\" "		/* 8 */
			 "FROM \"sys\".\"_tables\" \"fkt\","
			      "\"sys\".\"keycolumns\" \"fkkc\","
			      "\"sys\".\"keys\" \"fkk\","
			      "\"sys\".\"_tables\" \"pkt\","
			      "\"sys\".\"keycolumns\" \"pkkc\","
			      "\"sys\".\"keys\" \"pkk\","
			      "\"sys\".\"schemas\" \"ps\","
			      "\"sys\".\"schemas\" \"fs\" "
			 "WHERE \"fkt\".\"id\" = \"fkk\".\"table_id\" AND "
			       "\"pkt\".\"id\" = \"pkk\".\"table_id\" AND "
			       "\"fkk\".\"id\" = \"fkkc\".\"id\" AND "
			       "\"pkk\".\"id\" = \"pkkc\".\"id\" AND "
			       "\"fkk\".\"rkey\" = \"pkk\".\"id\" AND "
			       "\"fkkc\".\"nr\" = \"pkkc\".\"nr\" AND "
			       "\"pkt\".\"schema_id\" = \"ps\".\"id\" AND "
			       "\"fkt\".\"schema_id\" = \"fs\".\"id\" AND "
			       "\"fs\".\"name\" = '%s' AND "
			       "\"fkt\".\"name\" = '%s'"
			 "ORDER BY \"fkk\".\"name\", \"nr\"", schema, tname);
	} else {
		query = "SELECT \"ps\".\"name\","		/* 0 */
			       "\"pkt\".\"name\","		/* 1 */
			       "\"pkkc\".\"column\","		/* 2 */
			       "\"fkkc\".\"column\","		/* 3 */
			       "\"fkkc\".\"nr\","		/* 4 */
			       "\"fkk\".\"name\","		/* 5 */
			       "\"fkk\".\"action\","		/* 6 */
			       "\"fs\".\"name\","		/* 7 */
			       "\"fkt\".\"name\" "		/* 8 */
			"FROM \"sys\".\"_tables\" \"fkt\","
			     "\"sys\".\"keycolumns\" \"fkkc\","
			     "\"sys\".\"keys\" \"fkk\","
			     "\"sys\".\"_tables\" \"pkt\","
			     "\"sys\".\"keycolumns\" \"pkkc\","
			     "\"sys\".\"keys\" \"pkk\","
			     "\"sys\".\"schemas\" \"ps\","
			     "\"sys\".\"schemas\" \"fs\" "
			"WHERE \"fkt\".\"id\" = \"fkk\".\"table_id\" AND "
			      "\"pkt\".\"id\" = \"pkk\".\"table_id\" AND "
			      "\"fkk\".\"id\" = \"fkkc\".\"id\" AND "
			      "\"pkk\".\"id\" = \"pkkc\".\"id\" AND "
			      "\"fkk\".\"rkey\" = \"pkk\".\"id\" AND "
			      "\"fkkc\".\"nr\" = \"pkkc\".\"nr\" AND "
			      "\"pkt\".\"schema_id\" = \"ps\".\"id\" AND "
			      "\"fkt\".\"schema_id\" = \"fs\".\"id\" "
			"ORDER BY \"fs\".\"name\",\"fkt\".\"name\","
			      "\"fkk\".\"name\", \"nr\"";
	}
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		rc = 1;
		goto bailout;
	}
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

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			rc = 1;
			goto bailout;
		}
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
		if (tname == NULL) {
			stream_printf(toConsole, "ALTER TABLE ");
			quoted_print(toConsole, c_fsname);
			stream_printf(toConsole, ".");
			quoted_print(toConsole, c_ftname);
			stream_printf(toConsole, " ADD ");
		} else {
			stream_printf(toConsole, ",\n\t");
		}
		if (c_fkname) {
			stream_printf(toConsole, "CONSTRAINT ");
			quoted_print(toConsole, c_fkname);
			stream_write(toConsole, " ", 1, 1);
		}
		stream_printf(toConsole, "FOREIGN KEY (");
		for (i = 0; i < nkeys; i++) {
			if (i > 0)
				stream_printf(toConsole, ", ");
			quoted_print(toConsole, fkeys[i]);
		}
		stream_printf(toConsole, ") REFERENCES ");
		quoted_print(toConsole, c_psname);
		stream_printf(toConsole, ".");
		quoted_print(toConsole, c_ptname);
		stream_printf(toConsole, " (");
		for (i = 0; i < nkeys; i++) {
			if (i > 0)
				stream_printf(toConsole, ", ");
			quoted_print(toConsole, pkeys[i]);
		}
		stream_printf(toConsole, ")");
		free(fkeys);
		free(pkeys);
		if (c_faction) {
			int action = atoi(c_faction);
			int on_update = (action >> 8) & 255;
			int on_delete = action & 255;

			if (0 < on_delete && on_delete < NR_ACTIONS && on_delete != 2 /* RESTRICT -- default */)
				stream_printf(toConsole, " ON DELETE %s", actions[on_delete]);
			if (0 < on_update && on_update < NR_ACTIONS && on_delete != 2 /* RESTRICT -- default */)
				stream_printf(toConsole, " ON UPDATE %s", actions[on_update]);
		}
		if (tname == NULL)
			stream_printf(toConsole, ";\n");

		if (stream_errnr(toConsole)) {
			rc = 1;
			goto bailout;
		}
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		rc = 1;
	}

  bailout:
	if (hdl)
		mapi_close_handle(hdl);

	if (query != NULL && maxquerylen != 0)
		free(query);

	return rc;
}

int
describe_table(Mapi mid, char *schema, char *tname, stream *toConsole, int foreign)
{
	int cnt;
	int slen;
	MapiHdl hdl = NULL;
	char *query;
	char *view = NULL;
	size_t maxquerylen;
	char *sname = NULL;
	int cap;
#define CAP(X) ((cap = (int) (X)) < 0 ? 0 : cap)


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
		 "SELECT \"t\".\"name\", \"t\".\"query\" "
		 "FROM \"sys\".\"_tables\" \"t\", \"sys\".\"schemas\" \"s\" "
		 "WHERE \"s\".\"name\" = '%s' "
		 "AND \"t\".\"schema_id\" = \"s\".\"id\" "
		 "AND \"t\".\"name\" = '%s'",
		 schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		cnt++;
		view = mapi_fetch_field(hdl, 1);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
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

	if (view) {
		/* the table is actually a view */
		stream_printf(toConsole, "%s\n", view);
		goto doreturn;
	}

	stream_printf(toConsole, "CREATE TABLE ");

	quoted_print(toConsole, schema);
	stream_printf(toConsole, ".");

	quoted_print(toConsole, tname);
	stream_printf(toConsole, " (\n");

	snprintf(query, maxquerylen,
		 "SELECT \"c\".\"name\","		/* 0 */
			"\"c\".\"type\","		/* 1 */
			"\"c\".\"type_digits\","	/* 2 */
			"\"c\".\"type_scale\","		/* 3 */
			"\"c\".\"null\","		/* 4 */
			"\"c\".\"default\","		/* 5 */
			"\"c\".\"number\" "		/* 6 */
		 "FROM \"sys\".\"_columns\" \"c\", "
		      "\"sys\".\"_tables\" \"t\", "
		      "\"sys\".\"schemas\" \"s\" "
		 "WHERE \"c\".\"table_id\" = \"t\".\"id\" "
		 "AND '%s' = \"t\".\"name\" "
		 "AND \"t\".\"schema_id\" = \"s\".\"id\" "
		 "AND \"s\".\"name\" = '%s' "
		 "ORDER BY \"number\"", tname, schema);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}

	slen = mapi_get_len(hdl, 0);
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		char *c_name = mapi_fetch_field(hdl, 0);
		char *c_type = mapi_fetch_field(hdl, 1);
		char *c_type_digits = mapi_fetch_field(hdl, 2);
		char *c_type_scale = mapi_fetch_field(hdl, 3);
		char *c_null = mapi_fetch_field(hdl, 4);
		char *c_default = mapi_fetch_field(hdl, 5);
		int space = 0;

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			goto bailout;
		}
		if (cnt)
			stream_printf(toConsole, ",\n");

		stream_write(toConsole, "\t", 1, 1);
		quoted_print(toConsole, c_name);
		stream_printf(toConsole, "%*s", CAP(slen - strlen(c_name)), "");
		stream_write(toConsole, " ", 1, 1);
		/* map wrd type to something legal */
		if (strcmp(c_type, "wrd") == 0) {
			if (strcmp(c_type_scale, "32") == 0)
				c_type = "int";
			else
				c_type = "bigint";
		}
		if (strcmp(c_type, "boolean") == 0 ||
		    strcmp(c_type, "int") == 0 ||
		    strcmp(c_type, "smallint") == 0 ||
		    strcmp(c_type, "tinyint") == 0 ||
		    strcmp(c_type, "bigint") == 0 ||
		    strcmp(c_type, "date") == 0) {
			space = stream_printf(toConsole, "%s", c_type);
		} else if (strcmp(c_type, "month_interval") == 0) {
			if (strcmp(c_type_digits, "1") == 0)
				space = stream_printf(toConsole, "INTERVAL YEAR");
			else if (strcmp(c_type_digits, "2") == 0)
				space = stream_printf(toConsole, "INTERVAL YEAR TO MONTH");
			else if (strcmp(c_type_digits, "3") == 0)
				space = stream_printf(toConsole, "INTERVAL MONTH");
			else
				fprintf(stderr, "Internal error: unrecognized month interval %s\n", c_type_digits);
		} else if (strcmp(c_type, "sec_interval") == 0) {
			if (strcmp(c_type_digits, "4") == 0)
				space = stream_printf(toConsole, "INTERVAL DAY");
			else if (strcmp(c_type_digits, "5") == 0)
				space = stream_printf(toConsole, "INTERVAL DAY TO HOUR");
			else if (strcmp(c_type_digits, "6") == 0)
				space = stream_printf(toConsole, "INTERVAL DAY TO MINUTE");
			else if (strcmp(c_type_digits, "7") == 0)
				space = stream_printf(toConsole, "INTERVAL DAY TO SECOND");
			else if (strcmp(c_type_digits, "8") == 0)
				space = stream_printf(toConsole, "INTERVAL HOUR");
			else if (strcmp(c_type_digits, "9") == 0)
				space = stream_printf(toConsole, "INTERVAL HOUR TO MINUTE");
			else if (strcmp(c_type_digits, "10") == 0)
				space = stream_printf(toConsole, "INTERVAL HOUR TO SECOND");
			else if (strcmp(c_type_digits, "11") == 0)
				space = stream_printf(toConsole, "INTERVAL MINUTE");
			else if (strcmp(c_type_digits, "12") == 0)
				space = stream_printf(toConsole, "INTERVAL MINUTE TO SECOND");
			else if (strcmp(c_type_digits, "13") == 0)
				space = stream_printf(toConsole, "INTERVAL SECOND");
			else
				fprintf(stderr, "Internal error: unrecognized second interval %s\n", c_type_digits);
		} else if (strcmp(c_type, "clob") == 0) {
			space = stream_printf(toConsole, "CHARACTER LARGE OBJECT");
			if (strcmp(c_type_digits, "0") != 0)
				space += stream_printf(toConsole, "(%s)", c_type_digits);
		} else if (strcmp(c_type, "blob") == 0) {
			space = stream_printf(toConsole, "BINARY LARGE OBJECT");
			if (strcmp(c_type_digits, "0") != 0)
				space += stream_printf(toConsole, "(%s)", c_type_digits);
		} else if (strcmp(c_type, "timestamp") == 0 ||
			   strcmp(c_type, "timestamptz") == 0) {
			space = stream_printf(toConsole, "TIMESTAMP", c_type);
			if (strcmp(c_type_digits, "7") != 0)
				space += stream_printf(toConsole, "(%d)", atoi(c_type_digits) - 1);
			if (strcmp(c_type, "timestamptz") == 0)
				space += stream_printf(toConsole, " WITH TIME ZONE");
		} else if (strcmp(c_type, "time") == 0 ||
			   strcmp(c_type, "timetz") == 0) {
			space = stream_printf(toConsole, "TIME", c_type);
			if (strcmp(c_type_digits, "1") != 0)
				space += stream_printf(toConsole, "(%d)", atoi(c_type_digits) - 1);
			if (strcmp(c_type, "timetz") == 0)
				space += stream_printf(toConsole, " WITH TIME ZONE");
		} else if (strcmp(c_type, "real") == 0) {
			if (strcmp(c_type_digits, "24") == 0 &&
			    strcmp(c_type_scale, "0") == 0)
				space = stream_printf(toConsole, "real");
			else if (strcmp(c_type_scale, "0") == 0)
				space = stream_printf(toConsole, "float(%s)", c_type_digits);
			else
				space = stream_printf(toConsole, "float(%s,%s)",
						c_type_digits, c_type_scale);
		} else if (strcmp(c_type, "double") == 0) {
			if (strcmp(c_type_digits, "53") == 0 &&
			    strcmp(c_type_scale, "0") == 0)
				space = stream_printf(toConsole, "double");
			else if (strcmp(c_type_scale, "0") == 0)
				space = stream_printf(toConsole, "float(%s)", c_type_digits);
			else
				space = stream_printf(toConsole, "float(%s,%s)",
						c_type_digits, c_type_scale);
		} else if (strcmp(c_type, "decimal") == 0 &&
			   strcmp(c_type_digits, "1") == 0 &&
			   strcmp(c_type_scale, "0") == 0) {
			space = stream_printf(toConsole, "decimal");
		} else if (strcmp(c_type_digits, "0") == 0) {
			space = stream_printf(toConsole, "%s", c_type);
		} else if (strcmp(c_type_scale, "0") == 0) {
			space = stream_printf(toConsole, "%s(%s)", c_type, c_type_digits);
		} else {
			space = stream_printf(toConsole, "%s(%s,%s)",
					c_type, c_type_digits, c_type_scale);
		}
		if (strcmp(c_null, "false") == 0)
			stream_printf(toConsole, "%*s NOT NULL",
					CAP(13 - space), "");
		if (c_default != NULL)
			stream_printf(toConsole, "%*s DEFAULT %s",
					CAP(13 - space), "", c_default);
		cnt++;
		if (stream_errnr(toConsole))
			goto bailout;
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		goto bailout;
	}
	mapi_close_handle(hdl);
	hdl = NULL;
	/* presumably we don't need to order on id, since there should
	   only be a single primary key, but it doesn't hurt, and the
	   code is then close to the code for the uniqueness
	   constraint */
	snprintf(query, maxquerylen,
		 "SELECT \"kc\".\"column\","		/* 0 */
			"\"kc\".\"nr\", "		/* 1 */
			"\"k\".\"name\", "		/* 2 */
			"\"k\".\"id\" "			/* 3 */
		 "FROM \"sys\".\"keycolumns\" \"kc\", "
		      "\"sys\".\"keys\" \"k\", "
		      "\"sys\".\"schemas\" \"s\", "
		      "\"sys\".\"_tables\" \"t\" "
		 "WHERE \"kc\".\"id\" = \"k\".\"id\" AND "
		       "\"k\".\"table_id\" = \"t\".\"id\" AND "
		       "\"k\".\"type\" = 0 AND "
		       "\"t\".\"schema_id\" = \"s\".\"id\" AND "
		       "\"s\".\"name\" = '%s' AND "
		       "\"t\".\"name\" = '%s' "
		 "ORDER BY \"id\", \"nr\"", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		char *c_column = mapi_fetch_field(hdl, 0);
		char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			goto bailout;
		}
		if (cnt == 0) {
			stream_printf(toConsole, ",\n\t");
			if (k_name) {
				stream_printf(toConsole, "CONSTRAINT ");
				quoted_print(toConsole, k_name);
				stream_write(toConsole, " ", 1, 1);
			}
			stream_printf(toConsole, "PRIMARY KEY (");
		} else
			stream_printf(toConsole, ", ");
		quoted_print(toConsole, c_column);
		cnt++;
		if (stream_errnr(toConsole))
			goto bailout;
	}
	if (cnt)
		stream_printf(toConsole, ")");
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		goto bailout;
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	snprintf(query, maxquerylen,
		 "SELECT \"kc\".\"column\","		/* 0 */
			"\"kc\".\"nr\", "		/* 1 */
			"\"k\".\"name\", "		/* 2 */
			"\"k\".\"id\" "			/* 3 */
		 "FROM \"sys\".\"keycolumns\" \"kc\", "
		      "\"sys\".\"keys\" \"k\", "
		      "\"sys\".\"schemas\" \"s\", "
		      "\"sys\".\"_tables\" \"t\" "
		 "WHERE \"kc\".\"id\" = \"k\".\"id\" AND "
		       "\"k\".\"table_id\" = \"t\".\"id\" AND "
		       "\"k\".\"type\" = 1 AND "
		       "\"t\".\"schema_id\" = \"s\".\"id\" AND "
		       "\"s\".\"name\" = '%s' AND "
		       "\"t\".\"name\" = '%s' "
		 "ORDER BY \"id\", \"nr\"", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		char *c_column = mapi_fetch_field(hdl, 0);
		char *kc_nr = mapi_fetch_field(hdl, 1);
		char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			goto bailout;
		}
		if (strcmp(kc_nr, "0") == 0) {
			if (cnt)
				stream_write(toConsole, ")", 1, 1);
			stream_printf(toConsole, ",\n\t");
			if (k_name) {
				stream_printf(toConsole, "CONSTRAINT ");
				quoted_print(toConsole, k_name);
				stream_write(toConsole, " ", 1, 1);
			}
			stream_printf(toConsole, "UNIQUE (");
			cnt = 1;
		} else
			stream_printf(toConsole, ", ");
		quoted_print(toConsole, c_column);
		if (stream_errnr(toConsole))
			goto bailout;
	}
	if (cnt)
		stream_write(toConsole, ")", 1, 1);
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		goto bailout;
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	if (foreign &&
	    dump_foreign_keys(mid, schema, tname, toConsole))
		goto bailout;

	stream_printf(toConsole, "\n");

	stream_printf(toConsole, ");\n");

	snprintf(query, maxquerylen,
		 "SELECT \"i\".\"name\", "		/* 0 */
			"\"k\".\"name\", "		/* 1 */
			"\"kc\".\"nr\", "		/* 2 */
			"\"c\".\"name\" "		/* 3 */
		 "FROM \"sys\".\"idxs\" AS \"i\" LEFT JOIN \"sys\".\"keys\" AS \"k\" "
				"ON \"i\".\"name\" = \"k\".\"name\", "
		      "\"sys\".\"keycolumns\" AS \"kc\", "
		      "\"sys\".\"_columns\" AS \"c\", "
		      "\"sys\".\"schemas\" \"s\", "
		      "\"sys\".\"_tables\" AS \"t\" "
		 "WHERE \"i\".\"table_id\" = \"t\".\"id\" AND "
		       "\"i\".\"id\" = \"kc\".\"id\" AND "
		       "\"t\".\"id\" = \"c\".\"table_id\" AND "
		       "\"kc\".\"column\" = \"c\".\"name\" AND "
		       "(\"k\".\"type\" IS NULL OR \"k\".\"type\" = 1) AND "
		       "\"t\".\"schema_id\" = \"s\".\"id\" AND "
		       "\"s\".\"name\" = '%s' AND "
		       "\"t\".\"name\" = '%s' "
		 "ORDER BY \"i\".\"name\", \"kc\".\"nr\"", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}
	cnt = 0;
	while (mapi_fetch_row(hdl) != 0) {
		char *i_name = mapi_fetch_field(hdl, 0);
		char *k_name = mapi_fetch_field(hdl, 1);
		char *kc_nr = mapi_fetch_field(hdl, 2);
		char *c_name = mapi_fetch_field(hdl, 3);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			goto bailout;
		}
		if (k_name != NULL) {
			/* unique key, already handled */
			continue;
		}

		if (strcmp(kc_nr, "0") == 0) {
			if (cnt)
				stream_printf(toConsole, ");\n");
			stream_printf(toConsole, "CREATE INDEX ");
			quoted_print(toConsole, i_name);
			stream_printf(toConsole, " ON ");
			quoted_print(toConsole, schema);
			stream_printf(toConsole, ".");
			quoted_print(toConsole, tname);
			stream_printf(toConsole, " (");
			cnt = 1;
		} else
			stream_printf(toConsole, ", ");
		quoted_print(toConsole, c_name);
		if (stream_errnr(toConsole))
			goto bailout;
	}
	if (cnt)
		stream_printf(toConsole, ");\n");
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		goto bailout;
	}

  doreturn:
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
	if (hdl)
		mapi_close_handle(hdl);
	if (view)
		free(view);
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	return 1;
}

int
dump_table_data(Mapi mid, char *schema, char *tname, stream *toConsole)
{
	int cnt, i;
	MapiHdl hdl = NULL;
	char *query;
	size_t maxquerylen;
	int *string;
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
		 "SELECT \"t\".\"name\", \"t\".\"query\" "
		 "FROM \"sys\".\"_tables\" \"t\", \"sys\".\"schemas\" \"s\" "
		 "WHERE \"s\".\"name\" = '%s' "
		 "AND \"t\".\"schema_id\" = \"s\".\"id\" "
		 "AND \"t\".\"name\" = '%s'",
		 schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}
	if (mapi_rows_affected(hdl) != 1) {
		if (mapi_rows_affected(hdl) == 0)
			fprintf(stderr, "Table %s.%s does not exist.\n", schema, tname);
		else
			fprintf(stderr, "Table %s.%s not unique.\n", schema, tname);
		goto bailout;
	}
	while ((mapi_fetch_row(hdl)) != 0) {
		if (mapi_fetch_field(hdl, 1)) {
			/* the table is actually a view */
			goto doreturn;
		}
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		goto bailout;
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	snprintf(query, maxquerylen, "SELECT count(*) FROM \"%s\".\"%s\"", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}
	if (mapi_fetch_row(hdl)) {
		char *cntfld = mapi_fetch_field(hdl, 0);

		if (strcmp(cntfld, "0") == 0) {
			/* no records to dump, so return early */
			goto doreturn;
		}

		stream_printf(toConsole, "COPY %s RECORDS INTO ", cntfld);
		quoted_print(toConsole, schema);
		stream_printf(toConsole, ".");
		quoted_print(toConsole, tname);
		stream_printf(toConsole, " FROM stdin USING DELIMITERS '\\t','\\n','\"';\n");
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	snprintf(query, maxquerylen, "SELECT * FROM \"%s\".\"%s\"", schema, tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl)
			mapi_explain_query(hdl, stderr);
		else
			mapi_explain(mid, stderr);
		goto bailout;
	}

	cnt = mapi_get_field_count(hdl);
	string = malloc(sizeof(int) * cnt);
	for (i = 0; i < cnt; i++) {
		string[i] = 0;
		if (strcmp(mapi_get_type(hdl, i), "char") == 0 ||
		    strcmp(mapi_get_type(hdl, i), "varchar") == 0 ||
		    strcmp(mapi_get_type(hdl, i), "clob") == 0) {
			string[i] = 1;
		}
	}
	while (mapi_fetch_row(hdl)) {
		char *s;

		for (i = 0; i < cnt; i++) {
			s = mapi_fetch_field(hdl, i);
			if (s == NULL)
				stream_printf(toConsole, "NULL");
			else if (string[i]) {
				/* write double-quoted string with
				   certain characters escaped */
				quoted_print(toConsole, s);
			} else
				stream_printf(toConsole, "%s", s);
			if (i < cnt - 1)
				stream_write(toConsole, "\t", 1, 1);
			else
				stream_write(toConsole, "\n", 1, 1);
		}
		if (stream_errnr(toConsole))
			goto bailout;
	}
	free(string);
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		goto bailout;
	}
  doreturn:
	if (hdl)
		mapi_close_handle(hdl);
	if (query != NULL)
		free(query);
	if (sname != NULL)
		free(sname);
	return 0;
  bailout:
	if (hdl)
		mapi_close_handle(hdl);
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	return 1;
}

int
dump_table(Mapi mid, char *schema, char *tname, stream *toConsole, int describe, int foreign)
{
	int rc;

	rc = describe_table(mid, schema, tname, toConsole, foreign);
	if (rc == 0 && !describe)
		rc = dump_table_data(mid, schema, tname, toConsole);
	return rc;
}

int
dump_functions(Mapi mid, stream *toConsole, const char *sname)
{
	const char functions[] = "SELECT \"f\".\"func\" "
		"FROM \"sys\".\"schemas\" \"s\","
		     "\"sys\".\"functions\" \"f\" "
		"WHERE \"f\".\"sql\" = TRUE AND "
		      "\"s\".\"id\" = \"f\".\"schema_id\""
		      "%s%s%s "
		"ORDER BY \"f\".\"id\"";
	MapiHdl hdl;
	char *q;

	if (sname != NULL) {
		size_t l = sizeof(functions) + strlen(sname) + 20;

		q = malloc(l);
		snprintf(q, l, functions, " AND \"s\".\"name\" = '", sname, "'");
	} else {
		q = malloc(sizeof(functions));
		snprintf(q, sizeof(functions), functions, "", "", "");
	}
	hdl = mapi_query(mid, q);
	free(q);
	if (hdl == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}
	while (!stream_errnr(toConsole) &&
	       mapi_fetch_row(hdl) != 0) {
		char *query = mapi_fetch_field(hdl, 0);

		stream_printf(toConsole, "%s\n", query);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);
	return stream_errnr(toConsole) ? 1 : 0;
}

int
dump_tables(Mapi mid, stream *toConsole, int describe)
{
	const char *start = "START TRANSACTION";
	const char *end = "ROLLBACK";
	const char *chkhash = "SELECT \"id\" "
		"FROM \"sys\".\"functions\" "
		"WHERE \"name\" = 'password_hash'";
	const char *createhash = "CREATE FUNCTION \"password_hash\" (\"username\" STRING) "
		"RETURNS STRING "
		"EXTERNAL NAME \"sql\".\"password\"";
	const char *drophash = "DROP FUNCTION \"password_hash\"";
	const char *users1 = "SELECT \"name\", "
		     "\"fullname\", "
		     "\"password_hash\"(\"name\") "
		"FROM \"sys\".\"db_user_info\" "
		"WHERE \"name\" <> 'monetdb' "
		"ORDER BY \"name\"";
	const char *users2 = "SELECT \"ui\".\"name\", "
		     "\"s\".\"name\" "
		"FROM \"sys\".\"db_user_info\" \"ui\", "
		     "\"sys\".\"schemas\" \"s\" "
		"WHERE \"ui\".\"default_schema\" = \"s\".\"id\" AND "
		      "\"ui\".\"name\" <> 'monetdb' AND "
		      "\"s\".\"name\" <> 'sys' "
		"ORDER BY \"ui\".\"name\"";
	const char *roles = "SELECT \"name\" "
		"FROM \"sys\".\"auths\" "
		"WHERE \"name\" NOT IN (SELECT \"name\" "
				       "FROM \"sys\".\"db_user_info\") AND "
		      "\"grantor\" <> 0 "
		"ORDER BY \"name\"";
	const char *grants = "SELECT \"a1\".\"name\", "
		     "\"a2\".\"name\" "
		"FROM \"sys\".\"auths\" \"a1\", "
		     "\"sys\".\"auths\" \"a2\", "
		     "\"sys\".\"user_role\" \"ur\" "
		"WHERE \"a1\".\"id\" = \"ur\".\"login_id\" AND "
		      "\"a2\".\"id\" = \"ur\".\"role_id\" "
		"ORDER BY \"a1\".\"name\", \"a2\".\"name\"";
	const char *schemas = "SELECT \"s\".\"name\", \"a\".\"name\" "
		"FROM \"sys\".\"schemas\" \"s\", "
		     "\"sys\".\"auths\" \"a\" "
		"WHERE \"s\".\"authorization\" = \"a\".\"id\" AND "
		      "\"s\".\"name\" NOT IN ('sys', 'tmp') "
		"ORDER BY \"s\".\"name\"";
	/* alternative, but then need to handle NULL in second column:
	   SELECT "s"."name", "a"."name"
	   FROM "sys"."schemas" "s"
		LEFT OUTER JOIN "sys"."auths" "a"
		     ON "s"."authorization" = "a"."id" AND
		"s"."name" NOT IN ('sys', 'tmp')
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
	const char *sequences1 = "SELECT \"sch\".\"name\",\"seq\".\"name\" "
		"FROM \"sys\".\"schemas\" \"sch\", "
		     "\"sys\".\"sequences\" \"seq\" "
		"WHERE \"sch\".\"id\" = \"seq\".\"schema_id\" "
		"ORDER BY \"sch\".\"name\",\"seq\".\"name\"";
	const char *sequences2 = "SELECT \"s\".\"name\","
		     "\"seq\".\"name\","
		     "get_value_for(\"s\".\"name\",\"seq\".\"name\"),"
		     "\"seq\".\"minvalue\","
		     "\"seq\".\"maxvalue\","
		     "\"seq\".\"increment\","
		     "\"seq\".\"cycle\" "
		"FROM \"sys\".\"sequences\" \"seq\", "
		     "\"sys\".\"schemas\" \"s\" "
		"WHERE \"s\".\"id\" = \"seq\".\"schema_id\" "
		"ORDER BY \"s\".\"name\",\"seq\".\"name\"";
	const char *tables_and_functions = "WITH \"tf_xYzzY\" AS ("
			"SELECT \"s\".\"name\" AS \"sname\", "
			       "\"f\".\"name\" AS \"name\", "
			       "\"f\".\"id\" AS \"id\", "
			       "\"f\".\"func\" AS \"func\" "
			"FROM \"sys\".\"schemas\" \"s\", "
			     "\"sys\".\"functions\" \"f\" "
			"WHERE \"f\".\"sql\" = TRUE AND "
			      "\"s\".\"id\" = \"f\".\"schema_id\" "
			"UNION "
			"SELECT \"s\".\"name\" AS \"sname\", "
			       "\"t\".\"name\" AS \"name\", "
			       "\"t\".\"id\" AS \"id\", "
			       "CAST(NULL AS VARCHAR(8196)) AS \"func\" "
			"FROM \"sys\".\"schemas\" \"s\", "
			     "\"sys\".\"_tables\" \"t\" "
			"WHERE \"t\".\"type\" BETWEEN 0 AND 1 AND "
			      "\"t\".\"system\" = FALSE AND "
			      "\"s\".\"id\" = \"t\".\"schema_id\" "
			"UNION "
			"SELECT \"s\".\"name\" AS \"sname\", "
			       "\"tr\".\"name\" AS \"name\", "
			       "\"tr\".\"id\" AS \"id\", "
			       "\"tr\".\"statement\" AS \"func\" "
			"FROM \"sys\".\"triggers\" \"tr\", "
			     "\"sys\".\"schemas\" \"s\", "
			     "\"sys\".\"_tables\" \"t\" "
			"WHERE \"s\".\"id\" = \"t\".\"schema_id\" AND "
			      "\"t\".\"id\" = \"tr\".\"table_id\""
		") "
		"SELECT * FROM \"tf_xYzzY\" ORDER BY \"tf_xYzzY\".\"id\"";
	char *sname;
	char *curschema = NULL;
	MapiHdl hdl;
	int create_hash_func = 0;
	int rc = 0;

	/* start a transaction for the dump */
	if (!describe)
		stream_printf(toConsole, "START TRANSACTION;\n");

	if ((hdl = mapi_query(mid, start)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}
	mapi_close_handle(hdl);

	sname = get_schema(mid);
	if (sname == NULL)
		return 1;
	if (strcmp(sname, "sys") == 0 || strcmp(sname, "tmp") == 0) {
		free(sname);
		sname = NULL;

		/* dump roles */
		if ((hdl = mapi_query(mid, roles)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}

		while (mapi_fetch_row(hdl) != 0) {
			char *name = mapi_fetch_field(hdl, 0);

			stream_printf(toConsole, "CREATE ROLE ");
			quoted_print(toConsole, name);
			stream_printf(toConsole, ";\n");
		}
		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		mapi_close_handle(hdl);


		/* dump users, part 1 */
		/* first make sure the password_hash function exists */
		if ((hdl = mapi_query(mid, chkhash)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}
		create_hash_func = mapi_rows_affected(hdl) == 0;
		mapi_close_handle(hdl);
		if (create_hash_func) {
			if ((hdl = mapi_query(mid, createhash)) == NULL || mapi_error(mid)) {
				if (hdl) {
					mapi_explain_query(hdl, stderr);
					mapi_close_handle(hdl);
				} else
					mapi_explain(mid, stderr);
				return 1;
			}
			mapi_close_handle(hdl);
		}

		if ((hdl = mapi_query(mid, users1)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}

		while (mapi_fetch_row(hdl) != 0) {
			char *uname = mapi_fetch_field(hdl, 0);
			char *fullname = mapi_fetch_field(hdl, 1);
			char *pwhash = mapi_fetch_field(hdl, 2);

			stream_printf(toConsole, "CREATE USER ");
			quoted_print(toConsole, uname);
			stream_printf(toConsole, " WITH ENCRYPTED PASSWORD '%s' NAME '%s' SCHEMA \"sys\";\n", pwhash, fullname);
		}
		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		mapi_close_handle(hdl);

		/* clean up -- not strictly necessary due to ROLLBACK */
		if (create_hash_func) {
			if ((hdl = mapi_query(mid, drophash)) == NULL || mapi_error(mid)) {
				if (hdl) {
					mapi_explain_query(hdl, stderr);
					mapi_close_handle(hdl);
				} else
					mapi_explain(mid, stderr);
				return 1;
			}
			mapi_close_handle(hdl);
		}

		/* dump schemas */
		if ((hdl = mapi_query(mid, schemas)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}

		while (mapi_fetch_row(hdl) != 0) {
			char *sname = mapi_fetch_field(hdl, 0);
			char *aname = mapi_fetch_field(hdl, 1);

			stream_printf(toConsole, "CREATE SCHEMA ");
			quoted_print(toConsole, sname);
			if (strcmp(aname, "sysadmin") != 0) {
				stream_printf(toConsole, " AUTHORIZATION ");
				quoted_print(toConsole, aname);
			}
			stream_printf(toConsole, ";\n");
		}
		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		mapi_close_handle(hdl);

		/* dump users, part 2 */
		if ((hdl = mapi_query(mid, users2)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}

		while (mapi_fetch_row(hdl) != 0) {
			char *uname = mapi_fetch_field(hdl, 0);
			char *sname = mapi_fetch_field(hdl, 1);

			stream_printf(toConsole, "ALTER USER ");
			quoted_print(toConsole, uname);
			stream_printf(toConsole, " SET SCHEMA ");
			quoted_print(toConsole, sname);
			stream_printf(toConsole, ";\n");
		}
		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		mapi_close_handle(hdl);

		/* grant user privileges */
		if ((hdl = mapi_query(mid, grants)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}

		while (mapi_fetch_row(hdl) != 0) {
			char *uname = mapi_fetch_field(hdl, 0);
			char *rname = mapi_fetch_field(hdl, 1);

			stream_printf(toConsole, "GRANT ");
			quoted_print(toConsole, rname);
			stream_printf(toConsole, " TO ");
			quoted_print(toConsole, uname);
			stream_printf(toConsole, ";\n");
		}
		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		mapi_close_handle(hdl);
	} else {
		stream_printf(toConsole, "SET SCHEMA ");
		quoted_print(toConsole, sname);
		stream_printf(toConsole, ";\n");
		curschema = strdup(sname);
	}

	/* dump sequences, part 1 */
	if ((hdl = mapi_query(mid, sequences1)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}

	while (mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *name = mapi_fetch_field(hdl, 1);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		stream_printf(toConsole, "CREATE SEQUENCE ");
		quoted_print(toConsole, schema);
		stream_printf(toConsole, ".");
		quoted_print(toConsole, name);
		stream_printf(toConsole, " AS INTEGER;\n");
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);

	/* dump tables */
	if ((hdl = mapi_query(mid, tables_and_functions)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}

	while (rc == 0 &&
	       !stream_errnr(toConsole) &&
	       mapi_fetch_row(hdl) != 0) {
		char *schema = mapi_fetch_field(hdl, 0);
		char *tname = mapi_fetch_field(hdl, 1);
		char *func = mapi_fetch_field(hdl, 3);

		if (mapi_error(mid)) {
			mapi_explain(mid, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			stream_printf(toConsole, "SET SCHEMA ");
			quoted_print(toConsole, curschema);
			stream_printf(toConsole, ";\n");
		}
		if (func == NULL) {
			if (schema)
				schema = strdup(schema);
			tname = strdup(tname);
			rc = dump_table(mid, schema, tname, toConsole, describe, describe);
			if (schema)
				free(schema);
			free(tname);
		} else
			stream_printf(toConsole, "%s\n", func);
	}
	if (curschema) {
		if (sname == NULL || strcmp(sname, curschema) != 0) {
			stream_printf(toConsole, "SET SCHEMA ");
			quoted_print(toConsole, sname ? sname : "sys");
			stream_printf(toConsole, ";\n");
		}
		free(curschema);
		curschema = NULL;
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);
	if (stream_errnr(toConsole))
		return 1;

	if (!describe) {
		if (dump_foreign_keys(mid, NULL, NULL, toConsole))
			return 1;

		/* dump sequences, part 2 */
		if ((hdl = mapi_query(mid, sequences2)) == NULL || mapi_error(mid)) {
			if (hdl) {
				mapi_explain_query(hdl, stderr);
				mapi_close_handle(hdl);
			} else
				mapi_explain(mid, stderr);
			return 1;
		}

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

			stream_printf(toConsole, "ALTER SEQUENCE ");
			quoted_print(toConsole, schema);
			stream_printf(toConsole, ".");
			quoted_print(toConsole, name);
			stream_printf(toConsole, " RESTART WITH %s", restart);
			if (strcmp(increment, "1") != 0)
				stream_printf(toConsole, " INCREMENT BY %s", increment);
			if (strcmp(minvalue, "0") != 0)
				stream_printf(toConsole, " MINVALUE %s", minvalue);
			if (strcmp(maxvalue, "0") != 0)
				stream_printf(toConsole, " MAXVALUE %s", maxvalue);
			stream_printf(toConsole, " %sCYCLE;\n", strcmp(cycle, "true") == 0 ? "" : "NO ");
			if (stream_errnr(toConsole)) {
				mapi_close_handle(hdl);
				return 1;
			}
		}
		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			return 1;
		}
		mapi_close_handle(hdl);
	}

	if ((hdl = mapi_query(mid, end)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}
	mapi_close_handle(hdl);

	/* finally commit the whole transaction */
	if (!describe)
		stream_printf(toConsole, "COMMIT;\n");

	return rc;
}
