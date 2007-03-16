#include "clients_config.h"
#include <monet_options.h>
#include "Mapi.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include "msqldump.h"

static void
quoted_print(FILE *f, const char *s)
{
	putc('"', f);
	while (*s) {
		switch (*s) {
		case '\\':
		case '"':
			putc('\\', f);
			putc(*s, f);
			break;
		case '\n':
			putc('\\', f);
			putc('n', f);
			break;
		case '\t':
			putc('\\', f);
			putc('t', f);
			break;
		default:
			if ((0 < *s && *s < 32) || *s == '\377')
				fprintf(f, "\\%03o", *s & 0377);
			else
				putc(*s, f);
			break;
		}
		s++;
	}
	putc('"', f);
}

int
dump_table(Mapi mid, char *tname, FILE *toConsole)
{
	int cnt = 0, i;
	MapiHdl hdl;
	char *query;
	size_t maxquerylen = BUFSIZ + strlen(tname);
	int *string;

	query = malloc(maxquerylen);
	fprintf(toConsole, "CREATE TABLE ");
	quoted_print(toConsole, tname);
	fprintf(toConsole, " (\n");

	snprintf(query, maxquerylen,
		 "SELECT \"c\".\"name\","		/* 0 */
			"\"c\".\"type\","		/* 1 */
			"\"c\".\"type_digits\","	/* 2 */
			"\"c\".\"type_scale\","		/* 3 */
			"\"c\".\"null\","		/* 4 */
			"\"c\".\"default\","		/* 5 */
			"\"c\".\"number\" "		/* 6 */
		 "FROM \"columns\" \"c\",\"tables\" \"t\" "
		 "WHERE \"c\".\"table_id\" = \"t\".\"id\" "
		 "AND '%s' = \"t\".\"name\" ORDER BY \"number\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}

	while ((mapi_fetch_row(hdl)) != 0) {
		char *c_name = mapi_fetch_field(hdl, 0);
		char *c_type = mapi_fetch_field(hdl, 1);
		char *c_type_digits = mapi_fetch_field(hdl, 2);
		char *c_type_scale = mapi_fetch_field(hdl, 3);
		char *c_null = mapi_fetch_field(hdl, 4);
		char *c_default = mapi_fetch_field(hdl, 5);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			free(query);
			return 1;
		}
		if (cnt)
			fprintf(toConsole, ",\n");

		putc('\t', toConsole);
		quoted_print(toConsole, c_name);
		putc(' ', toConsole);
		if (strcmp(c_type, "boolean") == 0 ||
		    strcmp(c_type, "int") == 0 ||
		    strcmp(c_type, "smallint") == 0 ||
		    strcmp(c_type, "bigint") == 0 ||
		    strcmp(c_type, "double") == 0 ||
		    strcmp(c_type, "real") == 0 ||
		    strcmp(c_type, "date") == 0) {
			fprintf(toConsole, "%s", c_type);
		} else if (strcmp(c_type, "month_interval") == 0) {
			if (*c_type_scale == '1') {
				if (*c_type_digits == 1)
					fprintf(toConsole, "INTERVAL YEAR");
				else
					fprintf(toConsole, "INTERVAL YEAR TO MONTH");
			} else
				fprintf(toConsole, "INTERVAL MONTH");
		} else if (strcmp(c_type, "sec_interval") == 0) {
			switch (*c_type_scale) {
			case '3':
				switch (*c_type_digits) {
				case '3':
					fprintf(toConsole, "INTERVAL DAY");
					break;
				case '4':
					fprintf(toConsole, "INTERVAL DAY TO HOUR");
					break;
				case '5':
					fprintf(toConsole, "INTERVAL DAY TO MINUTE");
					break;
				case '6':
					fprintf(toConsole, "INTERVAL DAY TO SECOND");
					break;
				}
				break;
			case '4':
				switch (*c_type_digits) {
				case '4':
					fprintf(toConsole, "INTERVAL HOUR");
					break;
				case '5':
					fprintf(toConsole, "INTERVAL HOUR TO MINUTE");
					break;
				case '6':
					fprintf(toConsole, "INTERVAL HOUR TO SECOND");
					break;
				}
				break;
			case '5':
				switch (*c_type_digits) {
				case '5':
					fprintf(toConsole, "INTERVAL MINUTE");
					break;
				case '6':
					fprintf(toConsole, "INTERVAL MINUTE TO SECOND");
					break;
				}
				break;
			case '6':
				fprintf(toConsole, "INTERVAL SECOND");
				break;
			}		
		} else if (strcmp(c_type, "clob") == 0) {
			fprintf(toConsole, "CHARACTER LARGE OBJECT");
			if (strcmp(c_type_digits, "0") != 0)
				fprintf(toConsole, "(%s)", c_type_digits);
		} else if (strcmp(c_type, "timestamp") == 0 ||
			   strcmp(c_type, "time") == 0) {
			fprintf(toConsole, "%s", c_type);
			if (strcmp(c_type_digits, "0") != 0)
				fprintf(toConsole, "(%s)", c_type_digits);
			if (strcmp(c_type_scale, "1") == 0)
				fprintf(toConsole, " WITH TIME ZONE");
		} else if (strcmp(c_type_digits, "0") == 0) {
			fprintf(toConsole, "%s", c_type);
		} else if (strcmp(c_type_scale, "0") == 0) {
			fprintf(toConsole, "%s(%s)", c_type, c_type_digits);
		} else {
			fprintf(toConsole, "%s(%s,%s)", c_type, c_type_digits, c_type_scale);
		}
		if (strcmp(c_null, "false") == 0)
			fprintf(toConsole, " NOT NULL");
		if (c_default != NULL)
			fprintf(toConsole, " DEFAULT %s", c_default);
		cnt++;
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);
	snprintf(query, maxquerylen,
		 "SELECT \"kc\".\"column\","		/* 0 */
			"\"kc\".\"nr\", "		/* 1 */
			"\"k\".\"name\" "		/* 2 */
		 "FROM \"keycolumns\" \"kc\", "
		      "\"keys\" \"k\", "
		      "\"tables\" \"t\" "
		 "WHERE \"kc\".\"id\" = \"k\".\"id\" AND "
		       "\"k\".\"table_id\" = \"t\".\"id\" AND "
		       "\"k\".\"type\" = 0 AND "
		       "\"t\".\"name\" = '%s' "
		 "ORDER BY \"nr\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		char *c_column = mapi_fetch_field(hdl, 0);
		char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			free(query);
			return 1;
		}
		if (cnt == 0) {
			fprintf(toConsole, ",\n\t");
			if (k_name) {
				fprintf(toConsole, "CONSTRAINT ");
				quoted_print(toConsole, k_name);
				putc(' ', toConsole);
			}
			fprintf(toConsole, "PRIMARY KEY (");
		} else
			fprintf(toConsole, ", ");
		quoted_print(toConsole, c_column);
		cnt++;
	}
	if (cnt)
		fprintf(toConsole, ")");
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);

	snprintf(query, maxquerylen,
		 "SELECT \"kc\".\"column\","		/* 0 */
			"\"kc\".\"nr\", "		/* 1 */
			"\"k\".\"name\" "		/* 2 */
		 "FROM \"keycolumns\" \"kc\", "
		      "\"keys\" \"k\", "
		      "\"tables\" \"t\" "
		 "WHERE \"kc\".\"id\" = \"k\".\"id\" AND "
		       "\"k\".\"table_id\" = \"t\".\"id\" AND "
		       "\"k\".\"type\" = 1 AND "
		       "\"t\".\"name\" = '%s' "
		 "ORDER BY \"nr\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		char *c_column = mapi_fetch_field(hdl, 0);
		char *kc_nr = mapi_fetch_field(hdl, 1);
		char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			free(query);
			return 1;
		}
		if (strcmp(kc_nr, "0") == 0) {
			if (cnt)
				putc(')', toConsole);
			fprintf(toConsole, ",\n\t");
			if (k_name) {
				fprintf(toConsole, "CONSTRAINT ");
				quoted_print(toConsole, k_name);
				putc(' ', toConsole);
			}
			fprintf(toConsole, "UNIQUE (");
			cnt = 1;
		} else
			fprintf(toConsole, ", ");
		quoted_print(toConsole, c_column);
	}
	if (cnt)
		putc(')', toConsole);
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);

	snprintf(query, maxquerylen,
		 "SELECT \"pkt\".\"name\","		/* 0 */
			"\"pkkc\".\"column\","		/* 1 */
			"\"fkkc\".\"column\","		/* 2 */
			"\"fkkc\".\"nr\","		/* 3 */
			"\"fkk\".\"name\""		/* 4 */
		 "FROM \"tables\" \"fkt\","
		      "\"keycolumns\" \"fkkc\","
		      "\"keys\" \"fkk\","
		      "\"tables\" \"pkt\","
		      "\"keycolumns\" \"pkkc\","
		      "\"keys\" \"pkk\""
		 "WHERE \"fkt\".\"id\" = \"fkk\".\"table_id\" AND "
		       "\"pkt\".\"id\" = \"pkk\".\"table_id\" AND "
		       "\"fkk\".\"id\" = \"fkkc\".\"id\" AND "
		       "\"pkk\".\"id\" = \"pkkc\".\"id\" AND "
		       "\"fkk\".\"rkey\" = \"pkk\".\"id\" AND "
		       "\"fkkc\".\"nr\" = \"pkkc\".\"nr\" AND "
		       "\"fkt\".\"name\" = '%s'"
		 "ORDER BY \"fkk\".\"name\", \"nr\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}
	cnt = mapi_fetch_row(hdl);
	while (cnt != 0) {
		char *c_name = mapi_fetch_field(hdl, 0);
		char *c_pcolumn = mapi_fetch_field(hdl, 1);
		char *c_fcolumn = mapi_fetch_field(hdl, 2);
		char *c_nr = mapi_fetch_field(hdl, 3);
		char *c_fkname = mapi_fetch_field(hdl, 4);
		char **fkeys, **pkeys;
		int nkeys = 0;

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			free(query);
			return 1;
		}
		assert(strcmp(c_nr, "0") == 0);
		(void) c_nr;	/* pacify compilers in case assertions are disabled */
		nkeys = 1;
		fkeys = malloc(nkeys * sizeof(*fkeys));
		pkeys = malloc(nkeys * sizeof(*pkeys));
		pkeys[nkeys - 1] = c_pcolumn;
		fkeys[nkeys - 1] = c_fcolumn;
		while ((cnt = mapi_fetch_row(hdl)) != 0 && strcmp(mapi_fetch_field(hdl, 3), "0") != 0) {
			nkeys++;
			pkeys = realloc(pkeys, nkeys * sizeof(*pkeys));
			fkeys = realloc(fkeys, nkeys * sizeof(*fkeys));
			pkeys[nkeys - 1] = mapi_fetch_field(hdl, 1);
			fkeys[nkeys - 1] = mapi_fetch_field(hdl, 2);
		}
		fprintf(toConsole, ",\n\t");
		if (c_fkname) {
			fprintf(toConsole, "CONSTRAINT ");
			quoted_print(toConsole, c_fkname);
			putc(' ', toConsole);
		}
		fprintf(toConsole, "FOREIGN KEY (");
		for (i = 0; i < nkeys; i++) {
			if (i > 0)
				fprintf(toConsole, ", ");
			quoted_print(toConsole, fkeys[i]);
		}
		fprintf(toConsole, ") REFERENCES ");
		quoted_print(toConsole, c_name);
		fprintf(toConsole, " (");
		for (i = 0; i < nkeys; i++) {
			if (i > 0)
				fprintf(toConsole, ", ");
			quoted_print(toConsole, pkeys[i]);
		}
		fprintf(toConsole, ")");
		free(fkeys);
		free(pkeys);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);

	fprintf(toConsole, "\n");

	fprintf(toConsole, ");\n");

	snprintf(query, maxquerylen,
		 "SELECT \"i\".\"name\", "		/* 0 */
			"\"k\".\"name\", "		/* 1 */
			"\"kc\".\"nr\", "		/* 2 */
			"\"c\".\"name\" "		/* 3 */
		 "FROM \"idxs\" AS \"i\" LEFT JOIN \"keys\" AS \"k\" "
				"ON \"i\".\"name\" = \"k\".\"name\", "
		      "\"keycolumns\" AS \"kc\", "
		      "\"columns\" AS \"c\", "
		      "\"tables\" AS \"t\" "
		 "WHERE \"i\".\"table_id\" = \"t\".\"id\" AND "
		       "\"i\".\"id\" = \"kc\".\"id\" AND "
		       "\"t\".\"id\" = \"c\".\"table_id\" AND "
		       "\"kc\".\"column\" = \"c\".\"name\" AND "
		       "(\"k\".\"type\" IS NULL OR \"k\".\"type\" = 1) AND "
		       "\"t\".\"name\" = '%s' "
		 "ORDER BY \"i\".\"name\", \"kc\".\"nr\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}
	cnt = 0;
	while (mapi_fetch_row(hdl) != 0) {
		char *i_name = mapi_fetch_field(hdl, 0);
		char *k_name = mapi_fetch_field(hdl, 1);
		char *kc_nr = mapi_fetch_field(hdl, 2);
		char *c_name = mapi_fetch_field(hdl, 3);

		if (mapi_error(mid)) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
			free(query);
			return 1;
		}
		if (k_name != NULL) {
			/* unique key, already handled */
			continue;
		}

		if (strcmp(kc_nr, "0") == 0) {
			if (cnt)
				fprintf(toConsole, ");\n");
			fprintf(toConsole, "CREATE INDEX ");
			quoted_print(toConsole, i_name);
			fprintf(toConsole, " ON ");
			quoted_print(toConsole, tname);
			fprintf(toConsole, " (");
			cnt = 1;
		} else
			fprintf(toConsole, ", ");
		quoted_print(toConsole, c_name);
	}
	if (cnt)
		fprintf(toConsole, ");\n");
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);

	snprintf(query, maxquerylen, "SELECT count(*) FROM \"%s\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}
	if (mapi_fetch_row(hdl)) {
		char *cntfld = mapi_fetch_field(hdl, 0);

		fprintf(toConsole, "COPY %s RECORDS INTO ", cntfld);
		quoted_print(toConsole, tname);
		fprintf(toConsole, " FROM stdin USING DELIMITERS '\\t';\n");
	}
	mapi_close_handle(hdl);

	snprintf(query, maxquerylen, "SELECT * FROM \"%s\"", tname);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		free(query);
		return 1;
	}

	cnt = mapi_get_field_count(hdl);
	string = malloc(sizeof(int) * cnt);
	for (i = 0; i < cnt; i++) {
		string[i] = 0;
		if (strcmp(mapi_get_type(hdl, i), "char") == 0 || strcmp(mapi_get_type(hdl, i), "varchar") == 0) {
			string[i] = 1;
		}
	}
	while (mapi_fetch_row(hdl)) {
		char *s;

		for (i = 0; i < cnt; i++) {
			s = mapi_fetch_field(hdl, i);
			if (s == NULL)
				fputs("NULL", toConsole);
			else if (string[i]) {
				/* write double-quoted string with
				   certain characters escaped */
				quoted_print(toConsole, s);
			} else
				fputs(s, toConsole);
			if (i < cnt - 1)
				putc('\t', toConsole);
			else
				putc('\n', toConsole);
		}
	}
	free(string);
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		free(query);
		return 1;
	}
	mapi_close_handle(hdl);
	free(query);
	return 0;
}

int
dump_tables(Mapi mid, FILE *toConsole)
{
	const char *start = "START TRANSACTION";
	const char *end = "COMMIT";
	const char *sequences1 = "SELECT \"name\" FROM \"sequences\"";
	const char *sequences2 = "SELECT seq.\"name\",get_value_for(s.\"name\",seq.\"name\"),\"minvalue\",\"maxvalue\",\"increment\",\"cycle\" FROM \"sequences\" seq, \"schemas\" s WHERE s.id = seq.schema_id";
	const char *tables = "SELECT \"name\" FROM \"_tables\" WHERE "
		"\"type\" = 0 AND \"system\" = FALSE";
	const char *views = "SELECT \"name\",\"query\" FROM \"_tables\" WHERE "
		"\"type\" = 1 AND \"system\" = FALSE";
	const char *functions = "SELECT \"func\" FROM \"functions\" WHERE "
		"\"sql\" = TRUE";
	MapiHdl hdl;
	int rc = 0;

	/* start a transaction for the dump */
	fprintf(toConsole, "START TRANSACTION;\n");

	if ((hdl = mapi_query(mid, start)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}
	mapi_close_handle(hdl);

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
		char *name = mapi_fetch_field(hdl, 0);

		fprintf(toConsole, "CREATE SEQUENCE \"%s\" AS INTEGER;\n", name);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);

	/* dump tables */
	if ((hdl = mapi_query(mid, tables)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}

	while (mapi_fetch_row(hdl) != 0) {
		char *tname = mapi_fetch_field(hdl, 0);

		rc += dump_table(mid, tname, toConsole);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);

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
		char *name = mapi_fetch_field(hdl, 0);
		char *restart = mapi_fetch_field(hdl, 1);
		char *minvalue = mapi_fetch_field(hdl, 2);
		char *maxvalue = mapi_fetch_field(hdl, 3);
		char *increment = mapi_fetch_field(hdl, 4);
		char *cycle = mapi_fetch_field(hdl, 5);

		fprintf(toConsole, "ALTER SEQUENCE \"%s\" RESTART WITH %s", name, restart);
		if (strcmp(increment, "1") != 0)
			fprintf(toConsole, " INCREMENT BY %s", increment);
		if (strcmp(minvalue, "0") != 0)
			fprintf(toConsole, " MINVALUE %s", minvalue);
		if (strcmp(maxvalue, "0") != 0)
			fprintf(toConsole, " MAXVALUE %s", maxvalue);
		fprintf(toConsole, " %sCYCLE;\n", strcmp(cycle, "true") == 0 ? "" : "NO ");
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);

	/* dump views */
	if ((hdl = mapi_query(mid, views)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}
	while (mapi_fetch_row(hdl) != 0) {
		char *vname = mapi_fetch_field(hdl, 0);
		char *query = mapi_fetch_field(hdl, 1);

		fprintf(toConsole, "CREATE VIEW ");
		quoted_print(toConsole, vname);
		fprintf(toConsole, " AS %s\n", query);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);

	/* dump functions */
	if ((hdl = mapi_query(mid, functions)) == NULL || mapi_error(mid)) {
		if (hdl) {
			mapi_explain_query(hdl, stderr);
			mapi_close_handle(hdl);
		} else
			mapi_explain(mid, stderr);
		return 1;
	}
	while (mapi_fetch_row(hdl) != 0) {
		char *query = mapi_fetch_field(hdl, 0);

		fprintf(toConsole, "%s\n", query);
	}
	if (mapi_error(mid)) {
		mapi_explain_query(hdl, stderr);
		mapi_close_handle(hdl);
		return 1;
	}
	mapi_close_handle(hdl);

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
	fprintf(toConsole, "COMMIT;\n");

	return rc;
}
