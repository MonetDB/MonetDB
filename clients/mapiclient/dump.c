/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "mapi.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>

// TODO get rid of this ugly work around: Properly factor out mapi cals from dump.c
#ifdef COMPILING_MONETDBE

#define Mapi monetdbe_Mapi
#define MapiHdl monetdbe_MapiHdl
#define MapiHdl monetdbe_MapiHdl
#define MapiMsg monetdbe_MapiMsg

#define mapi_error monetdbe_mapi_error
#define mapi_query monetdbe_mapi_query
#define mapi_error monetdbe_mapi_error
#define mapi_close_handle monetdbe_mapi_close_handle
#define mapi_fetch_row monetdbe_mapi_fetch_row
#define mapi_fetch_field monetdbe_mapi_fetch_field
#define mapi_get_type monetdbe_mapi_get_type
#define mapi_seek_row monetdbe_mapi_seek_row
#define mapi_get_row_count monetdbe_mapi_get_row_count
#define mapi_rows_affected monetdbe_mapi_rows_affected
#define mapi_get_field_count monetdbe_mapi_get_field_count
#define mapi_result_error monetdbe_mapi_result_error
#define mapi_get_len monetdbe_mapi_get_len
#define mapi_explain monetdbe_mapi_explain
#define mapi_explain_query monetdbe_mapi_explain_query
#define mapi_explain_result monetdbe_mapi_explain_result

#include "monetdbe_mapi.h"
#else
#include "mapi.h"
#endif

#include "msqldump.h"

static int
dquoted_print(stream *f, const char *s, const char *suff)
{
	int space = 0;

	if (mnstr_write(f, "\"", 1, 1) < 0)
		return -1;
	space++;
	while (*s) {
		size_t n;
		if ((n = strcspn(s, "\"")) > 0) {
			if (mnstr_write(f, s, 1, n) < 0)
				return -1;
			space += (int) n;
			s += n;
		}
		if (*s) {
			assert(*s == '"');
			if (mnstr_write(f, "\"\"", 1, 2) < 0)
				return -1;
			space += 2;
			s++;
		}
	}
	if (mnstr_write(f, "\"", 1, 1) < 0)
		return -1;
	space++;
	if (suff != NULL) {
		int n;
		if ((n = mnstr_printf(f, "%s", suff)) < 0)
			return -1;
		space += n;
	}
	return space;
}

static int
squoted_print(stream *f, const char *s, char quote, bool noescape)
{
	assert(quote == '\'' || quote == '"');
	if (mnstr_printf(f, "%c", quote) < 0)
		return -1;
	while (*s) {
		size_t n = noescape ? strcspn(s, "'\"") :
			strcspn(s, "\\'\"\177"
					"\001\002\003\004\005\006\007"
					"\010\011\012\013\014\015\016\017"
					"\020\021\022\023\024\025\026\027"
					"\030\031\032\033\034\035\036\037");
		if (n > 0 && mnstr_write(f, s, 1, n) < 0)
			return -1;
		s += n;
		switch (*s) {
		case '\0':
			continue;
		case '\\':
			if (mnstr_write(f, "\\\\", 1, 2) < 0)
				return -1;
			break;
		case '\'':
		case '"':
			if (mnstr_write(f, s, 1, 1) < 0 ||
			    (*s == quote && mnstr_write(f, s, 1, 1) < 0))
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
			if (mnstr_printf(f, "\\%03o", (uint8_t) *s) < 0)
				return -1;
			break;
		}
		s++;
	}
	if (mnstr_printf(f, "%c", quote) < 0)
		return -1;
	return 0;
}

static char *
descape(const char *s)
{
	const char *p;
	size_t n = 1;

	for (p = s; *p; p++) {
		n += *p == '"';
	}
	n += p - s;
	char *d = malloc(n);
	if (d == NULL)
		return NULL;
	for (p = s, n = 0; *p; p++) {
		d[n++] = *p;
		if (*p == '"')
			d[n++] = '"';
	}
	d[n] = 0;
	return d;
}

static char *
sescape(const char *s)
{
	const char *p;
	size_t n = 1;

	for (p = s; *p; p++) {
		n += *p == '\'' || *p == '\\';
	}
	n += p - s;
	char *d = malloc(n);
	if (d == NULL)
		return NULL;
	for (p = s, n = 0; *p; p++) {
		d[n++] = *p;
		if (*p == '\'')
			d[n++] = '\'';
		else if (*p == '\\')
			d[n++] = '\\';
	}
	d[n] = 0;
	return d;
}

static int
comment_on(stream *sqlf, const char *object,
	   const char *ident1, const char *ident2, const char *ident3,
	   const char *remark)
{
	if (remark) {
		if (mnstr_printf(sqlf, "COMMENT ON %s ", object) < 0 ||
		    dquoted_print(sqlf, ident1, NULL) < 0)
			return -1;
		if (ident2) {
			if (mnstr_printf(sqlf, ".") < 0 ||
			    dquoted_print(sqlf, ident2, NULL) < 0)
				return -1;
			if (ident3) {
				if (mnstr_printf(sqlf, ".") < 0 ||
				    dquoted_print(sqlf, ident3, NULL) < 0)
					return -1;
			}
		}
		if (mnstr_write(sqlf, " IS ", 1, 4) < 0 ||
		    squoted_print(sqlf, remark, '\'', false) < 0 ||
		    mnstr_write(sqlf, ";\n", 1, 2) < 0)
			return -1;
	}
	return 0;
}

static const char *actions[] = {
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
	char *nsname = NULL, *sname = NULL;
	MapiHdl hdl;

	if ((hdl = mapi_query(mid, "SELECT current_schema")) == NULL ||
	    mapi_error(mid))
		goto bailout;
	while ((mapi_fetch_row(hdl)) != 0) {
		nsname = mapi_fetch_field(hdl, 0);

		if (mapi_error(mid))
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	/* copy before closing the handle */
	if (nsname)
		sname = strdup(nsname);
	if (nsname && !sname)
		goto bailout;
	mapi_close_handle(hdl);
	return sname;

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
	return NULL;
}

/* return TRUE if the HUGEINT type exists */
static bool
has_hugeint(Mapi mid)
{
	MapiHdl hdl;
	bool ret;
	static int answer = -1;

	if (answer >= 0)
		return (bool) answer;

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
	answer = (int) ret;
	return answer;

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

static bool
has_schema_path(Mapi mid)
{
	MapiHdl hdl;
	bool ret;
	static int answer = -1;

	if (answer >= 0)
		return answer;

	if ((hdl = mapi_query(mid, "select id from sys._columns where table_id = (select id from sys._tables where name = 'db_user_info' and schema_id = (select id from sys.schemas where name = 'sys')) and name = 'schema_path'")) == NULL ||
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
	answer = ret;
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
	return false;
}

static bool
has_schema_max_memory(Mapi mid)
{
	MapiHdl hdl;
	bool ret;
	static int answer = -1;

	if (answer >= 0)
		return answer;

	if ((hdl = mapi_query(mid, "select id from sys._columns where table_id = (select id from sys._tables where name = 'db_user_info' and schema_id = (select id from sys.schemas where name = 'sys')) and name = 'max_memory'")) == NULL ||
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
	answer = ret;
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
	return false;
}

static bool
has_table_partitions(Mapi mid)
{
	MapiHdl hdl;
	bool ret;
	static int answer = -1;

	if (answer >= 0)
		return answer;

	if ((hdl = mapi_query(mid,
			      "select id from sys._tables"
			      " where name = 'table_partitions'"
			      " and schema_id = ("
			      "select id from sys.schemas"
			      " where name = 'sys')")) == NULL ||
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
	answer = ret;
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
	return false;
}

static bool
has_remote_user_info_table(Mapi mid)
{
	MapiHdl hdl;
	bool ret;
	static int answer = -1;

	if (answer >= 0)
		return answer;

	if ((hdl = mapi_query(mid,
			      "select id from sys._tables"
			      " where name = 'remote_user_info'"
			      " and schema_id = ("
			      "select id from sys.schemas"
			      " where name = 'sys')")) == NULL ||
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
	answer = ret;
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
	return false;
}

static int
dump_foreign_keys(Mapi mid, const char *schema, const char *tname, const char *tid, stream *sqlf)
{
	MapiHdl hdl = NULL;
	int cnt, i;
	char *query;
	size_t maxquerylen = 0;

	if (tname != NULL) {
		char *s = sescape(schema);
		char *t = sescape(tname);
		if (s == NULL || t == NULL) {
			free(s);
			free(t);
			goto bailout;
		}
		maxquerylen = 1024 + strlen(t) + strlen(s);
		query = malloc(maxquerylen);
		if (query == NULL) {
			free(s);
			free(t);
			goto bailout;
		}
		snprintf(query, maxquerylen,
			 "SELECT ps.name, "			/* 0 */
			        "pkt.name, "		/* 1 */
				"pkkc.name, "			/* 2 */
				"fkkc.name, "			/* 3 */
				"fkkc.nr, "				/* 4 */
				"fkk.name, "			/* 5 */
				"fkk.\"action\", "		/* 6 */
				"fs.name, "				/* 7 */
				"fkt.name "				/* 8 */
			 "FROM sys._tables fkt, "
			      "sys.objects fkkc, "
			      "sys.keys fkk, "
			      "sys._tables pkt, "
			      "sys.objects pkkc, "
			      "sys.keys pkk, "
			      "sys.schemas ps, "
			      "sys.schemas fs "
			 "WHERE fkt.id = fkk.table_id "
			   "AND pkt.id = pkk.table_id "
			   "AND fkk.id = fkkc.id "
			   "AND pkk.id = pkkc.id "
			   "AND fkk.rkey = pkk.id "
			   "AND fkkc.nr = pkkc.nr "
			   "AND pkt.schema_id = ps.id "
			   "AND fkt.schema_id = fs.id "
			   "AND fs.name = '%s' "
			   "AND fkt.name = '%s' "
			 "ORDER BY fkk.name, fkkc.nr", s, t);
		free(s);
		free(t);
	} else if (tid != NULL) {
		maxquerylen = 1024 + strlen(tid);
		query = malloc(maxquerylen);
		if (query == NULL)
			goto bailout;
		snprintf(query, maxquerylen,
			 "SELECT ps.name, "			/* 0 */
			        "pkt.name, "		/* 1 */
				"pkkc.name, "			/* 2 */
				"fkkc.name, "			/* 3 */
				"fkkc.nr, "				/* 4 */
				"fkk.name, "			/* 5 */
				"fkk.\"action\", "		/* 6 */
				"0, "					/* 7 */
				"fkt.name "				/* 8 */
			 "FROM sys._tables fkt, "
			      "sys.objects fkkc, "
			      "sys.keys fkk, "
			      "sys._tables pkt, "
			      "sys.objects pkkc, "
			      "sys.keys pkk, "
			      "sys.schemas ps "
			 "WHERE fkt.id = fkk.table_id "
			   "AND pkt.id = pkk.table_id "
			   "AND fkk.id = fkkc.id "
			   "AND pkk.id = pkkc.id "
			   "AND fkk.rkey = pkk.id "
			   "AND fkkc.nr = pkkc.nr "
			   "AND pkt.schema_id = ps.id "
			   "AND fkt.id = %s "
			 "ORDER BY fkk.name, fkkc.nr", tid);
	} else {
		query = "SELECT ps.name, "		/* 0 */
			       "pkt.name, "			/* 1 */
			       "pkkc.name, "		/* 2 */
			       "fkkc.name, "		/* 3 */
			       "fkkc.nr, "			/* 4 */
			       "fkk.name, "			/* 5 */
			       "fkk.\"action\", "	/* 6 */
			       "fs.name, "			/* 7 */
			       "fkt.name "			/* 8 */
			"FROM sys._tables fkt, "
			     "sys.objects fkkc, "
			     "sys.keys fkk, "
			     "sys._tables pkt, "
			     "sys.objects pkkc, "
			     "sys.keys pkk, "
			     "sys.schemas ps, "
			     "sys.schemas fs "
			"WHERE fkt.id = fkk.table_id "
			  "AND pkt.id = pkk.table_id "
			  "AND fkk.id = fkkc.id "
			  "AND pkk.id = pkkc.id "
			  "AND fkk.rkey = pkk.id "
			  "AND fkkc.nr = pkkc.nr "
			  "AND pkt.schema_id = ps.id "
			  "AND fkt.schema_id = fs.id "
			  "AND fkt.system = FALSE "
			"ORDER BY fs.name, fkt.name, "
			         "fkk.name, fkkc.nr";
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
		int nkeys = 1;

		if (mapi_error(mid) || c_psname == NULL || c_ptname == NULL ||
			c_pcolumn == NULL || c_fcolumn == NULL || c_nr == NULL ||
			c_fkname == NULL || c_faction == NULL || c_fsname == NULL ||
			c_ftname == NULL) {
			/* none of the columns should be NULL */
			goto bailout;
		}
		assert(strcmp(c_nr, "0") == 0);
		(void) c_nr;	/* pacify compilers in case assertions are disabled */
		fkeys = malloc(nkeys * sizeof(*fkeys));
		pkeys = malloc(nkeys * sizeof(*pkeys));
		if (fkeys == NULL || pkeys == NULL) {
			free(fkeys);
			free(pkeys);
			goto bailout;
		}
		pkeys[0] = strdup(c_pcolumn);
		fkeys[0] = strdup(c_fcolumn);
		c_psname = strdup(c_psname);
		c_ptname = strdup(c_ptname);
		c_pcolumn = strdup(c_pcolumn);
		c_fcolumn = strdup(c_fcolumn);
		c_fkname = strdup(c_fkname);
		c_faction = strdup(c_faction);
		c_fsname = strdup(c_fsname);
		c_ftname = strdup(c_ftname);
		if (c_psname == NULL || c_ptname == NULL || c_pcolumn == NULL ||
			c_fcolumn == NULL || c_nr == NULL || c_fkname == NULL ||
			c_faction == NULL || c_fsname == NULL || c_ftname == NULL ||
			fkeys[0] == NULL || pkeys[0] == NULL) {
		  freeall_bailout:
			/* free all temporarily allocated data, then bailout */
			while (nkeys-- > 0) {
				if (pkeys)
					free(pkeys[nkeys]);
				if (fkeys)
					free(fkeys[nkeys]);
			}
			free(fkeys);
			free(pkeys);
			free(c_psname);
			free(c_ptname);
			free(c_pcolumn);
			free(c_fcolumn);
			free(c_fkname);
			free(c_faction);
			free(c_fsname);
			free(c_ftname);
			goto bailout;
		}
		while ((cnt = mapi_fetch_row(hdl)) != 0 && strcmp(mapi_fetch_field(hdl, 4), "0") != 0) {
			char *pkey = mapi_fetch_field(hdl, 2);
			char *fkey = mapi_fetch_field(hdl, 3);
			char **tkeys;

			if (pkey == NULL || fkey == NULL) {
				/* we're not expecting NULL values */
				goto freeall_bailout;
			}
			tkeys = realloc(pkeys, (nkeys + 1) * sizeof(*pkeys));
			if (tkeys == NULL)
				goto freeall_bailout;
			pkeys = tkeys;
			tkeys = realloc(fkeys, (nkeys + 1) * sizeof(*fkeys));
			if (tkeys == NULL)
				goto freeall_bailout;
			fkeys = tkeys;
			nkeys++;
			pkeys[nkeys - 1] = strdup(pkey);
			fkeys[nkeys - 1] = strdup(fkey);
			if (pkeys[nkeys - 1] == NULL || fkeys[nkeys - 1] == NULL) {
				goto freeall_bailout;
			}
		}
		if (tname == NULL && tid == NULL) {
			mnstr_printf(sqlf, "ALTER TABLE ");
			dquoted_print(sqlf, c_fsname, ".");
			dquoted_print(sqlf, c_ftname, " ADD ");
		} else {
			mnstr_printf(sqlf, ",\n\t");
		}
		if (c_fkname) {
			mnstr_printf(sqlf, "CONSTRAINT ");
			dquoted_print(sqlf, c_fkname, " ");
		}
		mnstr_printf(sqlf, "FOREIGN KEY (");
		for (i = 0; i < nkeys; i++) {
			if (i > 0)
				mnstr_printf(sqlf, ", ");
			dquoted_print(sqlf, fkeys[i], NULL);
		}
		mnstr_printf(sqlf, ") REFERENCES ");
		dquoted_print(sqlf, c_psname, ".");
		dquoted_print(sqlf, c_ptname, " (");
		for (i = 0; i < nkeys; i++) {
			if (i > 0)
				mnstr_printf(sqlf, ", ");
			dquoted_print(sqlf, pkeys[i], NULL);
		}
		mnstr_printf(sqlf, ")");
		if (c_faction) {
			int action = atoi(c_faction);
			int on_update;
			int on_delete;

			if ((on_delete = action & 255) != 0 &&
			    on_delete < NR_ACTIONS &&
			    on_delete != 2	   /* RESTRICT -- default */)
				mnstr_printf(sqlf, " ON DELETE %s",
					     actions[on_delete]);
			if ((on_update = (action >> 8) & 255) != 0 &&
			    on_update < NR_ACTIONS &&
			    on_update != 2	   /* RESTRICT -- default */)
				mnstr_printf(sqlf, " ON UPDATE %s",
					     actions[on_update]);
		}
		free(c_psname);
		free(c_ptname);
		free(c_pcolumn);
		free(c_fcolumn);
		free(c_fkname);
		free(c_faction);
		free(c_fsname);
		free(c_ftname);
		while (nkeys-- > 0) {
			free(pkeys[nkeys]);
			free(fkeys[nkeys]);
		}
		free(fkeys);
		free(pkeys);

		if (tname == NULL && tid == NULL)
			mnstr_printf(sqlf, ";\n");

		if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR)
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
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");

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
	stream *sqlf,
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
dump_type(Mapi mid, stream *sqlf, const char *c_type, const char *c_type_digits, const char *c_type_scale, bool hashge)
{
	int space = 0;

	if (strcmp(c_type, "boolean") == 0) {
		space = mnstr_printf(sqlf, "BOOLEAN");
	} else if (strcmp(c_type, "int") == 0) {
		space = mnstr_printf(sqlf, "INTEGER");
	} else if (strcmp(c_type, "smallint") == 0) {
		space = mnstr_printf(sqlf, "SMALLINT");
	} else if (strcmp(c_type, "tinyint") == 0) {
		space = mnstr_printf(sqlf, "TINYINT");
	} else if (strcmp(c_type, "bigint") == 0) {
		space = mnstr_printf(sqlf, "BIGINT");
	} else if (strcmp(c_type, "hugeint") == 0) {
		space = mnstr_printf(sqlf, "HUGEINT");
	} else if (strcmp(c_type, "date") == 0) {
		space = mnstr_printf(sqlf, "DATE");
	} else if (strcmp(c_type, "month_interval") == 0) {
		if (strcmp(c_type_digits, "1") == 0)
			space = mnstr_printf(sqlf, "INTERVAL YEAR");
		else if (strcmp(c_type_digits, "2") == 0)
			space = mnstr_printf(sqlf, "INTERVAL YEAR TO MONTH");
		else if (strcmp(c_type_digits, "3") == 0)
			space = mnstr_printf(sqlf, "INTERVAL MONTH");
		else
			fprintf(stderr, "Internal error: unrecognized month interval %s\n", c_type_digits);
	} else if (strcmp(c_type, "day_interval") == 0 || strcmp(c_type, "sec_interval") == 0) {
		if (strcmp(c_type_digits, "4") == 0)
			space = mnstr_printf(sqlf, "INTERVAL DAY");
		else if (strcmp(c_type_digits, "5") == 0)
			space = mnstr_printf(sqlf, "INTERVAL DAY TO HOUR");
		else if (strcmp(c_type_digits, "6") == 0)
			space = mnstr_printf(sqlf, "INTERVAL DAY TO MINUTE");
		else if (strcmp(c_type_digits, "7") == 0)
			space = mnstr_printf(sqlf, "INTERVAL DAY TO SECOND");
		else if (strcmp(c_type_digits, "8") == 0)
			space = mnstr_printf(sqlf, "INTERVAL HOUR");
		else if (strcmp(c_type_digits, "9") == 0)
			space = mnstr_printf(sqlf, "INTERVAL HOUR TO MINUTE");
		else if (strcmp(c_type_digits, "10") == 0)
			space = mnstr_printf(sqlf, "INTERVAL HOUR TO SECOND");
		else if (strcmp(c_type_digits, "11") == 0)
			space = mnstr_printf(sqlf, "INTERVAL MINUTE");
		else if (strcmp(c_type_digits, "12") == 0)
			space = mnstr_printf(sqlf, "INTERVAL MINUTE TO SECOND");
		else if (strcmp(c_type_digits, "13") == 0)
			space = mnstr_printf(sqlf, "INTERVAL SECOND");
		else
			fprintf(stderr, "Internal error: unrecognized second interval %s\n", c_type_digits);
	} else if (strcmp(c_type, "clob") == 0 ||
		   (strcmp(c_type, "varchar") == 0 &&
		    strcmp(c_type_digits, "0") == 0)) {
		space = mnstr_printf(sqlf, "CHARACTER LARGE OBJECT");
		if (strcmp(c_type_digits, "0") != 0)
			space += mnstr_printf(sqlf, "(%s)", c_type_digits);
	} else if (strcmp(c_type, "blob") == 0) {
		space = mnstr_printf(sqlf, "BINARY LARGE OBJECT");
		if (strcmp(c_type_digits, "0") != 0)
			space += mnstr_printf(sqlf, "(%s)", c_type_digits);
	} else if (strcmp(c_type, "timestamp") == 0 ||
		   strcmp(c_type, "timestamptz") == 0) {
		space = mnstr_printf(sqlf, "TIMESTAMP");
		if (strcmp(c_type_digits, "7") != 0)
			space += mnstr_printf(sqlf, "(%d)", atoi(c_type_digits) - 1);
		if (strcmp(c_type, "timestamptz") == 0)
			space += mnstr_printf(sqlf, " WITH TIME ZONE");
	} else if (strcmp(c_type, "time") == 0 ||
		   strcmp(c_type, "timetz") == 0) {
		space = mnstr_printf(sqlf, "TIME");
		if (strcmp(c_type_digits, "1") != 0)
			space += mnstr_printf(sqlf, "(%d)", atoi(c_type_digits) - 1);
		if (strcmp(c_type, "timetz") == 0)
			space += mnstr_printf(sqlf, " WITH TIME ZONE");
	} else if (strcmp(c_type, "real") == 0) {
		if (strcmp(c_type_digits, "24") == 0 &&
		    strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(sqlf, "REAL");
		else if (strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(sqlf, "FLOAT(%s)", c_type_digits);
		else
			space = mnstr_printf(sqlf, "FLOAT(%s,%s)",
					c_type_digits, c_type_scale);
	} else if (strcmp(c_type, "double") == 0) {
		if (strcmp(c_type_digits, "53") == 0 &&
		    strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(sqlf, "DOUBLE");
		else if (strcmp(c_type_scale, "0") == 0)
			space = mnstr_printf(sqlf, "FLOAT(%s)", c_type_digits);
		else
			space = mnstr_printf(sqlf, "FLOAT(%s,%s)",
					c_type_digits, c_type_scale);
	} else if (strcmp(c_type, "decimal") == 0 &&
		   strcmp(c_type_digits, "1") == 0 &&
		   strcmp(c_type_scale, "0") == 0) {
		space = mnstr_printf(sqlf, "DECIMAL");
	} else if (strcmp(c_type, "table") == 0) {
		mnstr_printf(sqlf, "TABLE ");
		dump_column_definition(mid, sqlf, NULL, NULL, c_type_digits, 1, hashge);
	} else if (strcmp(c_type, "geometry") == 0 &&
		   strcmp(c_type_digits, "0") != 0) {
		const char *geom = NULL;
		int sub = atoi(c_type_digits);

		if (sub > 0 && (sub & 3) == 0 &&
		    (sub >> 2) < (int) (sizeof(geomsubtypes) / sizeof(geomsubtypes[0])))
			geom = geomsubtypes[sub >> 2];
		if (geom) {
			mnstr_printf(sqlf, "GEOMETRY(%s", geom);
			if (strcmp(c_type_scale, "0") != 0)
				mnstr_printf(sqlf, ",%s", c_type_scale);
			mnstr_printf(sqlf, ")");
		} else {
			mnstr_printf(sqlf, "GEOMETRY");
		}
	} else if (strcmp(c_type_digits, "0") == 0) {
		space = mnstr_printf(sqlf, "%s", toUpper(c_type));
	} else if (strcmp(c_type_scale, "0") == 0) {
		space = mnstr_printf(sqlf, "%s(%s)",
				toUpper(c_type), c_type_digits);
	} else {
		if (strcmp(c_type, "decimal") == 0) {
			if (strcmp(c_type_digits, "39") == 0)
				c_type_digits = "38";
			else if (!hashge && strcmp(c_type_digits, "19") == 0)
				c_type_digits = "18";
		}
		space = mnstr_printf(sqlf, "%s(%s,%s)",
				toUpper(c_type), c_type_digits, c_type_scale);
	}
	return space;
}

static int
dump_column_definition(Mapi mid, stream *sqlf, const char *schema,
					   const char *tname, const char *tid, bool foreign,
					   bool hashge)
{
	MapiHdl hdl = NULL;
	char *query = NULL;
	char *s, *t;
	size_t maxquerylen = 1024;
	int cnt;
	int slen;
	int cap;
#define CAP(X) ((cap = (int) (X)) < 0 ? 0 : cap)

	t = tname ? sescape(tname) : NULL;
	s = schema ? sescape(schema) : NULL;
	if (tid == NULL) {
		if (tname == NULL || schema == NULL) {
			if (t != NULL)
				free(t);
			if (s != NULL)
				free(s);
			return 1;
		}
		maxquerylen += 2 * strlen(tname) + 2 * strlen(schema);
	}
	else
		maxquerylen += strlen(tid);
	if ((query = malloc(maxquerylen)) == NULL)
		goto bailout;

	mnstr_printf(sqlf, "(\n");

	if (tid)
		snprintf(query, maxquerylen,
			 "SELECT c.name, "		/* 0 */
				"c.type, "			/* 1 */
				"c.type_digits, "	/* 2 */
				"c.type_scale, "	/* 3 */
				"c.\"null\", "		/* 4 */
				"c.number "			/* 5 */
			 "FROM sys._columns c "
			 "WHERE c.table_id = %s "
			 "ORDER BY c.number", tid);
	else
		snprintf(query, maxquerylen,
			 "SELECT c.name, "		/* 0 */
				"c.type, "			/* 1 */
				"c.type_digits, "	/* 2 */
				"c.type_scale, "	/* 3 */
				"c.\"null\", "		/* 4 */
				"c.number "			/* 5 */
			 "FROM sys._columns c, "
			      "sys._tables t, "
			      "sys.schemas s "
			 "WHERE c.table_id = t.id "
			   "AND t.name = '%s' "
			   "AND t.schema_id = s.id "
			   "AND s.name = '%s' "
			 "ORDER BY c.number", t, s);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	slen = mapi_get_len(hdl, 0) + 3; /* add quotes and space */
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *c_name = mapi_fetch_field(hdl, 0);
		char *c_type = strdup(mapi_fetch_field(hdl, 1)); /* copy variables used outside this scope (look for possible mapi cache incoherency) */
		char *c_type_digits = strdup(mapi_fetch_field(hdl, 2));
		char *c_type_scale = strdup(mapi_fetch_field(hdl, 3));
		const char *c_null = mapi_fetch_field(hdl, 4);
		int space;

		if (mapi_error(mid) || !c_type || !c_type_digits || !c_type_scale) {
			free(c_type);
			free(c_type_digits);
			free(c_type_scale);
			goto bailout;
		}

		if (cnt)
			mnstr_printf(sqlf, ",\n");

		mnstr_printf(sqlf, "\t");
		space = dquoted_print(sqlf, c_name, " ");
		mnstr_printf(sqlf, "%*s", CAP(slen - space), "");
		if (s != NULL && t != NULL &&
			strcmp(c_type, "char") == 0 && strcmp(c_type_digits, "0") == 0) {
			/* if the number of characters is not specified (due to a bug),
			 * calculate a size */
			char *c = descape(c_name);
			if (c != NULL) {
				size_t qlen = strlen(c) + strlen(s) + strlen(t) + 64;
				char *q = malloc(qlen);
				if (q != NULL) {
					snprintf(q, qlen, "SELECT max(length(\"%s\")) FROM \"%s\".\"%s\"", c, s, t);
					MapiHdl h = mapi_query(mid, q);
					if (h != NULL) {
						if (mapi_fetch_row(h) != 0) {
							const char *d = mapi_fetch_field(h, 0);
							free(c_type_digits);
							/* if NULL, i.e. no non-NULL values, fill in 1 */
							c_type_digits = strdup(d ? d : "1");
							fprintf(stderr, "Warning: fixing size of CHAR column for %s of table %s.%s\n", c_name, schema, tname);
						}
						mapi_close_handle(h);
					}
					free(q);
				}
				free(c);
			}
			if (c_type_digits == NULL)
				goto bailout;
		}
		space = dump_type(mid, sqlf, c_type, c_type_digits, c_type_scale, hashge);
		if (strcmp(c_null, "false") == 0) {
			mnstr_printf(sqlf, "%*s NOT NULL",
						 CAP(13 - space), "");
			space = 13;
		}

		cnt++;
		free(c_type);
		free(c_type_digits);
		free(c_type_scale);
		if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR)
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
				"kc.nr, "			/* 1 */
				"k.name, "			/* 2 */
				"kc.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k "
			 "WHERE kc.id = k.id "
			   "AND k.table_id = %s "
			   "AND k.type = 0 "
			 "ORDER BY kc.id, kc.nr", tid);
	else
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "			/* 1 */
				"k.name, "			/* 2 */
				"kc.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k, "
			      "sys.schemas s, "
			      "sys._tables t "
			 "WHERE kc.id = k.id "
			   "AND k.table_id = t.id "
			   "AND k.type = 0 "
			   "AND t.schema_id = s.id "
			   "AND s.name = '%s' "
			   "AND t.name = '%s' "
			 "ORDER BY kc.id, kc.nr", s, t);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *c_column = mapi_fetch_field(hdl, 0);
		const char *k_name = mapi_fetch_field(hdl, 2);

		if (mapi_error(mid))
			goto bailout;
		if (cnt == 0) {
			mnstr_printf(sqlf, ",\n\t");
			if (k_name) {
				mnstr_printf(sqlf, "CONSTRAINT ");
				dquoted_print(sqlf, k_name, " ");
			}
			mnstr_printf(sqlf, "PRIMARY KEY (");
		} else
			mnstr_printf(sqlf, ", ");
		dquoted_print(sqlf, c_column, NULL);
		cnt++;
		if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR)
			goto bailout;
	}
	if (cnt)
		mnstr_printf(sqlf, ")");
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	if (tid)
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "			/* 1 */
				"k.name, "			/* 2 */
				"kc.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k "
			 "WHERE kc.id = k.id "
			   "AND k.table_id = %s "
			   "AND k.type = 1 "
			 "ORDER BY kc.id, kc.nr", tid);
	else
		snprintf(query, maxquerylen,
			 "SELECT kc.name, "		/* 0 */
				"kc.nr, "			/* 1 */
				"k.name, "			/* 2 */
				"kc.id "			/* 3 */
			 "FROM sys.objects kc, "
			      "sys.keys k, "
			      "sys.schemas s, "
			      "sys._tables t "
			 "WHERE kc.id = k.id "
			   "AND k.table_id = t.id "
			   "AND k.type = 1 "
			   "AND t.schema_id = s.id "
			   "AND s.name = '%s' "
			   "AND t.name = '%s' "
			 "ORDER BY kc.id, kc.nr", s, t);
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
				mnstr_write(sqlf, ")", 1, 1);
			mnstr_printf(sqlf, ",\n\t");
			if (k_name) {
				mnstr_printf(sqlf, "CONSTRAINT ");
				dquoted_print(sqlf, k_name, " ");
			}
			mnstr_printf(sqlf, "UNIQUE (");
			cnt = 1;
		} else
			mnstr_printf(sqlf, ", ");
		dquoted_print(sqlf, c_column, NULL);
		if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR)
			goto bailout;
	}
	if (cnt)
		mnstr_write(sqlf, ")", 1, 1);
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	if (foreign &&
	    dump_foreign_keys(mid, schema, tname, tid, sqlf))
		goto bailout;

	mnstr_printf(sqlf, "\n");

	mnstr_printf(sqlf, ")");

	if (t != NULL)
		free(t);
	if (s != NULL)
		free(s);
	free(query);
	return 0;

bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");
	if (query != NULL)
		free(query);
	if (t != NULL)
		free(t);
	if (s != NULL)
		free(s);
	return 1;
}

static int
describe_table(Mapi mid, const char *schema, const char *tname,
	       stream *sqlf, bool foreign, bool databaseDump)
{
	int cnt, table_id = 0;
	MapiHdl hdl = NULL;
	char *query = NULL, *view = NULL, *remark = NULL, *s = NULL, *t = NULL;
	int type = 0;
	int ca = 0;
	size_t maxquerylen;
	bool hashge;

	hashge = has_hugeint(mid);

	s = sescape(schema);
	t = sescape(tname);
	maxquerylen = 5120 + strlen(t) + strlen(s);
	query = malloc(maxquerylen);
	if (query == NULL)
		goto bailout;

	snprintf(query, maxquerylen,
		 "SELECT t.name, t.query, t.type, t.id, c.remark, t.commit_action "
		 "FROM sys.schemas s, sys._tables t "
			"LEFT OUTER JOIN sys.comments c ON t.id = c.id "
		 "WHERE s.name = '%s' "
		   "AND t.schema_id = s.id "
		   "AND t.name = '%s'",
		 s, t);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	cnt = 0;
	while ((mapi_fetch_row(hdl)) != 0) {
		cnt++;
		view = mapi_fetch_field(hdl, 2);
		if (view)
			type = atoi(view);
		view = mapi_fetch_field(hdl, 1);
		table_id = atoi(mapi_fetch_field(hdl, 3));
		remark = mapi_fetch_field(hdl, 4);
		ca = atoi(mapi_fetch_field(hdl, 5));
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
		if (!(view = strdup(view)))
			goto bailout;
	}
	if (remark) {
		if (!(remark = strdup(remark)))
			goto bailout;
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	if (cnt != 1) {
		if (cnt == 0)
			fprintf(stderr, "table %s.%s does not exist\n", schema, tname);
		else
			fprintf(stderr, "table %s.%s is not unique, corrupt catalog?\n",
					schema, tname);
		goto bailout2;
	}

	if (type == 1) {
		/* the table is actually a view */
		mnstr_printf(sqlf, "%s\n", view);
		comment_on(sqlf, "VIEW", schema, tname, NULL, remark);
	} else {
		if (!databaseDump) { //if it is not a database dump the table might depend on UDFs that must be dumped first
			assert(table_id);
			snprintf(query, maxquerylen,
					 "SELECT f.id, s.name, f.name "
					 "FROM sys.schemas s, "
					      "sys.functions f "
					 "WHERE s.id = f.schema_id "
					   "AND f.id IN (SELECT id FROM sys.dependencies WHERE depend_id = '%d')",
					 table_id);
			if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
				goto bailout;
			while (mapi_fetch_row(hdl) != 0) {
				bool failure = false;
				char *function_id = strdup(mapi_fetch_field(hdl, 0));
				char *schema_name = strdup(mapi_fetch_field(hdl, 1));
				char *function_name = strdup(mapi_fetch_field(hdl, 2));

				if (function_id && schema_name && function_name)
					dump_functions(mid, sqlf, 0, schema_name, function_name, function_id);
				else
					failure = true;

				free(function_id);
				free(schema_name);
				free(function_name);

				if (failure)
					goto bailout;
			}
			mapi_close_handle(hdl);
			hdl = NULL;
		}
		/* the table is a real table */
		mnstr_printf(sqlf, "CREATE %sTABLE ",
					 ca > 0 ? "GLOBAL TEMPORARY " :
					 type == 3 ? "MERGE " :
					 type == 4 ? "STREAM " :
					 type == 5 ? "REMOTE " :
					 type == 6 ? "REPLICA " :
					 type == 7 ? "UNLOGGED " :
					 "");
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, tname, " ");

		if (dump_column_definition(mid, sqlf, schema, tname, NULL, foreign, hashge))
			goto bailout;
		if (ca > 0) {			/* temporary table */
			mnstr_printf(sqlf, " ON COMMIT %s",
						 ca == 1 /* the default */ ? "DELETE ROWS" :
						 ca == 2 ? "PRESERVE ROWS" :
						 /* ca == 3 */ "DROP");
		} else if (type == 5) { /* remote table */
			char *rt_user = NULL;
			char *rt_hash = NULL;
			if (has_remote_user_info_table(mid)) {
				snprintf(query, maxquerylen,
					"SELECT username, sys.decypher(password) "
					"FROM sys.remote_user_info where table_id = (select t.id from sys._tables t, sys.schemas s where "
					"t.schema_id = s.id and s.name = '%s' and t.name = '%s')", schema, tname);
			} else {
				snprintf(query, maxquerylen,
					"SELECT username, hash "
					"FROM sys.remote_table_credentials('%s.%s')",
					schema, tname);
			}
			if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
				goto bailout;
			cnt = 0;
			while (mapi_fetch_row(hdl) != 0) {
				rt_user = mapi_fetch_field(hdl, 0);
				rt_hash = mapi_fetch_field(hdl, 1);
			}
			mnstr_printf(sqlf, " ON ");
			squoted_print(sqlf, view, '\'', false);
			mnstr_printf(sqlf, " WITH USER ");
			squoted_print(sqlf, rt_user, '\'', false);
			mnstr_printf(sqlf, " ENCRYPTED PASSWORD ");
			squoted_print(sqlf, rt_hash, '\'', false);
			mapi_close_handle(hdl);
			hdl = NULL;
		} else if (type == 3 && has_table_partitions(mid)) { /* A merge table might be partitioned */
			int properties = 0;

			snprintf(query, maxquerylen, "SELECT tp.type FROM sys.table_partitions tp WHERE tp.table_id = '%d'", table_id);
			if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
				goto bailout;
			while (mapi_fetch_row(hdl) != 0)
				properties = atoi(mapi_fetch_field(hdl, 0));
			mapi_close_handle(hdl);

			if (properties) {
				bool list = (properties & 2) == 2, column = (properties & 4) == 4;
				const char *phow = list ? "VALUES" : "RANGE";
				const char *pusing = column ? "ON" : "USING";
				const char *expr = NULL;

				if (column) { /* by column */
					snprintf(query, maxquerylen,
							 "SELECT c.name FROM sys.schemas s, sys._tables t, sys._columns c, sys.table_partitions tp "
							 "WHERE s.name = '%s' AND t.name = '%s' AND s.id = t.schema_id AND t.id = c.table_id "
							 "AND c.id = tp.column_id", s, t);
				} else { /* by expression */
					snprintf(query, maxquerylen,
							 "SELECT tp.expression FROM sys.schemas s, sys._tables t, sys.table_partitions tp "
							 "WHERE s.name = '%s' AND t.name = '%s' AND s.id = t.schema_id AND t.id = tp.table_id",
							 s, t);
				}
				if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
					goto bailout;
				while (mapi_fetch_row(hdl) != 0)
					expr = mapi_fetch_field(hdl, 0);
				mnstr_printf(sqlf, " PARTITION BY %s %s (", phow, pusing);
				if (column)
					dquoted_print(sqlf, expr, ")");
				else
					mnstr_printf(sqlf, "%s)", expr);
				mapi_close_handle(hdl);
			}
		}
		mnstr_printf(sqlf, ";\n");
		comment_on(sqlf, "TABLE", schema, tname, NULL, remark);

		snprintf(query, maxquerylen,
			 "SELECT i.name, " /* 0 */
				"k.name, " /* 1 */
				"kc.nr, "  /* 2 */
				"c.name, " /* 3 */
				"it.idx "  /* 4 */
			   "FROM sys.idxs AS i "
				  "LEFT JOIN sys.keys AS k ON i.name = k.name, "
				"sys.objects AS kc, "
				"sys._columns AS c, "
				"sys.schemas s, "
				"sys._tables AS t, "
				"(VALUES (0, 'INDEX'), "
					"(4, 'IMPRINTS INDEX'), "
					"(5, 'ORDERED INDEX')) AS it (id, idx) "
			  "WHERE i.table_id = t.id "
			    "AND i.id = kc.id "
			    "AND t.id = c.table_id "
			    "AND kc.name = c.name "
			    "AND (k.type IS NULL OR k.type = 1) "
			    "AND t.schema_id = s.id "
			    "AND s.name = '%s' "
			    "AND t.name = '%s' "
			    "AND i.type in (0, 4, 5) "
			    "AND i.type = it.id "
			  "ORDER BY i.name, kc.nr", s, t);
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
					mnstr_printf(sqlf, ");\n");
				mnstr_printf(sqlf, "CREATE %s ", i_type);
				dquoted_print(sqlf, i_name, " ON ");
				dquoted_print(sqlf, schema, ".");
				dquoted_print(sqlf, tname, " (");
				cnt = 1;
			} else
				mnstr_printf(sqlf, ", ");
			dquoted_print(sqlf, c_name, NULL);
			if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR)
				goto bailout;
		}
		mapi_close_handle(hdl);
		hdl = NULL;
		if (cnt)
			mnstr_printf(sqlf, ");\n");
		snprintf(query, maxquerylen,
			 "SELECT i.name, c.remark "
			 "FROM sys.idxs i, sys.comments c "
			 "WHERE i.id = c.id "
			   "AND i.table_id = (SELECT id FROM sys._tables WHERE schema_id = (select id FROM sys.schemas WHERE name = '%s') AND name = '%s') "
			 "ORDER BY i.name",
			 s, t);
		if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
			goto bailout;
		while (mapi_fetch_row(hdl) != 0) {
			comment_on(sqlf, "INDEX", schema,
				   mapi_fetch_field(hdl, 0), NULL,
				   mapi_fetch_field(hdl, 1));
		}
		mapi_close_handle(hdl);
		hdl = NULL;
	}

	snprintf(query, maxquerylen,
		 "SELECT col.name, com.remark "
		 "FROM sys._columns col, sys.comments com "
		 "WHERE col.id = com.id "
		   "AND col.table_id = (SELECT id FROM sys._tables WHERE schema_id = (SELECT id FROM sys.schemas WHERE name = '%s') AND name = '%s') "
		 "ORDER BY col.number",
		 s, t);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	while (mapi_fetch_row(hdl) != 0) {
		comment_on(sqlf, "COLUMN", schema, tname,
				mapi_fetch_field(hdl, 0),
				mapi_fetch_field(hdl, 1));
	}
	mapi_close_handle(hdl);
	hdl = NULL;
	if (mapi_error(mid))
		goto bailout;

	free(s);
	free(t);
	if (view)
		free(view);
	if (remark)
		free(remark);
	if (query != NULL)
		free(query);
	return 0;

bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");
bailout2:
	if (view)
		free(view);
	if (remark)
		free(remark);
	if (query != NULL)
		free(query);
	if (s != NULL)
		free(s);
	if (t != NULL)
		free(t);
	return 1;
}

int
describe_sequence(Mapi mid, const char *schema, const char *tname, stream *sqlf)
{
	MapiHdl hdl = NULL;
	char *query = NULL;
	size_t maxquerylen;
	char *sname = NULL;

	if (schema == NULL) {
		if ((sname = strchr(tname, '.')) != NULL) {
			size_t len = sname - tname + 1;

			sname = malloc(len);
			if (sname == NULL)
				goto bailout;
			strcpy_len(sname, tname, len);
			tname += len;
		} else if ((sname = get_schema(mid)) == NULL) {
			return 1;
		}
		schema = sname;
	}

	maxquerylen = 5120 + strlen(tname) + strlen(schema);

	query = malloc(maxquerylen);
	if (query == NULL)
		goto bailout;

	snprintf(query, maxquerylen,
			 "SELECT c.remark, q.* "
			   "FROM sys.sequences seq LEFT OUTER JOIN sys.comments c ON seq.id = c.id, "
			        "sys.schemas s, "
			        "sys.describe_sequences q "
			  "WHERE s.id = seq.schema_id "
			    "AND s.name = '%s' "   /* schema name */
			    "AND seq.name = '%s' " /* sequence name */
			    "AND q.sch = '%s' "	   /* schema name */
			    "AND q.seq = '%s' "	   /* sequence name */
			  "ORDER BY q.sch, q.seq",
		schema, tname,
		schema, tname);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *remark = mapi_fetch_field(hdl, 0);
		const char *schema = mapi_fetch_field(hdl, 1);		/* sch */
		const char *name = mapi_fetch_field(hdl, 2);		/* seq */
		const char *restart = mapi_fetch_field(hdl, 4);		/* rs */
		const char *minvalue;
		const char *maxvalue;
		const char *increment = mapi_fetch_field(hdl, 7);	/* inc */
		const char *cacheinc = mapi_fetch_field(hdl, 8);	/* cache */
		const char *cycle = mapi_fetch_field(hdl, 9);		/* cycle */

		if (mapi_get_field_count(hdl) > 10) {
			/* new version (Jan2022) of sys.describe_sequences */
			minvalue = mapi_fetch_field(hdl, 12);			/* rmi */
			maxvalue = mapi_fetch_field(hdl, 13);			/* rma */
		} else {
			/* old version (pre Jan2022) of sys.describe_sequences */
			minvalue = mapi_fetch_field(hdl, 5);			/* minvalue */
			maxvalue = mapi_fetch_field(hdl, 6);			/* maxvalue */
			if (strcmp(minvalue, "0") == 0)
				minvalue = NULL;
			if (strcmp(maxvalue, "0") == 0)
				maxvalue = NULL;
		}
		mnstr_printf(sqlf, "CREATE SEQUENCE ");
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, name, NULL);
		mnstr_printf(sqlf, " START WITH %s", restart);
		if (strcmp(increment, "1") != 0)
			mnstr_printf(sqlf, " INCREMENT BY %s", increment);
		if (minvalue)
			mnstr_printf(sqlf, " MINVALUE %s", minvalue);
		if (maxvalue)
			mnstr_printf(sqlf, " MAXVALUE %s", maxvalue);
		if (strcmp(cacheinc, "1") != 0)
			mnstr_printf(sqlf, " CACHE %s", cacheinc);
		mnstr_printf(sqlf, " %sCYCLE;\n", strcmp(cycle, "true") == 0 ? "" : "NO ");
		comment_on(sqlf, "SEQUENCE", schema, name, NULL, remark);
		if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR) {
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
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");
	if (sname != NULL)
		free(sname);
	if (query != NULL)
		free(query);
	return 1;
}

int
describe_schema(Mapi mid, const char *sname, stream *sqlf)
{
	MapiHdl hdl = NULL;
	char schemas[5120];

	snprintf(schemas, sizeof(schemas),
		"SELECT s.name, a.name, c.remark "
		"FROM sys.auths a, "
		     "sys.schemas s LEFT OUTER JOIN sys.comments c ON s.id = c.id "
		"WHERE s.\"authorization\" = a.id "
		  "AND s.name = '%s' "
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
		const char *sname = mapi_fetch_field(hdl, 0);
		const char *aname = mapi_fetch_field(hdl, 1);
		const char *remark = mapi_fetch_field(hdl, 2);

		mnstr_printf(sqlf, "CREATE SCHEMA ");
		dquoted_print(sqlf, sname, NULL);
		if (strcmp(aname, "sysadmin") != 0) {
			mnstr_printf(sqlf, " AUTHORIZATION ");
			dquoted_print(sqlf, aname, NULL);
		}
		mnstr_printf(sqlf, ";\n");
		comment_on(sqlf, "SCHEMA", sname, NULL, NULL, remark);
	}

	mapi_close_handle(hdl);
	return 0;
}

static int
dump_table_data(Mapi mid, const char *schema, const char *tname,
				stream *sqlf, const char *ddir, const char *ext,
				bool useInserts, bool noescape)
{
	int cnt, i;
	int64_t rows;
	MapiHdl hdl = NULL;
	char *query = NULL;
	size_t maxquerylen;
	unsigned char *string = NULL;
	char *s, *t;
	stream *datf = sqlf;

	maxquerylen = 5120 + 2*strlen(tname) + 2*strlen(schema);
	query = malloc(maxquerylen);
	if (query == NULL)
		goto bailout;

	s = sescape(schema);
	t = sescape(tname);
	snprintf(query, maxquerylen,
		 "SELECT t.name, t.query, t.type "
		 "FROM sys._tables t, sys.schemas s "
		 "WHERE s.name = '%s' "
		   "AND t.schema_id = s.id "
		   "AND t.name = '%s'",
		 s, t);
	free(s);
	free(t);

	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	if (mapi_rows_affected(hdl) != 1) {
		if (mapi_rows_affected(hdl) == 0)
			fprintf(stderr, "table %s.%s does not exist\n", schema, tname);
		else
			fprintf(stderr, "table %s.%s is not unique\n", schema, tname);
		goto bailout;
	}
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *ttype = mapi_fetch_field(hdl, 2);
		if (strcmp(ttype, "1") == 0) {
			/* the table is actually a view */
			goto doreturn;
		}
		if (strcmp(ttype, "3") == 0) {
			/* merge table */
			goto doreturn;
		}
		if (strcmp(ttype, "4") == 0) {
			/* stream table */
			goto doreturn;
		}
		if (strcmp(ttype, "5") == 0) {
			/* remote table */
			goto doreturn;
		}
		if (strcmp(ttype, "6") == 0) {
			/* replica table */
			goto doreturn;
		}
		if (strcmp(ttype, "7") == 0) {
			/* unlogged table */
			goto doreturn;
		}
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	s = descape(schema);
	t = descape(tname);
	snprintf(query, maxquerylen, "SELECT * FROM \"%s\".\"%s\"", s, t);
	free(s);
	free(t);
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
		mnstr_printf(sqlf, "COPY %" PRId64 " RECORDS INTO ", rows);
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, tname, NULL);
		if (ddir) {
			size_t fnl = strlen(ddir) + strlen(schema) + strlen(tname) + (ext ? strlen(ext) + 1 : 0) + 7;
			char *fn = malloc(fnl);
			if (fn == NULL)
				goto bailout;
			int off;
#ifdef MSC_VER
			/* bloody Windows: enable %n format specifier */
			_set_printf_count_output(1);
#endif
			if (ext)
				snprintf(fn, fnl, "%s%c%n%s.%s.csv.%s", ddir, DIR_SEP, &off, schema, tname, ext);
			else
				snprintf(fn, fnl, "%s%c%n%s.%s.csv", ddir, DIR_SEP, &off, schema, tname);
			mnstr_printf(sqlf, " FROM E");
			squoted_print(sqlf, fn + off, '\'', false);
			mnstr_printf(sqlf, " ON CLIENT");
			datf = open_wastream(fn);
			free(fn);
			if (datf == NULL) {
				goto bailout;
			}
		} else {
			mnstr_printf(sqlf, " FROM stdin");
		}
		mnstr_printf(sqlf, " USING DELIMITERS E'\\t',E'\\n','\"'%s;\n",
					 noescape ? " NO ESCAPE" : "");
	}
	string = malloc(sizeof(unsigned char) * cnt);
	if (string == NULL)
		goto bailout;
	for (i = 0; i < cnt; i++) {
		const char *tp = mapi_get_type(hdl, i);
		string[i] = (strcmp(tp, "char") == 0 ||
			     strcmp(tp, "varchar") == 0 ||
			     strcmp(tp, "clob") == 0 ||
			     strcmp(tp, "timestamp") == 0 ||
			     strcmp(tp, "timestamptz") == 0 ||
			     strcmp(tp, "json") == 0 ||
			     strcmp(tp, "url") == 0 ||
			     strcmp(tp, "xml") == 0);
	}
	while (mapi_fetch_row(hdl)) {
		const char *s;

		if (useInserts) {
			mnstr_printf(datf, "INSERT INTO ");
			dquoted_print(datf, schema, ".");
			dquoted_print(datf, tname, " VALUES (");
		}

		for (i = 0; i < cnt; i++) {
			const char *tp = mapi_get_type(hdl, i);
			s = mapi_fetch_field(hdl, i);
			if (s == NULL)
				mnstr_printf(datf, "NULL");
			else if (useInserts) {
				if (strcmp(tp, "day_interval") == 0 || strcmp(tp, "sec_interval") == 0) {
					const char *p = strchr(s, '.');
					if (p == NULL)
						p = s + strlen(s);
					mnstr_printf(datf, "INTERVAL '%.*s' SECOND", (int) (p - s), s);
				} else if (strcmp(tp, "month_interval") == 0)
					mnstr_printf(datf, "INTERVAL '%s' MONTH", s);
				else if (strcmp(tp, "timestamptz") == 0)
					mnstr_printf(datf, "TIMESTAMP WITH TIME ZONE '%s'", s);
				else if (strcmp(tp, "timestamp") == 0)
					mnstr_printf(datf, "TIMESTAMP '%s'", s);
				else if (strcmp(tp, "timetz") == 0)
					mnstr_printf(datf, "TIME WITH TIME ZONE '%s'", s);
				else if (strcmp(tp, "time") == 0)
					mnstr_printf(datf, "TIME '%s'", s);
				else if (strcmp(tp, "date") == 0)
					mnstr_printf(datf, "DATE '%s'", s);
				else if (strcmp(tp, "blob") == 0)
					mnstr_printf(datf, "BINARY LARGE OBJECT '%s'", s);
				else if (strcmp(tp, "inet") == 0 ||
					 strcmp(tp, "json") == 0 ||
					 strcmp(tp, "url") == 0 ||
					 strcmp(tp, "uuid") == 0 ||
					 string[i])
					squoted_print(datf, s, '\'', false);
				else
					mnstr_printf(datf, "%s", s);
			} else if (string[i]) {
				/* write double-quoted string with
				   certain characters escaped */
				squoted_print(datf, s, '"', noescape);
			} else if (strcmp(tp, "blob") == 0) {
				/* inside blobs, special characters
				   don't occur */
				mnstr_printf(datf, "\"%s\"", s);
			} else
				mnstr_printf(datf, "%s", s);

			if (useInserts) {
				if (i < cnt - 1)
					mnstr_printf(datf, ", ");
				else
					mnstr_printf(datf, ");\n");
			} else {
				if (i < cnt - 1)
					mnstr_write(datf, "\t", 1, 1);
				else
					mnstr_write(datf, "\n", 1, 1);
			}
		}
		if (mnstr_errnr(datf) != MNSTR_NO__ERROR)
			goto bailout;
	}
	if (mapi_error(mid))
		goto bailout;
	free(string);

  doreturn:
	if (datf != sqlf)
		close_stream(datf);
	if (hdl)
		mapi_close_handle(hdl);
	if (query != NULL)
		free(query);
	return 0;

bailout:
	if (datf != NULL && datf != sqlf)
		close_stream(datf);
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");
	if (query != NULL)
		free(query);
	if (string != NULL)
		free(string);
	return 1;
}

static int
dump_table_storage(Mapi mid, const char *schema, const char *tname, stream *sqlf)
{
	char *query = NULL;
	size_t maxquerylen;
	MapiHdl hdl = NULL;
	char *s = NULL;
	char *t = NULL;
	int rc = 1;

	maxquerylen = 5120 + 2*strlen(tname) + 2*strlen(schema);
	query = malloc(maxquerylen);
	s = sescape(schema);
	t = sescape(tname);
	if (query == NULL || s == NULL || t == NULL)
		goto bailout;

	snprintf(query, maxquerylen,
			 "SELECT name, storage FROM sys._columns "
			 "WHERE storage IS NOT NULL "
			 "AND table_id = (SELECT id FROM sys._tables WHERE name = '%s' "
			 "AND schema_id = (SELECT id FROM sys.schemas WHERE name = '%s'))",
			 t, s);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *cname = mapi_fetch_field(hdl, 0);
		const char *storage = mapi_fetch_field(hdl, 1);
		char *stg = sescape(storage);
		if (stg == NULL)
			goto bailout;
		mnstr_printf(sqlf, "ALTER TABLE ");
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, tname, " ");
		mnstr_printf(sqlf, "ALTER COLUMN ");
		dquoted_print(sqlf, cname, " ");
		mnstr_printf(sqlf, "SET STORAGE '%s';\n", stg);
		free(stg);
	}
	rc = 0;						/* success */
  bailout:
	free(query);
	free(s);
	free(t);
	mapi_close_handle(hdl);		/* may be NULL */
	return rc;
}

static int
dump_table_access(Mapi mid, const char *schema, const char *tname, stream *sqlf)
{
	char *query = NULL;
	size_t maxquerylen;
	MapiHdl hdl = NULL;
	char *s = NULL;
	char *t = NULL;
	int rc = 1;

	maxquerylen = 5120 + 2*strlen(tname) + 2*strlen(schema);
	query = malloc(maxquerylen);
	s = sescape(schema);
	t = sescape(tname);
	if (query == NULL || s == NULL || t == NULL)
		goto bailout;

	snprintf(query, maxquerylen,
			 "SELECT t.access FROM sys._tables t, sys.schemas s "
			 "WHERE s.name = '%s' AND t.schema_id = s.id AND t.name = '%s'",
			 s, t);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	if (mapi_rows_affected(hdl) != 1) {
		if (mapi_rows_affected(hdl) == 0)
			fprintf(stderr, "table %s.%s does not exist\n", schema, tname);
		else
			fprintf(stderr, "table %s.%s is not unique\n", schema, tname);
		goto bailout;
	}
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *access = mapi_fetch_field(hdl, 0);
		if (access && (*access == '1' || *access == '2')) {
			mnstr_printf(sqlf, "ALTER TABLE ");
			dquoted_print(sqlf, schema, ".");
			dquoted_print(sqlf, tname, " ");
			mnstr_printf(sqlf, "SET %s ONLY;\n", *access == '1' ? "READ" : "INSERT");
		}
	}
	rc = 0;						/* success */
  bailout:
	free(query);
	free(s);
	free(t);
	mapi_close_handle(hdl);		/* may be NULL */
	return rc;
}

static int
dump_table_defaults(Mapi mid, const char *schema, const char *tname, stream *sqlf)
{
	char *query = NULL;
	size_t maxquerylen;
	MapiHdl hdl = NULL;
	char *s = NULL;
	char *t = NULL;
	int rc = 1;

	maxquerylen = 512;
	if (schema != NULL && tname != NULL) {
		maxquerylen += 2*strlen(tname) + 2*strlen(schema);
		s = sescape(schema);
		t = sescape(tname);
		if (s == NULL || t == NULL)
			goto bailout;
	}
	query = malloc(maxquerylen);
	if (query == NULL)
		goto bailout;

	if (schema == NULL && tname == NULL)
		snprintf(query, maxquerylen,
				 "SELECT s.name, t.name, c.name, c.\"default\" "
				 "FROM sys._columns c, sys._tables t, sys.schemas s "
				 "WHERE c.\"default\" IS NOT NULL "
				 "AND c.table_id = t.id "
				 "AND t.schema_id = s.id "
				 "AND NOT t.system");
	else
		snprintf(query, maxquerylen,
				 "SELECT s.name, t.name, c.name, c.\"default\" "
				 "FROM sys._columns c, sys._tables t, sys.schemas s "
				 "WHERE c.\"default\" IS NOT NULL "
				 "AND c.table_id = t.id "
				 "AND t.schema_id = s.id "
				 "AND t.name = '%s' AND s.name = '%s'",
				 t, s);
	if ((hdl = mapi_query(mid, query)) == NULL || mapi_error(mid))
		goto bailout;
	while ((mapi_fetch_row(hdl)) != 0) {
		const char *sch = mapi_fetch_field(hdl, 0);
		const char *tab = mapi_fetch_field(hdl, 1);
		const char *col = mapi_fetch_field(hdl, 2);
		const char *def = mapi_fetch_field(hdl, 3);
		mnstr_printf(sqlf, "ALTER TABLE ");
		dquoted_print(sqlf, sch, ".");
		dquoted_print(sqlf, tab, " ");
		mnstr_printf(sqlf, "ALTER COLUMN ");
		dquoted_print(sqlf, col, " ");
		mnstr_printf(sqlf, "SET DEFAULT %s;\n", def);
	}
	rc = 0;						/* success */
  bailout:
	free(query);
	free(s);
	free(t);
	mapi_close_handle(hdl);		/* may be NULL */
	return rc;
}

int
dump_table(Mapi mid, const char *schema, const char *tname, stream *sqlf,
		   const char *ddir, const char *ext,
		   bool describe, bool foreign, bool useInserts, bool databaseDump,
		   bool noescape, bool percent)
{
	char *sname = NULL;
	int rc = 1;

	if (schema == NULL) {
		if ((sname = strchr(tname, '.')) != NULL) {
			size_t len = sname - tname + 1;

			sname = malloc(len);
			if (sname == NULL) {
				fprintf(stderr, "malloc failure\n");
				return 1;
			}
			strcpy_len(sname, tname, len);
			tname += len;
		} else if ((sname = get_schema(mid)) == NULL) {
			return 1;
		}
		schema = sname;

		if (percent && (strchr(schema, '%') != NULL || strchr(tname, '%') != NULL)) {
			char *s = sescape(schema);
			char *t = sescape(tname);
			if (s == NULL || t == NULL) {
				free(s);
				free(t);
				fprintf(stderr, "malloc failure\n");
				goto doreturn;
			}
			size_t qlen = strlen(s) + strlen(t) + 256;
			char *query = malloc(qlen);
			if (query == NULL) {
				free(s);
				free(t);
				fprintf(stderr, "malloc failure\n");
				goto doreturn;
			}
			snprintf(query, qlen, "SELECT s.name, t.name FROM sys._tables t, sys.schemas s WHERE t.schema_id = s.id AND s.name LIKE '%s' AND t.name LIKE '%s' ORDER BY t.id", s, t);
			free(s);
			free(t);
			MapiHdl hdl = mapi_query(mid, query);
			free(query);
			if (hdl == NULL) {
				if (mapi_error(mid))
					mapi_explain(mid, stderr);
				else
					fprintf(stderr, "malloc failure\n");
				goto doreturn;
			}
			if (mapi_error(mid)) {
				if (mapi_result_error(hdl))
					mapi_explain_result(hdl, stderr);
				else if (mapi_error(mid))
					mapi_explain_query(hdl, stderr);
				else
					fprintf(stderr, "malloc failure\n");
				mapi_close_handle(hdl);
				goto doreturn;
			}
			struct tables {
				char *schema;
				char *table;
			} *tables;
			int64_t rows = mapi_get_row_count(hdl);
			if (rows == 0) {
				mapi_close_handle(hdl);
				fprintf(stderr, "no tables matching %s.%s\n", schema, tname);
				goto doreturn;
			}
			tables = malloc((size_t) rows * sizeof(struct tables));
			if (tables == NULL) {
				mapi_close_handle(hdl);
				fprintf(stderr, "malloc failure\n");
				goto doreturn;
			}
			for (int64_t i = 0; i < rows; i++) {
				mapi_fetch_row(hdl);
				tables[i].schema = strdup(mapi_fetch_field(hdl, 0));
				tables[i].table = strdup(mapi_fetch_field(hdl, 1));
				if (tables[i].schema == NULL || tables[i].table == NULL) {
					do {
						free(tables[i].schema);
						free(tables[i].table);
					} while (i-- > 0);
					free(tables);
					mapi_close_handle(hdl);
					fprintf(stderr, "malloc failure\n");
					goto doreturn;
				}
			}
			mapi_close_handle(hdl);
			for (int64_t i = 0; i < rows; i++) {
				rc = dump_table(mid, tables[i].schema, tables[i].table, sqlf,
								ddir, ext,
								describe, foreign, useInserts, databaseDump,
								noescape, false);
				if (rc != 0)
					break;
			}
			for (int64_t i = 0; i < rows; i++) {
				free(tables[i].schema);
				free(tables[i].table);
			}
			free(tables);
			goto doreturn;
		}
	}

	rc = describe_table(mid, schema, tname, sqlf, foreign, databaseDump);
	if (rc == 0)
		rc = dump_table_storage(mid, schema, tname, sqlf);
	if (rc == 0 && !describe)
		rc = dump_table_data(mid, schema, tname, sqlf, ddir, ext, useInserts, noescape);
	if (rc == 0)
		rc = dump_table_access(mid, schema, tname, sqlf);
	if (rc == 0 && !databaseDump)
		rc = dump_table_defaults(mid, schema, tname, sqlf);
  doreturn:
	free(sname);				/* may be NULL, but that's OK */
	return rc;
}

static int
dump_function(Mapi mid, stream *sqlf, const char *fid, bool hashge)
{
	MapiHdl hdl = NULL;
	size_t query_size = 5120 + strlen(fid);
	int query_len;
	char *query;
	const char *sep;
	char *ffunc = NULL, *flkey = NULL, *remark = NULL;
	char *sname, *fname, *ftkey;
	int flang, ftype;

	query = malloc(query_size);
	if (query == NULL)
		goto bailout;

	query_len = snprintf(query, query_size,
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
			   "JOIN sys.function_types ft ON f.type = ft.function_type_id "
			   "LEFT OUTER JOIN sys.function_languages fl ON f.language = fl.language_id "
			   "LEFT OUTER JOIN sys.comments c ON f.id = c.id "
		      "WHERE f.id = %s",
		      fid);
	assert(query_len < (int) query_size);
	if (query_len < 0 || query_len >= (int) query_size ||
	    (hdl = mapi_query(mid, query)) == NULL || mapi_error(mid)) {
		free(query);
		goto bailout;
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

		if (remark == NULL || sname == NULL || fname == NULL || ftkey == NULL) {
			if (remark)
				free(remark);
			if (sname)
				free(sname);
			if (fname)
				free(fname);
			if (ftkey)
				free(ftkey);
			if (query)
				free(query);
			goto bailout;
		}
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
		mnstr_printf(sqlf, "%s\n", ffunc);
		if (remark == NULL) {
			mapi_close_handle(hdl);
			free(query);
			return 0;
		}
	} else {
		mnstr_printf(sqlf, "CREATE %s ", ftkey);
		dquoted_print(sqlf, sname, ".");
		dquoted_print(sqlf, fname, "(");
	}
	/* strdup these two because they are needed after another query */
	if (flkey) {
		if ((flkey = strdup(flkey)) == NULL) {
			if (remark) {
				free(remark);
				free(sname);
				free(fname);
				free(ftkey);
			}
			goto bailout;
		}
	}
	ffunc = strdup(ffunc);
	query_len = snprintf(query, query_size,
			     "SELECT a.name, a.type, a.type_digits, "
				    "a.type_scale, a.inout "
			     "FROM sys.args a, sys.functions f "
			     "WHERE a.func_id = f.id AND f.id = %s "
			     "ORDER BY a.inout DESC, a.number", fid);
	assert(query_len < (int) query_size);
	if (!ffunc || query_len < 0 || query_len >= (int) query_size) {
		free(ffunc);
		free(flkey);
		if (remark) {
			free(remark);
			free(sname);
			free(fname);
			free(ftkey);
		}
		free(query);
		goto bailout;
	}
	mapi_close_handle(hdl);
	hdl = mapi_query(mid, query);
	free(query);
	if (hdl == NULL || mapi_error(mid)) {
		free(ffunc);
		free(flkey);
		if (remark) {
			free(remark);
			free(sname);
			free(fname);
			free(ftkey);
		}
		goto bailout;
	}
	if (flang != 1 && flang != 2) {
		sep = "";
		while (mapi_fetch_row(hdl) != 0) {
			const char *aname = mapi_fetch_field(hdl, 0);
			char *atype = mapi_fetch_field(hdl, 1);
			char *adigs = mapi_fetch_field(hdl, 2);
			char *ascal = mapi_fetch_field(hdl, 3);
			const char *ainou = mapi_fetch_field(hdl, 4);

			if (strcmp(ainou, "0") == 0) {
				/* end of arguments */
				break;
			}

			atype = strdup(atype);
			adigs = strdup(adigs);
			ascal = strdup(ascal);
			if (atype == NULL || adigs == NULL || ascal == NULL) {
				free(atype);
				free(adigs);
				free(ascal);
				free(ffunc);
				free(flkey);
				if (remark) {
					free(remark);
					free(sname);
					free(fname);
					free(ftkey);
				}
				goto bailout;
			}

			mnstr_printf(sqlf, "%s", sep);
			dquoted_print(sqlf, aname, " ");
			dump_type(mid, sqlf, atype, adigs, ascal, hashge);
			sep = ", ";

			free(atype);
			free(adigs);
			free(ascal);
		}
		mnstr_printf(sqlf, ")");
		if (ftype == 1 || ftype == 3 || ftype == 5) {
			sep = "TABLE (";
			mnstr_printf(sqlf, " RETURNS ");
			do {
				const char *aname = mapi_fetch_field(hdl, 0);
				char *atype = strdup(mapi_fetch_field(hdl, 1));
				char *adigs = strdup(mapi_fetch_field(hdl, 2));
				char *ascal = strdup(mapi_fetch_field(hdl, 3));

				if (atype == NULL || adigs == NULL || ascal == NULL) {
					free(atype);
					free(adigs);
					free(ascal);
					free(ffunc);
					free(flkey);
					if (remark) {
						free(remark);
						free(sname);
						free(fname);
						free(ftkey);
					}
					goto bailout;
				}

				assert(strcmp(mapi_fetch_field(hdl, 4), "0") == 0);
				if (ftype == 5) {
					mnstr_printf(sqlf, "%s", sep);
					dquoted_print(sqlf, aname, " ");
					sep = ", ";
				}
				dump_type(mid, sqlf, atype, adigs, ascal, hashge);

				free(atype);
				free(adigs);
				free(ascal);
			} while (mapi_fetch_row(hdl) != 0);
			if (ftype == 5)
				mnstr_printf(sqlf, ")");
		}
		if (flkey) {
			mnstr_printf(sqlf, " LANGUAGE %s", flkey);
			free(flkey);
		}
		mnstr_printf(sqlf, "\n%s\n", ffunc);
	}
	free(ffunc);
	if (remark) {
		if (mapi_seek_row(hdl, 0, MAPI_SEEK_SET) != MOK ||
		    mnstr_printf(sqlf, "COMMENT ON %s ", ftkey) < 0 ||
		    dquoted_print(sqlf, sname, ".") < 0 ||
		    dquoted_print(sqlf, fname, "(") < 0) {
			free(sname);
			free(fname);
			free(ftkey);
			free(remark);
			goto bailout;
		}
		free(sname);
		free(fname);
		free(ftkey);
		sep = "";
		while (mapi_fetch_row(hdl) != 0) {
			char *atype = strdup(mapi_fetch_field(hdl, 1));
			char *adigs = strdup(mapi_fetch_field(hdl, 2));
			char *ascal = strdup(mapi_fetch_field(hdl, 3));
			const char *ainou = mapi_fetch_field(hdl, 4);

			if (!atype || !adigs || !ascal) {
				free(atype);
				free(adigs);
				free(ascal);
				free(remark);
				goto bailout;
			}

			if (strcmp(ainou, "0") == 0) {
				/* end of arguments */
				free(atype);
				free(adigs);
				free(ascal);
				break;
			}
			mnstr_printf(sqlf, "%s", sep);
			dump_type(mid, sqlf, atype, adigs, ascal, hashge);
			sep = ", ";

			free(atype);
			free(adigs);
			free(ascal);
		}
		mnstr_printf(sqlf, ") IS ");
		squoted_print(sqlf, remark, '\'', false);
		mnstr_printf(sqlf, ";\n");
		free(remark);
	}
	mapi_close_handle(hdl);
	return 0;
bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");
	return 1;
}

int
dump_functions(Mapi mid, stream *sqlf, char set_schema, const char *sname, const char *fname, const char *id)
{
	MapiHdl hdl = NULL;
	char *query = NULL;
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
				size_t len = dot - fname + 1;

				to_free = malloc(len);
				if (to_free == NULL)
					goto bailout;
				strcpy_len(to_free, fname, len);
				fname += len;
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
	if (query == NULL)
		goto bailout;

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
			query_len += snprintf(query + query_len, query_size - query_len, "AND f.name = '%s' ", fname);
		if (!wantSystem) {
			query_len += snprintf(query + query_len, query_size - query_len, "AND NOT f.system ");
		}
	}
	query_len += snprintf(query + query_len, query_size - query_len, "ORDER BY f.func, f.id");
	assert(query_len < (int) query_size);
	if (query_len >= (int) query_size) {
		free(query);
		goto bailout;
	}

	hdl = mapi_query(mid, query);
	free(query);
	if (hdl == NULL || mapi_error(mid))
		goto bailout;
	prev_sid = 0;
	while (mnstr_errnr(sqlf) == MNSTR_NO__ERROR && mapi_fetch_row(hdl) != 0) {
		long sid = strtol(mapi_fetch_field(hdl, 0), NULL, 10);
		const char *schema = mapi_fetch_field(hdl, 1);
		char *fid = strdup(mapi_fetch_field(hdl, 2));

		if (fid) {
			if (set_schema && sid != prev_sid) {
				mnstr_printf(sqlf, "SET SCHEMA ");
				dquoted_print(sqlf, schema, ";\n");
				prev_sid = sid;
			}
			dump_function(mid, sqlf, fid, hashge);
			free(fid);
		} else {
			goto bailout;
		}
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if (to_free)
		free(to_free);
	return mnstr_errnr(sqlf) != MNSTR_NO__ERROR;

bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");
	if (to_free)
		free(to_free);
	return 1;
}

int
dump_database(Mapi mid, stream *sqlf, const char *ddir, const char *ext, bool describe, bool useInserts, bool noescape)
{
	const char *start_trx = "START TRANSACTION";
	const char *end = "ROLLBACK";
	const char *types =
		"SELECT s.name, "
		       "t.systemname, "
		       "t.sqlname "
		"FROM sys.types t LEFT JOIN sys.schemas s ON s.id = t.schema_id "
		"WHERE t.eclass = 18 "
		  "AND (t.schema_id <> 2000 "
		        "OR (t.schema_id = 2000 "
		             "AND t.sqlname NOT IN ('geometrya','mbr','url','inet','json','uuid')))"
		"ORDER BY s.name, t.sqlname";
	const char *users =
		has_schema_path(mid) ?
		has_schema_max_memory(mid) ?
		"SELECT ui.name, "
		       "ui.fullname, "
		       "sys.password_hash(ui.name), "
		       "s.name, "
			   "ui.schema_path, "
			   "ui.max_memory, "
			   "ui.max_workers, "
			   "ui.optimizer, "
			   "au.name "
		"FROM sys.db_user_info ui LEFT OUTER JOIN sys.auths au on ui.default_role = au.id, "
		     "sys.schemas s "
		"WHERE ui.default_schema = s.id "
		  "AND ui.name <> 'monetdb' "
		  "AND ui.name <> '.snapshot' "
		"ORDER BY ui.name" :
		"SELECT ui.name, "
		       "ui.fullname, "
		       "sys.password_hash(ui.name), "
		       "s.name, "
			   "ui.schema_path, "
			   "0, 0, 'default_pipe', cast(null as clob) "
		"FROM sys.db_user_info ui, "
		     "sys.schemas s "
		"WHERE ui.default_schema = s.id "
		  "AND ui.name <> 'monetdb' "
		  "AND ui.name <> '.snapshot' "
		"ORDER BY ui.name" :
		"SELECT ui.name, "
		       "ui.fullname, "
		       "sys.password_hash(ui.name), "
		       "s.name, "
			   "cast(null as clob), "
			   "0, 0, 'default_pipe', cast(null as clob) "
		"FROM sys.db_user_info ui, "
		     "sys.schemas s "
		"WHERE ui.default_schema = s.id "
		  "AND ui.name <> 'monetdb' "
		  "AND ui.name <> '.snapshot' "
		"ORDER BY ui.name";
	const char *roles =
		"SELECT name "
		"FROM sys.auths "
		"WHERE name NOT IN (SELECT name FROM sys.db_user_info) "
		  "AND grantor <> 0 "
		"ORDER BY name";
	const char *grants =
		/* all grants granting roles to users excepting the default role */
		"SELECT a1.name, "
		       "a2.name "
		"FROM sys.auths a1, "
		     "sys.auths a2, "
		     "sys.user_role ur, "
		     "sys.db_user_info ui "
		"WHERE a1.id = ur.login_id "
		  "AND a2.id = ur.role_id "
		  "AND a1.name = ui.name "
		  "AND a2.id <> ui.default_role "
		"ORDER BY a1.name, a2.name";
	const char *table_grants =
		"SELECT s.name, t.name, "
		       "a.name, "
		       "sum(p.privileges), "
		       "g.name, go.opt "
		"FROM sys.schemas s, sys.tables t, "
		     "sys.auths a, sys.privileges p, "
		     "sys.auths g, "
		     "(VALUES (0, ''), (1, ' WITH GRANT OPTION')) AS go (id, opt) "
		"WHERE p.obj_id = t.id "
		  "AND p.auth_id = a.id "
		  "AND t.schema_id = s.id "
		  "AND t.system = FALSE "
		  "AND p.grantor = g.id "
		  "AND p.grantable = go.id "
		"GROUP BY s.name, t.name, a.name, g.name, go.opt "
		"ORDER BY s.name, t.name, a.name, g.name, go.opt";
	const char *column_grants =
		"SELECT s.name, t.name, "
		       "c.name, a.name, "
		       "pc.privilege_code_name, "
		       "g.name, go.opt "
		"FROM sys.schemas s, "
		     "sys.tables t, "
		     "sys.columns c, "
		     "sys.auths a, "
		     "sys.privileges p, "
		     "sys.auths g, "
		     "sys.privilege_codes pc, "
		     "(VALUES (0, ''), (1, ' WITH GRANT OPTION')) AS go (id, opt) "
		"WHERE p.obj_id = c.id "
		  "AND c.table_id = t.id "
		  "AND p.auth_id = a.id "
		  "AND t.schema_id = s.id "
		  "AND t.system = FALSE "
		  "AND p.grantor = g.id "
		  "AND p.privileges = pc.privilege_code_id "
		  "AND p.grantable = go.id "
		"ORDER BY s.name, t.name, c.name, a.name, g.name, p.grantable";
	const char *function_grants =
		"SELECT f.id, "
			   "s.name, "
			   "f.name, "
			   "a.type, "
			   "a.type_digits, "
			   "a.type_scale, "
			   "a.inout, "
			   "a.number, "
			   "au.name, "
			   "pc.privilege_code_name, "
			   "go.opt, "
			   "ft.function_type_keyword "
		"FROM sys.schemas s, "
			 "sys.functions f LEFT OUTER JOIN sys.args a ON f.id = a.func_id, "
			 "sys.auths au, "
			 "sys.privileges p, "
			 "sys.auths g, "
			 "sys.function_types ft, "
			 "sys.privilege_codes pc, "
			 "(VALUES (0, ''), (1, ' WITH GRANT OPTION')) AS go (id, opt) "
		"WHERE NOT f.system "
		  "AND s.id = f.schema_id "
		  "AND f.id = p.obj_id "
		  "AND p.auth_id = au.id "
		  "AND p.grantor = g.id "
		  "AND p.privileges = pc.privilege_code_id "
		  "AND f.type = ft.function_type_id "
		  "AND p.grantable = go.id "
		"ORDER BY s.name, "
				 "f.name, "
				 "au.name, "
				 "g.name, "
				 "p.grantable, "
				 "f.id, "
				 "a.inout DESC, "
				 "a.number";
	const char *global_grants =
		"SELECT a.name, pc.grnt, g.name, go.opt "
		"FROM sys.privileges p, "
		     "sys.auths a, "
		     "sys.auths g, "
		     "(VALUES (0, 'COPY INTO'), (1, 'COPY FROM')) AS pc (id, grnt), "
		     "(VALUES (0, ''), (1, ' WITH GRANT OPTION')) AS go (id, opt) "
		"WHERE p.obj_id = 0 "
		  "AND p.auth_id = a.id "
		  "AND p.grantor = g.id "
		  "AND p.privileges = pc.id "
		  "AND p.grantable = go.id "
		"ORDER BY a.name, g.name, go.opt";
	const char *schemas =
		"SELECT s.name, a.name, rem.remark "
		"FROM sys.schemas s LEFT OUTER JOIN sys.comments rem ON s.id = rem.id, "
		     "sys.auths a "
		"WHERE s.\"authorization\" = a.id "
		  "AND s.system = FALSE "
		"ORDER BY s.name";
	const char *sequences1 =
		"SELECT sch.name, seq.name, rem.remark "
		"FROM sys.schemas sch, "
		     "sys.sequences seq LEFT OUTER JOIN sys.comments rem ON seq.id = rem.id "
		"WHERE sch.id = seq.schema_id "
		"ORDER BY sch.name, seq.name";
	const char *sequences2 =
		"SELECT * FROM sys.describe_sequences ORDER BY sch, seq";
	const char *tables =
		"SELECT t.id AS id, "
			   "s.name AS sname, "
			   "t.name AS name, "
			   "t.type AS type "
		"FROM sys.schemas s, "
			  "sys._tables t "
		"WHERE t.type IN (0, 3, 4, 5, 6) "
		  "AND t.system = FALSE "
		  "AND s.id = t.schema_id "
		"ORDER BY id";
	const char *mergetables =
		has_table_partitions(mid) ?
		"SELECT subq.s1name, "
		       "subq.t1name, "
		       "subq.s2name, "
		       "subq.t2name, "
		       "table_partitions.type "
		"FROM (SELECT t1.id, "
			     "t1.type, "
			     "s1.name AS s1name, "
			     "t1.name AS t1name, "
			     "s2.name AS s2name, "
			     "t2.name AS t2name "
		      "FROM sys.schemas s1, "
			   "sys._tables t1, "
			   "sys.dependencies d, "
			   "sys.schemas s2, "
			   "sys._tables t2 "
		      "WHERE t1.type IN (3, 6) "
			"AND t1.schema_id = s1.id "
			"AND s1.name <> 'tmp' "
			"AND t1.system = FALSE "
			"AND t1.id = d.depend_id "
			"AND d.id = t2.id "
			"AND t2.schema_id = s2.id "
		      "ORDER BY t1.id, t2.id) subq "
			"LEFT OUTER JOIN sys.table_partitions "
				"ON subq.id = table_partitions.table_id"
		:
		"SELECT s1.name, "
		       "t1.name, "
		       "s2.name, "
		       "t2.name, "
		       "0 "
		"FROM sys.schemas s1, "
		     "sys._tables t1, "
		     "sys.dependencies d, "
		     "sys.schemas s2, "
		     "sys._tables t2 "
		"WHERE t1.type = 3 "
		  "AND t1.schema_id = s1.id "
		  "AND s1.name <> 'tmp' "
		  "AND t1.system = FALSE "
		  "AND t1.id = d.depend_id "
		  "AND d.id = t2.id "
		  "AND t2.schema_id = s2.id "
		"ORDER BY t1.id, t2.id";
	/* we must dump views, functions/procedures and triggers in order
	 * of creation since they can refer to each other */
	const char *views_functions_triggers =
		"with vft (sname, name, id, query, remark) AS ("
			"SELECT s.name AS sname, " /* views */
			       "t.name AS name, "
			       "t.id AS id, "
			       "t.query AS query, "
			       "rem.remark AS remark "
			"FROM sys.schemas s, "
			     "sys._tables t LEFT OUTER JOIN sys.comments rem ON t.id = rem.id "
			"WHERE t.type = 1 "
			  "AND t.system = FALSE "
			  "AND s.id = t.schema_id "
			  "AND s.name <> 'tmp' "
			"UNION ALL "
			"SELECT s.name AS sname, " /* functions and procedures */
			       "f.name AS name, "
			       "f.id AS id, "
			       "NULL AS query, "
			       "NULL AS remark " /* emitted separately */
			"FROM sys.schemas s, "
			     "sys.functions f "
			"WHERE s.id = f.schema_id "
			"AND NOT f.system "
			"UNION ALL "
			"SELECT s.name AS sname, " /* triggers */
			       "tr.name AS name, "
			       "tr.id AS id, "
			       "tr.\"statement\" AS query, "
			       "NULL AS remark " /* not available yet */
			"FROM sys.triggers tr, "
			     "sys.schemas s, "
			     "sys._tables t "
			"WHERE s.id = t.schema_id "
			  "AND t.id = tr.table_id "
			  "AND t.system = FALSE"
		") "
		"SELECT id, sname, name, query, remark FROM vft ORDER BY id";
	char *sname = NULL;
	char *curschema = NULL;
	MapiHdl hdl = NULL;
	int rc = 0;
	int lastfid = 0;
	const char *sep;
	bool hashge = has_hugeint(mid);

	/* start a transaction for the dump */
	mnstr_printf(sqlf, "%s;\n", start_trx);

	if ((hdl = mapi_query(mid, start_trx)) == NULL || mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	sname = get_schema(mid);
	if (sname == NULL)
		goto bailout2;
	mnstr_printf(sqlf, "SET SCHEMA ");
	dquoted_print(sqlf, sname, ";\n");
	curschema = strdup(sname);
	if (curschema == NULL)
		goto bailout;
	if (strcmp(sname, "sys") == 0 || strcmp(sname, "tmp") == 0) {
		free(sname);
		sname = NULL;

		/* dump roles */
		if ((hdl = mapi_query(mid, roles)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *name = mapi_fetch_field(hdl, 0);

			mnstr_printf(sqlf, "CREATE ROLE ");
			dquoted_print(sqlf, name, ";\n");
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
			const char *spath = mapi_fetch_field(hdl, 4);
			const char *mmemory = mapi_fetch_field(hdl, 5);
			const char *mworkers = mapi_fetch_field(hdl, 6);
			const char *optimizer = mapi_fetch_field(hdl, 7);
			const char *defrole = mapi_fetch_field(hdl, 8);

			mnstr_printf(sqlf, "CREATE USER ");
			dquoted_print(sqlf, uname, " ");
			mnstr_printf(sqlf, "WITH ENCRYPTED PASSWORD ");
			squoted_print(sqlf, pwhash, '\'', false);
			mnstr_printf(sqlf, " NAME ");
			squoted_print(sqlf, fullname, '\'', false);
			mnstr_printf(sqlf, " SCHEMA ");
			dquoted_print(sqlf, describe ? sname : "sys", NULL);
			if (spath && strcmp(spath, "\"sys\"") != 0) {
				mnstr_printf(sqlf, " SCHEMA PATH ");
				squoted_print(sqlf, spath, '\'', false);
			}
			if (mmemory && strcmp(mmemory, "0") != 0) {
				mnstr_printf(sqlf, " MAX_MEMORY %s", mmemory);
			}
			if (mworkers && strcmp(mworkers, "0") != 0) {
				mnstr_printf(sqlf, " MAX_WORKERS %s", mworkers);
			}
			if (optimizer && strcmp(optimizer, "default_pipe") != 0) {
				mnstr_printf(sqlf, " OPTIMIZER ");
				squoted_print(sqlf, optimizer, '\'', false);
			}
			if (defrole && strcmp(defrole, uname) != 0) {
				mnstr_printf(sqlf, " DEFAULT ROLE ");
				dquoted_print(sqlf, defrole, NULL);
			}
			mnstr_printf(sqlf, ";\n");
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);

		/* dump schemas */
		if ((hdl = mapi_query(mid, schemas)) == NULL ||
		    mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *sname = mapi_fetch_field(hdl, 0);
			const char *aname = mapi_fetch_field(hdl, 1);
			const char *remark = mapi_fetch_field(hdl, 2);

			mnstr_printf(sqlf, "CREATE SCHEMA ");
			dquoted_print(sqlf, sname, NULL);
			if (strcmp(aname, "sysadmin") != 0) {
				mnstr_printf(sqlf,
					     " AUTHORIZATION ");
				dquoted_print(sqlf, aname, NULL);
			}
			mnstr_printf(sqlf, ";\n");
			comment_on(sqlf, "SCHEMA", sname, NULL, NULL, remark);
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
				mnstr_printf(sqlf, "ALTER USER ");
				dquoted_print(sqlf, uname, " SET SCHEMA ");
				dquoted_print(sqlf, sname, ";\n");
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

			mnstr_printf(sqlf, "GRANT ");
			dquoted_print(sqlf, rname, " TO ");
			if (strcmp(uname, "public") == 0)
				mnstr_printf(sqlf, "PUBLIC");
			else
				dquoted_print(sqlf, uname, NULL);
			/* optional WITH ADMIN OPTION and FROM
			   (CURRENT_USER|CURRENT_ROLE) are ignored by
			   server, so we can't dump them */
			mnstr_printf(sqlf, ";\n");
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);

		/* grant global privileges */
		if ((hdl = mapi_query(mid, global_grants)) == NULL || mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *uname = mapi_fetch_field(hdl, 0);
			const char *grant = mapi_fetch_field(hdl, 1);
			//const char *gname = mapi_fetch_field(hdl, 2);
			const char *grantable = mapi_fetch_field(hdl, 3);
			mnstr_printf(sqlf, "GRANT %s TO ", grant);
			dquoted_print(sqlf, uname, grantable);
			mnstr_printf(sqlf, ";\n");
		}
		if (mapi_error(mid))
			goto bailout;
		mapi_close_handle(hdl);
	}

	/* dump types */
	if ((hdl = mapi_query(mid, types)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *sname = mapi_fetch_field(hdl, 0);
		const char *sysname = mapi_fetch_field(hdl, 1);
		const char *sqlname = mapi_fetch_field(hdl, 2);
		mnstr_printf(sqlf, "CREATE TYPE ");
		dquoted_print(sqlf, sname, ".");
		dquoted_print(sqlf, sqlname, " EXTERNAL NAME ");
		dquoted_print(sqlf, sysname, ";\n");
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	/* dump sequences, part 1 */
	if ((hdl = mapi_query(mid, sequences1)) == NULL || mapi_error(mid))
		goto bailout;

	while (mapi_fetch_row(hdl) != 0) {
		const char *schema = mapi_fetch_field(hdl, 0);
		const char *name = mapi_fetch_field(hdl, 1);
		const char *remark = mapi_fetch_field(hdl, 2);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		mnstr_printf(sqlf, "CREATE SEQUENCE ");
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, name, " AS INTEGER;\n");
		comment_on(sqlf, "SEQUENCE", schema, name, NULL, remark);
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);
	hdl = NULL;

	/* Tables, views, triggers, and functions can all reference each
	 * other, so we need to be very careful in how we dump them.  We
	 * first dump the tables (all types), including data, but without
	 * the DEFAULT clause which is the one that can reference functions,
	 * views, and other tables.  Then we add tables to the MERGE tables
	 * they belong to.  After this, we dump the views, triggers, and
	 * functions in order of original creation (they can't be altered
	 * afterwards, so they can only reference objects that were created
	 * earlier).  Finally, we set the DEFAULT clauses on the tables. */

	/* dump tables */
	if ((hdl = mapi_query(mid, tables)) == NULL || mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       mnstr_errnr(sqlf) == MNSTR_NO__ERROR &&
	       mapi_fetch_row(hdl) != 0) {
		char *id = strdup(mapi_fetch_field(hdl, 0));
		char *schema = strdup(mapi_fetch_field(hdl, 1));
		char *name = strdup(mapi_fetch_field(hdl, 2));
		const char *type = mapi_fetch_field(hdl, 3);

		if (mapi_error(mid) || id == NULL || schema == NULL || name == NULL) {
			free(id);
			free(schema);
			free(name);
			goto bailout;
		}
		if (sname != NULL && strcmp(schema, sname) != 0) {
			free(id);
			free(schema);
			free(name);
			continue;
		}
		if (strcmp(schema, "tmp") != 0) {
			if (curschema == NULL || strcmp(schema, curschema) != 0) {
				if (curschema)
					free(curschema);
				curschema = strdup(schema);
				if (curschema == NULL) {
					free(id);
					free(schema);
					free(name);
					goto bailout;
				}
				mnstr_printf(sqlf, "SET SCHEMA ");
				dquoted_print(sqlf, curschema, ";\n");
			}
		}
		int ptype = atoi(type), dont_describe = (ptype == 3 || ptype == 5);
		rc = dump_table(mid, schema, name, sqlf, ddir, ext, dont_describe || describe, describe, useInserts, true, noescape, false);
		free(id);
		free(schema);
		free(name);
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	/* add tables to MERGE tables */
	if ((hdl = mapi_query(mid, mergetables)) == NULL || mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       mnstr_errnr(sqlf) == MNSTR_NO__ERROR &&
	       mapi_fetch_row(hdl) != 0) {
		const char *schema1 = mapi_fetch_field(hdl, 0);
		const char *tname1 = mapi_fetch_field(hdl, 1);
		const char *schema2 = mapi_fetch_field(hdl, 2);
		const char *tname2 = mapi_fetch_field(hdl, 3);
		const char *prop = mapi_fetch_field(hdl, 4);
		int properties = prop ? atoi(prop) : 0;

		if (mapi_error(mid))
			goto bailout;
		if (schema1 == NULL || schema2 == NULL) {
			/* cannot happen, but make analysis tools happy */
			continue;
		}
		if (sname != NULL && strcmp(schema1, sname) != 0)
			continue;
		mnstr_printf(sqlf, "ALTER TABLE ");
		dquoted_print(sqlf, schema1, ".");
		dquoted_print(sqlf, tname1, " ADD TABLE ");
		dquoted_print(sqlf, schema2, ".");
		dquoted_print(sqlf, tname2, NULL);
		if (properties) {
			MapiHdl shdl = NULL;
			char *s2 = sescape(schema2);
			char *t2 = sescape(tname2);
			const size_t query_size = 5120;
			char *query = malloc(query_size);
			if (query == NULL)
				goto bailout;

			mnstr_printf(sqlf, " AS PARTITION");
			if ((properties & 2) == 2) { /* by values */
				int i = 0;
				bool first = true, found_nil = false;
				snprintf(query, query_size,
					 "SELECT vp.value "
					 "FROM sys.schemas s, "
					      "sys._tables t, "
					      "sys.value_partitions vp "
					 "WHERE s.name = '%s' "
					   "AND t.name = '%s' "
					   "AND s.id = t.schema_id "
					   "AND t.id = vp.table_id",
					 s2, t2);
				shdl = mapi_query(mid, query);
				free(query);
				if (shdl == NULL || mapi_error(mid)) {
					mapi_close_handle(shdl);
					goto bailout;
				}
				while (mapi_fetch_row(shdl) != 0) {
					char *nextv = mapi_fetch_field(shdl, 0);
					if (first && nextv == NULL) {
						found_nil = true;
						first = false; // if the partition can hold null values, is explicit in the first entry
						continue;
					}
					if (nextv) {
						if (i == 0) {
							// start by writing the IN clause
							mnstr_printf(sqlf, " IN (");
						} else {
							mnstr_printf(sqlf, ", ");
						}
						squoted_print(sqlf, nextv, '\'', false);
						i++;
					}
					first = false;
				}
				mapi_close_handle(shdl);
				if (i > 0) {
					mnstr_printf(sqlf, ")");
				}
				if (found_nil) {
					mnstr_printf(sqlf, " %s NULL VALUES", (i == 0) ? "FOR" : "WITH");
				}
			} else { /* by range */
				char *minv = NULL, *maxv = NULL, *wnulls = NULL;
				snprintf(query, query_size,
					 "SELECT rp.minimum, "
						"rp.maximum, "
						"rp.with_nulls "
					 "FROM sys.schemas s, "
					      "sys._tables t, "
					      "sys.range_partitions rp "
					 "WHERE s.name = '%s' "
					   "AND t.name = '%s' "
					   "AND s.id = t.schema_id "
					   "AND t.id = rp.table_id",
					 s2, t2);
				shdl = mapi_query(mid, query);
				free(query);
				if (shdl == NULL || mapi_error(mid)) {
					mapi_close_handle(shdl);
					goto bailout;
				}
				while (mapi_fetch_row(shdl) != 0) {
					minv = mapi_fetch_field(shdl, 0);
					maxv = mapi_fetch_field(shdl, 1);
					wnulls = mapi_fetch_field(shdl, 2);
				}
				if (minv || maxv || !wnulls || (!minv && !maxv && wnulls && strcmp(wnulls, "false") == 0)) {
					mnstr_printf(sqlf, " FROM ");
					if (minv)
						squoted_print(sqlf, minv, '\'', false);
					else
						mnstr_printf(sqlf, "RANGE MINVALUE");
					mnstr_printf(sqlf, " TO ");
					if (maxv)
						squoted_print(sqlf, maxv, '\'', false);
					else
						mnstr_printf(sqlf, "RANGE MAXVALUE");
				}
				if (!wnulls || strcmp(wnulls, "true") == 0)
					mnstr_printf(sqlf, " %s NULL VALUES", (minv || maxv || !wnulls) ? "WITH" : "FOR");
				mapi_close_handle(shdl);
			}
			free(s2);
			free(t2);
		}
		mnstr_printf(sqlf, ";\n");
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	/* dump views, functions and triggers */
	if ((hdl = mapi_query(mid, views_functions_triggers)) == NULL ||
		mapi_error(mid))
		goto bailout;

	while (rc == 0 &&
	       mnstr_errnr(sqlf) == MNSTR_NO__ERROR &&
	       mapi_fetch_row(hdl) != 0) {
		char *id = strdup(mapi_fetch_field(hdl, 0));
		char *schema = strdup(mapi_fetch_field(hdl, 1));
		char *name = strdup(mapi_fetch_field(hdl, 2));
		const char *query = mapi_fetch_field(hdl, 3);
		const char *remark = mapi_fetch_field(hdl, 4);

		if (mapi_error(mid) || id == NULL || schema == NULL || name == NULL) {
			free(id);
			free(schema);
			free(name);
			goto bailout;
		}
		if (sname != NULL && strcmp(schema, sname) != 0) {
			free(id);
			free(schema);
			free(name);
			continue;
		}
		if (curschema == NULL || strcmp(schema, curschema) != 0) {
			if (curschema)
				free(curschema);
			curschema = strdup(schema);
			if (curschema == NULL) {
				free(id);
				free(schema);
				free(name);
				goto bailout;
			}
			mnstr_printf(sqlf, "SET SCHEMA ");
			dquoted_print(sqlf, curschema, ";\n");
		}
		if (query) {
			/* view or trigger */
			mnstr_printf(sqlf, "%s\n", query);
			/* only views have comments due to query */
			comment_on(sqlf, "VIEW", schema, name, NULL, remark);
		} else {
			/* procedure */
			dump_functions(mid, sqlf, 0, schema, name, id);
		}
		free(id);
		free(schema);
		free(name);
	}
	mapi_close_handle(hdl);
	hdl = NULL;

	/* dump DEFAULT clauses for tables */
	if (dump_table_defaults(mid, NULL, NULL, sqlf))
		goto bailout2;

	if (!describe) {
		if (dump_foreign_keys(mid, NULL, NULL, NULL, sqlf))
			goto bailout2;

		/* dump sequences, part 2 */
		if ((hdl = mapi_query(mid, sequences2)) == NULL ||
		    mapi_error(mid))
			goto bailout;

		while (mapi_fetch_row(hdl) != 0) {
			const char *schema = mapi_fetch_field(hdl, 0);		/* sch */
			const char *name = mapi_fetch_field(hdl, 1);		/* seq */
			const char *restart = mapi_fetch_field(hdl, 3);		/* rs */
			const char *minvalue;
			const char *maxvalue;
			const char *increment = mapi_fetch_field(hdl, 6);	/* inc */
			const char *cycle = mapi_fetch_field(hdl, 8);		/* cycle */

			if (mapi_get_field_count(hdl) > 9) {
				/* new version (Jan2022) of sys.describe_sequences */
				minvalue = mapi_fetch_field(hdl, 11);			/* rmi */
				maxvalue = mapi_fetch_field(hdl, 12);			/* rma */
			} else {
				/* old version (pre Jan2022) of sys.describe_sequences */
				minvalue = mapi_fetch_field(hdl, 4);			/* minvalue */
				maxvalue = mapi_fetch_field(hdl, 5);			/* maxvalue */
				if (strcmp(minvalue, "0") == 0)
					minvalue = NULL;
				if (strcmp(maxvalue, "0") == 0)
					maxvalue = NULL;
			}

			if (sname != NULL && strcmp(schema, sname) != 0)
				continue;

			mnstr_printf(sqlf,
				     "ALTER SEQUENCE ");
			dquoted_print(sqlf, schema, ".");
			dquoted_print(sqlf, name, NULL);
			mnstr_printf(sqlf, " RESTART WITH %s", restart);
			if (strcmp(increment, "1") != 0)
				mnstr_printf(sqlf, " INCREMENT BY %s", increment);
			if (minvalue)
				mnstr_printf(sqlf, " MINVALUE %s", minvalue);
			if (maxvalue)
				mnstr_printf(sqlf, " MAXVALUE %s", maxvalue);
			mnstr_printf(sqlf, " %sCYCLE;\n", strcmp(cycle, "true") == 0 ? "" : "NO ");
			if (mnstr_errnr(sqlf) != MNSTR_NO__ERROR) {
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
		mnstr_printf(sqlf, "GRANT");
		if (priv == 79) {
			mnstr_printf(sqlf, " ALL PRIVILEGES");
		} else {
			sep = "";

			if (priv & 1) {
				mnstr_printf(sqlf, "%s SELECT", sep);
				sep = ",";
			}
			if (priv & 2) {
				mnstr_printf(sqlf, "%s UPDATE", sep);
				sep = ",";
			}
			if (priv & 4) {
				mnstr_printf(sqlf, "%s INSERT", sep);
				sep = ",";
			}
			if (priv & 8) {
				mnstr_printf(sqlf, "%s DELETE", sep);
				sep = ",";
			}
			if (priv & 16) {
				mnstr_printf(sqlf, "%s EXECUTE", sep);
				sep = ",";
			}
			if (priv & 32) {
				mnstr_printf(sqlf, "%s GRANT", sep);
				sep = ",";
			}
			if (priv & 64) {
				mnstr_printf(sqlf, "%s TRUNCATE", sep);
				// sep = ",";		/* sep will be overwritten after this */
			}
		}
		mnstr_printf(sqlf, " ON TABLE ");
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, tname, " TO ");
		dquoted_print(sqlf, aname, grantable);
		mnstr_printf(sqlf, ";\n");
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
		mnstr_printf(sqlf, "GRANT %s(", priv);
		dquoted_print(sqlf, cname, ") ON ");
		dquoted_print(sqlf, schema, ".");
		dquoted_print(sqlf, tname, " TO ");
		if (strcmp(aname, "public") == 0) {
			mnstr_printf(sqlf, "PUBLIC%s", grantable);
		} else {
			dquoted_print(sqlf, aname, grantable);
		}
		mnstr_printf(sqlf, ";\n");
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if ((hdl = mapi_query(mid, function_grants)) == NULL ||
	    mapi_error(mid))
		goto bailout;

	sep = "";
	while (mapi_fetch_row(hdl) != 0) {
		const char *fid = mapi_fetch_field(hdl, 0);
		const char *schema = mapi_fetch_field(hdl, 1);
		const char *fname = mapi_fetch_field(hdl, 2);
		const char *argtype = mapi_fetch_field(hdl, 3);
		const char *argdigits = mapi_fetch_field(hdl, 4);
		const char *argscale = mapi_fetch_field(hdl, 5);
		const char *arginout = mapi_fetch_field(hdl, 6);
		const char *argnumber = mapi_fetch_field(hdl, 7);
		const char *aname = mapi_fetch_field(hdl, 8);
		const char *priv = mapi_fetch_field(hdl, 9);
		const char *grantable = mapi_fetch_field(hdl, 10);
		const char *ftype = mapi_fetch_field(hdl, 11);

		if (sname != NULL && strcmp(schema, sname) != 0)
			continue;
		int thisfid = atoi(fid);
		if (lastfid != thisfid) {
			lastfid = thisfid;
			sep = "";
			mnstr_printf(sqlf, "GRANT %s ON %s ", priv, ftype);
			dquoted_print(sqlf, schema, ".");
			dquoted_print(sqlf, fname, "(");
		}
		if (arginout != NULL && strcmp(arginout, "1") == 0) {
			mnstr_printf(sqlf, "%s", sep);
			dump_type(mid, sqlf, argtype, argdigits, argscale, hashge);
			sep = ", ";
		} else if (argnumber == NULL || strcmp(argnumber, "0") == 0) {
			mnstr_printf(sqlf, ") TO ");
			if (strcmp(aname, "public") == 0) {
				mnstr_printf(sqlf, "PUBLIC%s", grantable);
			} else {
				dquoted_print(sqlf, aname, grantable);
			}
			mnstr_printf(sqlf, ";\n");
		}
	}
	if (mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	if (curschema) {
		if (strcmp(sname ? sname : "sys", curschema) != 0) {
			mnstr_printf(sqlf, "SET SCHEMA ");
			dquoted_print(sqlf, sname ? sname : "sys", ";\n");
		}
		free(curschema);
		curschema = NULL;
	}

	if ((hdl = mapi_query(mid, end)) == NULL || mapi_error(mid))
		goto bailout;
	mapi_close_handle(hdl);

	/* finally commit the whole transaction */
	mnstr_printf(sqlf, "COMMIT;\n");
	if (sname)
		free(sname);
	return rc;

bailout:
	if (hdl) {
		if (mapi_result_error(hdl))
			mapi_explain_result(hdl, stderr);
		else if (mapi_error(mid))
			mapi_explain_query(hdl, stderr);
		else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
			fprintf(stderr, "malloc failure\n");
		mapi_close_handle(hdl);
	} else if (mapi_error(mid))
		mapi_explain(mid, stderr);
	else if (mnstr_errnr(sqlf) == MNSTR_NO__ERROR)
		fprintf(stderr, "malloc failure\n");

bailout2:
	if (sname)
		free(sname);
	if (curschema)
		free(curschema);
	hdl = mapi_query(mid, end);
	if (hdl)
		mapi_close_handle(hdl);
	return 1;
}

void
dump_version(Mapi mid, stream *sqlf, const char *prefix)
{
	MapiHdl hdl;
	char *dbname = NULL, *uri = NULL, *dbver = NULL, *dbrel = NULL, *dbrev = NULL;
	const char *name, *val;

	if ((hdl = mapi_query(mid,
			      "SELECT name, value "
			      "FROM sys.env() AS env "
			      "WHERE name IN ('gdk_dbname', "
					"'monet_version', "
					"'monet_release', "
					"'merovingian_uri', "
					"'revision')")) == NULL ||
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
			} else if (strcmp(name, "revision") == 0) {
				assert(dbrev == NULL);
				dbrev = strdup(val);
			}
		}
	}
	if (uri != NULL) {
		if (dbname != NULL)
			free(dbname);
		dbname = uri;
		uri = NULL;
	}
	mnstr_printf(sqlf, "%s MonetDB", prefix);
	if (dbver)
		mnstr_printf(sqlf, " v%s", dbver);
	if (dbrel && strcmp(dbrel, "unreleased") != 0)
		mnstr_printf(sqlf, " (%s)", dbrel);
	else if (dbrev && strcmp(dbrev, "Unknown") != 0)
		mnstr_printf(sqlf, " (hg id: %s)", dbrev);
	if (dbname)
		mnstr_printf(sqlf, ", '%s'", dbname);
	mnstr_printf(sqlf, "\n");

  cleanup:
	if (dbname != NULL)
		free(dbname);
	if (dbver != NULL)
		free(dbver);
	if (dbrel != NULL)
		free(dbrel);
	if (uri != NULL)
		free(uri);
	if (dbrev != NULL)
		free(dbrev);
	if (hdl)
		mapi_close_handle(hdl);
}
