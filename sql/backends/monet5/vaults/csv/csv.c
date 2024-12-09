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
#include "rel_file_loader.h"
#include "rel_exp.h"

#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_linker.h"
#include "mal_backend.h"
#include "sql_types.h"
#include "rel_bin.h"

#include <unistd.h>

static stream *
csv_open_file(char* filename)
{
	return open_rastream(filename);
}

/* todo handle escapes */
static const char *
next_delim(const char *s, const char *e, char delim, char quote)
{
	if (s && e) {
		bool inquote = false;
		for(;  s < e; s++) {
			if (*s == quote)
				inquote = !inquote;
			else if (!inquote && *s == delim)
				return s;
		}
		if (s <= e)
			return s;
	}
	return NULL;
}

/* todo detect escapes */
static char
detect_quote(const char *buf)
{
	const char *cur = buf;
	const char *l = NULL;
	/* "'(none) */
	bool has_double_quote = true, has_single_quote = true;
	while ((has_double_quote || has_single_quote) && (l = strchr(cur, '\n')) != NULL) {
		const char *s = cur, *t;
		if (has_double_quote && ((t = strchr(s, '"')) == NULL || t > l))  /* no quote not used */
			has_double_quote = false;
		if (has_single_quote && ((t = strchr(s, '\'')) == NULL || t > l))  /* no quote not used */
			has_single_quote = false;
		cur = l+1;
	}
	if (has_double_quote && !has_single_quote)
		return '"';
	if (has_single_quote && !has_double_quote)
		return '\'';
	/* no quote */
	return '\0';
}

#define DLEN 4
static char
detect_delimiter(const char *buf, char q, int *nr_fields)
{
	const char delimiter[] = ",|;\t";
	int cnts[DLEN][2] = { 0 }, l = 0;

	const char *cur = buf;

	for (l = 0; l < 2; l++) { /* start with 2 lines only */
		const char *e = strchr(cur, '\n');
		if (!e)
			break;
		int i = 0;
		const char *dp = delimiter;
		for (char d = *dp; d; d=*(++dp), i++) {
			const char *s = cur;
			/* all lines should have some numbers */
			if (l && cnts[i][l])
				if (cnts[i][0] != cnts[i][1])
					break;
			int nr = 1;
			while( (s = next_delim(s, e, d, q)) != NULL && s<e ) {
				if (s+1 <= e)
					nr++;
				s++;
			}
			cnts[i][l] = nr;
		}
		cur = e+1;
	}
	if (l) {
		int maxpos = -1, maxcnt = 0;
		for (int i = 0; i<DLEN; i++) {
			if (cnts[i][0] == cnts[i][1] && maxcnt < cnts[i][0]) {
				maxcnt = cnts[i][0];
				maxpos = i;
			}
		}
		if (maxpos>=0) {
			*nr_fields = maxcnt;
			return delimiter[maxpos];
		}
	}
	/* nothing detected */
	return ' ';
}

typedef enum csv {
	CSV_NULL = 0,
	CSV_BOOLEAN,
	CSV_BIGINT,
	CSV_DECIMAL,
	CSV_DOUBLE,
	CSV_TIME,
	CSV_DATE,
	CSV_TIMESTAMP,
	CSV_STRING,
	//later: UUID, INET, JSON, URL etc
} csv_types_t;

typedef struct csv_type {
	csv_types_t type;
	int scale;
} csv_type;

static char*
csv_type_map(csv_type ct)
{
	switch(ct.type) {
	case CSV_NULL:
		return  "null";
	case CSV_BOOLEAN:
		return  "boolean";
	case CSV_BIGINT:
		return  "bigint";
	case CSV_DECIMAL:
		return  "decimal";
	case CSV_DOUBLE:
		return  "double";
	case CSV_TIME:
		return  "time";
	case CSV_DATE:
		return  "date";
	case CSV_TIMESTAMP:
		return  "timestamp";
	case CSV_STRING:
		return  "varchar";
	}
	return  "varchar";
}

static bool
detect_null(const char *s, const char *e)
{
	if (e == s)
		return true;
	/* TODO parse NULL value(s) */
	return false;
}

static bool
detect_bool(const char *s, const char *e)
{
	if ((e - s) == 1 && (*s == 'T' || *s == 't' || *s == 'F' || *s == 'f'))
		return true;
	if (strcmp(s,"TRUE") == 0 || strcmp(s,"true") == 0 || strcmp(s,"FALSE") == 0 || strcmp(s,"false") == 0)
		return true;
	if (strcmp(s,"NULL") == 0)
		return true;
	return false;
}

static bool
detect_bigint(const char *s, const char *e)
{
	if (s[0] == '-' || s[0] == '+')
		s++;
	while(s < e) {
		if (!isdigit(*s))
			break;
		s++;
	}
	if (s==e)
		return true;
	return false;
}

static bool
detect_decimal(const char *s, const char *e, int *scale)
{
	int dotseen = 0;

	if (s[0] == '-' || s[0] == '+')
		s++;
	while(s < e) {
		if (!dotseen && *s == '.')
			dotseen = (int)(e-(s+1));
		else if (!isdigit(*s))
			break;
		s++;
	}
	if (s==e && dotseen) {
		*scale = dotseen;
		return true;
	}
	return false;
}

static bool
detect_time(const char *s, const char *e)
{
	/* TODO detect time with timezone */
	if ((e-s) != 5)
		return false;
	/* 00:00 - 23:59 */
	if (s[2] != ':')
		return false;
	if ((((s[0] == '0' || s[0] == '1') &&
	      (s[1] >= '0' && s[1] <= '9'))  ||
	      (s[0] == '2' && (s[1] >= '0' && s[1] <= '3'))) &&
		(s[3] >= '0' && s[3] <= '5' && s[4] >= '0' && s[4] <= '9'))
		return true;
	return false;
}

static bool
detect_date(const char *s, const char *e)
{
	/* TODO detect negative years */
	if ((e-s) != 10)
		return false;
	/* YYYY-MM-DD */
	if ( s[4] == '-' && s[7] == '-' &&
	   ((s[5] == '0' && s[6] >= '0' && s[6] <= '9') ||
	    (s[5] == '1' && s[6] >= '0' && s[6] <= '2')) &&
	    (s[8] >= '0' && s[8] <= '3' && s[9] >= '0' && s[9] <= '9'))
		return true;
	return false;
}

static bool
detect_timestamp(const char *s, const char *e)
{
	/* TODO detect negative years */
	if ((e-s) != 16)
		return false;
	/* DATE TIME */
	if (detect_date(s, s+5) && detect_time(s+6, e))
		return true;
	return false;
}

/* per row */
static  csv_type *
detect_types_row(const char *s, const char *e, char delim, char quote, int nr_fields)
{
	csv_type *types = (csv_type*)GDKmalloc(sizeof(csv_type)*nr_fields);
	if (!types)
		return NULL;
	for(int i = 0; i< nr_fields; i++) {
		const char *n = (i<nr_fields-1)?next_delim(s, e, delim, quote):e;
		int scale = 0;

		types[i].type = CSV_STRING;
		types[i].scale = 0;
		if (n && s) {
			if (detect_null(s,n))
				types[i].type = CSV_NULL;
			else if (detect_bool(s,n))
				types[i].type = CSV_BOOLEAN;
			else if (detect_bigint(s, n))
				types[i].type = CSV_BIGINT;
			else if (detect_decimal(s, n, &scale)) {
				types[i].type = CSV_DECIMAL;
				types[i].scale = scale;
			}
			else if (detect_time(s, n))
				types[i].type = CSV_TIME;
			else if (detect_date(s, n))
				types[i].type = CSV_DATE;
			else if (detect_timestamp(s, n))
				types[i].type = CSV_TIMESTAMP;
			s = n+1;
		}
	}
	return types;
}

static csv_type *
detect_types(const char *buf, char delim, char quote, int nr_fields, bool *has_header)
{
	const char *cur = buf;
	csv_type *types = NULL;
	int nr_lines = 0;

	while ( true ) {
		const char *e = strchr(cur, '\n');

		if (!e)
			break;
		csv_type *ntypes = detect_types_row(cur, e, delim, quote, nr_fields);
		if (!ntypes)
			return NULL;
		cur = e+1;
		int i = 0;
		if (!types) {
			for(i = 0; i<nr_fields && ntypes[i].type == CSV_STRING; i++)  ;

			if (i == nr_fields)
				*has_header = true;
		} else { /* check if all are string, then no header */
			for(i = 0; i<nr_fields && ntypes[i].type == CSV_STRING; i++)  ;

			if (i == nr_fields)
				*has_header = false;
		}
		if (nr_lines == 1)
			for(i = 0; i<nr_fields; i++)
				types[i] = ntypes[i];
		if (nr_lines > 1) {
			for(i = 0; i<nr_fields; i++) {
				if (types[i].type == ntypes[i].type && types[i].type == CSV_DECIMAL && types[i].scale != ntypes[i].scale) {
					types[i].type = CSV_DOUBLE;
					types[i].scale = 0;
				} else if (types[i].type < ntypes[i].type)
					types[i] = ntypes[i];
			}
		}
		if (types)
			GDKfree(ntypes);
		else
			types = ntypes;
		nr_lines++;
	}
	if (types) { /* NULL -> STRING */
		for(int i = 0; i<nr_fields; i++) {
			if (types[i].type == CSV_NULL)
				types[i].type = CSV_STRING;
		}
	}
	return types;
}

static const char *
get_name(allocator *sa, const char *s, const char *es, const char **E, char delim, char quote, bool has_header, int col)
{
	if (!has_header) {
		char buff[25];
		snprintf(buff, 25, "name_%i", col);
		return SA_STRDUP(sa, buff);
	} else {
		const char *e = next_delim(s, es, delim, quote);
		if (e) {
			char *end = (char*)e;
			if (s[0] == quote) {
				s++;
				end--;
			}
			end[0] = 0;
			*E = e+1;
			return SA_STRDUP(sa, s);
		}
	}
	return NULL;
}

typedef struct csv_t {
	char sname[1];
	char quote;
	char delim;
	bool has_header;
	bool extra_tsep;
} csv_t;

/*
 * returns an error string (static or via tmp sa_allocator allocated), NULL on success
 *
 * Extend the subfunc f with result columns, ie.
	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist; use tname if passed, for the relation name
 * Fill the list res_exps, with one result expressions per resulting column.
 */
static str
csv_relation(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname)
{
	stream *file = csv_open_file(filename);
	if (file == NULL)
		return RUNTIME_FILE_NOT_FOUND;

	/*
	 * detect delimiter ;|,\t  using quote \" or \' or none TODO escape \"\'\\ or none
	 * detect types
	 * detect header
	 */
	char buf[8196+1];
	ssize_t l = mnstr_read(file, buf, 1, 8196);
	mnstr_close(file);
	mnstr_destroy(file);
	if (l<0)
		return RUNTIME_LOAD_ERROR;
	buf[l] = 0;
	bool has_header = false, extra_tsep = false;
	int nr_fields = 0;
	char q = detect_quote(buf);
	char d = detect_delimiter(buf, q, &nr_fields);
	csv_type *types = detect_types(buf, d, q, nr_fields, &has_header);

	if (!tname)
		tname = "csv";

	f->tname = tname;

	const char *p = buf, *ep = strchr(p, '\n');
	list *typelist = sa_list(sql->sa);
	list *nameslist = sa_list(sql->sa);
	for(int col = 0; col < nr_fields; col++) {
		const char *name = get_name(sql->sa, p, ep, &p, d, q, has_header, col);
		append(nameslist, (char*)name);
		char* st = csv_type_map(types[col]);

		if(st) {
			sql_subtype *t = (types[col].type == CSV_DECIMAL)?
					sql_bind_subtype(sql->sa, st, 18, types[col].scale):
					sql_bind_subtype(sql->sa, st,  0, types[col].scale);
			if (!t && (col+1) == nr_fields && types[col].type == CSV_NULL) {
				nr_fields--;
				extra_tsep = true;
			} else if (t) {
				list_append(typelist, t);
				sql_exp *ne = exp_column(sql->sa, tname, name, t, CARD_MULTI, 1, 0, 0);
				set_basecol(ne);
				ne->alias.label = -(sql->nid++);
				list_append(res_exps, ne);
			} else {
				GDKfree(types);
				return sa_message(sql->sa, "csv" "type %s not found\n", st);
			}
		} else {
			/* shouldn't be possible, we fallback to strings */
			GDKfree(types);
			return sa_message(sql->sa, "csv" "type unknown\n");
		}
	}
	GDKfree(types);
	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist;

	csv_t *r = (csv_t *)sa_alloc(sql->sa, sizeof(csv_t));
	r->sname[0] = 0;
	r->quote = q;
	r->delim = d;
	r->extra_tsep = extra_tsep;
	r->has_header = has_header;
	f->sname = (char*)r; /* pass schema++ */
	return NULL;
}

static void *
csv_load(void *BE, sql_subfunc *f, char *filename, sql_exp *topn)
{
	backend *be = (backend*)BE;
	mvc *sql = be->mvc;
	csv_t *r = (csv_t *)f->sname;
	sql_table *t = NULL;

	if (mvc_create_table( &t, be->mvc, be->mvc->session->tr->tmp/* misuse tmp schema */, f->tname /*gettable name*/, tt_table, false, SQL_DECLARED_TABLE, 0, 0, false) != LOG_OK)
		/* alloc error */
		return NULL;

	node *n, *nn = f->colnames->h, *tn = f->coltypes->h;
	for (n = f->res->h; n; n = n->next, nn = nn->next, tn = tn->next) {
		const char *name = nn->data;
		sql_subtype *tp = tn->data;
		sql_column *c = NULL;

		if (!tp || mvc_create_column(&c, be->mvc, t, name, tp) != LOG_OK) {
			return NULL;
		}
	}
	/* (res bats) := import(table T, 'delimit', '\n', 'quote', str:nil, fname, lng:nil, 0/1, 0, str:nil, int:nil, * int:nil ); */

	/* lookup copy_from */
	sql_subfunc *cf = sql_find_func(sql, "sys", "copyfrom", 14, F_UNION, true, NULL);
	cf->res = f->res;

	sql_subtype tpe;
	sql_find_subtype(&tpe, "varchar", 0, 0);
	char tsep[2], rsep[3], ssep[2];
	tsep[0] = r->delim;
	tsep[1] = 0;
	ssep[0] = r->quote;
	ssep[1] = 0;
	if (r->extra_tsep) {
		rsep[0] = r->delim;
		rsep[1] = '\n';
		rsep[2] = 0;
	} else {
		rsep[0] = '\n';
		rsep[1] = 0;
	}
	list *args = new_exp_list(sql->sa);

	append(args, exp_atom_ptr(sql->sa, t));
	append(args, exp_atom_str(sql->sa, tsep, &tpe));
	append(args, exp_atom_str(sql->sa, rsep, &tpe));
	append(args, exp_atom_str(sql->sa, ssep, &tpe));

	append(args, exp_atom_str(sql->sa, "", &tpe));
	append(args, exp_atom_str(sql->sa, filename, &tpe));
	append(args, topn ? topn: exp_atom_lng(sql->sa, -1));
	append(args, exp_atom_lng(sql->sa, r->has_header?2:1));

	append(args, exp_atom_int(sql->sa, 0));
	append(args, exp_atom_str(sql->sa, NULL, &tpe));
	append(args, exp_atom_int(sql->sa, 0));
	append(args, exp_atom_int(sql->sa, 0));

	append(args, exp_atom_str(sql->sa, ".", &tpe));
	append(args, exp_atom_str(sql->sa, NULL, &tpe));

	sql_exp *import = exp_op(sql->sa, args, cf);

	return exp_bin(be, import, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
}

static str
CSVprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;

	fl_register("csv", &csv_relation, &csv_load);
	fl_register("tsv", &csv_relation, &csv_load);
	fl_register("psv", &csv_relation, &csv_load);
	return MAL_SUCCEED;
}

static str
CSVepilogue(void *ret)
{
	fl_unregister("csv");
	fl_unregister("tsv");
	fl_unregister("psv");
	(void)ret;
	return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func csv_init_funcs[] = {
	pattern("csv", "prelude", CSVprelude, false, "", noargs),
	command("csv", "epilogue", CSVepilogue, false, "", noargs),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_csv_mal)
{ mal_module("csv", NULL, csv_init_funcs); }
