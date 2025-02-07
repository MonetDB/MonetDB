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
#include "json.h"
#include "mutils.h"

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include <unistd.h>
// #include <glob.h> // not available on Windows


typedef struct JSONFileHandle {
	allocator *sa;
	char *filename;
	int fd;
	size_t size;
} JSONFileHandle;


static JSONFileHandle *
json_open(const char *fname, allocator *sa)
{
	if (!sa)
		return NULL;
	int fd = MT_open(fname, O_RDONLY);
	if (fd < 0){
		// TODO add relevant trace component
		TRC_ERROR(SQL_EXECUTION, "Error opening file %s", fname);
		return NULL;
	}
	struct stat stb;
    if (MT_stat(fname, &stb) != 0) {
		TRC_ERROR(SQL_EXECUTION, "Error stat file %s", fname);
		close(fd);
		return NULL;
	}
	JSONFileHandle *res = sa_alloc(sa, sizeof(JSONFileHandle));
	res->sa = sa;
	res->filename = sa_strdup(sa, fname);
	res->fd = fd;
	res->size = stb.st_size;
	return res;
}

static void
json_close(JSONFileHandle *jfh)
{
	if (jfh && jfh->fd)
		close(jfh->fd);
}


static char *
read_json_file(JSONFileHandle *jfh)
{
	char *content = NULL;
	if (jfh) {
		unsigned int length = (unsigned int)jfh->size;
		content = sa_zalloc(jfh->sa, length + 1);
		if (content) {
			ssize_t nbytes = read(jfh->fd, content, length);
			if (nbytes < 0)
				return NULL;
			content[length + 1] = '\0';
		}
	}
	return content;
}


static size_t
append_terms(allocator *sa, JSON *jt, size_t offset, BAT *b, char **error)
{
	JSONterm *t = jt->elm + offset;
	char *v = NULL;
	JSONterm *prev = offset > 0 ? (jt->elm + (offset - 1)) : NULL;
	JSONterm *next = offset < (size_t)jt->free ? jt->elm + (offset + 1): NULL;
	switch(t->kind) {
		case JSON_ARRAY:
			if ( (prev == NULL && next && next->kind > JSON_ARRAY)
				   	|| (prev && prev->kind == JSON_ARRAY) ) {
				// array of basic types or array of arrays
				v = sa_strndup(sa, t->value, t->valuelen);
				size_t depth = 0;
				do {
					offset += 1;
					next = offset < (size_t)jt->free ? jt->elm + offset : NULL;
					if (next && next->kind <=JSON_ARRAY)
						depth ++;
					if ((depth > 0 && next && (next->kind == JSON_VALUE || next->kind == 0))
							|| (depth > 0 && next == NULL))
						depth --;
				} while((next && next->kind != JSON_VALUE) || depth > 0);
			} else {
				offset += 1;
			}
			break;
		case JSON_OBJECT:
			v = sa_strndup(sa, t->value, t->valuelen);
			size_t depth = 0;
			do {
				offset += 1;
				next = offset < (size_t)jt->free ? jt->elm + offset : NULL;
				if (next && next->kind <=JSON_ARRAY)
					depth ++;
				if ((depth > 0 && next && (next->kind == JSON_VALUE || next->kind == 0))
					   	|| (depth > 0 && next == NULL))
					depth --;
			} while((next && next->kind != JSON_VALUE) || depth > 0);
			break;
		case JSON_ELEMENT:
		case JSON_STRING:
		case JSON_NUMBER:
			// should not happen
			assert(0);
			break;
		case JSON_VALUE:
			offset +=1;
			break;
		default:
			*error = createException(SQL, "json.append_terms", "unknown json term");
			break;
	}
	if (v) {
		if (BUNappend(b, v, false) != GDK_SUCCEED) {
			*error = createException(SQL, "json.append_terms", "BUNappend failed!");
		}
	}
	return offset;
}


static str
json_relation(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname)
{
	(void) filename;
	char *res = MAL_SUCCEED;
	list *types = sa_list(sql->sa);
	list *names = sa_list(sql->sa);
	// use file name as columnn name ?
	char *cname = sa_strdup(sql->sa, "json");
	list_append(names, cname);
	sql_schema *jsons = mvc_bind_schema(sql, "sys");
	if (!jsons)
		return NULL;
	sql_subtype *st = SA_NEW(sql->sa, sql_subtype);
	st->digits = st->scale = 0;
	st->multiset = 0;
	st->type = schema_bind_type(sql, jsons, "json");
	list_append(types, st);
	sql_exp *ne = exp_column(sql->sa, a_create(sql->sa, tname), cname, st, CARD_MULTI, 1, 0, 0);
	set_basecol(ne);
	ne->alias.label = -(sql->nid++);
	list_append(res_exps, ne);
	f->tname = tname;
	f->res = types;
	f->coltypes = types;
	f->colnames = names;
	return res;
}


static void *
json_load(void *BE, sql_subfunc *f, char *filename, sql_exp *topn)
{
	(void) topn; // TODO include topn
	backend *be = BE;
	allocator *sa = be->mvc->sa;
	sql_subtype *tpe = f->res->h->data;
	const char *tname = f->tname;
	const char *cname = f->colnames->h->data;

	stmt *s = stmt_none(be);
	InstrPtr q = newStmt(be->mb, "json", "read_json");
	q = pushStr(be->mb, q, filename);
	pushInstruction(be->mb, q);
	s->nr = getDestVar(q);
	s->q = q;
	s->nrcols = 1;
	s->op4.typeval = *tpe;
	// is alias essential here?
	s = stmt_alias(be, s, 1, a_create(sa, tname), cname);
	return s;
}

int TYPE_json;

static str
JSONprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;
	TYPE_json = ATOMindex("json");

	fl_register("json", &json_relation, &json_load);
	return MAL_SUCCEED;
}

static str
JSONepilogue(void *ret)
{
	fl_unregister("json");
	(void)ret;
	return MAL_SUCCEED;
}


static str
JSONread_json(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt; (void) mb;
	char *msg = MAL_SUCCEED;
	char *fname = *(str*)getArgReference(stk, pci, pci->retc);
	allocator *sa = sa_create(NULL);
	JSONFileHandle *jfh = json_open(fname, sa);
	const char* json_str = NULL;
	JSON *jt = NULL;
	BAT *b = NULL;
	if (!jfh) {
		sa_destroy(sa);
		msg = createException(SQL, "json.read_json", "Failed to open file %s", fname);
		return msg;
	}
	json_str = read_json_file(jfh);
	json_close(jfh);
	if (json_str)
		jt = JSONparse(json_str);
	if (jt) {
		if (jt->error == NULL) {
			b = COLnew(0, TYPE_json, 0, TRANSIENT);
			size_t offset = 0;
			char *error = NULL;
			// append terms
			do {
				offset = append_terms(sa, jt, offset, b, &error);
				if (error) {
					msg = error;
					break;
				}
			} while(offset < (size_t)jt->free);
			if (msg == MAL_SUCCEED) {
				bat *res = getArgReference_bat(stk, pci, 0);
				*res = b->batCacheid;
				BBPkeepref(b);
			} else
				BBPreclaim(b);
		} else {
			msg = jt->error;
		}
		JSONfree(jt);
	} else {
		msg = createException(SQL, "json.read_json", "JSONparse error");
	}
	sa_destroy(sa);
	return msg;
}

#include "mel.h"

static mel_func json_init_funcs[] = {
	pattern("json", "prelude", JSONprelude, false, "", noargs),
	command("json", "epilogue", JSONepilogue, false, "", noargs),
	pattern("json", "read_json", JSONread_json, false, "Reads json file into a table", args(1,2, batarg("", json), arg("filename", str))),
{ .imp=NULL }
};

#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_json_mal)
{ mal_module("json", NULL, json_init_funcs); }

