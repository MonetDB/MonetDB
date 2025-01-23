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

#include <unistd.h>
#include <glob.h>


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
	int fd = open(fname, O_RDONLY);
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
		size_t length = jfh->size;
		content = sa_zalloc(jfh->sa, length + 1);
		if (content) {
			read(jfh->fd, content, length);
			content[length + 1] = '\0';
		}
	}
	return content;
}

typedef struct jobject {
	struct jobject *parent;
	list *fields;
} jobject;

static jobject *
new_jobject(allocator *sa, jobject *parent)
{
	jobject *res = SA_NEW(sa, jobject);
	res->parent = parent;
	res->fields = sa_list(sa);
	return res;
}

static size_t
json2subtypes(mvc *sql, JSONterm *t, sql_alias **alias_pptr, list *types, list *exps, list *names, jobject **parent_pptr)
{
	sql_subtype *tpe = NULL;
	sql_exp *ne = NULL;
	sql_alias *nalias = NULL;
	const char *cname = NULL;
	size_t offset = 1;
	jobject *parent = *parent_pptr;
	sql_alias *alias = *alias_pptr;

	switch(t->kind) {
		case JSON_ARRAY:
			tpe = sql_create_subtype(sql->sa, SA_ZNEW(sql->sa, sql_type), 0, 0);
			tpe->type->composite = true;
			tpe->multiset = MS_ARRAY;
			cname = alias->name;
			// append to parent fields
			if (parent && parent->fields) {
				sql_arg *field = sql_create_arg(sql->sa, cname, tpe, false); // ?inout
				list_append(parent->fields, field);
			}
			ne = exp_column(sql->sa, alias, cname, tpe, CARD_MULTI, 1, 0, 0);
			list_append(types, tpe);
			list_append(exps , ne);
			list_append(names, (char*)cname);
			nalias = &ne->alias;
			*alias_pptr = nalias;
			offset += json2subtypes(sql, t+1, alias_pptr, types, exps, names, parent_pptr);
			break;
		case JSON_OBJECT:
			tpe = sql_create_subtype(sql->sa, SA_ZNEW(sql->sa, sql_type), 0, 0);
			tpe->type->composite = true;
			// new object
			jobject *jo = new_jobject(sql->sa, parent);
			tpe->type->d.fields = jo->fields;
			cname = alias->name;
			if (parent) {
				sql_arg *field = sql_create_arg(sql->sa, cname, tpe, false); // ?inout
				list_append(parent->fields, field);
			}
			ne = exp_column(sql->sa, alias, cname, tpe, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			list_append(exps, ne);
			list_append(types, tpe);
			list_append(names, (char*)cname);
			nalias = &ne->alias;
			*alias_pptr = nalias;
			*parent_pptr = jo;
			offset += json2subtypes(sql, t+1, alias_pptr, types, exps, names, parent_pptr);
			break;
		case JSON_ELEMENT:
			cname = sa_strndup(sql->sa, t->value, t->valuelen);
			nalias = a_create(sql->sa, cname);
			nalias->parent = alias;
			*alias_pptr = nalias;
			offset += json2subtypes(sql, t+1, alias_pptr, types, exps, names, parent_pptr);
			break;
		case JSON_STRING:
			tpe = sql_bind_localtype("str");
			cname = alias->name;
			// append to parent fields
			if (parent) {
				sql_arg *field = sql_create_arg(sql->sa, cname, tpe, false); // ?inout
				list_append(parent->fields, field);
			}
			ne = exp_column(sql->sa, alias->parent, cname, tpe, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			list_append(exps, ne);
			list_append(types, tpe);
			list_append(names, (char*)cname);
			// adjust one level
			*alias_pptr = alias->parent;
			offset += json2subtypes(sql, t+1, alias_pptr, types, exps, names, parent_pptr);
			break;
		case JSON_NUMBER:
			tpe = sql_bind_localtype("int");
			cname = alias->name;
			// append to parent fields
			if (parent) {
				sql_arg *field = sql_create_arg(sql->sa, cname, tpe, false); // ?inout
				list_append(parent->fields, field);
			}
			ne = exp_column(sql->sa, alias->parent, cname, tpe, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			list_append(exps, ne);
			list_append(types, tpe);
			list_append(names, (char*)cname);
			// adjust one level
			*alias_pptr = alias->parent;
			offset += json2subtypes(sql, t+1, alias_pptr, types, exps, names, parent_pptr);
			break;
		case JSON_VALUE:
			// break one level
			if (parent)
				*parent_pptr = parent->parent;
			if (alias)
				*alias_pptr = alias->parent;
			break;
		default:
			// error ?
			offset = 0;
			break;
	}
	return offset;
}


static str
json_relation(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname)
{
	char *res = MAL_SUCCEED;
	f->tname = tname;
	allocator *sa = sa_create(NULL);
	JSONFileHandle *jfh = json_open(filename, sa);
	const char* json_str = NULL;
	if (jfh) {
		json_str = read_json_file(jfh);
		json_close(jfh);
	}
	JSON *jt = JSONparse(json_str); // should take allocator
	if (jt && jt->error == NULL) {
		list *types = sa_list(sql->sa);
		list *names = sa_list(sql->sa);
		size_t offset = 0;
		sql_alias *alias = a_create(sql->sa, tname);
		jobject *jo = NULL;
		do {
			size_t prev_offset = offset;
			offset += json2subtypes(sql, jt->elm + offset, &alias, types, res_exps, names, &jo);
			if (offset == prev_offset) {
				res = createException(SQL, SQLSTATE(42000), "json" "json2subtypes failure for %s", filename);
				break;
			}
		} while(offset < (size_t)jt->free);
		JSONfree(jt);
		if (res == MAL_SUCCEED) {
			f->res = types;
			f->coltypes = types;
			f->colnames = names;
		}
	} else {
		res = jt ? jt->error : createException(SQL, SQLSTATE(42000), "json" "Failure parsing %s", filename);
	}
	sa_destroy(sa);
	return res;
}


static void *
json_load(void *BE, sql_subfunc *f, char *filename, sql_exp *topn)
{
	(void) f;
	(void) topn;
	backend *be = BE;
	stmt *s = stmt_none(be);
	InstrPtr q = newStmt(be->mb, "json", "read_json");
	q = pushStr(be->mb, q, filename);
	pushInstruction(be->mb, q);
	s->nr = getDestVar(q);
	//s->nrcols = 1;
	s->q = q;
	//s->op4.typeval = *st;
	//s = stmt_alias(be, s, i+1, "bla", column_name);
	return s;
}

static str
JSONprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;

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
	if (jfh) {
		json_str = read_json_file(jfh);
		json_close(jfh);
	}
	JSON *jt = JSONparse(json_str);
	// TODO do something
	JSONfree(jt);
	sa_destroy(sa);
	return msg;
}

#include "mel.h"

static mel_func json_init_funcs[] = {
	pattern("json", "prelude", JSONprelude, false, "", noargs),
	command("json", "epilogue", JSONepilogue, false, "", noargs),
	pattern("json", "read_json", JSONread_json, false, "Reads json file into table", args(1,2, batvarargany("t",0), arg("filename", str))),
{ .imp=NULL }
};

#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_json_mal)
{ mal_module("json", NULL, json_init_funcs); }

