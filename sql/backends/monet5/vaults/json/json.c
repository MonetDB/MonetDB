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


static str
json_relation(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname)
{
	(void) sql;
	(void) f;
	(void) res_exps;
	f->tname = tname;
	allocator *sa = sa_create(NULL);
	JSONFileHandle *jfh = json_open(filename, sa);
	const char* json_str = NULL;
	if (jfh) {
		json_str = read_json_file(jfh);
		json_close(jfh);
	}
	JSON *jt = JSONparse(json_str);
	if (jt) {

	}
	JSONfree(jt);
	sa_destroy(sa);
	return MAL_SUCCEED;
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

