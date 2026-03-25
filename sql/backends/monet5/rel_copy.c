/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"

#include "rel_bin.h"
#include "rel_copy.h"
#include "mal_builder.h"
#include "sql_pp_statement.h"
#include "bin_partition.h"
#include "sql_scenario.h"

static int
get_copy_blocksize(void) {
	int size = GDKgetenv_int(COPY_BLOCKSIZE_SETTING, -1);
	return size > 0 ? size : DEFAULT_COPY_BLOCKSIZE;
}

static int
allocation_size(int blocksize)
{
	return blocksize;
}

static ValPtr
take_parameter(node **n, int type)
{
	node *node = *n;
	assert(n != NULL);
	if (node == NULL)
		return NULL;

	*n = node->next;

	sql_exp *e = node->data;
	assert(e != NULL);
	if (e == NULL)
		return NULL;

	atom *a = e->l;
	assert(a != NULL);
	if (a == NULL)
		return NULL;

	int atyp = atom_type(a)->type->localtype;
	assert(atyp == type);
	if (atyp != type)
		return NULL;

	return &a->data;
}

static int
emit_pipelined_loop(
	backend *be, MalBlkPtr mb,
	str fname, int onclient, int block_size,
	int var_col_sep, int var_line_sep, int var_quote_char, str null_representation,
	bool escape, str fixed_width, bool best_effort, lng offset, lng nrecords_or_minusone)
{
	InstrPtr q;
	int alloc = allocation_size(block_size);
    int streams_type = ATOMindex("streams");

	// Determine the number of records to read
	int var_nrecords = getLngConstant(mb, nrecords_or_minusone >= 0 ? nrecords_or_minusone : GDK_lng_max);

	// Open the input
	int var_stream = newTmpVariable(mb, streams_type);
	if (onclient) {
		// ON CLIENT
		q = newStmt(mb, "copy", "request_upload");
		setDestVar(q, var_stream);
		q = pushStr(mb, q, fname);
		q = pushInt(mb, q, onclient);
	} else if (fname != NULL) {
		// ON SERVER
		q = newStmt(mb, "streams", "openRead");
		setDestVar(q, var_stream);
		q = pushStr(mb, q, fname);
	} else {
		// FROM STDIN
		q = newStmt(mb, "copy", "from_stdin");
		setDestVar(q, var_stream);
		q = pushLng(mb, q, offset);
		q = pushArgument(mb, q, var_nrecords);
		q = pushBit(mb, q, nrecords_or_minusone < 0);
		q = pushArgument(mb, q, var_line_sep);
		q = pushArgument(mb, q, var_quote_char);
		q = pushBit(mb, q, escape);

		// offset has been handled
		offset = 0;
	}
	pushInstruction(mb, q);
	if (be->pp)
		moveInstruction(be->mb, be->mb->stop-1, be->pp_pc++);

	q = newStmt(mb, "copy", "new");
	q = pushArgument(mb, q, var_stream);
	q = pushLng(mb, q, offset);
	q = pushArgument(mb, q, var_nrecords);
	q = pushLng(mb, q, alloc);
	q = pushArgument(mb, q, var_col_sep);
	q = pushArgument(mb, q, var_line_sep);
	q = pushArgument(mb, q, var_quote_char);
	q = pushStr(mb, q, null_representation);
	q = pushBit(mb, q, escape);
	if (fixed_width == NULL)
		fixed_width = (str)str_nil;
	q = pushStr(mb, q, fixed_width);
	q = pushBit(mb, q, best_effort);
	pushInstruction(mb, q);
	int our_block = getDestVar(q);

	if (be->pp) {
		stmt_concat_add_source(be);
	} else {
		pp_cleanup(be, our_block); /* cleanup at end of pipeline block */
		// START LOOP
		set_pipeline(be, stmt_pp_start_generator(be, our_block, false));
		be->need_pipeline = false;
	}
	return our_block;
}

stmt *
exp2bin_copyparpipe(backend *be, sql_exp *copyfrom)
{
	const int block_size = get_copy_blocksize();
	InstrPtr q;
	MalBlkPtr mb = be->mb;
	mvc *mvc = be->mvc;
	allocator *sa = mvc->sa;

	list *intermediate_stmts = sa_list(sa);
	int int_bat_type = newBatType(TYPE_int);

	// Extract parameters
	list *copyfrom_args = copyfrom->l;
	node *n = copyfrom_args->h;

	sql_table *table = take_parameter(&n, TYPE_ptr)->val.pval;
	const char *table_name = table->base.name;
	int column_count = ol_length(table->columns);

	str col_sep = take_parameter(&n, TYPE_str)->val.sval;
	str line_sep = take_parameter(&n, TYPE_str)->val.sval;
	str quote_char = take_parameter(&n, TYPE_str)->val.sval;
	str null_representation = take_parameter(&n, TYPE_str)->val.sval;
	str fname = take_parameter(&n, TYPE_str)->val.sval;
	lng num_rows = take_parameter(&n, TYPE_lng)->val.lval;
	lng offset = take_parameter(&n, TYPE_lng)->val.lval;
	bool best_effort = 0 != take_parameter(&n, TYPE_int)->val.fval;
	str fixed_width = take_parameter(&n, TYPE_str)->val.sval;
	int on_client = take_parameter(&n, TYPE_int)->val.ival;
	bool escape = 0 != take_parameter(&n, TYPE_int)->val.ival;
	str dec_sep = take_parameter(&n, TYPE_str)->val.sval;
	str dec_skip = take_parameter(&n, TYPE_str)->val.sval;
	if (dec_sep == NULL)
		dec_sep = (str)str_nil;
	if (dec_skip == NULL)
		dec_skip = (str)str_nil;

	int var_col_sep = getStrConstant(mb, col_sep);
	int var_line_sep = getStrConstant(mb, line_sep);
	int var_quote_char = getStrConstant(mb, quote_char ? quote_char : (char*)str_nil);
	// convert offset to 0-based
	if (offset > 0)
		offset--;

	int our_block = emit_pipelined_loop(be, mb, fname, on_client, block_size, var_col_sep, var_line_sep, var_quote_char, null_representation, escape, fixed_width, best_effort, offset, num_rows);

	// Initialize the failures bat if BEST EFFORT is on
	int rows = newTmpVariable(mb, newBatType(TYPE_oid));
	if (best_effort) {
		q = newStmt(mb, "copy", "rows");
		setDestVar(q, rows);
	} else {
		q = newAssignment(mb);
		setDestVar(q, rows);
		pushNil(mb, q, newBatType(TYPE_oid));
	}
	pushInstruction(mb, q);
	stmt *rows_stmt = stmt_none(be);
	if (!rows_stmt)
		return NULL;
	rows_stmt->nr = getArg(q, 0);
	rows_stmt->nrcols = 1;
	rows_stmt->q = q;

	assert(column_count > 0);
	q = newStmt(mb, "copy", "splitlines");
	setDestType(mb, q, int_bat_type);
	for (int i = 1; i < column_count; i++) {
		int v = newTmpVariable(mb, int_bat_type);
		q = pushReturn(mb, q, v);
	}
	q = pushArgument(mb, q, our_block);
	q = pushArgument(mb, q, rows);
	q = pushArgument(mb, q, be->pipeline);
	pushInstruction(mb, q);

	InstrPtr splitlines_instr = q;

	int i = 0;
	for (node *n = table->columns->l->h; n; n = n->next, i++) {
		int var_indices = getArg(splitlines_instr, i);

		sql_column *col = n->data;
		if (col->base.name[0] == '%')
			continue;
		sql_type *type = col->type.type;
		const char *column_name = col->base.name;
		unsigned int digits = col->type.digits;
		unsigned int scale = col->type.scale;
		int localtype = col->type.type->localtype;
		int scale_extra = 1;

		if (type->eclass == EC_MONTH || type->eclass == EC_NUM) {
				q = newStmtArgs(mb, "copy", "parse_integer", 12);
				q = pushArgument(mb, q, our_block);
				q = pushArgument(mb, q, be->pipeline);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, rows);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				pushInstruction(mb, q);
		} else if (type->eclass == EC_DEC || type->eclass == EC_SEC) {
				if (type->eclass == EC_SEC)
					scale += 3;
				if (type->eclass == EC_SEC && digits < 13)
					digits = 18;
				q = newStmt(mb, "copy", "parse_decimal");
				q = pushArgument(mb, q, our_block);
				q = pushArgument(mb, q, be->pipeline);
				q = pushArgument(mb, q, var_indices);
				q = pushInt(mb, q, digits);
				q = pushInt(mb, q, scale);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, rows);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				q = pushStr(mb, q, dec_sep);
				q = pushStr(mb, q, dec_skip);
				pushInstruction(mb, q);

				if (scale_extra != 1) {
					int ints = getDestVar(q);
					q = newStmt(mb, "copy", "scale");
					q = pushArgument(mb, q, ints);
					q = pushInt(mb, q, scale_extra);
					q = pushArgument(mb, q, rows);
					q = pushInt(mb, q, i);
					q = pushStr(mb, q, col->base.name);
					pushInstruction(mb, q);
				}
		} else if (type->eclass == EC_FLT) {
				q = newStmt(mb, "copy", "parse_float");
				q = pushArgument(mb, q, our_block);
				q = pushArgument(mb, q, be->pipeline);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, rows);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				q = pushStr(mb, q, dec_sep);
				q = pushStr(mb, q, dec_skip);
				pushInstruction(mb, q);

		} else if (type->eclass == EC_STRING) {
				q = newStmt(mb, "copy", "parse_string");
				q = pushArgument(mb, q, our_block);
				q = pushArgument(mb, q, be->pipeline);
				q = pushArgument(mb, q, var_indices);
				q = pushInt(mb, q, digits);
				q = pushArgument(mb, q, rows);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				pushInstruction(mb, q);
		} else {
				q = newStmt(mb, "copy", "parse_generic");
				q = pushArgument(mb, q, our_block);
				q = pushArgument(mb, q, be->pipeline);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, rows);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				pushInstruction(mb, q);
		}
		stmt *s = stmt_none(be);
		s->nr = getArg(q, 0);
		s->nrcols = 1;
		s->q = q;
		s->op4.typeval = col->type;
		s = stmt_alias(be, s, i+1, table_name, column_name);
		list_append(intermediate_stmts, s);
	}
	if (best_effort) {
		for (node *n = intermediate_stmts->h; n; n = n->next)
			n->data = stmt_project(be, rows_stmt, n->data);
	}
	return stmt_list(be, intermediate_stmts);
}
