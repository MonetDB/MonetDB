/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_bin.h"
#include "rel_copy.h"
#include "mal_builder.h"
#include "opt_prelude.h"


static int
extract_parameter(backend *be, list *stmts, sql_exp *copyfrom, int argno)
{
	list *args = copyfrom->l;
	node *n = args->h;
	for (int i = 0; i < argno; i++)
		n = n->next;
	sql_exp *exp = n->data;
	stmt *st = exp_bin(be, exp, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
	list_append(stmts, st);
	return st->nr;
}

static int
emit_receive(MalBlkPtr mb, int var_channel, int tpe)
{
	InstrPtr q = newAssignment(mb);
	q = pushReturn(mb, q, var_channel);
	q = pushArgument(mb, q, var_channel);
	q = pushNil(mb, q, tpe);
	return getDestVar(q);
}

static void
emit_send(MalBlkPtr mb, int var_channel, int tpe, int var_msg)
{
	InstrPtr q = newAssignment(mb);
	setReturnArgument(q, var_channel);
	q = pushReturn(mb, q, var_msg);
	q = pushArgument(mb, q, var_msg);
	q = pushNil(mb, q, tpe);
}

stmt *
rel2bin_copyparpipe(backend *be, sql_rel *rel, list *refs, sql_exp *copyfrom)
{
	(void)rel;
	(void)refs;
	const int block_size = 1024 * 1024;
	const int margin = 8 * 1024;

	InstrPtr q;
	MalBlkPtr mb = be->mb;
	mvc *mvc = be->mvc;
	sql_allocator *sa = mvc->sa;
	list *intermediate_stmts = sa_list(sa);

	int streams_type = ATOMindex("streams");
	int bte_bat_type = newBatType(TYPE_bte);
	int int_bat_type = newBatType(TYPE_int);

	// Extract table name
	list *copyfrom_args = copyfrom->l;
	node *n = copyfrom_args->h;
	sql_exp *first_arg_exp = n->data;
	if (first_arg_exp->type != e_atom)
		return NULL;
	atom *first_arg_atom = first_arg_exp->l;
	sql_table *table = first_arg_atom->data.val.pval;
	const char *table_name = table->base.name;
	const char *schema_name = table->s->base.name;
	int column_count = ol_length(table->columns);

	// Extract other arguments
	int var_col_sep = extract_parameter(be, intermediate_stmts, copyfrom, 1);
	int var_line_sep = extract_parameter(be, intermediate_stmts, copyfrom, 2);
	int var_quote_char = extract_parameter(be, intermediate_stmts, copyfrom, 3);
	int var_null_representation = extract_parameter(be, intermediate_stmts, copyfrom, 4);
	int var_fname = extract_parameter(be, intermediate_stmts, copyfrom, 5);
	int var_num_rows = extract_parameter(be, intermediate_stmts, copyfrom, 6);
	int var_offset = extract_parameter(be, intermediate_stmts, copyfrom, 7);
	int var_best_effort = extract_parameter(be, intermediate_stmts, copyfrom, 8);
	int var_fixed_width = extract_parameter(be, intermediate_stmts, copyfrom, 9);
	int var_on_client = extract_parameter(be, intermediate_stmts, copyfrom, 10);
	int var_escape = extract_parameter(be, intermediate_stmts, copyfrom, 11);

	// coerce var_escape to bit
	q = newStmt(mb, "calc", "!=");
	q = pushArgument(mb, q, var_escape);
	q = pushInt(mb, q, 0);
	var_escape = getDestVar(q);


	// TODO: Deal with the following
	(void)var_num_rows;
	(void)var_offset;
	(void)var_best_effort;
	(void)var_on_client;
	(void)var_fixed_width;

	q = newAssignment(mb);
	q = pushLng(mb, q, 0);
	int var_total_row_count = getDestVar(q);

	q = newStmt(mb, "streams", "openRead");
	q = pushArgument(mb, q, var_fname);
	int var_stream_channel = getDestVar(q);

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_bte);
	q = pushLng(mb, q, block_size + margin);
	int var_block_channel = getDestVar(q);

	q = newAssignment(mb);
	q = pushInt(mb, q, 0);
	int var_skip_amounts_channel = getDestVar(q);

	q = newAssignment(mb);
	q = pushNil(mb, q, TYPE_bit);
	int var_claim_channel = getDestVar(q);


	// START LOOP
	q = newAssignment(mb);
	q->barrier = BARRIERsymbol;
	q = pushBit(mb, q, true);
	int var_loop_barrier = getDestVar(q);

	int var_s = emit_receive(mb, var_stream_channel, streams_type);

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_bte);
	q = pushLng(mb, q, 300);
	int var_next_block = getDestVar(q);

	// START READ BLOCK
	q = newStmt(mb, "calc", "isnotnil");
	q->barrier = BARRIERsymbol;
	q = pushArgument(mb, q, var_s);
	int var_read_barrier = getDestVar(q);

	q = newStmt(mb, "copy", "read");
	q = pushArgument(mb, q, var_s);
	q = pushLng(mb, q, block_size);
	q = pushArgument(mb, q, var_next_block);
	int var_nread = getDestVar(q);

	q = newStmt(mb, "calc", ">");
	q->barrier = LEAVEsymbol;
	setReturnArgument(q, var_read_barrier);
	q = pushArgument(mb, q, var_nread);
	q = pushLng(mb, q, 0);

	q = newStmt(mb, "streams", "close");
	q = pushArgument(mb, q, var_s);

	q = newAssignment(mb);
	setReturnArgument(q, var_s);
	q = pushNil(mb, q, streams_type);

	// END READ BLOCK
	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	getDestVar(q) = var_read_barrier;

	emit_send(mb, var_stream_channel, streams_type, var_s);

	int var_our_block = emit_receive(mb, var_block_channel, bte_bat_type);
	int var_our_skip_amount = emit_receive(mb, var_skip_amounts_channel, TYPE_int);

	q = newStmt(mb, "aggr", "count");
	q = pushArgument(mb, q, var_our_block);
	int var_our_count = getDestVar(q);

	q = newStmt(mb, "aggr", "count");
	q = pushArgument(mb, q, var_next_block);
	int var_next_count = getDestVar(q);

	q = newStmt(mb, "calc", "+");
	q = pushArgument(mb, q, var_our_count);
	q = pushArgument(mb, q, var_next_count);
	int var_total_count = getDestVar(q);

	q = newStmt(mb, "calc", "==");
	q->barrier = LEAVEsymbol;
	setReturnArgument(q, var_loop_barrier);
	q = pushArgument(mb, q, var_total_count);
	q = pushLng(mb, q, 0);

	q = newStmt(mb, "copy", "fixlines");
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_int));
	q = pushArgument(mb, q, var_our_block);
	q = pushArgument(mb, q, var_our_skip_amount);
	q = pushArgument(mb, q, var_next_block);
	q = pushArgument(mb, q, var_line_sep);
	q = pushArgument(mb, q, var_quote_char);
	q = pushArgument(mb, q, var_escape);
	int var_our_line_count = getArg(q, 0);
	int var_next_skip_amount = getArg(q, 1);

	emit_send(mb, var_block_channel, bte_bat_type, var_next_block);
	emit_send(mb, var_skip_amounts_channel, TYPE_int, var_next_skip_amount);

	int var_claim_token = emit_receive(mb, var_claim_channel, TYPE_bit);

	q = newStmt(mb, "sql", "claim");
	q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_oid)));
	q = pushArgument(mb, q, be->mvc_var);
	q = pushStr(mb, q, schema_name);
	q = pushStr(mb, q, table_name);
	q = pushArgument(mb, q, var_our_line_count);
	int var_position = getArg(q, 0);
	int var_positions = getArg(q, 1);

	emit_send(mb, var_claim_channel, TYPE_bit, var_claim_token);

	//
	q = newStmt(mb, "calc", "==");
	q->barrier = REDOsymbol;
	getDestVar(q) = var_loop_barrier;
	q = pushArgument(mb, q, var_our_line_count);
	q = pushLng(mb, q, 0);

	assert(column_count > 0);
	q = newStmt(mb, "copy", "splitlines");
	setDestType(mb, q, int_bat_type);
	for (int i = 1; i < column_count; i++) {
		int v = newTmpVariable(mb, int_bat_type);
		q = pushReturn(mb, q, v);
	}
	q = pushArgument(mb, q, var_our_block);
	q = pushArgument(mb, q, var_our_skip_amount);
	q = pushArgument(mb, q, var_our_line_count);
	q = pushArgument(mb, q, var_col_sep);
	q = pushArgument(mb, q, var_line_sep);
	q = pushArgument(mb, q, var_quote_char);
	q = pushArgument(mb, q, var_null_representation);
	q = pushArgument(mb, q, var_escape);
	InstrPtr splitlines_instr = q;

	int i = 0;
	for (node *n = table->columns->l->h; n != NULL; n = n->next) {
		int var_indices = getArg(splitlines_instr, i++);

		sql_column *col = n->data;
		sql_type *type = col->type.type;
		const char *column_name = col->base.name;

		switch (type->eclass) {
			case EC_DEC:
				q = newStmt(mb, "copy", "parse_decimal");
				q = pushArgument(mb, q, var_our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushInt(mb, q, col->type.digits);
				q = pushInt(mb, q, col->type.scale);
				break;
			default:
				q = newStmt(mb, "copy", "parse_generic");
				q = pushArgument(mb, q, var_our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, col->type.type->localtype);
				break;
		}
		int var_converted = getDestVar(q);

		q = newStmt(mb, "sql", "append");
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, schema_name);
		q = pushStr(mb, q, table_name);
		q = pushStr(mb, q, column_name);
		q = pushArgument(mb, q, var_position);
		q = pushArgument(mb, q, var_positions);
		q = pushArgument(mb, q, var_converted);
	}

	q = newStmt(mb, "calc", "+");
	setDestVar(q, var_total_row_count);
	q = pushArgument(mb, q, var_total_row_count);
	q = pushArgument(mb, q, var_our_line_count);


	// END LOOP
	q = newAssignment(mb);
	q->barrier = REDOsymbol;
	pushBit(mb, q, true);
	getDestVar(q) = var_loop_barrier;


	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	getDestVar(q) = var_loop_barrier;

	add_to_rowcount_accumulator(be, var_total_row_count);
	// dump_code(-1);


	// I'm assuming that attaching the stmt list to op4.lval
	// will make some else free the arg_stmt's we created here.
	stmt *dummy_stmt = stmt_none(be);
	dummy_stmt->op4.lval = intermediate_stmts;
	return dummy_stmt;
}
