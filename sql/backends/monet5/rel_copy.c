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
#include "sql_scenario.h"

void dump_code(int);

int
parallel_copy_level(void)
{
	if (!SQLrunning)
		return 0;
	int level = GDKgetenv_int(COPY_PARALLEL_SETTING, int_nil);
	if (is_int_nil(level))
		level = 1;
	return level;
}

static int
get_copy_blocksize(void) {
	int size = GDKgetenv_int(COPY_BLOCKSIZE_SETTING, -1);
	return size > 0 ? size : DEFAULT_COPY_BLOCKSIZE;
}

static int
allocation_size(int blocksize)
{
	int alt;
	int size;

	size = blocksize + blocksize / 8;

	alt = blocksize + 4096;
	size = size < alt ? alt : size;

	alt = 8192;
	size = size < alt ? alt : size;

	return size;
}

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

static InstrPtr
emit_channel(MalBlkPtr mb, int var_initial_value)
{
	InstrPtr q = newStmt(mb, "pipeline", "channel");
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_int));
	q = pushArgument(mb, q, var_initial_value);
	return q;
}

static int
emit_receive(MalBlkPtr mb, int var_handle, InstrPtr channel_stmt)
{
	assert(0 == strcmp("pipeline", channel_stmt->modname));
	assert(0 == strcmp("channel", channel_stmt->fcnname));
	int var_mailbox = getArg(channel_stmt, 0);
	int var_channel = getArg(channel_stmt, 1);

	InstrPtr q = newStmt(mb, "pipeline", "recv");
	q = pushArgument(mb, q, var_handle);
	q = pushArgument(mb, q, var_mailbox);
	q = pushArgument(mb, q, var_channel);
	return getDestVar(q);
}

static void
emit_send(MalBlkPtr mb, int var_handle, InstrPtr channel_stmt, int var_value)
{
	assert(0 == strcmp("pipeline", channel_stmt->modname));
	assert(0 == strcmp("channel", channel_stmt->fcnname));
	int var_mailbox = getArg(channel_stmt, 0);
	int var_channel = getArg(channel_stmt, 1);

	InstrPtr q = newStmt(mb, "pipeline", "send");
	q = pushArgument(mb, q, var_handle);
	q = pushArgument(mb, q, var_mailbox);
	q = pushArgument(mb, q, var_channel);
	q = pushArgument(mb, q, var_value);
}


struct loop_vars {
	int loop_barrier;
	int loop_iter;
	int loop_handle;
	int our_block;
	int earlier_line_count;
	int our_line_count;
};


static void
emit_onserver_loop(
	MalBlkPtr mb, struct loop_vars *loop_vars,
	int var_fname, int block_size,
	int var_line_sep, int var_quote_char, int var_escape, int var_offset, int var_nrecords)
{
	(void)var_offset;
	InstrPtr q;
	int bte_bat_type = newBatType(TYPE_bte);
	int alloc = allocation_size(block_size);

	// set up the stream channel
	q = newStmt(mb, "streams", "openRead");
	q = pushArgument(mb, q, var_fname);
	int var_stream = getDestVar(q);

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_bte);
	q = pushLng(mb, q, alloc);
	int var_block = getDestVar(q);

	// emit the offset handling
	q = newStmt(mb, "calc", ">");
	q->barrier = BARRIERsymbol;
	q = pushArgument(mb, q, var_offset);
	q = pushLng(mb, q, 0);
	int offset_handling = getDestVar(q);

	q = newStmt(mb, "calc", "isnil");
	q->barrier = LEAVEsymbol;
	q = pushArgument(mb, q, var_stream);
	setDestVar(q, offset_handling);

	q = newStmt(mb, "copy", "read");
	q = pushArgument(mb, q, var_stream);
	q = pushLng(mb, q, block_size);
	q = pushArgument(mb, q, var_block);
	setDestVar(q, var_stream);

	q = newStmt(mb, "copy", "skiplines");
	q = pushArgument(mb, q, var_block);
	q = pushArgument(mb, q, var_offset);
	setDestVar(q, var_offset);

	q = newStmt(mb, "calc", ">");
	q->barrier = REDOsymbol;
	q = pushArgument(mb, q, var_offset);
	q = pushLng(mb, q, 0);
	setDestVar(q, offset_handling);

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	setDestVar(q, offset_handling);

	q = newAssignment(mb);
	q = pushLng(mb, q, 0);
	int var_initial_line_count = getDestVar(q);

	// set up the channels
	InstrPtr stream_channel_stmt = emit_channel(mb, var_stream);
	InstrPtr block_channel_stmt = emit_channel(mb, var_block);
	InstrPtr line_count_stmt = emit_channel(mb, var_initial_line_count);


	// START LOOP
	q = newStmt(mb, languageRef, pipelinesRef);
	q->barrier = BARRIERsymbol;
	setArgType(mb, q, 0, TYPE_bit);
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_int));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_ptr));
	q = pushInt(mb, q, -1);
	// int var_iter_id = getArg(q, 1);
	loop_vars->loop_barrier = getArg(q, 0);
	loop_vars->loop_iter = getArg(q, 1);
	loop_vars->loop_handle = getArg(q, 2);

	int var_s = emit_receive(mb, loop_vars->loop_handle, stream_channel_stmt);

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_bte);
	q = pushLng(mb, q, alloc);
	int var_next_block = getDestVar(q);

	q = newStmt(mb, "copy", "read");
	q = pushArgument(mb, q, var_s);
	q = pushLng(mb, q, block_size);
	q = pushArgument(mb, q, var_next_block);
	setDestVar(q, var_s);

	emit_send(mb, loop_vars->loop_handle, stream_channel_stmt, var_s);

	loop_vars->earlier_line_count = emit_receive(mb, loop_vars->loop_handle, line_count_stmt);
	loop_vars->our_block = emit_receive(mb, loop_vars->loop_handle, block_channel_stmt);

	q = newStmt(mb, "aggr", "count");
	q = pushArgument(mb, q, loop_vars->our_block);
	int var_our_count = getDestVar(q);

	q = newStmt(mb, "aggr", "count");
	q = pushArgument(mb, q, var_next_block);
	int var_next_count = getDestVar(q);

	q = newStmt(mb, "calc", "+");
	q = pushArgument(mb, q, var_our_count);
	q = pushArgument(mb, q, var_next_count);
	int var_total_count = getDestVar(q);

	q = newStmt(mb, "calc", "==");
	q = pushArgument(mb, q, var_total_count);
	q = pushLng(mb, q, 0);
	int var_total_count_is_zero = getDestVar(q);

	q = newStmt(mb, "calc", "-");
	q = pushArgument(mb, q, var_nrecords);
	q = pushArgument(mb, q, loop_vars->earlier_line_count);
	int var_todo = getDestVar(q);

	q = newStmt(mb, "calc", "<=");
	q = pushArgument(mb, q, var_todo);
	q = pushLng(mb, q, 0);
	int var_no_more_needed = getDestVar(q);

	q = newStmt(mb, "calc", "or");
	q->barrier = BARRIERsymbol;
	q = pushArgument(mb, q, var_total_count_is_zero);
	q = pushArgument(mb, q, var_no_more_needed);
	int var_quit_barrier = getDestVar(q);

	// Before we exit the main loop we make sure to unblock the next
	// thread.

	emit_send(mb, loop_vars->loop_handle, line_count_stmt, loop_vars->earlier_line_count);
	emit_send(mb, loop_vars->loop_handle, block_channel_stmt, var_next_block);

	q = newAssignment(mb);
	q->barrier = LEAVEsymbol;
	setReturnArgument(q, loop_vars->loop_barrier);
	q = pushBit(mb, q, true);

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	getDestVar(q) = var_quit_barrier;

	q = newStmt(mb, "copy", "fixlines");
	setArgType(mb, q, 0, bte_bat_type);
	q = pushReturn(mb, q, newTmpVariable(mb, bte_bat_type));
	q = pushReturn(mb, q, newTmpVariable(mb, TYPE_lng));
	q = pushArgument(mb, q, loop_vars->our_block);
	q = pushArgument(mb, q, var_next_block);
	q = pushArgument(mb, q, var_line_sep);
	q = pushArgument(mb, q, var_quote_char);
	q = pushArgument(mb, q, var_escape);
	q = pushArgument(mb, q, var_todo);
	// use the variables defined by fixlines from now on:
	loop_vars->our_block = getArg(q, 0);
	var_next_block = getArg(q, 1);
	loop_vars->our_line_count = getArg(q, 2);

	q = newStmt(mb, "calc", "+");
	q = pushArgument(mb, q, loop_vars->earlier_line_count);
	q = pushArgument(mb, q, loop_vars->our_line_count);
	int var_next_line_count = getDestVar(q);

	emit_send(mb, loop_vars->loop_handle, line_count_stmt, var_next_line_count);
	emit_send(mb, loop_vars->loop_handle, block_channel_stmt, var_next_block);
}

static void
emit_loop_end(MalBlkPtr mb, struct loop_vars *loop_vars)
{
	InstrPtr q;

	q = newStmt(mb, "pipeline", "counter");
	setReturnArgument(q, loop_vars->loop_iter);
	q = pushArgument(mb, q, loop_vars->loop_handle);

	q = newAssignment(mb);
	q->barrier = REDOsymbol;
	pushBit(mb, q, true);
	getDestVar(q) = loop_vars->loop_barrier;

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	getDestVar(q) = loop_vars->loop_barrier;
}

stmt *
rel2bin_copyparpipe(backend *be, sql_rel *rel, list *refs, sql_exp *copyfrom)
{
	switch (parallel_copy_level()) {
		case 0:
			assert(0 /* how did we get here, then? */);
			return NULL;
		case 1:
			break; // main case, below
		default:
			assert(0 /* invalid parallel level */);
			return NULL;
	}
	(void)rel;
	(void)refs;
	const int block_size = get_copy_blocksize();

	struct loop_vars loop_vars;
	InstrPtr q;
	MalBlkPtr mb = be->mb;
	mvc *mvc = be->mvc;
	sql_allocator *sa = mvc->sa;
	list *intermediate_stmts = sa_list(sa);

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

	// remove special negative case for num_rows
	q = newStmt(mb, "calc", "<");
	q->barrier = BARRIERsymbol;
	q = pushArgument(mb, q, var_num_rows);
	q = pushLng(mb, q, 0);
	int num_rows_calculation = getDestVar(q);

	q = newAssignment(mb);
	q = pushLng(mb, q, GDK_lng_max);
	setDestVar(q, var_num_rows);

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	setDestVar(q, num_rows_calculation);

	// convert offset to 0-based
	q = newStmt(mb, "calc", ">");
	q->barrier = BARRIERsymbol;
	q = pushArgument(mb, q, var_offset);
	q = pushLng(mb, q, 0);
	int offset_calculation = getDestVar(q);

	q = newStmt(mb, "calc", "-");
	q = pushArgument(mb, q, var_offset);
	q = pushLng(mb, q, 1);
	setDestVar(q, var_offset);

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	setDestVar(q, offset_calculation);

	// TODO: Deal with the following
	(void)var_num_rows;
	(void)var_best_effort;
	(void)var_on_client;
	(void)var_fixed_width;

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_oid);
	q = pushLng(mb, q, 0);
	int var_new_oids_bat = getDestVar(q);

	q = newAssignment(mb);
	q = pushNil(mb, q, TYPE_bit);
	InstrPtr claim_channel_stmt = emit_channel(mb, getDestVar(q));

	emit_onserver_loop(mb, &loop_vars, var_fname, block_size, var_line_sep, var_quote_char, var_escape, var_offset, var_num_rows);

	int var_claim_token = emit_receive(mb, loop_vars.loop_handle, claim_channel_stmt);

	q = newStmt(mb, "sql", "claim");
	q = pushReturn(mb, q, newTmpVariable(mb, newBatType(TYPE_oid)));
	q = pushArgument(mb, q, be->mvc_var);
	q = pushStr(mb, q, schema_name);
	q = pushStr(mb, q, table_name);
	q = pushArgument(mb, q, loop_vars.our_line_count);
	int var_position = getArg(q, 0);
	int var_positions = getArg(q, 1);

	q = newStmt(mb, "copy", "trackrowids");
	q = pushReturn(mb, q, var_new_oids_bat);
	q = pushArgument(mb, q, var_new_oids_bat);
	q = pushArgument(mb, q, loop_vars.our_line_count);
	q = pushArgument(mb, q, var_position);
	q = pushArgument(mb, q, var_positions);

	emit_send(mb, loop_vars.loop_handle, claim_channel_stmt, var_claim_token);

	// q = newStmt(mb, "calc", "==");
	// q->barrier = REDOsymbol;
	// getDestVar(q) = loop_vars.loop_barrier;
	// q = pushArgument(mb, q, loop_vars.our_line_count);
	// q = pushLng(mb, q, 0);

	assert(column_count > 0);
	q = newStmt(mb, "copy", "splitlines");
	setDestType(mb, q, int_bat_type);
	for (int i = 1; i < column_count; i++) {
		int v = newTmpVariable(mb, int_bat_type);
		q = pushReturn(mb, q, v);
	}
	q = pushArgument(mb, q, loop_vars.our_block);
	q = pushArgument(mb, q, loop_vars.earlier_line_count);
	q = pushArgument(mb, q, loop_vars.our_line_count);
	q = pushArgument(mb, q, var_col_sep);
	q = pushArgument(mb, q, var_line_sep);
	q = pushArgument(mb, q, var_quote_char);
	q = pushArgument(mb, q, var_null_representation);
	q = pushArgument(mb, q, var_escape);
	InstrPtr splitlines_instr = q;

	int i = 0;
	for (node *n = table->columns->l->h; n != NULL; n = n->next, i++) {
		int var_indices = getArg(splitlines_instr, i);

		sql_column *col = n->data;
		sql_type *type = col->type.type;
		const char *column_name = col->base.name;

		switch (type->eclass) {
			case EC_NUM:
				q = newStmtArgs(mb, "copy", "parse_integer", 12);
				q = pushArgument(mb, q, loop_vars.our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, col->type.type->localtype);
				q = pushArgument(mb, q, loop_vars.earlier_line_count);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				break;
			case EC_DEC:
				q = newStmt(mb, "copy", "parse_decimal");
				q = pushArgument(mb, q, loop_vars.our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushInt(mb, q, col->type.digits);
				q = pushInt(mb, q, col->type.scale);
				q = pushNil(mb, q, col->type.type->localtype);
				q = pushArgument(mb, q, loop_vars.earlier_line_count);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				break;
			default:
				q = newStmt(mb, "copy", "parse_generic");
				q = pushArgument(mb, q, loop_vars.our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, col->type.type->localtype);
				q = pushArgument(mb, q, loop_vars.earlier_line_count);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
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

	// END LOOP
	emit_loop_end(mb, &loop_vars);

	q = newStmt(mb, "aggr", "count");
	q = pushArgument(mb, q, var_new_oids_bat);
	q = pushBit(mb, q, false);
	int var_row_count = getDestVar(q);

	add_to_rowcount_accumulator(be, var_row_count);
	// dump_code(-1);

	// I'm assuming that attaching the stmt list to op4.lval
	// will make some else free the arg_stmt's we created here.
	stmt *dummy_stmt = stmt_none(be);
	dummy_stmt->op4.lval = intermediate_stmts;
	return dummy_stmt;
}
