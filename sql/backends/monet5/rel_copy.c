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
	int defer_close;
	int our_block;
	int earlier_line_count;
	int our_line_count;
	int failures_bat;
};


static void
emit_pipelined_loop(
	MalBlkPtr mb, struct loop_vars *loop_vars,
	str fname, bool onclient, int block_size,
	int var_line_sep, int var_quote_char, bool escape,
	int var_failures_bat,
	lng offset, lng nrecords_or_minusone)
{
	InstrPtr q;
	int bte_bat_type = newBatType(TYPE_bte);
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
		q = pushBit(mb, q, false);
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

	q = newStmt(mb, "copy", "defer_close");
	q = pushArgument(mb, q, var_stream);
	loop_vars->defer_close = getDestVar(q);

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_bte);
	q = pushLng(mb, q, alloc);
	int var_block = getDestVar(q);

	// emit the offset handling
	if (offset > 0) {
		q = newAssignment(mb);
		q = pushLng(mb, q, offset);
		int var_offset = getDestVar(q);

		q = newAssignment(mb);
		q->barrier = BARRIERsymbol;
		q = pushBit(mb, q, true);
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
	}

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
	q = pushBit(mb, q, escape);
	q = pushArgument(mb, q, var_failures_bat);
	q = pushArgument(mb, q, loop_vars->earlier_line_count);
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
emit_redo(MalBlkPtr mb, struct loop_vars *loop_vars)
{
	InstrPtr q;

	q = newStmt(mb, "pipeline", "counter");
	setReturnArgument(q, loop_vars->loop_iter);
	q = pushArgument(mb, q, loop_vars->loop_handle);

	q = newAssignment(mb);
	q->barrier = REDOsymbol;
	pushBit(mb, q, true);
	getDestVar(q) = loop_vars->loop_barrier;
}

static void
emit_loop_end(MalBlkPtr mb, struct loop_vars *loop_vars)
{
	InstrPtr q;

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	getDestVar(q) = loop_vars->loop_barrier;

	q = newStmt(mb, "language", "pass");
	q = pushArgument(mb, q, loop_vars->defer_close);
}

stmt *
rel2bin_copyparpipe(backend *be, sql_rel *rel, list *refs, sql_exp *copyfrom)
{
	(void)rel;
	(void)refs;
	const int block_size = get_copy_blocksize();

	struct loop_vars loop_vars = { 0 };
	InstrPtr q;
	MalBlkPtr mb = be->mb;
	mvc *mvc = be->mvc;
	sql_allocator *sa = mvc->sa;

	switch (parallel_copy_level()) {
		case 0:
			assert(0 /* how did we get here, then? */);
			return NULL;
		case 1:
		case 2:
			break; // main case, below
		default:
			assert(0 /* invalid parallel level */);
			return NULL;
	}

	list *intermediate_stmts = sa_list(sa);
	int int_bat_type = newBatType(TYPE_int);

	// Extract parameters
	list *copyfrom_args = copyfrom->l;
	node *n = copyfrom_args->h;

	sql_table *table = take_parameter(&n, TYPE_ptr)->val.pval;
	const char *table_name = table->base.name;
	const char *schema_name = table->s->base.name;
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

	int var_col_sep = getStrConstant(mb, col_sep);
	int var_line_sep = getStrConstant(mb, line_sep);
	int var_quote_char = getStrConstant(mb, quote_char ? quote_char : (char*)str_nil);
	// convert offset to 0-based
	if (offset > 0)
		offset--;

	// TODO: Deal with the following
	(void)fixed_width;

	q = newStmt(mb, "bat", "new");
	q = pushNil(mb, q, TYPE_oid);
	q = pushLng(mb, q, 0);
	int var_new_oids_bat = getDestVar(q);

	// Initialize the failures bat if BEST EFFORT is on
	int var_failures_bat = newTmpVariable(mb, newBatType(TYPE_oid));
	if (best_effort) {
		q = newStmt(mb, "bat", "new");
		setDestVar(q, var_failures_bat);
		q = pushNil(mb, q, TYPE_oid);
		q = pushLng(mb, q, 0);
	} else {
		q = newAssignment(mb);
		setDestVar(q, var_failures_bat);
		pushNil(mb, q, newBatType(TYPE_oid));
	}

	q = newAssignment(mb);
	q = pushNil(mb, q, TYPE_bit);
	InstrPtr claim_channel_stmt = emit_channel(mb, getDestVar(q));

	emit_pipelined_loop(mb, &loop_vars, fname, on_client, block_size, var_line_sep, var_quote_char, escape, var_failures_bat, offset, num_rows);

	int var_claim_token = emit_receive(mb, loop_vars.loop_handle, claim_channel_stmt);

	q = newStmt(mb, "calc", "==");
	q->barrier = BARRIERsymbol;
	q = pushArgument(mb, q, loop_vars.our_line_count);
	q = pushLng(mb, q, 0);
	int var_claim_block = getDestVar(q);

	emit_send(mb, loop_vars.loop_handle, claim_channel_stmt, var_claim_token);

	emit_redo(mb, &loop_vars);

	q = newAssignment(mb);
	q->barrier = EXITsymbol;
	q = pushBit(mb, q, true);
	setDestVar(q, var_claim_block);

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
	q = pushStr(mb, q, null_representation);
	q = pushArgument(mb, q, var_failures_bat);
	q = pushBit(mb, q, escape);
	InstrPtr splitlines_instr = q;

	int i = 0;
	for (node *n = table->columns->l->h; n != NULL; n = n->next, i++) {
		int var_indices = getArg(splitlines_instr, i);

		sql_column *col = n->data;
		sql_type *type = col->type.type;
		const char *column_name = col->base.name;
		unsigned int digits = col->type.digits;
		unsigned int scale = col->type.scale;
		int localtype = col->type.type->localtype;

		switch (type->eclass) {
			case EC_NUM:
				q = newStmtArgs(mb, "copy", "parse_integer", 12);
				q = pushArgument(mb, q, loop_vars.our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, var_failures_bat);
				q = pushArgument(mb, q, loop_vars.earlier_line_count);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				break;
			case EC_SEC:
				scale = 3;
				digits = 18;
				/* fallthrough */
			case EC_MONTH:
			case EC_DEC:
				if (type->eclass == EC_MONTH)
					digits = 9;
				q = newStmt(mb, "copy", "parse_decimal");
				q = pushArgument(mb, q, loop_vars.our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushInt(mb, q, digits);
				q = pushInt(mb, q, scale);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, var_failures_bat);
				q = pushArgument(mb, q, loop_vars.earlier_line_count);
				q = pushInt(mb, q, i);
				q = pushStr(mb, q, col->base.name);
				break;
			default:
				q = newStmt(mb, "copy", "parse_generic");
				q = pushArgument(mb, q, loop_vars.our_block);
				q = pushArgument(mb, q, var_indices);
				q = pushNil(mb, q, localtype);
				q = pushArgument(mb, q, var_failures_bat);
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
	emit_redo(mb, &loop_vars);
	emit_loop_end(mb, &loop_vars);

	// BEST EFFORT post processing

	if (best_effort) {
		// Map from line numbers to delete to row id's to delete.
		// There are still duplicates.
		q = newStmt(mb, "algebra", "projection");
		q = pushArgument(mb, q, var_failures_bat);
		q = pushArgument(mb, q, var_new_oids_bat);
		int var_rows_to_delete_with_duplicates = getDestVar(q);

		q = newStmt(mb, "algebra", "difference");
		q = pushArgument(mb, q, var_new_oids_bat);
		q = pushArgument(mb, q, var_rows_to_delete_with_duplicates);
		q = pushNil(mb, q, newBatType(TYPE_oid));
		q = pushNil(mb, q, newBatType(TYPE_oid));
		q = pushBit(mb, q, false);
		q = pushBit(mb, q, true);
		q = pushNil(mb, q, TYPE_lng);
		int var_tmp = getDestVar(q);

		q = newStmt(mb, "algebra", "projection");
		q = pushArgument(mb, q, var_tmp);
		q = pushArgument(mb, q, var_new_oids_bat);
		int var_rows_to_retain = getDestVar(q);

		q = newStmt(mb, "algebra", "difference");
		q = pushArgument(mb, q, var_new_oids_bat);
		q = pushArgument(mb, q, var_rows_to_retain);
		q = pushNil(mb, q, newBatType(TYPE_oid));
		q = pushNil(mb, q, newBatType(TYPE_oid));
		q = pushBit(mb, q, false);
		q = pushBit(mb, q, true);
		q = pushNil(mb, q, TYPE_lng);
		var_tmp = getDestVar(q);

		q = newStmt(mb, "algebra", "projection");
		q = pushArgument(mb, q, var_tmp);
		q = pushArgument(mb, q, var_new_oids_bat);
		int var_rows_to_delete = getDestVar(q);

		q = newStmt(mb, "sql", "delete");
		q = pushArgument(mb, q, be->mvc_var);
		q = pushStr(mb, q, schema_name);
		q = pushStr(mb, q, table_name);
		q = pushArgument(mb, q, var_rows_to_delete);

		q = newAssignment(mb);
		q = pushArgument(mb, q, var_rows_to_retain);
		setDestVar(q, var_new_oids_bat);
	}

	// END OF BEST EFFORT

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
