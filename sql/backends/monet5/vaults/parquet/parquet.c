#include <parquet-glib/parquet-glib.h>
#include <parquet-glib/arrow-file-reader.h>
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/metadata.h>

#include "monetdb_config.h"
#include "rel_file_loader.h"
#include "rel_exp.h"

#include "parquet.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_linker.h"
#include "sql_types.h"
#include "sql_statement.h"

#include <unistd.h>

static parquet_file *
parquet_open_file(char* filename)
{
    GParquetArrowFileReader *reader;
    GError *g_error = NULL;

    parquet_file *file = GDKmalloc(sizeof(parquet_file));

    // TODO: access is not available on Windows.
    // TODO: probably replace this with GDKfdlocate located in gdk_storage.h
    if (access(filename, F_OK) != 0) {
      reader = NULL;
    }
    else {
      reader = gparquet_arrow_file_reader_new_path(filename, &g_error);
    }

    file->filename = filename;
    file->reader = reader;
    file->error = g_error;

    return file;
}

static char* parquet_type_map(GArrowType type) {
    switch(type) {
      case GARROW_TYPE_NA:
        return  "NA";

      case GARROW_TYPE_BOOLEAN:
        return  "BOOL";

      case GARROW_TYPE_UINT8:
      case GARROW_TYPE_INT8:
        return  "TINYINT";

      case GARROW_TYPE_INT16:
      case GARROW_TYPE_UINT16:
        return  "SMALLINT";

      case GARROW_TYPE_UINT32:
      case GARROW_TYPE_INT32:
        return  "INT";

      case GARROW_TYPE_UINT64:
      case GARROW_TYPE_INT64:
        return  "bigint";

      case GARROW_TYPE_FLOAT:
      case GARROW_TYPE_HALF_FLOAT:
        return  "FLOAT";

      case GARROW_TYPE_DOUBLE:
        return  "DOUBLE";

      case GARROW_TYPE_STRING:
        return  "varchar";

      case GARROW_TYPE_BINARY:
      case GARROW_TYPE_FIXED_SIZE_BINARY:
        return  "BLOB";

      case GARROW_TYPE_DATE32:
      case GARROW_TYPE_DATE64:
        return  "DATE";

      case GARROW_TYPE_TIMESTAMP:
      case GARROW_TYPE_TIME32:
      case GARROW_TYPE_TIME64:
        return  "TIMESTAMP";

      case GARROW_TYPE_DECIMAL128:
      case GARROW_TYPE_DECIMAL256:
        return  "DECIMAL";

      case GARROW_TYPE_LIST:
      case GARROW_TYPE_LARGE_LIST:
      case GARROW_TYPE_STRUCT:
      case GARROW_TYPE_SPARSE_UNION:
      case GARROW_TYPE_DENSE_UNION:
      case GARROW_TYPE_DICTIONARY:
      case GARROW_TYPE_MAP:
      case GARROW_TYPE_EXTENSION:
      case GARROW_TYPE_FIXED_SIZE_LIST:
      case GARROW_TYPE_DURATION:
        return NULL;


      case GARROW_TYPE_LARGE_STRING:
        return  "TEXT";

      case GARROW_TYPE_LARGE_BINARY:
        return  "Large binary";

      ///
      /// TODO: figure out what this should be.
      ///
      case GARROW_TYPE_MONTH_INTERVAL:
      case GARROW_TYPE_DAY_TIME_INTERVAL:
      case GARROW_TYPE_MONTH_DAY_NANO_INTERVAL:
        return  NULL;
    } // switch

    return NULL;
}

// This function is called while the relational plan is being built (the sym to rel step).
// It needs to figure out the types of the columns.
// If it goes well you can see the result by running
//
//     PLAN SELECT * FROM 'data.parquet';
static str
parquet_add_types(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname)
{
	parquet_file *file = parquet_open_file(filename);

	if(file->reader == NULL) {
		throw(SQL, SQLSTATE(42000), "parquet" RUNTIME_FILE_NOT_FOUND);
	}

	GError *table_error = NULL;
	GArrowTable *table = gparquet_arrow_file_reader_read_table(file->reader, &table_error);

	if(table_error) {
		throw(SQL, SQLSTATE(42000), "parquet" RUNTIME_LOAD_ERROR); // TODO: different error.
	}

	guint n_columns = garrow_table_get_n_columns(table);

	// list *types = sa_list(sql->sa);

	if (!tname)
		tname = "parquet";

	for(int col = 0; col < (int)n_columns; col++) {
		GArrowChunkedArray *array = garrow_table_get_column_data(table, col);
		GArrowType type = garrow_chunked_array_get_value_type(array);
    char buff[25];
    snprintf(buff, 100, "name_%i", col);
    str name = GDKstrdup(buff);
		char* st = parquet_type_map(type);

		if(st) {
			sql_subtype *t = sql_bind_subtype(sql->sa, st, 0, 0);
			if (!t)
				throw(SQL, SQLSTATE(42000), "Cannot resolve type '%s'", st);

			// list_append(types, t);
			list_append(res_exps, exp_column(sql->sa, NULL, name, t, CARD_MULTI, 1, 0, 0));
		}
		else {
			throw(SQL, SQLSTATE(42000), "parquet" RUNTIME_LOAD_ERROR); // TODO: this should throw a 'unsupported column type' error.
		}
	}

	(void)table;
	/* cleanup tbl */
	f->res = res_exps;

	/* close file */
	GDKfree(file);
	return MAL_SUCCEED;
}

static stmt*
parquet_emit_plan(backend *be, sql_subfunc *f, char *filename)
{
	// We cannot use stmt_unop() because our f is bound to a generic sql_func,
	// not to one that is specific to Parquet and contains a reference
	// to PARQUETload.
	// This means we have to emit the MAL code itself and create a stmt that
	// reflects it.
	mvc *mvc = be->mvc;
	MalBlkPtr mb = be->mb;

	list *return_types = f->res;

	// This is the statement we append to the MAL block:
	int nargs = list_length(return_types) + 1;
	InstrPtr p = newStmtArgs(mb, "parquet", "load_table", nargs);
	if (p == NULL)
		return sql_error(mvc, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	// Add the return variables
	for (node *n = return_types->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *subtype = &e->tpe;
		int data_type = subtype->type->localtype;
		int bat_type = newBatType(data_type);
		if (n == return_types->h) {
			// The first return position has already been created by
			// newStmtArgs above
			setArgType(mb, p, 0, bat_type);
		} else {
			// The other return positions are created by us
			int var = newTmpVariable(mb, bat_type);
			p = pushReturn(mb, p, var);
		}
	}
	// Then add the filename
	p = pushStr(mb, p, filename);
	// And add the MAL statement to the block
	pushInstruction(mb, p);

	// Later on, the rest of the SQL compiler will need to know which
	// MAL variables we stored the result BATs in.
	// That information goes into the stmt we return.
	//
	// I'm not sure about the official way to do this, most stmt_* functions
	// assume that we already have stmts for the individual columns.
	// We don't so we apply voodoo voodoo wave dead chicken.
	list *result_column_stmts = sa_list(mvc->sa);
	if (!result_column_stmts)
		return NULL;
	int i = 0;
	for (node *n = return_types->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *subtype = &e->tpe;
		stmt *s = stmt_blackbox_result(be, p, i++, subtype);
		result_column_stmts = list_append(result_column_stmts, s);
	}

	return stmt_list(be, result_column_stmts);
}

// This function is called while the MAL plan is being built (the rel to bin step).
// If it goes well you can see the result by running
//
//     EXPLAIN SELECT * FROM 'data.parquet';
static void *
parquet_generate_plan(void *BE, sql_subfunc *f, char *filename)
{
	backend *be = (backend*)BE;

	// So, basically, our task is to append MAL statements to the MAL block 'mb'.
	// We will return a 'stmt', which is a kind of summary of the statements we
	// produced. In particular, the stmt contains the list of MAL variables in
	// which the generated statements leave the result BATs.
	stmt *s = parquet_emit_plan(be, f, filename);

	// For technical reasons we need to return the stmt as a void pointer.
	return (void*)s;
}

// This function is called when the module is loaded.
// It registers the parquet loader.
static str
PARQUETprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    (void)cntxt;
    (void)mb;
    (void)stk;
    (void)pci;
	fl_register("parquet", &parquet_add_types, &parquet_generate_plan);
    return MAL_SUCCEED;
}

// This function is called if the module is ever unloaded.
// Currently, it does nothing.
static str
PARQUETepilogue(void *ret)
{
    (void)ret;
    return MAL_SUCCEED;
}

// This function is called when the parquet.load_table operator in the MAL
// plan is executed.
static str
PARQUETload(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;
	throw(MAL, "parquet.load_table", SQLSTATE(42000) "Not implemented yet");
}


#include "sql_scenario.h"
#include "mel.h"

static mel_func parquet_init_funcs[] = {
 pattern("parquet", "prelude", PARQUETprelude, false, "", noargs),
 command("parquet", "epilogue", PARQUETepilogue, false, "", noargs),
 pattern("parquet", "load_table", PARQUETload, true, "load data from the parquet file", args(1,2,
	batvarargany("", 0),
	arg("filename", str),
 )),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_parquet_mal)
{ mal_module("parquet", NULL, parquet_init_funcs); }

