#include <parquet-glib/parquet-glib.h>
#include <parquet-glib/arrow-file-reader.h>
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/metadata.h>

#include "monetdb_config.h"
#include "rel_file_loader.h"

#include "parquet.h"
#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_runtime.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_debugger.h"
#include "mal_linker.h"
#include "mal_utils.h"
#include "sql_types.h"

#include <unistd.h>

static parquet_file *
parquet_open_file(char* filename)
{
    GParquetArrowFileReader *reader;
    GError *g_error = NULL;

    parquet_file *file = GDKmalloc(sizeof(parquet_file));

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
        return  "boolean";
        
      case GARROW_TYPE_UINT8:
        return  "uint8";
        
      case GARROW_TYPE_INT8:
        return  "int8";
        
      case GARROW_TYPE_UINT16:
        return  "uint16";
        
      case GARROW_TYPE_INT16:
        return  "int16";
        
      case GARROW_TYPE_UINT32:
        return  "uint32";
        
      case GARROW_TYPE_INT32:
        return  "int32";
        
      case GARROW_TYPE_UINT64:
        return  "uint64";
        
      case GARROW_TYPE_INT64:
        return  "int64";
        
      case GARROW_TYPE_HALF_FLOAT:
        return  "half float";
        
      case GARROW_TYPE_FLOAT:
        return  "type float";
        
      case GARROW_TYPE_DOUBLE:
        return  "double";
        
      case GARROW_TYPE_STRING:
        return  "type string";
        
      case GARROW_TYPE_BINARY:
        return  "type binary";
        
      case GARROW_TYPE_FIXED_SIZE_BINARY:
        return  "fixed size binary";
        
      case GARROW_TYPE_DATE32:
        return  "date32";
        
      case GARROW_TYPE_DATE64:
        return  "date64";
        
      case GARROW_TYPE_TIMESTAMP:
        return  "timestamp";
        
      case GARROW_TYPE_TIME32:
        return  "time32";
        
      case GARROW_TYPE_TIME64:
        return  "time64";
        
      case GARROW_TYPE_MONTH_INTERVAL:
        return  "type month interval";
        
      case GARROW_TYPE_DAY_TIME_INTERVAL:
        return  "day time interval";
        
      case GARROW_TYPE_DECIMAL128:
        return  "decimal128";
        
      case GARROW_TYPE_DECIMAL256:
        return  "decimal256";
        
      case GARROW_TYPE_LIST:
        return  "type list";
        
      case GARROW_TYPE_STRUCT:
        return  "type struct";
        
      case GARROW_TYPE_SPARSE_UNION:
        return  "sparse union";
        
      case GARROW_TYPE_DENSE_UNION:
        return  "dense union";
        
      case GARROW_TYPE_DICTIONARY:
        return  "type dict";
        
      case GARROW_TYPE_MAP:
        return  "type map";
        
      case GARROW_TYPE_EXTENSION:
        return  "type extension";
        
      case GARROW_TYPE_FIXED_SIZE_LIST:
        return  "fixed size list";
        
      case GARROW_TYPE_DURATION:
        return  "type duration";
        
      case GARROW_TYPE_LARGE_STRING:
        return  "large string";
        
      case GARROW_TYPE_LARGE_BINARY:
        return  "Large binary";
        
      case GARROW_TYPE_LARGE_LIST:
        return  "Large list";
        
      case GARROW_TYPE_MONTH_DAY_NANO_INTERVAL:
        return  "Month Day interval";
    } // switch

    return "not implemented";
}

static str
parquet_add_types(mvc *sql, sql_subfunc *f, char *filename)
{
  parquet_file *file = parquet_open_file(filename);

	if(file->reader == NULL) {
    throw(SQL, SQLSTATE(42000), "parquet" RUNTIME_FILE_NOT_FOUND);
	}

  GError *table_error;
  GArrowTable *table = gparquet_arrow_file_reader_read_table(file->reader, &table_error);

  if(table_error) {
    throw(SQL, SQLSTATE(42000), "parquet" RUNTIME_LOAD_ERROR); // TODO: different error.
  }

  guint n_columns = garrow_table_get_n_columns(table);

  list *types = sa_list(sql->sa);

  for(int col = 0; col < (int)n_columns; col++) {
      GArrowChunkedArray *array = garrow_table_get_column_data(table, col);
      GArrowType type = garrow_chunked_array_get_value_type(array);
      char* st = parquet_type_map(type);

      printf("%s", st);
  }

	// for each parquet column
	// 	get type from parquet column

	// 	sql_subtype *t = find_type( using meta data);

    //     append(types, t);
    // }
	(void)table;
	/* cleanup tbl */
    f->res = types;

	/* close file */
	GDKfree(file);
	return "";
}

static int
parquet_load(mvc *sql, sql_subfunc *f, char *filename)
{
	(void)sql;
	(void)f;
	(void)filename;
	return 0;
}

static str
Parquetprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    (void)cntxt;
    (void)mb;
    (void)stk;
    (void)pci;
	fl_register("parquet", &parquet_add_types, &parquet_load);
    return MAL_SUCCEED;
}

static str
Parquetepilogue(void *ret)
{
    (void)ret;
    return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func parquet_init_funcs[] = {
 pattern("parquet", "prelude", Parquetprelude, false, "", noargs),
 command("parquet", "epilogue", Parquetepilogue, false, "", noargs),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_parquet_mal)
{ mal_module("parquet", NULL, parquet_init_funcs); }

