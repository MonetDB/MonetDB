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
        return  "BIGINT";
        
      case GARROW_TYPE_FLOAT:
      case GARROW_TYPE_HALF_FLOAT:
        return  "FLOAT";
        
      case GARROW_TYPE_DOUBLE:
        return  "DOUBLE";
        
      case GARROW_TYPE_STRING:
        return  "STRING";
        
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

static str
parquet_add_types(mvc *sql, sql_subfunc *f, char *filename)
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

  list *types = sa_list(sql->sa);
  list *col_names = sa_list(sql->sa);

  for(int col = 0; col < (int)n_columns; col++) {
      GArrowChunkedArray *array = garrow_table_get_column_data(table, col);
      GArrowType type = garrow_chunked_array_get_value_type(array);
      char* st = parquet_type_map(type);

      printf("%s\n", st);

      if(st) {
        	sql_subtype *t = sql_bind_subtype(sql->sa, st, 8, 0);

          sa_list_append(sql->sa, types, t);
      }
      else {
        throw(SQL, SQLSTATE(42000), "parquet" RUNTIME_LOAD_ERROR); // TODO: this should throw a 'unsupported column type' error.
      }
  }

  (void)table;
	/* cleanup tbl */
  f->res = types;
  f->colnames = col_names;

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

