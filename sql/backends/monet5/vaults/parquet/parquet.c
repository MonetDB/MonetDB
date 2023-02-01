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
#include "mal_debugger.h"
#include "mal_linker.h"
#include "mal_utils.h"

parquet_file *parquet_open_file(char* filename) {
    GParquetArrowFileReader *reader;
    GError *g_error;
    char* error = NULL;

    reader = gparquet_arrow_file_reader_new_path(filename, &g_error);

    if(!reader) {
        reader = NULL;
        error = g_error->message;
    }

    parquet_file *file = GDKmalloc(sizeof(parquet_file));

    file->filename = filename;
    file->reader = reader;
    file->error = error;

    return file;
}


parquet_table_metadata parquet_get_table_metadata(parquet_file *file) {
    GError *table_error;
    GArrowTable *table = gparquet_arrow_file_reader_read_table(file->reader, &table_error);

    if(table_error) {
        printf("%s", table_error->message);
    }

    guint64 n_rows = garrow_table_get_n_rows(table);

    parquet_table_metadata metadata = {"foo", n_rows};

    return metadata;
}

static int
parquet_add_types(sql_subfunc *f, char *filename)
{
	(void)f;
	(void)filename;
	return 0;
}

static int
parquet_load(sql_subfunc *f, char *filename)
{
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