#include "parquet.h"

#include <stdio.h>

#include <parquet-glib/parquet-glib.h>
#include <parquet-glib/arrow-file-reader.h>
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/metadata.h>


extern void *GDKmalloc(size_t size); /* FIXME */

parquet_file *open_file(char* filename) {
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


parquet_table_metadata get_table_metadata(parquet_file *file) {
    GError *table_error;
    GArrowTable *table = gparquet_arrow_file_reader_read_table(file->reader, &table_error);

    if(table_error) {
        printf("%s", table_error->message);
    }

    guint64 n_rows = garrow_table_get_n_rows(table);

    parquet_table_metadata metadata = {"foo", n_rows};

    return metadata;
}
