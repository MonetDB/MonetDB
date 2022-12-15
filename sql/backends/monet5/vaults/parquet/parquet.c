#include "parquet.h"

#include <stdio.h>

#include <parquet-glib/parquet-glib.h>
#include <parquet-glib/arrow-file-reader.h>
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/metadata.h>

parquet_file open_file(char* filename) {
    GParquetArrowFileReader *reader;
    GError *error;
  
    reader = gparquet_arrow_file_reader_new_path(filename, &error);

    if(!reader) {
        printf("%s", error->message);
        /* TODO: Throw a SQLState Error. */
        reader = NULL;
    }

    parquet_file file = { filename, reader };

    return file;
}


parquet_table_metadata get_table_metadata(parquet_file *file) {
    /* This shouldn't be possible, but check it for good measure */
    if(file->reader == NULL) {
        /* Throw error */
    }

    GError *table_error;
    GArrowTable *table = gparquet_arrow_file_reader_read_table(file->reader, &table_error);

    if(table_error) {
        printf("%s", table_error->message);
    }

    guint64 n_rows = garrow_table_get_n_rows(table);

    parquet_table_metadata metadata = {"foo", n_rows};

    return metadata;
}
