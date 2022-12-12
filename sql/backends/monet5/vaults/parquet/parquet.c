#include "parquet.h"

#include <stdio.h>

#include <parquet-glib/parquet-glib.h>
#include <parquet-glib/arrow-file-reader.h>
#include <arrow-glib/arrow-glib.h>
#include <parquet-glib/metadata.h>

struct parquet_file open_file(char* filename) {
    GParquetArrowFileReader *reader;
    GError *error;
  
    reader = gparquet_arrow_file_reader_new_path(filename, &error);

    if(!reader) {
        printf("%s", error->message);
        /* TODO: Throw a SQLState Error. */
        reader = NULL;
    }

    struct parquet_file file = { filename, reader };

    return file;
}
