#ifndef MONET_PARQUET
#define MONET_PARQUET

#include <parquet-glib/arrow-file-reader.h>

struct parquet_file {
    char* filename;
    GParquetArrowFileReader *reader;
};

struct parquet_table_metadata {
    char* table_name;
    int n_row;
};

/* Opens the file, and returns a struct containing the reader. */
struct parquet_file open_file(char* filename);

struct parquet_table_metadata get_table_metadata(struct parquet_file file);

#endif
