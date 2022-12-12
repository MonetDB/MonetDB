#ifndef MONET_PARQUET
#define MONET_PARQUET

#include <parquet-glib/arrow-file-reader.h>

struct parquet_file {
    char* filename;
    GParquetArrowFileReader *reader;
};

/* Opens the file, and returns a struct containing the reader. */
struct parquet_file open_file(char* filename);

#endif
