#ifndef MONET_PARQUET
#define MONET_PARQUET

#include <parquet-glib/arrow-file-reader.h>

typedef struct parquet_file {
    char* filename;
    GParquetArrowFileReader *reader;
    GError* error;
} parquet_file;


#endif
