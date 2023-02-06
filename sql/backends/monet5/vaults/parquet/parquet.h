#ifndef MONET_PARQUET
#define MONET_PARQUET

#include <parquet-glib/arrow-file-reader.h>

typedef struct parquet_file {
    char* filename;
    GParquetArrowFileReader *reader;
    char* error;
} parquet_file;


/* Opens the file, and returns a struct containing the reader. */
//parquet_file *parquet_open_file(char* filename);

//parquet_table_metadata parquet_get_table_metadata(parquet_file *file);

//void parquet_init(void);

#endif
