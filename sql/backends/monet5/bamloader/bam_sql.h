#ifndef BAM_SQL_H
#define BAM_SQL_H

#include "monetdb_config.h"
#include "sql_scenario.h"
#include "sql_mvc.h"

#include "bam_globals.h"

#ifdef WIN32
#ifndef LIBBAM
#define bam_export extern __declspec(dllimport)
#else
#define bam_export extern __declspec(dllexport)
#endif
#else
#define bam_export extern
#endif


#define DIR_SQL BAM_HOME"/sql"
#define MAX_SQL_FILE_LENGTH 65536
#define MAX_SQL_LINE_LENGTH 1024
#define SQL_FILE_INIT_SCHEMA "bam_schema.sql"
#define SQL_FILE_DROP_SCHEMA "bam_clear.sql"
#define SQL_FILE_CREATE_ALIGNMENTS_STORAGE_0 "bam_create_alignments_storage_0.sql"
#define SQL_FILE_CREATE_ALIGNMENTS_STORAGE_1 "bam_create_alignments_storage_1.sql"
#define SQL_FILE_CREATE_ALIGNMENTS_STORAGE_2 "bam_create_alignments_storage_2.sql"
#define SQL_FILE_DROP_ALIGNMENTS_STORAGE_0 "bam_drop_alignments_storage_0.sql"
#define SQL_FILE_DROP_ALIGNMENTS_STORAGE_1 "bam_drop_alignments_storage_1.sql"
#define SQL_FILE_DROP_ALIGNMENTS_STORAGE_2 "bam_drop_alignments_storage_2.sql"
#define SQL_FILE_DIVIDE_DATA "bam_divide_data.sql"

#define MAX_SQL_SEARCH_REPLACE_CHARS 32

str get_sql_from_file(str filename, char search[][MAX_SQL_SEARCH_REPLACE_CHARS], char replace[][MAX_SQL_SEARCH_REPLACE_CHARS], int nr_replacement_strings, str out_err);


#endif
