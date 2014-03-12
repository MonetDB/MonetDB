#ifndef _BAM_DB_INTERFACE_H
#define _BAM_DB_INTERFACE_H

#include "sql_scenario.h"
#include "sql_mvc.h"

#include "bam_wrapper.h"

/* macro to run SQL and incorporate logging; sql should be (char **) */
#define RUN_SQL(cntxt, sql, descr, msg) { \
    TO_LOG("%s\n", *sql); \
    msg = SQLstatementIntern(cntxt, sql, descr, TRUE, FALSE); \
}


/* SQL code for creating header tables; used to make calls to create_table_if_not_exists function */
#define SQL_CREATE_FILES \
    "CREATE TABLE bam.files ( \n\
        file_id                        BIGINT      NOT NULL, \n\
        file_location                  STRING      NOT NULL, \n\
        dbschema                       SMALLINT    NOT NULL, \n\
        format_version                 VARCHAR(7), \n\
        sorting_order                  VARCHAR(10), \n\
        comments                       STRING, \n\
        CONSTRAINT files_pkey_file_id PRIMARY KEY (file_id) \n\
    );"

#define SQL_CREATE_SQ \
    "CREATE TABLE bam.sq ( \n\
        sn                             STRING      NOT NULL, \n\
        file_id                        BIGINT      NOT NULL, \n\
        ln                             INT, \n\
        \"as\"                         INT, \n\
        m5                             STRING, \n\
        sp                             STRING, \n\
        ur                             STRING, \n\
        CONSTRAINT sq_pkey_sn_file_id PRIMARY KEY (sn, file_id), \n\
        CONSTRAINT sq_fkey_file_id FOREIGN KEY (file_id) REFERENCES bam.files (file_id) \n\
    );"

#define SQL_CREATE_RG \
    "CREATE TABLE bam.rg ( \n\
        id                             STRING      NOT NULL, \n\
        file_id                        BIGINT      NOT NULL, \n\
        cn                             STRING, \n\
        ds                             STRING, \n\
        dt                             TIMESTAMP, \n\
        fo                             STRING, \n\
        ks                             STRING, \n\
        lb                             STRING, \n\
        pg                             STRING, \n\
        pi                             INT, \n\
        pl                             STRING, \n\
        pu                             STRING, \n\
        sm                             STRING, \n\
        CONSTRAINT rg_pkey_id_file_id PRIMARY KEY (id, file_id), \n\
        CONSTRAINT rg_fkey_file_id FOREIGN KEY (file_id) REFERENCES bam.files (file_id) \n\
    );"

#define SQL_CREATE_PG \
    "CREATE TABLE bam.pg ( \n\
        id                             STRING      NOT NULL, \n\
        file_id                        BIGINT      NOT NULL, \n\
        pn                             STRING, \n\
        cl                             STRING, \n\
        pp                             STRING, \n\
        vn                             STRING, \n\
        CONSTRAINT pg_pkey_id_file_id PRIMARY KEY (id, file_id), \n\
        CONSTRAINT pg_fkey_file_id FOREIGN KEY (file_id) REFERENCES bam.files (file_id) \n\
    );"


str create_schema_if_not_exists(Client cntxt, mvc *m, str schemaname, str descr, sql_schema **ret);
str create_table_if_not_exists(Client cntxt, mvc *m, sql_schema *s, str tablename, str sql_creation, str descr, sql_table **ret);
str next_file_id(mvc *m, sql_table *files, lng *next_file_id);
str create_alignment_storage_0(Client cntxt, str descr, bam_wrapper *bw);
str create_alignment_storage_1(Client cntxt, str descr, bam_wrapper *bw);
str copy_into_db(Client cntxt, bam_wrapper *bw);
str drop_file(Client cntxt, str descr, lng file_id, sht dbschema);

/* TODO Consider creating something that truncates everything from the BAM schema */
#endif
