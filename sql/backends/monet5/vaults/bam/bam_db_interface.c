/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file handles all communication that is done with
 * the running database.
 */

#include "monetdb_config.h"
#include "bam_globals.h"
#include "bam_db_interface.h"

#define SQL_CREATE_STORAGE_0 \
	"CREATE TABLE bam.alignments_"LLFMT" ( \n\
		virtual_offset				BIGINT	  NOT NULL, \n\
		qname						 STRING	  NOT NULL, \n\
		flag						  SMALLINT	NOT NULL, \n\
		rname						 STRING	  NOT NULL, \n\
		pos						   INT		 NOT NULL, \n\
		mapq						  SMALLINT	NOT NULL, \n\
		cigar						 STRING	  NOT NULL, \n\
		rnext						 STRING	  NOT NULL, \n\
		pnext						 INT		 NOT NULL, \n\
		tlen						  INT		 NOT NULL, \n\
		seq						   STRING	  NOT NULL, \n\
		qual						  STRING	  NOT NULL, \n\
		CONSTRAINT alignments_"LLFMT"_pkey_virtual_offset PRIMARY KEY (virtual_offset) \n\
	); \n\
	\n\
	CREATE TABLE bam.alignments_extra_"LLFMT" ( \n\
		tag						   CHAR(2)	 NOT NULL, \n\
		virtual_offset				BIGINT	  NOT NULL, \n\
		type						  CHAR(1)	 NOT NULL, \n\
		value						 STRING, \n\
		CONSTRAINT alignments_extra_"LLFMT"_pkey_tag_virtual_offset PRIMARY KEY (tag, virtual_offset), \n\
		CONSTRAINT alignments_extra_"LLFMT"_fkey_virtual_offset FOREIGN KEY (virtual_offset) \n\
			REFERENCES bam.alignments_"LLFMT" (virtual_offset) \n\
	);"

#define SQL_CREATE_STORAGE_1 \
	"CREATE TABLE bam.paired_primary_alignments_"LLFMT" ( \n\
		l_virtual_offset			  BIGINT	  NOT NULL, \n\
		r_virtual_offset			  BIGINT	  NOT NULL, \n\
		qname						 STRING	  NOT NULL, \n\
		l_flag						SMALLINT	NOT NULL, \n\
		l_rname					   STRING	  NOT NULL, \n\
		l_pos						 INT		 NOT NULL, \n\
		l_mapq						SMALLINT	NOT NULL, \n\
		l_cigar					   STRING	  NOT NULL, \n\
		l_rnext					   STRING	  NOT NULL, \n\
		l_pnext					   INT		 NOT NULL, \n\
		l_tlen						INT		 NOT NULL, \n\
		l_seq						 STRING	  NOT NULL, \n\
		l_qual						STRING	  NOT NULL, \n\
		r_flag						SMALLINT	NOT NULL, \n\
		r_rname					   STRING	  NOT NULL, \n\
		r_pos						 INT		 NOT NULL, \n\
		r_mapq						SMALLINT	NOT NULL, \n\
		r_cigar					   STRING	  NOT NULL, \n\
		r_rnext					   STRING	  NOT NULL, \n\
		r_pnext					   INT		 NOT NULL, \n\
		r_tlen						INT		 NOT NULL, \n\
		r_seq						 STRING	  NOT NULL, \n\
		r_qual						STRING	  NOT NULL, \n\
		CONSTRAINT paired_primary_alignments_"LLFMT"_pkey_l_virtual_offset_r_virtual_offset \n\
			PRIMARY KEY (l_virtual_offset, r_virtual_offset) \n\
	); \n\
	\n\
	CREATE TABLE bam.paired_secondary_alignments_"LLFMT" ( \n\
		l_virtual_offset			  BIGINT	  NOT NULL, \n\
		r_virtual_offset			  BIGINT	  NOT NULL, \n\
		qname						 STRING	  NOT NULL, \n\
		l_flag						SMALLINT	NOT NULL, \n\
		l_rname					   STRING	  NOT NULL, \n\
		l_pos						 INT		 NOT NULL, \n\
		l_mapq						SMALLINT	NOT NULL, \n\
		l_cigar					   STRING	  NOT NULL, \n\
		l_rnext					   STRING	  NOT NULL, \n\
		l_pnext					   INT		 NOT NULL, \n\
		l_tlen						INT		 NOT NULL, \n\
		l_seq						 STRING	  NOT NULL, \n\
		l_qual						STRING	  NOT NULL, \n\
		r_flag						SMALLINT	NOT NULL, \n\
		r_rname					   STRING	  NOT NULL, \n\
		r_pos						 INT		 NOT NULL, \n\
		r_mapq						SMALLINT	NOT NULL, \n\
		r_cigar					   STRING	  NOT NULL, \n\
		r_rnext					   STRING	  NOT NULL, \n\
		r_pnext					   INT		 NOT NULL, \n\
		r_tlen						INT		 NOT NULL, \n\
		r_seq						 STRING	  NOT NULL, \n\
		r_qual						STRING	  NOT NULL, \n\
		CONSTRAINT paired_secondary_alignments_"LLFMT"_pkey_l_virtual_offset_r_virtual_offset \n\
			PRIMARY KEY (l_virtual_offset, r_virtual_offset) \n\
	); \n\
	\n\
	CREATE TABLE bam.unpaired_alignments_"LLFMT" ( \n\
		virtual_offset				BIGINT	  NOT NULL, \n\
		qname						 STRING	  NOT NULL, \n\
		flag						  SMALLINT	NOT NULL, \n\
		rname						 STRING	  NOT NULL, \n\
		pos						   INT		 NOT NULL, \n\
		mapq						  SMALLINT	NOT NULL, \n\
		cigar						 STRING	  NOT NULL, \n\
		rnext						 STRING	  NOT NULL, \n\
		pnext						 INT		 NOT NULL, \n\
		tlen						  INT		 NOT NULL, \n\
		seq						   STRING	  NOT NULL, \n\
		qual						  STRING	  NOT NULL, \n\
		CONSTRAINT unpaired_alignments_"LLFMT"_pkey_virtual_offset PRIMARY KEY (virtual_offset) \n\
	); \n\
	\n\
	CREATE TABLE bam.alignments_extra_"LLFMT" ( \n\
	tag						   CHAR(2)	 NOT NULL, \n\
	virtual_offset				BIGINT	  NOT NULL, \n\
	type						  CHAR(1)	 NOT NULL, \n\
	value						 STRING, \n\
	CONSTRAINT alignments_extra_"LLFMT"_pkey_tag_virtual_offset PRIMARY KEY (tag, virtual_offset) \n\
	); \n\
	\n\
	CREATE VIEW bam.unpaired_primary_alignments_"LLFMT" AS \n\
		SELECT l_virtual_offset AS virtual_offset, qname, l_flag AS flag, l_rname AS rname, l_pos AS pos, l_mapq AS mapq, \n\
			l_cigar AS cigar, l_rnext AS rnext, l_pnext AS pnext, l_tlen AS tlen, l_seq AS seq, l_qual AS qual \n\
		FROM bam.paired_primary_alignments_"LLFMT" \n\
		UNION ALL \n\
		SELECT r_virtual_offset AS virtual_offset, qname, r_flag AS flag, r_rname AS rname, r_pos AS pos, r_mapq AS mapq, \n\
			r_cigar AS cigar, r_rnext AS rnext, r_pnext AS pnext, r_tlen AS tlen, r_seq AS seq, r_qual AS qual \n\
		FROM bam.paired_primary_alignments_"LLFMT"; \n\
	\n\
	CREATE VIEW bam.unpaired_secondary_alignments_"LLFMT" AS \n\
		SELECT l_virtual_offset AS virtual_offset, qname, l_flag AS flag, l_rname AS rname, l_pos AS pos, l_mapq AS mapq, \n\
			l_cigar AS cigar, l_rnext AS rnext, l_pnext AS pnext, l_tlen AS tlen, l_seq AS seq, l_qual AS qual \n\
		FROM bam.paired_secondary_alignments_"LLFMT" \n\
		UNION ALL \n\
		SELECT r_virtual_offset AS virtual_offset, qname, r_flag AS flag, r_rname AS rname, r_pos AS pos, r_mapq AS mapq, \n\
			r_cigar AS cigar, r_rnext AS rnext, r_pnext AS pnext, r_tlen AS tlen, r_seq AS seq, r_qual AS qual \n\
		FROM bam.paired_secondary_alignments_"LLFMT"; \n\
	\n\
	CREATE VIEW bam.unpaired_all_alignments_"LLFMT" AS \n\
	SELECT * \n\
	FROM bam.unpaired_primary_alignments_"LLFMT" \n\
	UNION ALL \n\
	SELECT * \n\
	FROM bam.unpaired_secondary_alignments_"LLFMT" \n\
	UNION ALL \n\
	SELECT * \n\
	FROM bam.unpaired_alignments_"LLFMT";"

#define SQL_DROP_HEADER \
	"DELETE FROM bam.pg WHERE file_id = "LLFMT";\n" \
	"DELETE FROM bam.rg WHERE file_id = "LLFMT";\n" \
	"DELETE FROM bam.sq WHERE file_id = "LLFMT";\n" \
	"DELETE FROM bam.files WHERE file_id = "LLFMT";\n"

#define SQL_DROP_STORAGE_0 \
	"DROP TABLE bam.alignments_extra_"LLFMT";\n" \
	"DROP TABLE bam.alignments_"LLFMT";\n"

#define SQL_DROP_STORAGE_1 \
	"DROP TABLE bam.alignments_extra_"LLFMT";\n" \
	"DROP VIEW bam.unpaired_all_alignments_"LLFMT";\n"\
	"DROP VIEW bam.unpaired_secondary_alignments_"LLFMT";\n"\
	"DROP VIEW bam.unpaired_primary_alignments_"LLFMT";\n"\
	"DROP TABLE bam.paired_primary_alignments_"LLFMT";\n" \
	"DROP TABLE bam.paired_secondary_alignments_"LLFMT";\n" \
	"DROP TABLE bam.unpaired_alignments_"LLFMT";\n"



#define SQL_COPY_INTO_FILES "COPY BINARY INTO bam.files FROM ('%s', '%s', '%s', '%s', '%s', '%s');\n"
#define SQL_COPY_INTO_SQ	"COPY BINARY INTO bam.sq	FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"
#define SQL_COPY_INTO_RG	"COPY BINARY INTO bam.rg	FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"
#define SQL_COPY_INTO_PG	"COPY BINARY INTO bam.pg	FROM ('%s', '%s', '%s', '%s', '%s', '%s');\n"

#define SQL_COPY_INTO_ALIGNMENTS	"COPY BINARY INTO bam.%salignments_"LLFMT" FROM \
	('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"
#define SQL_COPY_INTO_PAIRED_ALIGNMENTS "COPY BINARY INTO bam.paired_%s_alignments_"LLFMT" FROM \
	('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', \
	 '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"

#define SQL_COPY_INTO_ALIGNMENTS_EXTRA "COPY BINARY INTO bam.alignments_extra_"LLFMT" FROM ('%s', '%s', '%s', '%s');\n"

#define BUF_SIZE_CREATE_STORAGE_0 2048
#define BUF_SIZE_CREATE_STORAGE_1 8192
#define BUF_SIZE_DROP_FILE 1024
#define BUF_SIZE_COPY_INTO 8192




/* Some buffers */
static char buf_sql_create_storage_0[BUF_SIZE_CREATE_STORAGE_0];
static char buf_sql_create_storage_1[BUF_SIZE_CREATE_STORAGE_1];
static char buf_sql_drop_file[BUF_SIZE_DROP_FILE];
static char buf_sql_copy_into[BUF_SIZE_COPY_INTO];



str
bind_bam_schema(mvc * m, sql_schema ** ret)
{
	sql_schema *result;

	if ((result = mvc_bind_schema(m, "bam")) == NULL) {
		throw(MAL, "bind_bam_schema",
				   "Could not find bam schema");
	}
	if (ret)
		*ret = result;

	return MAL_SUCCEED;
}

str
bind_table(mvc * m, sql_schema * s,
			   str tablename, sql_table ** ret)
{
	sql_table *result;

	if ((result = mvc_bind_table(m, s, tablename)) == NULL) {
		throw(MAL, "bind_table",
				   "Could not find table %s", tablename);
	}
	if (ret)
		*ret = result;

	return MAL_SUCCEED;
}



/**
 * Function returns the highest file id that currently exists in the
 * bam.files table and stores this number + 1 in the next_file_id
 * variable. Function doesn't use a mutex to guarantee that the
 * returned value is up to date at return time. If this is required,
 * the calling function should activate a mutex.
 */
str
next_file_id(mvc * m, sql_table * files, lng * next_file_id)
{
	sql_column *c;
	BAT *b = NULL;
	BATiter li;
	BUN p = 0, q = 0;
	lng max_file_id = 0;

	sht i;

	assert(m != NULL);
	assert(files != NULL);

	/* Try to bind the file_id column of the bam.files table */
	if ((c = mvc_bind_column(m, files, "file_id")) == NULL) {
		throw(MAL, "next_file_id",
			  "Could not retrieve the next file id: Error binding file_id column of 'files' table");
	}

	/* Loop through BATs for this column and find the maximum file_id */
	for(i=0; i<3; ++i) {
		b = store_funcs.bind_col(m->session->tr, c, i);

		li = bat_iterator(b);
		BATloop(b, p, q) {
			lng t = *(lng *) BUNtail(li, p);
			max_file_id = MAX(max_file_id, t);
		}
		BBPunfix(b->batCacheid);
	}

	*next_file_id = max_file_id + 1;
	return MAL_SUCCEED;
}

str
create_alignment_storage_0(Client cntxt, str descr, bam_wrapper * bw)
{
	str sql_create_storage = buf_sql_create_storage_0;
	str msg;

	snprintf(sql_create_storage, BUF_SIZE_CREATE_STORAGE_0,
		 SQL_CREATE_STORAGE_0, bw->file_id, bw->file_id, bw->file_id,
		 bw->file_id, bw->file_id, bw->file_id);
	RUN_SQL(cntxt, &sql_create_storage, descr, msg);
	if (msg != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "create_alignment_storage_0",
				"Could not create alignment storage for file '%s' (file id '"
				LLFMT "'): %s", bw->file_location,
				bw->file_id, msg);
	}
	return msg;
}

str
create_alignment_storage_1(Client cntxt, str descr, bam_wrapper * bw)
{
	str sql_create_storage = buf_sql_create_storage_1;
	str msg;

	snprintf(sql_create_storage, BUF_SIZE_CREATE_STORAGE_1,
		 SQL_CREATE_STORAGE_1, bw->file_id, bw->file_id, bw->file_id,
		 bw->file_id, bw->file_id, bw->file_id, bw->file_id,
		 bw->file_id, bw->file_id, bw->file_id, bw->file_id,
		 bw->file_id, bw->file_id, bw->file_id, bw->file_id,
		 bw->file_id, bw->file_id, bw->file_id);
	RUN_SQL(cntxt, &sql_create_storage, descr, msg);
	if (msg != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "create_alignment_storage_1",
				"Could not create alignment storage for file '%s' (file id '"
				LLFMT "'): %s", bw->file_location,
				bw->file_id, msg);
	}
	return msg;
}


/* Macro only used by copy_into_db to increase len by step only if it
 * is positive and throw an exception otherwise */
#define CHECK_STEP(len, step, table) {\
	if(step < 0) { \
		throw(MAL, "copy_into_db", "Could not construct SQL string for copying data to '"table"' table"); \
	} \
	len += step; \
}

/**
 * Copies the binary files, of which the paths are stored in
 * bam_wrapper, into the database using the Client context.
 */
str
copy_into_db(Client cntxt, bam_wrapper * bw)
{
	str sql_copy_into = buf_sql_copy_into;
	str msg;
	int step, len = 0;

	step = snprintf(sql_copy_into, BUF_SIZE_COPY_INTO,
			SQL_COPY_INTO_FILES, bw->fp_files[0], bw->fp_files[1],
			bw->fp_files[2], bw->fp_files[3], bw->fp_files[4],
			bw->fp_files[5]);
	CHECK_STEP(len, step, "files");

	if (bw->cnt_sq > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_SQ, bw->fp_sq[0], bw->fp_sq[1],
				bw->fp_sq[2], bw->fp_sq[3], bw->fp_sq[4],
				bw->fp_sq[5], bw->fp_sq[6]);
		CHECK_STEP(len, step, "sq");
	}

	if (bw->cnt_rg > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_RG, bw->fp_rg[0], bw->fp_rg[1],
				bw->fp_rg[2], bw->fp_rg[3], bw->fp_rg[4],
				bw->fp_rg[5], bw->fp_rg[6], bw->fp_rg[7],
				bw->fp_rg[8], bw->fp_rg[9], bw->fp_rg[10],
				bw->fp_rg[11], bw->fp_rg[12]);
		CHECK_STEP(len, step, "rg");
	}

	if (bw->cnt_pg > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_PG, bw->fp_pg[0], bw->fp_pg[1],
				bw->fp_pg[2], bw->fp_pg[3], bw->fp_pg[4],
				bw->fp_pg[5]);
		CHECK_STEP(len, step, "pg");
	}

	if (bw->cnt_alignments > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_ALIGNMENTS,
				(bw->dbschema == 1 ? "unpaired_" : ""),
				bw->file_id, bw->fp_alignments[0],
				bw->fp_alignments[1], bw->fp_alignments[2],
				bw->fp_alignments[3], bw->fp_alignments[4],
				bw->fp_alignments[5], bw->fp_alignments[6],
				bw->fp_alignments[7], bw->fp_alignments[8],
				bw->fp_alignments[9], bw->fp_alignments[10],
				bw->fp_alignments[11]);
		CHECK_STEP(len, step, "alignments");
	}

	if (bw->cnt_alignments_paired_primary > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_PAIRED_ALIGNMENTS, "primary",
				bw->file_id,
				bw->fp_alignments_paired_primary[0],
				bw->fp_alignments_paired_primary[1],
				bw->fp_alignments_paired_primary[2],
				bw->fp_alignments_paired_primary[3],
				bw->fp_alignments_paired_primary[4],
				bw->fp_alignments_paired_primary[5],
				bw->fp_alignments_paired_primary[6],
				bw->fp_alignments_paired_primary[7],
				bw->fp_alignments_paired_primary[8],
				bw->fp_alignments_paired_primary[9],
				bw->fp_alignments_paired_primary[10],
				bw->fp_alignments_paired_primary[11],
				bw->fp_alignments_paired_primary[12],
				bw->fp_alignments_paired_primary[13],
				bw->fp_alignments_paired_primary[14],
				bw->fp_alignments_paired_primary[15],
				bw->fp_alignments_paired_primary[16],
				bw->fp_alignments_paired_primary[17],
				bw->fp_alignments_paired_primary[18],
				bw->fp_alignments_paired_primary[19],
				bw->fp_alignments_paired_primary[20],
				bw->fp_alignments_paired_primary[21],
				bw->fp_alignments_paired_primary[22]);
		CHECK_STEP(len, step, "alignments_paired_primary");
	}

	if (bw->cnt_alignments_paired_secondary > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_PAIRED_ALIGNMENTS, "secondary",
				bw->file_id,
				bw->fp_alignments_paired_secondary[0],
				bw->fp_alignments_paired_secondary[1],
				bw->fp_alignments_paired_secondary[2],
				bw->fp_alignments_paired_secondary[3],
				bw->fp_alignments_paired_secondary[4],
				bw->fp_alignments_paired_secondary[5],
				bw->fp_alignments_paired_secondary[6],
				bw->fp_alignments_paired_secondary[7],
				bw->fp_alignments_paired_secondary[8],
				bw->fp_alignments_paired_secondary[9],
				bw->fp_alignments_paired_secondary[10],
				bw->fp_alignments_paired_secondary[11],
				bw->fp_alignments_paired_secondary[12],
				bw->fp_alignments_paired_secondary[13],
				bw->fp_alignments_paired_secondary[14],
				bw->fp_alignments_paired_secondary[15],
				bw->fp_alignments_paired_secondary[16],
				bw->fp_alignments_paired_secondary[17],
				bw->fp_alignments_paired_secondary[18],
				bw->fp_alignments_paired_secondary[19],
				bw->fp_alignments_paired_secondary[20],
				bw->fp_alignments_paired_secondary[21],
				bw->fp_alignments_paired_secondary[22]);
		CHECK_STEP(len, step, "alignments_paired_secondary");
	}

	if (bw->cnt_alignments_extra > 0) {
		step = snprintf(sql_copy_into + len, BUF_SIZE_COPY_INTO - len,
				SQL_COPY_INTO_ALIGNMENTS_EXTRA, bw->file_id,
				bw->fp_alignments_extra[0],
				bw->fp_alignments_extra[1],
				bw->fp_alignments_extra[2],
				bw->fp_alignments_extra[3]);
		CHECK_STEP(len, step, "alignments_extra");
	}

	RUN_SQL(cntxt, &sql_copy_into, "bam.file_copy_into", msg);
	if (msg != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "copy_into_db",
				"Could not copy data from binary files into the database for file '%s' (file id '"
				LLFMT "'): '%s'", bw->file_location,
				bw->file_id, msg);
	}
	return msg;
}

str
drop_file(Client cntxt, str descr, lng file_id, sht dbschema)
{
	str sql_drop_file = buf_sql_drop_file;
	int len;
	str msg;


	/* Below is a first effort to automatically retrieve the
	 * dbschema for the given file_id, however due to limited
	 * time, this is not done yet.  Therefore, it is commented
	 * out */

	/* First, find out the dbschema of the record with the given file id */
	/*mvc *m;
	 * sql_table *t_files;
	 * sql_column *c_file_id;
	 * BAT *b_file_id = NULL;
	 * BATiter iter_file_id;
	 * sht i;
	 * BUN p = 0, q = 0;
	 * str file_location = NULL;
	 *
	 * str msg;
	 */
	/* Retrieve SQL context */
	/*if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED) {
	 * REUSE_EXCEPTION(msg, MAL, "drop_file_by_id", "Could not retrieve SQL context: %s", msg);
	 * return msg;
	 * } */

	/* Retrieve bam.files table */
	/*if((t_files = mvc_bind_table(m, s, tablename)) == NULL) {
	 * throw(MAL, "drop_file_by_id", "Could not bind table 'bam.files'");
	 * } */

	/* Try to bind the file_id column of the bam.files table */
	/*if((c_file_id = mvc_bind_column(m, t_files, "file_id")) == NULL) {
	 * throw(MAL, "drop_file_by_id", "Could not bind to file_id column of table 'bam.files'");
	 * } */

	/*for(i=0; i<3; ++i) {
	 * b_file_id = store_funcs.bind_col(m->session->tr, c_file_id, i);
	 * if(b_file_id != NULL) {
	 * iter_file_id = bat_iterator(b_file_id);
	 * BATloop(b_file_id, p, q) {
	 * lng t = *(lng *)BUNtail(iter_file_id, p);
	 * if(t == file_id) { */
	/* We found the right file id, store oid */
	/*tuple_oid = *(oid *)BUNhead(iter_file_id, q);
	 * BBPunfix(b->batCacheid);
	 * break;
	 * }
	 * max_file_id = MAX(max_file_id, t);
	 * }
	 * BBPunfix(b->batCacheid);
	 * }
	 * if(oid != NULL) { */
	/* oid of record with right file_id is found; we can quit this loop */
	/*break;
	 * }
	 * }
	 *
	 * if(oid == NULL) {
	 * throw(MAL, "drop_file_by_id", "File entry with file_id='"LLFMT"' not found", file_id);
	 * } */

	if (dbschema == 0) {
		len = snprintf(sql_drop_file, BUF_SIZE_DROP_FILE,
				   SQL_DROP_STORAGE_0, file_id, file_id);
	} else {
		len = snprintf(sql_drop_file, BUF_SIZE_DROP_FILE,
				   SQL_DROP_STORAGE_1, file_id, file_id, file_id,
				   file_id, file_id, file_id, file_id);
	}
	if (len < -1) {
		throw(MAL, "drop_file",
			  "Could not construct SQL string for dropping file");
	}
	snprintf(sql_drop_file + len, BUF_SIZE_DROP_FILE - len,
		 SQL_DROP_HEADER, file_id, file_id, file_id, file_id);

	RUN_SQL(cntxt, &sql_drop_file, descr, msg);
	return msg;
}
