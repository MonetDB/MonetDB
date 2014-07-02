/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (author) R Cijvat
 * The code in this file defines export functionality, that enables
 * users to write data from the bam.export table to a SAM or BAM file.
 * After exporting data, the exporting functions will automatically
 * empty the bam.export table.
 */

#include "monetdb_config.h"

#include <htslib/sam.h>
#include <htslib/kstring.h>

#include "bam_globals.h"
//#include "bam_db_interface.h"
#include "bam_export.h"


typedef struct bam_field {
	str name;
	sql_column *c;
	BAT *b;
	BATiter iter;
	BUN cur;
} bam_field;


static str
bind_export_result(Client cntxt, MalBlkPtr mb, bam_field fields[11], int *tuple_count)
{
	mvc *m;
	sql_schema *s_bam;
	sql_table *t_export;

	int cnt = int_nil, i;
	str msg;

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "bind_export_result", "Could not retrieve SQL context: %s", msg);
		return msg;
	}

	if ((s_bam = mvc_bind_schema(m, "bam")) == NULL) {
		throw(MAL, "bind_export_result", "Could not find bam schema");
	}

	if ((t_export = mvc_bind_table(m, s_bam, "export")) == NULL) {
		throw(MAL, "bind_export_result", "Could not find bam.export table");
	}

	fields[0].name = "qname";
	fields[1].name = "flag";
	fields[2].name = "rname";
	fields[3].name = "pos";
	fields[4].name = "mapq";
	fields[5].name = "cigar";
	fields[6].name = "rnext";
	fields[7].name = "pnext";
	fields[8].name = "tlen";
	fields[9].name = "seq";
	fields[10].name = "qual";

	for (i=0; i<11; ++i) {
		int cnt_tmp;
		if ((fields[i].c = mvc_bind_column(m, t_export, fields[i].name)) == NULL) {
			throw(MAL, "bind_export_result", "Could not find bam.export.%s column", fields[i].name);
		}
		if ((fields[i].b = store_funcs.bind_col(m->session->tr, fields[i].c, RDONLY)) == NULL) {
		   throw(MAL, "bind_export_result", RUNTIME_OBJECT_MISSING);
		}
		cnt_tmp = BATcount(fields[i].b);
		if (cnt_tmp <= 0) {
			throw(MAL, "bind_export_result", "The bam.export table is empty");
		}
		if (i > 0 && cnt != cnt_tmp) {
			throw(MAL, "bind_export_result", "Misalignment in bam.export table: "
					"column '%s' has %d values; expected %d",
					fields[i].name, cnt_tmp, cnt);
		}
		cnt = cnt_tmp;
		fields[i].iter = bat_iterator(fields[i].b);
		fields[i].cur = BUNfirst(fields[i].b);
	}
	*tuple_count = cnt;
	return MAL_SUCCEED;
}

static str
write_header(stream *output, bam_field fields[11])
{
	int sq_table_size = 32; /* 32 will normally be enough since there are mostly <30 chromosomes */
	int sq_table_count = 0; /* Number of actual strings in there */
	str *sq_table = NULL;
	BUN p, q;
	BATiter iter = bat_iterator(fields[2].b);

	int i;
	str msg = MAL_SUCCEED;

	/* Some vars for building the sq_table */
	str cur;
	bit str_found;


	/* Start by building the table with sequences */
	if((sq_table = GDKmalloc(sq_table_size * sizeof(str))) == NULL) {
		msg = createException(MAL, "write_header", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	/* Build sq_table */
	/* TODO: Consider a faster lookup method instead of this simple linear approach.
	 * One could e.g. use Samtools dict structure or implement its own dict structure.
	 * However, I am not sure if it is worth the effort, since in general the size of
	 * the table will be small (<30 chromosomes).
	 */
	BATloop(fields[2].b, p, q) {
		cur = (str) BUNtail(iter, p);

		/* Do not print unknown chromosome (*) to header */
		if(strcmp(cur, "*") == 0) {
			continue;
		}

		/* Check sq_table for chromosome cur. Start checking in the end of the array,
		 * since it is likely that alignments from the same chromosome are printed
		 * after each other */
		str_found = FALSE;
		for(i=sq_table_count-1; i>=0; --i) {
			if (strcmp(cur, sq_table[i]) == 0) {
				/* Chromosome was already stored in the table */
				str_found = TRUE;
				break;
			}
		}

		if (str_found) {
			continue;
		}

		/* We have to insert a new chromosome. First check if there is enough space */
		if (sq_table_count == sq_table_size - 1) {
			/* Not enough space... */
			int new_size = sq_table_size * 2;
			if((sq_table = GDKrealloc(sq_table, new_size * sizeof(str))) == NULL) {
				msg = createException(MAL, "write_header", MAL_MALLOC_FAIL);
				goto cleanup;
			}

			sq_table_size = new_size;
		}

		/* Insert chromosome */
		if ((sq_table[sq_table_count] = GDKstrdup(cur)) == NULL) {
			msg = createException(MAL, "write_header", MAL_MALLOC_FAIL);
			goto cleanup;
		}

		/* Insertion succeeded */
		++sq_table_count;
	}

	mnstr_printf(output, "@HD\tVN:1.0\tSO:%s\n",
			BATtordered(fields[0].b) ? "queryname" :
				(BATordered(fields[2].b) && BATordered(fields[3].b) ? "coordinate" :
					"unsorted"));

	/* Use sq_table to print SQ header lines */
	for (i=0; i<sq_table_count; ++i) {
		mnstr_printf(output, "@SQ\tSN:%s\tLN:0\n", sq_table[i]);
		//mnstr_printf(output, "%s\t0\n", sq_table[i]);
	}

	mnstr_printf(output, "@CO\tSAM file generated by MonetDB BAM library\n");

cleanup:
	if (sq_table) {
		for (i=0; i<sq_table_count; ++i) {
			GDKfree(sq_table[i]);
		}
		GDKfree(sq_table);
	}
	return msg;
}

static void
cleanup_fields(bam_field fields[11]) {
	int i;
	for (i=0; i<11; ++i) {
		if (fields[i].b != NULL) {
			BBPreleaseref(fields[i].b->batCacheid);
		}
	}
}


#define CUR_STR(field, i) ((str) BUNtail(field.iter, (field.cur+i)))
#define CUR_SHT(field, i) (*(sht *) BUNtail(field.iter, (field.cur+i)))
#define CUR_INT(field, i) (*(int *) BUNtail(field.iter, (field.cur+i)))

str
sam_export(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* arg 1: path to desired output file */
	str output_path = *(str *) getArgReference(stk, pci, pci->retc);


	stream *output = NULL;
	bam_field fields[11];
	int tuple_count = 0;

	int i;
	//str sql;
	str msg = MAL_SUCCEED;

	memset(fields, 0, 11 * sizeof(bam_field));

	if ((output = bsopen(output_path)) == NULL) {
		msg = createException(MAL, "sam_export", "Could not open output file '%s' for writing", output_path);
		goto cleanup;
	}

	if((msg = bind_export_result(cntxt, mb, fields, &tuple_count)) != MAL_SUCCEED) {
		goto cleanup;
	}

	write_header(output, fields);


	for (i=0; i<tuple_count; ++i) {
		mnstr_printf(output, "%s\t%d\t%s\t%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\n",
				CUR_STR(fields[0], i), CUR_SHT(fields[1], i), CUR_STR(fields[2], i),
				CUR_INT(fields[3], i), CUR_SHT(fields[4], i), CUR_STR(fields[5], i),
				CUR_STR(fields[6], i), CUR_INT(fields[7], i), CUR_INT(fields[8], i),
				CUR_STR(fields[9], i), CUR_STR(fields[10], i));
	}

	/* If we got here, we succesfully exported the result. Drop all data in export table */
	/*sql = "DELETE FROM bam.export;";
	RUN_SQL(cntxt, &sql, "bam.drop_export", msg);
	if (msg != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "sam_export", "Could not clear the export table after exporting: %s", msg);
	}*/

	(void)stk;
	(void)pci;

cleanup:
	cleanup_fields(fields);
	if (output) {
		close_stream(output);
	}

	return msg;
}


/**
 * bam_export uses htslib functionality to write the export table to an actual bam file
 * It does this by first writing the SAM header to a temporary SAM file, since the htslib provides 
 * convenient header parsing functionality on files. 
 * We could do a similar thing for the alignments, but this would add a whole lot of I/O. So we took
 * some extra effort to write the alignments to bam1_t structs ourselves (of course with help of
 * htslib )
 */
str
bam_export(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	/* arg 1: path to desired output file */
	str output_path = *(str *) getArgReference(stk, pci, pci->retc);

	/* Stuff necessary for writing header to BAM */
	char header_path[1024] = "";
	stream *output_header = NULL;
	samFile *sam_header = NULL;
	bam_hdr_t *header = NULL;

	/* Actual BAM output file and vars to write alignments */
	samFile *output = NULL;
	kstring_t sam_line;
	bam1_t *alig = NULL;
	char parse_err_buf[2048];
	FILE *original_stderr;

	/* Others */
	bam_field fields[11];
	int tuple_count = 0;
	int i;
	//str sql;
	str msg = MAL_SUCCEED;

	/* First, open sam file to write header to */
	snprintf(header_path, 1024, "%s_hdr.sam", output_path);
	if ((output_header = bsopen(header_path)) == NULL) {
		msg = createException(MAL, "bam_export", "Could not open temporary output file '%s' for writing", header_path);
		goto cleanup;
	}

	if((msg = bind_export_result(cntxt, mb, fields, &tuple_count)) != MAL_SUCCEED) {
		goto cleanup;
	}

	/* And write header */
	write_header(output_header, fields);
	close_stream(output_header);
	output_header = NULL;

	
	/* Header has been written, now let htslib parse it into a bam_hdr_t struct for us */
	if ((sam_header = sam_open(header_path, "r")) == 0) {
		msg = createException(MAL, "bam_export", "Could not open temporary header file '%s' for reading", header_path);
		goto cleanup;
	}

	if ((header = sam_hdr_read(sam_header)) == 0) {
		msg = createException(MAL, "bam_export", "Could not read header from temporary file '%s'", header_path);
		goto cleanup;
	}


	/* Open the BAM file to which we are going to write everything */
	if ((output = sam_open(output_path, "wb")) == NULL) {
		msg = createException(MAL, "bam_export", "Could not open output file '%s' for writing", output_path);
		goto cleanup;
	}


	/* Now write the header and the alignments to the BAM file */
	sam_hdr_write(output, header); //always returns 0, so we have to assume that it will work

	alig = bam_init1();
	original_stderr = stderr;
	stderr = fmemopen(parse_err_buf, 2048, "w"); /* Used to redirect error messages from htslib into parse_err_buf */
	for (i=0; i<tuple_count; ++i) {
		sam_line.l = sam_line.m = 0;
		sam_line.s = NULL;
		ksprintf(&sam_line, "%s\t%d\t%s\t%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s",
				CUR_STR(fields[0], i), CUR_SHT(fields[1], i), CUR_STR(fields[2], i),
				CUR_INT(fields[3], i), CUR_SHT(fields[4], i), CUR_STR(fields[5], i),
				CUR_STR(fields[6], i), CUR_INT(fields[7], i), CUR_INT(fields[8], i),
				CUR_STR(fields[9], i), CUR_STR(fields[10], i));

		if(sam_parse1(&sam_line, header, alig) != 0) {
			fclose(stderr);
			stderr = original_stderr;
			msg = createException(MAL, "bam_export", "Could not create BAM alignment for alignment number '%d': %s", i, parse_err_buf);
			goto cleanup;
		}
		free(sam_line.s);
		sam_line.s = NULL;

		/* And write the alignment! */
		sam_write1(output, header, alig);
	}

	fclose(stderr);
	stderr = original_stderr;

cleanup:
	if(strcmp(header_path, "") != 0) {
		unlink(header_path);
	}
	if(output_header) {
		close_stream(output_header);
	}
	if(sam_header) {
		sam_close(sam_header);
	}
	if(header) {
		bam_hdr_destroy(header);
	}

	if(output) {
		sam_close(output);
	}
	if(sam_line.s != NULL) {
		free(sam_line.s);
	}
	if(alig) {
		bam_destroy1(alig);
	}

	cleanup_fields(fields);

	return msg;
}

