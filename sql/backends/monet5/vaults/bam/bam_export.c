/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file defines export functionality, that enables
 * users to write data from the bam.export table to a SAM or BAM file.
 * After exporting data, the exporting functions will automatically
 * empty the bam.export table.
 */

#include "monetdb_config.h"

#ifdef HAVE_SAMTOOLS_BAM_H
#include <samtools/bam.h>
#else
#include <bam/bam.h>
#endif

#include "bam_globals.h"
#include "bam_db_interface.h"
#include "bam_export.h"

#define NOT_IMPLEMENTED		/* export is not yet implemented */

typedef struct bam_field {
	str name;
	sql_column *c;
	BAT *b;
	BATiter iter;
	BUN cur;
} bam_field;


#ifndef NOT_IMPLEMENTED

/**
 * Copied directly from bam.h/bam_import.c for use by fill_bam_alig
 * Can not change the calls to realloc to GDKrealloc, since
 * bam_destroy1 does not use GDKfree...
 */
#ifndef kroundup32
/*! @function
  @abstract  Round an integer to the next closest power-2 integer.
  @param  x  integer to be rounded (in place)
  @discussion x will be modified.
 */
#define kroundup32(x) (--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4, (x)|=(x)>>8, (x)|=(x)>>16, ++(x))
#endif
static inline uint8_t *
alloc_data(bam1_t *b, int size)
{
	if (b->m_data < size) {
		b->m_data = size;
		kroundup32(b->m_data);
		b->data = (uint8_t*)realloc(b->data, b->m_data);
	}
	return b->data;
}

/* Error macro */
#define FILL_BAM_ALIG_ERR "Error processing alignment '%d': "

/**
 * I had to write this function, which contains much low level
 * function calls to the Samtools library, since the sam_read1 function
 * contained in bam.h/bam_import.c only works on files. This function is
 * an adjusted variant of the sam_read1 function that gets its fields
 * directly instead of reads it from a tamFile.
 * I wrote this function since I did not feel like first writing everything
 * to a SAM file and then applying sam_read1 since that is just a waste of I/O.
 * Note that I did do a write to/read from SAM for the BAM header, but headers are
 * insignificant compared to alignment data.
 *
 * I also added realloc checking, since this was not done in the Samtools code
 * and i properly throw a MAL_MALLOC_FAIL whenever applicable.
 */
static str
fill_bam_alig(str qname, sht flag, str rname, int pos,
	sht mapq, str cigar, str rnext, int pnext,
	int tlen, str seq, str qual,
	bam_header_t *header, bam1_t *b, int alignment_nr)
{
	int doff = 0;
	bam1_core_t *c = &b->core;

	/* Note: in sam_read1, str->s is often used. This string is not NULL terminated! */
	{ // qname
		c->l_qname = strlen(qname) + 1;
		if(alloc_data(b, doff + c->l_qname) == NULL) {
			throw(MAL, "fill_bam_alig",
				FILL_BAM_ALIG_ERR MAL_MALLOC_FAIL, alignment_nr);
		}
		memcpy(b->data + doff, qname, c->l_qname);
		doff += c->l_qname;
	}
	{ // flag
		c->flag = flag;
	}
	{ // rname, pos, mapq
		c->tid = bam_get_tid(header, rname);
		if (c->tid < 0 && strcmp(rname, "*") != 0) {
			/* Should not happen, since we built the header structure ourselves */
			throw(MAL, "fill_bam_alig",
				FILL_BAM_ALIG_ERR "SQ entry '%s' not found in header table",
				alignment_nr, rname);
		}
		c->pos = pos - 1;
		c->qual = mapq;
	}
	{ // cigar
		str s, t;
		int i, op;
		long x; /* type long returned by strtol() */
		c->n_cigar = 0;
		if (cigar[0] != '*') {
			uint32_t *cigar_enc;
			for (s = cigar; *s != '\0'; ++s) {
				if ((isalpha(*s)) || (*s=='=')) {
					++c->n_cigar;
				}
				else if (!isdigit(*s)) {
					throw(MAL, "fill_bam_alig",
						FILL_BAM_ALIG_ERR "Parse error while parsing CIGAR string '%s'",
						alignment_nr, cigar);
				}
			}
			if (alloc_data(b, doff + c->n_cigar * 4) == NULL) {
				throw(MAL, "fill_bam_alig",
					FILL_BAM_ALIG_ERR MAL_MALLOC_FAIL, alignment_nr);
			}
			cigar_enc = bam1_cigar(b);
			for (i = 0, s = cigar; i != c->n_cigar; ++i) {
				x = strtol(s, &t, 10);
				op = toupper(*t);
				if (op == 'M') op = BAM_CMATCH;
				else if (op == 'I') op = BAM_CINS;
				else if (op == 'D') op = BAM_CDEL;
				else if (op == 'N') op = BAM_CREF_SKIP;
				else if (op == 'S') op = BAM_CSOFT_CLIP;
				else if (op == 'H') op = BAM_CHARD_CLIP;
				else if (op == 'P') op = BAM_CPAD;
				else if (op == '=') op = BAM_CEQUAL;
				else if (op == 'X') op = BAM_CDIFF;
				else if (op == 'B') op = BAM_CBACK;
				else throw(MAL, "fill_bam_alig",
					FILL_BAM_ALIG_ERR "invalid CIGAR operation found in CIGAR string '%s': '%c'",
					alignment_nr, cigar, op);
				s = t + 1;
				cigar_enc[i] = bam_cigar_gen(x, op);
			}
			if (*s) throw(MAL, "fill_bam_alig",
				FILL_BAM_ALIG_ERR "Unmatched CIGAR operation in CIGAR string '%s'",
				alignment_nr, cigar);
			c->bin = bam_reg2bin(c->pos, bam_calend(c, cigar_enc));
			doff += c->n_cigar * 4;
		} else {
			if (!(c->flag&BAM_FUNMAP)) {
				c->flag |= BAM_FUNMAP;
			}
			c->bin = bam_reg2bin(c->pos, c->pos + 1);
		}
	}
	{ // rnext, pnext, tlen
		c->mtid = strcmp(rnext, "=") != 0 ? bam_get_tid(header, rnext) : c->tid;
		c->mpos = pnext - 1;
		c->isize = tlen;
	}
	{ // seq and qual
		int i;
		uint8_t *p = 0;
		if (strcmp(seq, "*") != 0) {
			c->l_qseq = strlen(seq);
			if (c->n_cigar && c->l_qseq != (int32_t)bam_cigar2qlen(c, bam1_cigar(b))) {
				throw(MAL, "fill_bam_alig",
					FILL_BAM_ALIG_ERR "CIGAR and sequence length inconsistency: %d (SEQ) vs %d (CIGAR)",
					alignment_nr, c->l_qseq, (int32_t)bam_cigar2qlen(c, bam1_cigar(b)));
			}
			p = (uint8_t*)alloc_data(b, doff + c->l_qseq + (c->l_qseq+1)/2);
			if(p == NULL) {
				throw(MAL, "fill_bam_alig",
					FILL_BAM_ALIG_ERR MAL_MALLOC_FAIL, alignment_nr);
			}
			p += doff;
			memset(p, 0, (c->l_qseq+1)/2);
			for (i = 0; i < c->l_qseq; ++i)
				p[i/2] |= bam_nt16_table[(int)seq[i]] << 4*(1-i%2);
		} else {
			c->l_qseq = 0;
		}
		if (strcmp(qual, "*") != 0 && (size_t)c->l_qseq != strlen(qual)) {
			throw(MAL, "fill_bam_alig",
				FILL_BAM_ALIG_ERR "SEQ and QUAL are inconsistent",
				alignment_nr);
		}
		p += (c->l_qseq+1)/2;
		if (strcmp(qual, "*") == 0) {
			for (i = 0; i < c->l_qseq; ++i) {
				p[i] = 0xff;
			}
		} else {
			for (i = 0; i < c->l_qseq; ++i) {
				p[i] = qual[i] - 33;
			}
		}
		doff += c->l_qseq + (c->l_qseq+1)/2;
	}

	b->data_len = doff;
	if (bam_no_B) {
		bam_remove_B(b);
	}

	return MAL_SUCCEED;
}
#endif




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
		fields[i].cur = 0;
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
			str *tmp;
			if((tmp = GDKrealloc(sq_table, new_size * sizeof(str))) == NULL) {
				msg = createException(MAL, "write_header", MAL_MALLOC_FAIL);
				goto cleanup;
			}
			sq_table = tmp;

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
				(BATtordered(fields[2].b) && BATtordered(fields[3].b) ? "coordinate" :
					"unsorted"));

	/* Use sq_table to print SQ header lines */
	for (i=0; i<sq_table_count; ++i) {
		mnstr_printf(output, "@SQ\tSN:%s\tLN:0\n", sq_table[i]);
		//mnstr_printf(output, "%s\t0\n", sq_table[i]);
	}

	mnstr_printf(output, "@CO\tSAM file generated by MonetDB\n");

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
			BBPunfix(fields[i].b->batCacheid);
		}
	}
}


#define CUR_STR(field, i) ((str) BUNtail(field.iter, (field.cur+i)))
#define CUR_SHT(field, i) (*(sht *) BUNtail(field.iter, (field.cur+i)))
#define CUR_INT(field, i) (*(int *) BUNtail(field.iter, (field.cur+i)))

str
sam_exportf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* arg 1: path to desired output file */
	str output_path = *getArgReference_str(stk, pci, pci->retc);


	stream *output = NULL;
	bam_field fields[11];
	int tuple_count = 0;

	int i;
	str sql;
	str msg = MAL_SUCCEED;

	memset(fields, 0, 11 * sizeof(bam_field));

	if ((output = bsopen(output_path)) == NULL) {
		msg = createException(MAL, "sam_export", "Could not open output file '%s' for writing", output_path);
		goto cleanup;
	}

	if((msg = bind_export_result(cntxt, mb, fields, &tuple_count)) != MAL_SUCCEED) {
		goto cleanup;
	}

	if ((msg = write_header(output, fields)) != MAL_SUCCEED)
		goto cleanup;


	for (i=0; i<tuple_count; ++i) {
		mnstr_printf(output, "%s\t%d\t%s\t%d\t%d\t%s\t%s\t%d\t%d\t%s\t%s\n",
				CUR_STR(fields[0], i), CUR_SHT(fields[1], i), CUR_STR(fields[2], i),
				CUR_INT(fields[3], i), CUR_SHT(fields[4], i), CUR_STR(fields[5], i),
				CUR_STR(fields[6], i), CUR_INT(fields[7], i), CUR_INT(fields[8], i),
				CUR_STR(fields[9], i), CUR_STR(fields[10], i));
	}

	/* If we got here, we succesfully exported the result. Drop all data in export table */
	sql = "DELETE FROM bam.export;";
	RUN_SQL(cntxt, &sql, "bam.drop_export", msg);
	if (msg != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "sam_export", "Could not clear the export table after exporting: %s", msg);
	}

	(void)stk;
	(void)pci;

cleanup:
	cleanup_fields(fields);
	if (output) {
		close_stream(output);
	}

	return msg;
}



str
bam_exportf(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
#ifdef NOT_IMPLEMENTED
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;

	throw(MAL, "bam_export", "Exporting to BAM files is not implemented yet. This is our first priority for the next release of the BAM library.");
#else
	/* arg 1: path to desired output file */
	str output_path = *getArgReference_str(stk, pci, pci->retc);

	bamFile output = NULL;
	bam_field fields[11];
	int tuple_count = 0;

	char output_header_path[1024] = "";
	stream *output_header = NULL;
	tamFile sam_header = NULL;
	bam_header_t *header = NULL;

	bam1_t *alig = NULL;

	int i;
	str sql;
	str msg = MAL_SUCCEED;

	if ((output = bam_open(output_path, "wb")) == NULL) {
		msg = createException(MAL, "bam_export", "Could not open output file '%s' for writing", output_path);
		goto cleanup;
	}

	snprintf(output_header_path, 1024, "%s_tmp.sam", output_path);
	if ((output_header = bsopen(output_header_path)) == NULL) {
		msg = createException(MAL, "bam_export", "Could not open temporary output file '%s' for writing", output_header_path);
		goto cleanup;
	}

	if((msg = bind_export_result(cntxt, mb, fields, &tuple_count)) != MAL_SUCCEED) {
		goto cleanup;
	}

	write_header(output_header, fields);
	close_stream(output_header);
	output_header = NULL;

	if((sam_header = sam_open(output_header_path)) == NULL) {
		msg = createException(MAL, "bam_export", "Could not open temporarily written SAM header file '%s'", output_header_path);
		goto cleanup;
	}

	if((header = sam_header_read(sam_header)) == NULL) {
		msg = createException(MAL, "bam_export", "Could not parse the temporarily written SAM header in file '%s'", output_header_path);
		goto cleanup;
	}

	bam_header_write(output, header);


	if((alig = bam_init1()) == NULL) {
		throw(MAL, "bam_export", MAL_MALLOC_FAIL);
	}

	for (i=0; i<tuple_count; ++i) {
		msg = fill_bam_alig(
			CUR_STR(fields[0], i), CUR_SHT(fields[1], i), CUR_STR(fields[2], i),
			CUR_INT(fields[3], i), CUR_SHT(fields[4], i), CUR_STR(fields[5], i),
			CUR_STR(fields[6], i), CUR_INT(fields[7], i), CUR_INT(fields[8], i),
			CUR_STR(fields[9], i), CUR_STR(fields[10], i),
			header, alig, i+1);

		if(msg != MAL_SUCCEED) {
			goto cleanup;
		}

		bam_write1(output, alig);
	}

	/* If we got here, we succesfully exported the result. Drop all data in export table */
	sql = "DELETE FROM bam.export;";
	RUN_SQL(cntxt, &sql, "bam.drop_export", msg);
	if (msg != MAL_SUCCEED) {
		REUSE_EXCEPTION(msg, MAL, "sam_export", "Could not clear the export table after exporting: %s", msg);
	}

	(void)stk;
	(void)pci;

cleanup:
	cleanup_fields(fields);
	if (output) {
		bam_close(output);
	}
	if(output_header) {
		close_stream(output_header);
	}
	//if(sam_header) {
	//	sam_close(sam_header);
	//}
	if(header) {
		bam_header_destroy(header);
	}
	if(alig) {
		bam_destroy1(alig);
	}
	if (strcmp(output_header_path, "") != 0) {
		unlink(output_header_path);
	}
	return msg;
#endif
}

