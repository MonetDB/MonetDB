/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file handles all operations that are specific to
 * the BAM file format.  For many of these operations, the Samtools
 * API is used.
 */

#ifndef _BAM_WRAPPER_H
#define _BAM_WRAPPER_H

#include "gdk.h"

#ifdef HAVE_SAMTOOLS_SAM_H
#include <samtools/sam.h>
#else
#include <bam/sam.h>
#endif

#define BW_FP_BUF_SIZE 1024

typedef sht ordering;
typedef enum {SAM, BAM} filetype;

typedef struct sam_data {
	stream *input;
	str header;
} sam_data;

typedef struct bam_data {
	bamFile input;
	bam_header_t *header;
} bam_data;

typedef struct bam_wrapper {
	/* Type */
	filetype type;

	union {
		sam_data sam;
		bam_data bam;
	};

	/* General */
	ordering ord;
	lng file_id;
	str file_location;
	sht dbschema;

	/* Counters for encountered data */
	unsigned int cnt_sq;
	unsigned int cnt_rg;
	unsigned int cnt_pg;
	lng cnt_alignments;
	lng cnt_alignments_extra;
	lng cnt_alignments_paired_primary;
	lng cnt_alignments_paired_secondary;
	lng cnt_alignments_total;

	/* File paths to binary files */
	char fp_files[6][BW_FP_BUF_SIZE];
	char fp_sq[7][BW_FP_BUF_SIZE];
	char fp_rg[13][BW_FP_BUF_SIZE];
	char fp_pg[6][BW_FP_BUF_SIZE];
	char fp_alignments[12][BW_FP_BUF_SIZE];	/* Used for unpaired alignments in the case of schema 1 */
	char fp_alignments_extra[4][BW_FP_BUF_SIZE];
	char fp_alignments_paired_primary[23][BW_FP_BUF_SIZE];	/* Only used for schema 1 */
	char fp_alignments_paired_secondary[23][BW_FP_BUF_SIZE];	/* Only used for schema 1 */

	/* streams for binary files */
	stream *files[6];
	stream *sq[7];
	stream *rg[13];
	stream *pg[6];
	stream *alignments[12];	/* Used for unpaired alignments in the case of schema 1 */
	stream *alignments_extra[4];
	stream *alignments_paired_primary[23];
	stream *alignments_paired_secondary[23];

} bam_wrapper;


str ordering_str(ordering ord);
str init_bam_wrapper(bam_wrapper * bw, filetype type, str file_location,
			 lng file_id, sht dbschema);
void clear_bam_wrapper(bam_wrapper * bw);
str process_header(bam_wrapper * bw);
str process_alignments(bam_wrapper *bw, bit *some_thread_failed);	/* (*some_thread_failed) will be set to TRUE if some thread fails during processing alignments */

#define DIR_BINARIES "bam_binaries"

/* Possible ordering orders for BAM file */
#define ORDERING_UNSORTED   0
#define ORDERING_QUERYNAME  1
#define ORDERING_COORDINATE 2
#define ORDERING_UNKNOWN	9


#endif
