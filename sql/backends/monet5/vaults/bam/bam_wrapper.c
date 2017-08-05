/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 */

#include "monetdb_config.h"
#include "mal_exception.h"
#include "stream.h"
#include "bam_globals.h"

#ifdef HAVE_SAMTOOLS_SAM_H
#include <samtools/sam.h>
#else
#include <bam/sam.h>
#endif

#ifdef HAVE_SAMTOOLS_KSTRING_H
#include <samtools/kstring.h>
#elif defined (HAVE_BAM_KSTRING_H)
#include <bam/kstring.h>
#else
/* Ubuntu doesn't distribute samtools/kstring.h, so we need our own
 * version */
#include "mykstring.h"
#endif

#include "bam_wrapper.h"

str
ordering_str(ordering ord)
{
	switch (ord) {
	case ORDERING_UNSORTED:
		return "unsorted";
	case ORDERING_QUERYNAME:
		return "queryname";
	case ORDERING_COORDINATE:
		return "coordinate";
	default:
		return "unknown";
	}
}

static ordering
get_ordering(str ord)
{
	if (strcmp(ord, "unsorted") == 0)
		return ORDERING_UNSORTED;
	if (strcmp(ord, "queryname") == 0)
		return ORDERING_QUERYNAME;
	if (strcmp(ord, "coordinate") == 0)
		return ORDERING_COORDINATE;
	return ORDERING_UNKNOWN;
}


#define ERR_INIT_BAM_WRAPPER SQLSTATE(BA000) "Could not initialize wrapper for BAM file '%s': "
/**
 * Takes a bam_wrapper and initializes it. Note that in order for the
 * accompanying clear function to work, the bam_wrapper should be
 * initialized to zero before the fields are filled in by the init
 * function.
 *
 * Binaries will be opened in dbfarm/bam, since that is the current
 * working directory.
 */
str
init_bam_wrapper(bam_wrapper * bw, filetype type, str file_location,
		 lng file_id, sht dbschema)
{
	int i;
	char flushdir[128];

	/* Enables clear function to check variables */
	memset(bw, 0, sizeof(bam_wrapper));

	if (mkdir(DIR_BINARIES, 0777) == -1 && errno != EEXIST) {
		throw(MAL, "init_bam_wrapper",
			  ERR_INIT_BAM_WRAPPER "Directory '" DIR_BINARIES
			  "' could not be created (%s)", file_location,
			  strerror(errno));
	}

	snprintf(flushdir, 128, DIR_BINARIES "/" LLFMT, file_id);
	if (mkdir(flushdir, 0777) == -1 && errno != EEXIST) {
		throw(MAL, "init_bam_wrapper",
			  ERR_INIT_BAM_WRAPPER
			  "Directory '%s' could not be created (%s)",
			  file_location, flushdir, strerror(errno));
	}

	// Construct all the file paths to the binary files
	for (i = 0; i < 6; ++i) {
		snprintf(bw->fp_files[i], BW_FP_BUF_SIZE, "%s/files_%d",
			 flushdir, i);
	}
	for (i = 0; i < 7; ++i) {
		snprintf(bw->fp_sq[i], BW_FP_BUF_SIZE, "%s/sq_%d", flushdir,
			 i);
	}
	for (i = 0; i < 13; ++i) {
		snprintf(bw->fp_rg[i], BW_FP_BUF_SIZE, "%s/rg_%d", flushdir,
			 i);
	}
	for (i = 0; i < 6; ++i) {
		snprintf(bw->fp_pg[i], BW_FP_BUF_SIZE, "%s/pg_%d", flushdir,
			 i);
	}
	for (i = 0; i < 12; ++i) {
		snprintf(bw->fp_alignments[i], BW_FP_BUF_SIZE,
			 "%s/alignments_%d", flushdir, i);
	}
	for (i = 0; i < 4; ++i) {
		snprintf(bw->fp_alignments_extra[i], BW_FP_BUF_SIZE,
			 "%s/alignments_extra_%d", flushdir, i);
	}
	if (dbschema == 1) {
		for (i = 0; i < 23; ++i) {
			snprintf(bw->fp_alignments_paired_primary[i],
				 BW_FP_BUF_SIZE,
				 "%s/alignments_paired_primary_%d", flushdir,
				 i);
			snprintf(bw->fp_alignments_paired_secondary[i],
				 BW_FP_BUF_SIZE,
				 "%s/alignments_paired_secondary_%d",
				 flushdir, i);
		}
	}

	if (type == BAM) {
		/* Open BAM file and read its header */
		if ((bw->bam.input = bam_open(file_location, "r")) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER "BAM file could not be opened",
				  file_location);
		}
		if ((bw->bam.header = bam_header_read(bw->bam.input)) == NULL) {
			throw(MAL, "init_bam_wrapper",
				ERR_INIT_BAM_WRAPPER "Unable to read header from file",
				file_location);
		}
	} else {
		/* Open SAM file and read its header */
		int bufsize = 4096;
		lng header_len = 0;
		if ((bw->sam.input = open_rastream(file_location)) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER "SAM file could not be opened",
				  file_location);
		}
		if ((bw->sam.header = (str)GDKmalloc(bufsize * sizeof(char))) == NULL) {
			throw(MAL, "init_bam_wrapper", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		while (TRUE) {
			int read = mnstr_readline(bw->sam.input, bw->sam.header + header_len, bufsize - header_len);

			if (read <= 0) {
				throw(MAL, "init_bam_wrapper",
					  ERR_INIT_BAM_WRAPPER "Could not read line of SAM header",
					  file_location);
			}

			if (bw->sam.header[header_len] != '@') {
				/* This is not a header line, we assume that the header is finished.
				 * Rewind stream to start of line and stop reading */
				if (mnstr_fsetpos(bw->sam.input, header_len) < 0) {
					throw(MAL, "init_bam_wrapper",
					  ERR_INIT_BAM_WRAPPER "Could not read last line of SAM header",
					  file_location);
				}
				bw->sam.header[header_len] = '\0'; /* Truncate alignment data */
				break;
			}

			if (bw->sam.header[header_len+read-1] != '\n') {
				/* This line was not completed. Increase buffer size, rewind stream
				 * and try again */
				str tmp;
				bufsize *= 2;
				if ((tmp = GDKrealloc(bw->sam.header, bufsize * sizeof(char))) == NULL) {
					throw(MAL, "init_bam_wrapper", SQLSTATE(HY001) MAL_MALLOC_FAIL);
				}
				bw->sam.header = tmp;
				if (mnstr_fsetpos(bw->sam.input, header_len) < 0) {
					throw(MAL, "init_bam_wrapper",
					  ERR_INIT_BAM_WRAPPER "Could not read last line of SAM header",
					  file_location);
				}
				continue;
			}

			/* Only if no special cases occured, we added the read characters to the header */
			header_len += read;
		}
	}

	/* Set ordering to unknown, since we don't know until we have
	 * processed the header */
	bw->type = type;
	bw->ord = ORDERING_UNKNOWN;

	bw->file_id = file_id;
	bw->file_location = file_location;
	bw->dbschema = dbschema;

	bw->cnt_sq = 0;
	bw->cnt_rg = 0;
	bw->cnt_pg = 0;
	bw->cnt_alignments = 0;
	bw->cnt_alignments_extra = 0;
	bw->cnt_alignments_paired_primary = 0;
	bw->cnt_alignments_paired_secondary = 0;
	bw->cnt_alignments_total = 0;


	return MAL_SUCCEED;
}


static str 
open_header_write_streams(bam_wrapper * bw)
{
	int i;

	for (i = 0; i < 6; ++i) {
		if ((bw->files[i] = bsopen(bw->fp_files[i])) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER
				  "Binary file '%s' could not be opened",
				  bw->file_location, bw->fp_files[i]);
		}
	}
	for (i = 0; i < 7; ++i) {
		if ((bw->sq[i] = bsopen(bw->fp_sq[i])) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER
				  "Binary file '%s' could not be opened",
				  bw->file_location, bw->fp_sq[i]);
		}
	}
	for (i = 0; i < 13; ++i) {
		if ((bw->rg[i] = bsopen(bw->fp_rg[i])) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER
				  "Binary file '%s' could not be opened",
				  bw->file_location, bw->fp_rg[i]);
		}
	}
	for (i = 0; i < 6; ++i) {
		if ((bw->pg[i] = bsopen(bw->fp_pg[i])) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER
				  "Binary file '%s' could not be opened",
				  bw->file_location, bw->fp_pg[i]);
		}
	}
	return MAL_SUCCEED;
}


static str
open_alignment_write_streams(bam_wrapper * bw) {
	int i;

	for (i = 0; i < 12; ++i) {
		if ((bw->alignments[i] =
			 bsopen(bw->fp_alignments[i])) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER
				  "Binary file '%s' could not be opened",
				  bw->file_location, bw->fp_alignments[i]);
		}
	}
	for (i = 0; i < 4; ++i) {
		if ((bw->alignments_extra[i] =
			 bsopen(bw->fp_alignments_extra[i])) == NULL) {
			throw(MAL, "init_bam_wrapper",
				  ERR_INIT_BAM_WRAPPER
				  "Binary file '%s' could not be opened",
				  bw->file_location, bw->fp_alignments_extra[i]);
		}
	}
	if (bw->dbschema == 1) {
		for (i = 0; i < 23; ++i) {
			if ((bw->alignments_paired_primary[i] =
				 bsopen(bw->fp_alignments_paired_primary[i])) ==
				NULL) {
				throw(MAL, "init_bam_wrapper",
					  ERR_INIT_BAM_WRAPPER
					  "Binary file '%s' could not be opened",
					  bw->file_location,
					  bw->fp_alignments_paired_primary[i]);
			}
			if ((bw->alignments_paired_secondary[i] =
				 bsopen(bw->fp_alignments_paired_secondary[i])) ==
				NULL) {
				throw(MAL, "init_bam_wrapper",
					  ERR_INIT_BAM_WRAPPER
					  "Binary file '%s' could not be opened",
					  bw->file_location,
					  bw->fp_alignments_paired_secondary[i]);
			}
		}
	}
	return MAL_SUCCEED;
}


static void
close_header_write_streams(bam_wrapper * bw) {
	int i;

	for (i = 0; i < 6; ++i) {
		if (bw->files[i]) {
			close_stream(bw->files[i]);
			/* Set to NULL to prevent errors in case
			 * close_streams gets called again */
			bw->files[i] = NULL;
		}
	}
	for (i = 0; i < 7; ++i) {
		if (bw->sq[i]) {
			close_stream(bw->sq[i]);
			bw->sq[i] = NULL;
		}
	}
	for (i = 0; i < 13; ++i) {
		if (bw->rg[i]) {
			close_stream(bw->rg[i]);
			bw->rg[i] = NULL;
		}
	}
	for (i = 0; i < 6; ++i) {
		if (bw->pg[i]) {
			close_stream(bw->pg[i]);
			bw->pg[i] = NULL;
		}
	}
}


static void
close_alignment_write_streams(bam_wrapper * bw) {
	int i;

	for (i = 0; i < 12; ++i) {
		if (bw->alignments[i]) {
			close_stream(bw->alignments[i]);
			bw->alignments[i] = NULL;
		}
	}
	for (i = 0; i < 4; ++i) {
		if (bw->alignments_extra[i]) {
			close_stream(bw->alignments_extra[i]);
			bw->alignments_extra[i] = NULL;
		}
	}
	if (bw->dbschema == 1) {
		for (i = 0; i < 23; ++i) {
			if (bw->alignments_paired_primary[i]) {
				close_stream(bw->
						 alignments_paired_primary[i]);
				bw->alignments_paired_primary[i] = NULL;
			}
			if (bw->alignments_paired_secondary[i]) {
				close_stream(bw->
						 alignments_paired_secondary[i]);
				bw->alignments_paired_secondary[i] = NULL;
			}
		}
	}
}

void
clear_bam_wrapper(bam_wrapper * bw)
{
	char flushdir[128];
	int i;

	/* Clear bam/sam specific fields */
	if (bw->type == BAM) {
		if (bw->bam.header) {
			bam_header_destroy(bw->bam.header);
		}
		if (bw->bam.input) {
			bam_close(bw->bam.input);
		}
	} else {
		if (bw->sam.input) {
			close_stream(bw->sam.input);
		}
		if (bw->sam.header) {
			GDKfree(bw->sam.header);
		}
	}

	/* Close file streams if this was not done yet */
	close_header_write_streams(bw);
	close_alignment_write_streams(bw);

	/* And remove the write stream files that still exist */
	for (i = 0; i < 6; ++i) {
		unlink(bw->fp_files[i]);
	}
	for (i = 0; i < 7; ++i) {
		unlink(bw->fp_sq[i]);
	}
	for (i = 0; i < 13; ++i) {
		unlink(bw->fp_rg[i]);
	}
	for (i = 0; i < 6; ++i) {
		unlink(bw->fp_pg[i]);
	}
	for (i = 0; i < 12; ++i) {
		unlink(bw->fp_alignments[i]);
	}
	for (i = 0; i < 4; ++i) {
		unlink(bw->fp_alignments_extra[i]);
	}
	if (bw->dbschema == 1) {
		for (i = 0; i < 23; ++i) {
			unlink(bw->fp_alignments_paired_primary[i]);
			unlink(bw->fp_alignments_paired_primary[i]);
		}
	}

	/* Finally, attempt to remove flush directory */
	snprintf(flushdir, 128, DIR_BINARIES "/" LLFMT, bw->file_id);
	rmdir(flushdir);
}





/**
 * Reads the string in 'src' until one of the delimiters in 'delims'
 * is encountered and stores the resulting string in 'ret'
 * returns strlen(ret) or -1 when GDKmalloc on *ret fails
 * src will be advanced during the reading.
 */
static int
read_string_until_delim(str * src, str * ret, char *delims, sht nr_delims)
{
	int i, size = 0;
	int end = FALSE;

	while (TRUE) {
		/* is the current character one of the delimiters? */
		for (i = 0; i < nr_delims; ++i) {
			if (*(*src + size) == delims[i]) {
				/* end this loop, we have encountered
				 * a delimiter at *(src+size) */
				/* this means that the length of the
				 * string before this delimiter equals
				 * size */
				end = TRUE;
				break;
			}
		}
		if (end)
			break;
		++size;
	}

	/* we now know the size, copy the right part of src to ret */
	*ret = GDKmalloc((size + 1) * sizeof(char));
	if (*ret == NULL)
		return -1;

	strncpy(*ret, *src, size);
	*(*ret + size) = '\0';

	/* and advance src */
	*src += size;

	/* done, return size of *ret */
	return size;
}







/**
 * Processing of header
 */

/**
 * Structs used for storing header lines
 */
typedef struct bam_header_option {
	char tag[3];
	str value;
} bam_header_option;

typedef struct bam_header_line {
	char header_tag[3];
	bam_header_option *options;
	sht nr_options;
} bam_header_line;


#define ERR_PROCESS_HEADER_LINE SQLSTATE(BA000) "Could not parse a header line in BAM file '%s': "

/**
 * Parses the next BAM header line from the given header.
 * Considers the first rule of the input (*header) as a header line
 * and attempts to parse it into the provided bam_header_line
 * structure. In case the function fails, the calling function must
 * call clear_bam_header_line to free possible resources that are
 * malloced by process_header_line.  The *eof flag will be set to
 * TRUE if the input doesn't contain a header line anymore.  The
 * function needs the file_location in order to generate decent error
 * messages
 */
static str
process_header_line(str * header, bam_header_line * ret_hl, bit * eof,
			str file_location)
{
	bam_header_option *opt = NULL;

	memset(ret_hl, 0, sizeof(bam_header_line));	/* Enable clear function to check all variables */

	/* start by stripping \n, \r, \t and spaces */
	while (**header == '\n' || **header == '\r' || **header == '\t'
		   || **header == ' ') {
		(*header)++;
	}

	if (**header == '\0') {
		/* eof reached */
		*eof = TRUE;
		return MAL_SUCCEED;
	}

	if (**header != '@') {
		/* first character on header line should really be @ */
		throw(MAL, "process_header_line",
			  ERR_PROCESS_HEADER_LINE
			  "Detected header line that does not start with '@'",
			  file_location);
	}

	/* strip the @ */
	(*header)++;

	/* eof not reached, so a header tag should be present, store
	 * it as long as no \0 is there */
	if (**header == '\0' || *(*header + 1) == '\0') {
		throw(MAL, "process_header_line",
			  ERR_PROCESS_HEADER_LINE "Unexpected end of header",
			  file_location);
	}
	ret_hl->header_tag[0] = **header;
	ret_hl->header_tag[1] = *(*header + 1);
	ret_hl->header_tag[2] = '\0';
	*header += 2;

	/* if this is a comment, we only need to read the rest of the line */
	if (strcmp(ret_hl->header_tag, "CO") == 0) {
		/* initialize opt */
		if ((opt =
			 (bam_header_option *)
			 GDKmalloc(sizeof(bam_header_option))) == NULL) {
			throw(MAL, "process_header_line",
				  SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}

		/* indicate that no tag exists for this option */
		opt->tag[0] = '\0';
		if (read_string_until_delim(header, &opt->value, "\n\0", 2) ==
			-1) {
			GDKfree(opt);
			throw(MAL, "process_header_line",
				  SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		/* option only has to point to a single
		 * bam_header_option in this case */
		ret_hl->options = opt;
		ret_hl->nr_options = 1;
		return MAL_SUCCEED;
	}

	/* reserve enough space for the options (max 12 for RG) */
	if ((ret_hl->options =
		 GDKmalloc(12 * sizeof(bam_header_option))) == NULL) {
		throw(MAL, "process_header_line", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	/* Enables clear function to check individual options */
	memset(ret_hl->options, 0, 12 * sizeof(bam_header_option));

	/* now get all options */
	while (TRUE) {		/* iterate once in this loop for every option */
		/* throw away tab(s)/space(s) */
		while (**header == '\t' || **header == ' ')
			(*header)++;

		/* if after this, a newline or \0 is presented, we are done */
		if (**header == '\n' || **header == '\0')
			break;

		/* a new option will be presented, see if we don't
		 * already have too many options */
		if (ret_hl->nr_options == 12) {
			throw(MAL, "process_header_line",
				  ERR_PROCESS_HEADER_LINE
				  "Detected header line with more than 12 options",
				  file_location);
		}

		/* make opt point to the right bam_header_option */
		opt = &ret_hl->options[ret_hl->nr_options++];

		/* read tag */
		if (**header == '\0' || *(*header + 1) == '\0') {
			throw(MAL, "process_header_line",
				  ERR_PROCESS_HEADER_LINE
				  "Unexpected end of header", file_location);
		}
		opt->tag[0] = **header;
		opt->tag[1] = *(*header + 1);
		opt->tag[2] = '\0';
		*header += 2;

		/* a colon should be presented at this point */
		if (**header != ':') {
			throw(MAL, "process_header_line",
				  ERR_PROCESS_HEADER_LINE
				  "Expected a colon (:) after option tag in header line",
				  file_location);
		}
		(*header)++;

		/* read value of this option */
		if (read_string_until_delim(header, &opt->value, "\t\n\0", 3)
			== -1) {
			throw(MAL, "process_header_line",
				  SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}

	/* real number of options is now known, shrink the options
	 * array in the header line. */
	opt = GDKrealloc(ret_hl->options,
					 ret_hl->nr_options * sizeof(bam_header_option));
	if (opt == NULL)
		throw(MAL, "process_header_line", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	ret_hl->options = opt;

	return MAL_SUCCEED;
}

/*
* Free the memory occupied by a bam_header_line structure
*/
static void
clear_bam_header_line(bam_header_line * hl)
{
	sht o;

	if (hl->options) {
		for (o = 0; o < hl->nr_options; ++o) {
			if (hl->options[o].value)
				GDKfree(hl->options[o].value);
		}
		GDKfree(hl->options);
	}
}



/**
 * Macros for appending data to the streams, evaluate to 0 on failure
 */
#define APPEND_STR(strm, s) (mnstr_writeBteArray(strm, (signed char*)s, strlen(s)) && mnstr_writeBte(strm, '\n'))
#define APPEND_SHT(strm, i) mnstr_writeSht(strm, i)
#define APPEND_INT(strm, i) mnstr_writeInt(strm, i)
#define APPEND_LNG(strm, i) mnstr_writeLng(strm, i)



/**
 * Macros used only by process_header for appending header data to
 * streams under certain conditions
 */
#define APPEND_OPTION_COND_STR(strm, opt, cmp, flag)			\
	if (strcmp((opt).tag, (cmp)) == 0) {				\
		if (!APPEND_STR(strm, (opt).value)) {			\
			throw(MAL, "process_header", ERR_PROCESS_HEADER "Could not write option '%s:%s' to binary file", \
				  bw->file_location, (opt).tag, (opt).value); \
		}							\
		(flag) = TRUE;						\
		continue;						\
	}

#define APPEND_OPTION_COND_INT(file, opt, cmp, flag, l, s)		\
	if (strcmp((opt).tag, (cmp)) == 0) {				\
		(l) = strtol((opt).value, &s, 10);			\
		if ((s) == ((opt).value) || (l) == LONG_MIN || (l) == LONG_MAX) \
			l = -1;						\
		if (!APPEND_INT(file, l)) {				\
			throw(MAL, "process_header", ERR_PROCESS_HEADER "Could not write option '%s:%s' to binary file", \
				  bw->file_location, (opt).tag, (opt).value); \
		}							\
		(flag) = TRUE;						\
		continue;						\
	}

#define APPEND_OPTION_COND_LNG(file, opt, cmp, flag, l, s)		\
	if (strcmp((opt).tag, (cmp)) == 0) {				\
		(l) = strtol((opt).value, &s, 10);			\
		if ((s) == ((opt).value) || (l) == LONG_MIN || (l) == LONG_MAX) \
			l = -1;					\
		if (!APPEND_LNG(file, l)) {				\
			throw(MAL, "process_header", ERR_PROCESS_HEADER "Could not write option '%s:%s' to binary file", \
				  bw->file_location, (opt).tag, (opt).value); \
		}							\
		(flag) = TRUE;						\
		continue;						\
	}

/**
 * Macros used only by process_header for convenient error handling
 */
#define ERR_PROCESS_HEADER "Could not parse header of BAM file '%s': "
#define ERR_NULL_INSERTION(hline, field)				\
	do {								\
		clear_bam_header_line(&hl);				\
		throw(MAL, "process_header", ERR_PROCESS_HEADER "Could not write NULL to binary file for "hline":"field" option", \
			  bw->file_location);				\
	} while (0)


/**
 * Parse the ASCII BAM header of the given BAM file wrapper.
 */
str
process_header(bam_wrapper * bw)
{
	str header_str;

	bam_header_line hl;
	int i, o;
	str msg = NULL;

	/* declare variables for checking mandatory fields */
	int nr_hd_lines = 0;	/* used to restrict the total number of header lines to max 1 */
	kstring_t hd_comment;
	bit comment_found = FALSE;
	bit hd_fields_found[2] = { FALSE, FALSE };
	bit sq_fields_found[6];
	bit rg_fields_found[12];
	bit pg_fields_found[5];
	bit eof = FALSE;
	str s;
	lng l;

	if((msg = open_header_write_streams(bw)) != MAL_SUCCEED) {
		return msg;
	}

	hl.options = NULL;
	if (bw->type == BAM) {
		header_str = bw->bam.header->text;
	} else {
		header_str = bw->sam.header;
	}

	hd_comment.l = hd_comment.m = 0;
	hd_comment.s = NULL;

	if (!APPEND_LNG(bw->files[0], bw->file_id)) {
		throw(MAL, "process_header",
			  ERR_PROCESS_HEADER "Could not write file id " LLFMT
			  " to binary file", bw->file_location, bw->file_id);
	}
	if (!APPEND_STR(bw->files[1], bw->file_location)) {
		throw(MAL, "process_header",
			  ERR_PROCESS_HEADER
			  "Could not write file location to binary file",
			  bw->file_location);
	}
	if (!APPEND_SHT(bw->files[2], bw->dbschema)) {
		throw(MAL, "process_header",
			  ERR_PROCESS_HEADER
			  "Could not write dbschema to binary file",
			  bw->file_location);
	}

	if(header_str != NULL) {

		/* loop will run until no more header lines are found */
		while (TRUE) {
			/* try to read the next header line */
			if ((msg =
				 process_header_line(&header_str, &hl, &eof,
							 bw->file_location)) !=
				MAL_SUCCEED) {
				clear_bam_header_line(&hl);
				return msg;
			}

			/* if eof is set to TRUE by process_header_line,
			 * this indicates that we reached the end of the
			 * header */
			if (eof) {
				clear_bam_header_line(&hl);
				break;
			}

			/* read and interpret the header tag */
			if (strcmp(hl.header_tag, "HD") == 0) {
				++nr_hd_lines;
				if (nr_hd_lines > 1) {
					clear_bam_header_line(&hl);
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "More than one HD line found in header",
						  bw->file_location);
				}

				for (o = 0; o < hl.nr_options; ++o) {
					/* If this option contains sorting
					 * order, save it in the bam_wrapper
					 * struct */
					if (strcmp(hl.options[o].tag, "SO") == 0) {
						bw->ord =
							get_ordering(hl.options[o].
									 value);
					}
					APPEND_OPTION_COND_STR(bw->files[3],
								   hl.options[o], "VN",
								   hd_fields_found[0]);
					APPEND_OPTION_COND_STR(bw->files[4],
								   hl.options[o], "SO",
								   hd_fields_found[1]);

					/* if this point is reached, option
					 * wasn't recognized */
					msg = createException(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Unknown option '%s' found in header tag HD",
						  bw->file_location, hl.options[o].tag);
					clear_bam_header_line(&hl);
					return msg;
				}
				if (!hd_fields_found[0]) {
					clear_bam_header_line(&hl);
					throw(MAL, "process_header",
						  "VN tag not found in HD header line\n");
				}
			} else if (strcmp(hl.header_tag, "SQ") == 0) {
				++bw->cnt_sq;
				if (!APPEND_LNG(bw->sq[1], bw->file_id)) {
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Could not write file id " LLFMT
						  " to binary file", bw->file_location,
						  bw->file_id);
				}

				for (i = 0; i < 6; ++i) {
					sq_fields_found[i] = FALSE;
				}

				for (o = 0; o < hl.nr_options; ++o) {
					APPEND_OPTION_COND_STR(bw->sq[0],
								   hl.options[o], "SN",
								   sq_fields_found[0]);
					APPEND_OPTION_COND_INT(bw->sq[2],
								   hl.options[o], "LN",
								   sq_fields_found[1], l,
								   s);
					APPEND_OPTION_COND_INT(bw->sq[3],
								   hl.options[o], "AS",
								   sq_fields_found[2], l,
								   s);
					APPEND_OPTION_COND_STR(bw->sq[4],
								   hl.options[o], "M5",
								   sq_fields_found[3]);
					APPEND_OPTION_COND_STR(bw->sq[5],
								   hl.options[o], "SP",
								   sq_fields_found[4]);
					APPEND_OPTION_COND_STR(bw->sq[6],
								   hl.options[o], "UR",
								   sq_fields_found[5]);

					/* if this point is reached, option
					 * wasn't recognized */
					msg = createException(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Unknown option '%s' found in header tag SQ",
						  bw->file_location, hl.options[o].tag);
					clear_bam_header_line(&hl);
					return msg;
				}
				if (!sq_fields_found[0]) {
					clear_bam_header_line(&hl);
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "SN tag not found in SQ header line",
						  bw->file_location);
				}
				/*We don't require the LN field for our
				 * primary key, so to increase user
				 * friendliness, we accept an absence of the
				 * LN option, even though the specification
				 * says it is required */
				/*if(!sq_fields_found[1]) {
				 * clear_bam_header_line(&hl);
				 * throw(MAL, "process_header", ERR_PROCESS_HEADER "LN tag not found in SQ header line", bw->file_location);
				 * } */

				/* Insert NULL values where needed */
				if (!sq_fields_found[1]
					&& !APPEND_INT(bw->sq[2], int_nil))
					ERR_NULL_INSERTION("SQ", "LN");
				if (!sq_fields_found[2]
					&& !APPEND_INT(bw->sq[3], int_nil))
					ERR_NULL_INSERTION("SQ", "AS");
				if (!sq_fields_found[3]
					&& !APPEND_STR(bw->sq[4], str_nil))
					ERR_NULL_INSERTION("SQ", "M5");
				if (!sq_fields_found[4]
					&& !APPEND_STR(bw->sq[5], str_nil))
					ERR_NULL_INSERTION("SQ", "SP");
				if (!sq_fields_found[5]
					&& !APPEND_STR(bw->sq[6], str_nil))
					ERR_NULL_INSERTION("SQ", "UR");

			} else if (strcmp(hl.header_tag, "RG") == 0) {
				++bw->cnt_rg;
				if (!APPEND_LNG(bw->rg[1], bw->file_id)) {
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Could not write file id " LLFMT
						  " to binary file", bw->file_location,
						  bw->file_id);
				}

				for (i = 0; i < 12; ++i) {
					rg_fields_found[i] = FALSE;
				}

				for (o = 0; o < hl.nr_options; ++o) {
					APPEND_OPTION_COND_STR(bw->rg[0],
								   hl.options[o], "ID",
								   rg_fields_found[0]);
					APPEND_OPTION_COND_STR(bw->rg[2],
								   hl.options[o], "CN",
								   rg_fields_found[1]);
					APPEND_OPTION_COND_STR(bw->rg[3],
								   hl.options[o], "DS",
								   rg_fields_found[2]);
					APPEND_OPTION_COND_LNG(bw->rg[4],
								   hl.options[o], "DT",
								   rg_fields_found[3], l,
								   s);
					APPEND_OPTION_COND_STR(bw->rg[5],
								   hl.options[o], "FO",
								   rg_fields_found[4]);
					APPEND_OPTION_COND_STR(bw->rg[6],
								   hl.options[o], "KS",
								   rg_fields_found[5]);
					APPEND_OPTION_COND_STR(bw->rg[7],
								   hl.options[o], "LB",
								   rg_fields_found[6]);
					APPEND_OPTION_COND_STR(bw->rg[8],
								   hl.options[o], "PG",
								   rg_fields_found[7]);
					APPEND_OPTION_COND_INT(bw->rg[9],
								   hl.options[o], "PI",
								   rg_fields_found[8], l,
								   s);
					APPEND_OPTION_COND_STR(bw->rg[10],
								   hl.options[o], "PL",
								   rg_fields_found[9]);
					APPEND_OPTION_COND_STR(bw->rg[11],
								   hl.options[o], "PU",
								   rg_fields_found[10]);
					APPEND_OPTION_COND_STR(bw->rg[12],
								   hl.options[o], "SM",
								   rg_fields_found[11]);

					/* if this point is reached, option wasn't recognized */
					msg = createException(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Unknown option '%s' found in header tag RG",
						  bw->file_location, hl.options[o].tag);
					clear_bam_header_line(&hl);
					return msg;
				}
				if (!rg_fields_found[0]) {
					clear_bam_header_line(&hl);
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "ID tag not found in RG header line",
						  bw->file_location);
				}

				/* Insert NULL values where needed */
				if (!rg_fields_found[1]
					&& !APPEND_STR(bw->rg[2], str_nil))
					ERR_NULL_INSERTION("RG", "CN");
				if (!rg_fields_found[2]
					&& !APPEND_STR(bw->rg[3], str_nil))
					ERR_NULL_INSERTION("RG", "DS");
				if (!rg_fields_found[3]
					&& !APPEND_LNG(bw->rg[4], lng_nil))
					ERR_NULL_INSERTION("RG", "DT");
				if (!rg_fields_found[4]
					&& !APPEND_STR(bw->rg[5], str_nil))
					ERR_NULL_INSERTION("RG", "FO");
				if (!rg_fields_found[5]
					&& !APPEND_STR(bw->rg[6], str_nil))
					ERR_NULL_INSERTION("RG", "KS");
				if (!rg_fields_found[6]
					&& !APPEND_STR(bw->rg[7], str_nil))
					ERR_NULL_INSERTION("RG", "LB");
				if (!rg_fields_found[7]
					&& !APPEND_STR(bw->rg[8], str_nil))
					ERR_NULL_INSERTION("RG", "PG");
				if (!rg_fields_found[8]
					&& !APPEND_INT(bw->rg[9], int_nil))
					ERR_NULL_INSERTION("RG", "PI");
				if (!rg_fields_found[9]
					&& !APPEND_STR(bw->rg[10], str_nil))
					ERR_NULL_INSERTION("RG", "PL");
				if (!rg_fields_found[10]
					&& !APPEND_STR(bw->rg[11], str_nil))
					ERR_NULL_INSERTION("RG", "PU");
				if (!rg_fields_found[11]
					&& !APPEND_STR(bw->rg[12], str_nil))
					ERR_NULL_INSERTION("RG", "SM");

			} else if (strcmp(hl.header_tag, "PG") == 0) {
				++bw->cnt_pg;
				if (!APPEND_LNG(bw->pg[1], bw->file_id)) {
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Could not write file id " LLFMT
						  " to binary file", bw->file_location,
						  bw->file_id);
				}

				for (i = 0; i < 5; ++i) {
					pg_fields_found[i] = FALSE;
				}

				for (o = 0; o < hl.nr_options; ++o) {
					APPEND_OPTION_COND_STR(bw->pg[0],
								   hl.options[o], "ID",
								   pg_fields_found[0]);
					APPEND_OPTION_COND_STR(bw->pg[2],
								   hl.options[o], "PN",
								   pg_fields_found[1]);
					APPEND_OPTION_COND_STR(bw->pg[3],
								   hl.options[o], "CL",
								   pg_fields_found[2]);
					APPEND_OPTION_COND_STR(bw->pg[4],
								   hl.options[o], "PP",
								   pg_fields_found[3]);
					APPEND_OPTION_COND_STR(bw->pg[5],
								   hl.options[o], "VN",
								   pg_fields_found[4]);

					/* if this point is reached, option wasn't recognized */
					msg = createException(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "Unknown option '%s' found in header tag PG",
						  bw->file_location, hl.options[o].tag);
					clear_bam_header_line(&hl);
					return msg;
				}
				if (!pg_fields_found[0]) {
					clear_bam_header_line(&hl);
					throw(MAL, "process_header",
						  ERR_PROCESS_HEADER
						  "ID tag not found in PG header line",
						  bw->file_location);
				}
				/* Insert NULL values where needed */
				if (!pg_fields_found[1]
					&& !APPEND_STR(bw->pg[2], str_nil))
					ERR_NULL_INSERTION("PG", "PN");
				if (!pg_fields_found[2]
					&& !APPEND_STR(bw->pg[3], str_nil))
					ERR_NULL_INSERTION("PG", "CL");
				if (!pg_fields_found[3]
					&& !APPEND_STR(bw->pg[4], str_nil))
					ERR_NULL_INSERTION("PG", "PP");
				if (!pg_fields_found[4]
					&& !APPEND_STR(bw->pg[5], str_nil))
					ERR_NULL_INSERTION("PG", "VN");
			} else if (strcmp(hl.header_tag, "CO") == 0) {
			/** a comment hl only has a single option, of which the
				 * tag = NULL and the value contains the actual comment
				 * Concatenate comment to earlier comments Seperate
				 * different comments with a newline
				 */
				if (comment_found) {
					kputc('\n', &hd_comment);
				}
				kputs(hl.options[0].value, &hd_comment);
				comment_found = TRUE;
			} else {
				msg = createException(MAL, "process_header",
					  ERR_PROCESS_HEADER
					  "Incorrect header tag '%s' found in BAM file",
					  bw->file_location, hl.header_tag);
				clear_bam_header_line(&hl);
				return msg;
			}

			/* everything went ok, clear the header line and move
			 * on to the next header line */
			clear_bam_header_line(&hl);
		}
	}

	if (!hd_fields_found[0] && !APPEND_STR(bw->files[3], str_nil))
		ERR_NULL_INSERTION("HD", "VN");
	if (!hd_fields_found[1] && !APPEND_STR(bw->files[4], str_nil))
		ERR_NULL_INSERTION("HD", "SO");

	if (!APPEND_STR
		(bw->files[5],
		 (comment_found && hd_comment.s ? hd_comment.s : str_nil))) {
		throw(MAL, "process_header",
			  ERR_PROCESS_HEADER
			  "Could not insert header comments in binary file",
			  bw->file_location);
	}

	if (hd_comment.s)
		free(hd_comment.s);

	close_header_write_streams(bw);

	return MAL_SUCCEED;
}

















/**
 * Processing of alignments
 */



typedef struct alignment {
	lng virtual_offset;
	str qname;
	sht flag;
	str rname;
	int pos;
	sht mapq;
	str cigar;
	str rnext;
	int pnext;
	int tlen;
	str seq;
	str qual;

	str aux; /* Only used when loading SAM */

	int qname_size; /* Current buffer size of qname strs (Only used when type = SAM or dbschema = 1) */
	int rname_size; /* Current buffer size of rname/rnext strs (only used when loading SAM) */
	int cigar_size; /* Current buffer size of cigar str */
	int seq_size; /* Current buffer size of seq and qual str */
	int aux_size; /* Current buffer size of aux (only used when loading SAM) */

	bit written;
} alignment;

static bit
init_alignment(bam_wrapper *bw, alignment * alig)
{
	bit result;

	/* Enables clear function to check variables */
	memset(alig, 0, sizeof(alignment));

	alig->qname_size = 10000;
	alig->rname_size = 10000;
	alig->cigar_size = 10000;
	alig->seq_size = 10000;
	alig->aux_size = 10000;

	/* Dynamic buffers to be able to expand when necessary */
	alig->cigar = (str) GDKmalloc(alig->cigar_size * sizeof(char));
	alig->seq = (str) GDKmalloc(alig->seq_size * sizeof(char));
	alig->qual = (str) GDKmalloc(alig->seq_size * sizeof(char));

	result = (alig->cigar != NULL && alig->seq != NULL
		&& alig->qual != NULL);

	if (bw->type == SAM || bw->dbschema == 1) {
		/* In this case we need to allocate space for the qname string
		 * type == SAM   => qname not stored in bam1_t struct
		 * dbschema == 1 => Alignments need to be stored for more than one
		 *				  iteration
		 * In other cases, it remains NULL and it will point to the qname
		 * in the bam1_t struct
		 */
		alig->qname = (str) GDKmalloc(alig->qname_size * sizeof(char));
	}
	if (bw->type == SAM) {
		/* If we are loading SAM, we need buffers for rname and rnext,
		 * since this is not taken care of by a bam_header_t dict now */
		alig->rname = (str) GDKmalloc(alig->rname_size * sizeof(char));
		alig->rnext = (str) GDKmalloc(alig->rname_size * sizeof(char));

		/* For SAM, we also need to have a buffer for aux, since this is
		 * not stored in a bam1_t struct anymore */
		 alig->aux = (str) GDKmalloc(alig->aux_size * sizeof(char));
		 result = (result && alig->rname != NULL &&
				alig->rnext != NULL && alig->aux != NULL);
	}

	return result;
}

static void
clear_alignment(bam_wrapper *bw, alignment * alig)
{
	if (alig->cigar)
		GDKfree(alig->cigar);
	if (alig->seq)
		GDKfree(alig->seq);
	if (alig->qual)
		GDKfree(alig->qual);
	if (bw->type == SAM || bw->dbschema == 1) {
		if (alig->qname)
			GDKfree(alig->qname);
	}
	if (bw->type == SAM) {
		if(alig->rname)
			GDKfree(alig->rname);
		if(alig->rnext)
			GDKfree(alig->rnext);
		if(alig->aux)
			GDKfree(alig->aux);
	}
}

/**
 * Function checks whether or not the character buffers in the given
 * alignment are big enough to hold the given number of characters. If
 * not, it doubles the buffer sizes.
 *
 * Function is only used for loading BAM files
 */
static inline bit
check_alignment_buffers(bam_wrapper *bw, alignment * alig, int qname_size,
		int cigar_size, int seq_size)
{
	bit resized[] = { FALSE, FALSE, FALSE };
	char *tmp;

	assert (bw->type == BAM);

	if (bw->dbschema == 1) {
		while (qname_size >= alig->qname_size) {
			resized[0] = TRUE;
			alig->qname_size *= 2;
		}
	}
	while (cigar_size >= alig->cigar_size) {
		resized[1] = TRUE;
		alig->cigar_size *= 2;
	}
	while (seq_size >= alig->seq_size) {
		resized[2] = TRUE;
		alig->seq_size *= 2;
	}
	if (resized[0]) {
		tmp = GDKrealloc(alig->cigar, alig->cigar_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->cigar = tmp;
	}
	if (resized[1]) {
		tmp = GDKrealloc(alig->cigar, alig->cigar_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->cigar = tmp;
	}
	if (resized[2]) {
		tmp = GDKrealloc(alig->seq, alig->seq_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->seq = tmp;
		tmp = GDKrealloc(alig->qual, alig->seq_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->qual = tmp;
	}

#ifdef BAM_DEBUG
	if (resized[0])
		TO_LOG("<bam_loader> Increased size of qname buffer to %d characters\n", alig->qname_size);
	if (resized[1])
		TO_LOG("<bam_loader> Increased size of cigar buffer to %d characters\n", alig->cigar_size);
	if (resized[2])
		TO_LOG("<bam_loader> Increased size of seq and qual buffers to %d characters\n", alig->seq_size);
#endif

	return (alig->cigar != NULL && alig->seq != NULL
		&& alig->qual != NULL);
}



/**
 * The next checker functions are used only for loading SAM files
 */


/**
 * Function checks if qname buffer can hold one more character
 */
static inline bit
check_qname_buffer(alignment * alig, int cur_size) {
	if (cur_size + 1 >= alig->qname_size) {
		char *tmp;
		alig->qname_size *= 2;
		tmp = GDKrealloc(alig->qname, alig->qname_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->qname = tmp;
		TO_LOG("<bam_loader> Increased size of qname buffer to %d characters\n", alig->qname_size);
	}
	return alig->qname != NULL;
}


/**
 * Function checks if rname/rnext buffers can hold one more character
 */
static inline bit
check_rname_rnext_buffers(alignment * alig, int cur_size) {
	if (cur_size + 1 >= alig->rname_size) {
		char *tmp;
		alig->rname_size *= 2;
		tmp = GDKrealloc(alig->rname, alig->rname_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->rname = tmp;
		tmp = GDKrealloc(alig->rnext, alig->rname_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->rnext = tmp;
		TO_LOG("<bam_loader> Increased size of cigar buffer to %d characters\n", alig->rname_size);
	}
	return alig->rname != NULL && alig->rnext != NULL;
}


/**
 * Function checks if cigar buffer can hold one more character
 */
static inline bit
check_cigar_buffer(alignment * alig, int cur_size) {
	if (cur_size + 1 >= alig->cigar_size) {
		char *tmp;
		alig->cigar_size *= 2;
		tmp = GDKrealloc(alig->cigar, alig->cigar_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->cigar = tmp;
		TO_LOG("<bam_loader> Increased size of cigar buffer to %d characters\n", alig->cigar_size);
	}
	return alig->cigar != NULL;
}

/**
 * Function checks if seq/qual buffers can hold one more character
 */
static inline bit
check_seq_qual_buffers(alignment * alig, int cur_size) {
	if (cur_size + 1 >= alig->seq_size) {
		char *tmp;
		alig->seq_size *= 2;
		tmp = GDKrealloc(alig->seq, alig->seq_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->seq = tmp;
		tmp = GDKrealloc(alig->qual, alig->seq_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->qual = tmp;
		TO_LOG("<bam_loader> Increased size of seq and qual buffers to %d characters\n", alig->seq_size);
	}
	return alig->seq != NULL && alig->qual != NULL;
}

/**
 * Function checks if aux buffer can hold one more character
 */
static inline bit
check_aux_buffer(alignment * alig, int cur_size) {
	if (cur_size + 1 >= alig->aux_size) {
		char *tmp;
		alig->aux_size *= 2;
		tmp = GDKrealloc(alig->aux, alig->aux_size * sizeof(char));
		if (tmp == NULL)
			return 0;
		alig->aux = tmp;
		TO_LOG("<bam_loader> Increased size of aux buffer to %d characters\n", alig->aux_size);
	}
	return alig->aux != NULL;
}



typedef bit (*buffer_check)(alignment *, int);


/**
 * Macros to easily extract flag fields from an alignment
 */

#define KTH_BIT(nr, k) (((nr) & (1 << (k))) == (1 << (k)))

#define FIRS_SEGM(a) (KTH_BIT((a).flag, 6))
#define LAST_SEGM(a) (KTH_BIT((a).flag, 7))
#define SECO_ALIG(a) (KTH_BIT((a).flag, 8))



/**
 * Macro's for building alignment processing errors
 */
#define ERR_PROCESS_ALIGNMENT SQLSTATE(BA000) "Could not process alignment for BAM file '%s': "
#define WRITE_ERR_PROCESS_ALIGNMENT(field) \
	throw(MAL, "process_alignments", ERR_PROCESS_ALIGNMENT "Could not write field '%s' to binary file", bw->file_location, field)

static inline int
next_alignment_field(stream * input, alignment * a,
	str buffer, buffer_check bc, bit delim_tab, bit * eol, bit * eof) {
	signed char c;
	int index = 0;
	if (mnstr_readBte(input, &c) == 0) {
		*eof = TRUE;
		return 0;
	}
	while ((!delim_tab || c != '\t') && c != '\n' && c != '\0') {
		buffer[index++] = c;
		if (mnstr_readBte(input, &c) == 0) {
			*eof = TRUE;
			return index;
		}
		/* Make sure there is enough space for next char */
		if (bc && !bc(a, index)) {
			return -1;
		}
	}
	buffer[index] = '\0';
	*eol = c == '\n';
	return index;
}


static str
next_sam_alignment(stream * input, lng virtual_offset,
		alignment * a, int * aux_len, bit * eof) {

	bit eol = FALSE;
	char lngbuf[64];

	/* virtual_offset */
	a->virtual_offset = virtual_offset;

	/* qname */
	if (next_alignment_field(input, a, a->qname,
			check_qname_buffer, TRUE, &eol, eof) < 0 || eol || *eof) {
		if (*eof) {
			/* When reading fails during qname, we assume that
			 * the file just ended */
			return MAL_SUCCEED;
		}
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after qname)");
	}

	/* flag */
	if (next_alignment_field(input, a, lngbuf, NULL, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after flag)");
	}
	a->flag = strtol(lngbuf, NULL, 10);

	/* rname */
	if (next_alignment_field(input, a, a->rname,
			check_rname_rnext_buffers, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after rname)");
	}

	/* pos */
	if (next_alignment_field(input, a, lngbuf, NULL, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after pos)");
	}
	a->pos = strtol(lngbuf, NULL, 10);

	/* mapq */
	if (next_alignment_field(input, a, lngbuf, NULL, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after mapq)");
	}
	a->mapq = strtol(lngbuf, NULL, 10);

	/* cigar */
	if (next_alignment_field(input, a, a->cigar,
			check_cigar_buffer, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after cigar)");
	}

	/* rnext */
	if (next_alignment_field(input, a, a->rnext,
			check_rname_rnext_buffers, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after rnext)");
	}

	/* pnext */
	if (next_alignment_field(input, a, lngbuf, NULL, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after pnext)");
	}
	a->pnext = strtol(lngbuf, NULL, 10);

	/* tlen */
	if (next_alignment_field(input, a, lngbuf, NULL, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after tlen)");
	}
	a->tlen = strtol(lngbuf, NULL, 10);

	/* seq */
	if (next_alignment_field(input, a, a->seq,
			check_seq_qual_buffers, TRUE, &eol, eof) < 0 || eol || *eof) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Unexpected end of line (after seq)");
	}

	/* qual */
	if (next_alignment_field(input, a, a->qual,
			check_seq_qual_buffers, TRUE, &eol, eof) < 0) {
		throw(MAL, "next_sam_alignment", SQLSTATE(BA000) "Could not read quality string (after qual)");
	}

	/* aux */
	if(!eol) {
		*aux_len = next_alignment_field(input, a, a->aux,
				check_aux_buffer, FALSE, &eol, eof);
	} else {
		*aux_len = 0;
		a->aux[0] = '\0';
	}

	assert(eol);

	return MAL_SUCCEED;
}

/**
 * Given a Samtools native structure bam1_t, retrieve all information
 * from it and store it in an alignment struct
 */
static str
bam1_t2alignment(bam_wrapper * bw, lng virtual_offset, bam1_t * a_in,
		  alignment * a_out)
{
	uint8_t *s;
	int i;

	assert(bw->type == BAM);

	a_out->written = FALSE;

	/* Start by making sure that the buffers in a_out are large enough */
	if (!check_alignment_buffers
		(bw, a_out, a_in->core.l_qname, a_in->core.n_cigar * 4, a_in->core.l_qseq)) {
		throw(MAL, "process_alignment", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	/* virtual_offset */
	a_out->virtual_offset = virtual_offset;

	/* qname */
	if (bw->dbschema == 0) {
		/* This case is equivalent to !(type == SAM || dbschema == 1),
		 * since we have (type == BAM && dbschema == 0)
		 * So, no space is allocated for qname since we will point
		 * directly into the bam1_t struct */
		a_out->qname = bam1_qname(a_in);
	} else {
		/* Copy for pairwise schema, since we then want to keep it
		 * alive after we finish processing this alignment */
		strcpy(a_out->qname, bam1_qname(a_in));
	}

	/* flag */
	a_out->flag = a_in->core.flag;

	/* rname */
	if (a_in->core.tid < 0) {
		a_out->rname = "*";
	} else {
		a_out->rname =
			bw->bam.header->target_name[a_in->core.tid];
	}

	/* pos */
	a_out->pos = a_in->core.pos + 1;

	/* mapq */
	a_out->mapq = a_in->core.qual;

	/* cigar */
	if (a_in->core.n_cigar == 0) {
		a_out->cigar[0] = '*';
		a_out->cigar[1] = '\0';
	} else {
		uint32_t *cigar_bin = bam1_cigar(a_in);
		uint32_t c;
		int index = 0;

		for (i = 0; i < a_in->core.n_cigar; ++i) {
			/* work around unaligned access */
			memcpy(&c, &cigar_bin[i], sizeof(c));
			snprintf(&a_out->cigar[index],
				 a_out->cigar_size - index, "%u%c",
				 c >> BAM_CIGAR_SHIFT,
				 bam_cigar_opchr(c));
			index += strlen(&a_out->cigar[index]);
		}
	}

	/* rnext */
	if (a_in->core.mtid < 0) {
		a_out->rnext = "*";
	} else if (a_in->core.mtid == a_in->core.tid) {
		a_out->rnext = "=";
	} else {
		a_out->rnext =
			bw->bam.header->target_name[a_in->core.mtid];
	}

	/* pnext */
	a_out->pnext = a_in->core.mpos + 1;

	/* tlen */
	a_out->tlen = a_in->core.isize;

	/* seq and qual */
	if (a_in->core.l_qseq) {
		s = bam1_seq(a_in);
		for (i = 0; i < a_in->core.l_qseq; ++i) {
			a_out->seq[i] = bam_nt16_rev_table[bam1_seqi(s, i)];
		}
		a_out->seq[a_in->core.l_qseq] = '\0';

		s = bam1_qual(a_in);
		if (s[0] == 0xff) {
			a_out->seq[0] = '*';
			a_out->seq[1] = '*';
		} else {
			for (i = 0; i < a_in->core.l_qseq; ++i) {
				a_out->qual[i] = s[i] + 33;
			}
			a_out->qual[a_in->core.l_qseq] = '\0';
		}
	} else {
		a_out->seq[0] = a_out->qual[0] = '*';
		a_out->seq[1] = a_out->qual[1] = '\0';
	}

	return MAL_SUCCEED;
}


static str
write_aux(bam_wrapper * bw, str tag, lng virtual_offset, str type, str val) {
	if (!APPEND_STR(bw->alignments_extra[0], tag))
		WRITE_ERR_PROCESS_ALIGNMENT("extra:tag");
	if (!APPEND_LNG(bw->alignments_extra[1], virtual_offset))
		WRITE_ERR_PROCESS_ALIGNMENT("extra:virtual_offset");
	if (!APPEND_STR(bw->alignments_extra[2], type))
		WRITE_ERR_PROCESS_ALIGNMENT("extra:type");
	if (!APPEND_STR
		(bw->alignments_extra[3],
		 (val ? val : str_nil)))
		WRITE_ERR_PROCESS_ALIGNMENT("extra:value");
	++bw->cnt_alignments_extra;
	return MAL_SUCCEED;
}

static str
write_aux_str(bam_wrapper * bw, str aux, int aux_len, lng virtual_offset) {
	str s = aux;
	char tag[3];
	char type[2];
	str val;

	str msg;

	tag[2] = '\0';
	type[1] = '\0';

	/* Loop until we arrive at a point where there is no more room
	 * for a key:type: string (which takes 5 characters) */
	while (s <= aux + aux_len - 5) {
		tag[0] = *s;
		tag[1] = *(s+1);
		type[0] = *(s+3);
		s += 5;
		if(read_string_until_delim(&s, &val, "\t\n\0", 3) < 0) {
			throw(MAL, "write_aux_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		if((msg = write_aux(bw, tag, virtual_offset, type, val)) != MAL_SUCCEED) {
			GDKfree(val);
			return msg;
		}
		GDKfree(val);
		++s;
	}
	return MAL_SUCCEED;
}

static str
write_aux_bam1_t(bam_wrapper * bw, bam1_t *alig, lng virtual_offset) {
	int i;
	uint8_t *s = bam1_aux(alig);
	str msg;
	while (s < alig->data + alig->data_len) {
		char tag_str[3] = { (char) s[0], (char) s[1], '\0' };
		char type_str[2] = { (char) s[2], '\0' };
		char type = (char) s[2];
		kstring_t aux_value_stream;

		aux_value_stream.l = aux_value_stream.m = 0;
		aux_value_stream.s = 0;
		s += 3;

		if (type == 'C' || type == 'c' || type == 'S' || type == 's'
			|| type == 'I')
			type_str[0] = 'i';
		else if (type != 'A' && type != 'i' && type != 'f'
			 && type != 'd' && type != 'Z' && type != 'H'
			 && type != 'B')
			type_str[0] = '?';

		if (type == 'A') {
			kputc(*s, &aux_value_stream);
			++s;
		} else if (type == 'C') {
			type_str[0] = 'i';
			kputw(*s, &aux_value_stream);
			++s;
		} else if (type == 'c') {
			type_str[0] = 'i';
			kputw(*(int8_t *) s, &aux_value_stream);
			++s;
		} else if (type == 'S') {
			uint16_t u;
			memcpy(&u, s, sizeof(uint16_t));
			type_str[0] = 'i';
			kputw(u, &aux_value_stream);
			s += 2;
		} else if (type == 's') {
			int16_t i;
			memcpy(&i, s, sizeof(int16_t));
			type_str[0] = 'i';
			kputw(i, &aux_value_stream);
			s += 2;
		} else if (type == 'I') {
			uint32_t u;
			memcpy(&u, s, sizeof(uint32_t));
			type_str[0] = 'i';
			kputuw(u, &aux_value_stream);
			s += 4;
		} else if (type == 'i') {
			int32_t i;
			memcpy(&i, s, sizeof(int32_t));
			kputw(i, &aux_value_stream);
			s += 4;
		} else if (type == 'f') {
			float f;
			memcpy(&f, s, sizeof(float));
			ksprintf(&aux_value_stream, "%g", f);
			s += 4;
		} else if (type == 'd') {
			double d;
			memcpy(&d, s, sizeof(double));
			ksprintf(&aux_value_stream, "%lg", d);
			s += 8;
		} else if (type == 'Z' || type == 'H') {
			while (*s) {
				kputc(*s, &aux_value_stream);
				++s;
			}
			++s;
		} else if (type == 'B') {
			uint8_t sub_type = *(s++);
			int32_t n;

			memcpy(&n, s, 4);
			s += 4;	/* now point to the start of the array */
			kputc(sub_type, &aux_value_stream);	/* write the type */
			for (i = 0; i < n; ++i) {
				kputc(',', &aux_value_stream);
				if ('c' == sub_type) {
					kputw(*(int8_t *) s,
						  &aux_value_stream);
					++s;
				} else if ('C' == sub_type) {
					kputw(*(uint8_t *) s,
						  &aux_value_stream);
					++s;
				} else if ('s' == sub_type) {
					int16_t i;
					memcpy(&i, s, sizeof(int16_t));
					kputw(i, &aux_value_stream);
					s += 2;
				} else if ('S' == sub_type) {
					uint16_t u;
					memcpy(&u, s, sizeof(uint16_t));
					kputw(u, &aux_value_stream);
					s += 2;
				} else if ('i' == sub_type) {
					int32_t i;
					memcpy(&i, s, sizeof(int32_t));
					kputw(i, &aux_value_stream);
					s += 4;
				} else if ('I' == sub_type) {
					uint32_t u;
					memcpy(&u, s, sizeof(uint32_t));
					kputuw(u, &aux_value_stream);
					s += 4;
				} else if ('f' == sub_type) {
					float f;
					memcpy(&f, s, sizeof(float));
					ksprintf(&aux_value_stream, "%g", f);
					s += 4;
				}
			}
		}

		if((msg = write_aux(bw, tag_str, virtual_offset,
				type_str, aux_value_stream.s)) != MAL_SUCCEED) {
			if (aux_value_stream.s != NULL) {
				free(aux_value_stream.s);
			}
			return msg;
		}

		if (aux_value_stream.s != NULL) {
			free(aux_value_stream.s);
		}
	}

	return MAL_SUCCEED;
}





/* Macros for appending data from an alignment struct to binary
 * files. Note that str msg should be defined in the caller. */

#define ERR_APPEND_ALIGNMENT(msg, fnc, field)		\
	do {											\
		msg = createException(MAL, fnc,									\
							  SQLSTATE(BA000) "Could not append alignment from file '%s' to binary files: Could not write field '%s' to binary file", \
							  bw->file_location, field);				\
		goto cleanup;													\
	} while (0)

#define APPEND_ALIGNMENT(msg, fnc, a, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12) \
	do {																\
		if(!APPEND_LNG(f1, (a).virtual_offset))							\
			ERR_APPEND_ALIGNMENT(msg, fnc, "virtual_offset");			\
		if(f2 != NULL) {												\
			if(!APPEND_STR(f2, (a).qname))								\
				ERR_APPEND_ALIGNMENT(msg, fnc, "qname");				\
		}																\
		if(!APPEND_SHT(f3, (a).flag))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "flag");						\
		if(!APPEND_STR(f4, (a).rname))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "rname");					\
		if(!APPEND_INT(f5, (a).pos))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "pos");						\
		if(!APPEND_SHT(f6, (a).mapq))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "mapq");						\
		if(!APPEND_STR(f7, (a).cigar))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "cigar");					\
		if(!APPEND_STR(f8, (a).rnext))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "rnext");					\
		if(!APPEND_INT(f9, (a).pnext))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "pnext");					\
		if(!APPEND_INT(f10, (a).tlen))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "tlen");						\
		if(!APPEND_STR(f11, (a).seq))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "seq");						\
		if(!APPEND_STR(f12, (a).qual))									\
			ERR_APPEND_ALIGNMENT(msg, fnc, "qual");						\
	} while (0)

#define APPEND_ALIGNMENT_UNPAIRED(msg, fnc, a, bw)	\
		APPEND_ALIGNMENT(msg, fnc, a,				\
						 bw->alignments[0],			\
						 bw->alignments[1],			\
						 bw->alignments[2],			\
						 bw->alignments[3],			\
						 bw->alignments[4],			\
						 bw->alignments[5],			\
						 bw->alignments[6],			\
						 bw->alignments[7],			\
						 bw->alignments[8],			\
						 bw->alignments[9],			\
						 bw->alignments[10],		\
						 bw->alignments[11])

#define APPEND_ALIGNMENT_PRIM_PAIRED_L(msg, fnc, a, bw)			\
		APPEND_ALIGNMENT(msg, fnc, a,							\
						 bw->alignments_paired_primary[0],		\
						 bw->alignments_paired_primary[2],		\
						 bw->alignments_paired_primary[3],		\
						 bw->alignments_paired_primary[4],		\
						 bw->alignments_paired_primary[5],		\
						 bw->alignments_paired_primary[6],		\
						 bw->alignments_paired_primary[7],		\
						 bw->alignments_paired_primary[8],		\
						 bw->alignments_paired_primary[9],		\
						 bw->alignments_paired_primary[10],		\
						 bw->alignments_paired_primary[11],		\
						 bw->alignments_paired_primary[12])

#define APPEND_ALIGNMENT_PRIM_PAIRED_R(msg, fnc, a, bw)		\
		APPEND_ALIGNMENT(msg, fnc, a,						\
						 bw->alignments_paired_primary[1],	\
						 NULL,								\
						 bw->alignments_paired_primary[13],	\
						 bw->alignments_paired_primary[14],	\
						 bw->alignments_paired_primary[15],	\
						 bw->alignments_paired_primary[16],	\
						 bw->alignments_paired_primary[17],	\
						 bw->alignments_paired_primary[18],	\
						 bw->alignments_paired_primary[19],	\
						 bw->alignments_paired_primary[20],	\
						 bw->alignments_paired_primary[21],	\
						 bw->alignments_paired_primary[22])

#define APPEND_ALIGNMENT_SECO_PAIRED_L(msg, fnc, a, bw)			\
		APPEND_ALIGNMENT(msg, fnc, a,							\
						 bw->alignments_paired_secondary[0],	\
						 bw->alignments_paired_secondary[2],	\
						 bw->alignments_paired_secondary[3],	\
						 bw->alignments_paired_secondary[4],	\
						 bw->alignments_paired_secondary[5],	\
						 bw->alignments_paired_secondary[6],	\
						 bw->alignments_paired_secondary[7],	\
						 bw->alignments_paired_secondary[8],	\
						 bw->alignments_paired_secondary[9],	\
						 bw->alignments_paired_secondary[10],	\
						 bw->alignments_paired_secondary[11],	\
						 bw->alignments_paired_secondary[12])

#define APPEND_ALIGNMENT_SECO_PAIRED_R(msg, fnc, a, bw)			\
		APPEND_ALIGNMENT(msg, fnc, a,							\
						 bw->alignments_paired_secondary[1],	\
						 NULL,									\
						 bw->alignments_paired_secondary[13],	\
						 bw->alignments_paired_secondary[14],	\
						 bw->alignments_paired_secondary[15],	\
						 bw->alignments_paired_secondary[16],	\
						 bw->alignments_paired_secondary[17],	\
						 bw->alignments_paired_secondary[18],	\
						 bw->alignments_paired_secondary[19],	\
						 bw->alignments_paired_secondary[20],	\
						 bw->alignments_paired_secondary[21],	\
						 bw->alignments_paired_secondary[22])



/*
 * Function is called when a collection of alignments is collected
 * that have the same qname; using this group primary and secondary
 * pairs are attempted to form; the remainder is considered unpaired.
 * Note that due to the filtering that is done on any alignment, there
 * should not exist alignments in the array with FIRS_SEGM ==
 * LAST_SEGM. Furthermore, secondary alignments should all have rname
 * <> '*', pos > 0, rnext <> '*' and pnext > 0
 */

static str
complete_qname_group(alignment ** alignments, int nr_alignments,
			 bam_wrapper * bw)
{
	int i, j, nr_primary = 0;
	alignment *a, *a2, *prim_firs_segm = NULL, *prim_last_segm = NULL;
	str msg = MAL_SUCCEED;


	/* Start with handling the primary alignments */
	for (i = 0; i < nr_alignments; ++i) {
		a = alignments[i];
		if (!SECO_ALIG(*a)) {
			/* a points to a primary alignment */
			++nr_primary;
			if (FIRS_SEGM(*a))
				prim_firs_segm = a;
			else
				prim_last_segm = a;
		}
	}
	if (nr_primary == 2 && prim_firs_segm != NULL
		&& prim_last_segm != NULL) {
		APPEND_ALIGNMENT_PRIM_PAIRED_L(msg, "complete_qname_group",
						   *prim_firs_segm, bw);
		APPEND_ALIGNMENT_PRIM_PAIRED_R(msg, "complete_qname_group",
						   *prim_last_segm, bw);

		prim_firs_segm->written = TRUE;
		prim_last_segm->written = TRUE;
		++bw->cnt_alignments_paired_primary;
	}

	/* Now handle the secondary alignments */
	for (i = 0; i < nr_alignments; ++i) {
		a = alignments[i];
		if (a->written || !SECO_ALIG(*a))
			continue;

		for (j = i + 1; j < nr_alignments; ++j) {
			/* Loop starts from j=i+1 since we have
			 * symmetry; if a and b are found to be a
			 * pair, b and a will also be a pair */
			a2 = alignments[j];
			if (a2->written || !SECO_ALIG(*a2))
				continue;

			if (FIRS_SEGM(*a) == LAST_SEGM(*a2) &&
				((strcmp(a->rnext, "=") == 0
				  && strcmp(a->rname, a2->rname) == 0)
				 || strcmp(a->rnext, a2->rname) == 0)
				&& a->pnext == a2->pos
				&&
				((strcmp(a2->rnext, "=") == 0
				  && strcmp(a2->rname, a->rname) == 0)
				 || strcmp(a2->rnext, a->rname) == 0)
				&& a2->pnext == a->pos) {
				/* a and a2 form a secondary pair,
				 * write them */
				if (FIRS_SEGM(*a)) {
					APPEND_ALIGNMENT_SECO_PAIRED_L(msg,
									   "complete_qname_group",
									   *a,
									   bw);
					APPEND_ALIGNMENT_SECO_PAIRED_R(msg,
									   "complete_qname_group",
									   *a2,
									   bw);
				} else {
					APPEND_ALIGNMENT_SECO_PAIRED_L(msg,
									   "complete_qname_group",
									   *a2,
									   bw);
					APPEND_ALIGNMENT_SECO_PAIRED_R(msg,
									   "complete_qname_group",
									   *a,
									   bw);
				}
				a->written = TRUE;
				a2->written = TRUE;
				++bw->cnt_alignments_paired_secondary;
			}
		}
	}

	/* Now write all alignments that have not been written yet */
	for (i = 0; i < nr_alignments; ++i) {
		if (!alignments[i]->written) {
			APPEND_ALIGNMENT_UNPAIRED(msg, "complete_qname_group",
						  *alignments[i], bw);

			alignments[i]->written = TRUE;
			++bw->cnt_alignments;
		}
	}

	return MAL_SUCCEED;

cleanup:
	return msg;
}

#define BAMSAM_TELL(bw) (bw->type == BAM ? bam_tell(bw->bam.input) : (bw->cnt_alignments_total + 1))

str
process_alignments(bam_wrapper * bw, bit * some_thread_failed)
{
	alignment **aligs = NULL;
	int nr_aligs;

	lng voffset;
	bam1_t *alig = NULL;

	int alig_index = 0;

	bit eof = FALSE;
	int i;
	str msg = MAL_SUCCEED;

	/* Initiate to 16 for pairwise schema; if this turns out to be
	 * too small, realloc will be used */
	nr_aligs = bw->dbschema == 0 ? 1 : 16;

	voffset = BAMSAM_TELL(bw);

	if ((aligs =
		 (alignment **) GDKmalloc(nr_aligs * sizeof(alignment *))) == NULL) {
		msg = createException(MAL, "process_alignments",
			SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	/* Enable cleanup to check individual alignments */
	memset(aligs, 0, nr_aligs * sizeof(alignment *));

	for (i = 0; i < nr_aligs; ++i) {
		if ((aligs[i] = (alignment *)GDKmalloc(sizeof(alignment)))
			== NULL) {
			msg = createException(MAL, "process_alignments",
				SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto cleanup;
		}
	}

	for (i = 0; i < nr_aligs; ++i) {
		if (!init_alignment(bw, aligs[i])) {
			msg = createException(MAL, "process_alignments",
				SQLSTATE(HY001) MAL_MALLOC_FAIL);
			goto cleanup;
		}
	}

	if((alig = bam_init1()) == NULL) {
		msg = createException(MAL, "process_alignments",
			SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	if((msg = open_alignment_write_streams(bw)) != MAL_SUCCEED) {
		goto cleanup;
	}

	while (TRUE) { /* One iteration per alignment */
		alignment *a;
		int aux_len = 0;

		/* Start the processing of every alignment with
		 * checking if we should return due to another
		 * thread's failure */
		if (*some_thread_failed)
			goto cleanup;

		/* First retrieve the next alignment */
		a = aligs[alig_index];
		if (bw->type == BAM) {
			if (bam_read1(bw->bam.input, alig) < 0) {
				break;
			}
		} else {
			if((msg = next_sam_alignment(bw->sam.input, voffset, a, &aux_len, &eof)) != MAL_SUCCEED) {
				goto cleanup;
			}
			if (eof) {
				break;
			}
		}

		++bw->cnt_alignments_total;

		if (bw->dbschema == 1 && alig_index > 0
			&& strcmp((bw->type == BAM ? bam1_qname(alig) : a->qname),
				  aligs[alig_index - 1]->qname) != 0) {
			/* Qnames do not match, so the previous
			 * alignments can be considered complete. Use
			 * this knowledge to write the alignments for
			 * that qname to suitable files.
			 */
			if ((msg = complete_qname_group(aligs, alig_index, bw)) != MAL_SUCCEED)
				goto cleanup;

			/* All alignments for the previous qname are
			 * written to files, we can now start
			 * overwriting them. Swap alignments and reset index to 0 */
			aligs[alig_index] = aligs[0];
			aligs[0] = a;
			alig_index = 0;
		}

		if (bw->type == BAM) {
			if((msg = bam1_t2alignment(bw, voffset, alig, a)) != MAL_SUCCEED) {
				goto cleanup;
			}
		}

		/* At this point, a is completely up to date */

		if (bw->dbschema == 0) {
			/* Write data directly if we are loading into the sfw storage schema */
			APPEND_ALIGNMENT_UNPAIRED(msg, "process_alignments", *a, bw);
			++bw->cnt_alignments;
		}

		/* Always write auxiliary data immediately */
		if (bw->type == BAM) {
			msg = write_aux_bam1_t(bw, alig, voffset);
		} else {
			msg = write_aux_str(bw, a->aux, aux_len, voffset);
		}

		if (msg != MAL_SUCCEED) {
			goto cleanup;
		}

		/* voffset can be updated for the next iteration at
		 * this point already */
		voffset = BAMSAM_TELL(bw);

		if (bw->dbschema == 1) {
			/* We are building the paired
			 * schema. Therefore, a
			 * is now filled with alignment data. In some
			 * cases, we can dump it in unpaired storage
			 * immediately */

			if (FIRS_SEGM(*a) == LAST_SEGM(*a) ||
				(SECO_ALIG(*a) &&
				 (strcmp(a->rname, "*") == 0 || a->pos <= 0 ||
				  strcmp(a->rnext, "*") == 0 || a->pnext <= 0)
				)
				) {
				APPEND_ALIGNMENT_UNPAIRED(msg,
							  "process_alignments",
							  *a, bw);
				a->written = TRUE;
				++bw->cnt_alignments;
			} else {
				/* The alignment can not be written
				 * yet, so store it by increasing the
				 * index of the next alignment */
				++alig_index;

				/* Make sure we still have enough
				 * alignment structs */
				if (alig_index >= nr_aligs) {
					/* Double the size of the aligs array */
					int new_nr_aligs = 2 * nr_aligs;
					alignment **tmp;

					if ((tmp =
						 GDKrealloc(aligs,
							new_nr_aligs *
							sizeof(alignment *))) ==
						NULL) {
						GDKfree(aligs);
						msg = createException(MAL,
									  "process_alignments",
									  SQLSTATE(HY001) MAL_MALLOC_FAIL);
						goto cleanup;
					}
					aligs = tmp;

					/* Init newly allocated memory
					 * to zero for cleanup */
					memset(aligs + nr_aligs, 0,
						   (new_nr_aligs -
						nr_aligs) *
						   sizeof(alignment));

					/* And init all new alignments */
					for (i = nr_aligs; i < new_nr_aligs;
						 ++i) {
						if (!init_alignment
							(bw, aligs[i])) {
							msg = createException
								(MAL,
								 "process_alignments",
								 SQLSTATE(HY001) MAL_MALLOC_FAIL);
							goto cleanup;
						}
					}
					nr_aligs = new_nr_aligs;
					TO_LOG("<bam_loader> Increased size of alignment buffer to %d alignments\n", nr_aligs);
				}
			}
		}
	}
	if (bw->dbschema == 1) {
		/* alignments will still contain at least one
		 * alignment, so empty it */
		msg = complete_qname_group(aligs, alig_index, bw);
	}

	  cleanup:
	if (alig)
		bam_destroy1(alig);
	if (aligs) {
		for (i = 0; i < nr_aligs; ++i) {
			if (aligs[i]) {
				clear_alignment(bw, aligs[i]);
				GDKfree(aligs[i]);
			}
		}
		GDKfree(aligs);
	}
	close_alignment_write_streams(bw);
	if (msg != MAL_SUCCEED) {
		/* An error occurred in this thread, indicate this by
		 * setting the shared failure bit to TRUE */
		*some_thread_failed = TRUE;
	}
	return msg;
}
