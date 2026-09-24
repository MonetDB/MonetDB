/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * This file contains a reader of aligmnent data from *.sam and *.bam files
 * and load it into a transient MonetDB table consisting of 11 columns.
 * SAM stands for Sequence Alignment Map. Ref: https://en.wikipedia.org/wiki/SAM_(file_format)
 * BAM stands for Binary Alignment Map.   Ref: https://en.wikipedia.org/wiki/BAM_(file_format)
 * BAM is the compressed binary representation of SAM (Sequence Alignment Map).
 * BAM is one of the most common file formats for raw genome sequencing data.
 *
 * A SAM file is a TAB-delimited text format consisting of a header section, which is optional,
 * and an alignment section. If present, the header must be prior to the alignments.
 * Header lines start with ‘@’, while alignment lines do not.
 * Each alignment line has 11 mandatory fields for essential alignment information
 * such as mapping position, and variable number of optional fields for flexible
 * or aligner specific information.
 * Ref: https://samtools.github.io/hts-specs/SAMv1.pdf
 *
 * We use the htslib library for reading data from *.sam and *.bam files.
 * Ref: https://github.com/samtools/htslib
 * To install this library and its include files, follow instructions at:
 * https://github.com/samtools/htslib/blob/develop/INSTALL
 * Note: when you issue last step 'make install' you need root privileges so do a 'sudo make install' instead.
 *
 * Limitations:
 * - Only the alignment data is converted into bats, not the header information as it has a different table format.
 * - We currently only read the first 11 mandatory columns. Thus ignoring any additonal columns of info.
 * - The topn filter is not yet implemented.
 * - *.cram files are not yet supported.
 *
 * Author: Martin van Dinther
 */

#include "monetdb_config.h"
#include "gdk.h"		// COLnew(), bunfastapp()
#include "mal_builder.h"	// newStmtArgs(), pushStr()
#include "rel_exp.h"		// exp_column()
#include "rel_file_loader.h"	// fl_register(), fl_unregister()
#include "sql_monet_backend.h"	// backend, getBackendContext()

#include <htslib/sam.h>	// it includes hts.h
/* note there is no <htslib/bam.h> */

/* copied from monetdb5/modules/mal/tablet.c */
static BAT *
bat_create(int adt, BUN nr)
{
	BAT *b = COLnew(0, adt, nr, TRANSIENT);

	/* check for correct structures */
	if (b == NULL)
		return NULL;
	if ((b = BATsetaccess(b, BAT_APPEND)) == NULL) {
		return NULL;
	}

	/* disable all properties here */
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	b->tnil = false;
	b->tnonil = false;
	return b;
}

static void
sam_list_append(list * nameslist, char * name, list * typelist, sql_subtype * mtype, list *res_exps, mvc *sql, int has_nils)
{
	list_append(nameslist, name);
	list_append(typelist, mtype);
	if (res_exps) {
		sql_exp *ne = exp_column(sql->sa, "", name, mtype, CARD_MULTI, has_nils, 0, 0);
		set_basecol(ne);
		ne->alias.label = -(sql->nid++);
		list_append(res_exps, ne);
	}
}

/* nr of sam/bam file alignment info columns */
#define SAM_NR_COLS  11
#define SAM_MAX_STR_SIZE 4096

#define SAM_RELATION 1
#define SAM_LOADER   2

/*
 * sam_query() contains the logic for both sam_relation() and SAMloader() functions.
 * the caller argument is SAM_RELATION when called from sam_relation() or SAM_LOADER when called from SAMloader().
 */
static str
sam_query(int caller, mvc *sql, sql_subfunc *f, char *filename, list *res_exps, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	assert(caller == SAM_RELATION || caller == SAM_LOADER);

	if (sql == NULL)
		return "Missing mvc value";
	if (f == NULL)
		return "Missing sql_subfunc value";
	if (filename == NULL)
		return "Missing filename value";

	bam1_t *bamdata = NULL;
	sam_hdr_t *in_samhdr = NULL;
	samFile *infile = NULL;
	char * errmsg = NULL;

	if (!(bamdata = bam_init1())) {
		errmsg = "baminit1() failed to allocate memory";
		goto end;
	}

	// open input file
	if (!(infile = sam_open(filename, "r"))) {
		/* TODO sam_open() logs error msg to stderr when it fails:
			[E::hts_open_format] Failed to open file "file1.sam" : No such file or directory
			We need to prevent this logging to stderr.
		 */
		errmsg = "Could not open sam/bam file";
		/* errmsg = RUNTIME_FILE_NOT_FOUND; */
		goto end;
	}

	// Read a header from a SAM, BAM or CRAM file.
	if (!(in_samhdr = sam_hdr_read(infile))) {
		errmsg = "Failed to read sam/bam file header";
		goto end;
	}

	/* Alignment data section in a sam/bam/cram file defines 11 mandatory columns
		Ref: https://samtools.github.io/hts-specs/SAMv1.pdf  secction 1.4
		Col Field Type Regexp/Range Brief description
		1 QNAME String [!-?A-~]{1,254} Query template NAME
		2 FLAG Int [0, 2^16 − 1] bitwise FLAG
		3 RNAME String \*|[:rname:∧*=][:rname:]* Reference sequence NAME12
		4 POS Int [0, 2^31 − 1] 1-based leftmost mapping POSition
		5 MAPQ Int [0, 2^8 − 1] MAPping Quality
		6 CIGAR String \*|([0-9]+[MIDNSHP=X])+ CIGAR string
		7 RNEXT String \*|=|[:rname:∧*=][:rname:]* Reference name of the mate/next read
		8 PNEXT Int [0, 2^31 − 1] Position of the mate/next read
		9 TLEN Int [−2^31 + 1, 2^31 − 1] observed Template LENgth
		10 SEQ String \*|[A-Za-z=.]+ segment SEQuence
		11 QUAL String [!-~]+ ASCII of Phred-scaled base QUALity+33
	 */

	/* TODO extend with 2 (or more) optional fields (all varchar4096),
		Ref: https://samtools.github.io/hts-specs/SAMv1.pdf  secction 1.5
		Col Field Type Regexp/Range Brief description
		12 SAM_AUX String
		13 SAM_RGAUX String
	 */

	/* when called from sam_relation() */
	if (caller == SAM_RELATION) {
		sql_subtype * varchar255 = sql_bind_subtype(sql->sa, "varchar", (unsigned int) BAM_MAX_QNAME_LEN +1, 0);
		sql_subtype * varchar1020 = sql_bind_subtype(sql->sa, "varchar", (unsigned int) 1020, 0);
		sql_subtype * varchar4096 = sql_bind_subtype(sql->sa, "varchar", (unsigned int) SAM_MAX_STR_SIZE, 0);
		sql_subtype * int31 = sql_bind_subtype(sql->sa, "int", (unsigned int) 0, 0);
		sql_subtype * int15 = sql_bind_subtype(sql->sa, "smallint", (unsigned int) 0, 0);

		list * typelist = sa_list(sql->sa);
		list * nameslist = sa_list(sql->sa);

		sam_list_append(nameslist, "qname", typelist, varchar255, res_exps, sql, 0);
		sam_list_append(nameslist, "flag",  typelist, int31, res_exps, sql, 0);
		sam_list_append(nameslist, "rname", typelist, varchar1020, res_exps, sql, 0);
		sam_list_append(nameslist, "pos",   typelist, int31, res_exps, sql, 0);
		sam_list_append(nameslist, "mapq",  typelist, int15, res_exps, sql, 0);
		sam_list_append(nameslist, "cigar", typelist, varchar4096, res_exps, sql, 0);
		sam_list_append(nameslist, "rnext", typelist, varchar1020, res_exps, sql, 0);
		sam_list_append(nameslist, "pnext", typelist, int31, res_exps, sql, 0);
		sam_list_append(nameslist, "tlen",  typelist, int31, res_exps, sql, 0);
		sam_list_append(nameslist, "seq",   typelist, varchar4096, res_exps, sql, 0);
		sam_list_append(nameslist, "qual",  typelist, varchar4096, res_exps, sql, 0);
		/* TODO extend with 2 (or more) optional fields,
		sam_list_append(nameslist, "sam_aux",   typelist, varchar4096, res_exps, sql, 1);
		sam_list_append(nameslist, "sam_rgaux", typelist, varchar4096, res_exps, sql, 1);
		*/

		f->tname = ma_strdup(sql->sa, "");	/* TODO maybe use the filename without path and extension */
		f->colnames = nameslist;
		f->coltypes = typelist;
		f->res = typelist;
		goto end;
	}

	/* when called from SAMloader() */
	if (caller == SAM_LOADER) {
		typedef struct {
			int battype;	/* MonetDB atom type, used to create the BAT */
			BAT * bat;		/* MonetDB BAT */
		} rescol_t;

		rescol_t * colmetadata = (rescol_t *) GDKmalloc(SAM_NR_COLS * sizeof(rescol_t));
		if (colmetadata == NULL) {
			errmsg = "GDKzalloc colmetadata[SAM_NR_COLS] failed.";
			goto end;
		}

		assert(mb);
		assert(pci);
		int col;
		/* make bats with right atom type */
		for (col = 0; col < SAM_NR_COLS; col++) {
			int battype = getBatType(getArgType(mb, pci, col));
			TRC_DEBUG(LOADER, "Before create BAT %d type %d\n", col+1, battype);
			colmetadata[col].battype = battype;
			BAT * b = bat_create(battype, 0);
			if (b) {
				colmetadata[col].bat = b;
				TRC_DEBUG(LOADER, "Created BAT %d\n", col+1);
			} else {
				errmsg = "Failed to create bat.";
				/* cleanup already created bats */
				while (col > 0) {
					col--;
					BBPreclaim(colmetadata[col].bat);
				}
				GDKfree(colmetadata);
				goto end;
			}
		}

		int ret_r = 0;
		gdk_return gdkret = GDK_SUCCEED;
		int i = 0;
		int int_val = 0;
		sht sht_val = 0;
		unsigned long row = 0;
		char buf[SAM_MAX_STR_SIZE +1];	// buffer for composing string values
		size_t pos = 0;
		while ((ret_r = sam_read1(infile, in_samhdr, bamdata)) >= 0)
		{
			// TODO implement filters here (such as topn or quality filter) which exclude rows to be bunfastapp-ed

			//QNAME FLAG RNAME POS MAPQ CIGAR RNEXT PNEXT TLEN SEQ QUAL [TAG:TYPE:VALUE]…
			row++;
			TRC_DEBUG(LOADER, "Fetched row %lu\n", row);

			if (gdkret == GDK_SUCCEED) {
				/* 1 QNAME String [!-?A-~]{1,254} Query template NAME */
				gdkret = bunfastapp(colmetadata[0].bat, (void *) bam_get_qname(bamdata));
			}
			if (gdkret == GDK_SUCCEED) {
				/* 2 FLAG Int [0, 2^16 − 1] bitwise FLAG */
				int_val = bamdata->core.flag;
				gdkret = bunfastapp(colmetadata[1].bat, (void *) &int_val);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 3 RNAME String \*|[:rname:∧*=][:rname:]* Reference sequence NAME12 */
				const char * tidname = sam_hdr_tid2name(in_samhdr, bamdata->core.tid);
				gdkret = bunfastapp(colmetadata[2].bat, (void *) tidname? tidname: "");
			}
			if (gdkret == GDK_SUCCEED) {
				/* 4 POS Int [0, 2^31 − 1] 1-based leftmost mapping POSition */
				int_val = bamdata->core.pos + 1;
				gdkret = bunfastapp(colmetadata[3].bat, (void *) &int_val);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 5 MAPQ Int [0, 2^8 − 1] MAPping Quality */
				sht_val = bamdata->core.qual;
				gdkret = bunfastapp(colmetadata[4].bat, (void *) &sht_val);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 6 CIGAR String \*|([0-9]+[MIDNSHP=X])+ CIGAR string */
				pos = 0;
				uint32_t *cigar = bam_get_cigar(bamdata);
				for (uint32_t i = 0; i < bamdata->core.n_cigar; ++i) {	//no. of cigar data entries
					//the macros gives the count of operation and the symbol of operation for given cigar entry
					pos += snprintf(buf + pos, sizeof(buf) - pos, "%u%c", bam_cigar_oplen(cigar[i]), bam_cigar_opchr(cigar[i]));
				}
				gdkret = bunfastapp(colmetadata[5].bat, (void *) buf);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 7 RNEXT String \*|=|[:rname:∧*=][:rname:]* Reference name of the mate/next read */
				const char * tidname = sam_hdr_tid2name(in_samhdr, bamdata->core.tid);
				gdkret = bunfastapp(colmetadata[6].bat, (void *) tidname? tidname: "");
			}
			if (gdkret == GDK_SUCCEED) {
				/* 8 PNEXT Int [0, 2^31 − 1] Position of the mate/next read */
				int_val = bamdata->core.flag;
				gdkret = bunfastapp(colmetadata[7].bat, (void *) &int_val);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 9 TLEN Int [−2^31 + 1, 2^31 − 1] observed Template LENgth */
				int_val = bamdata->core.isize;
				gdkret = bunfastapp(colmetadata[8].bat, (void *) &int_val);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 10 SEQ String \*|[A-Za-z=.]+ segment SEQuence */
				pos = 0;
				uint8_t *data = bam_get_seq(bamdata);
				for (i = 0; i < bamdata->core.l_qseq ; ++i) {	//sequence length
					//retrieves the base from (internal compressed) sequence data
				buf[pos++] = (char) seq_nt16_str[bam_seqi(data, i)];
				}
				buf[pos] = '\0';
				gdkret = bunfastapp(colmetadata[9].bat, (void *) buf);
			}
			if (gdkret == GDK_SUCCEED) {
				/* 11 QUAL String [!-~]+ ASCII of Phred-scaled base QUALity+33 */
				pos = 0;
				for (i = 0; i < bamdata->core.l_qseq ; ++i) {
					//retrieves the quality value
					buf[pos++] = (char) bam_get_qual(bamdata)[i]+33;
				}
				buf[pos] = '\0';
				gdkret = bunfastapp(colmetadata[10].bat, (void *) buf);
			}

			/* TODO extend with 2 (or more) optional fields (all varchar4096),
				Ref: https://samtools.github.io/hts-specs/SAMv1.pdf  secction 1.5
				bam_get_aux(bamdata)
			 */

			if (gdkret != GDK_SUCCEED) {
				TRC_ERROR(LOADER, "bunfastapp(b, val) failed!\n");
				break;
			}
		}

		/* after doing bunfastapp() update the properties for each BAT */
		for (col = 0; col < SAM_NR_COLS; col++) {
			BAT * b = colmetadata[col].bat;
			BATsettrivprop(b);
			/* TODO check that nil and nonil are set correctly for first 11 mandatory cols */
		}

		/* pass bats to caller */
		for (col = 0; col < SAM_NR_COLS; col++) {
			bat * rescol = getArgReference_bat(stk, pci, col);
			BAT * b = colmetadata[col].bat;
			if (rescol && b) {
				*rescol = b->batCacheid;
				BBPkeepref(b);
			}
			TRC_DEBUG(LOADER, "col %d pass bat %d\n", col, b->ttype);
		}
		/* free locally allocated memory */
		GDKfree(colmetadata);
	} /* end of: if (caller == SAM_LOADER) */

  end:
	//cleanup
	if (in_samhdr) {
		sam_hdr_destroy(in_samhdr);
	}
	if (infile) {
		sam_close(infile);
	}
	if (bamdata) {
		bam_destroy1(bamdata);
	}
	return (errmsg != NULL) ? (str)errmsg : MAL_SUCCEED;
}

/*
 * returns an error string (static or via tmp sa_allocator allocated), NULL on success
 *
 * Extend the subfunc f with result columns, ie.
	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist; use tname if passed, for the relation name
 * Fill the list res_exps, with one result expressions per resulting column.
 */
static str
sam_relation(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname, lng *est)
{
	(void) tname;
	(void) est;
	return sam_query(SAM_RELATION, sql, f, filename, res_exps, NULL, NULL, NULL);
}

static void *
sam_load(void *BE, sql_subfunc *f, char *filename, sql_exp *topn)
{
	backend *be = (backend*)BE;
	if (!f)
		return NULL;

	(void)topn;

	InstrPtr q = newStmtArgs(be->mb, "sam", "loader", list_length(f->coltypes) + 2);
	if (q == NULL)
		return NULL;

	int col = 0;
	list *l = sa_list(be->mvc->sa);
	for (node *n = f->coltypes->h, *nn = f->colnames->h; n && nn; col++, n = n->next, nn = nn->next) {
		const char *name = nn->data;
		sql_subtype *tp = n->data;
		if (tp) {
			int type = newBatType(tp->type->localtype);
			if (col)
				q = pushReturn(be->mb, q, newTmpVariable(be->mb, type));
			else
				getArg(q, 0) = newTmpVariable(be->mb, type);
			stmt *s = stmt_blackbox_result(be, q, col, tp);
			s = stmt_alias(be, s, col+1, f->tname, name);
			list_append(l, s);
		}
	}
	q = pushStr(be->mb, q, filename);
	q = pushPtr(be->mb, q, f);
	pushInstruction(be->mb, q);
	return stmt_list(be, l);
}

static str
SAMloader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	if (mb == NULL)
		return "Missing MalBlkPtr";
	if (stk == NULL)
		return "Missing MalStkPtr";
	if (pci == NULL)
		return "Missing InstrPtr";

	backend *be = NULL;
	str msg = getBackendContext(cntxt, &be);
	if (msg != NULL)
		return msg;

	str filename = *getArgReference_str(stk, pci, pci->retc);
	sql_subfunc *f = *(sql_subfunc**)getArgReference_ptr(stk, pci, pci->retc+1);
	return sam_query(SAM_LOADER, be->mvc, f, filename, NULL, mb, stk, pci);
}

static str
SAMprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	(void)stk;
	(void)pci;
	fl_register("sam", &sam_relation, &sam_load);
	fl_register("bam", &sam_relation, &sam_load);
	return MAL_SUCCEED;
}

static str
SAMepilogue(Client cntxt, void *ret)
{
	(void)cntxt;
	(void)ret;
	fl_unregister("sam");
	fl_unregister("bam");
	return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func sam_init_funcs[] = {
	pattern("sam", "prelude", SAMprelude, false, "", noargs),
	command("sam", "epilogue", SAMepilogue, false, "", noargs),
	pattern("sam", "loader", SAMloader, true, "Import alignment data from a .sam/.bam file", args(1,3, batvarargany("",0),arg("filename",str),arg("func",ptr))),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_sam_mal)
{ mal_module("sam", NULL, sam_init_funcs); }

