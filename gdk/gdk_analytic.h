/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
* Pedro Ferreira
* This file contains declarations for SQL window analytical functions.
*/

#ifndef _GDK_ANALYTIC_H_
#define _GDK_ANALYTIC_H_

gdk_export gdk_return GDKanalyticaldiff(BAT *r, BAT *b, BAT *p, const bit *restrict npbit, int tpe);

gdk_export gdk_return GDKanalyticalntile(BAT *r, BAT *b, BAT *p, BAT *n, int tpe, const void *restrict ntile);
gdk_export gdk_return GDKanalyticallag(BAT *r, BAT *b, BAT *p, BUN lag, const void *restrict default_value, int tpe);
gdk_export gdk_return GDKanalyticallead(BAT *r, BAT *b, BAT *p, BUN lead, const void *restrict default_value, int tpe);

gdk_export gdk_return GDKanalyticalwindowbounds(BAT *r, BAT *b, BAT *p, BAT *l,
						const void *restrict bound,
						int tp1, int tp2, int unit,
						bool preceding, oid first_half);

gdk_export gdk_return GDKanalyticalfirst(BAT *r, BAT *b, BAT *s, BAT *e, int tpe);
gdk_export gdk_return GDKanalyticallast(BAT *r, BAT *b, BAT *s, BAT *e, int tpe);
gdk_export gdk_return GDKanalyticalnthvalue(BAT *r, BAT *b, BAT *s, BAT *e, BAT *l, lng *nth, int tp1);

gdk_export gdk_return GDKanalyticalmin(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalyticalmax(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalyticalcount(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, bit ignore_nils, int tpe, int frame_type);
gdk_export gdk_return GDKanalyticalsum(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tp1, int tp2, int frame_type);
gdk_export gdk_return GDKanalyticalprod(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tp1, int tp2, int frame_type);
gdk_export gdk_return GDKanalyticalavg(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalyticalavginteger(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);

gdk_export gdk_return GDKanalytical_stddev_samp(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalytical_stddev_pop(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalytical_variance_samp(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalytical_variance_pop(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalytical_covariance_pop(BAT *r, BAT *p, BAT *o, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalytical_covariance_samp(BAT *r, BAT *p, BAT *o, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe, int frame_type);
gdk_export gdk_return GDKanalytical_correlation(BAT *r, BAT *p, BAT *o, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe, int frame_type);

#define SEGMENT_TREE_FANOUT 16
#define NOTHING /* used for not used optional arguments for aggregate computation */

/* segment_tree is the tree as an array, levels_offset contains the offsets in the tree where which level does start,
   and nlevels contains the number of levels on the current segment tree */
gdk_export gdk_return GDKrebuild_segment_tree(oid ncount, oid data_size, void **segment_tree, oid *tree_capacity, oid **levels_offset, oid *nlevels);

/* segment_tree, levels_offset and nlevels must be already defined. ARG1, ARG2 and ARG3 are to be used by the aggregate */
#define populate_segment_tree(CAST, COUNT, INIT_AGGREGATE, COMPUTE_LEVEL0, COMPUTE_LEVELN, ARG1, ARG2, ARG3) \
	do {	\
		CAST *ctree = (CAST *) segment_tree; \
		CAST *prev_level_begin = ctree; \
		oid level_size = COUNT, tree_offset = 0, current_level = 0; \
	\
		levels_offset[current_level++] = 0; /* first level is trivial */ \
		for (oid pos = 0; pos < level_size; pos += SEGMENT_TREE_FANOUT) { \
			oid end = MIN(level_size, pos + SEGMENT_TREE_FANOUT); \
	\
			for (oid x = pos; x < end; x++) { \
				CAST computed; \
				COMPUTE_LEVEL0(x, ARG1, ARG2, ARG3);	\
				ctree[tree_offset++] = computed; \
			} \
		} \
	\
 		while (current_level < nlevels) { /* for the following levels we have to use the previous level results */  \
		 	oid prev_tree_offset = tree_offset; \
			levels_offset[current_level++] = tree_offset; \
			for (oid pos = 0; pos < level_size; pos += SEGMENT_TREE_FANOUT) { \
				oid begin = pos, end = MIN(level_size, pos + SEGMENT_TREE_FANOUT), width = end - begin; \
				CAST computed; \
	\
				INIT_AGGREGATE(ARG1, ARG2, ARG3);	\
				for (oid x = 0; x < width; x++) \
					COMPUTE_LEVELN(prev_level_begin[x], ARG1, ARG2, ARG3);	\
				ctree[tree_offset++] = computed; \
				prev_level_begin += width; \
			} \
			level_size = tree_offset - prev_tree_offset; \
		} \
	} while (0)

#define compute_on_segment_tree(CAST, START, END, INIT_AGGREGATE, COMPUTE, FINALIZE_AGGREGATE, ARG1, ARG2, ARG3)	\
	do { /* taken from https://www.vldb.org/pvldb/vol8/p1058-leis.pdf */	\
		oid begin = START, tend = END; \
		CAST computed; \
	\
		INIT_AGGREGATE(ARG1, ARG2, ARG3);	\
		for (oid level = 0; level < nlevels; level++) { \
			CAST *tlevel = (CAST *) segment_tree + levels_offset[level]; \
			oid parent_begin = begin / SEGMENT_TREE_FANOUT; \
			oid parent_end = tend / SEGMENT_TREE_FANOUT; \
	\
			if (parent_begin == parent_end) { \
				for (oid pos = begin; pos < tend; pos++) \
					COMPUTE(tlevel[pos], ARG1, ARG2, ARG3);	\
				break; \
			} \
			oid group_begin = parent_begin * SEGMENT_TREE_FANOUT; \
			if (begin != group_begin) { \
				oid limit = group_begin + SEGMENT_TREE_FANOUT; \
				for (oid pos = begin; pos < limit; pos++) \
					COMPUTE(tlevel[pos], ARG1, ARG2, ARG3);	\
				parent_begin++; \
			} \
			oid group_end = parent_end * SEGMENT_TREE_FANOUT; \
			if (tend != group_end) { \
				for (oid pos = group_end; pos < tend; pos++) \
					COMPUTE(tlevel[pos], ARG1, ARG2, ARG3);	\
			} \
			begin = parent_begin; \
			tend = parent_end; \
		} \
		FINALIZE_AGGREGATE(ARG1, ARG2, ARG3); \
	} while (0)

#endif //_GDK_ANALYTIC_H_
