/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * @f txtsim
 * @t Module providing similarity metrics for strings
 * @a Romulo Goncalves (from M4 to M5)
 * @d 01/12/2007
 * @v 0.1
 *
 * @+ String metrics
 *
 * Provides basic similarity metrics for strings.
 *
 */
#include "monetdb_config.h"
#include "mal.h"
#include <string.h>
#include "gdk.h"
#include "str.h"
#include <limits.h>
#include "mal_exception.h"


#define RETURN_NIL_IF(b,t) \
	if (b) {\
	   if (ATOMextern(t)) {\
	      *(ptr*) res = (ptr) ATOMnil(t);\
		  if ( *(ptr *) res == NULL)\
			throw(MAL,"txtsim", SQLSTATE(HY013) MAL_MALLOC_FAIL);\
	   } else {\
	      memcpy(res, ATOMnilptr(t), ATOMsize(t));\
 	   }\
	   return MAL_SUCCEED; \
	}

/* =========================================================================
 * LEVENSH?TEIN FUNCTION
 * Source:
 * http://www.merriampark.com/ld.htm
 * =========================================================================
 */

#define MYMIN(x,y) ( (x<y) ? x : y )
#define SMALLEST_OF(x,y,z) ( MYMIN(MYMIN(x,y),z) )
#define SMALLEST_OF4(x,y,z,z2) ( MYMIN(MYMIN(MYMIN(x,y),z),z2) )

/***************************************************
 * Get a pointer to the specified cell of the matrix
 **************************************************/

static inline int *
levenshtein_GetCellPointer(int *pOrigin, int col, int row, int nCols)
{
	return pOrigin + col + (row * (nCols + 1));
}

/******************************************************
 * Get the contents of the specified cell in the matrix
 *****************************************************/

static inline int
levenshtein_GetAt(int *pOrigin, int col, int row, int nCols)
{
	int *pCell;

	pCell = levenshtein_GetCellPointer(pOrigin, col, row, nCols);
	return *pCell;

}

/********************************************************
 * Fill the specified cell in the matrix with the value x
 *******************************************************/

static inline void
levenshtein_PutAt(int *pOrigin, int col, int row, int nCols, int x)
{
	int *pCell;

	pCell = levenshtein_GetCellPointer(pOrigin, col, row, nCols);
	*pCell = x;
}


/******************************
 * Compute Levenshtein distance
 *****************************/
static str
levenshtein_impl(int *result, str *S, str *T, int *insdel_cost, int *replace_cost, int *transpose_cost)
{
	char *s = *S;
	char *t = *T;
	int *d;			/* pointer to matrix */
	int n;			/* length of s */
	int m;			/* length of t */
	int i;			/* iterates through s */
	int j;			/* iterates through t */
	char s_i;		/* ith character of s */
	char t_j;		/* jth character of t */
	int cost;		/* cost */
	int cell;		/* contents of target cell */
	int above;		/* contents of cell immediately above */
	int left;		/* contents of cell immediately to left */
	int diag;		/* contents of cell immediately above and to left */
	int sz;			/* number of cells in matrix */
	int diag2 = 0, cost2 = 0;

	if (strNil(*S) || strNil(*T)) {
		*result = int_nil;
		return MAL_SUCCEED;
	}

	/* Step 1 */
	n = (int) strlen(s);	/* 64bit: assume strings are less than 2 GB */
	m = (int) strlen(t);
	if (n == 0) {
		*result = m;
		return MAL_SUCCEED;
	}
	if (m == 0) {
		*result = n;
		return MAL_SUCCEED;
	}
	sz = (n + 1) * (m + 1) * sizeof(int);
	d = (int *) GDKmalloc(sz);
	if ( d == NULL)
		throw(MAL,"levenshtein", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* Step 2 */
	for (i = 0; i <= n; i++) {
		levenshtein_PutAt(d, i, 0, n, i);
	}

	for (j = 0; j <= m; j++) {
		levenshtein_PutAt(d, 0, j, n, j);
	}

	/* Step 3 */
	for (i = 1; i <= n; i++) {

		s_i = s[i - 1];

		/* Step 4 */
		for (j = 1; j <= m; j++) {

			t_j = t[j - 1];

			/* Step 5 */
			if (s_i == t_j) {
				cost = 0;
			} else {
				cost = *replace_cost;
			}

			/* Step 6 */
			above = levenshtein_GetAt(d, i - 1, j, n);
			left = levenshtein_GetAt(d, i, j - 1, n);
			diag = levenshtein_GetAt(d, i - 1, j - 1, n);

			if (j >= 2 && i >= 2) {
				/* NEW: detect transpositions */

				diag2 = levenshtein_GetAt(d, i - 2, j - 2, n);
				if (s[i - 2] == t[j - 1] && s[i - 1] == t[j - 2]) {
					cost2 = *transpose_cost;
				} else {
					cost2 = 2;
				}
				cell = SMALLEST_OF4(above + *insdel_cost, left + *insdel_cost, diag + cost, diag2 + cost2);
			} else {
				cell = SMALLEST_OF(above + *insdel_cost, left + *insdel_cost, diag + cost);
			}
			levenshtein_PutAt(d, i, j, n, cell);
		}
	}

	/* Step 7 */
	*result = levenshtein_GetAt(d, n, m, n);
	GDKfree(d);
	return MAL_SUCCEED;
}

static str
levenshteinbasic_impl(int *result, str *s, str *t)
{
	int insdel = 1, replace = 1, transpose = 2;

	return levenshtein_impl(result, s, t, &insdel, &replace, &transpose);
}

static str
levenshteinbasic2_impl(int *result, str *s, str *t)
{
	int insdel = 1, replace = 1, transpose = 1;

	return levenshtein_impl(result, s, t, &insdel, &replace, &transpose);
}


/* =========================================================================
 * SOUNDEX FUNCTION
 * Source:
 * http://www.mit.edu/afs/sipb/service/rtfm/src/freeWAIS-sf-1.0/ir/soundex.c
 * =========================================================================
 */

#define SoundexLen 4		/* length of a soundex code */
#define SoundexKey "Z000"	/* default key for soundex code */

/* set letter values */
static int Code[] = { 0, 1, 2, 3, 0, 1, 2, 0, 0, 2, 2, 4, 5, 5, 0,
	1, 2, 6, 2, 3, 0, 1, 0, 2, 0, 2
};

static inline char
SCode(unsigned char c)
{
	if (c == 95)
		return (2);	/* german sz */
	return (Code[toupper(c) - 'A']);
}

static void
soundex_code(char *Name, char *Key)
{
	char LastLetter;
	int Index;

	/* set default key */
	strcpy(Key, SoundexKey);

	/* keep first letter */
	Key[0] = *Name;
	if (!isupper((unsigned char) (Key[0])))
		Key[0] = toupper(Key[0]);

	LastLetter = *Name;
	if (!*Name)
		return;
	Name++;

	/* scan rest of string */
	for (Index = 1; (Index <SoundexLen) &&*Name; Name++) {
		/* use only letters */
		if (isalpha((unsigned char) (*Name))) {
			/* ignore duplicate successive chars */
			if (LastLetter != *Name) {
				/* new LastLetter */
				LastLetter = *Name;

				/* ignore letters with code 0 */
				if (SCode(*Name) != 0) {
					Key[Index] = '0' + SCode(*Name);
					Index ++;
				}
			}
		}
	}
}

static str
soundex_impl(str *res, str *Name)
{
	RETURN_NIL_IF(strNil(*Name), TYPE_str);

	*res = (str) GDKmalloc(sizeof(char) * (SoundexLen + 1));
	if( *res == NULL)
		throw(MAL,"soundex", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* calculate Key for Name */
	soundex_code(*Name, *res);

	return MAL_SUCCEED;
}

static str
stringdiff_impl(int *res, str *s1, str *s2)
{
	str r = MAL_SUCCEED;
	char *S1 = NULL, *S2 = NULL;

	r = soundex_impl(&S1, s1);
	if( r != MAL_SUCCEED)
		return r;
	r = soundex_impl(&S2, s2);
	if( r != MAL_SUCCEED){
		GDKfree(S1);
		return r;
	}
	r = levenshteinbasic_impl(res, &S1, &S2);
	GDKfree(S1);
	GDKfree(S2);
	return r;
}

/******************************
 * QGRAMNORMALIZE
 *
 * This function 'normalizes' a string so valid q-grams can  be made of it:
 * All characters are transformed to uppercase, and all characters
 * which are not letters or digits are stripped to a single space.
 *
 * qgramnormalize("Hallo, allemaal!").print(); --> "HALLO ALLEMAAL"
 * qgramnormalize(" '' t ' est").print(); --> [ "T EST" ]
 *
 *****************************/
static str
CMDqgramnormalize(str *res, str *Input)
{
	char *input = *Input;
	int i, j = 0;
	char c, last = ' ';

	RETURN_NIL_IF(strNil(input), TYPE_str);
	*res = (str) GDKmalloc(sizeof(char) * (strlen(input) + 1));	/* normalized strings are never longer than original */
	if (*res == NULL)
		throw(MAL, "txtsim.qgramnormalize", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; input[i]; i++) {
		c = toupper(input[i]);
		if (!(('A' <= c && c <= 'Z') || isdigit((unsigned char) c)))
			c = ' ';
		if (c != ' ' || last != ' ') {
			(*res)[j++] = c;
		}
		last = c;
	}
	(*res)[j] = 0;
	/* strip final whitespace */
	while (j > 0 && (*res)[--j] == ' ')
		(*res)[j] = 0;

	return MAL_SUCCEED;
}

/* =========================================================================
 * FSTRCMP FUNCTION
 * Source:
 * http://search.cpan.org/src/MLEHMANN/String-Similarity-1/fstrcmp.c
 * =========================================================================
 */

/*
 * Data on one input string being compared.
 */
struct string_data {
	/* The string to be compared. */
	const char *data;

	/* The length of the string to be compared. */
	int data_length;

	/* The number of characters inserted or deleted. */
	int edit_count;
};

struct partition {
	/* Midpoints of this partition.  */
	int xmid, ymid;

	/* Nonzero if low half will be analyzed minimally.  */
	int lo_minimal;

	/* Likewise for high half.  */
	int hi_minimal;
};

/* NAME
	diag - find diagonal path

   SYNOPSIS
	int diag(int xoff, int xlim, int yoff, int ylim, int minimal,
		struct partition *part);

   DESCRIPTION
	Find the midpoint of the shortest edit script for a specified
	portion of the two strings.

	Scan from the beginnings of the strings, and simultaneously from
	the ends, doing a breadth-first search through the space of
	edit-sequence.  When the two searches meet, we have found the
	midpoint of the shortest edit sequence.

	If MINIMAL is nonzero, find the minimal edit script regardless
	of expense.  Otherwise, if the search is too expensive, use
	heuristics to stop the search and report a suboptimal answer.

   RETURNS
	Set PART->(XMID,YMID) to the midpoint (XMID,YMID).  The diagonal
	number XMID - YMID equals the number of inserted characters
	minus the number of deleted characters (counting only characters
	before the midpoint).  Return the approximate edit cost; this is
	the total number of characters inserted or deleted (counting
	only characters before the midpoint), unless a heuristic is used
	to terminate the search prematurely.

	Set PART->LEFT_MINIMAL to nonzero iff the minimal edit script
	for the left half of the partition is known; similarly for
	PART->RIGHT_MINIMAL.

   CAVEAT
	This function assumes that the first characters of the specified
	portions of the two strings do not match, and likewise that the
	last characters do not match.  The caller must trim matching
	characters from the beginning and end of the portions it is
	going to specify.

	If we return the "wrong" partitions, the worst this can do is
	cause suboptimal diff output.  It cannot cause incorrect diff
	output.  */

static inline int
diag(int xoff, int xlim, int yoff, int ylim, int minimal, struct partition *part, int too_expensive, struct string_data *string, int *fdiag, int *bdiag)
{
	int *const fd = fdiag;	/* Give the compiler a chance. */
	int *const bd = bdiag;	/* Additional help for the compiler. */
	const char *const xv = string[0].data;	/* Still more help for the compiler. */
	const char *const yv = string[1].data;	/* And more and more . . . */
	const int dmin = xoff - ylim;	/* Minimum valid diagonal. */
	const int dmax = xlim - yoff;	/* Maximum valid diagonal. */
	const int fmid = xoff - yoff;	/* Center diagonal of top-down search. */
	const int bmid = xlim - ylim;	/* Center diagonal of bottom-up search. */
	int fmin = fmid;
	int fmax = fmid;	/* Limits of top-down search. */
	int bmin = bmid;
	int bmax = bmid;	/* Limits of bottom-up search. */
	int c;			/* Cost. */
	int odd = (fmid - bmid) & 1;

	/*
	 * True if southeast corner is on an odd diagonal with respect
	 * to the northwest.
	 */
	fd[fmid] = xoff;
	bd[bmid] = xlim;
	for (c = 1;; ++c) {
		int d;		/* Active diagonal. */

		/* Extend the top-down search by an edit step in each diagonal. */
		if (fmin > dmin)
			fd[--fmin - 1] = -1;
		else
			++fmin;
		if (fmax < dmax)
			fd[++fmax + 1] = -1;
		else
			--fmax;
		for (d = fmax; d >= fmin; d -= 2) {
			int x;
			int y;
			int tlo;
			int thi;

			tlo = fd[d - 1], thi = fd[d + 1];

			if (tlo >= thi)
				x = tlo + 1;
			else
				x = thi;
			y = x - d;
			while (x < xlim && y < ylim && xv[x] == yv[y]) {
				++x;
				++y;
			}
			fd[d] = x;
			if (odd && bmin <= d && d <= bmax && bd[d] <= x) {
				part->xmid = x;
				part->ymid = y;
				part->lo_minimal = part->hi_minimal = 1;
				return 2 * c - 1;
			}
		}
		/* Similarly extend the bottom-up search.  */
		if (bmin > dmin)
			bd[--bmin - 1] = INT_MAX;
		else
			++bmin;
		if (bmax < dmax)
			bd[++bmax + 1] = INT_MAX;
		else
			--bmax;
		for (d = bmax; d >= bmin; d -= 2) {
			int x;
			int y;
			int tlo;
			int thi;

			tlo = bd[d - 1], thi = bd[d + 1];
			if (tlo < thi)
				x = tlo;
			else
				x = thi - 1;
			y = x - d;
			while (x > xoff && y > yoff && xv[x - 1] == yv[y - 1]) {
				--x;
				--y;
			}
			bd[d] = x;
			if (!odd && fmin <= d && d <= fmax && x <= fd[d]) {
				part->xmid = x;
				part->ymid = y;
				part->lo_minimal = part->hi_minimal = 1;
				return 2 * c;
			}
		}

		if (minimal)
			continue;

		/* Heuristic: if we've gone well beyond the call of duty, give up
		   and report halfway between our best results so far.  */
		if (c >= too_expensive) {
			int fxybest;
			int fxbest;
			int bxybest;
			int bxbest;

			/* Pacify `gcc -Wall'. */
			fxbest = 0;
			bxbest = 0;

			/* Find forward diagonal that maximizes X + Y.  */
			fxybest = -1;
			for (d = fmax; d >= fmin; d -= 2) {
				int x;
				int y;

				x = fd[d] < xlim ? fd[d] : xlim;
				y = x - d;

				if (ylim < y) {
					x = ylim + d;
					y = ylim;
				}
				if (fxybest < x + y) {
					fxybest = x + y;
					fxbest = x;
				}
			}
			/* Find backward diagonal that minimizes X + Y.  */
			bxybest = INT_MAX;
			for (d = bmax; d >= bmin; d -= 2) {
				int x;
				int y;

				x = xoff > bd[d] ? xoff : bd[d];
				y = x - d;

				if (y < yoff) {
					x = yoff + d;
					y = yoff;
				}
				if (x + y < bxybest) {
					bxybest = x + y;
					bxbest = x;
				}
			}
			/* Use the better of the two diagonals.  */
			if ((xlim + ylim) - bxybest < fxybest - (xoff + yoff)) {
				part->xmid = fxbest;
				part->ymid = fxybest - fxbest;
				part->lo_minimal = 1;
				part->hi_minimal = 0;
			} else {
				part->xmid = bxbest;
				part->ymid = bxybest - bxbest;
				part->lo_minimal = 0;
				part->hi_minimal = 1;
			}
			return 2 * c - 1;
		}
	}
}


/* NAME
	compareseq - find edit sequence

   SYNOPSIS
	void compareseq(int xoff, int xlim, int yoff, int ylim, int minimal);

   DESCRIPTION
	Compare in detail contiguous subsequences of the two strings
	which are known, as a whole, to match each other.

	The subsequence of string 0 is [XOFF, XLIM) and likewise for
	string 1.

	Note that XLIM, YLIM are exclusive bounds.  All character
	numbers are origin-0.

	If MINIMAL is nonzero, find a minimal difference no matter how
	expensive it is.  */

static inline void
compareseq(int xoff, int xlim, int yoff, int ylim, int minimal, int max_edits, int too_expensive, struct string_data *string, int *fdiag, int *bdiag) /* compareseq stops when edits > max_edits */
{
	const char *const xv = string[0].data;	/* Help the compiler.  */
	const char *const yv = string[1].data;

	if (string[1].edit_count + string[0].edit_count > max_edits)
		return;

	/* Slide down the bottom initial diagonal. */
	while (xoff < xlim && yoff < ylim && xv[xoff] == yv[yoff]) {
		++xoff;
		++yoff;
	}

	/* Slide up the top initial diagonal. */
	while (xlim > xoff && ylim > yoff && xv[xlim - 1] == yv[ylim - 1]) {
		--xlim;
		--ylim;
	}

	/* Handle simple cases. */
	if (xoff == xlim) {
		while (yoff < ylim) {
			++string[1].edit_count;
			++yoff;
		}
	} else if (yoff == ylim) {
		while (xoff < xlim) {
			++string[0].edit_count;
			++xoff;
		}
	} else {
		int c;
		struct partition part;

		/* Find a point of correspondence in the middle of the strings.  */
		c = diag(xoff, xlim, yoff, ylim, minimal, &part, too_expensive, string, fdiag, bdiag);
		if (c == 1) {
			/* The two subsequences differ by a single insert or delete;
			   record it and we are done.  */
			if (part.xmid - part.ymid < xoff - yoff)
				++string[1].edit_count;
			else
				++string[0].edit_count;
		} else {
			/* Use the partitions to split this problem into subproblems.  */
			compareseq(xoff, part.xmid, yoff, part.ymid, part.lo_minimal, max_edits, too_expensive, string, fdiag, bdiag);
			compareseq(part.xmid, xlim, part.ymid, ylim, part.hi_minimal, max_edits, too_expensive, string, fdiag, bdiag);
		}
	}
}

/* NAME
	fstrcmp - fuzzy string compare

   SYNOPSIS
	double fstrcmp(const char *s1, int l1, const char *s2, int l2, double);

   DESCRIPTION
	The fstrcmp function may be used to compare two string for
	similarity.  It is very useful in reducing "cascade" or
	"secondary" errors in compilers or other situations where
	symbol tables occur.

   RETURNS
	double; 0 if the strings are entirly dissimilar, 1 if the
	strings are identical, and a number in between if they are
	similar.  */

#define INITIAL_INT_BUFFER_LENGTH 2048

#define CHECK_INT_BUFFER_LENGTH(BUFFER, BUFFER_LEN, NEXT_LEN, OP) \
	do { \
		if ((NEXT_LEN) > *BUFFER_LEN) { \
			size_t newlen = (((NEXT_LEN) + 1023) & ~1023); /* align to a multiple of 1024 bytes */ \
			int *newbuf = GDKmalloc(newlen); \
			if (!newbuf) \
				throw(MAL, OP, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
			GDKfree(*BUFFER); \
			*BUFFER = newbuf; \
			*BUFFER_LEN = newlen; \
		} \
	} while (0)

static str
fstrcmp_impl_internal(dbl *ret, int **fdiag_buf, size_t *fdiag_buflen, str string1, str string2, dbl minimum)
{
	int i, max_edits, *fdiag, *bdiag, too_expensive = 1;
	size_t fdiag_len;
	struct string_data string[2];

	/* set the info for each string.  */
	string[0].data = string1;
	string[0].data_length = (int) strlen(string1); /* 64bit: assume string not too long */
	string[1].data = string2;
	string[1].data_length = (int) strlen(string2); /* 64bit: assume string not too long */

	/* short-circuit obvious comparisons */
	if (string[0].data_length == 0 && string[1].data_length == 0) {
		*ret = 1.0;
		return MAL_SUCCEED;
	}
	if (string[0].data_length == 0 || string[1].data_length == 0) {
		*ret = 0.0;
		return MAL_SUCCEED;
	}

	/* Set TOO_EXPENSIVE to be approximate square root of input size,
	   bounded below by 256.  */
	for (i = string[0].data_length + string[1].data_length; i != 0; i >>= 2)
		too_expensive <<= 1;
	if (too_expensive < 256)
		too_expensive = 256;

	/* Because fstrcmp is typically called multiple times, while scanning
	   symbol tables, etc, attempt to minimize the number of memory
	   allocations performed.  Thus, we use a static buffer for the
	   diagonal vectors, and never free them.  */
	fdiag_len = string[0].data_length + string[1].data_length + 3;
	CHECK_INT_BUFFER_LENGTH(fdiag_buf, fdiag_buflen, fdiag_len, "txtsim.similarity");
	fdiag = *fdiag_buf + string[1].data_length + 1;
	bdiag = fdiag + fdiag_len;

	max_edits = 1 + (int) ((string[0].data_length + string[1].data_length) * (1. - minimum));

	/* Now do the main comparison algorithm */
	string[0].edit_count = 0;
	string[1].edit_count = 0;
	compareseq(0, string[0].data_length, 0, string[1].data_length, 0, max_edits, too_expensive, string, fdiag, bdiag);

	/* The result is
	   ((number of chars in common) / (average length of the strings)).
	   This is admittedly biased towards finding that the strings are
	   similar, however it does produce meaningful results.  */
	*ret = ((double)
		(string[0].data_length + string[1].data_length - string[1].edit_count - string[0].edit_count)
		/ (string[0].data_length + string[1].data_length));
	return MAL_SUCCEED;
}

static str
fstrcmp_impl(dbl *ret, str *string1, str *string2, dbl *minimum)
{
	str s1 = *string1, s2 = *string2;
	dbl min = *minimum;

	if (strNil(s1) || strNil(s2) || is_dbl_nil(min)) {
		*ret = dbl_nil;
		return MAL_SUCCEED;
	} else {
		str msg = MAL_SUCCEED;
		int *fdiag_buf = NULL;
		size_t fdiag_buflen = INITIAL_INT_BUFFER_LENGTH;

		if (!(fdiag_buf = GDKmalloc(fdiag_buflen)))
			throw(MAL, "txtsim.similarity", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		msg = fstrcmp_impl_internal(ret, &fdiag_buf, &fdiag_buflen, s1, s1, min);
		GDKfree(fdiag_buf);
		return msg;
	}
}

static str
fstrcmp0_impl(dbl *ret, str *string1, str *string2)
{
	str s1 = *string1, s2 = *string2;

	if (strNil(s1) || strNil(s2)) {
		*ret = dbl_nil;
		return MAL_SUCCEED;
	} else {
		str msg = MAL_SUCCEED;
		int *fdiag_buf = NULL;
		size_t fdiag_buflen = INITIAL_INT_BUFFER_LENGTH;

		if (!(fdiag_buf = GDKmalloc(fdiag_buflen)))
			throw(MAL, "txtsim.similarity", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		msg = fstrcmp_impl_internal(ret, &fdiag_buf, &fdiag_buflen, s1, s2, 0.0);
		GDKfree(fdiag_buf);
		return msg;
	}
}

static str
fstrcmp0_impl_bulk(bat *res, bat *strings1, bat *strings2)
{
	BATiter lefti, righti;
	BAT *bn = NULL, *left = NULL, *right = NULL;
	BUN q = 0;
	size_t fdiag_buflen = INITIAL_INT_BUFFER_LENGTH;
	str x, y, msg = MAL_SUCCEED;
	bool nils = false;
	dbl *restrict vals;
	int *fdiag_buf = GDKmalloc(fdiag_buflen);

	if (!fdiag_buf) {
		msg = createException(MAL, "txtsim.similarity", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	if (!(left = BATdescriptor(*strings1)) || !(right = BATdescriptor(*strings2))) {
		msg = createException(MAL, "txtsim.similarity", SQLSTATE(HY005) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	q = BATcount(left);
	if (!(bn = COLnew(left->hseqbase, TYPE_dbl, q, TRANSIENT))) {
		msg = createException(MAL, "txtsim.similarity", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	lefti = bat_iterator(left);
	righti = bat_iterator(right);
	vals = Tloc(bn, 0);
	for (BUN i = 0; i < q && !msg; i++) {
		x = (str) BUNtvar(lefti, i);
		y = (str) BUNtvar(righti, i);

		if (strNil(x) || strNil(y)) {
			vals[i] = dbl_nil;
			nils = true;
		} else {
			msg = fstrcmp_impl_internal(&vals[i], &fdiag_buf, &fdiag_buflen, x, y, 0.0);
		}
	}

bailout:
	GDKfree(fdiag_buf);
	if (bn && !msg) {
		BATsetcount(bn, q);
		bn->tnil = nils;
		bn->tnonil = !nils;
		bn->tkey = BATcount(bn) <= 1;
		bn->tsorted = BATcount(bn) <= 1;
		bn->trevsorted = BATcount(bn) <= 1;
		bn->theap->dirty = true;
		BBPkeepref(*res = bn->batCacheid);
	} else if (bn)
		BBPreclaim(bn);
	if (left)
		BBPunfix(left->batCacheid);
	if (right)
		BBPunfix(right->batCacheid);
	return msg;
}


/* ============ Q-GRAM SELF JOIN ============== */

static str
CMDqgramselfjoin(bat *res1, bat *res2, bat *qid, bat *bid, bat *pid, bat *lid, flt *c, int *k)
{
	BAT *qgram, *id, *pos, *len;
	BUN n;
	BUN i, j;
	BAT *bn, *bn2;
	oid *qbuf;
	int *ibuf;
	int *pbuf;
	int *lbuf;
	str msg = MAL_SUCCEED;

	qgram = BATdescriptor(*qid);
	id = BATdescriptor(*bid);
	pos = BATdescriptor(*pid);
	len = BATdescriptor(*lid);
	if (qgram == NULL || id == NULL || pos == NULL || len == NULL) {
		if (qgram)
			BBPunfix(qgram->batCacheid);
		if (id)
			BBPunfix(id->batCacheid);
		if (pos)
			BBPunfix(pos->batCacheid);
		if (len)
			BBPunfix(len->batCacheid);
		throw(MAL, "txtsim.qgramselfjoin", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (qgram->ttype != TYPE_oid)
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": tail of BAT qgram must be oid");
	else if (id->ttype != TYPE_int)
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": tail of BAT id must be int");
	else if (pos->ttype != TYPE_int)
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": tail of BAT pos must be int");
	else if (len->ttype != TYPE_int)
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": tail of BAT len must be int");
	if (msg) {
		BBPunfix(qgram->batCacheid);
		BBPunfix(id->batCacheid);
		BBPunfix(pos->batCacheid);
		BBPunfix(len->batCacheid);
		return msg;
	}

	n = BATcount(qgram);
	qbuf = (oid *) Tloc(qgram, 0);
	ibuf = (int *) Tloc(id, 0);
	pbuf = (int *) Tloc(pos, 0);
	lbuf = (int *) Tloc(len, 0);

	/* if (BATcount(qgram)>1 && !BATtordered(qgram)) throw(MAL, "tstsim.qgramselfjoin", SEMANTIC_TYPE_MISMATCH); */

	if (!ALIGNsynced(qgram, id))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": qgram and id are not synced");

	else if (!ALIGNsynced(qgram, pos))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": qgram and pos are not synced");
	else if (!ALIGNsynced(qgram, len))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": qgram and len are not synced");

	else if (Tsize(qgram) != ATOMsize(qgram->ttype))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": qgram is not a true void bat");
	else if (Tsize(id) != ATOMsize(id->ttype))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": id is not a true void bat");

	else if (Tsize(pos) != ATOMsize(pos->ttype))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": pos is not a true void bat");
	else if (Tsize(len) != ATOMsize(len->ttype))
		msg = createException(MAL, "tstsim.qgramselfjoin",
			  SEMANTIC_TYPE_MISMATCH ": len is not a true void bat");
	if (msg) {
		BBPunfix(qgram->batCacheid);
		BBPunfix(id->batCacheid);
		BBPunfix(pos->batCacheid);
		BBPunfix(len->batCacheid);
		return msg;
	}

	bn = COLnew(0, TYPE_int, n, TRANSIENT);
	bn2 = COLnew(0, TYPE_int, n, TRANSIENT);
	if (bn == NULL || bn2 == NULL){
		BBPreclaim(bn);
		BBPreclaim(bn2);
		BBPunfix(qgram->batCacheid);
		BBPunfix(id->batCacheid);
		BBPunfix(pos->batCacheid);
		BBPunfix(len->batCacheid);
		throw(MAL, "txtsim.qgramselfjoin", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 0; i < n - 1; i++) {
		for (j = i + 1; (j < n && qbuf[j] == qbuf[i] && pbuf[j] <= (pbuf[i] + (*k + *c * MYMIN(lbuf[i], lbuf[j])))); j++) {
			if (ibuf[i] != ibuf[j] && abs(lbuf[i] - lbuf[j]) <= (*k + *c * MYMIN(lbuf[i], lbuf[j]))) {
				if (BUNappend(bn, ibuf + i, false) != GDK_SUCCEED ||
					BUNappend(bn2, ibuf + j, false) != GDK_SUCCEED) {
					BBPunfix(qgram->batCacheid);
					BBPunfix(id->batCacheid);
					BBPunfix(pos->batCacheid);
					BBPunfix(len->batCacheid);
					BBPreclaim(bn);
					BBPreclaim(bn2);
					throw(MAL, "txtsim.qgramselfjoin", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		}
	}

	BBPunfix(qgram->batCacheid);
	BBPunfix(id->batCacheid);
	BBPunfix(pos->batCacheid);
	BBPunfix(len->batCacheid);

	BBPkeepref(*res1 = bn->batCacheid);
	BBPkeepref(*res2 = bn2->batCacheid);

	return MAL_SUCCEED;
}

/* copy up to utf8len UTF-8 encoded characters from src to buf
 * stop early if buf (size given by bufsize) is too small, or if src runs out
 * return number of UTF-8 characters copied (excluding NUL)
 * close with NUL if enough space */
static size_t
utf8strncpy(char *buf, size_t bufsize, const char *src, size_t utf8len)
{
	size_t cnt = 0;

	while (utf8len != 0 && *src != 0 && bufsize != 0) {
		bufsize--;
		utf8len--;
		cnt++;
		if (((*buf++ = *src++) & 0x80) != 0) {
			while ((*src & 0xC0) == 0x80 && bufsize != 0) {
				*buf++ = *src++;
				bufsize--;
			}
		}
	}
	if (bufsize != 0)
		*buf = 0;
	return cnt;
}

static str
CMDstr2qgrams(bat *ret, str *val)
{
	BAT *bn;
	size_t i, len = strlen(*val) + 5;
	str s = GDKmalloc(len);
	char qgram[4 * 6 + 1];		/* 4 UTF-8 code points plus NULL byte */

	if (s == NULL)
		throw(MAL, "txtsim.str2qgram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	strcpy(s, "##");
	strcpy(s + 2, *val);
	strcpy(s + len - 3, "$$");
	bn = COLnew(0, TYPE_str, (BUN) strlen(*val), TRANSIENT);
	if (bn == NULL) {
		GDKfree(s);
		throw(MAL, "txtsim.str2qgram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	i = 0;
	while (s[i]) {
		if (utf8strncpy(qgram, sizeof(qgram), s + i, 4) < 4)
			break;
		if (BUNappend(bn, qgram, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			GDKfree(s);
			throw(MAL, "txtsim.str2qgram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if ((s[i++] & 0xC0) == 0xC0) {
			while ((s[i] & 0xC0) == 0x80)
				i++;
		}
	}
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(s);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func txtsim_init_funcs[] = {
 command("txtsim", "levenshtein", levenshtein_impl, false, "Calculates Levenshtein distance (edit distance) between two strings, variable operation costs (ins/del, replacement, transposition)", args(1,6, arg("",int),arg("s",str),arg("t",str),arg("insdel_cost",int),arg("replace_cost",int),arg("transpose_cost",int))),
 command("txtsim", "levenshtein", levenshteinbasic_impl, false, "Calculates Levenshtein distance (edit distance) between two strings", args(1,3, arg("",int),arg("s",str),arg("t",str))),
 command("txtsim", "editdistance", levenshteinbasic_impl, false, "Alias for Levenshtein(str,str)", args(1,3, arg("",int),arg("s",str),arg("t",str))),
 command("txtsim", "editdistance2", levenshteinbasic2_impl, false, "Calculates Levenshtein distance (edit distance) between two strings. Cost of transposition is 1 instead of 2", args(1,3, arg("",int),arg("s",str),arg("t",str))),
 command("txtsim", "similarity", fstrcmp_impl, false, "Normalized edit distance between two strings", args(1,4, arg("",dbl),arg("string1",str),arg("string2",str),arg("minimum",dbl))),
 command("txtsim", "similarity", fstrcmp0_impl, false, "Normalized edit distance between two strings", args(1,3, arg("",dbl),arg("string1",str),arg("string2",str))),
 command("battxtsim", "similarity", fstrcmp0_impl_bulk, false, "Normalized edit distance between two strings", args(1,3, batarg("",dbl),batarg("string1",str),batarg("string2",str))),
 command("txtsim", "soundex", soundex_impl, false, "Soundex function for phonetic matching", args(1,2, arg("",str),arg("name",str))),
 command("txtsim", "stringdiff", stringdiff_impl, false, "calculate the soundexed editdistance", args(1,3, arg("",int),arg("s1",str),arg("s2",str))),
 command("txtsim", "qgramnormalize", CMDqgramnormalize, false, "'Normalizes' strings (eg. toUpper and replaces non-alphanumerics with one space", args(1,2, arg("",str),arg("input",str))),
 command("txtsim", "qgramselfjoin", CMDqgramselfjoin, false, "QGram self-join on ordered(!) qgram tables and sub-ordered q-gram positions", args(2,8, batarg("",int),batarg("",int),batarg("qgram",oid),batarg("id",oid),batarg("pos",int),batarg("len",int),arg("c",flt),arg("k",int))),
 command("txtsim", "str2qgrams", CMDstr2qgrams, false, "Break the string into 4-grams", args(1,2, batarg("",str),arg("s",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_txtsim_mal)
{ mal_module("txtsim", NULL, txtsim_init_funcs); }
