/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The functions in this file represent the BAM SQL library, that make
 * querying BAM data easier.
 */

#include "monetdb_config.h"
#include "bam_lib.h"

/* Map string to integer by adding all odd and subtracting all even numbers
 * For the strings that we want to detect, this gives the following:
 * mult_segm -> 71
 * prop_alig -> 101
 * segm_unma -> 84
 * next_unma -> 89
 * segm_reve -> 73
 * next_reve -> 78
 * firs_segm -> 83
 * last_segm -> 97
 * seco_alig -> 106
 * qual_cont -> 98
 * opti_dupl -> 118
 * supp_alig -> 102
 *
 * These integers fit in an array of 48 positions
 * Define two arrays:
 * - One that maps int position to the bit position that we
 *   are looking for
 * - One that maps bit position to string, so we can check if
 *   the input really was one of the valid strings (since many)
 *   strings will map to the same integer
 */

sht intbit_map[] = {
	 0, -1,  4, -1, -1, -1, -1,  5, -1, -1,
	-1, -1,  6,  2, -1, -1, -1, -1,  3, -1,
	-1, -1, -1, -1, -1, -1,  7,  9, -1, -1,
	 1, 11, -1, -1, -1,  8, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, 10
};

str posstr_map[] = {
	"mult_segm",
	"prop_alig",
	"segm_unma",
	"next_unma",
	"segm_reve",
	"next_reve",
	"firs_segm",
	"last_segm",
	"seco_alig",
	"qual_cont",
	"opti_dupl",
	"supp_alig"
};

static sht 
flag_str2sht(str flag_str) {
	int i = 0;
	int mult = 1;
	int strnum = -71;
	char c;
	sht k = -1;
	while((c = flag_str[i++]) != '\0') {
		strnum += mult * (int)c;
		mult *= -1;
	}
	if(strnum < 0 || strnum > 47 || (k = intbit_map[strnum]) < 0 ||
			strcmp(posstr_map[k], flag_str) != 0) {
		k = -1;
	}
	return k;
}

#define kth_bit(flag, k) ((flag & (1 << k)) == (1 << k))

str
bam_flag(bit * ret, sht * flag, str * name)
{
	sht k;
	k = flag_str2sht(*name);

	if (k < 0)
		throw(MAL, "bam_flag", "SQLSTATE BA000 !""Unknown flag name given: %s\n",
			  *name);
	*ret = kth_bit(*flag, k);
	return MAL_SUCCEED;
}

char reverse_seq_map[] = {
	'T', //A
	'V', //B
	'G', //C
	'H', //D
	 0 , //E
	 0 , //F
	'C', //G
	'D', //H
	 0 , //I
	 0 , //J
	'M', //K
	 0 , //L
	'K', //M
	'N', //N
	 0 , //O
	 0 , //P
	 0 , //Q
	'Y', //R
	'S', //S
	'A', //T
	 0 , //U
	'B', //V
	'W', //W
	 0 , //X
	'R'  //Y
};

str
reverse_seq(str * ret, str * seq)
{
	str result;
	unsigned int i;
	unsigned int len = strlen(*seq);
	sht map_index;
	str forward = *seq;
	str backward;

	result = GDKmalloc((len + 1) * sizeof(char));
	if (result == NULL)
		throw(MAL, "reverse_seq", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);

	backward = &result[len-1];
	for (i = 0; i < len; ++i) {
		map_index = (sht)(*forward++ - 'A');
		if(map_index < 0 || map_index > 24 ||
				(*backward = reverse_seq_map[map_index]) == 0) {
			*backward = '?';
		}
		--backward;
	}
	result[len] = '\0';
	*ret = result;
	return MAL_SUCCEED;
}

str
reverse_qual(str * ret, str * qual)
{
	str result;
	unsigned int i;
	unsigned int len = strlen(*qual);
	str forward = *qual;
	str backward;

	result = GDKmalloc((len + 1) * sizeof(char));
	if (result == NULL)
		throw(MAL, "reverse_qual", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);

	backward = &result[len-1];
	for (i = 0; i < len; ++i)
		*backward-- = *forward++;
	result[len] = '\0';
	*ret = result;
	return MAL_SUCCEED;
}

#define next_cigar_op(fn) { \
	str tmp; \
	cnt = strtol(s, &tmp, 10); \
	if(cnt <= 0 || s == tmp || *s == '\0') { \
		throw(MAL, fn, "SQLSTATE BA000 !""Could not parse CIGAR string"); \
	} \
	s = tmp; \
	op = *s++; \
}

str
seq_length(int * ret, str * cigar)
{
	int result = 0;
	str s = *cigar;
	long int cnt;
	char op;

	if (*s == '\0' || (*s == '*' && *(s+1) == '\0')) {
		*ret = -1;
		return MAL_SUCCEED;
	}
	while (*s != '\0') {
		next_cigar_op("seq_length");
		if (op == 'M' || op == 'D' || op == 'N' || op == '='
			|| op == 'X')
			result += cnt;
	}
	*ret = result;
	return MAL_SUCCEED;
}

str
seq_char(str * ret, int * ref_pos, str * alg_seq, int * alg_pos, str * alg_cigar) 
{
	str s = *alg_cigar;
	int seq_pos = -1;
	int cur_ref_pos = *alg_pos - 1;

	long int cnt;
	char op;
	
	if (*s == '\0' || (*s == '*' && *(s+1) == '\0')) {
		*ret = GDKstrdup(str_nil);
		return MAL_SUCCEED;
	}
	while(TRUE) {
		bit advance_ref_pos;
		bit advance_seq_pos;

		next_cigar_op("seq_char");
		
		advance_ref_pos = (op == 'M' || op == 'D' || 
			op == 'N' || op == '=' || op == 'X');
		advance_seq_pos = (op == 'M' || op == 'I' || op == '='); // TODO: Find out which chars advance the seq pos exactly
		if(advance_seq_pos) {
			seq_pos += cnt;
		}
		if (advance_ref_pos) {
			cur_ref_pos += cnt;
			if(cur_ref_pos >= *ref_pos) {
				if(!advance_seq_pos) {
					seq_pos = -1;
				} else {
					seq_pos -= (cur_ref_pos - *ref_pos);
				}
				break;
			}
		}
		if(*s == '\0') {
			seq_pos = -1;
			break;
		}
	}
	if(seq_pos < 0 || seq_pos >= (int)strlen(*alg_seq)) {
		*ret = GDKstrdup(str_nil);
		return MAL_SUCCEED;
	}
	if(((*ret) = GDKmalloc(2 * sizeof(char))) == NULL) {
		throw(MAL, "seq_char", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
	}
	(*ret)[0] = (*alg_seq)[seq_pos];
	(*ret)[1] = '\0';
	return MAL_SUCCEED;
}

#define init_props() { \
	output->tsorted = TRUE; \
	output->trevsorted = TRUE; \
}

#define update_props(TPE) { \
	output->tsorted = output->tsorted && \
		(c == 0 || *cur_out >= prev_out); \
	output->trevsorted = output->trevsorted && \
		(c == 0 || *cur_out <= prev_out); \
	output->tnil = output->tnil || *cur_out == TPE##_nil; \
	output->tnonil = output->tnonil && *cur_out != TPE##_nil; \
}

#define finish_props() { \
	BATsetcount(output, BATcount(input)); \
	output->tkey = FALSE; /* Tail values are not unique */ \
}

str
bam_flag_bat(bat * ret, bat * bid, str * name)
{
	BAT *input, *output;
	sht prev_out = 0;
	sht *cur_in;
	bit *cur_out;
	sht k;
	BUN c;

	assert(ret != NULL && bid != NULL && name != NULL);

	k = flag_str2sht(*name);
	if (k < 0)
		throw(MAL, "bam_flag", "SQLSTATE BA000 !""Unknown flag name given: %s\n",
			  *name);

	if ((input = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bam_flag_bat", "SQLSTATE HY002 !"RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	output = COLnew(input->hseqbase, TYPE_bit, BATcount(input), TRANSIENT);
	if (output == NULL) {
		BBPunfix(input->batCacheid);
		throw(MAL, "bam_flag_bat", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
	}
	
	init_props();
	cur_in = (sht *) Tloc(input, 0);
	cur_out = (bit *) Tloc(output, 0);
	for(c = 0; c < BATcount(input); ++c) {
		*cur_out = kth_bit(*cur_in, k);
		update_props(bit);
		cur_in++;
		prev_out = *cur_out++;
	}
	finish_props();

	/* release input BAT-descriptor */
	BBPunfix(input->batCacheid);

	BBPkeepref((*ret = output->batCacheid));

	return MAL_SUCCEED;
}

#define transform_strbat(transform_fn) {								\
	BAT *input, *output;												\
	BATiter li;															\
	BUN p = 0, q = 0;													\
																		\
	assert(ret != NULL && bid != NULL);									\
																		\
	if ((input = BATdescriptor(*bid)) == NULL)							\
		throw(MAL, "reverse_seq_bat", "SQLSTATE HY002 !"RUNTIME_OBJECT_MISSING);			\
																		\
	/* allocate result BAT */											\
	output = COLnew(input->hseqbase, TYPE_str, BATcount(input), TRANSIENT); \
	if (output == NULL) {												\
		BBPunfix(input->batCacheid);									\
		throw(MAL, "reverse_seq_bat", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);					\
	}																	\
																		\
	li = bat_iterator(input);											\
																		\
	BATloop(input, p, q) {												\
		str t = (str) BUNtail(li, p);									\
		str r, msg;														\
																		\
		if ((msg = transform_fn(&r, &t)) != MAL_SUCCEED) {				\
			BBPunfix(input->batCacheid);								\
			BBPunfix(output->batCacheid);								\
			return msg;													\
		}																\
		if (BUNappend(output, (ptr) r, FALSE) != GDK_SUCCEED) {			\
			BBPunfix(input->batCacheid);								\
			BBPreclaim(output);											\
			throw(MAL, "reverse_seq_bat", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);				\
		}																\
		GDKfree(r);														\
	}																	\
																		\
	/* release input BAT-descriptor */									\
	BBPunfix(input->batCacheid);										\
																		\
	BBPkeepref((*ret = output->batCacheid));							\
																		\
	return MAL_SUCCEED;													\
}

str
reverse_seq_bat(bat * ret, bat * bid)
{
	transform_strbat(reverse_seq);
}

str
reverse_qual_bat(bat * ret, bat * bid)
{
	transform_strbat(reverse_qual);
}

str
seq_length_bat(bat * ret, bat * bid)
{
	BAT *input, *output;
	sht prev_out = 0;
	str cur_in;
	int *cur_out;
	BATiter li;
	BUN c = 0, p = 0, q = 0;
	str msg;

	assert(ret != NULL && bid != NULL);

	if ((input = BATdescriptor(*bid)) == NULL)
		throw(MAL, "seq_length_bat", "SQLSTATE HY002 !"RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	output = COLnew(input->hseqbase, TYPE_int, BATcount(input), TRANSIENT);
	if (output == NULL) {
		throw(MAL, "seq_length_bat", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
	}

	init_props();
	li = bat_iterator(input);
	cur_out = (int *) Tloc(output, 0);
	BATloop(input, p, q) {
		cur_in = (str) BUNtail(li, p);
		if ((msg = seq_length(cur_out, &cur_in)) != MAL_SUCCEED) {
			BBPunfix(output->batCacheid);
			return msg;
		}
		update_props(int);
		++c;
		prev_out = *cur_out++;
	}
	finish_props();

	/* release input BAT-descriptor */
	BBPunfix(input->batCacheid);

	BBPkeepref((*ret = output->batCacheid));

	return MAL_SUCCEED;
}

str
seq_char_bat(bat * ret, int * ref_pos, bat * alg_seq, bat * alg_pos, bat * alg_cigar)
{
	BAT *seqs = NULL, *poss = NULL, *cigars = NULL, *result = NULL;
	BUN seq = 0, pos = 0, cigar = 0, seq_end = 0;
	BATiter seq_it, pos_it, cigar_it;

	str msg = MAL_SUCCEED;

	assert(ret != NULL && ref_pos != NULL && alg_seq != NULL && alg_pos != NULL && alg_cigar != NULL);

	if ((seqs = BATdescriptor(*alg_seq)) == NULL ||
	    (poss = BATdescriptor(*alg_pos)) == NULL ||
		(cigars = BATdescriptor(*alg_cigar)) == NULL) {
		msg = createException(MAL, "seq_char_bat", "SQLSTATE HY002 !"RUNTIME_OBJECT_MISSING);
		goto cleanup;
	}

	if(BATcount(seqs) != BATcount(poss) || BATcount(seqs) != BATcount(cigars)) {
		msg = createException(MAL, "seq_char_bat", 
			"SQLSTATE BA000 !""Misalignment in input BATs: "BUNFMT"/"BUNFMT"/"BUNFMT, 
			BATcount(poss), BATcount(seqs), BATcount(cigars));
		goto cleanup;
	}
	
	/* allocate result BAT */
	result = COLnew(seqs->hseqbase, TYPE_str, BATcount(cigars), TRANSIENT);
	if (result == NULL) {
		msg = createException(MAL, "seq_char_bat", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
		goto cleanup;
	}

	seq = 0;
	pos = 0;
	cigar = 0;
	seq_end = BUNlast(seqs);

	seq_it = bat_iterator(seqs);
	pos_it = bat_iterator(poss);
	cigar_it = bat_iterator(cigars);

	while(seq < seq_end) {
		str seq_val = (str) BUNtail(seq_it, seq);
		int * pos_val = (int *) BUNtail(pos_it, pos);
		str cigar_val = (str) BUNtail(cigar_it, cigar);
		str r;
		str msg;

		if ((msg = seq_char(&r, ref_pos, &seq_val, pos_val, &cigar_val)) != MAL_SUCCEED) {
			goto cleanup;
		}
		if (BUNappend(result, (ptr) r, FALSE) != GDK_SUCCEED) {
			msg = createException(MAL, "seq_char_bat", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
			goto cleanup;
		}
		GDKfree(r);
		++seq;
		++pos;
		++cigar;
	}
	
cleanup:
	/* release input BAT-descriptors */
	if(seqs) BBPunfix(seqs->batCacheid);
	if(poss) BBPunfix(poss->batCacheid);
	if(cigars) BBPunfix(cigars->batCacheid);

	if(result) BBPkeepref((*ret = result->batCacheid));

	return msg;
}
