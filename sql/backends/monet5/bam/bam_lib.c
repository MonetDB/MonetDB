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
		throw(MAL, "bam_flag", "Unknown flag name given: %s\n",
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
		throw(MAL, "reverse_seq", MAL_MALLOC_FAIL);

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
		throw(MAL, "reverse_qual", MAL_MALLOC_FAIL);

	backward = &result[len-1];
	for (i = 0; i < len; ++i)
		*backward-- = *forward++;
	result[len] = '\0';
	*ret = result;
	return MAL_SUCCEED;
}

str
seq_length(int * ret, str * cigar)
{
	int result = 0;
	str cigar_consumable = *cigar;

	if (cigar_consumable[0] == '\0' || 
			(cigar_consumable[0] == '*' && cigar_consumable[1] == '\0')) {
		*ret = -1;
		return MAL_SUCCEED;
	}
	while (cigar_consumable[0] != '\0') {
		int cnt;
		char op;
		int nr_chars_read;

		if (sscanf
			(cigar_consumable, "%d%c%n", &cnt, &op,
			 &nr_chars_read) != 2)
			throw(MAL, "seq_length",
				  "Error parsing CIGAR string '%s'\n", *cigar);
		if (op == 'M' || op == 'D' || op == 'N' || op == '='
			|| op == 'X')
			result += cnt;
		cigar_consumable += nr_chars_read;
	}
	*ret = result;
	return MAL_SUCCEED;
}

str
seq_char(str * ret, int * ref_pos, str * alg_seq, int * alg_pos, str * alg_cigar) 
{
	str cigar_consumable = *alg_cigar;
	int seq_pos = -1;
	int cur_ref_pos = *alg_pos - 1;
	
	if (cigar_consumable[0] == '\0' || 
			(cigar_consumable[0] == '*' && cigar_consumable[1] == '\0')) {
		*ret = GDKstrdup(str_nil);
		return MAL_SUCCEED;
	}
	while(TRUE) {
		int cnt;
		char op;
		int nr_chars_read;
		bit advance_ref_pos;
		bit advance_seq_pos;

		if (sscanf
			(cigar_consumable, "%d%c%n", &cnt, &op,
			 &nr_chars_read) != 2)
			throw(MAL, "seq_char",
				  "Error parsing CIGAR string '%s'\n", *alg_cigar);
		advance_ref_pos = (op == 'M' || op == 'D' || 
			op == 'N' || op == '=' || op == 'X');
		advance_seq_pos = (op == 'M' || op == 'I' || op == '='); // TODO: Find out which chars advance the seq pos
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
		cigar_consumable += nr_chars_read;
		if(cigar_consumable[0] == '\0') {
			seq_pos = -1;
			break;
		}
	}
	if(seq_pos < 0 || seq_pos >= (int)strlen(*alg_seq)) {
		*ret = GDKstrdup(str_nil);
		return MAL_SUCCEED;
	}
	if(((*ret) = GDKmalloc(2 * sizeof(char))) == NULL) {
		throw(MAL, "seq_char", MAL_MALLOC_FAIL);
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
	output->T->nil = output->T->nil || *cur_out == TPE##_nil; \
	output->T->nonil = output->T->nonil && *cur_out != TPE##_nil; \
}

#define update_props_str() { \
	output->tsorted = output->tsorted && \
		(c == 0 || strcmp(*cur_out, prev_out) >= 0); \
	output->trevsorted = output->trevsorted && \
		(c == 0 || strcmp(*cur_out, prev_out) <= 0); \
	output->T->nil = output->T->nil || *cur_out == str_nil; \
	output->T->nonil = output->T->nonil && *cur_out != str_nil; \
}

#define finish_props() { \
	BATsetcount(output, BATcount(input)); \
	BATseqbase(output, input->hseqbase); \
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
		throw(MAL, "bam_flag", "Unknown flag name given: %s\n",
			  *name);

	if ((input = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bam_flag_bat", RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	output = BATnew(TYPE_void, TYPE_bit, BATcount(input), TRANSIENT);
	if (output == NULL) {
		BBPreleaseref(input->batCacheid);
		throw(MAL, "bam_flag_bat", MAL_MALLOC_FAIL);
	}
	
	init_props();
	cur_in = (sht *) Tloc(input, BUNfirst(input));
	cur_out = (bit *) Tloc(output, BUNfirst(output));
	for(c = 0; c < BATcount(input); ++c) {
		*cur_out = kth_bit(*cur_in, k);
		update_props(bit);
		cur_in++;
		prev_out = *cur_out++;
	}
	finish_props();

	/* release input BAT-descriptor */
	BBPreleaseref(input->batCacheid);

	BBPkeepref((*ret = output->batCacheid));

	return MAL_SUCCEED;
}

/* Commented out, since string heap addition needs to be added
str
reverse_seq_bat(bat * ret, bat * bid)
{
	BAT *input, *output;
	str prev_out = 0;
	str *cur_in;
	str *cur_out;
	BUN c;
	str msg;

	assert(ret != NULL && bid != NULL);

	if ((input = BATdescriptor(*bid)) == NULL)
		throw(MAL, "reverse_seq_bat", RUNTIME_OBJECT_MISSING);
*/
	/* allocate result BAT */
/*	output = BATnew(TYPE_void, TYPE_str, BATcount(input), TRANSIENT);
	if (output == NULL) {
		throw(MAL, "reverse_seq_bat", MAL_MALLOC_FAIL);
	}

	init_props();
	cur_in = (str *) Tloc(input, BUNfirst(input));
	cur_out = (str *) Tloc(output, BUNfirst(output));
	for(c = 0; c < BATcount(input); ++c) {
		if ((msg = reverse_seq(cur_out, cur_in)) != MAL_SUCCEED) {
			BBPreleaseref(output->batCacheid);
			return msg;
		}
		update_props_str();
		cur_in++;
		prev_out = *cur_out++;
	}
	finish_props();
*/
	/* release input BAT-descriptor */
/*	BBPreleaseref(input->batCacheid);

	BBPkeepref((*ret = output->batCacheid));

	return MAL_SUCCEED;
}*/

str
reverse_seq_bat(bat * ret, bat * bid)
{
	BAT *seqs, *result;
	BATiter li;
	BUN p = 0, q = 0;

	assert(ret != NULL && bid != NULL);

	if ((seqs = BATdescriptor(*bid)) == NULL)
		throw(MAL, "reverse_seq_bat", RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	result = BATnew(TYPE_void, TYPE_str, BATcount(seqs), TRANSIENT);
	if (result == NULL) {
		BBPreleaseref(seqs->batCacheid);
		throw(MAL, "reverse_seq_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, seqs->hseqbase);

	li = bat_iterator(seqs);

	BATloop(seqs, p, q) {
		str t = (str) BUNtail(li, p);
		str r, msg;

		if ((msg = reverse_seq(&r, &t)) != MAL_SUCCEED) {
			BBPreleaseref(seqs->batCacheid);
			BBPreleaseref(result->batCacheid);
			return msg;
		}
		BUNappend(result, (ptr) r, FALSE);
		GDKfree(r);
	}

	/* release input BAT-descriptor */
	BBPreleaseref(seqs->batCacheid);

	BBPkeepref((*ret = result->batCacheid));

	return MAL_SUCCEED;
}

str
reverse_qual_bat(bat * ret, bat * bid)
{
	BAT *quals, *result;
	BATiter li;
	BUN p = 0, q = 0;

	assert(ret != NULL && bid != NULL);

	if ((quals = BATdescriptor(*bid)) == NULL)
		throw(MAL, "reverse_qual_bat", RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	result = BATnew(TYPE_void, TYPE_str, BATcount(quals), TRANSIENT);
	if (result == NULL) {
		BBPreleaseref(quals->batCacheid);
		throw(MAL, "reverse_qual_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, quals->hseqbase);

	li = bat_iterator(quals);

	BATloop(quals, p, q) {
		str t = (str) BUNtail(li, p);
		str r, msg;

		if ((msg = reverse_qual(&r, &t)) != MAL_SUCCEED) {
			BBPreleaseref(quals->batCacheid);
			BBPreleaseref(result->batCacheid);
			return msg;
		}
		BUNappend(result, (ptr) r, FALSE);
		GDKfree(r);
	}

	/* release input BAT-descriptor */
	BBPreleaseref(quals->batCacheid);

	BBPkeepref((*ret = result->batCacheid));

	return MAL_SUCCEED;
}

/*str Does not work, probably has to do with we not being able to traverse the
      CIGAR string input BAT like this
seq_length_bat(bat * ret, bat * bid)
{
	BAT *input, *output;
	sht prev_out = 0;
	str *cur_in;
	int *cur_out;
	BUN c;
	str msg;

	assert(ret != NULL && bid != NULL);

	if ((input = BATdescriptor(*bid)) == NULL)
		throw(MAL, "seq_length_bat", RUNTIME_OBJECT_MISSING);
*/
	/* allocate result BAT */
/*	output = BATnew(TYPE_void, TYPE_int, BATcount(input), TRANSIENT);
	if (output == NULL) {
		throw(MAL, "seq_length_bat", MAL_MALLOC_FAIL);
	}

	init_props();
	cur_in = (str *) Tloc(input, BUNfirst(input));
	cur_out = (int *) Tloc(output, BUNfirst(output));
	for(c = 0; c < BATcount(input); ++c) {
		if ((msg = seq_length(cur_out, cur_in)) != MAL_SUCCEED) {
			BBPreleaseref(output->batCacheid);
			return msg;
		}
		update_props(int);
		cur_in++;
		prev_out = *cur_out++;
	}
	finish_props();
*/
	/* release input BAT-descriptor */
/*	BBPreleaseref(input->batCacheid);

	BBPkeepref((*ret = output->batCacheid));

	return MAL_SUCCEED;
}*/

str
seq_length_bat(bat * ret, bat * bid)
{
	BAT *cigars, *result;
	BATiter li;
	BUN p = 0, q = 0;

	assert(ret != NULL && bid != NULL);

	if ((cigars = BATdescriptor(*bid)) == NULL)
		throw(MAL, "seq_length_bat", RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	result = BATnew(TYPE_void, TYPE_int, BATcount(cigars), TRANSIENT);
	if (result == NULL) {
		BBPreleaseref(cigars->batCacheid);
		throw(MAL, "seq_length_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, cigars->hseqbase);

	li = bat_iterator(cigars);

	BATloop(cigars, p, q) {
		str t = (str) BUNtail(li, p);
		str msg;
		int r;

		if ((msg = seq_length(&r, &t)) != MAL_SUCCEED) {
			BBPreleaseref(cigars->batCacheid);
			BBPreleaseref(result->batCacheid);
			return msg;
		}
		BUNappend(result, (ptr) &r, FALSE);
	}

	/* release input BAT-descriptor */
	BBPreleaseref(cigars->batCacheid);

	BBPkeepref((*ret = result->batCacheid));

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
		msg = createException(MAL, "seq_char_bat", RUNTIME_OBJECT_MISSING);
		goto cleanup;
	}

	if(BATcount(seqs) != BATcount(poss) || BATcount(seqs) != BATcount(cigars)) {
		msg = createException(MAL, "seq_char_bat", 
			"Misalignment in input BATs: "BUNFMT"/"BUNFMT"/"BUNFMT, 
			BATcount(poss), BATcount(seqs), BATcount(cigars));
		goto cleanup;
	}
	
	/* allocate result BAT */
	result = BATnew(TYPE_void, TYPE_str, BATcount(cigars), TRANSIENT);
	if (result == NULL) {
		msg = createException(MAL, "seq_char_bat", MAL_MALLOC_FAIL);
		goto cleanup;
	}
	BATseqbase(result, seqs->hseqbase);

	seq = BUNfirst(seqs);
	pos = BUNfirst(poss);
	cigar = BUNfirst(cigars);
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
		BUNappend(result, (ptr) r, FALSE);
		++seq;
		++pos;
		++cigar;
	}
	
cleanup:
	/* release input BAT-descriptors */
	if(seqs) BBPreleaseref(seqs->batCacheid);
	if(poss) BBPreleaseref(poss->batCacheid);
	if(cigars) BBPreleaseref(cigars->batCacheid);

	if(result) BBPkeepref((*ret = result->batCacheid));

	return msg;
}
