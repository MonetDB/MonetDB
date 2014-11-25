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

#define flag_str2sht(flag_str)						\
	(strcmp(flag_str, "mult_segm") == 0 ? 0 :			\
	 (strcmp(flag_str, "prop_alig") == 0 ? 1 :			\
	  (strcmp(flag_str, "segm_unma") == 0 ? 2 :			\
	   (strcmp(flag_str, "next_unma") == 0 ? 3 :			\
		(strcmp(flag_str, "segm_reve") == 0 ? 4 :			\
		 (strcmp(flag_str, "next_reve") == 0 ? 5 :			\
		  (strcmp(flag_str, "firs_segm") == 0 ? 6 :			\
		   (strcmp(flag_str, "last_segm") == 0 ? 7 :		\
		(strcmp(flag_str, "seco_alig") == 0 ? 8 :		\
		 (strcmp(flag_str, "qual_cont") == 0 ? 9 :		\
		  (strcmp(flag_str, "opti_dupl") == 0 ? 10 :		\
		   (strcmp(flag_str, "supp_alig") == 0 ? 11 : -1))))))))))))

#define kth_bit(flag, k) ((flag & (1 << k)) == (1 << k))

str
bam_flag(bit * ret, sht * flag, str * name)
{
	sht k = flag_str2sht(*name);

	if (k < 0)
		throw(MAL, "bam_flag", "Unknown flag name given: %s\n",
			  *name);
	*ret = kth_bit(*flag, k);
	return MAL_SUCCEED;
}

// use a simple lookup table for these mappings
str
reverse_seq(str * ret, str * seq)
{
	str result;
	unsigned int i;
	unsigned int len = strlen(*seq);

	result = GDKmalloc((len + 1) * sizeof(char));
	if (result == NULL)
		throw(MAL, "reverse_seq", MAL_MALLOC_FAIL);
	for (i = 0; i < len; ++i) {
		switch ((*seq)[i]) {
		case 'A':
			result[len - i - 1] = 'T';
			break;
		case 'T':
			result[len - i - 1] = 'A';
			break;
		case 'C':
			result[len - i - 1] = 'G';
			break;
		case 'G':
			result[len - i - 1] = 'C';
			break;
		case 'R':
			result[len - i - 1] = 'Y';
			break;
		case 'Y':
			result[len - i - 1] = 'R';
			break;
		case 'S':
			result[len - i - 1] = 'S';
			break;
		case 'W':
			result[len - i - 1] = 'W';
			break;
		case 'K':
			result[len - i - 1] = 'M';
			break;
		case 'M':
			result[len - i - 1] = 'K';
			break;
		case 'H':
			result[len - i - 1] = 'D';
			break;
		case 'D':
			result[len - i - 1] = 'H';
			break;
		case 'V':
			result[len - i - 1] = 'B';
			break;
		case 'B':
			result[len - i - 1] = 'V';
			break;
		case 'N':
			result[len - i - 1] = 'N';
			break;
		default:
			GDKfree(result);
			throw(MAL, "reverse_seq",
				  "Invalid character found in sequence: '%c'\n",
				  (*seq)[i]);
		}
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

	result = GDKmalloc((len + 1) * sizeof(char));
	if (result == NULL)
		throw(MAL, "reverse_qual", MAL_MALLOC_FAIL);
	for (i = 0; i < len; ++i)
		result[len - i - 1] = (*qual)[i];
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
		advance_seq_pos = (op == 'M' || op == 'I'); // TODO: Find out which chars advance the seq pos
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




str
bam_flag_bat(bat * ret, bat * bid, str * name)
{
	BAT *flags, *result;
	BATiter li;
	BUN p = 0, q = 0;
	sht k;

	assert(ret != NULL && bid != NULL && name != NULL);

	k = flag_str2sht(*name);
	if (k < 0)
		throw(MAL, "bam_flag", "Unknown flag name given: %s\n",
			  *name);

	if ((flags = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bam_flag_bat", RUNTIME_OBJECT_MISSING);

	/* allocate result BAT */
	result = BATnew(TYPE_void, TYPE_bit, BATcount(flags), TRANSIENT);
	if (result == NULL) {
		BBPreleaseref(flags->batCacheid);
		throw(MAL, "bam_flag_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, flags->hseqbase);

	li = bat_iterator(flags);

	BATloop(flags, p, q) {
		sht t = *(sht *) BUNtail(li, p);
		bit r = kth_bit(t, k);

		BUNappend(result, (ptr) &r, FALSE);
	}

	/* release input BAT-descriptor */
	BBPreleaseref(flags->batCacheid);

	BBPkeepref((*ret = result->batCacheid));

	return MAL_SUCCEED;
}

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
			BBPreleaseref(result->batCacheid);
			BBPreleaseref(seqs->batCacheid);
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
			BBPreleaseref(result->batCacheid);
			BBPreleaseref(cigars->batCacheid);
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
	BAT *seqs = NULL, *poss = NULL, *refs = NULL, *cigars = NULL, *result = NULL;
	BUN ref= 0, seq = 0, pos = 0, cigar = 0, seq_end = 0;
	BATiter ref_it, seq_it, pos_it, cigar_it;

	assert(ret != NULL && ref_pos != NULL && alg_seq != NULL && alg_pos != NULL && alg_cigar != NULL);

	if ((seqs = BATdescriptor(*alg_seq)) == NULL ||
	    (poss = BATdescriptor(*alg_pos)) == NULL ||
	    (refs = BATdescriptor(*ref_pos)) == NULL ||
		(cigars = BATdescriptor(*alg_cigar)) == NULL) {
			if( seqs) BBPreleaseref(seqs->batCacheid);
			if( poss) BBPreleaseref(poss->batCacheid);
			if( refs) BBPreleaseref(refs->batCacheid);
			throw(MAL, "seq_char_bat", RUNTIME_OBJECT_MISSING);
	}

	if(BATcount(seqs) != BATcount(poss) || BATcount(seqs) != BATcount(cigars)) {
		BBPreleaseref(seqs->batCacheid);
		BBPreleaseref(poss->batCacheid);
		BBPreleaseref(refs->batCacheid);
		throw(MAL, "seq_char_bat", 
			"Misalignment in input BATs: "BUNFMT"/"BUNFMT"/"BUNFMT, 
			BATcount(poss), BATcount(seqs), BATcount(cigars));
	}

	/* allocate result BAT */
	result = BATnew(TYPE_void, TYPE_str, BATcount(cigars), TRANSIENT);
	if (result == NULL) {
		BBPreleaseref(seqs->batCacheid);
		BBPreleaseref(poss->batCacheid);
		BBPreleaseref(refs->batCacheid);
		throw(MAL, "seq_char_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, seqs->hseqbase);

	ref = BUNfirst(refs);
	seq = BUNfirst(seqs);
	pos = BUNfirst(poss);
	cigar = BUNfirst(cigars);
	seq_end = BUNlast(seqs);

	ref_it = bat_iterator(refs);
	seq_it = bat_iterator(seqs);
	pos_it = bat_iterator(poss);
	cigar_it = bat_iterator(cigars);

	while(seq < seq_end) {
		str seq_val = (str) BUNtail(seq_it, seq);
		int * ref_val = (int *) BUNtail(ref_it, ref);
		int * pos_val = (int *) BUNtail(pos_it, pos);
		str cigar_val = (str) BUNtail(cigar_it, cigar);
		str r;
		str msg;

		if ((msg = seq_char(&r, ref_val, &seq_val, pos_val, &cigar_val)) != MAL_SUCCEED) {
			BBPreleaseref(refs->batCacheid);
			BBPreleaseref(seqs->batCacheid);
			BBPreleaseref(poss->batCacheid);
			BBPreleaseref(cigars->batCacheid);
			BBPreleaseref(result->batCacheid);
			return msg;
		}
		BUNappend(result, (ptr) r, FALSE);
		++seq;
		++pos;
		++cigar;
	}

	/* release input BAT-descriptors */
	BBPreleaseref(refs->batCacheid);
	BBPreleaseref(seqs->batCacheid);
	BBPreleaseref(poss->batCacheid);
	BBPreleaseref(cigars->batCacheid);

	BBPkeepref((*ret = result->batCacheid));

	return MAL_SUCCEED;
}
