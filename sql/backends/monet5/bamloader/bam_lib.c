#include "monetdb_config.h"
#include "bam_lib.h"

#define flag_str2sht(flag_str) ( \
                                   (strcmp(flag_str, "mult_segm") == 0 ? 0 : \
                                   (strcmp(flag_str, "prop_alig") == 0 ? 1 : \
                                   (strcmp(flag_str, "segm_unma") == 0 ? 2 : \
                                   (strcmp(flag_str, "next_unma") == 0 ? 3 : \
                                   (strcmp(flag_str, "segm_reve") == 0 ? 4 : \
                                   (strcmp(flag_str, "next_reve") == 0 ? 5 : \
                                   (strcmp(flag_str, "firs_segm") == 0 ? 6 : \
                                   (strcmp(flag_str, "last_segm") == 0 ? 7 : \
                                   (strcmp(flag_str, "seco_alig") == 0 ? 8 : \
                                   (strcmp(flag_str, "qual_cont") == 0 ? 9 : \
                                   (strcmp(flag_str, "opti_dupl") == 0 ? 10 : -1))))))))))) \
                               )
                               
#define kth_bit(flag, k) ((flag & (1 << k)) == (1 << k))

str 
bam_flag(bit *ret, sht *flag, str *name)
{
    sht k = flag_str2sht(*name);
    if(k < 0) 
        throw(MAL, "bam_flag", "Unknown flag name given: %s\n", *name);
    *ret = kth_bit(*flag, k);
    return MAL_SUCCEED;
}

str 
reverse_seq(str *ret, str *seq)
{
    str result;
    unsigned int i;
    unsigned int len = strlen(*seq);
    result = GDKmalloc((len+1)*sizeof(char));
    if(result == NULL)
        throw(MAL, "reverse_seq", MAL_MALLOC_FAIL);
    for(i=0; i<len; ++i)
    {
        switch((*seq)[i])
        {
            case 'A': result[len-i-1] = 'T'; break;
            case 'T': result[len-i-1] = 'A'; break;
            case 'C': result[len-i-1] = 'G'; break;
            case 'G': result[len-i-1] = 'C'; break;
            case 'R': result[len-i-1] = 'Y'; break;
            case 'Y': result[len-i-1] = 'R'; break;
            case 'S': result[len-i-1] = 'S'; break;
            case 'W': result[len-i-1] = 'W'; break;
            case 'K': result[len-i-1] = 'M'; break;
            case 'M': result[len-i-1] = 'K'; break;
            case 'H': result[len-i-1] = 'D'; break;
            case 'D': result[len-i-1] = 'H'; break;
            case 'V': result[len-i-1] = 'B'; break;
            case 'B': result[len-i-1] = 'V'; break;
            case 'N': result[len-i-1] = 'N'; break;
            default : throw(MAL, "reverse_seq", "Invalid character found in sequence: '%c'\n", (*seq)[i]);
        }
    }
    result[len] = '\0';
    *ret = result;
    return MAL_SUCCEED;
}

str 
reverse_qual(str *ret, str *qual)
{
    str result;
    unsigned int i;
    unsigned int len = strlen(*qual);
    result = GDKmalloc((len+1)*sizeof(char));
    if(result == NULL)
        throw(MAL, "reverse_qual", MAL_MALLOC_FAIL);
    for(i=0; i<len; ++i)
        result[len-i-1] = (*qual)[i];
    result[len] = '\0';
    *ret = result;
    return MAL_SUCCEED;
}

str 
seq_length(int *ret, str *cigar)
{
    int result = 0;
    str cigar_consumable = *cigar;
    if(cigar_consumable[0] == '*' && cigar_consumable[1] == '\0')
    {
        *ret = -1;
        return MAL_SUCCEED;
    }
    while(cigar_consumable[0] != '\0')
    {
        int cnt;
        char op;
        int nr_chars_read;
        if(sscanf(cigar_consumable, "%d%c%n", &cnt, &op, &nr_chars_read) != 2)
            throw(MAL, "seq_length", "Error parsing CIGAR string '%s'\n", *cigar);
        if(op == 'M' || op == 'D' || op == 'N' || op == '=' || op == 'X') 
            result += cnt;
        cigar_consumable += nr_chars_read;
    }
    *ret = result;
    return MAL_SUCCEED;
}



str 
bam_flag_bat(bat *ret, bat *bid, str *name)
{
    BAT *flags, *result;
    BATiter li;
    BUN p = 0, q = 0;
    sht k;
    
    assert(ret != NULL && bid != NULL && name != NULL);
    
    k = flag_str2sht(*name);
    if(k < 0) 
        throw(MAL, "bam_flag", "Unknown flag name given: %s\n", *name);

    if ((flags = BATdescriptor(*bid)) == NULL)
		throw(MAL, "bam_flag_bat", RUNTIME_OBJECT_MISSING);
        
    /* allocate result BAT */
	result = BATnew(TYPE_oid, TYPE_bit, BATcount(flags));
	if (result == NULL) {
		throw(MAL, "bam_flag_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, flags->hseqbase);
    
    li = bat_iterator(flags);
    
    BATloop(flags, p, q) {
        ptr h = BUNhead(li, p);
        sht t = *(sht *)BUNtail(li, p);
        bit r = kth_bit(t, k);
        /* insert original head and the right flag in result BAT */
		/* BUNins() takes care of all necessary administration */
		BUNins(result, h, (ptr) &r, FALSE);
    }
    
    /* release input BAT-descriptor */
	BBPreleaseref(flags->batCacheid);

    BBPkeepref((*ret = result->batCacheid));

    return MAL_SUCCEED;
}

str 
reverse_seq_bat(bat *ret, bat *bid)
{
    BAT *seqs, *result;
    BATiter li;
    BUN p = 0, q = 0;
    
    assert(ret != NULL && bid != NULL);

    if ((seqs = BATdescriptor(*bid)) == NULL)
		throw(MAL, "reverse_seq_bat", RUNTIME_OBJECT_MISSING);
        
    /* allocate result BAT */
	result = BATnew(TYPE_oid, TYPE_str, BATcount(seqs));
	if (result == NULL) {
		throw(MAL, "reverse_seq_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, seqs->hseqbase);
    
    li = bat_iterator(seqs);
    
    BATloop(seqs, p, q) {
        ptr h = BUNhead(li, p);
        str t = (str)BUNtail(li, p);
        str r, msg;
        if((msg = reverse_seq(&r, &t)) != MAL_SUCCEED)
        {
            BBPreleaseref(result->batCacheid);
            return msg;
        }
        /* insert original head and the reversed sequence in result BAT */
		/* BUNins() takes care of all necessary administration */
		BUNins(result, h, (ptr) r, FALSE);
        GDKfree(r);
    }
    
    /* release input BAT-descriptor */
	BBPreleaseref(seqs->batCacheid);

    BBPkeepref((*ret = result->batCacheid));

    return MAL_SUCCEED;
}

str 
reverse_qual_bat(bat *ret, bat *bid)
{
    BAT *quals, *result;
    BATiter li;
    BUN p = 0, q = 0;
    
    assert(ret != NULL && bid != NULL);

    if ((quals = BATdescriptor(*bid)) == NULL)
		throw(MAL, "reverse_qual_bat", RUNTIME_OBJECT_MISSING);
        
    /* allocate result BAT */
	result = BATnew(TYPE_oid, TYPE_str, BATcount(quals));
	if (result == NULL) {
		throw(MAL, "reverse_qual_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, quals->hseqbase);
    
    li = bat_iterator(quals);
    
    BATloop(quals, p, q) {
        ptr h = BUNhead(li, p);
        str t = (str)BUNtail(li, p);
        str r, msg;
        if((msg = reverse_qual(&r, &t)) != MAL_SUCCEED)
        {
            BBPreleaseref(result->batCacheid);
            return msg;
        }
        /* insert original head and the reversed sequence in result BAT */
		/* BUNins() takes care of all necessary administration */
		BUNins(result, h, (ptr) r, FALSE);
        GDKfree(r);
    }
    
    /* release input BAT-descriptor */
	BBPreleaseref(quals->batCacheid);

    BBPkeepref((*ret = result->batCacheid));

    return MAL_SUCCEED;
}

str 
seq_length_bat(bat *ret, bat *bid)
{
    BAT *cigars, *result;
    BATiter li;
    BUN p = 0, q = 0;
    
    assert(ret != NULL && bid != NULL);

    if ((cigars = BATdescriptor(*bid)) == NULL)
		throw(MAL, "seq_length_bat", RUNTIME_OBJECT_MISSING);
        
    /* allocate result BAT */
	result = BATnew(TYPE_oid, TYPE_int, BATcount(cigars));
	if (result == NULL) {
		throw(MAL, "seq_length_bat", MAL_MALLOC_FAIL);
	}
	BATseqbase(result, cigars->hseqbase);
    
    li = bat_iterator(cigars);
    
    BATloop(cigars, p, q) {
        ptr h = BUNhead(li, p);
        str t = (str)BUNtail(li, p);
        str msg;
        int r;
        if((msg = seq_length(&r, &t)) != MAL_SUCCEED)
        {
            BBPreleaseref(result->batCacheid);
            return msg;
        }
        /* insert original head and the reversed sequence in result BAT */
		/* BUNins() takes care of all necessary administration */
		BUNins(result, h, (ptr) &r, FALSE);
    }
    
    /* release input BAT-descriptor */
	BBPreleaseref(cigars->batCacheid);

    BBPkeepref((*ret = result->batCacheid));

    return MAL_SUCCEED;
}
