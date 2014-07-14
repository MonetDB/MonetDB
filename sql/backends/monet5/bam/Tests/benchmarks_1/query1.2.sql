SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_primary_alignments_i
UNION
SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_alignments_i
WHERE bam_flag(flag, 'seco_alig') = False
ORDER BY rname, pos;
