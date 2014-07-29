SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE bam.bam_flag(flag, 'seco_alig') = False
ORDER BY rname, pos;

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_primary_alignments_3
UNION
SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_alignments_3
WHERE bam.bam_flag(flag, 'seco_alig') = False
ORDER BY rname, pos;
