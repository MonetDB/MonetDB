SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_i
    EXCEPT
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_j
)
ORDER BY qname;
