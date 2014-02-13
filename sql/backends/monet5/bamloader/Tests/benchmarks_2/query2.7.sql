SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE qname IN (
    SELECT DISTINCT qname
    FROM bam.intern_alignments_i
    EXCEPT
    SELECT DISTINCT qname
    FROM bam.intern_alignments_j
)
ORDER BY qname;
