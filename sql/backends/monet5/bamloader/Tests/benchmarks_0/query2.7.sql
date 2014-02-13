SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE qname IN (
    SELECT distinct qname
    FROM bam.alignments_i
    EXCEPT
    SELECT distinct qname
    FROM bam.alignments_j
)
ORDER BY qname;
