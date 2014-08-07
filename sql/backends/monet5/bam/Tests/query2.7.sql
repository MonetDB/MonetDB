SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE qname IN (
    SELECT distinct qname
    FROM bam.alignments_1
    EXCEPT
    SELECT distinct qname
    FROM bam.alignments_2
)
ORDER BY qname;

SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE qname IN (
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_3
    EXCEPT
    SELECT DISTINCT qname
    FROM bam.unpaired_all_alignments_4
)
ORDER BY qname;
