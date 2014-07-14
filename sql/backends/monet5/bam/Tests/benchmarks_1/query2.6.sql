WITH qnames_insct AS (
    SELECT distinct qname
    FROM bam.unpaired_all_alignments_i
    INTERSECT
    SELECT distinct qname
    FROM bam.unpaired_all_alignments_j
)
SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
UNION
SELECT 'f2', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_j
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
ORDER BY qname;
