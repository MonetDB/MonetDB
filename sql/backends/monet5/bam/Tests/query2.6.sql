WITH qnames_insct AS (
    SELECT distinct qname
    FROM bam.alignments_1
    INTERSECT
    SELECT distinct qname
    FROM bam.alignments_2
)
SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
UNION
SELECT 'f2', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_2
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
ORDER BY qname;

WITH qnames_insct AS (
    SELECT distinct qname
    FROM bam.unpaired_all_alignments_3
    INTERSECT
    SELECT distinct qname
    FROM bam.unpaired_all_alignments_4
)
SELECT 'f1', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
UNION
SELECT 'f2', qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_4
WHERE qname IN (
    SELECT *
    FROM qnames_insct
)
ORDER BY qname;
