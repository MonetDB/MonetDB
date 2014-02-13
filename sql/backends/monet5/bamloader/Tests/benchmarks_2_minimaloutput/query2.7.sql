SELECT 'f1', qname, virtual_offset
FROM bam.intern_alignments_i
WHERE qname IN (
    SELECT distinct qname
    FROM bam.intern_alignments_i
    EXCEPT
    SELECT distinct qname
    FROM bam.intern_alignments_j
)
ORDER BY qname;