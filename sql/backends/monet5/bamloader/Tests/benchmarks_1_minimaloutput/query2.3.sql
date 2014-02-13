SELECT qname, virtual_offset
FROM bam.unpaired_all_alignments_i
WHERE qname IN (
    SELECT qname
    FROM bam.unpaired_all_alignments_i
    GROUP BY qname
    HAVING SUM(bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;