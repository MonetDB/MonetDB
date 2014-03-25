SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_4
WHERE qname IN (
    SELECT qname
    FROM bam.alignments_4
    GROUP BY qname
    HAVING SUM(bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;
