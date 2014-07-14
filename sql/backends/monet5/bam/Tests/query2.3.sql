SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE qname IN (
    SELECT qname
    FROM bam.alignments_1
    GROUP BY qname
    HAVING SUM(bam.bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam.bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE qname IN (
    SELECT qname
    FROM bam.unpaired_all_alignments_3
    GROUP BY qname
    HAVING SUM(bam.bam_flag(flag, 'firs_segm')) = 0
        OR SUM(bam.bam_flag(flag, 'last_segm')) = 0
)
ORDER BY qname;
