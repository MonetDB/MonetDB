SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE qname IN (
    SELECT qname
    FROM (
        SELECT qname, bam_flag(flag, 'seco_alig') AS seco_alig, bam_flag(flag, 'segm_unma') AS segm_unma, 
            bam_flag(flag, 'firs_segm') AS firs_segm
        FROM bam.alignments_1
        WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
    ) AS qnames
    GROUP BY qname, firs_segm
    HAVING SUM(segm_unma) < COUNT(*)
       AND (COUNT(*) - SUM(seco_alig)) <> 1
)
ORDER BY qname;

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE qname IN (
    SELECT qname
    FROM (
        SELECT qname, bam_flag(flag, 'seco_alig') AS seco_alig, bam_flag(flag, 'segm_unma') AS segm_unma, 
            bam_flag(flag, 'firs_segm') AS firs_segm
        FROM bam.unpaired_all_alignments_3
        WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
    ) AS qnames
    GROUP BY qname, firs_segm
    HAVING SUM(segm_unma) < COUNT(*)
       AND (COUNT(*) - SUM(seco_alig)) <> 1
)
ORDER BY qname;
