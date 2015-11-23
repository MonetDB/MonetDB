SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE qname IN (
    SELECT qname
    FROM (
        SELECT qname, SUM(segm_unma), SUM(seco_alig), COUNT(*)
        FROM (
            SELECT qname, bam.bam_flag(flag, 'seco_alig') AS seco_alig, bam.bam_flag(flag, 'segm_unma') AS segm_unma, 
                bam.bam_flag(flag, 'firs_segm') AS firs_segm
            FROM bam.alignments_1
            WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
        ) AS sub
        GROUP BY qname, firs_segm
        HAVING SUM(segm_unma) < COUNT(*)
           AND (COUNT(*) - SUM(seco_alig)) <> 1
    ) AS qnames
);


SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE qname IN (
    SELECT qname
    FROM (
        SELECT qname, SUM(segm_unma), SUM(seco_alig), COUNT(*)
        FROM (
            SELECT qname, bam.bam_flag(flag, 'seco_alig') AS seco_alig, bam.bam_flag(flag, 'segm_unma') AS segm_unma, 
                bam.bam_flag(flag, 'firs_segm') AS firs_segm
            FROM bam.unpaired_all_alignments_3
            WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
        ) AS sub
        GROUP BY qname, firs_segm
        HAVING SUM(segm_unma) < COUNT(*)
           AND (COUNT(*) - SUM(seco_alig)) <> 1
    ) AS qnames
);


