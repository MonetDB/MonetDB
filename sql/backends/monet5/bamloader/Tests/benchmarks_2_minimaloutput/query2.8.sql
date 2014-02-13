SELECT f1.qname AS qname, f1.virtual_offset AS f1_virtual_offset, f2.virtual_offset AS f2_virtual_offset
FROM (
    SELECT qname, flag, rname, pos, virtual_offset
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS f1 JOIN (
    SELECT qname, flag, rname, pos, virtual_offset
    FROM bam.intern_alignments_j
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS f2 
    ON  f1.qname = f2.qname
    AND bam_flag(f1.flag, 'firs_segm') = bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;