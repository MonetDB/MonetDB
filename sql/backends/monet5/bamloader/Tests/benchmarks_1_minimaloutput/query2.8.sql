SELECT f1.qname AS qname, f1.virtual_offset AS f1_virtual_offset, f2.virtual_offset AS f2_virtual_offset
FROM (
   SELECT qname, flag, rname, pos, virtual_offset
   FROM bam.unpaired_primary_alignments_i
) AS f1 JOIN (
    SELECT qname, flag, rname, pos, virtual_offset
   FROM bam.unpaired_primary_alignments_j
) AS f2 
    ON  f1.qname = f2.qname
    AND bam_flag(f1.flag, 'firs_segm') = bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;