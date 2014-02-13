WITH alig AS (
    SELECT qname, flag, rname, pos, rnext, pnext, virtual_offset
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext <> '*'
      AND pnext > 0
)
SELECT l.qname AS qname, l.virtual_offset AS l_virtual_offset, r.virtual_offset AS r_virtual_offset
FROM (
    SELECT qname, rname, pos, rnext, pnext, virtual_offset
    FROM alig
    WHERE bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT qname, rname, pos, rnext, pnext, virtual_offset
    FROM alig
    WHERE bam_flag(flag, 'last_segm') = True
) AS r
    ON l.qname = r.qname
   AND ((l.rnext = '=' AND l.rname = r.rname) OR l.rnext = r.rname)
   AND l.pnext = r.pos
   AND ((r.rnext = '=' AND l.rname = r.rname) OR r.rnext = l.rname)
   AND r.pnext = l.pos
ORDER BY l.qname;