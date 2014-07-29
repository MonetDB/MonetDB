WITH alig AS (
    SELECT *
    FROM bam.alignments_1
    WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
      AND bam.bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext <> '*'
      AND pnext > 0
)
SELECT l.qname AS qname, l.flag AS l_flag, l.rname AS l_rname, l.pos AS l_pos, l.mapq AS l_mapq, l.cigar AS l_cigar, 
    l.rnext AS l_rnext, l.pnext AS l_pnext, l.tlen AS l_tlen, l.seq AS l_seq, l.qual AS l_qual, 
                         r.flag AS r_flag, r.rname AS r_rname, r.pos AS r_pos, r.mapq AS r_mapq, r.cigar AS r_cigar, 
    r.rnext AS r_rnext, r.pnext AS r_pnext, r.tlen AS r_tlen, r.seq AS r_seq, r.qual AS r_qual
FROM (
    SELECT *
    FROM alig
    WHERE bam.bam_flag(flag, 'firs_segm') = True
) AS l JOIN (
    SELECT *
    FROM alig
    WHERE bam.bam_flag(flag, 'last_segm') = True
) AS r
    ON l.qname = r.qname
   AND ((l.rnext = '=' AND l.rname = r.rname) OR l.rnext = r.rname)
   AND l.pnext = r.pos
   AND ((r.rnext = '=' AND l.rname = r.rname) OR r.rnext = l.rname)
   AND r.pnext = l.pos
ORDER BY l.qname;

SELECT qname, l_flag, l_rname, l_pos, l_mapq, l_cigar, l_rnext, l_pnext, l_tlen, l_seq, l_qual,
              r_flag, r_rname, r_pos, r_mapq, r_cigar, r_rnext, r_pnext, r_tlen, r_seq, r_qual
FROM bam.paired_secondary_alignments_3
ORDER BY qname;
