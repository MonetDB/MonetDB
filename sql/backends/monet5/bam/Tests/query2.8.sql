SELECT f1.qname AS qname, f1.flag AS f1_flag, f1.rname AS f1_rname, f1.pos AS f1_pos, f1.mapq AS f1_mapq, f1.cigar AS f1_cigar, 
    f1.rnext AS f1_rnext, f1.pnext AS f1_pnext, f1.tlen AS f1_tlen, f1.seq AS f1_seq, f1.qual AS f1_qual, 
                          f2.flag AS f2_flag, f2.rname AS f2_rname, f2.pos AS f2_pos, f2.mapq AS f2_mapq, f2.cigar AS f2_cigar, 
    f2.rnext AS f2_rnext, f2.pnext AS f2_pnext, f2.tlen AS f2_tlen, f2.seq AS f2_seq, f2.qual AS f2_qual
FROM (
    SELECT *
    FROM bam.alignments_1
    WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
      AND bam.bam_flag(flag, 'seco_alig') = False
) AS f1 JOIN (
    SELECT *
    FROM bam.alignments_2
    WHERE bam.bam_flag(flag, 'firs_segm') <> bam.bam_flag(flag, 'last_segm')
      AND bam.bam_flag(flag, 'seco_alig') = False
) AS f2 
    ON  f1.qname = f2.qname
    AND bam.bam_flag(f1.flag, 'firs_segm') = bam.bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;

SELECT f1.qname AS qname, f1.flag AS f1_flag, f1.rname AS f1_rname, f1.pos AS f1_pos, f1.mapq AS f1_mapq, f1.cigar AS f1_cigar, 
    f1.rnext AS f1_rnext, f1.pnext AS f1_pnext, f1.tlen AS f1_tlen, f1.seq AS f1_seq, f1.qual AS f1_qual, 
                          f2.flag AS f2_flag, f2.rname AS f2_rname, f2.pos AS f2_pos, f2.mapq AS f2_mapq, f2.cigar AS f2_cigar, 
    f2.rnext AS f2_rnext, f2.pnext AS f2_pnext, f2.tlen AS f2_tlen, f2.seq AS f2_seq, f2.qual AS f2_qual
FROM (
   SELECT *
   FROM bam.unpaired_primary_alignments_3
) AS f1 JOIN (
    SELECT *
   FROM bam.unpaired_primary_alignments_4
) AS f2 
    ON  f1.qname = f2.qname
    AND bam.bam_flag(f1.flag, 'firs_segm') = bam.bam_flag(f2.flag, 'firs_segm')
    AND (f1.rname <> f2.rname OR f1.pos <> f2.pos)
ORDER BY qname;
