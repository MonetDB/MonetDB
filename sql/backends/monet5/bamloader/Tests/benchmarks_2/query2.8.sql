SELECT i_f1.qname AS qname, i_f1.flag AS f1_flag, i_f1.rname AS f1_rname, i_f1.pos AS f1_pos, i_f1.mapq AS f1_mapq, i_f1.cigar AS f1_cigar, 
    i_f1.rnext AS f1_rnext, i_f1.pnext AS f1_pnext, i_f1.tlen AS f1_tlen, e_f1.seq AS f1_seq, e_f1.qual AS f1_qual, 
                          i_f2.flag AS f2_flag, i_f2.rname AS f2_rname, i_f2.pos AS f2_pos, i_f2.mapq AS f2_mapq, i_f2.cigar AS f2_cigar, 
    i_f2.rnext AS f2_rnext, i_f2.pnext AS f2_pnext, i_f2.tlen AS f2_tlen, e_f2.seq AS f2_seq, e_f2.qual AS f2_qual
FROM (
(
    SELECT *
    FROM bam.intern_alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS i_f1 JOIN (
    SELECT *
    FROM bam.intern_alignments_j
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = False
) AS i_f2 
    ON  i_f1.qname = i_f2.qname
    AND bam_flag(i_f1.flag, 'firs_segm') = bam_flag(i_f2.flag, 'firs_segm')
    AND (i_f1.rname <> i_f2.rname OR i_f1.pos <> i_f2.pos)
) JOIN bam.extern_alignments_i AS e_f1
    ON i_f1.virtual_offset = e_f1.virtual_offset
  JOIN bam.extern_alignments_j AS e_f2
    ON i_f2.virtual_offset = e_f2.virtual_offset
ORDER BY qname;