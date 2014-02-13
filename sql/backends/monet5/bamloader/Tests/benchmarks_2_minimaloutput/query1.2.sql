SELECT rname, pos, virtual_offset
FROM bam.intern_alignments_i
WHERE bam_flag(flag, 'seco_alig') = False
ORDER BY rname, pos;
