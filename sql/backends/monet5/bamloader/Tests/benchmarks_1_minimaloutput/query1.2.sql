SELECT rname, pos, virtual_offset
FROM bam.unpaired_primary_alignments_i
UNION
SELECT rname, pos, virtual_offset
FROM bam.unpaired_alignments_i
WHERE bam_flag(flag, 'seco_alig') = False
ORDER BY rname, pos;