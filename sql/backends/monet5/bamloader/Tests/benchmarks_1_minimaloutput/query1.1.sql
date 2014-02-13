SELECT qname, virtual_offset
FROM bam.unpaired_primary_alignments_i
UNION
SELECT qname, virtual_offset
FROM bam.unpaired_alignments_i
WHERE bam_flag(flag, 'seco_alig') = False
ORDER BY qname;
