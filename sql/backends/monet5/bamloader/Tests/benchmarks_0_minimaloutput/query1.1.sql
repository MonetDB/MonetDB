SELECT qname, virtual_offset
FROM bam.alignments_i
WHERE bam_flag(flag, 'seco_alig') = False
ORDER BY qname;
